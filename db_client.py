from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import os
import re

import pandas as pd
import pyodbc

import logging
log = logging.getLogger(__name__)


# ============================================================================
# Konfiguration
# ============================================================================

@dataclass
class SqlConfig:
    """
    Konfiguration für den DB-Zugriff.

    Hinweise:
      - Kein automatisches TOP-Limit. Die Abfrage wird genau so ausgeführt,
        wie sie in `sql` angegeben ist.
      - max_rows kann für spätere Features (Chunked-Read) genutzt werden,
        ändert aber die SQL aktuell NICHT.
    """
    server: str = ""
    database: str = ""
    user: str = ""
    password: str = ""
    sql: str = ""
    max_rows: Optional[int] = 250_000
    login_timeout_s: int = 15         # Login/Connect Timeout
    query_timeout_s: int = 60         # Abfrage-Timeout (pro Statement)


# ============================================================================
# DB-Client
# ============================================================================

class DbClient:
    """
    Kleiner Wrapper um pyodbc, der
      - eine gesunde Connection-String-Ermittlung vornimmt,
      - reine SELECTs validiert,
      - Timeouts setzt,
      - und das Result als DataFrame zurückliefert.
    """

    # Bevorzugte Treiber-Reihenfolge (Windows)
    _PREF_DRIVERS = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]

    # Schlüsselwörter, die in reinen SELECT-Statements NICHT vorkommen dürfen
    _FORBIDDEN = re.compile(
        r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    # Grobe SELECT-Erkennung (erlaubt auch WITH-CTE vor SELECT)
    _SELECT_ALLOW = re.compile(
        r"^\s*(WITH\b.*?\)\s*)?SELECT\b",
        re.IGNORECASE | re.DOTALL,
    )

    # Mehrere Statements trennen wir nicht sicher — warnen nur, wenn Semikolon-Kaskaden auffallen
    _SEMICOLON_MULTI = re.compile(r";\s*\S")

    # ------------------------------------------------------------------ #
    # Öffentliche API
    # ------------------------------------------------------------------ #

    def read_select(self, cfg: SqlConfig, cancel, progress) -> pd.DataFrame:
        """
        Führt die SELECT-Abfrage aus und gibt ein DataFrame zurück.

        Parameters
        ----------
        cfg: SqlConfig            - DB-Einstellungen inkl. SQL
        cancel: threading.Event   - Abbruchsignal (wird vor dem Start geprüft)
        progress: queue.Queue[str]- Statusmeldungen

        Returns
        -------
        pandas.DataFrame
        """
        sql = self._validate_sql(cfg.sql)
        if cancel.is_set():
            progress.put("DB: Abbruch vor Start.")
            return pd.DataFrame()

        progress.put("DB: Verbinde …")
        with self._connect(cfg) as conn:
            # pyodbc Query-Timeout (Sekunden) auf Connection-/Cursor-Ebene
            try:
                conn.timeout = int(cfg.query_timeout_s)
            except Exception:
                pass

            if cancel.is_set():
                progress.put("DB: Abbruch vor Ausführung.")
                return pd.DataFrame()

            progress.put("DB: Lese Daten …")
            try:
                # Hinweis: pandas warnt, dass fremde DBAPI nicht „getestet“ sind.
                # Für SQL Server via pyodbc ist das in Ordnung.
                df = pd.read_sql(sql, conn)
            except pyodbc.OperationalError as e:
                # Login/Timeout etc.
                raise RuntimeError(f"DB-Fehler (Operational): {e}") from e
            except pyodbc.ProgrammingError as e:
                # z. B. Syntaxfehler
                raise RuntimeError(f"DB-Fehler (SQL-Syntax/Programmierung): {e}") from e
            except Exception as e:
                raise RuntimeError(f"DB-Fehler: {e}") from e

        # Bytes/Varbinary-Spalten in Hex wandeln (Export/Preview-freundlich)
        if not df.empty:
            for col in df.columns:
                # Schnelle Heuristik: nur prüfen, wenn mind. ein bytes-Wert vorkommt
                if df[col].apply(lambda v: isinstance(v, (bytes, bytearray))).any():
                    df[col] = df[col].apply(lambda b: b.hex() if isinstance(b, (bytes, bytearray)) else b)

        progress.put(f"DB: {len(df):,} Zeilen gelesen.")
        return df

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _connect(self, cfg: SqlConfig) -> pyodbc.Connection:
        """
        Erzeugt eine pyodbc-Verbindung für SQL Server.

        Unterstützt:
          - DSN=... in `server`
          - SQL-Auth (user/password gesetzt)
          - Windows-Auth (Trusted_Connection=yes) bei leerem user
        """
        # DSN explizit?
        if cfg.server.strip().upper().startswith("DSN="):
            conn_str = f"{cfg.server};DATABASE={cfg.database};"
            # Trusted vom Benutzer regeln – oder per USER/PWD ergänzen
            if cfg.user:
                conn_str += f"UID={cfg.user};PWD={cfg.password};"
            else:
                conn_str += "Trusted_Connection=yes;"
            return self._open(conn_str, cfg)

        # Sonst: Treiber ermitteln
        driver = self._pick_driver()
        if not driver:
            # Fallback ohne Driver-Angabe (ODBC versucht Default)
            log.warning("Kein bevorzugter SQL Server ODBC-Treiber gefunden. Verwende Fallback ohne DRIVER=…")
            driver_part = ""
        else:
            driver_part = f"DRIVER={{{driver}}};"

        parts = [
            driver_part,
            f"SERVER={cfg.server};",
            f"DATABASE={cfg.database};",
            "Encrypt=yes;",
            "TrustServerCertificate=yes;",
        ]

        if cfg.user:
            parts.append(f"UID={cfg.user};PWD={cfg.password};")
        else:
            parts.append("Trusted_Connection=yes;")

        conn_str = "".join(parts)
        return self._open(conn_str, cfg)

    def _open(self, conn_str: str, cfg: SqlConfig) -> pyodbc.Connection:
        """Öffnet die pyodbc-Verbindung mit Login-Timeout und gibt sie zurück."""
        log.debug("ODBC connect", extra={"server": cfg.server, "database": cfg.database})
        try:
            return pyodbc.connect(
                conn_str,
                timeout=cfg.login_timeout_s,   # Login-Timeout
                autocommit=True,               # reine Reads; keine Transaktionen nötig
            )
        except pyodbc.InterfaceError as e:
            # Klassischer Fehler: DSN nicht gefunden / Treiber fehlt
            raise ConnectionError(str(e)) from e
        except Exception as e:
            raise ConnectionError(f"ODBC-Verbindung fehlgeschlagen: {e}") from e

    @classmethod
    def _pick_driver(cls) -> str | None:
        """Wählt den „besten“ installierten SQL Server ODBC-Treiber aus."""
        try:
            drivers = [d.strip() for d in pyodbc.drivers()]
        except Exception:
            drivers = []
        for pref in cls._PREF_DRIVERS:
            if pref in drivers:
                return pref
        return None

    @classmethod
    def _validate_sql(cls, sql: str) -> str:
        """
        Sehr konservative Validierung:
          - Erlaubt nur Statements, die mit SELECT (oder WITH … SELECT) beginnen.
          - Verbietet offensichtliche DDL/DML/EXEC-Schlüsselwörter.
          - Warnt bei mehreren Statements (Semikolon + Nicht-Leer danach).

        Wir führen KEINE Umschreibungen (z. B. TOP) am SQL durch.
        """
        if not sql or not sql.strip():
            raise ValueError("Leere SQL-Anweisung.")

        text = sql.strip()

        if not cls._SELECT_ALLOW.search(text):
            raise ValueError("Nur SELECT-Statements sind erlaubt (ggf. WITH…SELECT).")

        if cls._FORBIDDEN.search(text):
            raise ValueError("SQL enthält verbotene Schlüsselwörter (DML/DDL/EXEC).")

        if cls._SEMICOLON_MULTI.search(text):
            log.warning("SQL scheint mehrere Statements zu enthalten (Semikolon erkannt).")

        return text
