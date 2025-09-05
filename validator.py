from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Tuple
import math
import re
from datetime import datetime
from decimal import Decimal

import pandas as pd

try:
    from py_mini_racer import py_mini_racer
except Exception as e:  # pragma: no cover
    raise ImportError(
        "py-mini-racer ist nicht installiert. Bitte `pip install py-mini-racer` ausführen."
    ) from e


# ============================================================================
# Datentypen
# ============================================================================

@dataclass
class ValidatorResult:
    ok: bool
    msg: str | None = None
    extra: dict[str, Any] | None = None


# ============================================================================
# JS-Validator
# ============================================================================

class JsValidator:
    """
    Führt eine JS-Funktion `validate(dbSet, apiSet)` für jede gejointe Zeile aus.

    Rückgabewerte von `validate`:
      - boolean            -> ok
      - string             -> ok = False, msg = string
      - object/dict        -> erwartet { ok: bool, msg?: string, ... }
                              Weitere Felder werden als `validation_<key>`-Spalten übernommen.

    Logging:
      - console.log(...) im JS landet in einem JS-Array __logs__
      - Python liest die Logs zyklisch aus und schreibt sie (prefixed) in die Progress-Queue
    """

    def __init__(self, script_path: str | None = None, script_code: str | None = None) -> None:
        if not script_code and not script_path:
            raise ValueError("Es muss entweder script_code oder script_path angegeben werden.")
        self.path = Path(script_path) if script_path else None
        if self.path is not None and not self.path.exists():
            raise FileNotFoundError(f"Validator-Skript nicht gefunden: {self.path}")

        # JS-Kontext
        self.ctx = py_mini_racer.MiniRacer()

        # Robustes console.log (Arguments → Strings, JSON für Objekte)
        self.ctx.eval(
            """
            var __logs__ = [];
            var console = {
              log: function() {
                try {
                  var args = Array.prototype.slice.call(arguments).map(function(x){
                    try {
                      if (x === null || x === undefined) return String(x);
                      if (typeof x === 'object') return JSON.stringify(x);
                      return String(x);
                    } catch (e) {
                      return String(x);
                    }
                  });
                  __logs__.push(args.join(' '));
                } catch (e) {
                  __logs__.push('[console.log error] ' + String(e));
                }
              }
            };
            """
        )

        # Skript laden
        code = script_code if script_code is not None else self.path.read_text(encoding="utf-8")
        self.ctx.eval(code)

        # validate() vorhanden?
        has_fn = self.ctx.eval("typeof validate === 'function'")
        if not has_fn:
            raise ValueError("Im JS-Skript wurde keine Funktion `validate(dbSet, apiSet)` gefunden.")

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _flush_logs(self) -> list[str]:
        """Liest die in JS gesammelten console.log-Zeilen aus und leert den Puffer."""
        logs = self.ctx.eval("__logs__")
        self.ctx.eval("__logs__ = [];")
        if not isinstance(logs, list):
            return []
        return [str(x) for x in logs]

    @staticmethod
    def _coerce_value(v: Any) -> Any:
        """
        Konvertiert unterschiedliche Python/Pandas/Numpy-Typen in JSON-/JS-geeignete Werte:
        - bytes/bytearray -> hex-String
        - Pandas/NumPy NA/NaN/Inf -> None
        - Decimal -> float
        - datetime/date -> ISO-String
        - Sonst: JSON-geeignete Typen unverändert, andere -> str(v) (Fallback)
        """
        # Bytes -> hex
        if isinstance(v, (bytes, bytearray)):
            return v.hex()

        # Pandas/NumPy NaN/NA -> None
        try:
            import numpy as _np  # optional
            if v is pd.NA:  # type: ignore
                return None
            if isinstance(v, _np.generic):
                v = v.item()
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
        except Exception:
            pass

        # Decimal -> float
        if isinstance(v, Decimal):
            return float(v)

        # Datetime/Date -> ISO-String
        if isinstance(v, (datetime, )):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        if hasattr(v, "isoformat"):
            try:
                return v.isoformat()
            except Exception:
                pass

        # JSON-fähig oder String-Fallback
        if isinstance(v, (dict, list, str, int, float, bool)) or v is None:
            return v
        try:
            return str(v)
        except Exception:
            return None

    @staticmethod
    def _split_row(row: pd.Series) -> Tuple[dict[str, Any], dict[str, Any]]:
        """
        Zerlegt eine gejointe Zeile in zwei Objekte:
          - dbSet:  alle Spalten mit Prefix 'db_'
          - apiSet: alle Spalten mit Prefix 'api_'
        Werte werden zuvor mit _coerce_value() JS-tauglich gemacht.
        """
        db: dict[str, Any] = {}
        api: dict[str, Any] = {}
        for k, v in row.items():
            cv = JsValidator._coerce_value(v)
            if k.startswith("db_"):
                db[k] = cv
            elif k.startswith("api_"):
                api[k] = cv
        return db, api

    @staticmethod
    def _coerce_result(res: Any) -> ValidatorResult:
        """Normiert mögliche Rückgabewerte des JS in ein ValidatorResult."""
        if isinstance(res, bool):
            return ValidatorResult(ok=res, msg=None, extra=None)

        if isinstance(res, str):
            return ValidatorResult(ok=False, msg=res, extra=None)

        if isinstance(res, dict):
            ok = bool(res.get("ok", False))
            msg = res.get("msg")
            extra = {k: v for k, v in res.items() if k not in ("ok", "msg")}
            return ValidatorResult(ok=ok, msg=msg, extra=extra if extra else None)

        return ValidatorResult(
            ok=False,
            msg=f"Ungültiger Rückgabetyp: {type(res).__name__}",
            extra=None,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(
        self,
        df_joined: pd.DataFrame,
        progress_q=None,
        fix_dir: str | None = None,
    ):
        """
        Führt validate(dbSet, apiSet) für jede Zeile aus.

        Rückgabe:
          - DataFrame-Kopie mit Spalten:
              * validation_ok   (bool)
              * validation_msg  (str | None)
              * validation_<k>  (weitere Felder aus dem JS-Objekt)
          - Liste aller console.log()-Zeilen (zusätzlich wurden diese bereits über progress_q publiziert)

        Optional:
          - fix_dir: Wenn gesetzt, werden vorhandene Fix-Skripte (wawi/api) als .js-Dateien
                     dort gespeichert; ins Log kommt nur der Pfad.
        """

        def _write_fix_file(kind: str, content: str, directory: str) -> str:
            base = Path(directory)
            base.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"{kind}_fix_{ts}.js"
            # Windows-kompatibel „säubern“
            safe_name = re.sub(r'[^A-Za-z0-9_\\.-]', '_', fname)
            path = base / safe_name
            path.write_text(content, encoding="utf-8")
            return str(path)

        if df_joined is None or df_joined.empty:
            return df_joined.copy(), ["Keine Daten zum Prüfen."]

        results_ok: list[bool] = []
        results_msg: list[str | None] = []
        extra_columns: dict[str, list[Any]] = {}
        logs_total: list[str] = []

        if progress_q is not None:
            progress_q.put(f"[JS] Starte Validierung, {len(df_joined)} Zeilen …")

        # Optional: Sammelcontainer für Fix-Skripte (vom JS erzeugt)
        wawi_fix_script: str = ""
        api_fix_script: str = ""

        for idx, (_, row) in enumerate(df_joined.iterrows()):
            if progress_q is not None and (idx % 1000 == 0) and idx > 0:
                progress_q.put(f"[JS] geprüft: {idx} Zeilen …")

            dbSet, apiSet = self._split_row(row)

            try:
                res = self.ctx.call("validate", dbSet, apiSet)
            except Exception as e:
                res = {"ok": False, "msg": f"JS-Fehler: {e}"}

            coerced = self._coerce_result(res)
            results_ok.append(coerced.ok)
            results_msg.append(coerced.msg)

            if coerced.extra:
                for k, v in coerced.extra.items():
                    # falls "wawi_fix" / "api_fix" als Strings zurückkommen, sammeln
                    if k == "wawi_fix" and isinstance(v, str):
                        wawi_fix_script += (v if v.endswith("\n") else v + "\n")
                        continue
                    if k == "api_fix" and isinstance(v, str):
                        api_fix_script += (v if v.endswith("\n") else v + "\n")
                        continue

                    col = f"validation_{k}"
                    extra_columns.setdefault(col, []).append(v)

            # Logs einsammeln & publishen
            chunk_logs = self._flush_logs()
            if chunk_logs:
                logs_total.extend(chunk_logs)
                if progress_q is not None:
                    for line in chunk_logs:
                        progress_q.put(f"[JS] {line}")

        # Extra-Spalten auf gleiche Länge auffüllen
        max_len = len(results_ok)
        for col, arr in extra_columns.items():
            if len(arr) < max_len:
                arr.extend([None] * (max_len - len(arr)))

        out = df_joined.copy()
        out["validation_ok"] = results_ok
        out["validation_msg"] = results_msg
        for col, arr in extra_columns.items():
            out[col] = arr

        # Rest-Logs
        leftover = self._flush_logs()
        if leftover:
            logs_total.extend(leftover)
            if progress_q is not None:
                for line in leftover:
                    progress_q.put(f"[JS] {line}")

        # Fix-Skripte speichern (nur wenn nicht leer)
        if wawi_fix_script.strip():
            if fix_dir:
                try:
                    wpath = _write_fix_file("wawi", wawi_fix_script, fix_dir)
                    if progress_q is not None:
                        progress_q.put(f"[JS] Wawi-Fix gespeichert: {wpath}")
                except Exception as _e:
                    if progress_q is not None:
                        progress_q.put(f"[JS] Wawi-Fix konnte nicht gespeichert werden: {_e}")
            else:
                if progress_q is not None:
                    progress_q.put("[JS] Wawi-Fix vorhanden (kein fix_dir angegeben) - nicht gespeichert.")

        if api_fix_script.strip():
            if fix_dir:
                try:
                    apath = _write_fix_file("api", api_fix_script, fix_dir)
                    if progress_q is not None:
                        progress_q.put(f"[JS] API-Fix gespeichert: {apath}")
                except Exception as _e:
                    if progress_q is not None:
                        progress_q.put(f"[JS] API-Fix konnte nicht gespeichert werden: {_e}")
            else:
                if progress_q is not None:
                    progress_q.put("[JS] API-Fix vorhanden (kein fix_dir angegeben) - nicht gespeichert.")

        return out, logs_total
