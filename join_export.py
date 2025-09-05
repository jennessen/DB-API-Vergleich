from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import os
from datetime import datetime

import pandas as pd

import logging
log = logging.getLogger(__name__)



# ============================================================================
# Public API
# ============================================================================

def join_and_export(
    df_db: pd.DataFrame,
    df_api: pd.DataFrame,
    db_key: str,
    api_key: str,
    how: str,
    pre_db: str,
    pre_api: str,
    base_dir: Optional[str] = None,
    merged_override: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Führt den Join durch (sofern merged_override None ist) und exportiert DB, API und Merged als CSV + XLSX.

    Parameters
    ----------
    df_db, df_api : DataFrames mit den Originaldaten
    db_key, api_key : Join-Schlüssel (Spaltennamen in df_db / df_api)
    how : 'inner' | 'left' | 'right' | 'outer'
    pre_db, pre_api : Spaltenpräfixe für die Quelle-Spalten
    base_dir : Zielverzeichnis (wird erstellt), optional
    merged_override : Wenn gesetzt, wird dieser DataFrame als 'merged' verwendet und kein Join mehr durchgeführt

    Returns
    -------
    merged_df : pd.DataFrame
    paths : Dict[str, str] mit 'folder' und einzelnen Dateipfaden
    """
    # 1) Vorbereitung: Ausgabepfad
    out_dir = _make_out_dir(base_dir)
    paths: Dict[str, str] = {"folder": out_dir}

    # 2) Join erstellen oder override nutzen
    if merged_override is None:
        merged = _perform_join(
            df_db=df_db,
            df_api=df_api,
            db_key=db_key,
            api_key=api_key,
            how=how,
            pre_db=pre_db,
            pre_api=pre_api,
        )
    else:
        merged = merged_override.copy()

    # 3) Export CSV
    paths["db_csv"] = os.path.join(out_dir, "db.csv")
    paths["api_csv"] = os.path.join(out_dir, "api.csv")
    paths["merged_csv"] = os.path.join(out_dir, "merged.csv")

    df_db.to_csv(paths["db_csv"], index=False, encoding="utf-8-sig")
    df_api.to_csv(paths["api_csv"], index=False, encoding="utf-8-sig")
    merged.to_csv(paths["merged_csv"], index=False, encoding="utf-8-sig")

    # 4) Export Excel (ein Workbook, drei Sheets)
    paths["xlsx"] = os.path.join(out_dir, "export.xlsx")
    try:
        _to_excel_xlsxwriter(
            {
                "db": df_db,
                "api": df_api,
                "merged": merged,
            },
            paths["xlsx"],
        )
    except Exception as ex:
        log.warning("Excel-Export fehlgeschlagen: %s", ex)

    log.info("Export fertig: %s", out_dir)
    return merged, paths


# ============================================================================
# Internals: Join & Export
# ============================================================================

def _perform_join(
    df_db: pd.DataFrame,
    df_api: pd.DataFrame,
    db_key: str,
    api_key: str,
    how: str,
    pre_db: str,
    pre_api: str,
) -> pd.DataFrame:
    """
    Führt den eigentlichen Join aus:
      - Key-Spalten stripp'en und in string casten (wo möglich),
      - Spalten prefixen,
      - Merge durchführen.
    """
    def _safe_str_strip(series: pd.Series) -> pd.Series:
        try:
            return series.astype("string").str.strip()
        except Exception:
            return series

    if db_key not in df_db.columns:
        raise KeyError(f"DB-Key '{db_key}' nicht gefunden.")
    if api_key not in df_api.columns:
        raise KeyError(f"API-Key '{api_key}' nicht gefunden.")

    df1 = df_db.copy()
    df2 = df_api.copy()

    df1[db_key] = _safe_str_strip(df1[db_key])
    df2[api_key] = _safe_str_strip(df2[api_key])

    df1 = df1.add_prefix(pre_db)
    df2 = df2.add_prefix(pre_api)

    merged = pd.merge(
        df1,
        df2,
        left_on=pre_db + db_key,
        right_on=pre_api + api_key,
        how=how,
    )
    return merged


def _make_out_dir(base_dir: Optional[str]) -> str:
    """
    Erzeugt (falls nötig) das Ausgabeverzeichnis.
    Legt innerhalb des Basisverzeichnisses einen Zeitstempel-Ordner an.
    """
    base = base_dir or os.path.join(os.path.expanduser("~"), "Desktop", "DB_API_Compare_Exports")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(base, f"run_{ts}")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def _to_excel_xlsxwriter(sheets: Dict[str, pd.DataFrame], path: str) -> None:
    """
    Schreibt mehrere DataFrames in ein XLSX-Workbook (ein Sheet pro Eintrag).
    - Engine: xlsxwriter
    - AutoFilter in erster Zeile
    - Freeze Panes (1, 0)
    - Leichtgewichtiges AutoFit der Spaltenbreite (max. 60 Zeichen)
    """
    # WICHTIG: Für pandas>=2.1 ist 'options' nicht mehr im ExcelWriter-Konstruktor,
    # daher setzen wir Formatoptionen über das Workbook/Worksheet-API.
    with pd.ExcelWriter(path, engine="xlsxwriter") as xw:
        wb = xw.book

        for name, df in sheets.items():
            # Tabellenblatt schreiben
            df.to_excel(xw, sheet_name=name, index=False)
            ws = xw.sheets[name]

            if df.shape[1] == 0:
                continue

            # AutoFilter + Freeze Panes
            ws.autofilter(0, 0, max(0, len(df.index)), max(0, df.shape[1] - 1))
            ws.freeze_panes(1, 0)

            # AutoFit: schätze Breiten anhand Header + bis zu N ersten Zeilen
            _autofit_worksheet(ws, df)


def _autofit_worksheet(ws, df: pd.DataFrame, sample_rows: int = 200, min_w: int = 8, max_w: int = 60) -> None:
    """
    Einfaches AutoFit für xlsxwriter-Worksheet:
      - misst Header und bis zu `sample_rows` Datenzeilen,
      - setzt Spaltenbreite zwischen min_w und max_w.
    """
    # Headerbreiten
    col_widths = [max(min_w, min(max_w, len(str(col)) + 2)) for col in df.columns]

    # Datenbreiten (nur Stichprobe, um Performance zu schonen)
    if not df.empty:
        for i, col in enumerate(df.columns):
            # Nur einen Ausschnitt untersuchen
            sample = df[col].head(sample_rows)
            for val in sample:
                s = "" if val is None else str(val)
                # Excel nutzt eher Zeichenbreiten; +2 Padding
                w = len(s) + 2
                if w > col_widths[i]:
                    col_widths[i] = min(max_w, w)

    # Setze Breiten
    for idx, width in enumerate(col_widths):
        ws.set_column(idx, idx, width)
