from __future__ import annotations
import os
import threading
import queue
from dataclasses import dataclass
from typing import Optional, Tuple
import pandas as pd

from .ports import UiPort, UiInputs
from db_client import DbClient, SqlConfig
from api_client import ApiClient, ApiConfig
from join_export import join_and_export
from time_utils import make_iso_range
from sanitization import redact

try:
    from validator import JsValidator
except Exception:
    JsValidator = None


@dataclass
class RunState:
    df_db: Optional[pd.DataFrame] = None
    df_api: Optional[pd.DataFrame] = None
    df_merged: Optional[pd.DataFrame] = None


class AppController:
    def __init__(self, ui: UiPort, inputs: UiInputs) -> None:
        self.ui = ui
        self.inputs = inputs
        self._progress_q: "queue.Queue[str]" = queue.Queue()
        self._cancel = threading.Event()
        self._run_thread: Optional[threading.Thread] = None
        self._reval_thread: Optional[threading.Thread] = None
        self.state = RunState()

    # -------- Progress pump --------
    def start_progress_pump(self, tk_after_call) -> None:
        def _drain():
            try:
                while True:
                    msg = self._progress_q.get_nowait()
                    self.ui.log(msg)
            except queue.Empty:
                pass
            tk_after_call(200, _drain)
        tk_after_call(150, _drain)

    # -------- UI events --------
    def run(self) -> None:
        if self._run_thread and self._run_thread.is_alive():
            return
        self._cancel.clear()
        self.ui.set_running(True)
        self.ui.enable_export(False)
        self.ui.enable_revalidate(False)
        self.state = RunState()
        self._run_thread = threading.Thread(target=self._worker_run, daemon=True)
        self._run_thread.start()

    def cancel(self) -> None:
        self._cancel.set()
        self._progress_q.put("Abbruch angefordert …")

    def export(self) -> None:
        if not (self.state.df_db is not None and self.state.df_api is not None and self.state.df_merged is not None):
            self.ui.show_error("Export", "Kein Ergebnis zum Exportieren vorhanden.")
            return
        try:
            merged_out, paths = join_and_export(
                df_db=self.state.df_db,
                df_api=self.state.df_api,
                db_key=self.inputs.get_join_db_key().strip(),
                api_key=self.inputs.get_join_api_key().strip(),
                how=self.inputs.get_join_how(),
                pre_db=self.inputs.get_join_db_prefix().strip() or "db_",
                pre_api=self.inputs.get_join_api_prefix().strip() or "api_",
                base_dir=self.inputs.get_export_dir(),
                merged_override=self.state.df_merged,
            )
            self.state.df_merged = merged_out
            self._progress_q.put(f"Export abgeschlossen: {paths.get('folder', '')}")
        except Exception as ex:
            self.ui.show_error("Export-Fehler", redact(str(ex)))

    def revalidate(self) -> None:
        if self._reval_thread and self._reval_thread.is_alive():
            return
        if self.state.df_merged is None:
            self.ui.show_error("Prüfung", "Es liegen noch keine Daten vor.")
            return
        script = self.inputs.get_validator_script_path().strip()
        if not script:
            self.ui.show_error("Prüfung", "Kein Validator-Skript angegeben.")
            return
        if JsValidator is None:
            self.ui.show_error("Prüfung", "Validator nicht verfügbar (py-mini-racer fehlt).")
            return
        self.ui.set_status("Prüfung läuft …")
        self.ui.enable_revalidate(False)
        self._reval_thread = threading.Thread(target=self._worker_revalidate, daemon=True)
        self._reval_thread.start()

    # -------- workers --------
    def _worker_run(self) -> None:
        try:
            sql_cfg = SqlConfig(
                server=self.inputs.get_db_server().strip(),
                database=self.inputs.get_db_database().strip(),
                user=self.inputs.get_db_user().strip(),
                password=self.inputs.get_db_password().strip(),
                sql=self.inputs.get_db_sql(),
                max_rows=self.inputs.get_profile_max_rows(),
            )
            api_cfg = ApiConfig(
                base_url=self.inputs.get_api_base_url().strip(),
                role=self.inputs.get_api_role().strip(),
                resource=self.inputs.get_api_resource().strip(),
                alias=self.inputs.get_api_alias().strip(),
                auth=self.inputs.get_api_auth().strip(),
                use_updates=self.inputs.get_api_use_updates(),
                page_cap=self.inputs.get_profile_page_cap(),
                timeout_s=self.inputs.get_profile_api_timeout(),
                select=(self.inputs.get_api_select() or "").strip(),
                expand=(getattr(self.inputs, 'get_api_expand', lambda: "")() or "").strip(),
                filter=(getattr(self.inputs, 'get_api_filter', lambda: "")() or "").strip(),
            )

            from_iso, to_iso = self._resolve_iso_range(api_cfg.use_updates)

            self._progress_q.put("Lese DB …")
            df_db = DbClient().read_select(sql_cfg, cancel=self._cancel, progress=self._progress_q)
            if self._cancel.is_set(): return

            self._progress_q.put("Rufe API auf …")
            api = ApiClient()
            df_api = api.get_dataframe(api_cfg, from_iso, to_iso, cancel=self._cancel, progress=self._progress_q)
            if self._cancel.is_set(): return

            self._progress_q.put("Join …")
            df_merged = self._perform_join_only(
                df_db=df_db,
                df_api=df_api,
                db_key=self.inputs.get_join_db_key().strip(),
                api_key=self.inputs.get_join_api_key().strip(),
                how=self.inputs.get_join_how(),
                pre_db=self.inputs.get_join_db_prefix().strip() or "db_",
                pre_api=self.inputs.get_join_api_prefix().strip() or "api_",
            )

            if self.inputs.get_validate_on_run() and self.inputs.get_validator_script_path().strip():
                df_merged = self._run_validator(df_merged)

            if self._cancel.is_set(): return
            self.state = RunState(df_db=df_db, df_api=df_api, df_merged=df_merged)
            self.ui.show_dataframe(df_merged)
            self._progress_q.put("Join abgeschlossen – Vorschau aktualisiert.")
            self.ui.enable_export(True)
            self.ui.enable_revalidate(True)

        except Exception as ex:
            self.ui.show_error("Fehler", redact(str(ex)))
        finally:
            self.ui.set_running(False)

    def _worker_revalidate(self) -> None:
        try:
            base = self.state.df_merged.copy()
            drop_cols = [c for c in base.columns if c.startswith("validation_")]
            if drop_cols:
                base = base.drop(columns=drop_cols, errors="ignore")

            self._progress_q.put("Erneute Prüfung: starte Validator …")
            self.ui.open_validator_window()
            validated = self._run_validator(base)
            self.state.df_merged = validated
            self.ui.show_dataframe(validated)
            self._progress_q.put("Erneute Prüfung abgeschlossen – Vorschau aktualisiert.")
        except Exception as ex:
            self.ui.show_error("Prüfung fehlgeschlagen", redact(str(ex)))
        finally:
            self.ui.set_status("Bereit.")
            self.ui.enable_revalidate(True)

    # -------- helpers --------
    def _run_validator(self, df_in: pd.DataFrame) -> pd.DataFrame:
        script_path = self.inputs.get_validator_script_path().strip()
        if not script_path: return df_in
        if JsValidator is None:
            self._progress_q.put("Validator nicht verfügbar: py-mini-racer nicht installiert")
            return df_in
        self.ui.open_validator_window()
        self._progress_q.put(f"Validator: {script_path}")
        validator = JsValidator(script_path)
        validated, _ = validator.run(
            df_in,
            progress_q=self._progress_q,
            fix_dir=os.path.join(self.inputs.get_export_dir(), "validator_fixes"),
        )
        return validated

    def _resolve_iso_range(self, use_updates: bool) -> Tuple[str, str]:
        if not use_updates:
            return "", ""
        from_date = self.inputs.get_api_from_date().strip()
        to_date = self.inputs.get_api_to_date().strip()
        from_time = self.inputs.get_api_from_time().strip()
        to_time = self.inputs.get_api_to_time().strip()
        tz = self.inputs.get_profile_timezone()

        if (from_date.lower().startswith("yyyy")) or (to_date.lower().startswith("yyyy")):
            import datetime as dt
            today = dt.date.today()
            return make_iso_range(today, "00:00:00", today, "23:59:59", tz)

        fdate = pd.to_datetime(from_date).date()
        tdate = pd.to_datetime(to_date).date()
        return make_iso_range(fdate, from_time, tdate, to_time, tz)

    @staticmethod
    def _perform_join_only(df_db: pd.DataFrame, df_api: pd.DataFrame,
                           db_key: str, api_key: str, how: str,
                           pre_db: str, pre_api: str) -> pd.DataFrame:
        def _safe_str_strip(series: pd.Series) -> pd.Series:
            try:
                return series.astype("string").str.strip()
            except Exception:
                return series
        if db_key not in df_db.columns:
            raise KeyError(f"DB-Key '{db_key}' nicht gefunden.")
        if api_key not in df_api.columns:
            raise KeyError(f"API-Key '{api_key}' nicht gefunden.")
        df1 = df_db.copy(); df2 = df_api.copy()
        df1[db_key] = _safe_str_strip(df1[db_key])
        df2[api_key] = _safe_str_strip(df2[api_key])
        df1 = df1.add_prefix(pre_db)
        df2 = df2.add_prefix(pre_api)
        return pd.merge(
            df1, df2,
            left_on=pre_db + db_key,
            right_on=pre_api + api_key,
            how=how,
        )
