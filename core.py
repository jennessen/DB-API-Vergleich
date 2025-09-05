import os
import threading
import pandas as pd
from typing import Optional, Tuple
from api_client import ApiClient, ApiConfig
from db_client import DbClient, SqlConfig
from time_utils import make_iso_range

class ComparisonCore:
    """Core logic for the DB⇄API comparison.

    This module contains all non-UI functionality so the UI can be replaced later
    (e.g. CLI, web) without touching the business logic.
    """

    def __init__(self, export_dir: str) -> None:
        """Initialize the core with base export directory used by validators/fixes."""
        self.export_dir = export_dir

    def build_time_range(self, from_date_str: str, from_time: str, to_date_str: str, to_time: str, timezone: str) -> Tuple[str, str]:
        """Build ISO time-range strings (from_iso, to_iso).

        - If placeholder like YYYY-MM-DD is left in inputs, use today's 00:00:00–23:59:59.
        - Otherwise parse dates and combine with provided times.
        """
        import datetime as _dt
        if from_date_str.lower().startswith("yyyy") or to_date_str.lower().startswith("yyyy"):
            today = _dt.date.today()
            return make_iso_range(today, "00:00:00", today, "23:59:59", timezone)
        fdate = pd.to_datetime(from_date_str).date()
        tdate = pd.to_datetime(to_date_str).date()
        return make_iso_range(fdate, from_time.strip(), tdate, to_time.strip(), timezone)

    def read_db(self, sql_cfg: SqlConfig, cancel: Optional[threading.Event], progress) -> pd.DataFrame:
        """Execute SELECT against the configured DB and return a DataFrame."""
        return DbClient().read_select(sql_cfg, cancel=cancel, progress=progress)

    def read_api(self, api_cfg: ApiConfig, from_iso: str, to_iso: str, cancel: Optional[threading.Event], progress) -> pd.DataFrame:
        """Fetch API data (optionally with updates window) and return a DataFrame."""
        return ApiClient().get_dataframe(api_cfg, from_iso, to_iso, cancel=cancel, progress=progress)

    @staticmethod
    def perform_join_only(df_db: pd.DataFrame, df_api: pd.DataFrame, db_key: str, api_key: str, how: str, pre_db: str, pre_api: str) -> pd.DataFrame:
        """Join DB/API DataFrames with key normalization and column prefixing; no export."""
        def _safe_str_strip(series: pd.Series) -> pd.Series:
            try:
                return series.astype("string").str.strip()
            except Exception:
                return series
        df1 = df_db.copy()
        df2 = df_api.copy()
        if db_key not in df1.columns:
            raise KeyError(f"DB-Key '{db_key}' nicht gefunden.")
        if api_key not in df2.columns:
            raise KeyError(f"API-Key '{api_key}' nicht gefunden.")
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

    def validate_if_possible(self, merged_df: pd.DataFrame, script_path: str, progress_q=None) -> Tuple[pd.DataFrame, bool]:
        """Optionally run the JS validator.

        Returns (df_out, ok_flag). If py-mini-racer or validator is unavailable or script_path is empty,
        returns the input DataFrame and ok_flag=False. If validator runs, returns validated DF and ok_flag=True.
        Any exception raised by JsValidator (e.g., unreadable script) is propagated to caller.
        """
        try:
            from validator import JsValidator  # optional
        except Exception:
            return merged_df, False
        if not script_path:
            return merged_df, False
        validator = JsValidator(script_path)
        merged_validated, _ = validator.run(
            merged_df,
            progress_q=progress_q,
            fix_dir=os.path.join(self.export_dir, "validator_fixes"),
        )
        return merged_validated, True
