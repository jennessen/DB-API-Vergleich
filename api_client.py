from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import logging
from urllib.parse import quote

import requests
import pandas as pd

log = logging.getLogger(__name__)

# ============================================================================
# Konfiguration
# ============================================================================

@dataclass
class ApiConfig:
    base_url: str
    role: str
    resource: str
    alias: str
    auth: str
    use_updates: bool = False
    page_cap: int = 100
    timeout_s: int = 60
    select: str = ""  # optionales OData $select


# ============================================================================
# API-Client
# ============================================================================

class ApiClient:
    """
    HTTP-Client mit Session, Retries und JSON-Normalisierung.

    Header-Set immer fix:
        'Authorization': cfg.auth (z. B. 'FFN ey…' oder 'Bearer …')
        'x-application-id': 'JAPP0XQADEV'
        'x-application-version': '0.1'
        'Content-Type': 'application/json'
        'Alias': alias.upper()
    """

    def __init__(self) -> None:
        self.session = self._build_session()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def get_dataframe(
        self,
        cfg: ApiConfig,
        from_iso: str,
        to_iso: str,
        cancel,
        progress,
    ) -> pd.DataFrame:
        if cancel.is_set():
            progress.put("API: Abbruch vor Start.")
            return pd.DataFrame()

        base = (cfg.base_url or "").rstrip("/")
        if not base:
            raise ValueError("API base_url ist leer.")
        if not cfg.role:
            raise ValueError("API role ist leer.")
        if not cfg.resource:
            raise ValueError("API resource ist leer.")

        url = f"{base}/api/v1/{cfg.role}/{cfg.resource}"

        params: List[str] = []
        if cfg.use_updates:
            if not from_iso or not to_iso:
                raise ValueError("Updates-Endpunkt benötigt from/to ISO-Zeitstempel.")
            params.append(f"fromDate={self._q(from_iso, safe=':-T+Z')}")
            params.append(f"toDate={self._q(to_iso, safe=':-T+Z')}")
            url = f"{url}/updates"

        if cfg.select:
            select_clean = cfg.select.replace(" ", "")
            params.append(f"$select={self._q(select_clean, safe='$._,()=/')}")

        if params:
            url = f"{url}?{'&'.join(params)}"

        headers = self._headers(cfg)

        all_items: List[dict] = []
        for chunk in self._iter_pages(
            url=url,
            headers=headers,
            use_updates=cfg.use_updates,
            timeout_s=cfg.timeout_s,
            page_cap=cfg.page_cap,
            cancel=cancel,
            progress=progress,
        ):
            all_items.extend(chunk)

        if cancel.is_set() or not all_items:
            return pd.DataFrame()

        df = pd.json_normalize(all_items, sep=".")
        progress.put(f"API: {len(df):,} Zeilen empfangen.")
        return df

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    @staticmethod
    def _q(val: str, safe: str) -> str:
        return quote(val, safe=safe) if val else ""

    @staticmethod
    def _headers(cfg: ApiConfig) -> Dict[str, str]:
        """Setzt die festen Header + Authorization/Alias dynamisch."""
        h: Dict[str, str] = {
            "Authorization": cfg.auth,
            "x-application-id": "JAPP0XQADEV",
            "x-application-version": "0.1",
            "Content-Type": "application/json",
        }
        if cfg.alias:
            h["Alias"] = cfg.alias.upper().strip()
        return h

    def _iter_pages(
        self,
        url: str,
        headers: Dict[str, str],
        use_updates: bool,
        timeout_s: int,
        page_cap: int,
        cancel,
        progress,
    ) -> Iterable[List[dict]]:
        page = 0
        next_url: Optional[str] = url

        while next_url and (page < page_cap) and not cancel.is_set():
            progress.put(f"GET {next_url}")
            r = self.session.get(next_url, headers=headers, timeout=timeout_s)

            if r.status_code == 401:
                raise PermissionError("API 401: Unauthorized.")
            if r.status_code == 429:
                ra = r.headers.get("Retry-After", "")
                raise RuntimeError(f"API 429: Rate limit erreicht. Retry-After={ra!s}")
            if r.status_code >= 400:
                txt = (r.text or "")[:240]
                raise RuntimeError(f"API {r.status_code}: {txt}")

            try:
                payload = r.json()
            except Exception as e:
                raise RuntimeError(f"API: Ungültige JSON-Antwort: {e}") from e

            if use_updates:
                chunk = payload.get("data")
                if not isinstance(chunk, list):
                    chunk = []
                next_url = payload.get("nextChunkUrl")
            else:
                chunk = payload.get("items")
                if not isinstance(chunk, list):
                    chunk = []
                links = payload.get("_links", {}) or {}
                next_url = links.get("next")

            yield chunk
            page += 1

        if page >= page_cap:
            progress.put("API: Seitencap erreicht — abgebrochen, um Endlosschleifen zu vermeiden.")

    # ------------------------------------------------------------------ #
    # Session/Retry
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_session() -> requests.Session:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        sess = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)

        return sess
