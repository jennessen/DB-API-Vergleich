from __future__ import annotations

from datetime import date, datetime, time
from typing import Tuple

import pytz


# ============================================================================
# Hilfsfunktionen für Zeit/Datum
# ============================================================================

def make_iso_range(
    from_date: date,
    from_time: str,
    to_date: date,
    to_time: str,
    tz_name: str = "Europe/Berlin",
) -> Tuple[str, str]:
    """
    Erzeugt zwei ISO-8601 Zeitstempel (tz-aware, UTC-normalisiert) aus Datum + Zeit + Zeitzone.

    Parameters
    ----------
    from_date : datetime.date
        Start-Datum
    from_time : str
        Start-Zeit im Format HH:MM[:SS]
    to_date : datetime.date
        End-Datum
    to_time : str
        End-Zeit im Format HH:MM[:SS]
    tz_name : str
        Name der Zeitzone (Default: Europe/Berlin)

    Returns
    -------
    (from_iso, to_iso) : Tuple[str, str]
        Zwei ISO-8601 Strings in UTC, z. B. '2025-08-28T00:00:00Z'
    """
    tz = pytz.timezone(tz_name)

    fparts = [int(p) for p in from_time.split(":")]
    tparts = [int(p) for p in to_time.split(":")]

    ftime = time(*fparts)
    ttime = time(*tparts)

    # naive Datetimes zusammensetzen
    dt_from_naive = datetime.combine(from_date, ftime)
    dt_to_naive = datetime.combine(to_date, ttime)

    # In lokaler TZ -> UTC
    dt_from = tz.localize(dt_from_naive).astimezone(pytz.UTC)
    dt_to = tz.localize(dt_to_naive).astimezone(pytz.UTC)

    return dt_from.isoformat().replace("+00:00", "Z"), dt_to.isoformat().replace("+00:00", "Z")


def now_iso_utc() -> str:
    """
    Aktuellen Zeitpunkt als ISO-8601 UTC-String (mit 'Z') zurückgeben.
    """
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def parse_iso(s: str) -> datetime:
    """
    ISO-8601 String in tz-aware datetime (UTC) parsen.
    - Akzeptiert 'Z' als UTC.
    - Liefert datetime mit tzinfo=UTC.
    """
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(pytz.UTC)
