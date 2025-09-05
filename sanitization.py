from __future__ import annotations

import re
from typing import Dict

"""
Redaction-Helfer:
- Entfernt/verschleiert Zugangsdaten und Tokens aus Strings (Fehlertexte, Logs, URLs).
- Nutze `redact(text)` für freie Texte.
- Optional: `redact_headers(headers)` um HTTP-Header sicher zu loggen.
"""

# --- robuste, nicht-gierige Muster für typische Secrets ---
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Authorization-Header / Tokens
    (re.compile(r"(?i)\bAuthorization\s*:\s*Bearer\s+[A-Za-z0-9\-._=]+"), "Authorization: Bearer ***REDACTED***"),
    (re.compile(r"(?i)\bAuthorization\s*:\s*Basic\s+[A-Za-z0-9+/=]+"),     "Authorization: Basic ***REDACTED***"),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9\-._=]+"),                      "Bearer ***REDACTED***"),

    # API-Key Header / Felder
    (re.compile(r"(?i)\b(x-api-key|api-key|apikey)\s*[:=]\s*([^\s;,&]+)"), r"\1: ***REDACTED***"),

    # Query- oder Body-Parameter
    (re.compile(r"(?i)([\?&])(?:access_)?token=([^&#\s]+)"),               r"\1token=***REDACTED***"),
    (re.compile(r"(?i)([\?&])sig=([^&#\s]+)"),                             r"\1sig=***REDACTED***"),
    (re.compile(r"(?i)([\?&])key=([^&#\s]+)"),                             r"\1key=***REDACTED***"),

    # Connection-Strings (ODBC / ADO.NET-Stil)
    (re.compile(r"(?i)\b(PWD|Password)\s*=\s*[^;]+"),                      r"\1=***REDACTED***"),

    # Simple JSON-Felder
    (re.compile(r'(?i)("?(password|pwd|secret|token|api[_-]?key)"?\s*:\s*)"[^"]+"'), r'\1"***REDACTED***"'),
]

_MAX_LEN = 4000  # harte Obergrenze zum Schutz der UI


def redact(text: str | bytes | None) -> str:
    """
    Entfernt/verschleiert sensible Inhalte aus `text`.
    - Eingabe kann None/bytes/str sein.
    - Ergebnis ist max. `_MAX_LEN` Zeichen lang (mit '…' am Ende, falls gekürzt).
    """
    if text is None:
        return ""
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8", errors="replace")
        except Exception:
            text = str(text)

    out = text
    for pattern, repl in _PATTERNS:
        out = pattern.sub(repl, out)

    # weiche Bereinigung mehrfacher Leerzeichen nach Ersetzungen
    out = re.sub(r"[ \t]{3,}", "  ", out)

    # harte Längenbegrenzung
    if len(out) > _MAX_LEN:
        out = out[:_MAX_LEN - 1] + "…"

    return out


def redact_headers(headers: Dict[str, str] | None) -> Dict[str, str]:
    """
    Gibt eine kopierte Header-Map zurück, in der sensible Header maskiert sind.
    Praktisch für Logging von Request/Response-Metadaten.
    """
    if not headers:
        return {}
    safe = dict(headers)
    # gängige Header abdecken
    for key in list(safe.keys()):
        k = key.lower()
        if k in ("authorization", "proxy-authorization", "x-api-key"):
            safe[key] = "***REDACTED***"
    return safe
