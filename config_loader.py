from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict
import os
import json


# ============================================================================
# Datenklassen
# ============================================================================

@dataclass
class DbProfile:
    server: str = ""
    database: str = ""
    user: str = ""
    password: str = ""
    sql: str = ""
    max_rows: int | None = 250000


@dataclass
class ApiProfile:
    # Hinweis: base_key referenziert einen Eintrag in AppSettings.api_urls
    base_key: str = ""
    role: str = "merchant"
    resource: str = ""        # genau eine Ressource (erforderlich für API-Call)
    alias: str = ""
    auth: str = ""
    use_updates: bool = False
    page_cap: int = 100
    timeout_s: int = 60
    select: str = ""          # optionales OData $select (String)


@dataclass
class JoinProfile:
    db_key: str = "id"
    api_key: str = "id"
    how: str = "inner"        # inner | left | right | outer
    db_prefix: str = "db_"
    api_prefix: str = "api_"
    validator_script: str = ""   # Pfad zu rules.js (optional)
    validate_on_run: bool = False


@dataclass
class Profile:
    db: DbProfile = field(default_factory=DbProfile)
    api: ApiProfile = field(default_factory=ApiProfile)
    join: JoinProfile = field(default_factory=JoinProfile)
    timezone: str = "Europe/Berlin"


@dataclass
class AppSettings:
    api_urls: Dict[str, str] = field(default_factory=dict)
    profiles: Dict[str, Profile] = field(default_factory=dict)
    app_version: str = "0.2.0"
    export_dir: str = os.path.join(
        os.path.expanduser("~"), "Desktop", "DB_API_Compare_Exports"
    )
    logging_dir: str = os.path.join(
        os.path.expanduser("~"), "Desktop", "DB_API_Compare_Logs"
    )


# ============================================================================
# Laden der Konfiguration (ohne Altlasten)
# ============================================================================

def load_config(path: str) -> AppSettings:
    """
    Lädt die Konfiguration aus `config.json`.

    Erwartete Struktur (ohne Altlasten):
    {
      "api_urls": { "QA": "https://qa.example.com", "PROD": "https://api.example.com" },
      "MeinProfil": {
        "timezone": "Europe/Berlin",
        "db": {
          "server": "...",
          "database": "...",
          "user": "...",
          "password": "...",
          "sql": "SELECT ...",
          "max_rows": 250000
        },
        "api": {
          "base_key": "QA",
          "role": "merchant",
          "resource": "orders",           // String
          "alias": "ABC",
          "auth": "Bearer ...",
          "use_updates": false,
          "page_cap": 100,
          "timeout_s": 60,
          "select": "id,createdAt,status" // String (optional)
        },
        "join": {
          "db_key": "KundenNr",
          "api_key": "customerNumber",
          "how": "inner",
          "db_prefix": "db_",
          "api_prefix": "api_",
          "validator_script": "rules.js",
          "validate_on_run": true
        }
      }
    }
    """
    if not os.path.exists(path):
        return AppSettings()

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    api_urls = raw.get("api_urls", {})
    profiles: Dict[str, Profile] = {}

    # Alle top-level Keys außer "api_urls" werden als Profile interpretiert
    for name, node in ((k, v) for k, v in raw.items() if k != "api_urls"):
        db = node.get("db", {})
        api = node.get("api", {})
        join = node.get("join", {})

        # Join.how validieren
        how = (join.get("how") or "inner").lower()
        if how not in {"inner", "left", "right", "outer"}:
            how = "inner"

        prof = Profile(
            db=DbProfile(
                server=db.get("server", ""),
                database=db.get("database", ""),
                user=db.get("user", ""),
                password=db.get("password", ""),
                sql=db.get("sql", ""),
                max_rows=db.get("max_rows", 250000),
            ),
            api=ApiProfile(
                base_key=api.get("base_key", next(iter(api_urls), "")),
                role=api.get("role", "merchant"),
                resource=api.get("resource", ""),
                alias=api.get("alias", ""),
                auth=api.get("auth", ""),
                use_updates=api.get("use_updates", False),
                page_cap=api.get("page_cap", 100),
                timeout_s=api.get("timeout_s", 60),
                select=api.get("select", ""),
            ),
            join=JoinProfile(
                db_key=join.get("db_key", "id"),
                api_key=join.get("api_key", "id"),
                how=how,
                db_prefix=join.get("db_prefix", "db_"),
                api_prefix=join.get("api_prefix", "api_"),
                validator_script=join.get("validator_script", ""),
                validate_on_run=join.get("validate_on_run", False),
            ),
            timezone=node.get("timezone", "Europe/Berlin"),
        )

        profiles[name] = prof

    return AppSettings(api_urls=api_urls, profiles=profiles)
