# Repository Guidelines

## Project Structure & Module Organization
- `app.py`: Tkinter GUI entrypoint (DB ↔ API comparison).
- `api_client.py`, `db_client.py`: HTTP and database access layers.
- `join_export.py`, `preview.py`, `time_utils.py`, `sanitization.py`: merge/export, UI preview, helpers.
- `config_loader.py`, `config.json`: configuration schema and runtime settings.
- `logging_setup.py`: centralized logging to console and daily files.
- `build/`, `dist/`: PyInstaller artifacts; `.venv/` for local env; `DB_API_Vergleich.spec` build spec.

## Build, Test, and Development Commands
- Create venv (Windows): `python -m venv .venv && .venv\Scripts\activate`
- Install deps: `pip install -r requirements.txt`
- Run app (GUI): `python app.py`
- Build executable: `pyinstaller DB_API_Vergleich.spec` (outputs to `dist/`).
- Clean build artifacts: remove `build/` and `dist/` directories as needed.

## Coding Style & Naming Conventions
- Python 3.10+; 4-space indentation; type hints required (dataclasses used).
- Names: modules/functions `snake_case`; classes `PascalCase`; constants `UPPER_SNAKE_CASE`.
- Logging: use `logging.getLogger(__name__)` after `setup_logging(...)` is called in `app.py`.
- Recommended (optional): format with Black and isort; lint with Flake8.
  - Example: `pip install black isort flake8 && black . && isort . && flake8`.

## Testing Guidelines
- Preferred: `pytest` with unit tests under `tests/` named `test_*.py`.
- Mock external systems (HTTP, ODBC) and provide sample profiles for deterministic tests.
- Run tests (if added): `pytest -q`.
- Aim for coverage on data transforms (`join_export.py`), config parsing, and sanitization.

## Commit & Pull Request Guidelines
- Commits: follow Conventional Commits (e.g., `feat:`, `fix:`, `chore:`). Keep messages imperative and scoped.
- PRs: include description, linked issues, and screenshots/GIFs for UI changes.
- Add notes on configuration changes (sample `config.json` snippet) and any migration steps.

## Security & Configuration Tips
- Do not commit real secrets in `config.json` (passwords, tokens). Use placeholders locally.
- Config is loaded via `config_loader.load_config('config.json')` next to the binary/script.
- Exports/logs default to Desktop (`export_dir`, `logging_dir`); override via `config.json` if needed.



## Update Prompt 1
````
Du bist ein Refactoring-Agent. Nimm die folgenden Änderungen an einem bestehenden Python-Projekt vor. Arbeite deterministisch, mache nur die beschriebenen Änderungen, erhalte bestehende Signaturen soweit möglich aufrecht und lass alle anderen Dateien unangetastet.

### Ziel
In `validator.py` werden aktuell die Inhalte der Fix-Skripte (`wawi_fix_script` und `api_fix_script`) via `progress_q.put(...)` vollständig in das Log geschrieben. Das erzeugt massiven Log-Spam. Stattdessen sollen die (optional vorhandenen) Skript-Strings in Dateien geschrieben werden. In die Log-Queue kommt nur noch ein kurzer Hinweis mit dem Pfad. Wenn kein Zielverzeichnis übergeben wird, keine Datei schreiben und nur eine kurze Info loggen – ohne den Script-Inhalt.

### Änderungen (validator.py)
1. Funktionssignatur erweitern  
   In Klasse `JsValidator`, Methode `run(...)`, Signatur erweitern von  
   ```python
   def run(self, df_joined: pd.DataFrame, progress_q=None):
````

auf

```python
def run(self, df_joined: pd.DataFrame, progress_q=None, fix_dir: str | None = None):
```

2. Helper zum Schreiben der Skripte
   Direkt innerhalb von `run` (oder als private Instanzmethode) hinzufügen:

   ```python
   from pathlib import Path
   import re
   def _write_fix_file(kind: str, content: str, directory: str) -> str:
       base = Path(directory)
       base.mkdir(parents=True, exist_ok=True)
       from datetime import datetime
       ts = datetime.now().strftime("%Y%m%d_%H%M%S")
       fname = f"{kind}_fix_{ts}.js"
       fname = re.sub(r'[^A-Za-z0-9_\\.-]', '_', fname)
       path = base / fname
       path.write_text(content, encoding="utf-8")
       return str(path)
   ```

3. Verhalten beim Vorliegen von Fix-Skripten
   Ersetze die bisherigen Log-Zeilen:

   ```python
   if wawi_fix_script is not None:
       if progress_q is not None:
           progress_q.put(f"Wawi Fix")
           progress_q.put(wawi_fix_script)
   if api_fix_script is not None:
       if progress_q is not None:
           progress_q.put(f"API Fix")
           progress_q.put(api_fix_script)
   ```

   durch:

   ```python
   if wawi_fix_script is not None:
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
               progress_q.put("[JS] Wawi-Fix vorhanden (kein fix_dir angegeben) – nicht gespeichert.")

   if api_fix_script is not None:
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
               progress_q.put("[JS] API-Fix vorhanden (kein fix_dir angegeben) – nicht gespeichert.")
   ```

4. Return-Wert beibehalten
   Weiterhin `return out, logs_total`. Keine neuen Rückgabe-Elemente einführen.

5. Imports
   Falls `re`, `Path`, `datetime` noch nicht importiert sind, sauber ergänzen.

### Optionale Anpassung (app.py)

An der Stelle, wo `validator.run(...)` aufgerufen wird, ein `fix_dir` übergeben:

```python
merged_validated, _ = validator.run(
    merged_only,
    progress_q=self._progress_q,
    fix_dir=os.path.join(self.config_obj.export_dir, "validator_fixes"),
)
```

### Akzeptanzkriterien

* Keine langen Script-Strings mehr in der Log-Queue.
* Wenn `fix_dir` gesetzt: `.js`-Dateien mit Timestamp im Namen, Hinweis mit Pfad im Log.
* Wenn `fix_dir` nicht gesetzt: nur kurzer Hinweis im Log, kein Inhalt.
* Rückgabewert unverändert.
* Code PEP8-konform, stabil auf Windows.
