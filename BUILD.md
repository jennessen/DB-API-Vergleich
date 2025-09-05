# Build-Anleitung (Windows)

Diese Anleitung beschreibt, wie Sie aus diesem Projekt eine ausführbare EXE erstellen, die die GUI aus `ui_single.py` startet.

Die PyInstaller-Konfiguration ist in `DB_API_Vergleich.spec` enthalten und wurde bereits so angepasst, dass `ui_single.py` als Einstiegspunkt verwendet wird und `config.json` neben die EXE gelegt wird.

## Voraussetzungen
- Windows
- Python 3.10/3.11 (64-bit empfohlen)
- PIP installiert

## Abhängigkeiten installieren
Öffnen Sie PowerShell im Projektordner (C:\VCS\DB API Vergleich) und führen Sie aus:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
# Falls noch nicht enthalten:
pip install pyinstaller ttkbootstrap tkcalendar
```

## EXE bauen (PyInstaller)

```powershell
# Im Projektordner ausführen
pyinstaller --clean --noconfirm DB_API_Vergleich.spec
```

Ergebnis:
- One-File-EXE unter: `dist\DB_API_Vergleich.exe`

Hinweis: Die `config.json` wird anhand der Spec-Datei automatisch neben die EXE kopiert, sodass die Anwendung diese laden kann.

## Starten der Anwendung
- Doppelklick auf `dist\DB_API_Vergleich.exe`

## Häufige Hinweise/Fehlerbehebung
- Fehlende Module: Installieren Sie sie in der aktiven Umgebung (`pip install <paket>`), und führen Sie den Build erneut aus.
- Tkinter/ttkbootstrap Themes: Falls Sie `ttkbootstrap` nicht nutzen möchten, wird automatisch auf Standard-Tkinter zurückgefallen.
- Antivirus/SmartScreen: Signieren Sie die EXE oder bestätigen Sie die Ausführung, falls Windows die Datei blockiert.
- Logs: Die Anwendung schreibt Logs in einen Ordner auf dem Desktop (siehe `logging_setup.py`).

## Optional: Anpassungen
- Konsolenfenster anzeigen: In der Spec-Datei `console=False` auf `True` ändern.
- Icon hinzufügen: In der Spec-Datei dem `EXE(...)`-Aufruf ein `icon='pfad\\zu\\icon.ico'` übergeben und neu bauen.
- Weitere Daten einbinden: In `DB_API_Vergleich.spec` neue Dateien zu `datas` hinzufügen: `datas.append(("C:\\pfad\\zu\\datei.ext", "."))`.
