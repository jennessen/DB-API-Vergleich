from __future__ import annotations

import os
import sys
import logging
import threading
import queue
from typing import Optional

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, Toplevel
from tkinter.scrolledtext import ScrolledText

import pandas as pd

from config_loader import load_config, AppSettings, Profile
from logging_setup import setup_logging
from api_client import ApiClient, ApiConfig
from db_client import DbClient, SqlConfig
from join_export import join_and_export
from preview import TreePreview
from time_utils import make_iso_range
from sanitization import redact

# Optional: JS-Validator (py-mini-racer)
try:
    from validator import JsValidator
except Exception:
    JsValidator = None

log = logging.getLogger(__name__)


# ============================================================================
# UI: Fenster für JS-Validator-Logs
# ============================================================================

class ValidatorLogWindow(Toplevel):
    """Separates Fenster, in das console.log() Ausgaben aus dem JS-Validator laufen."""

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master)
        self.title("Validator-Logs")
        self.geometry("900x420")

        self.text = ScrolledText(self, state=tk.DISABLED)
        self.text.pack(fill=tk.BOTH, expand=True)

    def append(self, line: str) -> None:
        self.text.config(state=tk.NORMAL)
        self.text.insert(tk.END, line + "\n")
        self.text.see(tk.END)
        self.text.config(state=tk.DISABLED)


# ============================================================================
# Haupt-App
# ============================================================================

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self.title("DB ⇄ API Vergleich")
        self.geometry("1280x900")
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Pfade (PyInstaller-kompatibel)
        app_dir = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
        cfg_path = os.path.join(app_dir, "config.json")

        # Config + Logging
        self.config_obj: AppSettings = load_config(cfg_path)
        setup_logging(self.config_obj.logging_dir)
        log.info("App started", extra={"version": self.config_obj.app_version})

        # Threading/Progress
        self._task_thread: Optional[threading.Thread] = None
        self._reval_thread: Optional[threading.Thread] = None
        self._cancel_event = threading.Event()
        self._progress_q: "queue.Queue[str]" = queue.Queue()
        self._val_window: Optional[ValidatorLogWindow] = None

        # Letztes Ergebnis für manuellen Export / erneute Validierung
        self._last_df_db: Optional[pd.DataFrame] = None
        self._last_df_api: Optional[pd.DataFrame] = None
        self._last_df_merged: Optional[pd.DataFrame] = None

        # UI
        self._build_ui()
        self._apply_profile_defaults()

        # Progress-Drain starten
        self.after(150, self._drain_progress)

    # --------------------------------------------------------------------- #
    # UI-Aufbau
    # --------------------------------------------------------------------- #

    def _build_ui(self) -> None:
        """Erstellt die Tabs und Eingabeelemente der Anwendung."""

        # Profil-Zeile
        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=10, pady=6)

        ttk.Label(top, text="Profil").pack(side=tk.LEFT)

        self.cmb_profile = ttk.Combobox(
            top,
            values=list(self.config_obj.profiles.keys()),
            state="readonly",
            width=40,
        )
        if self.config_obj.profiles:
            self.cmb_profile.current(0)

        self.cmb_profile.pack(side=tk.LEFT, padx=6)
        self.cmb_profile.bind("<<ComboboxSelected>>", lambda e: self._apply_profile_defaults())

        # Tabs
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True)

        # --- DB Tab ---
        self.db_tab = ttk.Frame(nb)
        nb.add(self.db_tab, text="DB")

        self.ent_server = self._labeled_entry(self.db_tab, "Server", row=0)
        self.ent_database = self._labeled_entry(self.db_tab, "Datenbank", row=1)
        self.ent_user = self._labeled_entry(self.db_tab, "Benutzer", row=2)
        self.ent_pwd = self._labeled_entry(self.db_tab, "Passwort", row=3, show="*")

        sql_frame = ttk.LabelFrame(self.db_tab, text="SQL (nur SELECT)")
        sql_frame.grid(row=4, column=0, columnspan=3, sticky=tk.NSEW, padx=8, pady=8)

        self.txt_sql = ScrolledText(sql_frame, height=14)
        self.txt_sql.pack(fill=tk.BOTH, expand=True)

        self.db_tab.grid_rowconfigure(4, weight=1)
        self.db_tab.grid_columnconfigure(1, weight=1)

        # --- API Tab ---
        self.api_tab = ttk.Frame(nb)
        nb.add(self.api_tab, text="API")

        self.cmb_base = self._labeled_combo(
            self.api_tab, "API Base URL", row=0, values=list(self.config_obj.api_urls.keys())
        )
        self.cmb_role = self._labeled_combo(
            self.api_tab, "Rolle", row=1, values=["merchant", "fulfiller"]
        )

        self.ent_resource = self._labeled_entry(self.api_tab, "Ressource", row=2)
        self.ent_alias = self._labeled_entry(self.api_tab, "Alias", row=3)
        self.ent_auth = self._labeled_entry(self.api_tab, "Auth (Bearer/FFN …)", row=4)
        self.ent_select = self._labeled_entry(self.api_tab, "OData $select (optional)", row=5)

        ttk.Label(self.api_tab, text="From").grid(row=6, column=0, sticky=tk.W, padx=6, pady=4)
        self.ent_from_date = ttk.Entry(self.api_tab, width=12)
        self.ent_from_date.insert(0, "YYYY-MM-DD")
        self.ent_from_date.grid(row=6, column=1, sticky=tk.W)

        self.ent_from_time = ttk.Entry(self.api_tab, width=10)
        self.ent_from_time.insert(0, "00:00:00")
        self.ent_from_time.grid(row=6, column=2, sticky=tk.W, padx=4)

        ttk.Label(self.api_tab, text="To").grid(row=7, column=0, sticky=tk.W, padx=6, pady=4)
        self.ent_to_date = ttk.Entry(self.api_tab, width=12)
        self.ent_to_date.insert(0, "YYYY-MM-DD")
        self.ent_to_date.grid(row=7, column=1, sticky=tk.W)

        self.ent_to_time = ttk.Entry(self.api_tab, width=10)
        self.ent_to_time.insert(0, "23:59:59")
        self.ent_to_time.grid(row=7, column=2, sticky=tk.W, padx=4)

        self.var_updates = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            self.api_tab,
            text="Updates-Endpunkt verwenden",
            variable=self.var_updates,
        ).grid(row=8, column=0, columnspan=3, sticky=tk.W, padx=6)

        # --- Join Tab ---
        self.join_tab = ttk.Frame(nb)
        nb.add(self.join_tab, text="Join")

        self.ent_dbkey = self._labeled_entry(self.join_tab, "DB-Key", row=0, default="id")
        self.ent_apikey = self._labeled_entry(self.join_tab, "API-Key", row=1, default="id")

        self.cmb_how = self._labeled_combo(
            self.join_tab,
            "How",
            row=2,
            values=["inner", "left", "right", "outer"],
            current=0,
        )

        self.ent_dbpref = self._labeled_entry(self.join_tab, "DB-Präfix", row=3, default="db_")
        self.ent_apipref = self._labeled_entry(self.join_tab, "API-Präfix", row=4, default="api_")

        # Validator-Controls
        self.ent_validator = self._labeled_entry(self.join_tab, "Validator Script (JS)", row=5, default="")
        btn_browse = ttk.Button(self.join_tab, text="Durchsuchen…", command=self._browse_validator)
        btn_browse.grid(row=5, column=2, sticky=tk.W, padx=4)

        self.var_validate = tk.BooleanVar(value=False)
        chk = ttk.Checkbutton(
            self.join_tab,
            text="Prüfung ausführen",
            variable=self.var_validate,
        )
        chk.grid(row=6, column=0, columnspan=1, sticky=tk.W, padx=6, pady=4)

        # NEU: „Erneut prüfen“-Button (initial disabled)
        self.btn_revalidate = ttk.Button(self.join_tab, text="Erneut prüfen", command=self.on_revalidate, state=tk.DISABLED)
        self.btn_revalidate.grid(row=6, column=1, sticky=tk.W, padx=6, pady=4)

        # --- Toolbar ---
        bar = ttk.Frame(self)
        bar.pack(fill=tk.X, padx=10, pady=8)

        self.btn_run = ttk.Button(bar, text="Ausführen", command=self.on_run)
        self.btn_run.pack(side=tk.LEFT)

        self.btn_cancel = ttk.Button(bar, text="Abbrechen", command=self.on_cancel, state=tk.DISABLED)
        self.btn_cancel.pack(side=tk.LEFT, padx=6)

        # Export-Button (initial disabled)
        self.btn_export = ttk.Button(bar, text="Exportieren", command=self.on_export, state=tk.DISABLED)
        self.btn_export.pack(side=tk.LEFT, padx=6)

        self.lbl_status = ttk.Label(bar, text="Bereit.")
        self.lbl_status.pack(side=tk.LEFT, padx=10)

        self.pb = ttk.Progressbar(bar, mode="indeterminate")
        self.pb.pack(fill=tk.X, expand=True, padx=8)

        # Preview + Log
        self.preview = TreePreview(self)
        self.preview.frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)

        self.txt_log = ScrolledText(self, height=8, state=tk.DISABLED)
        self.txt_log.pack(fill=tk.BOTH, expand=False, padx=10, pady=6)

    # --------------------------------------------------------------------- #
    # UI-Helfer
    # --------------------------------------------------------------------- #

    def _labeled_entry(
        self,
        parent: tk.Misc,
        label: str,
        row: int,
        default: str = "",
        show: Optional[str] = None,
        width: int = 40,
    ) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        entry = ttk.Entry(parent, show=show, width=width) if show else ttk.Entry(parent, width=width)
        entry.grid(row=row, column=1, sticky=tk.W, padx=6, pady=4)

        if default:
            entry.insert(0, default)

        parent.grid_columnconfigure(1, weight=1)
        return entry

    def _labeled_combo(
        self,
        parent: tk.Misc,
        label: str,
        row: int,
        values: list[str],
        current: int = 0,
    ) -> ttk.Combobox:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)

        combo = ttk.Combobox(parent, values=values, state="readonly", width=40)
        if values:
            combo.current(current)
        combo.grid(row=row, column=1, sticky=tk.W, padx=6, pady=4)

        return combo

    def _browse_validator(self) -> None:
        path = filedialog.askopenfilename(
            title="Validator-Skript auswählen",
            filetypes=[("JavaScript", "*.js"), ("Alle Dateien", "*.*")],
        )
        if path:
            self.ent_validator.delete(0, tk.END)
            self.ent_validator.insert(0, path)

    # Thread-safe UI helpers
    def _open_validator_window(self):
        if self._val_window is None or not self._val_window.winfo_exists():
            self._val_window = ValidatorLogWindow(self)
            self._val_window.lift()
            try:
                self._val_window.focus_force()
            except Exception:
                pass

    def _show_error(self, title: str, msg: str):
        messagebox.showerror(title, msg)

    # --------------------------------------------------------------------- #
    # Profile → UI Defaults
    # --------------------------------------------------------------------- #

    def _apply_profile_defaults(self) -> None:
        """Übernimmt aus dem ausgewählten Profil die Defaults in die UI."""
        if self.cmb_profile.get():
            name = self.cmb_profile.get()
        else:
            name = next(iter(self.config_obj.profiles), "")

        prof: Optional[Profile] = self.config_obj.profiles.get(name)
        if not prof:
            return

        # DB
        self.ent_server.delete(0, tk.END)
        self.ent_server.insert(0, prof.db.server)

        self.ent_database.delete(0, tk.END)
        self.ent_database.insert(0, prof.db.database)

        self.ent_user.delete(0, tk.END)
        self.ent_user.insert(0, prof.db.user)

        self.ent_pwd.delete(0, tk.END)
        self.ent_pwd.insert(0, prof.db.password)

        self.txt_sql.delete("1.0", tk.END)
        self.txt_sql.insert("1.0", prof.db.sql)

        # API
        try:
            self.cmb_base.set(prof.api.base_key or next(iter(self.config_obj.api_urls)))
        except StopIteration:
            self.cmb_base.set("")

        self.cmb_role.set(prof.api.role)

        self.ent_resource.delete(0, tk.END)
        self.ent_resource.insert(0, prof.api.resource)

        self.ent_alias.delete(0, tk.END)
        self.ent_alias.insert(0, prof.api.alias)

        self.ent_auth.delete(0, tk.END)
        self.ent_auth.insert(0, prof.api.auth)

        self.ent_select.delete(0, tk.END)
        self.ent_select.insert(0, getattr(prof.api, "select", "") or "")

        self.var_updates.set(bool(getattr(prof.api, "use_updates", False)))

        # Join
        self.ent_dbkey.delete(0, tk.END)
        self.ent_dbkey.insert(0, prof.join.db_key)

        self.ent_apikey.delete(0, tk.END)
        self.ent_apikey.insert(0, prof.join.api_key)

        how_val = prof.join.how if prof.join.how in ["inner", "left", "right", "outer"] else "inner"
        self.cmb_how.set(how_val)

        self.ent_dbpref.delete(0, tk.END)
        self.ent_dbpref.insert(0, prof.join.db_prefix)

        self.ent_apipref.delete(0, tk.END)
        self.ent_apipref.insert(0, prof.join.api_prefix)

        self.ent_validator.delete(0, tk.END)
        self.ent_validator.insert(0, getattr(prof.join, "validator_script", "") or "")

        self.var_validate.set(bool(getattr(prof.join, "validate_on_run", False)))

    # --------------------------------------------------------------------- #
    # Thread-Steuerung
    # --------------------------------------------------------------------- #

    def on_run(self) -> None:
        if self._task_thread and self._task_thread.is_alive():
            return

        # Reset Buttons/Ergebnisse
        self.btn_export.configure(state=tk.DISABLED)
        self.btn_revalidate.configure(state=tk.DISABLED)
        self._last_df_db = None
        self._last_df_api = None
        self._last_df_merged = None

        self._cancel_event.clear()
        self._set_running(True)

        self._task_thread = threading.Thread(target=self._worker, daemon=True)
        self._task_thread.start()

    def on_cancel(self) -> None:
        self._cancel_event.set()
        self._progress_q.put("Abbruch angefordert …")

    def _set_running(self, running: bool) -> None:
        self.btn_run.config(state=tk.DISABLED if running else tk.NORMAL)
        self.btn_cancel.config(state=tk.NORMAL if running else tk.DISABLED)

        if running:
            self.pb.start(10)
            self.lbl_status.config(text="Läuft …")
        else:
            self.pb.stop()
            self.lbl_status.config(text="Bereit.")

    # --------------------------------------------------------------------- #
    # Export-Handler (manuell)
    # --------------------------------------------------------------------- #

    def on_export(self) -> None:
        """Exportiert die zuletzt erzeugten DataFrames (DB, API, Merged) nach CSV/XLSX."""
        if self._last_df_db is None or self._last_df_api is None or self._last_df_merged is None:
            messagebox.showwarning("Export", "Kein Ergebnis zum Exportieren vorhanden.")
            return

        try:
            merged_out, paths = join_and_export(
                df_db=self._last_df_db,
                df_api=self._last_df_api,
                db_key=self.ent_dbkey.get().strip(),
                api_key=self.ent_apikey.get().strip(),
                how=self.cmb_how.get(),
                pre_db=self.ent_dbpref.get().strip() or "db_",
                pre_api=self.ent_apipref.get().strip() or "api_",
                base_dir=self.config_obj.export_dir,
                merged_override=self._last_df_merged,  # validierte/aktuelle Merged-Version
            )
            self._last_df_merged = merged_out
            self._progress_q.put(f"Export abgeschlossen: {paths.get('folder', '')}")
            messagebox.showinfo("Export", f"Export unter: {paths.get('folder', '')}")
        except Exception as ex:
            msg = redact(str(ex))
            log.exception("Export failed")
            messagebox.showerror("Export-Fehler", msg)

    # --------------------------------------------------------------------- #
    # Re-Validation (nur vorhandene Daten)
    # --------------------------------------------------------------------- #

    def on_revalidate(self) -> None:
        """Startet die erneute Ausführung des Validator-Skripts auf den vorhandenen Daten (asynchron)."""
        if self._reval_thread and self._reval_thread.is_alive():
            return

        if self._last_df_merged is None:
            messagebox.showwarning("Prüfung", "Es liegen noch keine Daten vor.")
            return

        script_path = self.ent_validator.get().strip()
        if not script_path:
            messagebox.showwarning("Prüfung", "Kein Validator-Skript angegeben.")
            return
        if JsValidator is None:
            messagebox.showwarning("Prüfung", "Validator nicht verfügbar (py-mini-racer fehlt).")
            return

        # UI kurz in „laufend“-Zustand versetzen
        self.btn_revalidate.configure(state=tk.DISABLED)
        self.pb.start(10)
        self.lbl_status.config(text="Prüfung läuft …")

        self._reval_thread = threading.Thread(target=self._revalidate_worker, daemon=True)
        self._reval_thread.start()

    def _revalidate_worker(self) -> None:
        """Worker für erneute Validierung ohne neue DB/API-Calls."""
        try:
            # Basis-DF: vorhandenes Merged ohne alte validation_* Spalten
            base = self._last_df_merged.copy()
            drop_cols = [c for c in base.columns if c.startswith("validation_")]
            if drop_cols:
                base = base.drop(columns=drop_cols, errors="ignore")

            self._progress_q.put("Erneute Prüfung: starte Validator …")
            # Thread-safe das Fenster öffnen
            self.after(0, self._open_validator_window)

            validator = JsValidator(self.ent_validator.get().strip())

            merged_validated, _ = validator.run(
                base,
                progress_q=self._progress_q,
                fix_dir=os.path.join(self.config_obj.export_dir, "validator_fixes"),
            )

            # Ergebnis übernehmen und Vorschau aktualisieren
            self._last_df_merged = merged_validated
            self.preview.show_dataframe(merged_validated)
            self._progress_q.put("Erneute Prüfung abgeschlossen – Vorschau aktualisiert.")

        except Exception as ex:
            msg = redact(str(ex))
            log.exception("Revalidate failed")
            self.after(0, lambda: self._show_error("Prüfung fehlgeschlagen", msg))

        finally:
            # UI zurücksetzen (Export weiter allowed)
            self.pb.stop()
            self.lbl_status.config(text="Bereit.")
            self.btn_revalidate.configure(state=tk.NORMAL)

    # --------------------------------------------------------------------- #
    # Worker (führt den gesamten Lauf aus – ohne Export)
    # --------------------------------------------------------------------- #

    def _worker(self) -> None:
        try:
            # Profil
            prof_key = self.cmb_profile.get()
            profile = self.config_obj.profiles[prof_key]

            # DB-Config
            sql_cfg = SqlConfig(
                server=self.ent_server.get().strip(),
                database=self.ent_database.get().strip(),
                user=self.ent_user.get().strip(),
                password=self.ent_pwd.get().strip(),
                sql=self.txt_sql.get("1.0", tk.END),
                max_rows=profile.db.max_rows,
            )

            # API-Config
            api_cfg = ApiConfig(
                base_url=self.config_obj.api_urls.get(self.cmb_base.get(), "").strip(),
                role=self.cmb_role.get().strip(),
                resource=self.ent_resource.get().strip(),
                alias=self.ent_alias.get().strip(),
                auth=self.ent_auth.get().strip(),
                use_updates=self.var_updates.get(),
                page_cap=profile.api.page_cap,
                timeout_s=profile.api.timeout_s,
                select=(self.ent_select.get() or "").strip(),
            )

            # Zeitfenster nur relevant, wenn Updates aktiv sind
            from_iso = ""
            to_iso = ""
            if api_cfg.use_updates:
                try:
                    from_date_str = self.ent_from_date.get().strip()
                    to_date_str = self.ent_to_date.get().strip()

                    # Falls Platzhalter stehen gelassen wurden → heutigen Tag 00:00–23:59:59
                    if from_date_str.lower().startswith("yyyy") or to_date_str.lower().startswith("yyyy"):
                        import datetime as _dt
                        today = _dt.date.today()
                        from_iso, to_iso = make_iso_range(
                            today, "00:00:00", today, "23:59:59", profile.timezone
                        )
                    else:
                        fdate = pd.to_datetime(from_date_str).date()
                        tdate = pd.to_datetime(to_date_str).date()
                        from_iso, to_iso = make_iso_range(
                            fdate,
                            self.ent_from_time.get().strip(),
                            tdate,
                            self.ent_to_time.get().strip(),
                            profile.timezone,
                        )
                except Exception as ex:
                    raise ValueError(f"Ungültiger Zeitraum/Zeiteinstellungen: {ex}")

            # --- DB lesen ---
            self._progress_q.put("Lese DB …")
            df_db = DbClient().read_select(
                sql_cfg,
                cancel=self._cancel_event,
                progress=self._progress_q,
            )
            if self._cancel_event.is_set():
                return

            # --- API lesen ---
            self._progress_q.put("Rufe API auf …")
            api = ApiClient()
            df_api = api.get_dataframe(
                api_cfg,
                from_iso,
                to_iso,
                cancel=self._cancel_event,
                progress=self._progress_q,
            )
            if self._cancel_event.is_set():
                return

            # --- Join (ohne Export) ---
            self._progress_q.put("Join …")
            merged_only = self._perform_join_only(
                df_db=df_db,
                df_api=df_api,
                db_key=self.ent_dbkey.get().strip(),
                api_key=self.ent_apikey.get().strip(),
                how=self.cmb_how.get(),
                pre_db=self.ent_dbpref.get().strip() or "db_",
                pre_api=self.ent_apipref.get().strip() or "api_",
            )

            # --- Optional: Validierung ---
            merged = merged_only
            if self.var_validate.get() and self.ent_validator.get().strip():
                if JsValidator is None:
                    self._progress_q.put("Validator nicht verfügbar: py-mini-racer nicht installiert")
                else:
                    try:
                        self._progress_q.put("Validator initialisieren …")
                        # Thread-safe Fenster öffnen
                        self.after(0, self._open_validator_window)

                        script_path = self.ent_validator.get().strip()
                        self._progress_q.put(f"Validator: {script_path}")

                        validator = JsValidator(script_path)

                        self._progress_q.put("Validiere Zeilen …")
                        merged_validated, _ = validator.run(
                            merged_only,
                            progress_q=self._progress_q,
                            fix_dir=os.path.join(self.config_obj.export_dir, "validator_fixes"),
                        )
                        merged = merged_validated
                    except Exception as ex:
                        self._progress_q.put(f"Validator-Fehler: {ex}")

            if self._cancel_event.is_set():
                return

            # Vorschau aktualisieren
            self.preview.show_dataframe(merged)
            self._progress_q.put("Join abgeschlossen – Vorschau aktualisiert.")

            # Ergebnisse für manuellen Export merken
            self._last_df_db = df_db
            self._last_df_api = df_api
            self._last_df_merged = merged

            # Export- und Revalidate-Button freigeben
            self.btn_export.configure(state=tk.NORMAL)
            self.btn_revalidate.configure(state=tk.NORMAL)

        except Exception as ex:
            msg = redact(str(ex))
            log.exception("Run failed")
            # Thread-safe Messagebox
            self.after(0, lambda: self._show_error("Fehler", msg))

        finally:
            self._set_running(False)

    # --------------------------------------------------------------------- #
    # Join-only Helper
    # --------------------------------------------------------------------- #

    @staticmethod
    def _perform_join_only(
        df_db: pd.DataFrame,
        df_api: pd.DataFrame,
        db_key: str,
        api_key: str,
        how: str,
        pre_db: str,
        pre_api: str,
    ) -> pd.DataFrame:
        """Erzeugt den gejointen DataFrame (ohne Export), inklusive Prefixing und Key-Pflege."""

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

    # --------------------------------------------------------------------- #
    # Progress/Log
    # --------------------------------------------------------------------- #

    def _drain_progress(self) -> None:
        """Leert die Progress-Queue regelmäßig in das Log und spiegelt JS-Logs in das Fenster."""
        try:
            while True:
                msg = self._progress_q.get_nowait()

                self._append_log(msg)

                # JS-Logs zusätzlich live ins Validator-Fenster
                if msg.startswith("[JS] "):
                    if self._val_window is not None:
                        try:
                            if self._val_window.winfo_exists():
                                self._val_window.append(msg[5:])
                            else:
                                self._val_window = None
                        except Exception:
                            self._val_window = None

        except queue.Empty:
            pass

        # regelmäßig weiter pollen
        self.after(200, self._drain_progress)

    def _append_log(self, text: str) -> None:
        self.txt_log.config(state=tk.NORMAL)
        self.txt_log.insert(tk.END, text + "\n")
        self.txt_log.see(tk.END)
        self.txt_log.config(state=tk.DISABLED)

    # --------------------------------------------------------------------- #
    # Close
    # --------------------------------------------------------------------- #

    def on_close(self) -> None:
        if self._task_thread and self._task_thread.is_alive():
            if not messagebox.askyesno("Beenden", "Ein Lauf ist aktiv. Wirklich beenden?"):
                return
        self.destroy()


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    App().mainloop()
