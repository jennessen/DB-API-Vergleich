from __future__ import annotations

import os
import sys
import logging
from typing import Optional

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# Optional: ttkbootstrap für modernes Theme
USE_BOOTSTRAP = True
try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
except Exception:
    USE_BOOTSTRAP = False

import pandas as pd

# App-Module
from logging_setup import setup_logging
from config_loader import load_config, AppSettings, Profile
from preview import TreePreview

# Controller-Architektur
from core.controller import AppController
from core.ports import UiPort, UiInputs

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optionales Fenster für Validator-Logs (wenn der Controller es öffnen will)
# --------------------------------------------------------------------------- #

class ValidatorLogWindow(tk.Toplevel):
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


# --------------------------------------------------------------------------- #
# Verdrahtete Single-Screen UI (Adapter) – implementiert UiPort + UiInputs
# --------------------------------------------------------------------------- #

class SingleScreenApp(tk.Tk, UiPort, UiInputs):
    def __init__(self) -> None:
        super().__init__()
        # Theme
        if USE_BOOTSTRAP:
            self.style = tb.Style(theme="flatly")  # "flatly", "cosmo", "darkly", …
        self.title("DB ⇄ API Vergleich")
        self.geometry("1400x900")
        self.minsize(1180, 760)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Pfade (PyInstaller-kompatibel) + Config + Logging
        app_dir = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
        cfg_path = os.path.join(app_dir, "config.json")

        self.config_obj: AppSettings = load_config(cfg_path)
        setup_logging(self.config_obj.logging_dir)
        log.info("UI loaded", extra={"app_version": self.config_obj.app_version})

        # Controller
        self.controller = AppController(ui=self, inputs=self)

        # Validator-Logfenster (optional)
        self._val_window: Optional[ValidatorLogWindow] = None

        # UI bauen
        self._build_layout()
        self._apply_profile_defaults()

        # Progress-Pumpe starten (Controller → UI)
        self.controller.start_progress_pump(self.after)

        # Shortcuts
        self.bind("<F5>", lambda e: self.controller.run())
        self.bind("<Escape>", lambda e: self.controller.cancel())

    # ------------------------------------------------------------------ #
    # Layout
    # ------------------------------------------------------------------ #
    def _build_layout(self) -> None:
        # Header
        header = ttk.Frame(self)
        header.pack(fill=tk.X, padx=12, pady=(10, 8))
        header.columnconfigure(2, weight=1)

        ttk.Label(
            header, text="DB ⇄ API Vergleich", font=("TkDefaultFont", 13, "bold")
        ).grid(row=0, column=0, sticky="w")

        right = ttk.Frame(header); right.grid(row=0, column=2, sticky="e")

        ttk.Label(right, text="Profil").pack(side="left", padx=(0, 6))
        self.cmb_profile = ttk.Combobox(right, state="readonly", width=28)
        self.cmb_profile["values"] = list(self.config_obj.profiles.keys())
        if self.config_obj.profiles:
            self.cmb_profile.current(0)
        self.cmb_profile.pack(side="left")
        self.cmb_profile.bind("<<ComboboxSelected>>", lambda e: self._apply_profile_defaults())

        # Suchfeld (optional: globaler Tabellenfilter – reine UI)
        self.ent_search = ttk.Entry(right, width=26)
        self.ent_search.insert(0, "Suchen…")
        self.ent_search.pack(side="left", padx=(10, 8))

        # Aktionen
        self.btn_run = ttk.Button(right, text="▶ Ausführen", command=self.controller.run)
        self.btn_run.pack(side="left", padx=(4, 2))
        self.btn_cancel = ttk.Button(right, text="✖ Abbrechen", command=self.controller.cancel, state=tk.DISABLED)
        self.btn_cancel.pack(side="left", padx=2)
        self.btn_export = ttk.Button(right, text="⭳ Exportieren", command=self.controller.export, state=tk.DISABLED)
        self.btn_export.pack(side="left", padx=2)
        self.btn_reval = ttk.Button(right, text="♻︎ Erneut prüfen", command=self.controller.revalidate, state=tk.DISABLED)
        self.btn_reval.pack(side="left", padx=2)

        # Theme toggle
        self.btn_theme = ttk.Button(right, text="🌙", command=self._toggle_theme)
        self.btn_theme.pack(side="left", padx=(8, 0))

        # Body: Split in left (inputs) and right (preview+log)
        body = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))

        left = ttk.Frame(body)
        rightpane = ttk.Frame(body)
        body.add(left, weight=1)
        body.add(rightpane, weight=2)

        # LEFT: Inputs
        self._build_left_inputs(left)

        # RIGHT: Preview + Logs
        self._build_right_side(rightpane)

        # Statusbar
        status = ttk.Frame(self); status.pack(fill=tk.X, padx=12, pady=(0, 10))
        status.columnconfigure(1, weight=1)
        self.lbl_status = ttk.Label(status, text="Bereit."); self.lbl_status.grid(row=0, column=0, sticky="w")
        self.pb = ttk.Progressbar(status, mode="indeterminate"); self.pb.grid(row=0, column=1, sticky="ew", padx=12)

    # LEFT
    def _build_left_inputs(self, parent: tk.Misc) -> None:
        parent.columnconfigure(0, weight=1)

        # ------ DB ------
        db = self._card(parent, "Datenbank")
        db.grid(row=0, column=0, sticky="nsew", padx=4, pady=(0, 8))
        self.ent_server = self._labeled_entry(db, "Server", 0)
        self.ent_database = self._labeled_entry(db, "Datenbank", 1)
        self.ent_user = self._labeled_entry(db, "Benutzer", 2)
        self.ent_pwd = self._labeled_entry(db, "Passwort", 3, show="*")

        sql = ttk.Labelframe(db, text="SQL (nur SELECT)")
        sql.grid(row=4, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        sql.rowconfigure(0, weight=1); sql.columnconfigure(0, weight=1)
        self.txt_sql = ScrolledText(sql, height=8); self.txt_sql.grid(row=0, column=0, sticky="nsew")

        # ------ API ------
        api = self._card(parent, "API")
        api.grid(row=1, column=0, sticky="nsew", padx=4, pady=8)

        self.cmb_base = self._labeled_combo(api, "API Base URL", 0, list(self.config_obj.api_urls.keys()))
        self.cmb_role = self._labeled_combo(api, "Rolle", 1, ["merchant", "fulfiller"])
        self.ent_resource = self._labeled_entry(api, "Ressource", 2)
        self.ent_alias = self._labeled_entry(api, "Alias", 3)
        self.ent_auth = self._labeled_entry(api, "Auth (Bearer/FFN …)", 4)
        self.ent_select = self._labeled_entry(api, "OData $select (optional)", 5)
        self.ent_expand = self._labeled_entry(api, "OData $expand (optional)", 6)
        self.ent_filter = self._labeled_entry(api, "OData $filter (optional)", 7)

        row = 8
        ttk.Label(api, text="From").grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        self.ent_from_date = ttk.Entry(api, width=12); self.ent_from_date.insert(0, "YYYY-MM-DD")
        self.ent_from_date.grid(row=row, column=1, sticky=tk.W)
        self.ent_from_time = ttk.Entry(api, width=10); self.ent_from_time.insert(0, "00:00:00")
        self.ent_from_time.grid(row=row, column=2, sticky=tk.W, padx=4)

        row += 1
        ttk.Label(api, text="To").grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        self.ent_to_date = ttk.Entry(api, width=12); self.ent_to_date.insert(0, "YYYY-MM-DD")
        self.ent_to_date.grid(row=row, column=1, sticky=tk.W)
        self.ent_to_time = ttk.Entry(api, width=10); self.ent_to_time.insert(0, "23:59:59")
        self.ent_to_time.grid(row=row, column=2, sticky=tk.W, padx=4)

        self.var_updates = tk.BooleanVar(value=False)
        ttk.Checkbutton(api, text="Updates-Endpunkt verwenden", variable=self.var_updates)\
            .grid(row=row + 1, column=0, columnspan=3, sticky=tk.W, padx=6, pady=(6, 2))

        # ------ JOIN & VALIDATOR ------
        join = self._card(parent, "Join & Validator")
        join.grid(row=2, column=0, sticky="nsew", padx=4, pady=8)

        self.ent_dbkey = self._labeled_entry(join, "DB-Key", 0)
        self.ent_apikey = self._labeled_entry(join, "API-Key", 1)
        self.cmb_how = self._labeled_combo(join, "How", 2, ["inner", "left", "right", "outer"]); self.cmb_how.set("inner")
        self.ent_dbpref = self._labeled_entry(join, "DB-Präfix", 3); self.ent_dbpref.insert(0, "db_")
        self.ent_apipref = self._labeled_entry(join, "API-Präfix", 4); self.ent_apipref.insert(0, "api_")

        ttk.Label(join, text="Validator Script (JS)").grid(row=5, column=0, sticky=tk.W, padx=6, pady=4)
        rowf = ttk.Frame(join); rowf.grid(row=5, column=1, sticky="ew"); rowf.columnconfigure(0, weight=1)
        self.ent_validator = ttk.Entry(rowf); self.ent_validator.grid(row=0, column=0, sticky="ew")
        ttk.Button(rowf, text="…", command=self._browse_validator).grid(row=0, column=1, padx=6)

        self.var_validate = tk.BooleanVar(value=False)
        ttk.Checkbutton(join, text="Prüfung ausführen", variable=self.var_validate).grid(
            row=6, column=0, sticky=tk.W, padx=6, pady=(6, 2)
        )
        self.btn_revalidate = ttk.Button(join, text="Erneut prüfen", command=self.controller.revalidate, state=tk.DISABLED)
        self.btn_revalidate.grid(row=6, column=1, sticky="w", padx=6, pady=(6, 2))

    # RIGHT
    def _build_right_side(self, parent: tk.Misc) -> None:
        parent.rowconfigure(0, weight=3)
        parent.rowconfigure(1, weight=2)
        parent.columnconfigure(0, weight=1)

        # Preview (mit TreePreview)
        prev = self._card(parent, "Vorschau")
        prev.grid(row=0, column=0, sticky="nsew", padx=6, pady=(0, 8))
        prev.columnconfigure(0, weight=1)
        prev.rowconfigure(1, weight=1)

        pbar = ttk.Frame(prev); pbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.var_err_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(pbar, text="Nur Fehler", variable=self.var_err_only).pack(side="left")
        ttk.Label(pbar, text="Rows/Seite").pack(side="left", padx=(10, 6))
        self.cmb_page = ttk.Combobox(pbar, values=["200", "500", "1000", "2000"], width=6, state="readonly")
        self.cmb_page.set("1000"); self.cmb_page.pack(side="left")

        # Deine existierende Preview-Komponente
        self.preview = TreePreview(prev)
        self.preview.frame.grid(row=1, column=0, sticky="nsew")

        # Logs
        logs = self._card(parent, "Logs")
        logs.grid(row=1, column=0, sticky="nsew", padx=6, pady=(8, 0))
        logs.columnconfigure(0, weight=1); logs.rowconfigure(0, weight=1)
        self.txt_log = ScrolledText(logs, height=8, state=tk.NORMAL)
        self.txt_log.grid(row=0, column=0, sticky="nsew")

    # ------------------------------------------------------------------ #
    # Helpers (UI building)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _card(parent: tk.Misc, title: str) -> ttk.Labelframe:
        lf = ttk.Labelframe(parent, text=title)
        lf.grid_columnconfigure(1, weight=1)
        return lf

    def _labeled_entry(self, parent: tk.Misc, label: str, row: int, show: str | None = None, width: int = 34) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        ent = ttk.Entry(parent, width=width, show=show) if show else ttk.Entry(parent, width=width)
        ent.grid(row=row, column=1, sticky="ew", padx=6, pady=4)
        parent.grid_columnconfigure(1, weight=1)
        return ent

    def _labeled_combo(self, parent: tk.Misc, label: str, row: int, values: list[str]) -> ttk.Combobox:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        combo = ttk.Combobox(parent, values=values, state="readonly", width=32)
        if values: combo.current(0)
        combo.grid(row=row, column=1, sticky="ew", padx=6, pady=4)
        return combo

    def _browse_validator(self) -> None:
        path = filedialog.askopenfilename(
            title="Validator-Skript auswählen",
            filetypes=[("JavaScript", "*.js"), ("Alle Dateien", "*.*")],
        )
        if path:
            self.ent_validator.delete(0, tk.END)
            self.ent_validator.insert(0, path)

    def _toggle_theme(self) -> None:
        if not USE_BOOTSTRAP:
            self._append_log("Theme-Toggle benötigt ttkbootstrap.")
            return
        cur = self.style.theme.name
        new = "darkly" if cur != "darkly" else "flatly"
        self.style.theme_use(new)
        self.btn_theme.config(text="☀" if new == "darkly" else "🌙")

    # ------------------------------------------------------------------ #
    # UiPort (Controller → UI)
    # ------------------------------------------------------------------ #
    def set_running(self, running: bool) -> None:
        self.btn_cancel.config(state=tk.NORMAL if running else tk.DISABLED)
        self.btn_run.config(state=tk.DISABLED if running else tk.NORMAL)
        if running:
            self.pb.start(10); self.set_status("Läuft …")
        else:
            self.pb.stop(); self.set_status("Bereit.")

    def log(self, message: str) -> None:
        # Auch JS-Logs an Validator-Fenster spiegeln
        self.txt_log.insert(tk.END, message + "\n")
        self.txt_log.see(tk.END)
        if message.startswith("[JS] ") and self._val_window and self._val_window.winfo_exists():
            self._val_window.append(message[5:])

    def show_error(self, title: str, message: str) -> None:
        messagebox.showerror(title, message)

    def set_status(self, text: str) -> None:
        self.lbl_status.config(text=text)

    def open_validator_window(self) -> None:
        if self._val_window is None or not self._val_window.winfo_exists():
            self._val_window = ValidatorLogWindow(self)
            self._val_window.lift()
            try:
                self._val_window.focus_force()
            except Exception:
                pass

    def show_dataframe(self, df: pd.DataFrame) -> None:
        self.preview.show_dataframe(df)

    def enable_export(self, enabled: bool) -> None:
        self.btn_export.config(state=tk.NORMAL if enabled else tk.DISABLED)

    def enable_revalidate(self, enabled: bool) -> None:
        self.btn_reval.config(state=tk.NORMAL if enabled else tk.DISABLED)

    # ------------------------------------------------------------------ #
    # UiInputs (Controller liest Werte)
    # ------------------------------------------------------------------ #
    # DB
    def get_db_server(self) -> str: return self.ent_server.get()
    def get_db_database(self) -> str: return self.ent_database.get()
    def get_db_user(self) -> str: return self.ent_user.get()
    def get_db_password(self) -> str: return self.ent_pwd.get()
    def get_db_sql(self) -> str: return self.txt_sql.get("1.0", tk.END)

    # API
    def get_api_base_url(self) -> str:
        key = self.cmb_base.get()
        return (self.config_obj.api_urls or {}).get(key, "")
    def get_api_role(self) -> str: return self.cmb_role.get()
    def get_api_resource(self) -> str: return self.ent_resource.get()
    def get_api_alias(self) -> str: return self.ent_alias.get()
    def get_api_auth(self) -> str: return self.ent_auth.get()
    def get_api_use_updates(self) -> bool: return bool(self.var_updates.get())
    def get_api_select(self) -> str: return self.ent_select.get()
    def get_api_expand(self) -> str: return self.ent_expand.get()
    def get_api_filter(self) -> str: return self.ent_filter.get()
    def get_api_from_date(self) -> str: return self.ent_from_date.get()
    def get_api_from_time(self) -> str: return self.ent_from_time.get()
    def get_api_to_date(self) -> str: return self.ent_to_date.get()
    def get_api_to_time(self) -> str: return self.ent_to_time.get()

    # Join
    def get_join_db_key(self) -> str: return self.ent_dbkey.get()
    def get_join_api_key(self) -> str: return self.ent_apikey.get()
    def get_join_how(self) -> str: return self.cmb_how.get()
    def get_join_db_prefix(self) -> str: return self.ent_dbpref.get()
    def get_join_api_prefix(self) -> str: return self.ent_apipref.get()
    def get_validator_script_path(self) -> str: return self.ent_validator.get()
    def get_validate_on_run(self) -> bool: return bool(self.var_validate.get())

    # Sonstiges aus Profil
    def _current_profile(self) -> Optional[Profile]:
        name = self.cmb_profile.get() or next(iter(self.config_obj.profiles), "")
        return self.config_obj.profiles.get(name)

    def get_export_dir(self) -> str: return self.config_obj.export_dir
    def get_profile_timezone(self) -> str:
        prof = self._current_profile()
        return prof.timezone if prof else "Europe/Berlin"
    def get_profile_max_rows(self) -> int:
        prof = self._current_profile()
        return prof.db.max_rows if prof else 1_000_000
    def get_profile_page_cap(self) -> int:
        prof = self._current_profile()
        return prof.api.page_cap if prof else 100
    def get_profile_api_timeout(self) -> int:
        prof = self._current_profile()
        return prof.api.timeout_s if prof else 60

    # ------------------------------------------------------------------ #
    # Profil → UI Defaults
    # ------------------------------------------------------------------ #
    def _apply_profile_defaults(self) -> None:
        prof = self._current_profile()
        if not prof:
            return

        # DB
        self.ent_server.delete(0, tk.END); self.ent_server.insert(0, prof.db.server)
        self.ent_database.delete(0, tk.END); self.ent_database.insert(0, prof.db.database)
        self.ent_user.delete(0, tk.END); self.ent_user.insert(0, prof.db.user)
        self.ent_pwd.delete(0, tk.END); self.ent_pwd.insert(0, prof.db.password)
        self.txt_sql.delete("1.0", tk.END); self.txt_sql.insert("1.0", prof.db.sql)

        # API
        try:
            self.cmb_base.set(prof.api.base_key or next(iter(self.config_obj.api_urls)))
        except StopIteration:
            self.cmb_base.set("")
        self.cmb_role.set(prof.api.role)
        self.ent_resource.delete(0, tk.END); self.ent_resource.insert(0, prof.api.resource)
        self.ent_alias.delete(0, tk.END); self.ent_alias.insert(0, prof.api.alias)
        self.ent_auth.delete(0, tk.END); self.ent_auth.insert(0, prof.api.auth)
        self.ent_select.delete(0, tk.END); self.ent_select.insert(0, getattr(prof.api, "select", "") or "")
        # Neue OData-Parameter
        if hasattr(self, 'ent_expand'):
            self.ent_expand.delete(0, tk.END); self.ent_expand.insert(0, getattr(prof.api, "expand", "") or "")
        if hasattr(self, 'ent_filter'):
            self.ent_filter.delete(0, tk.END); self.ent_filter.insert(0, getattr(prof.api, "filter", "") or "")

        # Join
        self.ent_dbkey.delete(0, tk.END); self.ent_dbkey.insert(0, prof.join.db_key)
        self.ent_apikey.delete(0, tk.END); self.ent_apikey.insert(0, prof.join.api_key)
        how_val = prof.join.how if prof.join.how in ["inner", "left", "right", "outer"] else "inner"
        self.cmb_how.set(how_val)
        self.ent_dbpref.delete(0, tk.END); self.ent_dbpref.insert(0, prof.join.db_prefix)
        self.ent_apipref.delete(0, tk.END); self.ent_apipref.insert(0, prof.join.api_prefix)
        self.ent_validator.delete(0, tk.END); self.ent_validator.insert(0, getattr(prof.join, "validator_script", "") or "")
        self.var_validate.set(bool(getattr(prof.join, "validate_on_run", False)))

    # ------------------------------------------------------------------ #
    # Misc
    # ------------------------------------------------------------------ #
    def _append_log(self, text: str) -> None:
        self.txt_log.insert(tk.END, text + "\n")
        self.txt_log.see(tk.END)

    def on_close(self) -> None:
        self.controller.cancel()
        self.destroy()


if __name__ == "__main__":
    SingleScreenApp().mainloop()
