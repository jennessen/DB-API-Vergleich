from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Sequence, List, Any

import tkinter as tk
from tkinter import ttk

import pandas as pd


# ============================================================================
# Utils
# ============================================================================

def _to_cell_str(v: Any) -> str:
    """Robuste Zellformatierung für Anzeigezwecke."""
    # Bytes -> hex
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    # Pandas NA -> leer
    if v is pd.NA:  # type: ignore
        return ""
    # Datetime -> ISO (pandas/numpy kompatibel)
    try:
        if pd.api.types.is_datetime64_any_dtype(type(v)) or isinstance(v, (pd.Timestamp, )):
            # Fallback: str() falls isoformat nicht vorhanden
            try:
                return v.isoformat()
            except Exception:
                return str(v)
    except Exception:
        pass
    # Alles andere als String
    return "" if v is None else str(v)


def _autofit_widths(columns: Sequence[str], df: pd.DataFrame, sample_rows: int = 200, min_w: int = 6, max_w: int = 60) -> List[int]:
    """Schätzt sinnvolle Spaltenbreiten anhand Header und Stichprobe der Daten."""
    widths = [max(min_w, min(max_w, len(str(c)) + 2)) for c in columns]
    if df is None or df.empty:
        return widths

    sample = df.head(sample_rows)
    for i, col in enumerate(columns):
        if col not in sample.columns:
            continue
        for val in sample[col]:
            w = len(_to_cell_str(val)) + 2
            if w > widths[i]:
                widths[i] = min(max_w, w)
    return widths


# ============================================================================
# Public Widget
# ============================================================================

@dataclass
class PreviewSettings:
    page_size: int = 1000
    zebra: bool = True


class TreePreview:
    """
    Treeview-basierte DataFrame-Vorschau mit Paging.
    Nutzung:
        pv = TreePreview(parent)
        pv.frame.pack(...)
        pv.show_dataframe(df)

    Zusätzlich unterstützt:
    - Spaltenfilter (UI in der Toolbar)
    - Globaler Filter (über set_global_filter), durchsucht alle Spalten
    - "Nur Fehler"-Ansicht, wenn Spalte 'validation_ok' vorhanden ist
    """

    def __init__(self, parent: tk.Misc, settings: Optional[PreviewSettings] = None) -> None:
        self.parent = parent
        self.settings = settings or PreviewSettings()

        # State
        self._df: Optional[pd.DataFrame] = None
        self._df_view: Optional[pd.DataFrame] = None  # evtl. gefilterte Sicht
        self._columns: List[str] = []
        self._page: int = 0
        self._pages: int = 0
        self._filter_col: Optional[str] = None
        self._filter_text: str = ""
        self._global_filter_text: str = ""
        self._only_errors: bool = False  # nutzt Spalte 'validation_ok' falls vorhanden

        # UI
        self.frame = ttk.Frame(parent)

        # Toolbar
        tb = ttk.Frame(self.frame)
        tb.pack(fill=tk.X, padx=4, pady=2)

        self.btn_prev = ttk.Button(tb, text="◀", width=3, command=self._prev_page)
        self.btn_prev.pack(side=tk.LEFT)
        self.btn_next = ttk.Button(tb, text="▶", width=3, command=self._next_page)
        self.btn_next.pack(side=tk.LEFT, padx=(2, 6))

        ttk.Label(tb, text="Seite:").pack(side=tk.LEFT)
        self.ent_page = ttk.Entry(tb, width=6)
        self.ent_page.insert(0, "1")
        self.ent_page.pack(side=tk.LEFT)
        ttk.Label(tb, text="/").pack(side=tk.LEFT)
        self.lbl_pages = ttk.Label(tb, text="1")
        self.lbl_pages.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(tb, text="Gehe", command=self._goto_page_from_entry).pack(side=tk.LEFT)

        ttk.Separator(tb, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        self.var_only_err = tk.BooleanVar(value=False)
        self.chk_only_err = ttk.Checkbutton(tb, text="Nur Fehler", variable=self.var_only_err, command=self._toggle_only_errors)
        self.chk_only_err.pack(side=tk.LEFT)

        ttk.Separator(tb, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        ttk.Label(tb, text="Filter:").pack(side=tk.LEFT)
        self.cmb_filter_col = ttk.Combobox(tb, values=[], width=30, state="readonly")
        self.cmb_filter_col.pack(side=tk.LEFT, padx=(0, 4))
        self.ent_filter = ttk.Entry(tb, width=24)
        self.ent_filter.pack(side=tk.LEFT)
        ttk.Button(tb, text="Anwenden", command=self._apply_filter).pack(side=tk.LEFT, padx=(4, 0))
        ttk.Button(tb, text="Reset", command=self._reset_filter).pack(side=tk.LEFT, padx=(4, 0))

        # Tree
        tree_container = ttk.Frame(self.frame)
        tree_container.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        self.tree = ttk.Treeview(tree_container, columns=(), show="headings")
        ysb = ttk.Scrollbar(tree_container, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_container, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=ysb.set, xscroll=xsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # Status
        self.status = ttk.Label(self.frame, text="0 Zeilen")
        self.status.pack(fill=tk.X, padx=4, pady=(0, 4))

        # Styles (Zebra)
        self._style = ttk.Style(self.frame)
        if self.settings.zebra:
            self._style.map("Treeview")

        # Keybindings
        self.frame.bind_all("<Left>", lambda e: self._prev_page())
        self.frame.bind_all("<Right>", lambda e: self._next_page())
        self.frame.bind_all("<Control-f>", lambda e: self.ent_filter.focus_set())

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def show_dataframe(self, df: pd.DataFrame) -> None:
        """Zeigt das übergebene DataFrame in der Treeview (mit Paging) an."""
        self._df = df.copy() if df is not None else None
        self._only_errors = False
        self.var_only_err.set(False)

        # Filter zurücksetzen
        self._filter_text = ""
        self._global_filter_text = ""
        self._filter_col = None
        self.ent_filter.delete(0, tk.END)

        self._rebuild_view()
        self._rebuild_tree()
        self._load_page(0)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _rebuild_view(self) -> None:
        """Wendet „Nur Fehler“-Sicht und (optional) Spaltenfilter auf _df an und setzt _df_view."""
        if self._df is None:
            self._df_view = None
            return

        dfv = self._df

        # Nur Fehler (falls validation_ok existiert)
        if self._only_errors and "validation_ok" in dfv.columns:
            try:
                dfv = dfv[dfv["validation_ok"] != True]
            except Exception:
                pass

        # Spaltenfilter
        if self._filter_col and self._filter_text:
            col = self._filter_col
            txt = self._filter_text.lower()
            if col in dfv.columns:
                try:
                    mask = dfv[col].astype("string").str.lower().str.contains(txt, na=False)
                    dfv = dfv[mask]
                except Exception:
                    pass

        # Globaler Filter über alle Spalten
        if self._global_filter_text:
            gtxt = self._global_filter_text.lower()
            try:
                # baue eine Zeilenmaske, die true ist, wenn irgendeine Spalte den Text enthält
                masks = []
                for c in dfv.columns:
                    try:
                        masks.append(dfv[c].astype("string").str.lower().str.contains(gtxt, na=False))
                    except Exception:
                        # nicht-stringbare Spalten ignorieren
                        continue
                if masks:
                    import functools
                    import operator
                    rowmask = functools.reduce(operator.or_, masks)
                    dfv = dfv[rowmask]
            except Exception:
                pass

        self._df_view = dfv.reset_index(drop=True)

        # Columns aktualisieren
        self._columns = [str(c) for c in (self._df_view.columns if self._df_view is not None else [])]
        self.cmb_filter_col["values"] = self._columns
        if self._filter_col not in self._columns:
            self._filter_col = None
            self.cmb_filter_col.set("")

        # Seitenzahl
        n = 0 if self._df_view is None else len(self._df_view)
        self._pages = max(1, math.ceil(n / self.settings.page_size))
        self.lbl_pages.config(text=str(self._pages))

    def _rebuild_tree(self) -> None:
        """Baut die Treeview-Spalten neu auf (Header, Breiten, Styles)."""
        # Clear
        for c in self.tree["columns"]:
            self.tree.heading(c, text="")
        self.tree["columns"] = self._columns
        self.tree.delete(*self.tree.get_children())

        for col in self._columns:
            self.tree.heading(col, text=col)

        # Breiten abschätzen
        widths = _autofit_widths(self._columns, self._df_view if self._df_view is not None else pd.DataFrame())
        for i, col in enumerate(self._columns):
            self.tree.column(col, width=max(40, int(widths[i] * 7)))  # grobe Umrechnung Zeichen → Pixel

    def _load_page(self, page_index: int) -> None:
        """Lädt die Seite `page_index` aus _df_view in die Treeview."""
        if self._df_view is None:
            # leer
            self.tree.delete(*self.tree.get_children())
            self.ent_page.delete(0, tk.END)
            self.ent_page.insert(0, "0")
            self.status.config(text="0 Zeilen")
            return

        page_index = max(0, min(self._pages - 1, page_index))
        self._page = page_index

        start = page_index * self.settings.page_size
        end = min(len(self._df_view), start + self.settings.page_size)
        chunk = self._df_view.iloc[start:end]

        # Tree löschen & befüllen
        self.tree.delete(*self.tree.get_children())
        if not chunk.empty:
            values_iter = (tuple(_to_cell_str(x) for x in row) for row in chunk.itertuples(index=False, name=None))
            # batch insert
            for vals in values_iter:
                self.tree.insert("", "end", values=vals)

        # Status aktualisieren
        self.ent_page.delete(0, tk.END)
        self.ent_page.insert(0, str(self._page + 1))
        n_total = len(self._df_view)
        self.status.config(text=f"Zeilen: {n_total:,}  |  Seite {self._page + 1} / {self._pages}  |  Anzeige {start + 1}–{end}")

        # „Nur Fehler“ Checkbox aktivieren/deaktivieren
        self._update_only_errors_visibility()

    def _update_only_errors_visibility(self) -> None:
        """Blendet die 'Nur Fehler'-Checkbox ein/aus je nach vorhandener Spalte."""
        if self._df_view is None:
            self.chk_only_err.configure(state=tk.DISABLED)
            return
        if "validation_ok" in self._df_view.columns:
            self.chk_only_err.configure(state=tk.NORMAL)
        else:
            self.chk_only_err.configure(state=tk.DISABLED)
            self.var_only_err.set(False)
            self._only_errors = False

    # ------------------------------------------------------------------ #
    # Events / Commands
    # ------------------------------------------------------------------ #

    def _prev_page(self) -> None:
        if self._page > 0:
            self._load_page(self._page - 1)

    def _next_page(self) -> None:
        if self._page + 1 < self._pages:
            self._load_page(self._page + 1)

    def _goto_page_from_entry(self) -> None:
        try:
            p = int(self.ent_page.get().strip()) - 1
        except Exception:
            p = self._page
        self._load_page(p)

    def _apply_filter(self) -> None:
        self._filter_col = self.cmb_filter_col.get().strip() or None
        self._filter_text = self.ent_filter.get().strip()
        self._rebuild_view()
        self._load_page(0)

    def _reset_filter(self) -> None:
        self._filter_col = None
        self._filter_text = ""
        self._global_filter_text = ""
        self.ent_filter.delete(0, tk.END)
        self.cmb_filter_col.set("")
        self._rebuild_view()
        self._load_page(0)

    def _toggle_only_errors(self) -> None:
        self._only_errors = bool(self.var_only_err.get())
        self._rebuild_view()
        self._load_page(0)

    # ------------------------------------------------------------------ #
    # Externer globaler Filter (für Header-Suchfeld)
    # ------------------------------------------------------------------ #
    def set_global_filter(self, text: str) -> None:
        """Setzt einen globalen Filtertext, der alle Spalten durchsucht."""
        self._global_filter_text = text or ""
        self._rebuild_view()
        self._load_page(0)
