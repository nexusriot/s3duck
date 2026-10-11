"""Deep search across a bucket, with saved searches.
"""

import re
import threading
from PyQt6 import QtWidgets
from PyQt6.QtCore import QDate, QThread, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDateEdit, QDialog, QGroupBox,
    QHBoxLayout, QInputDialog, QLabel, QLineEdit, QMenu, QPushButton,
    QTableWidget, QTableWidgetItem, QToolButton, QVBoxLayout,
)
from core.naming import _human_bytes
from datetime import datetime, time as dtime, timezone
from model import Model
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class SearchDialog(DialogDismissMixin, QDialog):
    """Recursively search the current bucket/prefix by key substring."""

    MAX_RESULTS = 1000

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._thread = None
        self._worker = None
        self._cancel = None
        self._results = []

        scope = self._prefix if self._prefix else "(bucket root)"
        self.setWindowTitle(f"Search — {model.bucket}/{self._prefix}")
        self.resize(760, 480)

        self._query = QLineEdit()
        self._query.setPlaceholderText(f"Search under {scope} and below…")
        self._query.returnPressed.connect(self._run_search)
        self._btn_search = QPushButton("Search")
        self._btn_search.clicked.connect(self._run_search)
        top = QHBoxLayout()
        top.addWidget(self._query, 1)
        top.addWidget(self._btn_search)

        # Saved searches: with results now driving the queue, a recurring
        # cleanup is worth one click rather than a re-typed form.
        self._saved = QComboBox()
        self._saved.setMinimumWidth(180)
        self._saved.currentIndexChanged.connect(self._on_saved_chosen)
        self._btn_save_search = QPushButton("Save…")
        self._btn_save_search.clicked.connect(self._save_search)
        self._btn_delete_search = QPushButton("Delete")
        self._btn_delete_search.clicked.connect(self._delete_search)
        saved_row = QHBoxLayout()
        saved_row.addWidget(QLabel("Saved"))
        saved_row.addWidget(self._saved)
        saved_row.addWidget(self._btn_save_search)
        saved_row.addWidget(self._btn_delete_search)
        saved_row.addStretch(1)

        self._regex = QCheckBox("Regex")
        self._case = QCheckBox("Case sensitive")
        self._exts = QLineEdit()
        self._exts.setPlaceholderText("Extensions, e.g. txt, .log")
        self._min_size = QLineEdit()
        self._min_size.setPlaceholderText("min")
        self._min_size.setMaximumWidth(90)
        self._max_size = QLineEdit()
        self._max_size.setPlaceholderText("max")
        self._max_size.setMaximumWidth(90)
        self._size_unit = QComboBox()
        for label, factor in (("B", 1), ("KB", 1024),
                              ("MB", 1024 ** 2), ("GB", 1024 ** 3)):
            self._size_unit.addItem(label, factor)
        self._size_unit.setCurrentIndex(0)

        self._use_after = QCheckBox("Modified after")
        self._after = QDateEdit()
        self._after.setCalendarPopup(True)
        self._after.setDate(QDate.currentDate().addMonths(-1))
        self._after.setEnabled(False)
        self._use_after.toggled.connect(self._after.setEnabled)
        self._use_before = QCheckBox("before")
        self._before = QDateEdit()
        self._before.setCalendarPopup(True)
        self._before.setDate(QDate.currentDate())
        self._before.setEnabled(False)
        self._use_before.toggled.connect(self._before.setEnabled)

        flags_row = QHBoxLayout()
        flags_row.addWidget(self._regex)
        flags_row.addWidget(self._case)
        flags_row.addStretch(1)

        size_row = QHBoxLayout()
        size_row.addWidget(QLabel("Size"))
        size_row.addWidget(self._min_size)
        size_row.addWidget(QLabel("to"))
        size_row.addWidget(self._max_size)
        size_row.addWidget(self._size_unit)
        size_row.addSpacing(12)
        size_row.addWidget(QLabel("Extensions"))
        size_row.addWidget(self._exts, 1)

        date_row = QHBoxLayout()
        date_row.addWidget(self._use_after)
        date_row.addWidget(self._after)
        date_row.addWidget(self._use_before)
        date_row.addWidget(self._before)
        date_row.addStretch(1)

        filters = QGroupBox("Filters")
        filters_lay = QVBoxLayout()
        filters_lay.addLayout(flags_row)
        filters_lay.addLayout(size_row)
        filters_lay.addLayout(date_row)
        filters.setLayout(filters_lay)
        self._filters_box = filters

        self._info = QLabel("")
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["Key", "Size", "Modified"])
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        # Extended, not single: a search that cannot act on what it found is
        # only half a tool — the queue can already delete, tag, re-type and
        # download any list of keys.
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.doubleClicked.connect(self._goto_selected)
        self._table.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._result_menu)

        self._btn_goto = QPushButton("Go to location")
        self._btn_copy = QPushButton("Copy keys")
        self._btn_actions = QToolButton()
        self._btn_actions.setText("Actions")
        self._btn_actions.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        self._btn_actions.setMenu(QMenu(self._btn_actions))
        self._btn_select_all = QPushButton("Select all")
        close_btn = QPushButton("Close")
        self._btn_goto.clicked.connect(self._goto_selected)
        self._btn_copy.clicked.connect(self._copy_selected)
        self._btn_select_all.clicked.connect(self._table.selectAll)
        close_btn.clicked.connect(self.reject)
        self._btn_goto.setEnabled(False)
        self._btn_copy.setEnabled(False)
        self._btn_actions.setEnabled(False)
        self._btn_select_all.setEnabled(False)
        self._table.itemSelectionChanged.connect(self._update_buttons)
        row = QHBoxLayout()
        row.addWidget(self._btn_goto)
        row.addWidget(self._btn_copy)
        row.addWidget(self._btn_actions)
        row.addWidget(self._btn_select_all)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addLayout(saved_row)
        lay.addWidget(filters)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload_saved()
        self._query.setFocus()

    def _update_buttons(self):
        selected = self.selected_keys()
        self._btn_goto.setEnabled(len(selected) == 1)
        self._btn_copy.setEnabled(bool(selected))
        self._btn_actions.setEnabled(bool(selected))
        self._btn_select_all.setEnabled(bool(self._results))
        if selected:
            self._build_actions_menu(self._btn_actions.menu(), selected)

    def selected_keys(self) -> list:
        """Every result row the user has selected, in listing order."""
        rows = sorted({index.row()
                       for index in self._table.selectedIndexes()})
        return [self._results[row][0] for row in rows
                if 0 <= row < len(self._results)]

    def _build_actions_menu(self, menu, keys):
        """
        Fill *menu* with everything the queue can do to these keys.

        Built on demand rather than once, because read-only state and the
        selection size both change what belongs in it.
        """
        menu.clear()
        count = len(keys)
        targets = [(key.rsplit("/", 1)[-1], key, False) for key in keys]
        writable = not self._mw.is_read_only()

        act_download = menu.addAction(f"Download {count} object(s)…")
        act_download.triggered.connect(
            lambda: self._act(self._mw.download_keys, keys))
        act_copy_uri = menu.addAction("Copy s3:// URIs")
        act_copy_uri.triggered.connect(lambda: self._copy_uris(keys))
        if not writable:
            hint = menu.addAction("(read-only profile)")
            hint.setEnabled(False)
            return menu
        menu.addSeparator()
        act_tags = menu.addAction("Add / remove tags…")
        act_tags.triggered.connect(
            lambda: self._act(self._mw.bulk_tags, targets))
        act_class = menu.addAction("Change storage class…")
        act_class.triggered.connect(
            lambda: self._act(self._mw.change_storage_class_ui, targets))
        act_restore = menu.addAction("Restore from Glacier…")
        act_restore.triggered.connect(
            lambda: self._act(self._mw.restore_from_glacier, targets))
        act_type = menu.addAction("Fix Content-Type from extension")
        act_type.triggered.connect(
            lambda: self._act(self._mw.fix_content_type_ui, targets))
        menu.addSeparator()
        act_delete = menu.addAction(f"Delete {count} object(s)…")
        act_delete.triggered.connect(
            lambda: self._act(self._mw.delete_keys, keys))
        return menu

    def _act(self, handler, payload):
        """
        Run a main-window action against the selection.

        The dialog closes first: every one of these queues work that refreshes
        the listing underneath, and a search whose results have just been
        deleted is worse than no search at all.
        """
        self.accept()
        handler(payload)

    def _copy_uris(self, keys):
        bucket = self._model.bucket
        QtWidgets.QApplication.clipboard().setText(
            "\n".join(f"s3://{bucket}/{key}" for key in keys))
        self._mw.statusBar().showMessage(
            f"{len(keys)} URI(s) copied", 2000)

    def _result_menu(self, pos):
        keys = self.selected_keys()
        if not keys:
            return
        menu = QMenu(self)
        if len(keys) == 1:
            act_goto = menu.addAction("Go to location")
            act_goto.triggered.connect(self._goto_selected)
            menu.addSeparator()
        self._build_actions_menu(menu, keys)
        menu.exec(self._table.viewport().mapToGlobal(pos))

    @staticmethod
    def _parse_size(text, factor):
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return int(float(raw) * factor)
        except ValueError:
            return None

    def filter_kwargs(self) -> dict:
        """Collect the filter widgets into search_keys keyword arguments."""
        factor = self._size_unit.currentData() or 1
        exts = [part for part in re.split(r"[,\s]+", self._exts.text() or "")
                if part]
        kwargs = {
            "regex": self._regex.isChecked(),
            "case_sensitive": self._case.isChecked(),
            "min_size": self._parse_size(self._min_size.text(), factor),
            "max_size": self._parse_size(self._max_size.text(), factor),
            "extensions": exts or None,
        }
        if self._use_after.isChecked():
            kwargs["modified_after"] = datetime.combine(
                self._after.date().toPyDate(), dtime.min, tzinfo=timezone.utc)
        if self._use_before.isChecked():
            kwargs["modified_before"] = datetime.combine(
                self._before.date().toPyDate(), dtime.max, tzinfo=timezone.utc)
        return kwargs

    def search_config(self) -> dict:
        """Every filter widget's value, as something JSON can hold."""
        return {
            "query": self._query.text(),
            "regex": self._regex.isChecked(),
            "case": self._case.isChecked(),
            "extensions": self._exts.text(),
            "min_size": self._min_size.text(),
            "max_size": self._max_size.text(),
            "unit": int(self._size_unit.currentData() or 1),
            "use_after": self._use_after.isChecked(),
            "after": self._after.date().toString("yyyy-MM-dd"),
            "use_before": self._use_before.isChecked(),
            "before": self._before.date().toString("yyyy-MM-dd"),
        }

    def apply_search_config(self, config):
        """Fill the form from a saved search."""
        config = dict(config or {})
        self._query.setText(str(config.get("query") or ""))
        self._regex.setChecked(bool(config.get("regex")))
        self._case.setChecked(bool(config.get("case")))
        self._exts.setText(str(config.get("extensions") or ""))
        self._min_size.setText(str(config.get("min_size") or ""))
        self._max_size.setText(str(config.get("max_size") or ""))
        index = self._size_unit.findData(int(config.get("unit") or 1))
        self._size_unit.setCurrentIndex(max(index, 0))
        self._use_after.setChecked(bool(config.get("use_after")))
        self._use_before.setChecked(bool(config.get("use_before")))
        for checked, widget, value in (
            (config.get("use_after"), self._after, config.get("after")),
            (config.get("use_before"), self._before, config.get("before")),
        ):
            if checked and value:
                parsed = QDate.fromString(str(value), "yyyy-MM-dd")
                if parsed.isValid():
                    widget.setDate(parsed)

    def _reload_saved(self, select=""):
        entries = self._mw.load_saved_searches()
        self._saved.blockSignals(True)
        self._saved.clear()
        self._saved.addItem("(none)", None)
        for entry in entries:
            self._saved.addItem(str(entry.get("name") or ""), entry)
        index = self._saved.findText(select) if select else 0
        self._saved.setCurrentIndex(max(index, 0))
        self._saved.blockSignals(False)
        self._btn_delete_search.setEnabled(self._saved.currentIndex() > 0)

    def _on_saved_chosen(self, _index):
        entry = self._saved.currentData()
        self._btn_delete_search.setEnabled(entry is not None)
        if entry is None:
            return
        self.apply_search_config(entry.get("filters") or {})
        self._run_search()

    def _save_search(self):
        name, ok = QInputDialog.getText(
            self, "Save search", "Name:",
            text=self._saved.currentText()
            if self._saved.currentIndex() > 0 else "")
        name = (name or "").strip()
        if not ok or not name:
            return
        entries = [entry for entry in self._mw.load_saved_searches()
                   if str(entry.get("name") or "") != name]
        entries.append({"name": name, "filters": self.search_config()})
        entries.sort(key=lambda entry: str(entry.get("name") or "").lower())
        self._mw.store_saved_searches(entries)
        self._reload_saved(select=name)
        self._info.setText(f"Saved as '{name}'.")

    def _delete_search(self):
        entry = self._saved.currentData()
        if entry is None:
            return
        name = str(entry.get("name") or "")
        entries = [row for row in self._mw.load_saved_searches()
                   if str(row.get("name") or "") != name]
        self._mw.store_saved_searches(entries)
        self._reload_saved()

    def _has_filters(self) -> bool:
        kwargs = self.filter_kwargs()
        return any([
            kwargs["min_size"] is not None, kwargs["max_size"] is not None,
            kwargs["extensions"], "modified_after" in kwargs,
            "modified_before" in kwargs,
        ])

    def _run_search(self):
        if self._thread is not None:
            return
        q = self._query.text().strip()
        # With filters active an empty query is meaningful ("everything of this
        # kind"), so only require text when nothing else narrows the search.
        if not q and not self._has_filters():
            return
        try:
            filters = self.filter_kwargs()
            Model.build_search_matcher(q, **filters)  # validate the regex now
        except ValueError as exc:
            self._info.setText(str(exc))
            return
        self._info.setText("Searching…")
        self._table.setRowCount(0)
        self._results = []
        self._btn_search.setEnabled(False)
        prefix = self._prefix
        limit = self.MAX_RESULTS
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _search(_w):
            return clone.search_keys(prefix, q, cancel_event=cancel,
                                     max_results=limit, **filters)

        self._thread = QThread(self)
        self._worker = FuncWorker(_search)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_results)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def stop_threads(self):
        if self._cancel is not None:
            self._cancel.set()
        self._stop_search_thread()

    def _stop_search_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_results(self, result, exc):
        self._stop_search_thread()
        self._btn_search.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Search failed: {exc}")
            return
        self._results = result or []
        for key, size, modified in self._results:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(key))
            self._table.setItem(r, 1, QTableWidgetItem(_human_bytes(size)))
            self._table.setItem(
                r, 2, QTableWidgetItem("" if modified is None else str(modified)))
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setStretchLastSection(True)
        capped = len(self._results) >= self.MAX_RESULTS
        self._info.setText(
            f"{len(self._results)} match(es)"
            + (f" — showing first {self.MAX_RESULTS}" if capped else "")
        )
        self._update_buttons()

    def _selected_key(self):
        r = self._table.currentRow()
        if 0 <= r < len(self._results):
            return self._results[r][0]
        return None

    def _copy_selected(self):
        keys = self.selected_keys()
        if not keys:
            return
        QtWidgets.QApplication.clipboard().setText("\n".join(keys))
        self._mw.statusBar().showMessage(
            "Key copied" if len(keys) == 1 else f"{len(keys)} keys copied",
            2000)

    def _goto_selected(self):
        key = self._selected_key()
        if not key:
            return
        self.accept()
        self._mw.goto_key(key)
