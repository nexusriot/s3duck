"""Finding objects that hold the same bytes, and choosing what to drop.
"""

import threading
from PyQt6.QtCore import QThread, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDialog, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QPushButton, QTreeWidget, QTreeWidgetItem,
    QVBoxLayout,
)
from core.dedup import (
    find_duplicate_groups, select_redundant_keys, summarize_duplicate_groups,
)
from core.naming import _human_bytes
from model import Model as DataModel, TransferCancelled
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class DuplicateFinderDialog(DialogDismissMixin, QDialog):
    """
    Find objects holding the same content and delete the redundant copies.

    Grouping uses size + ETag, which ListObjectsV2 already returns, so a scan
    costs no more than a listing. Groups whose ETags cannot settle the question
    are shown separately and are never auto-selected.
    """

    SIZE_UNITS = (("B", 1), ("KB", 1024), ("MB", 1024 ** 2), ("GB", 1024 ** 3))

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._groups = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Find duplicates — {model.bucket}/{self._prefix}")
        self.resize(900, 600)

        self._scope = QCheckBox(
            f"Search the whole bucket (not just {self._prefix or '/'})")
        self._scope.setEnabled(bool(self._prefix))

        self._min_size = QLineEdit("1")
        self._min_size.setMaximumWidth(90)
        self._min_unit = QComboBox()
        for label, factor in self.SIZE_UNITS:
            self._min_unit.addItem(label, factor)
        self._min_unit.setCurrentIndex(2)  # MB — tiny files are mostly noise

        self._btn_scan = QPushButton("Scan")
        self._btn_scan.clicked.connect(self._scan)

        top = QHBoxLayout()
        top.addWidget(QLabel("Ignore files smaller than"))
        top.addWidget(self._min_size)
        top.addWidget(self._min_unit)
        top.addStretch(1)
        top.addWidget(self._btn_scan)

        self._info = QLabel("Scan to look for duplicate objects.")
        self._info.setWordWrap(True)

        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Object", "Size", "Modified", "ETag"])
        self._tree.setColumnWidth(0, 420)
        self._tree.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._tree.itemChanged.connect(self._on_item_changed)

        self._btn_keep_newest = QPushButton("Select all but newest")
        self._btn_keep_oldest = QPushButton("Select all but oldest")
        self._btn_confirm = QPushButton("Confirm candidates")
        self._btn_confirm.setToolTip(
            "Ask the service for each candidate's stored digest — settles a "
            "group without downloading either copy, where the backend "
            "supports it")
        self._btn_confirm.clicked.connect(self._confirm_candidates)
        self._btn_clear = QPushButton("Clear selection")
        self._btn_delete = QPushButton("Delete selected…")
        close_btn = QPushButton("Close")
        self._btn_keep_newest.clicked.connect(
            lambda: self._auto_select("newest"))
        self._btn_keep_oldest.clicked.connect(
            lambda: self._auto_select("oldest"))
        self._btn_clear.clicked.connect(self._clear_selection)
        self._btn_delete.clicked.connect(self._delete_selected)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_keep_newest)
        row.addWidget(self._btn_keep_oldest)
        row.addWidget(self._btn_confirm)
        row.addWidget(self._btn_clear)
        row.addStretch(1)
        row.addWidget(self._btn_delete)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(self._scope)
        lay.addWidget(self._info)
        lay.addWidget(self._tree, 1)
        lay.addLayout(row)

        self._update_buttons()

    def stop_threads(self):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)

    def unconfirmed_groups(self) -> list:
        return [group for group in self._groups if not group.get("confirmed")]

    @staticmethod
    def confirm_groups(model, groups, cancel_event=None) -> list:
        """
        Settle each candidate group from stored digests alone.

        Returns the verdicts in the same order: "same" (a real duplicate),
        "different" (ruled out) or "unknown" (the backend cannot say without
        the bytes). Run off the UI thread — it is one request per member.
        """
        verdicts = []
        for group in groups or []:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            fingerprints = []
            for key, _modified in group.get("members") or []:
                try:
                    fingerprints.append(model.object_fingerprint(key))
                except Exception:
                    fingerprints.append(("", ""))
            verdicts.append(DataModel.compare_fingerprints(fingerprints))
        return verdicts

    def _confirm_candidates(self):
        if self._thread is not None:
            return
        pending = self.unconfirmed_groups()
        if not pending:
            return
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel
        self._info.setText(
            f"Confirming {len(pending)} candidate group(s) from stored "
            "digests…")
        self._btn_scan.setEnabled(False)
        self._btn_confirm.setEnabled(False)

        def _run(_w):
            return self.confirm_groups(clone, pending, cancel_event=cancel)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(
            lambda result, exc: self._on_confirmed(pending, result, exc))
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_confirmed(self, pending, verdicts, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_scan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Could not confirm: {exc}")
            self._update_buttons()
            return
        confirmed = 0
        ruled_out = 0
        for group, verdict in zip(pending, list(verdicts or [])):
            if verdict == "same":
                group["confirmed"] = True
                confirmed += 1
            elif verdict == "different":
                group["ruled_out"] = True
                ruled_out += 1
        if ruled_out:
            # A group proven to hold different bytes is not a finding.
            self._groups = [group for group in self._groups
                            if not group.get("ruled_out")]
        self._render()
        still = len(self.unconfirmed_groups())
        self._info.setText(
            f"{self._info.text()}  —  confirmed {confirmed}, ruled out "
            f"{ruled_out}, still undecidable {still}")

    def min_size_bytes(self) -> int:
        raw = (self._min_size.text() or "").strip()
        factor = self._min_unit.currentData() or 1
        try:
            value = int(float(raw) * factor)
        except ValueError:
            value = 1
        return max(1, value)

    def _scan(self):
        if self._thread is not None:
            return
        prefix = "" if self._scope.isChecked() else self._prefix
        min_size = self.min_size_bytes()
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel
        self._groups = []
        self._tree.clear()
        self._info.setText("Scanning…")
        self._btn_scan.setEnabled(False)
        self._update_buttons()

        def _run(_w):
            rows = clone.list_object_digests(prefix, cancel_event=cancel)
            return find_duplicate_groups(rows, min_size=min_size)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_scanned)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_scanned(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_scan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Scan failed: {exc}")
            self._update_buttons()
            return
        self._groups = result or []
        self._render()

    def _render(self):
        self._tree.blockSignals(True)
        self._tree.clear()
        for index, group in enumerate(self._groups):
            confirmed = group["confirmed"]
            title = (
                f"{group['count']} copies · {_human_bytes(group['size'])} each "
                f"· {_human_bytes(group['wasted'])} reclaimable"
            )
            if not confirmed:
                title += "  — same size, ETags cannot confirm"
            parent = QTreeWidgetItem([title, "", "", group["etag"]])
            parent.setFirstColumnSpanned(False)
            parent.setData(0, Qt.ItemDataRole.UserRole, ("group", index))
            self._tree.addTopLevelItem(parent)
            for key, modified in group["members"]:
                child = QTreeWidgetItem([
                    key,
                    _human_bytes(group["size"]),
                    "" if modified is None else str(modified),
                    group["etag"],
                ])
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, Qt.CheckState.Unchecked)
                child.setData(0, Qt.ItemDataRole.UserRole, ("key", key))
                parent.addChild(child)
            parent.setExpanded(True)
        self._tree.blockSignals(False)

        summary = summarize_duplicate_groups(self._groups)
        if not self._groups:
            self._info.setText("No duplicates found.")
        else:
            self._info.setText(
                f"{summary['groups']} group(s), {summary['redundant']} "
                f"redundant copy(ies), {_human_bytes(summary['wasted'])} "
                "reclaimable from confirmed matches. Nothing is selected until "
                "you choose — review before deleting."
            )
        self._update_buttons()

    def _on_item_changed(self, _item, _column):
        self._update_buttons()

    def _iter_key_items(self):
        for i in range(self._tree.topLevelItemCount()):
            parent = self._tree.topLevelItem(i)
            for j in range(parent.childCount()):
                yield parent.child(j)

    def selected_keys(self) -> list:
        out = []
        for item in self._iter_key_items():
            if item.checkState(0) == Qt.CheckState.Checked:
                kind, key = item.data(0, Qt.ItemDataRole.UserRole)
                if kind == "key":
                    out.append(key)
        return out

    def _auto_select(self, keep):
        chosen = select_redundant_keys(self._groups, keep=keep)
        self._tree.blockSignals(True)
        for item in self._iter_key_items():
            _kind, key = item.data(0, Qt.ItemDataRole.UserRole)
            item.setCheckState(
                0,
                Qt.CheckState.Checked if key in chosen else Qt.CheckState.Unchecked,
            )
        self._tree.blockSignals(False)
        self._update_buttons()

    def _clear_selection(self):
        self._tree.blockSignals(True)
        for item in self._iter_key_items():
            item.setCheckState(0, Qt.CheckState.Unchecked)
        self._tree.blockSignals(False)
        self._update_buttons()

    def _selection_would_empty_a_group(self) -> bool:
        """True if every copy in some group is checked — that deletes the
        content outright rather than de-duplicating it."""
        checked = set(self.selected_keys())
        for group in self._groups:
            keys = [k for k, _m in group["members"]]
            if keys and all(k in checked for k in keys):
                return True
        return False

    def _update_buttons(self):
        busy = self._thread is not None
        has_groups = bool(self._groups)
        writable = not self._mw.is_read_only()
        self._btn_keep_newest.setEnabled(has_groups and not busy)
        self._btn_keep_oldest.setEnabled(has_groups and not busy)
        self._btn_clear.setEnabled(has_groups and not busy)
        self._btn_confirm.setEnabled(
            bool(self.unconfirmed_groups()) and not busy)
        self._btn_delete.setEnabled(
            bool(self.selected_keys()) and not busy and writable)

    def _delete_selected(self):
        keys = self.selected_keys()
        if not keys:
            return
        total = 0
        for group in self._groups:
            for key, _m in group["members"]:
                if key in keys:
                    total += group["size"]
        warning = ""
        if self._selection_would_empty_a_group():
            warning = (
                "\n\nWARNING: every copy in at least one group is selected — "
                "that deletes the content entirely, not just the duplicates."
            )
        if QMessageBox.question(
            self, "Delete duplicates",
            f"Permanently delete {len(keys)} object(s), freeing about "
            f"{_human_bytes(total)}?{warning}",
        ) != QMessageBox.StandardButton.Yes:
            return
        self.accept()
        self._mw.delete_duplicate_keys(keys)
