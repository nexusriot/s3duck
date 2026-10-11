"""Per-object version history, and the diff between two of them.
"""

import difflib
import os
import threading
from PyQt6.QtCore import QThread, Qt
from PyQt6.QtGui import QFontDatabase
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QDialog, QFileDialog, QHBoxLayout,
    QLabel, QMessageBox, QPlainTextEdit, QProgressDialog, QPushButton,
    QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.naming import _human_bytes
from ui.widgets.highlighters import DiffHighlighter
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class VersionDiffDialog(DialogDismissMixin, QDialog):
    """
    A unified diff between two versions of one object.

    The versions list can already download any version; what it could not do
    is answer the question anyone actually opens it with — what changed.
    """

    MAX_BYTES = 512 * 1024

    def __init__(self, parent, model, key, older, newer):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._older = older
        self._newer = newer
        self._thread = None
        self._worker = None

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Diff — {base}")
        self.resize(880, 620)

        self._info = QLabel("Loading both versions…")
        self._info.setWordWrap(True)
        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._text.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        except Exception:
            pass
        self._highlighter = DiffHighlighter(self._text.document())

        copy_btn = QPushButton("Copy diff")
        copy_btn.clicked.connect(self._copy)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addWidget(copy_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self._info)
        layout.addWidget(self._text, 1)
        layout.addLayout(row)
        self._load()

    @staticmethod
    def build_diff(older_bytes, newer_bytes, older_label, newer_label,
                   truncated=False) -> tuple:
        """
        ``(text, problem)`` for two version bodies.

        Binary content is refused rather than rendered as mojibake: a diff of
        replacement characters looks like a change on every line.
        """
        for payload in (older_bytes, newer_bytes):
            if b"\x00" in (payload or b"")[:8192]:
                return "", "One of these versions is binary; there is nothing to diff."
        older_text = (older_bytes or b"").decode("utf-8", errors="replace")
        newer_text = (newer_bytes or b"").decode("utf-8", errors="replace")
        if "\ufffd" in older_text or "\ufffd" in newer_text:
            return "", "One of these versions is not UTF-8 text."
        if older_text == newer_text:
            note = " (within the part that was read)" if truncated else ""
            return "", f"These two versions are identical{note}."
        diff = difflib.unified_diff(
            older_text.splitlines(), newer_text.splitlines(),
            fromfile=older_label, tofile=newer_label, lineterm="")
        return "\n".join(diff), ""

    def _load(self):
        model = self._model
        key = self._key
        older, newer = self._older, self._newer
        limit = self.MAX_BYTES

        def _fetch(_worker):
            clone = model.clone_for_worker()
            return (clone.get_object_preview(key, limit, version_id=older),
                    clone.get_object_preview(key, limit, version_id=newer))

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_loaded(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        if exc is not None:
            self._info.setText(f"Could not read the versions: {exc}")
            return
        first, second = result
        truncated = bool(first.get("truncated") or second.get("truncated"))
        text, problem = self.build_diff(
            first.get("data"), second.get("data"),
            f"{self._key}@{self._older}", f"{self._key}@{self._newer}",
            truncated=truncated)
        if problem:
            self._info.setText(problem)
            self._text.setPlainText("")
            return
        self._info.setText(
            "Only the first "
            f"{_human_bytes(self.MAX_BYTES)} of each version was read."
            if truncated else "")
        self._text.setPlainText(text)

    def _copy(self):
        QApplication.clipboard().setText(self._text.toPlainText())

    def stop_threads(self):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)


class VersionsDialog(DialogDismissMixin, QDialog):
    """List, download, restore, and delete individual object versions."""

    def __init__(self, parent, main_window, model, key):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._key = key
        self._versions = []
        self._dl_thread = None
        self._dl_worker = None
        self._list_thread = None
        self._list_worker = None

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Versions — {base}")
        self.resize(720, 420)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(
            ["Modified", "Size", "Storage", "Version", "State"]
        )
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        # Extended so two rows can be picked for a diff; every other action
        # still works on exactly one.
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._btn_diff = QPushButton("Compare two…")
        self._btn_diff.setToolTip(
            "Select two versions to see what changed between them")
        self._btn_diff.clicked.connect(self._diff_selected)
        self._btn_download = QPushButton("Download…")
        self._btn_current = QPushButton("Make current")
        self._btn_delete = QPushButton("Delete version")
        close_btn = QPushButton("Close")
        self._btn_download.clicked.connect(self._download_selected)
        self._btn_current.clicked.connect(self._make_current_selected)
        self._btn_delete.clicked.connect(self._delete_selected)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_diff)
        row.addWidget(self._btn_download)
        row.addWidget(self._btn_current)
        row.addWidget(self._btn_delete)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload()

    def stop_threads(self):
        self._stop_list_thread()

    def _stop_list_thread(self):
        th, self._list_thread, self._list_worker = self._list_thread, None, None
        join_qthread(th)

    def _reload(self):
        """Fetch versions + bucket versioning status off the main thread; both
        are network calls and froze the dialog on buckets with many versions."""
        if self._list_thread is not None:
            return
        self._info.setText("Loading versions…")
        self._set_buttons_enabled(False)
        key = self._key
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return (clone.list_object_versions(key),
                    clone.get_bucket_versioning_status())

        self._list_thread = QThread(self)
        self._list_worker = FuncWorker(_fetch)
        self._list_worker.moveToThread(self._list_thread)
        self._list_thread.started.connect(self._list_worker.run)
        self._list_worker.done.connect(self._on_versions_loaded)
        self._list_worker.done.connect(self._list_thread.quit)
        release_worker_on_finish(self._list_thread, self._list_worker)
        self._list_thread.finished.connect(self._list_thread.deleteLater)
        self._list_thread.start()

    def _on_versions_loaded(self, result, exc):
        self._stop_list_thread()
        if exc is not None:
            self._versions = []
            self._render([], "")
            QMessageBox.warning(
                self, "Versions", f"Could not list versions:\n{exc}"
            )
            return
        versions, status = result
        self._versions = versions
        self._render(versions, status)

    def _set_buttons_enabled(self, enabled: bool):
        self._btn_download.setEnabled(enabled)
        self._btn_current.setEnabled(enabled)
        self._btn_delete.setEnabled(enabled)

    def _render(self, versions, status):
        if versions:
            head = f"{len(versions)} version(s)"
        else:
            head = "No stored versions"
        if status:
            head += f" — bucket versioning: {status}"
        elif not versions:
            head += " — this bucket may not have versioning enabled"
        self._info.setText(head)

        self._table.setRowCount(0)
        for v in versions:
            r = self._table.rowCount()
            self._table.insertRow(r)
            when = "" if v["last_modified"] is None else str(v["last_modified"])
            size = "—" if v["is_delete_marker"] else _human_bytes(v["size"])
            storage = "" if v["is_delete_marker"] else v["storage_class"]
            state = []
            if v["is_latest"]:
                state.append("latest")
            if v["is_delete_marker"]:
                state.append("delete-marker")
            cells = [when, size, storage, v["version_id"], ", ".join(state)]
            for c, txt in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(txt))
        self._table.resizeColumnsToContents()
        self._update_buttons()

    def _selected(self):
        rows = self._table.selectionModel().selectedRows()
        if len(rows) != 1:
            return None
        idx = rows[0].row()
        if 0 <= idx < len(self._versions):
            return self._versions[idx]
        return None

    def selected_versions(self) -> list:
        """Every selected row, oldest first — the order a diff reads in."""
        rows = sorted(index.row()
                      for index in self._table.selectionModel().selectedRows())
        picked = [self._versions[row] for row in rows
                  if 0 <= row < len(self._versions)]
        # The listing is newest first, so reverse into chronological order.
        return list(reversed(picked))

    def diffable_pair(self):
        """The two versions to diff, or None when the selection is not two."""
        picked = [v for v in self.selected_versions()
                  if not v["is_delete_marker"]]
        if len(picked) != 2:
            return None
        return picked[0]["version_id"], picked[1]["version_id"]

    def _diff_selected(self):
        pair = self.diffable_pair()
        if pair is None:
            return
        VersionDiffDialog(self, self._model, self._key, *pair).exec()

    def _update_buttons(self):
        v = self._selected()
        has = v is not None
        is_dm = bool(v and v["is_delete_marker"])
        self._btn_download.setEnabled(has and not is_dm)
        self._btn_current.setEnabled(has and not is_dm and not v["is_latest"])
        self._btn_delete.setEnabled(has)
        self._btn_diff.setEnabled(self.diffable_pair() is not None)

    def _make_current_selected(self):
        v = self._selected()
        if not v:
            return
        if QMessageBox.question(
            self, "Make current",
            "Promote this version to be the current object?\n\n"
            "A new current version is created with this version's data; "
            "nothing is deleted.",
        ) != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.make_version_current(
                self._key, v["version_id"], log_fn=self._mw.log
            )
        except Exception as exc:
            QMessageBox.warning(self, "Make current", f"Failed:\n{exc}")
            return
        self._mw.log(f"restored version {v['version_id']} of {self._key}")
        self._reload()
        self._mw.navigate()

    def _delete_selected(self):
        v = self._selected()
        if not v:
            return
        what = "delete marker" if v["is_delete_marker"] else "version"
        if QMessageBox.question(
            self, "Delete version",
            f"Permanently delete this {what}?\nThis cannot be undone.",
        ) != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.delete_object_version(
                self._key, v["version_id"], log_fn=self._mw.log
            )
        except Exception as exc:
            QMessageBox.warning(self, "Delete version", f"Failed:\n{exc}")
            return
        self._mw.log(f"deleted {what} {v['version_id']} of {self._key}")
        self._reload()
        self._mw.navigate()

    def _download_selected(self):
        if self._dl_thread is not None:
            return
        v = self._selected()
        if not v or v["is_delete_marker"]:
            return
        base = os.path.basename(self._key.rstrip("/")) or "object"
        path, _ = QFileDialog.getSaveFileName(self, "Save version as", base)
        if not path:
            return
        key = self._key
        vid = v["version_id"]
        clone = self._model.clone_for_worker()
        cancel = threading.Event()

        prog = QProgressDialog("Downloading…", "Cancel", 0, 100, self)
        prog.setWindowTitle("Download version")
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.setAutoClose(False)
        prog.setAutoReset(False)
        prog.canceled.connect(cancel.set)

        def _dl(w):
            def _cb(total, cur, _k):
                w.progress.emit(int(cur), int(total or 0))
            clone.download_object_version(
                key, vid, path, progress_cb=_cb, cancel_event=cancel
            )
            return path

        self._dl_thread = QThread(self)
        self._dl_worker = FuncWorker(_dl)
        self._dl_worker.moveToThread(self._dl_thread)
        self._dl_thread.started.connect(self._dl_worker.run)

        def _on_prog(cur, total):
            if total > 0:
                prog.setMaximum(100)
                prog.setValue(min(100, int(cur * 100 / total)))
            else:
                prog.setMaximum(0)

        def _on_done(result, exc):
            prog.close()
            self._dl_thread.quit()
            self._dl_thread = None
            self._dl_worker = None
            if exc is not None:
                if not cancel.is_set():
                    QMessageBox.warning(
                        self, "Download version", f"Failed:\n{exc}"
                    )
                return
            self._mw.statusBar().showMessage(f"Saved version to {result}", 4000)

        self._dl_worker.progress.connect(_on_prog)
        self._dl_worker.done.connect(_on_done)
        release_worker_on_finish(self._dl_thread, self._dl_worker)
        self._dl_thread.finished.connect(self._dl_thread.deleteLater)
        self._dl_thread.start()
        prog.show()
