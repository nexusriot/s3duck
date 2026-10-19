"""The other half of the dual pane -- a local folder or a remote prefix.
"""

import os
from PyQt6.QtCore import QThread
from PyQt6.QtGui import QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import (
    QAbstractItemView, QComboBox, QFileDialog, QHBoxLayout, QInputDialog,
    QLabel, QLineEdit, QPushButton, QTreeView, QVBoxLayout, QWidget,
)
from core.locations import parse_s3_location
from core.naming import _human_bytes
from model import FSObjectType
from utils import FuncWorker, join_qthread, release_worker_on_finish


class SecondPane(QWidget):
    """
    The other half of a two-pane view: a local folder or a remote prefix.

    Deliberately its own small model rather than a second MainWindow: only
    listing and selection are needed here, because every operation the pane
    takes part in is executed by the window's existing transfer queue.
    """

    LOCAL = "local"
    REMOTE = "remote"
    COLUMNS = ("Name", "Size", "Modified")

    def __init__(self, parent, main_window, model):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._thread = None
        self._worker = None
        self._entries = []
        self._bucket = model.bucket or ""
        self._prefix = model.current_folder or ""
        self._local_dir = os.path.expanduser("~")

        self.mode = QComboBox()
        self.mode.addItem("Local folder", self.LOCAL)
        self.mode.addItem("Remote prefix", self.REMOTE)
        self.mode.currentIndexChanged.connect(lambda _i: self.refresh())

        self.path = QLineEdit(self._local_dir)
        self.path.returnPressed.connect(self._go_typed)
        self.up_btn = QPushButton("↑")
        self.up_btn.setFixedWidth(28)
        self.up_btn.setToolTip("Up one level")
        self.up_btn.clicked.connect(self.go_up)
        self.browse_btn = QPushButton("…")
        self.browse_btn.setFixedWidth(28)
        self.browse_btn.clicked.connect(self._browse)

        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.addWidget(self.mode)
        top.addWidget(self.up_btn)
        top.addWidget(self.path, 1)
        top.addWidget(self.browse_btn)

        self.view = QTreeView()
        self.view.setRootIsDecorated(False)
        self.view.setUniformRowHeights(True)
        self.view.setSortingEnabled(False)
        self.view.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self.view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.view.doubleClicked.connect(self._on_activated)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(list(self.COLUMNS))
        self.view.setModel(self.model)

        self.status = QLabel("")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        layout.addLayout(top)
        layout.addWidget(self.view, 1)
        layout.addWidget(self.status)

    def current_mode(self) -> str:
        return self.mode.currentData() or self.LOCAL

    def location(self) -> str:
        if self.current_mode() == self.LOCAL:
            return self._local_dir
        return f"s3://{self._bucket}/{self._prefix}"

    def set_remote(self, bucket, prefix):
        """Point the pane at a remote prefix and show it."""
        self._bucket = bucket or ""
        self._prefix = prefix or ""
        index = self.mode.findData(self.REMOTE)
        if self.mode.currentIndex() != index:
            self.mode.setCurrentIndex(index)   # triggers refresh
        else:
            self.refresh()

    def set_local(self, path):
        self._local_dir = os.path.abspath(path or os.path.expanduser("~"))
        index = self.mode.findData(self.LOCAL)
        if self.mode.currentIndex() != index:
            self.mode.setCurrentIndex(index)
        else:
            self.refresh()

    def remote_target(self) -> tuple:
        return self._bucket, self._prefix

    def local_target(self) -> str:
        return self._local_dir

    def entries(self) -> list:
        """``[(name, is_dir, size)]`` for what is currently listed."""
        return list(self._entries)

    def selected(self) -> list:
        """The selected rows as ``(name, is_dir, size)``."""
        rows = sorted({index.row()
                       for index in self.view.selectionModel().selectedIndexes()})
        return [self._entries[row] for row in rows
                if 0 <= row < len(self._entries)]

    def _browse(self):
        if self.current_mode() == self.LOCAL:
            path = QFileDialog.getExistingDirectory(
                self, "Folder", self._local_dir)
            if path:
                self.set_local(path)
            return
        text, ok = QInputDialog.getText(
            self, "Remote prefix", "s3://bucket/prefix:",
            text=f"s3://{self._bucket}/{self._prefix}")
        if not ok:
            return
        bucket, prefix = parse_s3_location(text, "")
        if bucket:
            self.set_remote(bucket, prefix)

    def _go_typed(self):
        text = self.path.text().strip()
        if self.current_mode() == self.LOCAL:
            if os.path.isdir(text):
                self.set_local(text)
            return
        bucket, prefix = parse_s3_location(text, self._bucket)
        if bucket:
            self.set_remote(bucket, prefix)

    def go_up(self):
        if self.current_mode() == self.LOCAL:
            parent = os.path.dirname(self._local_dir.rstrip(os.sep))
            if parent and parent != self._local_dir:
                self.set_local(parent)
            return
        if not self._prefix:
            return
        trimmed = self._prefix.rstrip("/")
        parent = trimmed.rsplit("/", 1)[0] + "/" if "/" in trimmed else ""
        self.set_remote(self._bucket, parent)

    def _on_activated(self, index):
        row = index.row()
        if not (0 <= row < len(self._entries)):
            return
        name, is_dir, _size = self._entries[row]
        if not is_dir:
            return
        if self.current_mode() == self.LOCAL:
            self.set_local(os.path.join(self._local_dir, name))
        else:
            self.set_remote(self._bucket, (self._prefix or "") + name + "/")

    def refresh(self):
        self.path.setText(
            self._local_dir if self.current_mode() == self.LOCAL
            else f"s3://{self._bucket}/{self._prefix}")
        if self.current_mode() == self.LOCAL:
            self._show(self._scan_local())
            return
        if not self._bucket:
            self._show([])
            self.status.setText("No bucket selected")
            return
        if self._thread is not None:
            return
        clone = self._model.clone_for_worker()
        clone.bucket = self._bucket
        prefix = self._prefix

        def _list(_w):
            return clone.list(prefix)

        self.status.setText("Loading…")
        self._thread = QThread(self)
        self._worker = FuncWorker(_list)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_listed)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _scan_local(self):
        rows = []
        try:
            names = sorted(os.listdir(self._local_dir))
        except OSError as exc:
            self.status.setText(str(exc))
            return []
        for name in names:
            full = os.path.join(self._local_dir, name)
            try:
                is_dir = os.path.isdir(full)
                size = 0 if is_dir else os.path.getsize(full)
            except OSError:
                continue
            rows.append((name, is_dir, size))
        rows.sort(key=lambda row: (not row[1], row[0].lower()))
        return rows

    def _on_listed(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        if exc is not None:
            self.status.setText(f"Could not list: {exc}")
            self._show([])
            return
        rows = [(item.name, item.type_ == FSObjectType.FOLDER, int(item.size or 0))
                for item in (result or [])]
        rows.sort(key=lambda row: (not row[1], row[0].lower()))
        self._show(rows)

    def _show(self, rows):
        self._entries = list(rows)
        self.model.removeRows(0, self.model.rowCount())
        for name, is_dir, size in self._entries:
            cells = [
                QStandardItem(name),
                QStandardItem("" if is_dir else _human_bytes(size)),
                QStandardItem(""),
            ]
            for cell in cells:
                cell.setEditable(False)
            self.model.appendRow(cells)
        folders = sum(1 for _n, is_dir, _s in self._entries if is_dir)
        self.status.setText(
            f"{folders} folder(s), {len(self._entries) - folders} file(s)")

    def shutdown(self):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
