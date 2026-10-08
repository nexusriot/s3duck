"""Comparing the two panes, and the sync and watch that follow from it.
"""

import os
import re
import threading
from PyQt6.QtCore import QThread
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDialog, QDialogButtonBox,
    QFileDialog, QFormLayout, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QMessageBox, QPushButton, QRadioButton, QTableWidget, QTableWidgetItem,
    QVBoxLayout,
)
from core.naming import _human_bytes
from core.sync_plan import (
    build_compare_plan, build_sync_plan, summarize_compare_plan,
    summarize_sync_plan,
)
from model import safe_local_path
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
    scan_local_tree,
)


class PaneCompareDialog(DialogDismissMixin, QDialog):
    """
    What differs between the two panes, and a way to copy it either way.

    Both sides are walked recursively, so this also answers "are these two
    prefixes the same" for two locations in one account — which the sync
    dialog could only do by way of a local folder.
    """

    STATUS_LABELS = {
        "only_left": "only on the left",
        "only_right": "only on the right",
        "differs": "differs",
        "same": "same",
    }

    def __init__(self, parent, main_window, left, right):
        super().__init__(parent)
        self._mw = main_window
        # Each side is ("local", path) or ("remote", bucket, prefix).
        self._left = left
        self._right = right
        self._rows = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle("Compare panes")
        self.resize(860, 560)

        self._info = QLabel(
            f"{self.describe(left)}   ⟷   {self.describe(right)}")
        self._info.setWordWrap(True)

        self._hide_same = QCheckBox("Hide identical files")
        self._hide_same.setChecked(True)
        self._hide_same.toggled.connect(self._render)

        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText("Exclude, e.g.  *.tmp  .git/")

        self._btn_compare = QPushButton("Compare")
        self._btn_compare.clicked.connect(self._compare)

        top = QHBoxLayout()
        top.addWidget(self._hide_same)
        top.addWidget(QLabel("Exclude"))
        top.addWidget(self._exclude, 1)
        top.addWidget(self._btn_compare)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Path", "Left", "Right", "Status"])
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)

        self._summary = QLabel("Press Compare.")
        self._summary.setWordWrap(True)

        self._btn_to_right = QPushButton("Copy selected  →")
        self._btn_to_left = QPushButton("←  Copy selected")
        self._btn_to_right.clicked.connect(lambda: self._copy("right"))
        self._btn_to_left.clicked.connect(lambda: self._copy("left"))
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        row = QHBoxLayout()
        row.addWidget(self._btn_to_left)
        row.addWidget(self._btn_to_right)
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self._info)
        layout.addLayout(top)
        layout.addWidget(self._table, 1)
        layout.addWidget(self._summary)
        layout.addLayout(row)
        self._update_buttons()

    @staticmethod
    def describe(side) -> str:
        if side[0] == "local":
            return side[1]
        return f"s3://{side[1]}/{side[2]}"

    @staticmethod
    def read_side(side, model):
        """``{rel: (size, mtime)}`` for one side of the comparison."""
        if side[0] == "local":
            return scan_local_tree(side[1])
        clone = model.clone_for_worker()
        clone.bucket = side[1]
        return clone.list_tree(side[2])

    def _compare(self):
        if self._thread is not None:
            return
        left, right = self._left, self._right
        model = self._mw.data_model
        exclude = self._exclude.text()
        self._summary.setText("Comparing…")
        self._btn_compare.setEnabled(False)

        def _run(_worker):
            return build_compare_plan(
                self.read_side(left, model), self.read_side(right, model),
                exclude=exclude)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_compared)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def stop_threads(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_compared(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        self._btn_compare.setEnabled(True)
        if exc is not None:
            self._summary.setText(f"Could not compare: {exc}")
            return
        self._rows = list(result or [])
        self._render()

    def visible_rows(self) -> list:
        if self._hide_same.isChecked():
            return [row for row in self._rows if row["status"] != "same"]
        return list(self._rows)

    def _render(self):
        rows = self.visible_rows()
        self._table.setRowCount(0)
        for row in rows:
            index = self._table.rowCount()
            self._table.insertRow(index)
            cells = (
                row["rel"],
                "—" if row["left_size"] is None else _human_bytes(row["left_size"]),
                "—" if row["right_size"] is None else _human_bytes(row["right_size"]),
                self.STATUS_LABELS.get(row["status"], row["status"]),
            )
            for column, text in enumerate(cells):
                self._table.setItem(index, column, QTableWidgetItem(text))
        counts = summarize_compare_plan(self._rows)
        self._summary.setText(
            f"{counts['only_left']} only on the left, "
            f"{counts['only_right']} only on the right, "
            f"{counts['differs']} differ, {counts['same']} identical")
        self._update_buttons()

    def selected_rows(self) -> list:
        rows = self.visible_rows()
        indexes = sorted({index.row()
                          for index in self._table.selectedIndexes()})
        return [rows[index] for index in indexes if 0 <= index < len(rows)]

    def _update_buttons(self):
        selected = self.selected_rows()
        writable = not self._mw.is_read_only()
        # A row that exists only on one side can only travel away from it.
        to_right = [r for r in selected if r["status"] != "only_right"]
        to_left = [r for r in selected if r["status"] != "only_left"]
        self._btn_to_right.setEnabled(
            bool(to_right) and (self._right[0] == "local" or writable))
        self._btn_to_left.setEnabled(
            bool(to_left) and (self._left[0] == "local" or writable))

    def _copy(self, towards):
        source = self._left if towards == "right" else self._right
        target = self._right if towards == "right" else self._left
        skip = "only_right" if towards == "right" else "only_left"
        rows = [row for row in self.selected_rows() if row["status"] != skip]
        if not rows:
            return
        job, method, kwargs = self.build_job(source, target,
                                             [row["rel"] for row in rows])
        if not job:
            self._summary.setText("Nothing that can be copied that way.")
            return
        # build_job drops a key whose local destination would escape the
        # pane's folder. Say so rather than quietly copying fewer items than
        # were selected.
        dropped = len(rows) - len(job)
        if dropped:
            QMessageBox.warning(
                self, "Copy",
                f"{dropped} object(s) were skipped because their key would "
                f"be written outside {target[1]}."
            )
        self.accept()
        self._mw.assign_thread_operation(method, job, **kwargs)
        self._mw.statusBar().showMessage(
            f"Copying {len(job)} item(s) {towards}…", 4000)

    @staticmethod
    def build_job(source, target, rels):
        """
        Turn a set of relative paths into a queue job for this pair of sides.

        Returns ``(job, method, kwargs)``. Which operation it is depends
        entirely on the pair, which is why the panes do not decide it.
        """
        rels = [rel for rel in rels if rel]
        if not rels:
            return [], "", {}
        if source[0] == "local" and target[0] == "remote":
            prefix = target[2] or ""
            return ([(prefix + rel,
                      os.path.join(source[1], rel.replace("/", os.sep)))
                     for rel in rels], "upload", {})
        if source[0] == "remote" and target[0] == "local":
            prefix = source[2] or ""
            job = []
            for rel in rels:
                # A key carrying ".." would otherwise be written outside the
                # pane's folder; the same rule a folder download applies.
                local = safe_local_path(target[1], rel)
                if not local:
                    continue
                job.append((prefix + rel, local, None, target[1]))
            return job, "download", {"need_refresh": False}
        if source[0] == "remote" and target[0] == "remote":
            src_prefix, dst_prefix = source[2] or "", target[2] or ""
            cross = source[1] != target[1]
            job = [(src_prefix + rel, dst_prefix + rel, False,
                    target[1] if cross else None) for rel in rels]
            return job, "copy", {
                "source_bucket": source[1] if cross else ""}
        # local -> local is not this app's job.
        return [], "", {}


class WatchFolderDialog(QDialog):
    """Configure the folder that is mirrored up on an interval."""

    INTERVALS = ((30, "30 seconds"), (60, "1 minute"), (300, "5 minutes"),
                 (900, "15 minutes"), (3600, "1 hour"))

    def __init__(self, parent, bucket, prefix, config=None):
        super().__init__(parent)
        config = dict(config or {})
        self.setWindowTitle("Watch a folder")
        self.setMinimumWidth(520)

        self._local = QLineEdit(str(config.get("local") or ""))
        self._local.setPlaceholderText("local folder to mirror upwards…")
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._browse)
        local_row = QHBoxLayout()
        local_row.addWidget(self._local, 1)
        local_row.addWidget(browse)

        self._interval = QComboBox()
        for seconds, label in self.INTERVALS:
            self._interval.addItem(label, seconds)
        index = self._interval.findData(int(config.get("interval") or 300))
        self._interval.setCurrentIndex(max(index, 0))

        self._exclude = QLineEdit(str(config.get("exclude") or ""))
        self._exclude.setPlaceholderText("*.tmp  node_modules/  .git/")

        self._delete_extra = QCheckBox(
            "Delete objects that no longer exist locally")
        self._delete_extra.setChecked(bool(config.get("delete_extra")))

        form = QFormLayout()
        form.addRow(QLabel("Local folder"), local_row)
        form.addRow(QLabel("Destination"),
                    QLabel(f"s3://{bucket}/{prefix}"))
        form.addRow(QLabel("Check every"), self._interval)
        form.addRow(QLabel("Exclude"), self._exclude)
        form.addRow(self._delete_extra)

        note = QLabel(
            "Each check compares the folder with the prefix and queues only "
            "what differs. Watching stops when this window closes, and is "
            "never resumed on its own at startup.")
        note.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(note)
        layout.addWidget(buttons)

    def _browse(self):
        path = QFileDialog.getExistingDirectory(self, "Folder to watch")
        if path:
            self._local.setText(path)

    def _accept(self):
        if not os.path.isdir(self._local.text().strip()):
            QMessageBox.warning(
                self, "Watch a folder", "Choose an existing local folder.")
            return
        self.accept()

    def config(self) -> dict:
        return {
            "local": self._local.text().strip(),
            "interval": int(self._interval.currentData() or 300),
            "exclude": self._exclude.text().strip(),
            "delete_extra": self._delete_extra.isChecked(),
        }


class SyncDialog(DialogDismissMixin, QDialog):
    """Compare a local folder with the current prefix, show a dry-run plan,
    then execute it through the transfer queue."""

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._actions = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Sync — {model.bucket}/{self._prefix}")
        self.resize(820, 560)

        self._local = QLineEdit()
        self._local.setPlaceholderText("local folder…")
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._browse)
        local_row = QHBoxLayout()
        local_row.addWidget(self._local, 1)
        local_row.addWidget(browse)

        self._up = QRadioButton("Local → S3 (upload)")
        self._down = QRadioButton("S3 → local (download)")
        self._up.setChecked(True)
        self._delete_extra = QCheckBox(
            "Delete files at the destination that are missing at the source")

        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText(
            "Exclude, e.g.  *.tmp  node_modules/  .git/")
        self._exclude.setToolTip(
            "Space or comma separated globs. A trailing '/' excludes a whole "
            "directory."
        )
        exclude_row = QHBoxLayout()
        exclude_row.addWidget(QLabel("Exclude"))
        exclude_row.addWidget(self._exclude, 1)

        dir_row = QHBoxLayout()
        dir_row.addWidget(self._up)
        dir_row.addWidget(self._down)
        dir_row.addStretch(1)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Action", "Path", "Size", "Why"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._info = QLabel("Pick a local folder and preview the plan.")
        self._info.setWordWrap(True)

        self._btn_preview = QPushButton("Preview (dry run)")
        self._btn_run = QPushButton("Run sync")
        self._btn_run.setEnabled(False)
        close_btn = QPushButton("Close")
        self._btn_preview.clicked.connect(self._preview)
        self._btn_run.clicked.connect(self._run)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_preview)
        row.addWidget(self._btn_run)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(local_row)
        lay.addLayout(dir_row)
        lay.addLayout(exclude_row)
        lay.addWidget(self._delete_extra)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def stop_threads(self):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)

    def _browse(self):
        path = QFileDialog.getExistingDirectory(self, "Select local folder")
        if path:
            self._local.setText(path)

    def direction(self) -> str:
        return "upload" if self._up.isChecked() else "download"

    def exclude_patterns(self) -> list:
        return [p for p in re.split(r"[,\s]+", self._exclude.text() or "") if p]

    def _preview(self):
        if self._thread is not None:
            return
        local_dir = self._local.text().strip()
        if not local_dir or not os.path.isdir(local_dir):
            self._info.setText("Pick an existing local folder first.")
            return
        self._btn_preview.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._info.setText("Scanning both sides…")
        self._table.setRowCount(0)
        self._actions = []

        prefix = self._prefix
        direction = self.direction()
        delete_extra = self._delete_extra.isChecked()
        exclude = self.exclude_patterns()
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _scan(_w):
            remote = clone.list_tree(prefix, cancel_event=cancel)
            local = scan_local_tree(local_dir)
            return build_sync_plan(local, remote, direction=direction,
                                   delete_extra=delete_extra, exclude=exclude)

        self._thread = QThread(self)
        self._worker = FuncWorker(_scan)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_plan)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_plan(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_preview.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Could not build a plan: {exc}")
            return
        self._actions = result or []
        for entry in self._actions:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(entry["action"]))
            self._table.setItem(r, 1, QTableWidgetItem(entry["rel"]))
            self._table.setItem(
                r, 2, QTableWidgetItem(_human_bytes(entry.get("size") or 0)))
            self._table.setItem(r, 3, QTableWidgetItem(entry.get("reason", "")))
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)

        summary = summarize_sync_plan(self._actions)
        bits = [f"{k}: {v}" for k, v in sorted(summary.items())
                if k != "bytes" and v]
        self._info.setText(
            "  ".join(bits) + f"   ({_human_bytes(summary['bytes'])} to transfer)"
        )
        self._btn_run.setEnabled(bool(self.actionable()))

    def actionable(self):
        return [a for a in self._actions if a.get("action") != "skip"]

    def _run(self):
        todo = self.actionable()
        if not todo:
            return
        summary = summarize_sync_plan(todo)
        deletes = summary.get("delete_remote", 0) + summary.get("delete_local", 0)
        message = (
            f"Apply {len(todo)} action(s), transferring "
            f"{_human_bytes(summary['bytes'])}?"
        )
        if deletes:
            message += f"\n\n{deletes} file(s) will be DELETED."
        if QMessageBox.question(
            self, "Run sync", message,
        ) != QMessageBox.StandardButton.Yes:
            return
        self.accept()
        self._mw.start_sync(
            todo, self._local.text().strip(), self._prefix, self.direction(),
            bucket=self._model.bucket)
