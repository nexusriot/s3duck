"""Restoring a prefix to how it was at a chosen moment.
"""

import threading
from datetime import datetime, timezone
from PyQt6.QtCore import QDateTime, QThread
from PyQt6.QtWidgets import (
    QAbstractItemView, QDateTimeEdit, QDialog, QHBoxLayout, QHeaderView,
    QLabel, QMessageBox, QPushButton, QTableWidget, QTableWidgetItem,
    QVBoxLayout,
)
from core.naming import _human_bytes
from core.point_in_time import (
    DELETE, RESTORE, plan_point_in_time, restore_job, summarize_point_in_time,
)
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class PointInTimeDialog(DialogDismissMixin, QDialog):
    """
    Pick a moment, see what would change, then queue it.

    The plan is always shown before anything runs: on a large prefix it is
    the only way to notice that the chosen moment is a day off.
    """

    def __init__(self, parent, main_window, model, prefix, versioning=""):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._actions = []
        self._planned_for = None
        self._thread = None
        self._worker = None
        self._cancel = None
        self.setWindowTitle(
            f"Restore to a point in time — {model.bucket}/{self._prefix}")
        self.resize(820, 540)

        self.moment = QDateTimeEdit(QDateTime.currentDateTime().addSecs(-3600))
        self.moment.setCalendarPopup(True)
        self.moment.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.moment.dateTimeChanged.connect(lambda _d: self.invalidate())
        self.preview_btn = QPushButton("Preview")
        self.preview_btn.clicked.connect(lambda: self.preview())
        self.run_btn = QPushButton("Restore")
        self.run_btn.setEnabled(False)
        self.run_btn.clicked.connect(lambda: self.run())
        close = QPushButton("Close")
        close.clicked.connect(self.reject)

        note = QLabel(
            "Every key under this prefix is put back the way it was at that "
            "moment (local time): a changed object gets its old version "
            "copied back on top, and an object that did not exist then gets "
            "a delete marker. Nothing is destroyed, so this can itself be "
            "undone the same way.")
        note.setWordWrap(True)
        self.versioning_note = QLabel("")
        self.versioning_note.setWordWrap(True)
        if versioning not in ("Enabled", "Suspended"):
            self.versioning_note.setText(
                "Versioning is not enabled on this bucket, so there is no "
                "history to go back to; only objects whose single version "
                "is older than the chosen moment can be kept.")
        self.info = QLabel("Choose a moment and preview.")
        self.info.setWordWrap(True)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Action", "Key", "Version", "Why"])
        self.table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        top = QHBoxLayout()
        top.addWidget(QLabel("Restore to"))
        top.addWidget(self.moment)
        top.addWidget(self.preview_btn)
        top.addStretch(1)
        bottom = QHBoxLayout()
        bottom.addStretch(1)
        bottom.addWidget(self.run_btn)
        bottom.addWidget(close)
        lay = QVBoxLayout(self)
        lay.addWidget(note)
        lay.addWidget(self.versioning_note)
        lay.addLayout(top)
        lay.addWidget(self.info)
        lay.addWidget(self.table, 1)
        lay.addLayout(bottom)

    def chosen_moment(self):
        return datetime.fromtimestamp(
            self.moment.dateTime().toSecsSinceEpoch(), tz=timezone.utc)

    def invalidate(self):
        self._actions = []
        self._planned_for = None
        self.run_btn.setEnabled(False)
        self.table.setRowCount(0)
        self.info.setText("The moment changed — preview again.")

    def preview(self):
        if self._thread is not None:
            return False
        moment = self.chosen_moment()
        clone = self._model.clone_for_worker()
        prefix = self._prefix
        cancel = threading.Event()
        self._cancel = cancel

        def _plan(_w):
            return plan_point_in_time(
                clone.list_prefix_versions(prefix, cancel_event=cancel), moment)

        self.info.setText("Reading the version history…")
        self.preview_btn.setEnabled(False)
        self._pending_moment = moment
        self._thread = QThread(self)
        self._worker = FuncWorker(_plan)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self.on_planned)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()
        return True

    def on_planned(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self.preview_btn.setEnabled(True)
        if exc is not None:
            self.info.setText(f"Could not read the history: {exc}")
            return
        if self._pending_moment != self.chosen_moment():
            self.invalidate()
            return
        self._actions = list(result or [])
        self._planned_for = self._pending_moment
        self.table.setRowCount(0)
        for action in self._actions:
            if action["action"] not in (RESTORE, DELETE):
                continue
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, text in enumerate((
                    action["action"], action["key"], action["version_id"],
                    action["reason"])):
                self.table.setItem(row, col, QTableWidgetItem(text))
        summary = summarize_point_in_time(self._actions)
        self.info.setText(
            f"{summary[RESTORE]} to restore "
            f"({_human_bytes(summary['bytes'])}), {summary[DELETE]} to mark "
            f"deleted, {summary['skip']} unchanged.")
        self.run_btn.setEnabled(bool(restore_job(self._actions))
                                and not self._mw.is_read_only())

    def job(self) -> list:
        return restore_job(self._actions)

    def run(self):
        if self._planned_for is None or self._planned_for != self.chosen_moment():
            self.invalidate()
            return False
        job = self.job()
        if not job:
            return False
        summary = summarize_point_in_time(self._actions)
        if QMessageBox.question(
                self, "Restore to a point in time",
                f"Restore {summary[RESTORE]} object(s) and mark "
                f"{summary[DELETE]} deleted under "
                f"{self._model.bucket}/{self._prefix}?") \
                != QMessageBox.StandardButton.Yes:
            return False
        self._mw.assign_thread_operation(
            "restore_versions", job, source_bucket=self._model.bucket)
        self.accept()
        return True

    def stop_threads(self):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)
