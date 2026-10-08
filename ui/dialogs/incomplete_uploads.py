"""Multipart uploads that were started and never finished.
"""

import threading
from PyQt6.QtCore import QThread
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QDialog, QHBoxLayout, QInputDialog, QLabel,
    QMessageBox, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.naming import _human_bytes
from datetime import datetime, timedelta, timezone
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class IncompleteUploadsDialog(DialogDismissMixin, QDialog):
    """List and abort in-flight multipart uploads.

    Orphaned uploads (from a cancelled or crashed transfer) keep their already
    uploaded parts stored and billed, are invisible in normal object listings,
    and make DeleteBucket fail."""

    def __init__(self, parent, main_window, model, prefix=""):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._uploads = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Incomplete uploads — {model.bucket}")
        self.resize(820, 460)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._scope = QCheckBox(
            f"Only under current prefix ({self._prefix or '/'})"
        )
        self._scope.setChecked(bool(self._prefix))
        self._scope.setEnabled(bool(self._prefix))
        self._scope.toggled.connect(self._reload)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Key", "Initiated", "Size", "Upload ID"]
        )
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._btn_refresh = QPushButton("Refresh")
        self._btn_abort = QPushButton("Abort selected")
        self._btn_abort_old = QPushButton("Abort older than…")
        close_btn = QPushButton("Close")
        self._btn_refresh.clicked.connect(self._reload)
        self._btn_abort.clicked.connect(self._abort_selected)
        self._btn_abort_old.clicked.connect(self._abort_older_than)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_refresh)
        row.addWidget(self._btn_abort)
        row.addWidget(self._btn_abort_old)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._info)
        lay.addWidget(self._scope)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload()

    def stop_threads(self):
        self._stop_thread(cancel=True)

    def _stop_thread(self, cancel: bool = False):
        # Sizing every upload costs one ListParts call, so a scan can run for
        # minutes. When the dialog is closing, signal cancellation first —
        # otherwise the join times out and the dialog is destroyed with its
        # thread still running. On the completion path there is nothing left to
        # cancel, so the flag stays untouched.
        if cancel and self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)

    def _busy(self, busy: bool):
        self._btn_refresh.setEnabled(not busy)
        self._btn_abort_old.setEnabled(not busy)
        self._scope.setEnabled(not busy and bool(self._prefix))
        if busy:
            self._btn_abort.setEnabled(False)
        else:
            self._update_buttons()

    def _reload(self):
        if self._thread is not None:
            return
        self._info.setText("Scanning for incomplete uploads…")
        self._table.setRowCount(0)
        self._uploads = []
        self._busy(True)
        prefix = self._prefix if self._scope.isChecked() else ""
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _fetch(_w):
            return clone.list_multipart_uploads(
                prefix, with_sizes=True, cancel_event=cancel
            )

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
        self._stop_thread()
        self._busy(False)
        if exc is not None:
            self._info.setText(f"Could not list incomplete uploads: {exc}")
            return
        self._uploads = result or []
        wasted = sum(int(u["size"] or 0) for u in self._uploads)
        for u in self._uploads:
            r = self._table.rowCount()
            self._table.insertRow(r)
            when = "" if u["initiated"] is None else str(u["initiated"])
            size = "?" if u["size"] is None else _human_bytes(u["size"])
            cells = [u["key"], when, size, u["upload_id"]]
            for c, txt in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(txt))
        if self._uploads:
            self._info.setText(
                f"{len(self._uploads)} incomplete upload(s) — "
                f"{_human_bytes(wasted)} of stored parts still billed"
            )
        else:
            self._info.setText("No incomplete multipart uploads. 🎉")
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setStretchLastSection(True)
        self._update_buttons()

    def _update_buttons(self):
        self._btn_abort.setEnabled(
            self._thread is None and bool(self._selected_uploads())
        )

    def _selected_uploads(self):
        rows = {ix.row() for ix in self._table.selectedIndexes()}
        return [self._uploads[r] for r in sorted(rows)
                if 0 <= r < len(self._uploads)]

    def _abort_selected(self):
        self._abort(self._selected_uploads())

    def _abort_older_than(self):
        days, ok = QInputDialog.getInt(
            self, "Abort older than",
            "Abort uploads started more than N days ago:", 7, 0, 3650,
        )
        if not ok:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        targets = []
        for u in self._uploads:
            started = u["initiated"]
            # Only real datetimes are comparable; a backend returning something
            # else must not raise inside this slot.
            if not isinstance(started, datetime):
                continue
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            if started < cutoff:
                targets.append(u)
        if not targets:
            QMessageBox.information(
                self, "Abort older than",
                f"No incomplete uploads are older than {days} day(s).",
            )
            return
        self._abort(targets)

    def _abort(self, uploads):
        if not uploads or self._thread is not None:
            return
        wasted = sum(int(u["size"] or 0) for u in uploads)
        if QMessageBox.question(
            self, "Abort uploads",
            f"Abort {len(uploads)} incomplete upload(s), freeing about "
            f"{_human_bytes(wasted)}?\n\n"
            "Their uploaded parts are discarded permanently. Any transfer "
            "still running for them will fail.",
        ) != QMessageBox.StandardButton.Yes:
            return

        self._info.setText(f"Aborting {len(uploads)} upload(s)…")
        self._busy(True)
        targets = [(u["key"], u["upload_id"]) for u in uploads]
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _run(_w):
            failures = []
            for key, upload_id in targets:
                if cancel.is_set():
                    break
                try:
                    clone.abort_multipart_upload(key, upload_id)
                except Exception as exc:
                    failures.append(f"{key}: {exc}")
            return failures

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_aborted)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_aborted(self, result, exc):
        self._stop_thread()
        self._busy(False)
        if exc is not None:
            QMessageBox.warning(self, "Abort uploads", f"Failed:\n{exc}")
        else:
            failures = result or []
            self._mw.log(
                f"aborted incomplete uploads ({len(failures)} failure(s))"
            )
            if failures:
                QMessageBox.warning(
                    self, "Abort uploads",
                    "Some uploads could not be aborted:\n\n"
                    + "\n".join(failures[:20]),
                )
        self._reload()
