"""The transfer queue strip: one row per queued or running job.
"""

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QHBoxLayout, QLabel, QProgressBar, QPushButton, QScrollArea, QSizePolicy,
    QVBoxLayout, QWidget,
)
from core.formatting import _scaled_bar_values
from core.queue_state import sse_c_md5


class _QEntry:
    def __init__(self, entry_id, method, job, need_refresh=True, label="",
                 source_bucket="", dest_model=None, source_model=None,
                 if_none_match=False, origin=None, bucket=None,
                 profile=None):
        self.entry_id = entry_id
        self.method = method
        self.job = job
        self.need_refresh = need_refresh
        self.label = label
        # Where the job was made: the bucket that was open and the profile
        # whose credentials it was meant for. Job rows hold bare keys, so
        # without these a retry ran against whatever bucket happened to be
        # open by then -- a failed delete of "logs/" retried after browsing
        # elsewhere deleted the OTHER bucket's "logs/". None means unknown
        # (an entry built by hand rather than through the queue).
        self.bucket = bucket
        self.profile = profile
        # Set when the job's SOURCE keys live in a different bucket than the
        # one being viewed (cross-bucket paste); the worker's model is bound
        # to it before the transfer starts. Kept even for ordinary jobs:
        # history replay and retries must not inherit the bucket the window
        # happens to show.
        self.source_bucket = source_bucket or ""
        # Set for a cross-profile copy: a second model, built from another
        # profile's credentials, that the worker writes through.
        self.dest_model = dest_model
        # The source connection snapshot. Cross-profile pulls supply their
        # own model; ordinary jobs capture a private clone when queued.
        self.source_model = source_model
        self.origin = dict(origin or {})
        # An upload that must not overwrite: the user chose to skip what was
        # already there, so every write carries a precondition.
        self.if_none_match = bool(if_none_match)
        self.status = "queued"
        self.thread = None
        self.worker = None
        self.error = None
        # The same failure with the request id and HTTP status attached, kept
        # so a finished job can still be reported accurately hours later.
        self.error_details = ""
        self.error_transient = False
        # How many times this job has been re-queued automatically. Handed on
        # to the entry the retry creates: counted per entry, every retry
        # started again from zero and "retry once" retried forever.
        self.auto_retries = 0
        # Set once this job has been queued again (by hand or automatically).
        # The row keeps its "error" -- that attempt did fail -- but it is no
        # longer outstanding: "Retry failed" re-ran it every time otherwise,
        # including after the retry had succeeded.
        self.retried = False
        # When the first attempt of this job started (epoch seconds), kept
        # across retries. Undoing a delete removes only the markers written
        # since then, not every marker that ever piled up under the prefix.
        self.started_at = None
        # Indices of job rows the worker reported finished; a resume runs
        # only the rest.
        self.done_items = set()
        # Every row the job has had, across pauses: a resume trims job, but
        # undo-delete and history describe the whole thing.
        self.full_job = list(job or [])
        # MD5 of the SSE-C key the job was queued under ("" for none).
        self.sse_c_md5 = sse_c_md5(getattr(source_model, "sse_c_key", b"") or b"")
        # Set by Pause: the cancellation that follows ends the run as
        # "paused" rather than "cancelled".
        self.pause_requested = False
        # Called once the job really ends (not on a pause); see transfers.
        self.on_finished = None


class _QueueRow(QWidget):
    cancel_requested = pyqtSignal(int)
    retry_requested = pyqtSignal(int)
    details_requested = pyqtSignal(int)
    pause_requested = pyqtSignal(int)
    resume_requested = pyqtSignal(int)

    _OP_ICONS = {
        "upload": "⬆", "download": "⬇", "delete": "✕",
        "copy": "⇆", "move": "➜", "delete_buckets": "✕",
        "empty_buckets": "∅", "sync": "⇅", "undelete": "↺",
        "set_tags": "🏷", "zip_download": "🗜",
        "copy_to_profile": "⇆", "move_to_profile": "➜",
        "restore_versions": "⏱",
        "sync_to_profile": "⇅",
    }
    _STATUS_COLORS = {
        "queued": "#e4e4e8", "running": "#bbdefb",
        "done": "#c8e8c8", "cancelled": "#ffe0b2", "error": "#ffcdd2",
        "paused": "#fff3b0", "pausing": "#fff3b0",
    }

    def __init__(self, entry, parent=None):
        super().__init__(parent)
        self._entry_id = entry.entry_id
        self._retried = False

        icon = QLabel(self._OP_ICONS.get(entry.method, "?"))
        icon.setFixedWidth(20)
        icon.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self._desc = QLabel(entry.label)
        self._desc.setMinimumWidth(120)

        self._status = QLabel("queued")
        self._status.setFixedWidth(72)
        self._status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._update_status_style("queued")

        self._bar = QProgressBar()
        self._bar.setRange(0, 0)
        self._bar.setMaximumHeight(14)
        self._bar.setTextVisible(False)
        self._bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        self._cancel_btn = QPushButton("✕")
        self._cancel_btn.setFixedSize(22, 22)
        self._cancel_btn.setToolTip("Cancel")
        self._cancel_btn.clicked.connect(lambda: self.cancel_requested.emit(self._entry_id))

        self._pause_btn = QPushButton("⏸")
        self._pause_btn.setFixedSize(22, 22)
        self._pause_btn.setToolTip("Pause — keeps what is already transferred")
        self._pause_btn.clicked.connect(
            lambda: self.pause_requested.emit(self._entry_id))

        self._resume_btn = QPushButton("▶")
        self._resume_btn.setFixedSize(22, 22)
        self._resume_btn.setToolTip("Resume")
        self._resume_btn.clicked.connect(
            lambda: self.resume_requested.emit(self._entry_id))
        self._resume_btn.hide()

        self._retry_btn = QPushButton("↻")
        self._retry_btn.setFixedSize(22, 22)
        self._retry_btn.setToolTip("Retry")
        self._retry_btn.clicked.connect(lambda: self.retry_requested.emit(self._entry_id))
        self._retry_btn.hide()

        # Shown only on a failed row: what actually came back from the
        # service, which is the thing a bug report needs.
        self._details_btn = QPushButton("ℹ")
        self._details_btn.setFixedSize(22, 22)
        self._details_btn.setToolTip("Why it failed")
        self._details_btn.clicked.connect(
            lambda: self.details_requested.emit(self._entry_id))
        self._details_btn.hide()

        row = QHBoxLayout(self)
        row.setContentsMargins(4, 2, 4, 2)
        row.addWidget(icon)
        row.addWidget(self._desc, 1)
        row.addWidget(self._status)
        row.addWidget(self._bar, 1)
        row.addWidget(self._details_btn)
        row.addWidget(self._pause_btn)
        row.addWidget(self._resume_btn)
        row.addWidget(self._retry_btn)
        row.addWidget(self._cancel_btn)

    def _update_status_style(self, status: str):
        bg = self._STATUS_COLORS.get(status, "#e4e4e8")
        self._status.setText(status)
        # Force dark text so the label stays readable on the pastel chip in
        # both light and dark themes.
        self._status.setStyleSheet(
            f"background: {bg}; color: #202020; border-radius: 3px; padding: 1px 4px;"
        )

    def set_status(self, status: str):
        self._update_status_style(status)
        if status in ("done", "cancelled", "error"):
            self._bar.setRange(0, 1)
            self._bar.setValue(1 if status == "done" else 0)
            self._cancel_btn.setEnabled(False)
        else:
            self._cancel_btn.setEnabled(True)
        self._pause_btn.setVisible(status in ("queued", "running"))
        self._resume_btn.setVisible(status == "paused")
        # Only an unfinished job is worth re-running, and only once: a job
        # already queued again lives on in the row its retry created.
        self._retry_btn.setVisible(
            status in ("cancelled", "error") and not self._retried)
        self._details_btn.setVisible(status == "error")

    def set_retried(self, retried: bool):
        self._retried = bool(retried)
        self._retry_btn.setToolTip(
            "Already queued again" if self._retried else "Retry")
        self.set_status(self._status.text())

    def set_byte_progress(self, done: int, total: int):
        range_max, value = _scaled_bar_values(done, total)
        self._bar.setRange(0, range_max)
        self._bar.setValue(value)


class TransferQueuePanel(QWidget):
    cancel_requested = pyqtSignal(int)
    retry_requested = pyqtSignal(int)
    details_requested = pyqtSignal(int)
    retry_all_requested = pyqtSignal()
    history_requested = pyqtSignal()
    # The entry ids whose rows "Clear done" removed, so the window can forget
    # those jobs too -- otherwise "Retry failed" kept re-running rows the user
    # had already dismissed and could no longer see.
    rows_cleared = pyqtSignal(list)
    pause_requested = pyqtSignal(int)
    resume_requested = pyqtSignal(int)
    pause_all_requested = pyqtSignal()
    resume_all_requested = pyqtSignal()
    restore_requested = pyqtSignal()
    discard_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows: dict = {}

        hdr = QLabel("  Transfer Queue")
        # Plain bold label (no hardcoded background) so it adapts to the theme;
        # QLabel backgrounds are transparent by default.
        hdr.setStyleSheet("padding: 3px 6px; font-weight: bold;")

        clear_btn = QPushButton("Clear done")
        clear_btn.setFlat(True)
        clear_btn.clicked.connect(self._clear_done)

        retry_btn = QPushButton("Retry failed")
        retry_btn.setFlat(True)
        retry_btn.clicked.connect(self.retry_all_requested)

        pause_all_btn = QPushButton("Pause all")
        pause_all_btn.setFlat(True)
        pause_all_btn.clicked.connect(self.pause_all_requested)

        resume_all_btn = QPushButton("Resume all")
        resume_all_btn.setFlat(True)
        resume_all_btn.clicked.connect(self.resume_all_requested)

        history_btn = QPushButton("History")
        history_btn.setFlat(True)
        history_btn.clicked.connect(self.history_requested)

        hdr_row = QHBoxLayout()
        hdr_row.setContentsMargins(0, 0, 0, 0)
        hdr_row.setSpacing(0)
        hdr_row.addWidget(hdr, 1)
        hdr_row.addWidget(pause_all_btn)
        hdr_row.addWidget(resume_all_btn)
        hdr_row.addWidget(retry_btn)
        hdr_row.addWidget(history_btn)
        hdr_row.addWidget(clear_btn)

        self._content = QWidget()
        self._content_lay = QVBoxLayout(self._content)
        self._content_lay.setContentsMargins(0, 0, 0, 0)
        self._content_lay.setSpacing(1)
        self._content_lay.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidget(self._content)
        scroll.setWidgetResizable(True)
        scroll.setMaximumHeight(140)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        # Unfinished work from the last session. A bar rather than a modal:
        # the window may be opening headless, behind another one, or for a
        # profile whose jobs the user no longer wants, and none of those is a
        # reason to stop and ask.
        self._restore_bar = QWidget()
        restore_row = QHBoxLayout(self._restore_bar)
        restore_row.setContentsMargins(6, 2, 6, 2)
        self._restore_label = QLabel("")
        self._restore_label.setWordWrap(True)
        restore_btn = QPushButton("Restore")
        restore_btn.clicked.connect(self.restore_requested)
        discard_btn = QPushButton("Discard")
        discard_btn.clicked.connect(self.discard_requested)
        restore_row.addWidget(self._restore_label, 1)
        restore_row.addWidget(restore_btn)
        restore_row.addWidget(discard_btn)
        self._restore_bar.hide()

        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        lay.addLayout(hdr_row)
        lay.addWidget(self._restore_bar)
        lay.addWidget(scroll)

        self.hide()

    def add_entry(self, entry):
        row = _QueueRow(entry)
        row.cancel_requested.connect(self.cancel_requested)
        row.retry_requested.connect(self.retry_requested)
        row.details_requested.connect(self.details_requested)
        row.pause_requested.connect(self.pause_requested)
        row.resume_requested.connect(self.resume_requested)
        self._rows[entry.entry_id] = row
        count = self._content_lay.count()
        self._content_lay.insertWidget(count - 1, row)
        self.show()

    def update_status(self, entry):
        row = self._rows.get(entry.entry_id)
        if row is not None:
            row.set_status(entry.status)

    def remove_entry(self, entry_id):
        row = self._rows.pop(entry_id, None)
        if row is not None:
            self._content_lay.removeWidget(row)
            row.deleteLater()
        if not self._rows and self._restore_bar.isHidden():
            self.hide()

    def show_restore_offer(self, text):
        self._restore_label.setText(text)
        self._restore_bar.show()
        self.show()

    def hide_restore_offer(self):
        self._restore_bar.hide()
        if not self._rows:
            self.hide()

    def restore_offer_visible(self) -> bool:
        return not self._restore_bar.isHidden()

    def restore_offer_text(self) -> str:
        return self._restore_label.text()

    def update_byte_progress(self, entry_id: int, done: int, total: int):
        row = self._rows.get(entry_id)
        if row is not None:
            row.set_byte_progress(done, total)

    def set_retried(self, entry_id: int, retried: bool = True):
        row = self._rows.get(entry_id)
        if row is not None:
            row.set_retried(retried)

    def _clear_done(self):
        to_remove = [
            eid for eid, row in list(self._rows.items())
            if row._status.text() in ("done", "cancelled", "error")
        ]
        for eid in to_remove:
            row = self._rows.pop(eid)
            self._content_lay.removeWidget(row)
            row.deleteLater()
        if not self._rows and self._restore_bar.isHidden():
            self.hide()
        if to_remove:
            self.rows_cleared.emit(to_remove)
