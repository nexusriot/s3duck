"""The transfer queue: what is running, what is waiting, what failed."""

import diagnostics
import hashlib
import json
import os
import time
from PyQt6.QtCore import QThread, QTimer, Qt, pyqtSlot
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox, QSystemTrayIcon
from core.formatting import format_completion_notification
from core.locations import CROSS_PROFILE_METHODS
from core.naming import _human_bytes
from datetime import datetime
from model import plan_prefix_download, prefix_of
from ui.constants import (
    AUTO_RETRY_DELAY_MS, EMA_ALPHA, RATE_WINDOW_SEC,
    STALL_DECAY_INTERVAL_SEC,
)
from ui.dialogs.history import OverwriteDialog, TransferHistoryDialog
from ui.widgets.queue_panel import _QEntry
from utils import release_worker_on_finish
from version import __VERSION__
from workers.transfer import Worker

try:
    from PyQt6 import sip
except ImportError:
    sip = None


class TransferQueueMixin:
    """The transfer queue: what is running, what is waiting, what failed."""

    def _transfer_origin(self):
        model = self.data_model
        return {"profile": self.profile_name,
                "endpoint": model.profile_endpoint_url,
                "region": model.profile_region,
                "credential_id": hashlib.sha256(
                    str(model.access_key or "").encode()).hexdigest()}

    def transfers_active(self) -> bool:
        if self.thread is None:
            return False

        if sip is not None:
            try:
                if sip.isdeleted(self.thread):
                    self.thread = None
                    self.worker = None
                    return False
            except Exception:
                pass
        try:
            return self.thread.isRunning()
        except RuntimeError:
            self.thread = None
            self.worker = None
            return False

    def _on_batch_progress(self, done, total):
        self._smooth_total = max(1, int(total))
        self._smooth_done = max(0, int(done))

    def _on_tick(self):
        now = time.time()

        if not hasattr(self, "_rate_samples"):
            self._rate_samples = []
        self._rate_samples.append((now, self._smooth_done))
        cutoff = now - RATE_WINDOW_SEC
        self._rate_samples = [p for p in self._rate_samples if p[0] >= cutoff]

        inst_rate_bps = 0.0
        if len(self._rate_samples) >= 2:
            t0, b0 = self._rate_samples[0]
            t1, b1 = self._rate_samples[-1]
            dt = max(1e-6, t1 - t0)
            db = max(0, b1 - b0)
            inst_rate_bps = db / dt  # bytes/sec over recent window

        alpha = EMA_ALPHA
        self._smooth_rate_bps = (
            alpha * inst_rate_bps + (1 - alpha) * self._smooth_rate_bps
        )

        if self._last_tick_time == 0.0:
            # first tick init
            self._last_tick_time = now
            self._last_tick_bytes = self._smooth_done
        else:
            dt_long = now - self._last_tick_time
            if dt_long >= STALL_DECAY_INTERVAL_SEC:
                # if no new bytes lately, bleed off displayed rate
                if self._smooth_done <= self._last_tick_bytes:
                    self._smooth_rate_bps *= 0.5
                self._last_tick_time = now
                self._last_tick_bytes = self._smooth_done

        # avoid tiny random noise
        display_rate_bps = self._smooth_rate_bps
        if display_rate_bps < 1:
            display_rate_bps = 0.0

        pct = 0
        if self._smooth_total > 0:
            pct = int((self._smooth_done / self._smooth_total) * 100)
            pct = min(100, max(0, pct))

        self.pb.setMaximum(100)
        self.pb.setValue(pct)

        remaining = max(0, self._smooth_total - self._smooth_done)
        eta_txt = ""
        if display_rate_bps > 1 and remaining > 0 and pct < 100:
            eta_sec = int(remaining / display_rate_bps)
            m, s = divmod(eta_sec, 60)
            h, m = divmod(m, 60)
            eta_txt = f"  ETA {h:02d}:{m:02d}:{s:02d}"
        elif pct >= 100:
            eta_txt = "  Done"

        self.status_text.setText(
            f"{self._status_prefix} "
            f"{_human_bytes(self._smooth_done)} / {_human_bytes(self._smooth_total)}"
            f"  ({_human_bytes(display_rate_bps)}/s){eta_txt}"
        )

    def _resolve_overwrites(self, job, conflicts, *, what, index_of):
        """
        Apply the user's overwrite choice to *job*.

        Returns the job to run (possibly filtered), or None to cancel.
        ``index_of(entry)`` yields the destination identifying an entry.
        """
        if not conflicts:
            return job
        dlg = OverwriteDialog(self, sorted(conflicts), total=len(job), what=what)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return None
        if dlg.choice() == OverwriteDialog.OVERWRITE:
            return job
        remaining = [e for e in job if index_of(e) not in conflicts]
        skipped = len(job) - len(remaining)
        if skipped:
            self.log(f"Skipped {skipped} existing {what}(s)")
        if not remaining:
            self.statusBar().showMessage("Nothing left to do", 2000)
            return None
        return remaining

    def assign_thread_operation(self, method, job, need_refresh=True,
                                source_bucket="", dest_model=None,
                                source_model=None, if_none_match=False,
                                origin=None, bucket=None, auto_retries=0):
        """
        Queue *job* and return its entry, or None when nothing was queued.

        *bucket* is the bucket the job's keys belong to. A new job leaves it
        out and gets the one that is open now -- the job was just built from
        it. A retry or a history replay passes the bucket the job was made
        in, because by then the user may be looking at another one.

        *origin* is the connection the job was queued against (profile,
        endpoint, region and a hash of the key). A replay carries the
        recorded one so it can be checked against the connection open now.
        """
        if not job:
            return None
        if not self._credentials_ok_for_transfer():
            self.log(f"{method} not started: credentials")
            return None

        # Capture the account and bucket when the user queues the work, not
        # when a later retry happens to start it.
        source_bucket = source_bucket or (
            source_model.bucket if source_model is not None
            else self.data_model.bucket)
        if source_model is None:
            source_model = self.data_model.clone_for_worker()
            if source_bucket != self.data_model.bucket:
                source_model.bucket = source_bucket
                source_model.current_folder = ""
                source_model.prev_folder = ""
                source_model.endpoint_url = source_model.profile_endpoint_url
                source_model.use_path = source_model.profile_use_path
                source_model.region_name = source_model.profile_region
                source_model._client = None
        # Navigation clones normally share discoveries, but a transfer can
        # be retried after a profile switch replaces that shared cache.
        source_model.binding_cache = dict(getattr(source_model, "binding_cache", {}))

        label = self._queue_build_label(method, job)
        entry = _QEntry(
            entry_id=self._queue_next_id,
            method=method,
            job=job,
            need_refresh=need_refresh,
            label=label,
            source_bucket=source_bucket,
            dest_model=dest_model,
            source_model=source_model,
            if_none_match=if_none_match,
            origin=origin or self._transfer_origin(),
            bucket=((self.data_model.bucket or "") if bucket is None
                    else bucket),
            profile=self.profile_name,
        )
        entry.auto_retries = int(auto_retries or 0)
        self._queue_next_id += 1
        # Kept so a failed/cancelled row can be re-run from the queue panel.
        self._queue_entries[entry.entry_id] = entry

        self._wire_queue_panel()
        self._queue_panel.add_entry(entry)

        if self.transfers_active():
            self._transfer_queue.append(entry)
            self.log(f"queued {method}: {label}")
            return entry

        self._start_transfer(entry)
        return entry

    def _wire_queue_panel(self):
        """
        Forget the jobs whose rows "Clear done" removes.

        Connected on first use rather than where the panel is built: a row
        can only be cleared after one was added, and add_entry is reached
        from assign_thread_operation alone, so this runs before it matters.
        """
        if getattr(self, "_queue_panel_wired", False):
            return
        self._queue_panel.rows_cleared.connect(self._on_queue_rows_cleared)
        self._queue_panel_wired = True

    def _on_queue_rows_cleared(self, entry_ids):
        for entry_id in entry_ids:
            self._queue_entries.pop(entry_id, None)

    def _queue_build_label(self, method: str, job) -> str:
        n = len(job)
        verb = {
            "upload": "Upload", "download": "Download", "delete": "Delete",
            "copy": "Copy", "move": "Move",
            "restore": "Restore", "set_storage_class": "Set storage class",
            "delete_buckets": "Delete bucket",
            "empty_buckets": "Empty bucket",
            "sync": "Sync",
            "undelete": "Undo delete of",
            "set_tags": "Tag",
            "zip_download": "Zip",
            "copy_to_profile": "Copy to profile",
            "sync_to_profile": "Sync to profile",
        }.get(method, method.capitalize())
        return f"{verb} {n} item(s)"

    def _start_transfer(self, entry: '_QEntry'):
        method = entry.method
        job = entry.job
        need_refresh = entry.need_refresh

        self.log(f"starting {method}")
        if entry.started_at is None:
            entry.started_at = time.time()
        entry.status = "running"
        self._queue_panel.update_status(entry)
        self._active_entry = entry

        self.thread = QThread(self)
        # Its own boto3 client, so a region rebind inside the transfer cannot
        # race whatever the main thread reads from the shared model. The
        # binding cache is still shared, so discoveries are not lost.
        self.worker = Worker(self._worker_model_for(entry), job,
                             dest_model=entry.dest_model,
                             if_none_match=entry.if_none_match)
        self.worker.moveToThread(self.thread)

        entry.thread = self.thread
        entry.worker = self.worker

        m = getattr(self.worker, method)
        self.thread.started.connect(m)

        self.worker.progress.connect(self.report_logger_progress)
        self.worker.error.connect(self._on_transfer_error)

        entry.error = None
        entry.error_details = ""
        entry.error_transient = False

        def _record_error(msg: str, _entry=entry):
            _entry.error = msg

        def _record_details(text: str, transient: bool, _entry=entry):
            _entry.error_details = text
            _entry.error_transient = bool(transient)

        self.worker.error.connect(_record_error)
        self.worker.details.connect(_record_details)

        def _transfer_ui_start(prefix_text: str):
            self.pb.reset()
            self.pb.setValue(0)
            self.pb.show()
            self._status_prefix = prefix_text
            self.status_text.setText("Preparing…")
            self._smooth_total = 1
            self._smooth_done = 0
            self._rate_samples = []
            self._smooth_rate_bps = 0.0
            self._last_tick_time = 0.0
            self._last_tick_bytes = 0
            self._tick_timer.start()
            self.worker.batch_progress.connect(self._on_batch_progress)

        def _transfer_ui_stop():
            if self._closing:
                return
            try:
                self._on_tick()
            except Exception:
                pass
            try:
                self._tick_timer.stop()
            except Exception:
                pass
            self.pb.hide()

        if method == "download":
            _transfer_ui_start("Downloading…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "upload":
            _transfer_ui_start("Uploading…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "sync":
            _transfer_ui_start("Syncing…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "zip_download":
            _transfer_ui_start("Archiving…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method in ("upload", "download", "sync", "zip_download"):
            eid = entry.entry_id

            def _on_queue_bytes(done, total, _eid=eid):
                if self._closing:
                    return
                self._queue_panel.update_byte_progress(_eid, done, total)

            self.worker.batch_progress.connect(_on_queue_bytes)

        def _reenable_after_thread():
            if not self._closing:
                QTimer.singleShot(0, self.enable_action_buttons)

        def _clear_thread_refs():
            self.thread = None
            self.worker = None
            self._active_entry = None
            if self._closing:
                return
            try:
                if getattr(self, "btnCancel", None) is not None:
                    self.btnCancel.setEnabled(False)
            except Exception:
                pass
            QTimer.singleShot(0, self._queue_start_next)
            _reenable_after_thread()

        def _on_worker_finished(cancelled: bool):
            if self._closing:
                return
            if cancelled:
                self.log(f"{method} cancelled")
                entry.status = "cancelled"
            elif entry.error:
                # A failed job used to be reported as "done".
                self.log(f"{method} failed")
                if entry.error_details:
                    # One line so it stays greppable in the log view.
                    self.log(entry.error_details.replace("\n", " | "))
                entry.status = "error"
                self._maybe_auto_retry(entry)
            else:
                self.log(f"{method} completed")
                entry.status = "done"
            self._batch_stats[entry.status] = (
                self._batch_stats.get(entry.status, 0) + 1)
            self._queue_panel.update_status(entry)

            # _smooth_done only tracks byte-progress methods; for the rest it
            # still holds the previous transfer's total.
            if method in ("upload", "download", "sync", "zip_download"):
                self._record_history(entry, done_bytes=self._smooth_done)
            else:
                self._record_history(entry)

            if method == "delete" and entry.status == "done":
                self._record_undoable_delete(
                    entry.job, since=entry.started_at, bucket=entry.bucket)

            if need_refresh:
                QTimer.singleShot(0, lambda: self.navigate(force=True))

        self.worker.finished.connect(_on_worker_finished)
        self.worker.finished.connect(self.thread.quit)
        release_worker_on_finish(self.thread, self.worker)
        self.thread.finished.connect(_clear_thread_refs)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(_reenable_after_thread)

        self.thread.start()
        self.disable_action_buttons()

    def _worker_model_for(self, entry: '_QEntry'):
        """
        The model clone a transfer runs against.

        Copy/move workers resolve CopySource — and move's delete pass —
        against their model's bucket, so a cross-bucket paste must run on a
        model bound to the SOURCE bucket. The clone inherits the destination
        bucket's endpoint binding (possibly a virtual-host of the wrong
        bucket), so navigation and connection state are reset to the profile
        root and the usual probing rebinds the source on first use.

        Every other job is bound to the bucket it was MADE in, the same way.
        Binding to the open bucket was only right while the job ran straight
        away: a retry, "Retry failed" or a history replay starts after the
        user may have browsed elsewhere, and job rows are bare keys.
        """
        if entry.source_model is not None:
            return entry.source_model
        model = self.data_model.clone_for_worker()
        current = self.data_model.bucket or ""
        target = entry.source_bucket or entry.bucket
        if entry.source_bucket or (target is not None and target != current):
            model.bucket = target
            model.current_folder = ""
            model.prev_folder = ""
            model.endpoint_url = model.profile_endpoint_url
            model.use_path = model.profile_use_path
            model.region_name = model.profile_region
            model._client = None
        return model

    def _queue_start_next(self):
        if self.transfers_active():
            return
        if not self._transfer_queue:
            # Queue drained — report the batch once, not once per job.
            self._notify_transfers_finished()
            return
        entry = self._transfer_queue.pop(0)
        self._start_transfer(entry)

    def _notify_transfers_finished(self):
        stats = self._batch_stats
        if not any(stats.values()):
            return
        self._batch_stats = {"done": 0, "cancelled": 0, "error": 0}
        title, body = format_completion_notification(stats)
        self.log(f"{title}: {body}")
        self.statusBar().showMessage(f"{title} — {body}", 6000)

        enabled = self.store.get("notify_on_complete", "true")
        if str(enabled).lower() not in ("true", "1"):
            return
        # A desktop notification only helps when the window is not in front.
        if self.isActiveWindow():
            return
        tray = self._ensure_tray_icon()
        if tray is None:
            return
        try:
            tray.showMessage(
                title, body, QSystemTrayIcon.MessageIcon.Information, 8000)
        except Exception:
            pass

    def _on_queue_cancel_requested(self, entry_id: int):
        if self._active_entry is not None and self._active_entry.entry_id == entry_id:
            # Cancel only the running entry; queued entries keep waiting and
            # start once this one stops. (cancel_transfers aborts the whole
            # queue and stays on Esc / the toolbar Cancel button.)
            self.statusBar().showMessage("Canceling…", 2000)
            try:
                if self.worker is not None:
                    self.worker.cancel()
            except Exception:
                pass
            return
        for i, e in enumerate(self._transfer_queue):
            if e.entry_id == entry_id:
                e.status = "cancelled"
                self._transfer_queue.pop(i)
                self._queue_panel.update_status(e)
                return

    def load_transfer_history(self) -> list:
        with self.store.group():
            raw = self.settings.value("transfer_history", "") or ""
        try:
            entries = json.loads(raw) if raw else []
        except Exception:
            entries = []
        return entries if isinstance(entries, list) else []

    def _save_transfer_history(self, entries):
        with self.store.group():
            self.settings.setValue(
                "transfer_history", json.dumps(entries[:self.HISTORY_LIMIT]))

    def _record_history(self, entry, done_bytes=0):
        """Append a finished job to the persisted history, newest first."""
        job = entry.job or []
        record = {
            "when": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "method": entry.method,
            "label": entry.label,
            "items": len(job),
            "bytes": int(done_bytes or 0),
            "status": entry.status,
            # Both are what the job was queued against, so a replay cannot
            # inherit whichever profile and bucket the window shows now.
            "origin": entry.origin or self._transfer_origin(),
            "source_bucket": entry.source_bucket or self.data_model.bucket,
        }
        # Recorded for "Run again": the rows are bare keys, and a replay
        # against whatever bucket and profile are open by then would act on
        # the wrong objects.
        if entry.bucket is not None:
            record["bucket"] = entry.bucket
        if entry.profile is not None:
            record["profile"] = entry.profile
        # Big jobs would bloat the settings file; those simply are not re-runnable.
        if len(job) <= self.HISTORY_JOB_LIMIT:
            try:
                record["job"] = json.loads(json.dumps(job))
            except (TypeError, ValueError):
                pass
            else:
                if entry.if_none_match:
                    # Stored with the job: re-running an upload the user
                    # asked to skip existing objects for must still skip
                    # them, however long afterwards it is replayed.
                    record["if_none_match"] = True
        entries = self.load_transfer_history()
        entries.insert(0, record)
        self._save_transfer_history(entries)

    def clear_transfer_history(self):
        self._save_transfer_history([])
        self.log("transfer history cleared")

    def show_transfer_history(self):
        TransferHistoryDialog(self, self, self.load_transfer_history()).exec()

    def rerun_history_entry(self, entry):
        job = entry.get("job") or []
        method = entry.get("method")
        if not job or not method:
            return
        if method in CROSS_PROFILE_METHODS:
            # History is JSON; another profile's live credentials were never in
            # it, so this could only queue a job destined to fail.
            QMessageBox.information(
                self, "Transfer history",
                f"'{method}' cannot be re-run from history because it needs "
                "the other profile's connection. Start it again from the "
                "Tools menu.")
            return
        if method == "sync":
            # A sync job is a plan: what to send, fetch and DELETE, decided by
            # comparing the two sides when it was made. Replayed later it
            # deletes whatever has appeared under those names since, and
            # overwrites edits made since, without anything being compared.
            self._refuse_history_rerun(
                "A sync cannot be re-run from history: its plan was worked "
                "out from both sides as they were then, and replaying it now "
                "could delete or overwrite files changed since. Open Sync "
                "again to compare and review a fresh plan.")
            return
        if entry.get("profile") not in (None, self.profile_name):
            # The clearer half of the origin check below, worth its own
            # message: the name alone already says where this belongs.
            self._refuse_history_rerun(
                f"This entry belongs to profile '{entry.get('profile')}'. "
                "Switch to that profile to re-run it.")
            return
        if ("bucket" not in entry or "source_bucket" not in entry
                or not entry.get("origin")):
            # Recorded before history kept them. Guessing "the bucket that is
            # open now" is exactly how a replay deleted the wrong objects.
            self._refuse_history_rerun(
                "This entry was recorded without its bucket and connection, "
                "so it cannot be re-run safely. Start the operation again "
                "from where it belongs.")
            return
        if entry["origin"] != self._transfer_origin():
            # Same profile name, different connection behind it -- the
            # endpoint, region or access key was edited since. The job's bare
            # keys would resolve against an account it was never meant for.
            self._refuse_history_rerun(
                "This job's original connection cannot be verified. "
                "Open its original profile and start the operation again.")
            return
        # JSON turned the tuples into lists; the workers unpack either.
        self.log(f"re-running {method} from history ({len(job)} item(s))")
        self.assign_thread_operation(
            method,
            [tuple(i) if isinstance(i, list) else i for i in job],
            source_bucket=entry.get("source_bucket") or "",
            if_none_match=bool(entry.get("if_none_match")),
            origin=entry["origin"],
            bucket=entry.get("bucket") or "",
        )

    def _refuse_history_rerun(self, text):
        """
        Say why "Run again" did nothing.

        Opened rather than exec'd: the history dialog has already closed and
        nothing waits on the answer, so there is no reason to block in a
        nested event loop while it is read.
        """
        self.log(f"history: {text}")
        box = QMessageBox(QMessageBox.Icon.Information, "Transfer history",
                          text, QMessageBox.StandardButton.Ok, self)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        box.open()

    def auto_retry_enabled(self) -> bool:
        raw = self.store.get("auto_retry", "true")
        return str(raw).lower() in ("true", "1")

    def _maybe_auto_retry(self, entry):
        """
        Re-queue a job the service asked us to try again, once.

        Only the service's own "try again" answers qualify, and only once:
        a job retried forever on a timer is a queue that never drains, and
        an AccessDenied retried is just a slower AccessDenied.
        """
        if not entry.error_transient or entry.auto_retries >= 1:
            return False
        if not self.auto_retry_enabled():
            return False
        entry.auto_retries += 1
        self.log(
            f"{entry.method} hit a transient failure; retrying once in "
            f"{AUTO_RETRY_DELAY_MS // 1000}s")
        # Already spoken for: "Retry failed" or the row's own button pressed
        # during the wait would otherwise queue the same job a second time.
        self._set_retried(entry, True)
        # An owned timer, not QTimer.singleShot: a static one keeps firing
        # after the window is gone, and its callback would then start a
        # transfer against a torn-down model. A parented QTimer dies with us.
        self._pending_retries.append(entry)
        self._retry_timer.start(AUTO_RETRY_DELAY_MS)
        return True

    def _run_pending_retries(self):
        """Re-queue whatever the retry timer was holding."""
        pending, self._pending_retries = self._pending_retries, []
        if self._closing:
            return
        for entry in pending:
            if self._requeue(entry, automatic=True) is None:
                # Nothing was queued (refused credentials, another profile):
                # the row is the user's to retry again.
                self._set_retried(entry, False)

    def _set_retried(self, entry, retried):
        entry.retried = bool(retried)
        self._queue_panel.set_retried(entry.entry_id, entry.retried)

    def _requeue(self, entry, automatic=False):
        """
        Queue the same job again as a fresh entry, and return it.

        Returns None when nothing was queued. The job goes back to the bucket
        it was made in, and is refused outright when it was made under
        another profile: its keys mean nothing -- or something else -- in
        that account.
        """
        if entry.profile is not None and entry.profile != self.profile_name:
            self.log(
                f"not retrying {entry.method}: it belongs to profile "
                f"'{entry.profile}'")
            self.statusBar().showMessage(
                f"That job belongs to profile '{entry.profile}'", 4000)
            return None
        if automatic:
            self.log(f"retrying {entry.method}: {entry.label}")
        # if_none_match travels with the job: it is the user's answer to the
        # overwrite prompt, and a retry that drops it is a retry that quietly
        # overwrites what they asked to skip. An automatic retry also hands
        # on its count, so the retry of a retry is not "the first" again.
        fresh = self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh,
            source_bucket=entry.source_bucket, dest_model=entry.dest_model,
            source_model=entry.source_model,
            if_none_match=entry.if_none_match, origin=entry.origin,
            bucket=entry.bucket,
            auto_retries=entry.auto_retries if automatic else 0)
        if fresh is not None:
            self._set_retried(entry, True)
            # A partial first attempt left markers too; undo must cover them.
            fresh.started_at = entry.started_at
        return fresh

    def failed_entries(self) -> list:
        """Every failed job not yet queued again, oldest first."""
        return [entry for entry in
                sorted(self._queue_entries.values(),
                       key=lambda e: e.entry_id)
                if entry.status == "error" and not entry.retried]

    def retry_failed_transfers(self):
        """Re-queue every failed job in one go."""
        failed = self.failed_entries()
        if not failed:
            self.statusBar().showMessage("Nothing has failed", 2000)
            return
        self.log(f"retrying {len(failed)} failed job(s)")
        for entry in failed:
            self._requeue(entry)
        self.statusBar().showMessage(
            f"Re-queued {len(failed)} failed job(s)", 4000)

    def _on_queue_retry_requested(self, entry_id: int):
        """Re-queue a failed or cancelled job as a fresh entry."""
        entry = self._queue_entries.get(entry_id)
        if entry is None or entry.status not in ("cancelled", "error"):
            return
        if entry.retried:
            # Its retry is queued, running or done already; a second press
            # would run the same job twice.
            return
        self.log(f"retrying {entry.method}: {entry.label}")
        # Through _requeue, so the cross-profile models and the overwrite
        # answer travel with the job exactly as they do on an auto-retry.
        self._requeue(entry)

    def _toggle_queue_panel(self):
        if self._queue_panel.isVisible():
            self._queue_panel.hide()
        else:
            self._queue_panel.show()

    def cancel_transfers(self):
        for e in list(self._transfer_queue):
            e.status = "cancelled"
            self._queue_panel.update_status(e)
        self._transfer_queue.clear()

        # An automatic retry waiting on its timer is queued work too, just not
        # in the queue yet. Left armed, the job the user had just cancelled
        # with everything else started again a few seconds later. Before the
        # early return below: the wait is exactly when nothing is running.
        dropped, self._pending_retries = self._pending_retries, []
        self._retry_timer.stop()
        for e in dropped:
            # Still a failure, and still theirs to retry by hand.
            self._set_retried(e, False)
        if dropped:
            self.log(f"dropped {len(dropped)} pending automatic retry(ies)")

        if not self.transfers_active() or self.worker is None:
            return

        self.statusBar().showMessage("Canceling…", 2000)

        try:
            if self.worker is not None:
                self.worker.cancel()
        except Exception:
            pass

        try:
            if self.thread is not None:
                self.thread.requestInterruption()
        except Exception:
            pass
        try:
            self.btnCancel.setEnabled(False)
        except Exception:
            pass

    def report_logger_progress(self, msg):
        self.log(msg)

    @pyqtSlot(str)
    def _on_transfer_error(self, msg: str):
        self.statusBar().showMessage(f"Transfer failed: {msg}", 6000)
        QMessageBox.critical(self, "Transfer failed", msg)

    def _download_target(self, entry):
        """The local path an entry writes to — a file, or a folder's root."""
        key, local_name, _size, folder_path = entry
        if local_name:
            return local_name
        return os.path.join(
            folder_path, os.path.basename(str(key).rstrip("/")))

    def _folder_download_conflicts(self, folder_entries):
        """
        Local roots whose contents a folder download would overwrite.

        Reported per folder rather than per file: a folder is one job entry,
        so Skip can only drop the whole folder. Returns a set, or None if the
        scan failed or was cancelled and the caller should abort.
        """
        if not folder_entries:
            return set()
        clone = self.data_model.clone_for_worker()

        def _scan(_w):
            hits = set()
            for entry in folder_entries:
                key, _local, _size, folder_path = entry
                listed = list(clone.get_keys(prefix_of(key)))
                plan = plan_prefix_download(key, folder_path, listed)
                if any(os.path.exists(path) for _k, path, _s in plan.files):
                    hits.add(plan.base_dir)
            return hits

        result, exc = self._run_with_progress("Checking local folder…", _scan)
        if exc is not None:
            if QMessageBox.question(
                self, "Checking local folder",
                f"Could not check the destination for existing files:\n{exc}\n\n"
                "Continue anyway (existing files would be overwritten)?",
            ) != QMessageBox.StandardButton.Yes:
                return None
            return set()
        if result is None:
            return None  # cancelled
        return set(result)

    def _destination_conflicts(self, dst_keys, dst_bucket=None, dest_model=None):
        """
        Which of dst_keys already exist. Uses one listing per destination
        prefix (not a HEAD per key) on a worker thread.

        Returns a set, or None if the lookup failed/was cancelled and the
        caller should abort.
        """
        keys = [k for k in dst_keys if k]
        if not keys:
            return set()
        # A cross-profile paste writes through the other profile's model, so
        # the check has to be made on that connection, not the open one.
        clone = (dest_model or self.data_model).clone_for_worker()
        # Compared against the clone's own bucket, not the open one, so a
        # cross-profile destination is judged on the connection it will
        # actually be listed through. Same bucket: the binding it already
        # carries is the right one and is left alone.
        if dst_bucket and dst_bucket != (clone.bucket or ""):
            # Back to the profile root, as _worker_model_for does: the clone
            # carries the open bucket's endpoint, which may be a virtual host
            # of THAT bucket, and listing through it answered for the wrong
            # bucket -- "nothing exists there" -- and the copy overwrote.
            clone.bucket = dst_bucket
            clone.endpoint_url = clone.profile_endpoint_url
            clone.use_path = clone.profile_use_path
            clone.region_name = clone.profile_region
            clone._client = None

        def _scan(_w):
            return clone.existing_keys(keys)

        result, exc = self._run_with_progress("Checking destination…", _scan)
        if exc is not None:
            if QMessageBox.question(
                self, "Checking destination",
                f"Could not check the destination for existing objects:\n{exc}\n\n"
                "Continue anyway (existing objects would be overwritten)?",
            ) != QMessageBox.StandardButton.Yes:
                return None
            return set()
        if result is None:
            return None  # cancelled
        return set(result)

    def build_problem_report(self, log_lines=200) -> str:
        """
        Everything worth attaching to a bug report, in one place.

        Assembled rather than asked for: nobody reconstructs a Qt plugin list
        and the last failure's request id from memory.
        """
        sections = [
            diagnostics.format_report(diagnostics.collect(
                self.current_dir, version=__VERSION__,
                model=self.data_model, profile_name=self.profile_name)),
        ]
        failures = [entry for entry in self._queue_entries.values()
                    if entry.status == "error" and entry.error_details]
        if failures:
            latest = max(failures, key=lambda entry: entry.entry_id)
            sections.append(
                "== Last failure ==\n"
                f"{latest.method}: {latest.label}\n{latest.error_details}")
        tail = self.log_file.tail(log_lines)
        if not tail:
            tail = "\n".join(
                self.logview.toPlainText().splitlines()[-log_lines:])
        sections.append(f"== Log (last {log_lines} lines) ==\n{tail}")
        return "\n\n".join(sections)

    def show_failure_details(self, entry_id):
        """
        What the service actually said about a failed job.

        The log only ever kept ``str(exc)``, so a report of "it failed" could
        never be traced back to a request on the provider's side.
        """
        entry = self._queue_entries.get(int(entry_id))
        if entry is None:
            return
        text = entry.error_details or entry.error or "No details were recorded."
        header = f"{entry.method}: {entry.label}"
        box = QMessageBox(self)
        box.setWindowTitle("Failure details")
        box.setIcon(QMessageBox.Icon.Warning)
        box.setText(header)
        box.setDetailedText(text)
        copy_btn = box.addButton("Copy", QMessageBox.ButtonRole.ActionRole)
        box.addButton(QMessageBox.StandardButton.Close)
        box.exec()
        if box.clickedButton() is copy_btn:
            QApplication.clipboard().setText(f"{header}\n{text}")
            self.statusBar().showMessage("Failure details copied", 3000)

    def _credentials_ok_for_transfer(self) -> bool:
        """
        Guard the queue against credentials that will not outlive the job.

        A transfer that dies half way through an expired session leaves parts
        on the server and a confusing error, so this offers the refresh first.
        """
        state, label = self.credential_state()
        if state in ("none", "ok"):
            return True
        if self.can_refresh_credentials():
            question = (
                f"This profile's credentials {label or 'have expired'}. "
                "Refresh them before starting?")
            answer = QMessageBox.question(
                self, "Credentials", question,
                QMessageBox.StandardButton.Yes
                | QMessageBox.StandardButton.No
                | QMessageBox.StandardButton.Cancel)
            if answer == QMessageBox.StandardButton.Cancel:
                return False
            if answer == QMessageBox.StandardButton.Yes:
                return self.refresh_credentials()
            return True
        if state == "expired":
            answer = QMessageBox.question(
                self, "Credentials",
                "This profile's credentials have expired and cannot be "
                "refreshed from here. Start anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            return answer == QMessageBox.StandardButton.Yes
        self.log(f"warning: credentials {label}")
        return True
