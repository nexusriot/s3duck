"""The transfer queue: what is running, what is waiting, what failed."""

import diagnostics
import json
import os
import time
from PyQt6.QtCore import QThread, QTimer, pyqtSlot
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
                                source_model=None, if_none_match=False):
        if not job:
            return
        if not self._credentials_ok_for_transfer():
            self.log(f"{method} not started: credentials")
            return

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
        )
        self._queue_next_id += 1
        # Kept so a failed/cancelled row can be re-run from the queue panel.
        self._queue_entries[entry.entry_id] = entry

        self._queue_panel.add_entry(entry)

        if self.transfers_active():
            self._transfer_queue.append(entry)
            self.log(f"queued {method}: {label}")
            return

        self._start_transfer(entry)

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
                self._queue_panel.update_byte_progress(_eid, done, total)

            self.worker.batch_progress.connect(_on_queue_bytes)

        def _reenable_after_thread():
            QTimer.singleShot(0, self.enable_action_buttons)

        def _clear_thread_refs():
            self.thread = None
            self.worker = None
            self._active_entry = None
            try:
                if getattr(self, "btnCancel", None) is not None:
                    self.btnCancel.setEnabled(False)
            except Exception:
                pass
            QTimer.singleShot(0, self._queue_start_next)
            _reenable_after_thread()

        def _on_worker_finished(cancelled: bool):
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
                self._record_undoable_delete(entry.job)

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
        """
        if entry.source_model is not None:
            return entry.source_model
        model = self.data_model.clone_for_worker()
        if entry.source_bucket:
            model.bucket = entry.source_bucket
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
        }
        # Big jobs would bloat the settings file; those simply are not re-runnable.
        if len(job) <= self.HISTORY_JOB_LIMIT:
            try:
                record["job"] = json.loads(json.dumps(job))
            except (TypeError, ValueError):
                pass
            else:
                if entry.source_bucket:
                    record["source_bucket"] = entry.source_bucket
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
        # JSON turned the tuples into lists; the workers unpack either.
        self.log(f"re-running {method} from history ({len(job)} item(s))")
        self.assign_thread_operation(
            method,
            [tuple(i) if isinstance(i, list) else i for i in job],
            source_bucket=entry.get("source_bucket") or "",
        )

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
            self._requeue(entry, automatic=True)

    def _requeue(self, entry, automatic=False):
        """Queue the same job again as a fresh entry."""
        if automatic:
            self.log(f"retrying {entry.method}: {entry.label}")
        self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh,
            source_bucket=entry.source_bucket, dest_model=entry.dest_model,
            source_model=entry.source_model)

    def failed_entries(self) -> list:
        """Every queued job that ended in an error, oldest first."""
        return [entry for entry in
                sorted(self._queue_entries.values(),
                       key=lambda e: e.entry_id)
                if entry.status == "error"]

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
        self.log(f"retrying {entry.method}: {entry.label}")
        # The cross-profile jobs carry live models; dropping them re-queued a
        # job the worker could only refuse.
        self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh,
            source_bucket=entry.source_bucket, dest_model=entry.dest_model,
            source_model=entry.source_model)

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

    def _destination_conflicts(self, dst_keys, dst_bucket=None):
        """
        Which of dst_keys already exist. Uses one listing per destination
        prefix (not a HEAD per key) on a worker thread.

        Returns a set, or None if the lookup failed/was cancelled and the
        caller should abort.
        """
        keys = [k for k in dst_keys if k]
        if not keys:
            return set()
        clone = self.data_model.clone_for_worker()
        if dst_bucket:
            clone.bucket = dst_bucket
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
