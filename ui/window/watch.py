"""Watching a local folder and pushing what changes."""

import json
import os
from PyQt6.QtCore import QThread, QTimer
from PyQt6.QtWidgets import QDialog
from core.sync_plan import build_sync_plan
from ui.dialogs.compare import WatchFolderDialog
from utils import (
    FuncWorker, join_qthread, release_worker_on_finish, scan_local_tree,
)


class WatchSkipped(Exception):
    """A watch pass that would act on a local tree it could not trust."""


def plan_watch_pass(local_root, remote, config):
    """
    The upload plan for one watch pass, or WatchSkipped.

    REGRESSION (critical, data loss): the local scan reported a missing or
    unreadable folder as an EMPTY one, and with "delete extras" on the plan
    then deleted every remote object under the prefix -- an unplugged drive
    or a renamed folder emptied the backup on the next tick, unasked.
    """
    try:
        local = scan_local_tree(local_root, strict=True)
    except OSError as exc:
        raise WatchSkipped(f"cannot read {local_root}: {exc}") from exc
    delete_extra = bool(config.get("delete_extra"))
    if delete_extra and not local and remote:
        # An empty folder that used to hold files looks exactly like a mount
        # point whose drive is gone. Mirroring that is never worth guessing.
        raise WatchSkipped(
            f"{local_root} is empty but the remote prefix holds "
            f"{len(remote)} object(s); not deleting them")
    return build_sync_plan(
        local, remote, direction="upload", delete_extra=delete_extra,
        exclude=config.get("exclude") or "")


class WatchMixin:
    """Watching a local folder and pushing what changes."""

    def open_watch(self):
        """Start or stop mirroring a local folder up to this prefix."""
        if self.watch_active():
            self.stop_watch()
            return
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to watch a folder", 2000)
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        prefix = self.data_model.current_folder or ""
        dlg = WatchFolderDialog(
            self, self.data_model.bucket, prefix, self._watch_config)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        config = dlg.config()
        config["bucket"] = self.data_model.bucket
        config["prefix"] = prefix
        self._watch_config = config
        self._save_watch_config()
        self.start_watch(config)

    def watch_active(self) -> bool:
        return self._watch is not None

    def start_watch(self, config):
        """Begin the interval mirror described by *config*."""
        self._watch = dict(config)
        if self._watch_thread is None:
            self._watch_scan_config = dict(config)
        self._watch_problem = ""
        if self._watch_timer is None:
            self._watch_timer = QTimer(self)
            self._watch_timer.timeout.connect(self._watch_tick)
        self._watch_timer.setInterval(
            max(10, int(config.get("interval") or 300)) * 1000)
        self._watch_timer.start()
        self._update_watch_status()
        self.log(
            f"watching {config['local']} -> s3://{config['bucket']}/"
            f"{config['prefix']} every {config['interval']}s")
        self._watch_tick()

    def stop_watch(self):
        if self._watch_timer is not None:
            self._watch_timer.stop()
        self._watch = None
        self._update_watch_status()
        self.log("stopped watching")

    def _update_watch_status(self):
        if not self._watch:
            self.watch_status.hide()
        else:
            problem = getattr(self, "_watch_problem", "")
            name = os.path.basename(self._watch['local'].rstrip(os.sep))
            self.watch_status.setText(
                f"watching {name} — skipped" if problem
                else f"watching {name}")
            self.watch_status.setStyleSheet(
                "color: #c62828;" if problem else "")
            self.watch_status.setToolTip(
                (f"Last pass skipped: {problem}\n" if problem else "")
                + f"{self._watch['local']} -> s3://{self._watch['bucket']}/"
                f"{self._watch['prefix']}  (Tools -> Watch folder to stop)")
            self.watch_status.show()
        if getattr(self, "actWatchFolder", None) is not None:
            self.actWatchFolder.setText(
                "Stop watching folder" if self._watch else "Watch folder…")

    def _watch_tick(self):
        """
        One comparison pass. Skipped while the queue is busy, so a slow upload
        cannot pile a second copy of the same work behind itself.
        """
        if not self._watch or self._watch_thread is not None:
            return
        if self.transfers_active():
            return
        config = dict(self._watch)
        self._watch_scan_config = config
        clone = self.data_model.clone_for_worker()
        clone.bucket = config["bucket"]

        def _scan(_w):
            remote = clone.list_tree(config["prefix"])
            return plan_watch_pass(config["local"], remote, config)

        self._watch_thread = QThread(self)
        self._watch_worker = FuncWorker(_scan)
        self._watch_worker.moveToThread(self._watch_thread)
        self._watch_thread.started.connect(self._watch_worker.run)
        self._watch_worker.done.connect(self._on_watch_planned)
        self._watch_worker.done.connect(self._watch_thread.quit)
        release_worker_on_finish(self._watch_thread, self._watch_worker)
        self._watch_thread.finished.connect(self._watch_thread.deleteLater)
        self._watch_thread.start()

    def _on_watch_planned(self, result, exc):
        thread, self._watch_thread = self._watch_thread, None
        self._watch_worker = None
        join_qthread(thread)
        if not self._watch:
            return
        if self._watch != self._watch_scan_config:
            return
        if exc is not None:
            # Visible, not just logged: a watch that has stopped doing its
            # job must not keep looking like it is doing it.
            self._watch_problem = str(exc)
            self._update_watch_status()
            self.log(f"watch: skipped this pass ({exc})")
            self.statusBar().showMessage(f"Watch skipped: {exc}", 8000)
            return
        if getattr(self, "_watch_problem", ""):
            self._watch_problem = ""
            self._update_watch_status()
        actions = [entry for entry in (result or [])
                   if entry.get("action") != "skip"]
        if not actions:
            return
        self.log(f"watch: {len(actions)} change(s) to send")
        # The plan was compared against the watched bucket, which is not
        # necessarily the one the window is showing by now.
        self.start_sync(actions, self._watch["local"], self._watch["prefix"],
                        "upload", bucket=self._watch["bucket"])

    def _watch_settings_key(self) -> str:
        return self.store.profile_key("watch")

    def _save_watch_config(self):
        with self.store.group():
            self.settings.setValue(
                self._watch_settings_key(), json.dumps(self._watch_config or {}))

    def _load_watch_config(self):
        """
        Remember the last watch settings, but never start one on its own.

        A background uploader that resumes at launch without being asked is
        how a folder gets mirrored somewhere the user forgot about.
        """
        raw = self.store.get(self._watch_settings_key(), "")
        try:
            config = json.loads(raw or "{}")
        except (ValueError, TypeError):
            config = {}
        self._watch_config = config if isinstance(config, dict) else {}
