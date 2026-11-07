"""Editing an object in another application and saving it back.

The object is downloaded into the window's temp workspace, opened with the
desktop's default application, and the file is watched: every save there is
uploaded with an If-Match on the ETag it was opened at, so an object someone
else changed meanwhile is never overwritten without asking.
"""

import os
from datetime import datetime
from PyQt6.QtCore import QFileSystemWatcher, QThread, QTimer, QUrl
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import QMessageBox
from model import Model as DataModel
from utils import FuncWorker, join_qthread, release_worker_on_finish, run_with_progress


def conflict_copy_key(key, when=None) -> str:
    """Where a refused save lands instead: beside the object, never on it."""
    stamp = (when or datetime.now()).strftime("%Y-%m-%d %H%M%S")
    folder, _, name = key.rpartition("/")
    stem, dot, ext = name.rpartition(".")
    if not dot or not stem:
        stem, ext = name, ""
    new_name = f"{stem} (conflict {stamp})" + (f".{ext}" if ext else "")
    return f"{folder}/{new_name}" if folder else new_name


def _stat(path):
    try:
        info = os.stat(path)
    except OSError:
        return None
    return (info.st_size, info.st_mtime_ns)


class ExternalEditMixin:
    """Editing an object in another application and saving it back."""

    # Saves are read into memory and sent in one PUT, the same way the
    # in-app editor saves; past this an editor round trip is the wrong tool.
    EXTERNAL_EDIT_LIMIT = 100 * 1024 * 1024
    # Editors write a file in several steps; one upload per burst.
    EXTERNAL_EDIT_SETTLE_MS = 800

    def _edit_state(self):
        if getattr(self, "_external_edits", None) is None:
            self._external_edits = {}
            self._edit_pending = set()
            self._edit_watcher = QFileSystemWatcher(self)
            self._edit_watcher.fileChanged.connect(self._on_edit_file_changed)
            self._edit_watcher.directoryChanged.connect(self._on_edit_dir_changed)
            self._edit_timer = QTimer(self)
            self._edit_timer.setSingleShot(True)
            self._edit_timer.timeout.connect(self.flush_external_edits)
        return self._external_edits

    def external_edits(self) -> dict:
        return dict(getattr(self, "_external_edits", None) or {})

    def edit_externally(self, key, open_file=True):
        """Download *key*, open it in its default application, watch it."""
        if not key or key.endswith("/"):
            self.statusBar().showMessage("Only a file can be edited", 2000)
            return None
        edits = self._edit_state()
        for path, state in edits.items():
            if state["key"] == key and state["bucket"] == self.data_model.bucket:
                if open_file:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(path))
                return path
        clone = self.data_model.clone_for_worker()
        folder = self.temp_workspace.make(prefix="edit_")
        path = os.path.join(folder, os.path.basename(key) or "object")

        def _fetch(_w):
            head = clone.object_properties(key)
            size = int(head.get("ContentLength") or 0)
            if size > self.EXTERNAL_EDIT_LIMIT:
                raise ValueError(
                    f"{key} is too large to edit this way "
                    f"(the limit is {self.EXTERNAL_EDIT_LIMIT // (1024 * 1024)} MiB).")
            etag = clone.normalize_etag(head.get("ETag"))
            clone.download_file(key, path, folder)
            after = clone.normalize_etag(
                clone.object_properties(key).get("ETag"))
            if after != etag:
                raise RuntimeError(
                    f"{key} changed while it was being downloaded; try again.")
            return etag

        etag, exc = run_with_progress(self, f"Opening {key}…", _fetch)
        if etag is None and exc is None:
            return None
        if exc is not None:
            QMessageBox.warning(self, "Edit in external app", str(exc))
            return None
        read_only = self.is_read_only()
        edits[path] = {"key": key, "bucket": self.data_model.bucket,
                       "etag": etag, "seen": _stat(path), "model": clone,
                       "busy": False, "read_only": read_only}
        if not read_only:
            self._edit_watcher.addPath(path)
            self._edit_watcher.addPath(folder)
        self.log(f"editing {key} externally ({path})"
                 + ("; read-only profile, saves stay local" if read_only else ""))
        self._update_edit_status()
        if open_file:
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))
        return path

    def _on_edit_file_changed(self, path):
        if path in (self._external_edits or {}):
            self._edit_pending.add(path)
            self._edit_timer.start(self.EXTERNAL_EDIT_SETTLE_MS)

    def _on_edit_dir_changed(self, folder):
        """An editor that saves by rename replaces the watched file."""
        for path in self._external_edits or {}:
            if os.path.dirname(path) == folder and os.path.exists(path):
                # The old watch may still be listed for the replaced inode;
                # drop it and watch the file now at that path.
                if path in self._edit_watcher.files():
                    self._edit_watcher.removePath(path)
                self._edit_watcher.addPath(path)
                self._on_edit_file_changed(path)

    def flush_external_edits(self):
        """Upload every watched file that changed since it was last sent."""
        started = 0
        for path in sorted(getattr(self, "_edit_pending", ()) or ()):
            state = (self._external_edits or {}).get(path)
            if state is None or state["read_only"]:
                self._edit_pending.discard(path)
                continue
            if state["busy"]:
                continue
            now = _stat(path)
            if now is None or now == state["seen"]:
                self._edit_pending.discard(path)
                continue
            self._edit_pending.discard(path)
            self._upload_edit(path, state, now, if_match=state["etag"])
            started += 1
        return started

    def _upload_edit(self, path, state, now, if_match, key=None):
        key = key or state["key"]
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError as exc:
            self.log(f"external edit of {key}: could not read {path}: {exc}")
            return
        clone = state["model"]
        state["busy"] = True

        def _put(_w):
            return clone.put_object_body(key, data, if_match=if_match)

        thread = QThread(self)
        worker = FuncWorker(_put)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(
            lambda result, exc, _p=path, _n=now, _k=key, _t=thread:
            self._on_edit_uploaded(_p, _n, _k, result, exc, _t))
        worker.done.connect(thread.quit)
        release_worker_on_finish(thread, worker)
        thread.finished.connect(thread.deleteLater)
        self.__dict__.setdefault("_edit_threads", []).append(thread)
        thread.start()

    def _on_edit_uploaded(self, path, now, key, result, exc, thread):
        self.invalidate_listing_cache()
        threads = self.__dict__.get("_edit_threads", [])
        if thread in threads:
            threads.remove(thread)
        join_qthread(thread)
        state = (self._external_edits or {}).get(path)
        if state is None:
            return
        state["busy"] = False
        if exc is None:
            if key == state["key"]:
                state["etag"] = DataModel.normalize_etag(result)
            state["seen"] = now
            self.log(f"saved {path} back to {state['bucket']}/{key}")
            self.statusBar().showMessage(f"Saved {key}", 3000)
        elif isinstance(exc, DataModel.PreconditionFailed):
            self._resolve_edit_conflict(path, state, now)
        else:
            self.log(f"external edit of {key} not saved: {exc}")
            self.statusBar().showMessage(f"Could not save {key}: {exc}", 8000)
        if path in self._edit_pending:
            self._edit_timer.start(self.EXTERNAL_EDIT_SETTLE_MS)

    def _resolve_edit_conflict(self, path, state, now):
        box = QMessageBox(self)
        box.setWindowTitle("Edit in external app")
        box.setIcon(QMessageBox.Icon.Warning)
        box.setText(f"{state['key']} changed on the server since you opened "
                    "it. Your save was not uploaded.")
        overwrite = box.addButton("Overwrite", QMessageBox.ButtonRole.DestructiveRole)
        copy = box.addButton("Save as a copy", QMessageBox.ButtonRole.AcceptRole)
        box.addButton("Not now", QMessageBox.ButtonRole.RejectRole)
        box.exec()
        clicked = box.clickedButton()
        if clicked is overwrite:
            self._upload_edit(path, state, now, if_match="")
        elif clicked is copy:
            target = conflict_copy_key(state["key"])
            state["key"] = target
            state["etag"] = ""
            self._upload_edit(path, state, now, if_match="", key=target)
        else:
            # These bytes were refused once; only a further save asks again.
            state["seen"] = now
            self.log(f"external edit of {state['key']} kept local only")

    def stop_external_edits(self, ask=True):
        """
        Stop watching. Unsent changes are offered for upload first, because
        the temp workspace is removed with the window.
        """
        edits = getattr(self, "_external_edits", None) or {}
        unsent = [p for p, s in edits.items()
                  if not s["read_only"] and _stat(p) not in (None, s["seen"])]
        if ask and unsent and QMessageBox.question(
                self, "Edit in external app",
                f"{len(unsent)} externally edited file(s) have changes that "
                "were not uploaded yet. Upload them now?") \
                == QMessageBox.StandardButton.Yes:
            for path in unsent:
                state = edits[path]
                key, etag = state["key"], state["etag"]
                with open(path, "rb") as handle:
                    data = handle.read()
                _result, exc = run_with_progress(
                    self, f"Saving {key}…",
                    lambda _w, _m=state["model"], _k=key, _d=data, _e=etag:
                    _m.put_object_body(_k, _d, if_match=_e))
                if exc is not None:
                    QMessageBox.warning(self, "Edit in external app",
                                        f"{key} was not saved: {exc}")
        watcher = getattr(self, "_edit_watcher", None)
        if watcher is not None:
            paths = watcher.files() + watcher.directories()
            if paths:
                watcher.removePaths(paths)
        if getattr(self, "_edit_timer", None) is not None:
            self._edit_timer.stop()
        for thread in list(self.__dict__.get("_edit_threads", [])):
            join_qthread(thread)
        self.__dict__["_edit_threads"] = []
        if edits:
            self.log(f"stopped watching {len(edits)} externally edited file(s)")
        self._external_edits = {}
        self._edit_pending = set()
        self._update_edit_status()

    def _update_edit_status(self):
        action = getattr(self, "actStopEdits", None)
        count = len(getattr(self, "_external_edits", None) or {})
        if action is not None:
            action.setEnabled(bool(count))
            action.setText(f"Stop external edits ({count})" if count
                           else "Stop external edits")
