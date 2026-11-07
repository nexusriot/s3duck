"""Two-way sync between a local folder and a prefix: scan, run, remember."""

import os
from PyQt6.QtCore import QThread
from core.sync_plan import local_form
from core.two_way import (
    CONFLICT, build_state, load_state, md5_matches, plan_two_way, save_state,
    state_path,
)
from utils import (
    FuncWorker, join_qthread, release_worker_on_finish, scan_local_tree,
)


def remote_digest_tree(model, prefix, cancel_event=None):
    """
    ``({rel: (size, etag)}, skipped)`` for the objects under *prefix*.

    A key whose local name would differ from its rel ("a//b", "a\\\\b") is
    left out and named: two-way sync matches paths by name in both
    directions, and such a key has no single name on the local side.
    """
    tree, skipped = {}, []
    for key, size, etag, _modified in model.list_object_digests(
            prefix, cancel_event=cancel_event):
        rel = key[len(prefix):] if key.startswith(prefix) else key
        if not rel:
            continue
        if local_form(rel) != rel:
            skipped.append(rel)
            continue
        tree[rel] = (int(size or 0), str(etag or ""))
    return tree, skipped


def plan_two_way_pass(model, local_dir, prefix, state_file, exclude=None,
                      cancel_event=None) -> dict:
    """Everything the dialog needs: the plan, the state it was made against."""
    local = scan_local_tree(local_dir, strict=True)
    remote, skipped = remote_digest_tree(model, prefix, cancel_event)
    state = load_state(state_file)

    def identical(rel):
        return md5_matches(os.path.join(local_dir, *rel.split("/")),
                           remote[rel][1])

    plan = plan_two_way(local, remote, state, exclude=exclude,
                        identical=identical)
    return {"plan": plan, "state": state, "skipped": skipped}


class TwoWaySyncMixin:
    """Two-way sync between a local folder and a prefix: scan, run, remember."""

    def two_way_state_dir(self) -> str:
        override = getattr(self, "TWO_WAY_STATE_DIR", "")
        return override or os.path.join(
            os.path.expanduser("~"), ".config", "s3duck", "two_way")

    def two_way_state_file(self, local_dir, prefix, bucket=None) -> str:
        return state_path(self.two_way_state_dir(),
                          self.data_model.profile_endpoint_url,
                          bucket or self.data_model.bucket, prefix, local_dir)

    def start_two_way_sync(self, actions, local_dir, prefix, state, bucket=""):
        """
        Queue a two-way plan, and record the new state after success.

        Conflicts are not run; their old state is kept, so they are still
        conflicts next time instead of quietly becoming a one-way copy.
        """
        bucket = bucket or self.data_model.bucket
        tracked = {a["rel"] for a in actions}
        conflicts = {a["rel"] for a in actions if a.get("action") == CONFLICT}
        keep_old = conflicts | (set(state or {}) - tracked)
        runnable = [a for a in actions if a.get("action") not in (CONFLICT, "skip")]
        state_file = self.two_way_state_file(local_dir, prefix, bucket)
        def finish(completed=None):
            if completed is not None and completed.status != "done":
                # Equal size does not prove that a failed upload/download
                # synchronized the bytes. Keep the previous baseline so the
                # next pass still sees unsent edits and deletion conflicts.
                self.log("two-way sync: incomplete run; previous state kept")
                return
            self._record_two_way_state(
                local_dir, prefix, bucket, state_file, keep_old, state,
                tracked=tracked)
        entry = None
        if runnable:
            entry = self.start_sync(runnable, local_dir, prefix, "both",
                                    bucket=bucket)
        if not runnable:
            finish()
        elif entry is not None:
            entry.on_finished = finish
        return entry

    def _record_two_way_state(self, local_dir, prefix, bucket, state_file,
                              conflicts, old_state, tracked=None):
        """Rescan both sides off the UI thread and write what they agree on."""
        clone = self.data_model.clone_for_worker()
        clone.bucket = bucket

        def _scan(_w):
            local = scan_local_tree(local_dir, strict=True)
            remote, _skipped = remote_digest_tree(clone, prefix)
            keep_old = set(conflicts) & (set(local) | set(remote))
            if tracked is not None:
                # Excluded paths and files that appeared after the preview
                # have not been compared or transferred by this run.
                local = {rel: value for rel, value in local.items() if rel in tracked}
                remote = {rel: value for rel, value in remote.items() if rel in tracked}
            paths = build_state(local, remote, keep_old=keep_old,
                                old_state=old_state)
            save_state(state_file, paths, meta={
                "bucket": bucket, "prefix": prefix, "local": local_dir})
            return len(paths)

        thread = QThread(self)
        worker = FuncWorker(_scan)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(lambda result, exc, _t=thread:
                            self._on_two_way_recorded(result, exc, _t))
        worker.done.connect(thread.quit)
        release_worker_on_finish(thread, worker)
        thread.finished.connect(thread.deleteLater)
        self.__dict__.setdefault("_two_way_threads", []).append(thread)
        thread.start()
        return thread

    def _on_two_way_recorded(self, result, exc, thread):
        threads = self.__dict__.get("_two_way_threads", [])
        if thread in threads:
            threads.remove(thread)
        join_qthread(thread)
        if exc is not None:
            # Not fatal: the next run sees no shared history for these paths
            # and asks, rather than deleting anything.
            self.log(f"two-way sync: could not record the new state: {exc}")
            return
        self.log(f"two-way sync: {result} path(s) recorded as in step")

    def join_two_way_threads(self):
        for thread in list(self.__dict__.get("_two_way_threads", [])):
            join_qthread(thread)
        self.__dict__["_two_way_threads"] = []
