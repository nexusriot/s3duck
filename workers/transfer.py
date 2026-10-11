"""Every background transfer: upload, download, copy, move, sync and the rest.

One QObject per job, moved onto a QThread by the window. Progress is emitted
on a throttle because a byte-accurate signal per chunk is enough to make the
UI thread the bottleneck.
"""

import functools
import os
import threading
import time
import zipfile
from PyQt6.QtCore import QObject, pyqtSignal, pyqtSlot
from model import ObjectExists, TransferCancelled, run_parallel
from utils import describe_client_error, is_transient_error


PROGRESS_EMIT_INTERVAL_SEC = 0.6   # ~1.6 updates/sec
PROGRESS_MIN_BYTE_DELTA = 1 * 1024 * 1024  # also emit if at least 1MB progressed


def job_runner(label):
    """Wrap one background operation in the contract every one of them had.

    Sixteen operations each spelled this out: set ``cancelled = False``, run
    the work in a try, and in the handler decide whether the exception was a
    cancellation or a real failure -- reporting the failure three ways if it
    was -- then emit ``finished`` from a finally. That is nine identical lines
    per operation, and the one line that actually differed was the label in
    the failure message.

    Spelling it out sixteen times is not only repetition: ``finished`` is what
    releases the worker's thread and re-enables the toolbar, so an operation
    that was added without the finally would hang the UI with no error
    anywhere. Here it cannot be left out.

    Cancellation is deliberately not a failure. The user asked for it, so it
    reports nothing and only tells ``finished`` what happened.

    It is recognised by TYPE. The test used to be the word "cancelled"
    anywhere in str(exc), and that test can only ever err one way: towards
    silence. A delete refused on "cancelled-orders.csv", or a backend whose
    error body says "request cancelled by peer", was reported to the user as
    their own cancellation -- no dialog, no red row, no log line about what
    actually went wrong. The reverse mistake is harmless by comparison, so
    anything that is not a TransferCancelled is now a failure.
    """
    def decorate(operation):
        @functools.wraps(operation)
        def run(self):
            cancelled = False
            try:
                operation(self)
            except Exception as exc:
                msg = str(exc) or exc.__class__.__name__
                if isinstance(exc, TransferCancelled):
                    cancelled = True
                else:
                    self.progress.emit(f"{label} failed: {msg}")
                    self.details.emit(describe_client_error(exc),
                                      is_transient_error(exc))
                    self.error.emit(msg)
            finally:
                self.finished.emit(cancelled)
        return run
    return decorate


class Worker(QObject):
    finished = pyqtSignal(bool)
    progress = pyqtSignal(str)
    batch_progress = pyqtSignal(object, object)

    error = pyqtSignal(str)
    # The same failure with its request id and HTTP status attached — what a
    # provider's support desk asks for, and what str(exc) throws away — plus
    # whether the service was asking for patience rather than saying no.
    details = pyqtSignal(str, bool)

    def __init__(self, data_model, job, dest_model=None, if_none_match=False):
        super().__init__()
        self.data_model = data_model
        self.job = job
        self.dest_model = dest_model
        # Ask the service to refuse an upload whose key is already taken.
        # Set when the user chose to skip existing objects, so that choice
        # survives an object appearing between the check and the write.
        self.if_none_match = bool(if_none_match)
        self._cancel_event = threading.Event()

    @property
    def _file_workers(self) -> int:
        """How many files to move at once (1 keeps ordering deterministic)."""
        return int(getattr(self.data_model, "parallel_files", 1) or 1)

    @pyqtSlot()
    def cancel(self):
        self._cancel_event.set()
        self.progress.emit("cancel requested…")

    @job_runner("download")
    def download(self):
        # total bytes across everything in this batch, including dirs
        total_bytes_all = 0
        for key, local_name, size, folder_path in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")

            if local_name is not None:
                total_bytes_all += int(size or 0)
            else:
                for k, s in self.data_model.get_keys(key, log_fn=self.progress.emit):
                    if self._cancel_event.is_set():
                        raise TransferCancelled("cancelled")
                    if k and not k.endswith("/"):
                        total_bytes_all += int(s or 0)
        total_bytes_all = max(1, int(total_bytes_all))

        done_all = 0
        done_all_lock = threading.Lock()
        throttle_state = {"t": 0.0, "b": 0}

        def emit_throttled(current_total, file_cur, file_total, key):
            now = time.time()
            should_emit = False
            if (now - throttle_state["t"]) >= PROGRESS_EMIT_INTERVAL_SEC:
                should_emit = True
            elif (current_total - throttle_state["b"]) >= PROGRESS_MIN_BYTE_DELTA:
                should_emit = True
            elif current_total >= total_bytes_all:
                should_emit = True

            if should_emit:
                throttle_state["t"] = now
                throttle_state["b"] = current_total
                self.batch_progress.emit(int(current_total), int(total_bytes_all))

        def make_cb():
            last_sent_per_key = {}

            def _cb(total_file, cur_file, key):
                nonlocal done_all
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                key = str(key or "")
                with done_all_lock:
                    prev = int(last_sent_per_key.get(key, 0))
                    cur = int(cur_file)
                    if cur > prev:
                        delta = cur - prev
                        last_sent_per_key[key] = cur
                        done_all += delta
                    current_total = done_all

                emit_throttled(current_total, int(cur_file), int(total_file or 1), key)

            return _cb

        def _download_one(item):
            key, local_name, size, folder_path = item
            if local_name:
                msg = "downloading %s -> %s (%s)" % (key, local_name, size)
            else:
                msg = "downloading directory: %s -> %s" % (key, folder_path)
            self.progress.emit(msg)
            self.data_model.download_file(
                key, local_name, folder_path,
                progress_cb=make_cb(),
                cancel_event=self._cancel_event,
                log_fn=self.progress.emit,
            )

        run_parallel(self.job, _download_one, self._file_workers,
                     cancel_event=self._cancel_event)

        self.batch_progress.emit(int(done_all), int(total_bytes_all))

    @job_runner("delete")
    def delete(self):
        for key in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            msg = "moving %s -> /dev/null" % key
            self.progress.emit(msg)
            self.data_model.delete(key, log_fn=self.progress.emit)

    @job_runner("upload")
    def upload(self):
        total_bytes_all = 0
        for key, local_name in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            if local_name:
                try:
                    total_bytes_all += int(os.path.getsize(local_name))
                except Exception:
                    pass
        total_bytes_all = max(1, int(total_bytes_all))

        done_all = 0
        done_all_lock = threading.Lock()
        throttle_state = {"t": 0.0, "b": 0}

        def emit_throttled(current_total, file_cur, file_total, key):
            now = time.time()
            should_emit = False
            if (now - throttle_state["t"]) >= PROGRESS_EMIT_INTERVAL_SEC:
                should_emit = True
            elif (current_total - throttle_state["b"]) >= PROGRESS_MIN_BYTE_DELTA:
                should_emit = True
            elif current_total >= total_bytes_all:
                should_emit = True

            if should_emit:
                throttle_state["t"] = now
                throttle_state["b"] = current_total
                self.batch_progress.emit(int(current_total), int(total_bytes_all))

        def make_cb():
            last_sent_per_key = {}

            def _cb(total_file, cur_file, key):
                nonlocal done_all
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                key = str(key or "")
                with done_all_lock:
                    prev = int(last_sent_per_key.get(key, 0))
                    cur = int(cur_file)
                    if cur > prev:
                        delta = cur - prev
                        last_sent_per_key[key] = cur
                        done_all += delta
                    current_total = done_all

                emit_throttled(current_total, int(cur_file), int(total_file or 1), key)

            return _cb

        skipped = []
        skipped_lock = threading.Lock()

        def _upload_one(item):
            key, local_name = item
            if local_name is not None:
                msg = "uploading %s -> %s" % (local_name, key)
            else:
                msg = "creating folder %s" % key
            self.progress.emit(msg)
            callback = make_cb() if local_name else None
            try:
                self.data_model.upload_file(
                    local_name, key,
                    progress_cb=callback,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                    if_none_match=self.if_none_match,
                )
            except ObjectExists:
                # The key was taken between the destination check and the
                # write. Skipping is what the user asked for, so this is
                # an outcome to report, not a failure to abort the batch
                # over.
                with skipped_lock:
                    skipped.append(key)
                self.progress.emit(
                    f"skipped {key}: it already exists")
                # Its bytes went into the batch total, so without this the
                # bar stops short of the end and a job that did exactly
                # what was asked looks like one that died. The callback
                # credits the difference, which matters because a
                # conditional write is refused only after the body has
                # partly gone out and been counted already.
                if callback is not None:
                    try:
                        size = int(os.path.getsize(local_name))
                        callback(max(1, size), size, key)
                    except OSError:
                        pass

        run_parallel(self.job, _upload_one, self._file_workers,
                     cancel_event=self._cancel_event)

        if skipped:
            self.progress.emit(
                f"skipped {len(skipped)} object(s) that appeared at the "
                f"destination during the upload")
        self.batch_progress.emit(int(done_all), int(total_bytes_all))

    @job_runner("sync to profile")
    def sync_to_profile(self):
        # job = [(operation, src_key, dst_key)] against self.dest_model
        if self.dest_model is None:
            raise Exception("no destination profile for this sync")
        for operation, src_key, dst_key in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            if operation == "copy":
                self.progress.emit(
                    f"copying {src_key} -> {self.dest_model.bucket}/{dst_key}")
                self.data_model.copy_to_model(
                    src_key, self.dest_model, dst_key,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit)
            elif operation == "delete":
                self.progress.emit(
                    f"deleting {self.dest_model.bucket}/{dst_key}")
                self.dest_model.delete(
                    dst_key, log_fn=self.progress.emit,
                    cancel_event=self._cancel_event)

    @job_runner("copy to profile")
    def copy_to_profile(self):
        # job = [(src_key, dst_key, is_folder)] into self.dest_model
        if self.dest_model is None:
            raise Exception("no destination profile for this copy")
        where = f"{self.dest_model.bucket}"
        for src_key, dst_key, is_folder in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            if is_folder:
                self.progress.emit(
                    f"copying folder {src_key} -> {where}/{dst_key}")
                self.data_model.copy_prefix_to_model(
                    src_key, self.dest_model, dst_key,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit)
            else:
                self.progress.emit(f"copying {src_key} -> {where}/{dst_key}")
                self.data_model.copy_to_model(
                    src_key, self.dest_model, dst_key,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit)

    @job_runner("copy")
    def copy(self):
        # job = [(src_key, dst_key, is_folder, dst_bucket_or_None)]
        for src_key, dst_key, is_folder, dst_bucket in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            where = f" in {dst_bucket}" if dst_bucket else ""
            if is_folder:
                self.progress.emit(
                    f"copying folder {src_key} -> {dst_key}{where}")
                self.data_model.copy_prefix(
                    src_key, dst_key, dst_bucket=dst_bucket,
                    log_fn=self.progress.emit,
                    cancel_event=self._cancel_event,
                )
            else:
                self.progress.emit(f"copying {src_key} -> {dst_key}{where}")
                self.data_model.copy_object(
                    src_key, dst_key, dst_bucket=dst_bucket,
                    log_fn=self.progress.emit)

    @job_runner("move")
    def move(self):
        # job = [(src_key, dst_key, is_folder, dst_bucket_or_None)]
        for src_key, dst_key, is_folder, dst_bucket in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            where = f" in {dst_bucket}" if dst_bucket else ""
            if is_folder:
                self.progress.emit(
                    f"moving folder {src_key} -> {dst_key}{where}")
                self.data_model.copy_prefix(
                    src_key, dst_key, dst_bucket=dst_bucket,
                    log_fn=self.progress.emit,
                    cancel_event=self._cancel_event,
                )
            else:
                self.progress.emit(f"moving {src_key} -> {dst_key}{where}")
                self.data_model.copy_object(
                    src_key, dst_key, dst_bucket=dst_bucket,
                    log_fn=self.progress.emit)
        # Sources are removed only after every copy succeeded, so a failure
        # mid-way never leaves the data deleted-but-not-copied.
        for src_key, _dst_key, _is_folder, _dst_bucket in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            self.progress.emit(f"removing {src_key}")
            self.data_model.delete(src_key, log_fn=self.progress.emit)

    @job_runner("bucket delete")
    def delete_buckets(self):
        # job = [(bucket_name, recursive)]
        failures = []
        for bucket_name, recursive in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            try:
                if recursive:
                    self.progress.emit(f"deleting bucket {bucket_name} (recursive)")
                    self.data_model.delete_bucket_recursive(
                        bucket_name,
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit,
                    )
                else:
                    self.progress.emit(f"deleting bucket {bucket_name}")
                    self.data_model.delete_bucket(bucket_name)
                self.progress.emit(f"deleted bucket {bucket_name}")
            except TransferCancelled:
                raise
            except Exception as exc:
                # One bad bucket must not abandon the rest of the batch.
                failures.append(f"{bucket_name}: {exc}")
                self.progress.emit(f"delete failed for {bucket_name}: {exc}")
        if failures:
            self.error.emit(
                "Some buckets could not be deleted:\n\n" + "\n".join(failures)
            )

    @job_runner("sync")
    def sync(self):
        """
        job = [(action, rel, local_path, key, size)] — already filtered to the
        actions the user approved in the dry-run plan.
        """
        total_bytes_all = max(1, sum(
            int(size or 0) for action, _rel, _lp, _k, size in self.job
            if action in ("upload", "download")
        ))
        done_all = 0
        done_lock = threading.Lock()
        throttle = {"t": 0.0, "b": 0}

        def _emit(current):
            now = time.time()
            if ((now - throttle["t"]) >= PROGRESS_EMIT_INTERVAL_SEC
                    or (current - throttle["b"]) >= PROGRESS_MIN_BYTE_DELTA
                    or current >= total_bytes_all):
                throttle["t"] = now
                throttle["b"] = current
                self.batch_progress.emit(int(current), int(total_bytes_all))

        def _make_cb():
            seen = {}

            def _cb(_total_file, cur_file, key):
                nonlocal done_all
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                key = str(key or "")
                with done_lock:
                    prev = int(seen.get(key, 0))
                    cur = int(cur_file)
                    if cur > prev:
                        seen[key] = cur
                        done_all += cur - prev
                    current = done_all
                _emit(current)

            return _cb

        def _sync_one(item):
            action, rel, local_path, key, _size = item
            if action == "upload":
                self.progress.emit(f"sync upload {rel}")
                self.data_model.upload_file(
                    local_path, key, progress_cb=_make_cb(),
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )
            elif action == "download":
                self.progress.emit(f"sync download {rel}")
                parent = os.path.dirname(local_path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                self.data_model.download_file(
                    key, local_path, parent, progress_cb=_make_cb(),
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )
            elif action == "delete_remote":
                self.progress.emit(f"sync delete remote {rel}")
                self.data_model.delete(key, log_fn=self.progress.emit)
            elif action == "delete_local":
                self.progress.emit(f"sync delete local {rel}")
                try:
                    os.remove(local_path)
                except OSError as exc:
                    self.progress.emit(f"could not delete {local_path}: {exc}")

        run_parallel(self.job, _sync_one, self._file_workers,
                     cancel_event=self._cancel_event)

        self.batch_progress.emit(int(done_all), int(total_bytes_all))

    @job_runner("tagging")
    def set_tags(self):
        # job = [(key, is_folder, add_dict, remove_list, replace_bool)]
        changed = 0
        for key, is_folder, add, remove, replace in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            for k in self._iter_object_keys(key, is_folder):
                self.data_model.update_object_tags(
                    k, add=add, remove=remove, replace=replace)
                changed += 1
                self.progress.emit(f"tagged {k}")
        self.progress.emit(f"tagging done: {changed} object(s)")

    @job_runner("zip download")
    def zip_download(self):
        """
        job = [(zip_path, base_prefix, key, is_folder)] — every row carries the
        same archive path and base prefix. Objects stream straight into the zip
        so nothing is staged on disk twice.
        """
        archive = None
        complete = False
        try:
            zip_path = self.job[0][0]
            base_prefix = self.job[0][1]

            # Expand folders and total the bytes before opening the archive.
            entries = []
            total_bytes_all = 0
            for _zp, _bp, key, is_folder in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if is_folder:
                    for k, size in self.data_model.get_keys(
                            key, log_fn=self.progress.emit):
                        if not k or k.endswith("/"):
                            continue
                        entries.append((k, int(size or 0)))
                        total_bytes_all += int(size or 0)
                else:
                    size = self.data_model.get_size(key)
                    entries.append((key, int(size or 0)))
                    total_bytes_all += int(size or 0)
            total_bytes_all = max(1, total_bytes_all)

            done = 0
            throttle = {"t": 0.0, "b": 0}

            def _emit(current):
                now = time.time()
                if ((now - throttle["t"]) >= PROGRESS_EMIT_INTERVAL_SEC
                        or (current - throttle["b"]) >= PROGRESS_MIN_BYTE_DELTA
                        or current >= total_bytes_all):
                    throttle["t"] = now
                    throttle["b"] = current
                    self.batch_progress.emit(int(current), int(total_bytes_all))

            archive = zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
            for key, _size in entries:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                arcname = key[len(base_prefix):] if key.startswith(base_prefix) else key
                arcname = arcname.lstrip("/") or os.path.basename(key)
                self.progress.emit(f"archiving {key}")
                with archive.open(arcname, "w") as target:
                    for block in self.data_model.stream_object(
                            key, cancel_event=self._cancel_event):
                        target.write(block)
                        done += len(block)
                        _emit(done)
            self.batch_progress.emit(int(done), int(total_bytes_all))
            self.progress.emit(f"wrote {len(entries)} object(s) to {zip_path}")
            complete = True
        finally:
            if archive is not None:
                try:
                    archive.close()
                except Exception:
                    pass
            # A cancelled or failed archive is unusable; do not leave it
            # behind. A half-written zip that opens and is simply missing its
            # tail is worse than no zip at all, so the failure path removes it
            # too — it used to keep one and report an error beside it.
            # `complete` rather than the old `cancelled or failed`: the
            # decorator owns the handler now, and "the body did not finish"
            # is the same condition said from the inside.
            if not complete:
                try:
                    os.remove(self.job[0][0])
                except OSError:
                    pass

    @job_runner("undo")
    def undelete(self):
        # job = [(key,)]
        restored = 0
        for (key,) in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            self.progress.emit(f"restoring {key}")
            restored += self.data_model.undelete(key, log_fn=self.progress.emit)
        if restored:
            self.progress.emit(f"restored {restored} object(s)")
        else:
            self.progress.emit(
                "nothing to restore — the bucket has no delete markers "
                "(versioning was not enabled, so the delete was permanent)"
            )

    @job_runner("empty bucket")
    def empty_buckets(self):
        # job = [(bucket_name,)]
        failures = []
        for (bucket_name,) in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            try:
                self.progress.emit(f"emptying bucket {bucket_name}")
                self.data_model.empty_bucket(
                    bucket_name,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )
                self.progress.emit(f"emptied bucket {bucket_name}")
            except TransferCancelled:
                raise
            except Exception as exc:
                failures.append(f"{bucket_name}: {exc}")
                self.progress.emit(f"emptying failed for {bucket_name}: {exc}")
        if failures:
            self.error.emit(
                "Some buckets could not be emptied:\n\n" + "\n".join(failures)
            )

    def _iter_object_keys(self, key, is_folder):
        """Yield each concrete object key for a job target (recursing folders)."""
        if is_folder:
            for k, _ in self.data_model.get_keys(key, log_fn=self.progress.emit):
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if k and not k.endswith("/"):
                    yield k
        else:
            yield key

    @job_runner("storage-class change")
    def set_storage_class(self):
        # job = [(key, is_folder, storage_class)]
        changed = 0
        unchanged = 0
        failures = []
        for key, is_folder, storage_class in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            for k in self._iter_object_keys(key, is_folder):
                self.progress.emit(f"storage-class {storage_class}: {k}")
                try:
                    if self.data_model.change_storage_class(
                            k, storage_class,
                            log_fn=self.progress.emit):
                        changed += 1
                    else:
                        unchanged += 1
                except TransferCancelled:
                    raise
                except Exception as exc:
                    # One object the service will not move must not
                    # abandon the rest of the selection — the same
                    # bargain restore and delete_buckets already make.
                    # A backend that rejects the class outright (MinIO
                    # answers InvalidStorageClass) would otherwise fail
                    # the whole batch on its first object.
                    failures.append(f"{k}: {exc}")
                    self.progress.emit(
                        f"storage-class failed for {k}: {exc}")
        self.progress.emit(
            f"storage class: {changed} changed, {unchanged} already "
            f"there, {len(failures)} failed")
        if failures:
            self.error.emit(
                f"{len(failures)} object(s) could not be changed:\n\n"
                + "\n".join(failures[:10]))

    @job_runner("content-type fix")
    def set_content_type(self):
        # job = [(key, is_folder)]
        fixed = 0
        skipped = 0
        for key, is_folder in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            for k in self._iter_object_keys(key, is_folder):
                changed, content_type = self.data_model.retype_object(
                    k, log_fn=self.progress.emit)
                if changed:
                    fixed += 1
                    self.progress.emit(f"content-type {content_type}: {k}")
                else:
                    skipped += 1
        self.progress.emit(
            f"content-type done: {fixed} updated, {skipped} unchanged")

    @job_runner("restore")
    def restore(self):
        # job = [(key, is_folder, days, tier)]
        initiated = 0
        skipped = 0
        for key, is_folder, days, tier in self.job:
            if self._cancel_event.is_set():
                raise TransferCancelled("cancelled")
            for k in self._iter_object_keys(key, is_folder):
                ok, reason = self.data_model.restore_object(
                    k, days=days, tier=tier
                )
                if ok:
                    initiated += 1
                    self.progress.emit(f"restore initiated: {k}")
                else:
                    skipped += 1
                    self.progress.emit(f"restore skipped ({reason}): {k}")
        self.progress.emit(
            f"restore done: {initiated} initiated, {skipped} skipped"
        )
