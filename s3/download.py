"""Getting bytes out, and proving they arrived intact.
"""

import botocore.exceptions
import errno
import hashlib
import json
import os
import socket
import ssl
import threading
from core.errors import DownloadIncomplete, TransferCancelled
from core.integrity import etag_is_opaque
from s3.common import (
    CHECKSUM_DIGESTS, _BotoProgressAdapter, plan_prefix_download,
    precondition_failed, prefix_of, require_safe_local_destination,
    run_parallel,
)


class ObjectChangedDuringDownload(Exception):
    """The object was replaced while its ranges were being fetched."""


# OSError is also what a dropped connection or a timeout is: the builtin
# ConnectionError and TimeoutError derive from it, and so do botocore's
# connect and read timeouts, through the requests exceptions they inherit.
# None of those is about one key's local name -- the next file would meet
# them too -- so they still end the job, as themselves, where the transient
# retry can recognise them.
_NOT_A_LOCAL_WRITE = (ConnectionError, TimeoutError, socket.gaierror,
                      ssl.SSLError, botocore.exceptions.BotoCoreError)
# A full or read-only disk refuses every file after this one too. Carrying
# on would fetch each remaining object only to throw its bytes away.
_DISK_WIDE_ERRNOS = {code for code in (errno.ENOSPC, errno.EROFS,
                                       getattr(errno, "EDQUOT", None))
                     if code is not None}


def _is_per_file_write_error(exc) -> bool:
    """True for a local write refused because of THIS file and nothing else."""
    return (isinstance(exc, OSError)
            and not isinstance(exc, _NOT_A_LOCAL_WRITE)
            and exc.errno not in _DISK_WIDE_ERRNOS)


class DownloadMixin:
    """Getting bytes out, and proving they arrived intact.
    """

    @staticmethod
    def _discard_files(*paths):
        """Remove whatever of *paths* exists; a file already gone is fine."""
        for path in paths:
            try:
                os.remove(path)
            except OSError:
                pass

    @staticmethod
    def _check_ranged_reply(key, resp, start, end, data):
        """
        Refuse a ranged reply that is not the range that was asked for.

        The bytes are written at *start* whatever they are. A proxy or
        backend that ignores Range and answers with the whole object, or a
        reply that is short, was therefore written as if it were the chunk —
        a 4 KiB object came out as a 7 KiB file — and the result reported as
        a finished download.
        """
        wanted = end - start + 1
        if len(data) != wanted:
            raise Exception(
                f"Ranged read of '{key}' returned {len(data)} bytes for "
                f"bytes={start}-{end} ({wanted} expected); the server did "
                "not honour the range, so nothing was written")
        content_range = str((resp or {}).get("ContentRange") or "")
        if content_range:
            # "bytes 0-1023/4096"
            spec = content_range.split(" ", 1)[-1].split("/", 1)[0]
            first = spec.split("-", 1)[0].strip()
            if first.isdigit() and int(first) != start:
                raise Exception(
                    f"Ranged read of '{key}' returned {content_range!r} for "
                    f"bytes={start}-{end}; nothing was written")

    @staticmethod
    def _skip_range_response_checksum(params=None, context=None, **_kwargs):
        """
        Take response-checksum validation off a ranged GET.

        botocore 1.36 and newer put ChecksumMode=ENABLED on every GetObject
        and then compare the body they receive against the checksum the
        service returned. AWS omits that header on a partial response — a
        whole-object digest cannot describe a slice of the object — but a
        backend that returns it anyway makes botocore compare the slice
        against the whole and raise FlexibleChecksumError every time. Every
        ranged read then fails, which is every download past the resume
        threshold: on such a backend no large file could be fetched at all.

        Only the ranged reads are narrowed. A whole-object GET is still
        validated, because there the comparison means something.
        """
        headers = (params or {}).get("headers") or {}
        if not any(str(name).lower() == "range" for name in headers):
            return
        checksum = (context or {}).get("checksum")
        if checksum:
            checksum.pop("response_algorithms", None)

    def download_file(self, key: str, local_name: str, folder_path: str,
                      progress_cb=None, cancel_event=None, log_fn=None):
        """
        Download a single file or a whole prefix.
        - If local_name is truthy: single object -> local_name
        - Else: prefix -> recreate directory tree under folder_path/<basename(prefix)>/
        Supports cancellation via cancel_event (threading.Event).
        Automatically retries once per file on region/endpoint errors.
        """

        def _download_one(bucket, k, out_path, size):
            """Download a single object, retrying once on region errors."""
            def _do():
                require_safe_local_destination(
                    folder_path or os.path.dirname(out_path), out_path)
                self.client.download_file(
                    bucket,
                    k,
                    out_path,
                    Callback=_BotoProgressAdapter(
                        size, k, progress_cb, cancel_event=cancel_event,
                        limiter=self.rate_limiter),
                    Config=self.transfer_cfg_download,
                )
            try:
                _do()
            except TransferCancelled:
                raise
            except Exception as exc:
                if not self._is_region_error(exc):
                    raise
                if log_fn:
                    log_fn(f"Region error downloading '{k}': {exc}")
                self.rebind_bucket(log_fn=log_fn)
                _do()

        if not local_name:
            entries = []
            for k, size in self.get_keys(prefix_of(key), log_fn=log_fn):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                entries.append((k, size))

            plan = plan_prefix_download(key, folder_path, entries)
            if os.path.abspath(plan.base_dir) != os.path.abspath(folder_path):
                require_safe_local_destination(folder_path, plan.base_dir)
            os.makedirs(plan.base_dir, exist_ok=True)
            # Everything that will not be on disk at the end, and why: the
            # plan's refusals now, the filesystem's own as they happen.
            failed = [(k, plan.reasons[k]) for k in plan.unsafe]
            failed_lock = threading.Lock()
            if log_fn:
                for k, why in failed:
                    log_fn(f"skipped unsafe key {k}: {why}")

            def _record_failure(name, path, exc):
                if log_fn:
                    log_fn(f"could not write {name} to {path}: {exc}")
                with failed_lock:
                    failed.append((name, str(exc)))

            for out_path in plan.dirs:
                # Re-checked here, not just in the plan: the check is only
                # as old as the last listing, and a symlink can appear under
                # the root in between.
                require_safe_local_destination(folder_path, out_path)
                try:
                    os.makedirs(out_path, exist_ok=True)
                except OSError as exc:
                    # A local FILE already in the way, from an earlier
                    # download. An empty folder is not worth the rest.
                    if not _is_per_file_write_error(exc):
                        raise
                    rel = os.path.relpath(out_path, plan.base_dir)
                    _record_failure(
                        prefix_of(key) + rel.replace(os.sep, "/") + "/",
                        out_path, exc)

            def _fetch_one(item):
                k, out_path, size = item
                # The plan rules out what it can see coming; this is for what
                # it cannot -- a local folder or a read-only file already
                # where this object goes, a case-only clash on macOS, a name
                # an NTFS or FAT drive mounted on Linux refuses. Each costs
                # its own object. The parent folder is made by
                # _download_object, so a clash there lands here too.
                try:
                    self._download_object(
                        k, out_path, size=size, progress_cb=progress_cb,
                        cancel_event=cancel_event, log_fn=log_fn,
                        fetch=_download_one, local_root=folder_path)
                except OSError as exc:
                    if not _is_per_file_write_error(exc):
                        raise
                    _record_failure(k, out_path, exc)

            # Whole-prefix downloads are one job entry, so without this the
            # parallel-files setting would do nothing for "download a folder".
            # Cancellation and every other error still stop it at once.
            run_parallel(plan.files, _fetch_one, self.parallel_files,
                         cancel_event=cancel_event)
            if failed:
                raise DownloadIncomplete(
                    failed, total=len(plan.files) + len(plan.unsafe))
            return

        # A whole-prefix download is filtered by plan_prefix_download above;
        # a single object arrives with its destination already chosen, so the
        # same containment rule is enforced here rather than trusted to every
        # caller that builds one.
        if folder_path:
            require_safe_local_destination(folder_path, local_name)

        self._download_object(key, local_name, progress_cb=progress_cb,
                              cancel_event=cancel_event, log_fn=log_fn,
                              fetch=_download_one, local_root=folder_path)

    def _download_object(self, key, out_path, size=None, progress_cb=None,
                         cancel_event=None, log_fn=None, fetch=None,
                         local_root=None):
        """
        Download one object the way every download should behave: resumable
        when it is large, verified when verification is on.

        Both callers go through here. They used to not: a single object got
        the ranged path and the digest check, while each file of a whole
        folder went straight to the managed transfer — so the setting that
        exists to catch a corrupt download did nothing for a folder, and the
        large file most likely to be interrupted was the one that could not
        resume.

        *size*, when a listing already supplied it, is what decides whether
        the object is large. The HEAD that fetches the stored digest is only
        paid for when something will actually use it.
        """
        etag = ""
        local_root = local_root or os.path.dirname(out_path) or "."
        require_safe_local_destination(local_root, out_path)
        head = None
        ranged = size is None or int(size) >= int(self.resume_threshold)

        if ranged or self.verify_downloads:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            for attempt in (1, 2):
                try:
                    head = self.head_with_checksum(key)
                    size = head.get("ContentLength")
                    etag = self.normalize_etag(head.get("ETag"))
                    break
                except TransferCancelled:
                    raise
                except Exception as exc:
                    if attempt == 2 or not self._is_region_error(exc):
                        break  # non-fatal; proceed without size
                    if log_fn:
                        log_fn(f"Region error on head_object for '{key}': {exc}")
                    self.rebind_bucket(log_fn=log_fn)

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        # ensure parent dir exists (cheap safety)
        try:
            parent = os.path.dirname(out_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        except Exception:
            pass

        # Large downloads take the ranged path so an interruption can resume
        # instead of starting over; small ones stay on the managed transfer.
        # Only with something to pin the ranges to, though: an ETag (sent as
        # IfMatch) or a version id. A HEAD that failed leaves neither, and an
        # unpinned ranged download could splice two versions together — the
        # managed transfer, which does its own HEAD and pins its own ranges,
        # is the safe route then.
        version_id = (head or {}).get("VersionId")
        if (size is not None and int(size) >= int(self.resume_threshold)
                and (etag or version_id not in (None, "", "null"))):
            self._download_ranged(
                key, out_path, size, etag, progress_cb=progress_cb,
                cancel_event=cancel_event, log_fn=log_fn, head=head,
                local_root=local_root,
            )
            return

        # With verification on, the bytes land beside the destination and are
        # only moved into place once they are proven: a failed or interrupted
        # download must not replace a good local file with a bad one.
        staged = (self._resume_paths(out_path)[0] if self.verify_downloads
                  else out_path)
        require_safe_local_destination(local_root, staged)
        try:
            fetch(self.bucket, key, staged, size)

            # Verified while it is still the staged file. A mismatch found
            # after the file is under its final name leaves the bad bytes
            # there, where the next sync — same size, newer mtime — calls
            # them up to date and never repairs them.
            if self.verify_downloads and (etag or head):
                if not self.verify_download(staged, etag, head=head,
                                            log_fn=log_fn, key=key):
                    raise Exception(
                        f"Checksum mismatch after downloading '{key}'; the "
                        "local file did not match the object's stored digest "
                        "and was discarded"
                    )
        except BaseException:
            # Nothing resumes this path, so a staged file left behind is junk
            # next to the user's file -- and a stale one a later ranged
            # attempt would find in place of its own partial download.
            if staged != out_path:
                self._discard_files(staged)
            raise
        if staged != out_path:
            require_safe_local_destination(local_root, out_path)
            os.replace(staged, out_path)

    @staticmethod
    def _digest_range(handle, length, algorithm) -> tuple:
        """
        Digest ``length`` bytes from the open handle.

        Returns ``(base64_checksum, md5_bytes)`` — the caller needs the raw
        MD5 to rebuild a composite ETag and the base64 form to compare an S3
        checksum, and reading the range twice for that would double the I/O.
        """
        name = str(algorithm or "").upper()
        md5 = hashlib.md5()
        factory = CHECKSUM_DIGESTS.get(name)
        digest = factory() if factory is not None else None
        remaining = int(length)
        while remaining > 0:
            block = handle.read(min(1024 * 1024, remaining))
            if not block:
                break
            remaining -= len(block)
            md5.update(block)
            if digest is not None:
                digest.update(block)
        checksum = digest.b64() if digest is not None else ""
        return checksum, md5.digest()

    def verify_download(self, path, etag, head=None, log_fn=None,
                        key="") -> bool:
        """
        Check a downloaded file against the object's stored digest.

        Prefers an additional checksum, which — unlike an ETag — stays
        comparable for multipart objects when it is a full-object one. A
        per-part checksum or a multipart ETag is settled by asking the service
        for the object's real part boundaries; only when that is unavailable
        too does this report "not comparable" and pass.
        """
        algorithm, expected = self.stored_checksum(head)
        if algorithm and algorithm not in CHECKSUM_DIGESTS:
            # Stored with an algorithm this build cannot reproduce. Falling
            # through to the ETag compares something real; pretending the
            # checksum matched would not.
            if log_fn:
                log_fn(f"checksum skipped ({algorithm} needs the awscrt "
                       f"module): {path}")
            algorithm, expected = "", ""
        # Under SSE-KMS, DSSE-KMS and SSE-C the ETag is not a digest of the
        # bytes at all, single-part or multipart. Comparing it reported every
        # intact download of such an object as corrupt, so it takes no part
        # in the verdict; a stored checksum still does.
        opaque = etag_is_opaque(head)
        comparable_etag = "" if opaque else etag
        if algorithm and expected:
            if self.checksum_is_composite(expected):
                verdict = self._verify_by_parts(
                    path, key, comparable_etag, algorithm, log_fn=log_fn)
                if verdict is not None:
                    return verdict
                if log_fn:
                    log_fn(f"checksum skipped (per-part {algorithm}): {path}")
            else:
                actual = self.file_checksum(path, algorithm)
                if actual != expected:
                    if log_fn:
                        log_fn(f"CHECKSUM MISMATCH for {path}: "
                               f"{algorithm} {actual} != {expected}")
                    return False
                if log_fn:
                    log_fn(f"checksum ok ({algorithm}): {path}")
                return True

        if opaque:
            if log_fn:
                log_fn(f"checksum skipped (the ETag of an encrypted object "
                       f"is not an MD5): {path}")
            return True
        if not self.etag_is_md5(etag):
            verdict = self._verify_by_parts(path, key, etag, "", log_fn=log_fn)
            if verdict is not None:
                return verdict
            if log_fn:
                log_fn(f"checksum skipped (multipart ETag): {path}")
            return True
        actual = self.file_md5(path)
        expected = self.normalize_etag(etag)
        if actual != expected:
            if log_fn:
                log_fn(f"CHECKSUM MISMATCH for {path}: {actual} != {expected}")
            return False
        if log_fn:
            log_fn(f"checksum ok: {path}")
        return True

    def _download_ranged(self, key, out_path, size, etag, progress_cb=None,
                         cancel_event=None, log_fn=None, head=None,
                         local_root=None):
        """
        Download in fixed-size ranges into a '.s3duckpart' file, recording which
        chunks landed. An interrupted transfer resumes from the sidecar instead
        of restarting, and the chunks still download in parallel.

        Every range is pinned to *etag* with If-Match, so a part file only
        ever holds bytes of one version of the object. *head* is the HEAD
        that ETag came from: it carries the stored checksum, which is what
        verifies a large multipart or encrypted object the ETag cannot.
        """
        size = int(size)
        # Follows the configured part size, so one setting governs how both
        # uploads and resumable downloads are chunked. A sidecar written with a
        # different size is discarded below rather than misread.
        chunk_size = int(self.resume_chunk_size)
        total_chunks = max(1, (size + chunk_size - 1) // chunk_size)
        part_path, meta_path = self._resume_paths(out_path)
        etag = self.normalize_etag(etag)
        version = (head or {}).get("VersionId")
        if version == "null":
            version = None
        if not etag and not version:
            raise ValueError("A ranged download needs an ETag or version ID")
        root = local_root or os.path.dirname(out_path) or "."
        for path in (out_path, part_path, meta_path, meta_path + ".tmp"):
            require_safe_local_destination(root, path)
        identity = {"bucket": self.bucket, "key": key,
                    "endpoint": self.profile_endpoint_url, "version": version}

        done = set()
        if os.path.exists(part_path) and os.path.exists(meta_path):
            try:
                with open(meta_path) as handle:
                    saved = json.load(handle)
                # An empty ETag matches nothing on its own: a record keyed
                # on one would resume against whatever the key holds by the
                # next attempt. The identity carries the version id, which
                # pins a versioned object just as well, so either will do.
                if ((etag or version)
                        and saved.get("identity") == identity
                        and saved.get("etag") == etag
                        and int(saved.get("size", -1)) == size
                        and int(saved.get("chunk_size", -1)) == chunk_size
                        and os.path.getsize(part_path) == size):
                    done = {int(i) for i in saved.get("chunks", [])
                            if 0 <= int(i) < total_chunks}
                    if log_fn and done:
                        log_fn(
                            f"resuming {key}: {len(done)}/{total_chunks} chunks "
                            "already local"
                        )
                else:
                    # The object changed under us; the partial file is useless.
                    os.remove(part_path)
                    os.remove(meta_path)
            except Exception:
                done = set()

        if not os.path.exists(part_path):
            with open(part_path, "wb") as handle:
                handle.truncate(size)
            done = set()
        else:
            # A partial file whose sidecar is gone (a failed meta write, a
            # half-cleaned directory) is kept — every chunk is re-fetched
            # anyway — but it is NOT trusted to be the right length. One left
            # by a larger object would otherwise keep its tail past `size`,
            # and os.replace below would hand that over as the finished file.
            try:
                with open(part_path, "r+b") as handle:
                    handle.truncate(size)
            except OSError:
                pass

        state_lock = threading.Lock()
        transferred = sum(min(chunk_size, size - i * chunk_size) for i in done)

        def _save_meta():
            if not etag:
                return      # never a resumable record without a version
            tmp = meta_path + ".tmp"
            require_safe_local_destination(root, tmp)
            with open(tmp, "w") as handle:
                json.dump({"etag": etag, "size": size,
                           "identity": identity,
                           "chunk_size": chunk_size,
                           "chunks": sorted(done)}, handle)
            os.replace(tmp, meta_path)

        changed = None
        handle = open(part_path, "r+b")
        try:
            def _fetch(index):
                nonlocal transferred
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                start = index * chunk_size
                end = min(size, start + chunk_size) - 1
                params = dict(Bucket=self.bucket, Key=key,
                              Range=f"bytes={start}-{end}")
                # Pinned to the version the HEAD described. Without a pin an
                # object replaced mid-download came back as the old one's
                # first chunks followed by the new one's last: the right
                # size, a "success", and a file that never existed on the
                # server. The managed transfer has always done this.
                if version:
                    params["VersionId"] = version
                if etag:
                    params["IfMatch"] = f'"{etag}"'
                try:
                    resp = self.client.get_object(**params)
                except botocore.exceptions.ClientError as exc:
                    if etag and precondition_failed(exc):
                        raise ObjectChangedDuringDownload(
                            f"'{key}' changed on the server while it was "
                            "being downloaded; the partial download was "
                            "discarded. Download it again to get the new "
                            "version.") from exc
                    raise
                body = resp["Body"]
                try:
                    data = body.read()
                finally:
                    body.close()
                self._check_ranged_reply(key, resp, start, end, data)
                if self.rate_limiter is not None:
                    self.rate_limiter.consume(len(data))
                with state_lock:
                    require_safe_local_destination(root, part_path)
                    handle.seek(start)
                    handle.write(data)
                    handle.flush()
                    done.add(index)
                    _save_meta()
                    transferred += len(data)
                    current = transferred
                if progress_cb is not None:
                    progress_cb(size, min(current, size), key)

            pending = [i for i in range(total_chunks) if i not in done]
            try:
                run_parallel(pending, _fetch, self.parallel_files,
                             cancel_event=cancel_event)
            except ObjectChangedDuringDownload as exc:
                changed = exc
        finally:
            handle.close()

        if changed is not None:
            # The chunks already on disk belong to a version that is gone, so
            # resuming from them could only splice it with the new one.
            self._discard_files(part_path, meta_path)
            raise changed

        # Verified while it is still the partial file. A mismatch found after
        # os.replace left the bad bytes under the final name, where the next
        # sync — same size, newer mtime — called them up to date. Every chunk
        # is marked done, so the partial is useless too and goes with it.
        if self.verify_downloads and not self.verify_download(
                part_path, etag, head=head, log_fn=log_fn, key=key):
            self._discard_files(part_path, meta_path)
            raise Exception(
                f"Checksum mismatch after downloading '{key}'; the download "
                "did not match the object's stored digest and was discarded"
            )

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")
        require_safe_local_destination(root, out_path)
        os.replace(part_path, out_path)
        if os.path.exists(meta_path):
            os.remove(meta_path)

    def download_object_version(self, key, version_id, local_path,
                                progress_cb=None, cancel_event=None, log_fn=None):
        """Download one specific version of an object to local_path."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        size = None
        try:
            head = self.client.head_object(
                Bucket=self.bucket, Key=key, VersionId=version_id
            )
            size = head.get("ContentLength")
        except Exception:
            pass  # non-fatal; proceed without a known size

        def _do():
            self.client.download_file(
                self.bucket, key, local_path,
                ExtraArgs={"VersionId": version_id},
                Callback=_BotoProgressAdapter(
                    size, key, progress_cb, cancel_event=cancel_event,
                    limiter=self.rate_limiter,
                ),
                Config=self.transfer_cfg_download,
            )

        try:
            _do()
        except TransferCancelled:
            raise
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error downloading version of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def stream_object(self, key: str, chunk_size: int = 1024 * 1024,
                      cancel_event=None):
        """
        Yield an object's bytes in chunks without buffering it whole — used to
        feed a zip archive directly. Honours the shared bandwidth limit.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        body = resp["Body"]
        try:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                block = body.read(chunk_size)
                if not block:
                    return
                if self.rate_limiter is not None:
                    self.rate_limiter.consume(len(block))
                yield block
        finally:
            try:
                body.close()
            except Exception:
                pass

    def get_object_preview(self, key: str, max_bytes: int = 1024 * 1024,
                           version_id: str = "") -> dict:
        """
        Fetch up to max_bytes of an object for in-app preview.

        Returns a dict:
          {content_type, size (total, may be None), data (bytes),
           truncated (bool)}
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        def _do():
            kwargs = {"Bucket": self.bucket, "Key": key}
            if version_id:
                kwargs["VersionId"] = version_id
            if max_bytes and max_bytes > 0:
                kwargs["Range"] = f"bytes=0-{int(max_bytes) - 1}"
            try:
                resp = self.client.get_object(**kwargs)
            except botocore.exceptions.ClientError as exc:
                # An empty object (size 0) cannot satisfy a byte range; retry
                # unranged to preview it as empty rather than error out.
                code = exc.response.get("Error", {}).get("Code", "")
                if code in ("InvalidRange", "416") and "Range" in kwargs:
                    kwargs.pop("Range")
                    resp = self.client.get_object(**kwargs)
                else:
                    raise
            body = resp["Body"].read()

            # Prefer the total from Content-Range (present on a ranged reply);
            # fall back to Content-Length (the length of *this* body).
            total = None
            cr = resp.get("ContentRange") or ""
            if cr and "/" in cr:
                tail = cr.rsplit("/", 1)[1]
                if tail.isdigit():
                    total = int(tail)
            if total is None:
                cl = resp.get("ContentLength")
                total = int(cl) if cl is not None else None

            truncated = total is not None and len(body) < total
            return {
                "content_type": resp.get("ContentType", "") or "",
                "size": total,
                "data": body,
                "truncated": bool(truncated),
                # Carried so an edit can be written back conditionally: the
                # body the user is looking at is only safe to replace while
                # the object still has this ETag.
                "etag": self.normalize_etag(resp.get("ETag")),
            }

        try:
            return _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            self.rebind_bucket()
            return _do()
