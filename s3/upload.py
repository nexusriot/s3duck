"""Putting bytes in: multipart, resume, and the rules that shape a PUT.
"""

import botocore.exceptions
import fnmatch
import hashlib
import json
import mimetypes
import os
import threading
from core.errors import ObjectExists, TransferCancelled
from s3.common import (
    BUILTIN_CONTENT_TYPES, CHECKSUM_ALGORITHMS, FULL_OBJECT_CHECKSUMS,
    _BotoProgressAdapter, _ProgressReader, client_accepts,
    drop_unsendable_checksum_args, run_parallel,
)
from urllib.parse import urlencode


class UploadMixin:
    """Putting bytes in: multipart, resume, and the rules that shape a PUT.
    """

    @property
    def multipart_threshold_bytes(self) -> int:
        return int(self.multipart_threshold_mb) * 1024 * 1024

    @property
    def multipart_chunksize_bytes(self) -> int:
        return int(self.multipart_chunksize_mb) * 1024 * 1024

    def set_multipart_sizes(self, threshold_mb=None, chunksize_mb=None) -> tuple:
        """
        Set the multipart threshold and part size, in MiB.

        The part size is clamped to S3's own 5 MiB floor for non-final parts —
        a smaller one makes every multipart upload fail on completion — and the
        threshold is never allowed below the part size, which would ask boto3
        to split a file into a single part.
        """
        if chunksize_mb is not None:
            try:
                chunk = int(chunksize_mb)
            except (TypeError, ValueError):
                chunk = self.DEFAULT_MULTIPART_CHUNKSIZE_MB
            self.multipart_chunksize_mb = max(
                self.MIN_MULTIPART_CHUNKSIZE_MB,
                min(chunk, self.MAX_MULTIPART_CHUNKSIZE_MB))
        if threshold_mb is not None:
            try:
                threshold = int(threshold_mb)
            except (TypeError, ValueError):
                threshold = self.DEFAULT_MULTIPART_THRESHOLD_MB
            self.multipart_threshold_mb = max(
                self.multipart_chunksize_mb, threshold)
        self._rebuild_transfer_configs()
        # Ranged downloads resume in part-sized chunks, so they follow the
        # same settings rather than keeping a second, stale pair.
        self.resume_threshold = self.multipart_threshold_bytes
        self.resume_chunk_size = self.multipart_chunksize_bytes
        self.upload_chunk_size = self.multipart_chunksize_bytes
        return self.multipart_threshold_mb, self.multipart_chunksize_mb

    def set_upload_options(self, storage_class=None, sse=None, kms_key_id=None,
                           checksum_algorithm=None):
        """
        Configure what every subsequent upload sends alongside the body:
        a non-default storage class, server-side encryption and/or an
        additional checksum. Returns the resulting ExtraArgs dict.
        """
        args = {}
        if storage_class and storage_class != "STANDARD":
            args["StorageClass"] = storage_class
        if sse:
            args["ServerSideEncryption"] = sse
            if sse == "aws:kms" and kms_key_id:
                args["SSEKMSKeyId"] = kms_key_id
        algorithm = str(checksum_algorithm or "").upper()
        if algorithm in CHECKSUM_ALGORITHMS:
            args["ChecksumAlgorithm"] = algorithm
            if algorithm in FULL_OBJECT_CHECKSUMS:
                # Without this a multipart upload stores a per-part composite,
                # which is as unverifiable as a multipart ETag. Only the CRC
                # algorithms support a whole-object checksum; CRC64NVME has no
                # other form, so S3 would reject a composite request for it.
                args["ChecksumType"] = "FULL_OBJECT"
            self.checksum_algorithm = algorithm
        else:
            self.checksum_algorithm = ""
        self.upload_extra_args = args
        return args

    def set_content_type_options(self, detect=None, overrides=None):
        """
        Configure the per-file Content-Type sent with every upload.

        ``overrides`` maps a bare extension to a type and wins over both the
        built-in table and the platform's mimetypes database.
        """
        if detect is not None:
            self.detect_content_type = bool(detect)
        if overrides is not None:
            self.content_type_overrides = {
                str(ext).strip().lstrip("*").lstrip(".").lower():
                    str(value).strip()
                for ext, value in dict(overrides).items()
                if str(ext).strip().lstrip("*").lstrip(".") and str(value).strip()
            }
        return self.content_type_overrides

    def guess_content_type(self, name) -> str:
        """
        The Content-Type for a local path or key, or "" when nothing is known.

        Empty rather than a fallback on purpose: the caller must be able to
        tell "no idea" from "deliberately binary/octet-stream".
        """
        base = os.path.basename(str(name or ""))
        if "." not in base:
            return ""
        ext = base.rsplit(".", 1)[-1].lower()
        if ext in self.content_type_overrides:
            return self.content_type_overrides[ext]
        if ext in BUILTIN_CONTENT_TYPES:
            return BUILTIN_CONTENT_TYPES[ext]
        guessed, encoding = mimetypes.guess_type(base)
        # A .tar.gz is not "a gzip": the transfer encoding would make the
        # object undownloadable in a browser that silently decompresses it.
        if encoding:
            return ""
        return guessed or ""

    def set_upload_rules(self, rules):
        """Replace the per-destination upload rules."""
        cleaned = []
        for rule in list(rules or []):
            pattern = str(rule.get("pattern") or "").strip()
            if not pattern:
                continue
            cleaned.append({
                "pattern": pattern,
                "storage_class": str(rule.get("storage_class") or "").strip(),
                "tags": {str(k): str(v)
                         for k, v in dict(rule.get("tags") or {}).items()},
            })
        self.upload_rules = cleaned
        return cleaned

    def rule_for_key(self, key):
        """
        The first upload rule whose glob matches this destination key.

        First match wins rather than merging: a table where two rules
        half-apply is one nobody can predict.
        """
        target = str(key or "").lstrip("/")
        if not target:
            return None
        for rule in self.upload_rules:
            pattern = rule.get("pattern") or ""
            if fnmatch.fnmatchcase(target, pattern):
                return rule
            # A bare prefix is the obvious way to write "everything under
            # here", so accept it without demanding a trailing glob.
            if pattern.endswith("/") and target.startswith(pattern):
                return rule
        return None

    def upload_args_for(self, local_file, key="") -> dict:
        """
        The ExtraArgs for one upload: the shared options, this file's type,
        and whatever an upload rule says about its destination.
        """
        args = dict(self.upload_extra_args)
        if self.detect_content_type and local_file and "ContentType" not in args:
            content_type = self.guess_content_type(local_file)
            if content_type:
                args["ContentType"] = content_type
        rule = self.rule_for_key(key)
        if rule:
            if rule.get("storage_class"):
                args["StorageClass"] = rule["storage_class"]
            if rule.get("tags"):
                # S3 wants the tag set as a url-encoded query string here.
                args["Tagging"] = urlencode(sorted(rule["tags"].items()))
        return drop_unsendable_checksum_args(self.client, args)

    def _resume_paths(self, out_path):
        part = out_path + self.PART_SUFFIX
        return part, part + ".meta"

    def create_folder(self, key, log_fn=None):
        self._guard_write()
        try:
            return self.client.put_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error creating folder '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return self.client.put_object(Bucket=self.bucket, Key=key)

    def _upload_state_path(self, key, local_file) -> str:
        """
        Where the resume record for one upload lives.

        Not beside the source file: uploads read from directories that may be
        read-only or that the user would rather not have littered, and the
        record has to outlive the process to be useful at all.
        """
        # The PROFILE endpoint, not the live one: endpoint_url moves when a
        # bucket is rebound to another region, and keying on it made the
        # record unfindable on the retry — which silently started a second
        # multipart upload and orphaned the first one's parts.
        identity = "|".join([
            str(self.profile_endpoint_url or self.endpoint_url or ""),
            str(self.bucket or ""), str(key), os.path.abspath(local_file),
        ])
        digest = hashlib.sha256(identity.encode()).hexdigest()[:32]
        return os.path.join(self.upload_state_dir, digest + ".json")

    def _read_upload_state(self, path, key, size, mtime, chunk_size):
        """
        The stored upload id, but only if it still describes this exact upload.

        A changed size, mtime or part size means the parts already on the
        server no longer line up with what we would send, so resuming would
        assemble a corrupt object.
        """
        try:
            with open(path) as handle:
                saved = json.load(handle)
        except Exception:
            return None
        try:
            same = (
                saved.get("key") == key
                and saved.get("bucket") == self.bucket
                and int(saved.get("size", -1)) == int(size)
                and int(saved.get("chunk_size", -1)) == int(chunk_size)
                and abs(float(saved.get("mtime", -1)) - float(mtime)) < 0.001
            )
        except (TypeError, ValueError):
            return None
        return str(saved.get("upload_id") or "") if same else None

    def _write_upload_state(self, path, key, upload_id, size, mtime, chunk_size):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as handle:
            json.dump({
                "bucket": self.bucket, "key": key, "upload_id": upload_id,
                "size": int(size), "mtime": float(mtime),
                "chunk_size": int(chunk_size),
            }, handle)

    def _uploaded_parts(self, key, upload_id) -> dict:
        """
        Parts already stored server-side, as ``{number: part_dict}``.

        Read from the service rather than from our own record: the server is
        the only authority on what actually landed, and a part we *think* we
        sent may have failed after the record was written.
        """
        parts = {}
        marker = 0
        while True:
            resp = self.client.list_parts(
                Bucket=self.bucket, Key=key, UploadId=upload_id,
                PartNumberMarker=marker)
            for part in resp.get("Parts", []) or []:
                number = int(part.get("PartNumber", 0))
                if not number:
                    continue
                entry = {"PartNumber": number, "ETag": part.get("ETag"),
                         "Size": int(part.get("Size") or 0)}
                for field, value in part.items():
                    if field.startswith("Checksum"):
                        entry[field] = value
                parts[number] = entry
            if not resp.get("IsTruncated"):
                return parts
            try:
                following = int(resp.get("NextPartNumberMarker") or 0)
            except (TypeError, ValueError):
                return parts
            # S3-compatible backends differ in what they return; a marker that
            # does not move on would spin this thread forever, so take what we
            # have rather than hang the transfer.
            if following <= marker:
                return parts
            marker = following

    def _upload_resumable(self, local_file, key, size, progress_cb=None,
                          cancel_event=None, log_fn=None, if_none_match=False):
        """
        Upload a large file part by part so an interruption can resume.

        boto3's managed upload restarts from zero, which on a slow link makes a
        big file effectively untransferable. A failure or cancellation here
        deliberately leaves the multipart upload OPEN and its record on disk,
        because that is exactly what the next attempt resumes from — the
        Incomplete-uploads tool is how an abandoned one gets cleaned up.
        """
        # Raised when the configured part size would need more than S3's
        # 10 000 parts. Without this a file over chunk x 10 000 — 80 GiB at
        # the default 8 MiB — was refused on the part that crossed the line,
        # after everything before it had already been sent. boto3's managed
        # upload has adjusted for this all along (s3transfer's
        # ChunksizeAdjuster), so turning resumable uploads OFF used to be
        # what made a very large file transferable.
        chunk = int(self.upload_part_size(size, self.upload_chunk_size))
        total_parts = max(1, (int(size) + chunk - 1) // chunk)
        mtime = os.path.getmtime(local_file)
        state_path = self._upload_state_path(key, local_file)
        extra = self.upload_args_for(local_file, key)
        # CreateMultipartUpload is sent what we pass and nothing more, while
        # botocore adds a default checksum to every UploadPart on its own.
        # A service that checks a part against the upload it belongs to then
        # refuses all of them ("Checksum Type mismatch, expected: null,
        # actual: crc32"), which no amount of retrying fixes. Naming the
        # algorithm makes the two agree and changes nothing on the wire that
        # the SDK was not already sending.
        extra.setdefault("ChecksumAlgorithm", self.DEFAULT_PART_CHECKSUM)

        upload_id = self._read_upload_state(
            state_path, key, size, mtime, chunk)
        done = {}
        if upload_id:
            try:
                done = self._uploaded_parts(key, upload_id)
            except Exception as exc:
                if log_fn:
                    log_fn(f"cannot resume '{key}' ({exc}); starting over")
                upload_id = None
        if not upload_id:
            upload_id = self.client.create_multipart_upload(
                Bucket=self.bucket, Key=key, **extra)["UploadId"]
            done = {}
        self._write_upload_state(
            state_path, key, upload_id, size, mtime, chunk)

        # A stored part whose length disagrees with what we would send cannot
        # be trusted, so it is re-sent rather than reused.
        for number in list(done):
            start = (number - 1) * chunk
            expected = min(chunk, int(size) - start)
            if int(done[number].get("Size") or 0) != expected:
                del done[number]
        if done and log_fn:
            log_fn(f"resuming {key}: {len(done)}/{total_parts} part(s) "
                   "already uploaded")

        algorithm = extra.get("ChecksumAlgorithm")
        sent = sum(int(p.get("Size") or 0) for p in done.values())
        lock = threading.Lock()

        def _send(number):
            nonlocal sent
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            start = (number - 1) * chunk
            length = min(chunk, int(size) - start)
            with open(local_file, "rb") as handle:
                handle.seek(start)
                body = handle.read(length)
            # Paced before the request so the shared ceiling actually throttles
            # rather than being charged after the bytes are already gone.
            if self.rate_limiter is not None:
                self.rate_limiter.consume(len(body))
            params = dict(Bucket=self.bucket, Key=key, UploadId=upload_id,
                          PartNumber=number, Body=body)
            if algorithm:
                params["ChecksumAlgorithm"] = algorithm
            resp = self.client.upload_part(**params)
            entry = {"PartNumber": number, "ETag": resp.get("ETag")}
            for field, value in (resp or {}).items():
                if field.startswith("Checksum"):
                    entry[field] = value
            with lock:
                done[number] = entry
                sent += len(body)
                if progress_cb is not None:
                    progress_cb(max(1, int(size)), min(sent, int(size)), key)

        pending = [n for n in range(1, total_parts + 1) if n not in done]
        run_parallel(pending, _send, self.transfer_concurrency,
                     cancel_event=cancel_event)

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        parts = [{field: value for field, value in entry.items()
                  if field != "Size"}
                 for entry in sorted(done.values(),
                                     key=lambda p: p["PartNumber"])]
        # The precondition belongs on the completion: that is the moment the
        # object appears, and the only point in a multipart upload a service
        # can still refuse without having already replaced anything.
        complete = dict(Bucket=self.bucket, Key=key, UploadId=upload_id,
                        MultipartUpload={"Parts": parts})
        try:
            self.client.complete_multipart_upload(
                **(dict(complete, IfNoneMatch="*") if if_none_match
                   else complete))
        except (botocore.exceptions.ClientError,
                botocore.exceptions.ParamValidationError) as exc:
            if not if_none_match:
                raise
            if self._object_already_there(exc):
                # The parts stay: aborting here would throw away a transfer
                # whose outcome the caller may yet want to retry over the top.
                raise ObjectExists(key) from exc
            if not self._conditional_write_unsupported(exc):
                raise
            if log_fn:
                log_fn(f"this backend has no conditional writes; completing "
                       f"'{key}' without the precondition")
            self.client.complete_multipart_upload(**complete)
        # Only now is the record useless.
        try:
            os.remove(state_path)
        except OSError:
            pass
        if log_fn:
            log_fn(f"uploaded {key} in {total_parts} part(s)")

    @staticmethod
    def _conditional_write_unsupported(exc) -> bool:
        """
        Whether the backend has no conditional writes at all, as opposed to
        having refused this particular one.

        Deliberately narrow. A broad reading would turn an unrelated failure
        into a silent unconditional retry, which is the one outcome the
        precondition exists to prevent — so only a plain "not implemented",
        or a complaint naming the header itself, counts.
        """
        if isinstance(exc, botocore.exceptions.ParamValidationError):
            # boto3 refused the argument before sending anything: the
            # installed botocore predates IfNoneMatch. Same outcome as a
            # backend without conditional writes, and nothing was written.
            return "ifnonematch" in str(exc).lower()
        response = getattr(exc, "response", None) or {}
        error = response.get("Error") or {}
        code = str(error.get("Code", ""))
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        if code in ("NotImplemented", "XNotImplemented") or status == 501:
            return True
        text = f"{code} {error.get('Message', '')}".lower()
        return "if-none-match" in text or "ifnonematch" in text

    def supports_conditional_create(self) -> bool:
        """
        Whether this build can send IfNoneMatch at all.

        Both routes a conditional upload may take have to carry it — the
        single PUT and the completion of a multipart upload — so a botocore
        that knows only one of them still cannot promise "skip existing".
        """
        return (client_accepts(self.client, "PutObject", "IfNoneMatch")
                and client_accepts(self.client, "CompleteMultipartUpload",
                                   "IfNoneMatch"))

    def _put_object_if_absent(self, local_file, key, size, progress_cb=None,
                              cancel_event=None, log_fn=None) -> bool:
        """
        PutObject carrying IfNoneMatch, so a key that appeared since the
        destination check is refused rather than silently overwritten.

        Returns False when the backend has no conditional writes, leaving the
        caller to fall back; raises ObjectExists when the key was taken.
        """
        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")
        extra = dict(self.upload_args_for(local_file, key) or {})
        # PutObject has no ChecksumType — a single PUT is a whole object by
        # construction — and sending it is a hard ParamValidationError.
        extra.pop("ChecksumType", None)
        total = max(1, int(size or 0))
        if progress_cb is not None:
            progress_cb(total, 0, key)
        try:
            with open(local_file, "rb") as handle:
                self.client.put_object(
                    Bucket=self.bucket, Key=key,
                    Body=_ProgressReader(
                        handle, size, key, progress_cb,
                        cancel_event=cancel_event, limiter=self.rate_limiter),
                    IfNoneMatch="*", **extra)
        except (botocore.exceptions.ClientError,
                botocore.exceptions.ParamValidationError) as exc:
            if self._object_already_there(exc):
                raise ObjectExists(key) from exc
            if self._conditional_write_unsupported(exc):
                if log_fn:
                    log_fn(f"this backend has no conditional writes; "
                           f"uploading '{key}' without the precondition")
                return False
            raise
        if progress_cb is not None:
            progress_cb(total, int(size or 0), key)
        return True

    def upload_file(self, local_file, key, progress_cb=None, cancel_event=None,
                    log_fn=None, if_none_match=False):
        """
        Upload a file (with progress) or create a folder placeholder if local_file is None.
        Automatically retries once on region/endpoint errors.

        ``if_none_match`` asks the service to refuse the write if the key is
        already taken, raising ObjectExists. Checking the destination first
        and then writing is a snapshot with a gap after it; the precondition
        is what closes the gap, so "skip existing" means it even when the
        object appears mid-job. A backend without conditional writes degrades
        to the old check-then-write, and says so in the log.
        """
        self._guard_write()
        if local_file is None:
            self.create_folder("%s/" % key, log_fn=log_fn)
            return

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        try:
            total = os.path.getsize(local_file)
        except Exception:
            total = None

        if if_none_match and not self.supports_conditional_create():
            # An installed botocore too old to know the parameter would raise
            # ParamValidationError on every single upload. The paths below
            # recover from that, but only after the bytes have been sent, so
            # the cheap check happens here instead — once, before any work.
            if log_fn:
                log_fn("this boto3 build has no conditional writes "
                       "(IfNoneMatch); uploading without the precondition")
            if_none_match = False

        if if_none_match and total is not None:
            # boto3's managed upload cannot carry the precondition — IfNoneMatch
            # is not in s3transfer's allowed ExtraArgs — so a conditional
            # upload goes through the calls this file makes itself. A large
            # file therefore takes the resumable writer even when resumable
            # uploads are off: it is the only multipart path we control.
            def _do_conditional():
                """True when the write happened; False to fall through."""
                if int(total) < self.multipart_threshold_bytes:
                    return self._put_object_if_absent(
                        local_file, key, total, progress_cb=progress_cb,
                        cancel_event=cancel_event, log_fn=log_fn)
                self._upload_resumable(
                    local_file, key, total, progress_cb=progress_cb,
                    cancel_event=cancel_event, log_fn=log_fn,
                    if_none_match=True)
                return True

            # The same one-shot rebind the other two routes have. Without it
            # this method's own promise — retry once on a region error —
            # stopped holding the moment the user chose to skip existing
            # objects.
            try:
                if _do_conditional():
                    return
            except (TransferCancelled, ObjectExists):
                raise
            except Exception as exc:
                if not self._is_region_error(exc):
                    raise
                if log_fn:
                    log_fn(f"Region error uploading '{key}': {exc}")
                self.rebind_bucket(log_fn=log_fn)
                if _do_conditional():
                    return

        if (self.resumable_uploads and total is not None
                and int(total) >= self.multipart_threshold_bytes):
            def _do_resumable():
                self._upload_resumable(
                    local_file, key, total, progress_cb=progress_cb,
                    cancel_event=cancel_event, log_fn=log_fn)

            # Same one-shot rebind the managed upload does. Without it the same
            # bucket failed for a large file and succeeded for a small one.
            # The retry resumes from the record just written, and an upload id
            # the new endpoint rejects is simply started over.
            try:
                _do_resumable()
            except TransferCancelled:
                raise
            except Exception as exc:
                if not self._is_region_error(exc):
                    raise
                if log_fn:
                    log_fn(f"Region error uploading '{key}': {exc}")
                self.rebind_bucket(log_fn=log_fn)
                _do_resumable()
            return

        extra_args = self.upload_args_for(local_file, key) or None

        def _do_upload():
            self.client.upload_file(
                local_file,
                self.bucket,
                key,
                ExtraArgs=extra_args,
                Callback=_BotoProgressAdapter(
                    total, key, progress_cb, cancel_event=cancel_event,
                    limiter=self.rate_limiter),
                Config=self.transfer_cfg_upload,
            )

        try:
            _do_upload()
        except TransferCancelled:
            raise
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error uploading '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do_upload()

    # What a body rewrite carries over from the object it replaces. A PUT
    # makes a new object, so anything not sent is reset rather than kept.
    REWRITE_KEPT_FIELDS = (
        "ContentType", "CacheControl", "ContentDisposition", "ContentEncoding",
        "ContentLanguage", "WebsiteRedirectLocation", "ServerSideEncryption",
        "SSEKMSKeyId", "BucketKeyEnabled",
    )
    # A backend with no object tagging at all has no tags to lose.
    NO_TAGGING_CODES = ("NotImplemented", "MethodNotAllowed",
                        "UnsupportedOperation")

    def _rewrite_params(self, key) -> tuple:
        """
        ``(put_object params, current ETag)`` that keep *key* as it is.

        The in-app editor's Save was a bare PUT of the new body: the object's
        own Content-Type (charset and all) was replaced by a guess from the
        extension, and its Cache-Control, Content-Disposition, user metadata,
        tags, storage class and KMS key were all reset — a one-word edit
        re-tiered an IA object to STANDARD and stripped the headers a website
        served it with. Not carried: the ACL and Object Lock settings, which
        need their own calls.

        A tag set that cannot be read refuses the save rather than being
        written back empty.
        """
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        params = {}
        for field in self.REWRITE_KEPT_FIELDS:
            value = head.get(field)
            if value:
                params[field] = value
        if head.get("Metadata"):
            params["Metadata"] = dict(head["Metadata"])
        expires = head.get("Expires")
        if expires is not None and not isinstance(expires, str):
            params["Expires"] = expires
        # HEAD names no class for STANDARD. Saying nothing is right then: the
        # profile's default upload class is for new objects, not this one.
        storage_class = head.get("StorageClass")
        if storage_class and storage_class != "STANDARD":
            params["StorageClass"] = storage_class
        try:
            tag_set = self.client.get_object_tagging(
                Bucket=self.bucket, Key=key).get("TagSet") or []
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in self.NO_TAGGING_CODES:
                raise RuntimeError(
                    "Could not read the object's tags, and saving without "
                    f"them would delete them: {exc}") from exc
            tag_set = []
        if tag_set:
            # S3 wants the tag set as a url-encoded query string here.
            params["Tagging"] = urlencode(sorted(
                (str(tag.get("Key") or ""), str(tag.get("Value") or ""))
                for tag in tag_set if tag.get("Key")))
        return params, self.normalize_etag(head.get("ETag"))

    def put_object_body(self, key: str, data: bytes, *, if_match="",
                        content_type="", preserve_properties=False,
                        log_fn=None) -> str:
        """
        Replace an object's body, refusing when it changed since it was read.

        Returns the new ETag. The precondition is what makes in-app editing
        safe; a backend without conditional writes falls back to comparing the
        ETag first, which is racy but still catches the case that happens in
        practice — someone saved while the editor was open.

        *preserve_properties* is for rewriting an existing object in place:
        its headers, metadata, storage class, encryption and tags are read
        first and sent with the new body (see _rewrite_params). Without it
        the write is shaped like a fresh upload.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        expected = self.normalize_etag(if_match)
        if preserve_properties:
            params, current = self._rewrite_params(key)
            if expected and current and current != expected:
                # The properties just read belong to someone else's save.
                raise self.PreconditionFailed(
                    "The object changed on the server since it was opened.")
        else:
            params = dict(self.upload_extra_args)
            params.pop("ChecksumAlgorithm", None)
            params.pop("ChecksumType", None)
        if content_type:
            params["ContentType"] = content_type
        elif not params.get("ContentType"):
            guessed = self.guess_content_type(key)
            if guessed:
                params["ContentType"] = guessed

        def _put(conditional):
            kwargs = dict(Bucket=self.bucket, Key=key, Body=data, **params)
            if conditional and expected:
                kwargs["IfMatch"] = expected
            return self.client.put_object(**kwargs)

        def _verify_by_hand():
            if not expected:
                return
            head = self.client.head_object(Bucket=self.bucket, Key=key)
            if self.normalize_etag(head.get("ETag")) != expected:
                raise self.PreconditionFailed(
                    "The object changed on the server since it was opened.")

        # IfMatch is old on GetObject but was added to PutObject in the same
        # 2024 wave as IfNoneMatch, so an older botocore refuses it before
        # the request goes out. Standing down here keeps Save working; the
        # by-hand ETag check below is the same degradation a backend without
        # conditional writes already gets.
        if expected and not client_accepts(self.client, "PutObject", "IfMatch"):
            if log_fn:
                log_fn("this boto3 build has no conditional writes "
                       "(If-Match); checking the ETag by hand")
            _verify_by_hand()
            expected = ""

        try:
            resp = _put(bool(expected))
        except botocore.exceptions.ParamValidationError as exc:
            # The probe above should have caught this; a refusal here still
            # means the parameter never left, so Save degrades rather than
            # failing on a build whose model says one thing and whose
            # validator does another.
            if not expected or "ifmatch" not in str(exc).lower():
                raise
            if log_fn:
                log_fn("this boto3 build has no conditional writes "
                       "(If-Match); checking the ETag by hand")
            _verify_by_hand()
            resp = _put(False)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get(
                "ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("PreconditionFailed", "412") or status == 412:
                raise self.PreconditionFailed(
                    "The object changed on the server since it was opened."
                ) from exc
            if expected and (code in self.PRECONDITION_UNSUPPORTED_CODES
                             or status == 501):
                if log_fn:
                    log_fn(f"conditional write unsupported ({code}); "
                           "checking the ETag by hand")
                _verify_by_hand()
                resp = _put(False)
            elif self._is_region_error(exc):
                if log_fn:
                    log_fn(f"Region error writing '{key}': {exc}")
                self.rebind_bucket(log_fn=log_fn)
                resp = _put(bool(expected))
            else:
                raise
        if log_fn:
            log_fn(f"saved {key} ({len(data)} bytes)")
        return self.normalize_etag(resp.get("ETag"))
