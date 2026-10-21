"""Server-side copy, and the stream-through fallback when it is refused.
"""

import botocore.exceptions
import copy
import threading
from boto3.s3.transfer import TransferConfig
from core.errors import ArchivedObject, TransferCancelled
from s3.common import (
    _BotoProgressAdapter, drop_unsendable_checksum_args, precondition_failed,
    prefix_of, run_parallel,
)
from urllib.parse import quote, urlencode


class CopyMixin:
    """Server-side copy, and the stream-through fallback when it is refused.
    """

    @staticmethod
    def _carried_extra_args(resp, upload_extra_args) -> dict:
        """
        The ExtraArgs a streamed copy must re-send so the new object is not a
        bare octet-stream: streaming loses everything CopyObject would have
        preserved server-side. The destination's own upload policy wins.
        """
        extra = {}
        for field in ("ContentType", "CacheControl", "ContentDisposition",
                      "ContentEncoding"):
            value = (resp or {}).get(field)
            if value:
                extra[field] = value
        metadata = (resp or {}).get("Metadata") or {}
        if metadata:
            extra["Metadata"] = dict(metadata)
        extra.update(upload_extra_args or {})
        return extra

    def _storage_class_to_preserve(self, src_key, storage_class):
        """
        The class a copy of *src_key* must land in: what the caller already
        knew, or what the source itself says.

        ``None`` means "nobody told us", which is not the same as STANDARD.
        Every listing already carries the class -- it comes in the same
        element as the key and the size -- so the job rows built from one,
        and the recursive copy that fans out from one, answer this for free.
        The HEAD here is paid only by the routes with no listing behind them:
        an object picked in the other pane, or a job replayed from a history
        written before rows carried the class.

        Best effort on purpose: a HEAD the credentials cannot make is a
        reason to copy the object as STANDARD, never a reason to refuse the
        copy the user asked for.
        """
        if storage_class is not None:
            return str(storage_class or "STANDARD").upper()
        try:
            return self.current_storage_class(src_key)
        except Exception:
            return "STANDARD"

    @staticmethod
    def _preserved_class_args(storage_class) -> dict:
        """
        ``{"StorageClass": ...}`` when the class is worth naming, else ``{}``.

        S3 has no "whatever the source was" directive: an unqualified
        CopyObject creates the new object as STANDARD, which is how a rename
        used to re-tier an IA object nobody asked to re-tier. STANDARD is
        left unsaid because it is already what the service does, so the
        overwhelmingly common copy goes out as exactly the request it always
        was -- including to a backend that has never heard of storage
        classes.
        """
        storage_class = str(storage_class or "").upper()
        if not storage_class or storage_class == "STANDARD":
            return {}
        return {"StorageClass": storage_class}

    def _raise_if_archived(self, exc, src_key, storage_class=""):
        """
        Re-raise a refusal to read an archived source as something that says
        what to do about it.

        The service is the only authority on this, so the class is never used
        to refuse a copy in advance: a GLACIER object that has been restored
        copies perfectly well, and no listing says whether a restore is in
        effect. The copy is attempted and only the service's own answer is
        translated -- "InvalidObjectState", which means nothing to anyone who
        has not met it before.
        """
        response = getattr(exc, "response", None) or {}
        code = (response.get("Error") or {}).get("Code", "")
        if code not in self.ARCHIVED_SOURCE_CODES:
            return
        storage_class = str(storage_class or "").upper()
        named = (f" ({storage_class})"
                 if storage_class and storage_class != "STANDARD" else "")
        raise ArchivedObject(
            f"'{src_key}'{named} is archived and has not been restored, so "
            f"it cannot be copied or moved. Restore it first "
            f"(right-click → Restore from Glacier…), then try again."
        ) from exc

    # Codes a tag read answers when tags are simply not available here --
    # not permitted, or not implemented by the backend. The copy goes ahead
    # without them, exactly as it did before tags were carried at all.
    TAG_READ_UNAVAILABLE_CODES = (
        "AccessDenied", "NotImplemented", "XNotImplemented",
        "MethodNotAllowed", "InvalidRequest", "UnsupportedOperation",
        "NoSuchTagSet", "NoSuchTagSetError",
    )

    def _sized_transfer_config(self, config, size):
        """
        The upload TransferConfig for streaming *size* bytes, with a part
        size large enough for S3's 10 000 parts.

        A streamed copy hands s3transfer a response body, which cannot seek,
        so s3transfer never learns the size and never raises the part size
        to fit -- its ChunksizeAdjuster only does that for a size it knows.
        At the default 8 MiB every object over 80 GiB was refused on part
        10 001, after 80 GiB had already been moved. The GET that opened the
        body already said how long it is, so the size is applied here.
        """
        if size is None:
            return config
        preferred = int(getattr(config, "multipart_chunksize", 0)
                        or self.multipart_chunksize_bytes)
        part = self.upload_part_size(int(size), preferred)
        if config is not None and part == preferred:
            return config
        tuned = copy.copy(config) if config is not None else TransferConfig()
        tuned.multipart_chunksize = part
        return tuned

    def copy_to_model(self, src_key: str, dst_model, dst_key: str,
                      progress_cb=None, cancel_event=None, log_fn=None):
        """
        Copy one object into a DIFFERENT profile's storage.

        CopyObject is server-side and cannot cross credentials at all, so the
        bytes are read with this model's client and written with the
        destination model's — the only way to move data between accounts or
        between unrelated providers (MinIO to AWS, say).
        """
        dst_model._guard_write()
        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")
        resp = self.client.get_object(Bucket=self.bucket, Key=src_key)
        body = resp["Body"]
        extra = drop_unsendable_checksum_args(
            dst_model.client,
            self._carried_extra_args(resp, dst_model.upload_extra_args))
        callback = None
        if progress_cb is not None:
            callback = _BotoProgressAdapter(
                resp.get("ContentLength"), dst_key, progress_cb,
                cancel_event=cancel_event, limiter=self.rate_limiter)
        dst_model.client.upload_fileobj(
            body, dst_model.bucket, dst_key,
            ExtraArgs=extra or None,
            Callback=callback,
            Config=self._sized_transfer_config(
                dst_model.transfer_cfg_upload, resp.get("ContentLength")),
        )
        if log_fn:
            log_fn(f"copied {self.bucket}/{src_key} -> "
                   f"{dst_model.bucket}/{dst_key}")

    def copy_prefix_to_model(self, src_prefix: str, dst_model, dst_prefix: str,
                             progress_cb=None, cancel_event=None, log_fn=None):
        """Copy a whole prefix into another profile, preserving its shape."""
        dst_model._guard_write()
        # An empty prefix on either side means the bucket root, which
        # prefix_of answers as "" — a guard here would only hide a
        # regression in it.
        source = prefix_of(src_prefix)
        target = prefix_of(dst_prefix)
        for key, _size in self.get_keys(source, log_fn=log_fn):
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            if not key:
                continue
            rel = key[len(source):] if key.startswith(source) else ""
            if not rel:
                continue
            if key.endswith("/"):
                dst_model.create_folder(target + rel, log_fn=log_fn)
                continue
            self.copy_to_model(
                key, dst_model, target + rel, progress_cb=progress_cb,
                cancel_event=cancel_event, log_fn=log_fn)

    def _stream_copy(self, src_key: str, dst_key: str, dst_bucket: str,
                     log_fn=None, storage_class=""):
        """
        Copy by streaming the object through this process.

        CopyObject is server-side and requires one endpoint to reach both
        buckets, which is not true across regions or accounts. Reading from the
        source client and writing with a client bound to the destination works
        anywhere, at the cost of moving the bytes.
        """
        dst_client, _endpoint, _region, _use_path = self._try_bind_bucket(dst_bucket)
        resp = self.client.get_object(Bucket=self.bucket, Key=src_key)
        body = resp["Body"]
        extra = drop_unsendable_checksum_args(
            dst_client, self._carried_extra_args(resp, self.upload_extra_args))
        # The source's class wins over the profile's upload policy, which the
        # carried args brought along: this is a copy that happens to be
        # routed through an upload, and a cross-region copy landing in a
        # different class than the same copy made in-region would be a
        # difference nobody asked for.
        extra.pop("StorageClass", None)
        extra.update(self._preserved_class_args(storage_class))

        dst_client.upload_fileobj(
            body, dst_bucket, dst_key,
            ExtraArgs=extra or None,
            Config=self._sized_transfer_config(
                self.transfer_cfg_upload, resp.get("ContentLength")),
        )
        if log_fn:
            log_fn(f"streamed {src_key} -> {dst_bucket}/{dst_key}")

    def _oversized_copy_source(self, exc, src_key, src_bucket="") -> int:
        """
        The source's size when a CopyObject was refused for being too big,
        else 0.

        Measuring beats matching the message: the wording differs between
        AWS, MinIO and Ceph, while the 5 GiB rule does not.
        """
        response = getattr(exc, "response", None) or {}
        code = (response.get("Error") or {}).get("Code", "")
        if code not in self.COPY_TOO_LARGE_CODES:
            return 0
        try:
            head = self.client.head_object(
                Bucket=src_bucket or self.bucket, Key=src_key)
            size = int(head.get("ContentLength") or 0)
        except Exception:
            return 0
        return size if size > self.MAX_SINGLE_COPY_BYTES else 0

    def _copy_create_args(self, src_key, *, src_bucket="", extra=None,
                          head=None) -> dict:
        """
        What CreateMultipartUpload must be told so a multipart copy preserves
        what CopyObject would have preserved on its own.

        CopyObject has MetadataDirective=COPY; a multipart upload has no such
        thing — it defines the new object outright, so anything not passed
        here is lost. *extra* is the caller's own intent (a new storage class,
        replacement metadata) and wins over what the source carried. *head*
        is a HEAD of the source the caller already made. The tags are not
        read here: _copy_multipart carries them for every caller.
        """
        if head is None:
            # Raised, not swallowed. A HEAD that failed used to mean a copy
            # with no Content-Type and no metadata at all -- a different
            # object -- and on a move the original is deleted right after.
            head = self.client.head_object(
                Bucket=src_bucket or self.bucket, Key=src_key)
        args = {}
        for field in ("ContentType", "CacheControl", "ContentDisposition",
                      "ContentEncoding", "ContentLanguage", "Expires",
                      "StorageClass", "ServerSideEncryption", "SSEKMSKeyId",
                      "WebsiteRedirectLocation"):
            value = head.get(field)
            if value:
                args[field] = value
        if head.get("BucketKeyEnabled"):
            args["BucketKeyEnabled"] = True
        metadata = head.get("Metadata") or {}
        if metadata:
            args["Metadata"] = dict(metadata)
        args.update(extra or {})
        return args

    def _source_tagging(self, src_bucket, src_key, log_fn=None) -> str:
        """
        The source's tags as the url-encoded string CreateMultipartUpload
        takes, or "" when it has none or they cannot be read.

        CopyObject copies the tag set by default (TaggingDirective=COPY); a
        multipart upload starts with none, so a move of a tagged object over
        5 GiB used to delete the only copy of its tags.
        """
        try:
            resp = self.client.get_object_tagging(Bucket=src_bucket, Key=src_key)
        except botocore.exceptions.ClientError as exc:
            code = (exc.response.get("Error") or {}).get("Code", "")
            if code not in self.TAG_READ_UNAVAILABLE_CODES:
                raise
            if log_fn:
                log_fn(f"tags of '{src_key}' cannot be read ({code}); "
                       "copying it without them")
            return ""
        pairs = [(str(tag.get("Key")), str(tag.get("Value") or ""))
                 for tag in (resp.get("TagSet") or [])
                 if tag.get("Key") is not None]
        # %20 rather than '+' for a space: unambiguous however the service
        # decodes the header.
        return urlencode(pairs, quote_via=quote) if pairs else ""

    def _copy_multipart(self, src_key, dst_key, *, size, dst_bucket="",
                        src_bucket="", create_args=None, log_fn=None,
                        cancel_event=None, source_etag=None):
        """
        Server-side copy of an object too large for a single CopyObject.

        S3 caps a CopyObject source at 5 GiB. Past that the object is rebuilt
        at the destination out of UploadPartCopy ranges, which the service
        still performs itself — this is not the streaming fallback, and no
        part of the object travels through this process.

        Every range is pinned to one version of the source with
        CopySourceIfMatch. *source_etag* (and *size*) are what the caller's
        own HEAD saw; ``None`` means nobody looked, and the source is asked.
        """
        self._guard_write()
        dst_bucket = dst_bucket or self.bucket
        src_bucket = src_bucket or self.bucket
        if source_etag is None:
            head = self.client.head_object(Bucket=src_bucket, Key=src_key)
            source_etag = head.get("ETag") or ""
            # The ranges must describe the version being pinned, not the
            # size some earlier listing reported for an older one.
            if head.get("ContentLength") is not None:
                size = head["ContentLength"]
        size = int(size)
        part = self.copy_part_size(size, self.multipart_chunksize_bytes)
        total_parts = max(1, (size + part - 1) // part)
        copy_source = {"Bucket": src_bucket, "Key": src_key}

        create = dict(create_args or {})
        if "Tagging" not in create:
            tagging = self._source_tagging(src_bucket, src_key, log_fn=log_fn)
            if tagging:
                create["Tagging"] = tagging
        # Without the pin, a source replaced while its parts were being
        # copied came out as the old version's first parts followed by the
        # new one's last -- and a move then deleted the source.
        pin = {"CopySourceIfMatch": source_etag} if source_etag else {}

        upload_id = self.client.create_multipart_upload(
            Bucket=dst_bucket, Key=dst_key, **create
        )["UploadId"]
        parts = {}
        lock = threading.Lock()

        def _copy_part(number):
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            start = (number - 1) * part
            end = min(start + part, size) - 1
            resp = self.client.upload_part_copy(
                Bucket=dst_bucket, Key=dst_key, UploadId=upload_id,
                PartNumber=number, CopySource=copy_source,
                CopySourceRange=f"bytes={start}-{end}", **pin,
            )
            result = resp.get("CopyPartResult") or {}
            entry = {"PartNumber": number, "ETag": result.get("ETag")}
            for field, value in result.items():
                if field.startswith("Checksum"):
                    entry[field] = value
            with lock:
                parts[number] = entry

        try:
            run_parallel(list(range(1, total_parts + 1)), _copy_part,
                         self.transfer_concurrency, cancel_event=cancel_event)
            self.client.complete_multipart_upload(
                Bucket=dst_bucket, Key=dst_key, UploadId=upload_id,
                MultipartUpload={"Parts": [parts[n] for n in sorted(parts)]},
            )
        except Exception as exc:
            # Nothing here is resumable the way an upload is: the parts are
            # copies the service made, not bytes this machine still holds, so
            # leaving them behind would only bill for an upload that nobody
            # is coming back to finish.
            try:
                self.client.abort_multipart_upload(
                    Bucket=dst_bucket, Key=dst_key, UploadId=upload_id)
            except Exception:
                pass
            if source_etag and precondition_failed(exc):
                raise Exception(
                    f"'{src_key}' changed on the server while it was being "
                    "copied; nothing was written. Copy it again to get the "
                    "new version.") from exc
            raise
        if log_fn:
            log_fn(f"copied {src_key} -> {dst_bucket}/{dst_key} server-side "
                   f"in {total_parts} part(s)")

    def copy_object(self, src_key: str, dst_key: str, dst_bucket: str = None,
                    log_fn=None, allow_stream=True, size=None,
                    cancel_event=None, storage_class=None):
        """
        Copy one object, server-side when possible, into the class it is
        already in.

        *storage_class* is what the caller already knows -- the listing the
        job row was built from, or the one the recursive copy fanned out
        from. ``None`` means nobody knows, and the source is asked; see
        _storage_class_to_preserve. Either way the three routes below land
        the object in the same class, which an unqualified CopyObject does
        not: it creates the new object as STANDARD.

        *size*, when the caller already knows it, saves the round trip that
        would otherwise be spent discovering the limit by being refused.
        """
        # Before the class is looked up: a read-only profile must refuse the
        # copy without spending a request discovering what it would have
        # copied.
        self._guard_write()
        storage_class = self._storage_class_to_preserve(src_key, storage_class)
        try:
            return self._copy_object_routed(
                src_key, dst_key, dst_bucket=dst_bucket, log_fn=log_fn,
                allow_stream=allow_stream, size=size,
                cancel_event=cancel_event, storage_class=storage_class)
        except botocore.exceptions.ClientError as exc:
            # One place for it: every route here reads the source, so every
            # route refuses an archived one, and the raw code says nothing
            # about the restore that would fix it.
            self._raise_if_archived(exc, src_key, storage_class)
            raise

    def _copy_object_routed(self, src_key: str, dst_key: str,
                            dst_bucket: str = None, log_fn=None,
                            allow_stream=True, size=None, cancel_event=None,
                            storage_class=""):
        """
        The three routes of a copy, cheapest first.

        A single CopyObject handles almost everything. A source over S3's
        5 GiB copy limit is rebuilt from UploadPartCopy ranges, which is
        still server-side. Only a cross-region or cross-account destination
        — which the source's client cannot reach at all — falls back to
        moving the bytes through this process.
        """
        self._guard_write()
        dst_bucket = dst_bucket or self.bucket
        cross_bucket = dst_bucket != self.bucket
        copy_source = {"Bucket": self.bucket, "Key": src_key}
        preserved = self._preserved_class_args(storage_class)

        def _multipart(known_size):
            try:
                # One HEAD serves both ends: the attributes the new object
                # must keep, and the version (and size) every part is pinned
                # to -- a size from a listing may describe an older one.
                head = self.client.head_object(Bucket=self.bucket, Key=src_key)
                if head.get("ContentLength") is not None:
                    known_size = int(head["ContentLength"])
                self._copy_multipart(
                    src_key, dst_key, size=known_size, dst_bucket=dst_bucket,
                    # The create args are read from the source's own HEAD,
                    # which already names its class; passing it again is
                    # what holds when the copy was asked to preserve it.
                    create_args=self._copy_create_args(
                        src_key, extra=preserved, head=head),
                    source_etag=head.get("ETag") or "",
                    log_fn=log_fn, cancel_event=cancel_event)
            except botocore.exceptions.ClientError as exc:
                # The size route and the location route are independent: a
                # destination this client cannot reach refuses the multipart
                # copy exactly as it refuses the single one, and the answer
                # is the same fallback.
                code = exc.response.get("Error", {}).get("Code", "")
                if not (allow_stream and cross_bucket
                        and code in self.CROSS_LOCATION_CODES):
                    raise
                if log_fn:
                    log_fn(f"Multipart copy refused ({code}); streaming "
                           f"'{src_key}' to bucket '{dst_bucket}' instead")
                self._stream_copy(src_key, dst_key, dst_bucket, log_fn=log_fn,
                                  storage_class=storage_class)

        if size is not None and int(size) > self.MAX_SINGLE_COPY_BYTES:
            _multipart(int(size))
            return

        def _do():
            # MetadataDirective is COPY by default and nothing here replaces
            # metadata, but saying it is the belt and braces
            # change_storage_class() wears for the same request.
            self.client.copy_object(
                CopySource=copy_source, Bucket=dst_bucket, Key=dst_key,
                **(dict(preserved, MetadataDirective="COPY")
                   if preserved else {}))

        try:
            _do()
            return
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if preserved and code in self.UNSUPPORTED_STORAGE_CLASS_CODES:
                # A backend that names a class in its own listing and then
                # refuses it on a copy. Preserving the class is worth a
                # request, never the copy itself, so the whole thing is
                # retried as the plain copy it was before -- fallbacks and
                # all, since this attempt never got far enough to use them.
                if log_fn:
                    log_fn(f"'{storage_class}' was refused ({code}); copying "
                           f"'{src_key}' without preserving its class")
                return self._copy_object_routed(
                    src_key, dst_key, dst_bucket=dst_bucket, log_fn=log_fn,
                    allow_stream=allow_stream, size=size,
                    cancel_event=cancel_event, storage_class="STANDARD")
            oversized = self._oversized_copy_source(exc, src_key)
            if oversized:
                if log_fn:
                    log_fn(f"'{src_key}' is over the {self.MAX_SINGLE_COPY_BYTES} "
                           f"byte single-copy limit; copying it in parts")
                _multipart(oversized)
                return
            if (allow_stream and cross_bucket
                    and code in self.CROSS_LOCATION_CODES):
                if log_fn:
                    log_fn(
                        f"Server-side copy refused ({code}); streaming "
                        f"'{src_key}' to bucket '{dst_bucket}' instead"
                    )
                self._stream_copy(src_key, dst_key, dst_bucket, log_fn=log_fn,
                                  storage_class=storage_class)
                return
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error copying '{src_key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error copying '{src_key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def copy_prefix(self, src_prefix: str, dst_prefix: str, dst_bucket: str = None,
                    log_fn=None, cancel_event=None):
        """Server-side recursive copy of all objects under src_prefix to dst_prefix."""
        self._guard_write()
        dst_bucket = dst_bucket or self.bucket
        if dst_bucket == self.bucket:
            sp = src_prefix if src_prefix.endswith("/") else src_prefix + "/"
            dp = dst_prefix if dst_prefix.endswith("/") else dst_prefix + "/"
            # A destination inside the source would nest the tree into itself;
            # on a move the follow-up delete of src_prefix then wipes the copy.
            if dp.startswith(sp):
                raise ValueError(
                    f"Destination '{dst_prefix}' is inside source "
                    f"'{src_prefix}'; copying a folder into itself is not allowed"
                )
        def _copy_one(item):
            key, size, storage_class = item
            rel = key[len(src_prefix):]
            dst_key = dst_prefix + rel
            if log_fn:
                log_fn(f"copying {key} -> {dst_key}")
            # The listing already knows the size and the class, so an object
            # over the single-copy limit goes straight to the multipart route
            # rather than spending a request to be told it is too big, and no
            # object costs a HEAD to be told what tier it is already in.
            self.copy_object(key, dst_key, dst_bucket=dst_bucket,
                             log_fn=log_fn, size=size,
                             storage_class=storage_class,
                             cancel_event=cancel_event)

        keys = [(k, s, c) for k, s, c
                in self.get_keys_with_class(src_prefix, log_fn=log_fn) if k]
        run_parallel(keys, _copy_one, self.parallel_files,
                     cancel_event=cancel_event)
