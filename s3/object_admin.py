"""One object's metadata, tags, class, versions and integrity.
"""

import botocore.exceptions
import os
from core.errors import TransferCancelled
from s3.common import (
    CHECKSUM_ALGORITHMS, CHECKSUM_DIGESTS, KNOWN_CHECKSUM_ALGORITHMS,
)
from urllib.parse import quote


class ObjectAdminMixin:
    """One object's metadata, tags, class, versions and integrity.
    """

    @staticmethod
    def file_checksum(path, algorithm, chunk_size: int = 1024 * 1024) -> str:
        """
        The object checksum S3 would report for this file, base64-encoded.

        Only algorithms this machine can recompute are accepted — a checksum
        we cannot reproduce is worse than none, because it would silently
        pass every download instead of comparing anything.
        """
        name = str(algorithm or "").upper()
        factory = CHECKSUM_DIGESTS.get(name)
        if factory is None:
            raise ValueError(f"unsupported checksum algorithm: {algorithm}")
        digest = factory()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(chunk_size), b""):
                digest.update(block)
        return digest.b64()

    def get_object_parts(self, key: str, version_id: str = "") -> list:
        """
        The part boundaries and per-part checksums of a multipart object.

        This is the piece that makes a multipart object verifiable at all: an
        ETag or a composite checksum cannot be reproduced without knowing how
        the object was split. Returns [] when the backend does not implement
        GetObjectAttributes (most S3-compatible ones do not) or when the
        object is not multipart, so callers degrade to "not comparable"
        rather than failing a transfer.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        parts = []
        marker = 0
        while True:
            params = {
                "Bucket": self.bucket,
                "Key": key,
                "ObjectAttributes": ["ObjectParts"],
                "MaxParts": 1000,
            }
            if version_id:
                params["VersionId"] = version_id
            if marker:
                params["PartNumberMarker"] = marker
            try:
                resp = self.client.get_object_attributes(**params)
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in ("NotImplemented", "InvalidRequest",
                            "MethodNotAllowed", "AccessDenied",
                            "InvalidArgument", "UnsupportedOperation"):
                    return []
                raise
            except botocore.exceptions.ParamValidationError:
                # An older botocore without the operation at all.
                return []
            node = resp.get("ObjectParts") or {}
            for part in node.get("Parts") or []:
                entry = {"PartNumber": int(part.get("PartNumber") or 0),
                         "Size": int(part.get("Size") or 0)}
                for field, value in part.items():
                    if field.startswith("Checksum"):
                        entry[field] = value
                parts.append(entry)
            if not node.get("IsTruncated"):
                break
            try:
                following = int(node.get("NextPartNumberMarker") or 0)
            except (TypeError, ValueError):
                break
            # Same guard as _uploaded_parts: a marker that does not advance
            # would spin this thread forever.
            if following <= marker:
                break
            marker = following
        parts.sort(key=lambda entry: entry["PartNumber"])
        return parts

    @classmethod
    def file_part_digests(cls, path, sizes, algorithm="") -> tuple:
        """
        Per-part digests of a local file, split at the given part sizes.

        Returns ``(checksums, md5_digests)``. Raises ValueError when the sizes
        do not add up to the file: a mismatch there means the parts describe a
        different object, and comparing anyway would report a false pass.
        """
        total = sum(int(size) for size in sizes)
        actual = os.path.getsize(path)
        if total != actual:
            raise ValueError(
                f"part sizes total {total} bytes but the file is {actual}")
        checksums = []
        md5s = []
        with open(path, "rb") as handle:
            for size in sizes:
                checksum, md5 = cls._digest_range(handle, int(size), algorithm)
                checksums.append(checksum)
                md5s.append(md5)
        return checksums, md5s

    def verify_parts(self, path, parts, etag="", algorithm="", log_fn=None):
        """
        Check a downloaded file against a multipart object's real parts.

        Returns True (verified), False (mismatch) or None (nothing to compare
        with — no parts, or no per-part digest to check them against).
        """
        parts = list(parts or [])
        if not parts:
            return None
        sizes = [int(part.get("Size") or 0) for part in parts]
        if any(size <= 0 for size in sizes):
            return None
        field = f"Checksum{str(algorithm or '').upper()}"
        expected = [part.get(field) for part in parts]
        have_checksums = bool(algorithm) and all(expected)
        try:
            checksums, md5s = self.file_part_digests(
                path, sizes, algorithm if have_checksums else "")
        except (ValueError, OSError) as exc:
            if log_fn:
                log_fn(f"part verification skipped ({exc}): {path}")
            return None

        if have_checksums:
            for number, (actual, wanted) in enumerate(
                    zip(checksums, expected), start=1):
                if actual != wanted:
                    if log_fn:
                        log_fn(f"CHECKSUM MISMATCH in part {number} of {path}: "
                               f"{algorithm} {actual} != {wanted}")
                    return False
            if log_fn:
                log_fn(f"checksum ok ({algorithm}, {len(parts)} parts): {path}")
            return True

        wanted = self.normalize_etag(etag)
        if not wanted or "-" not in wanted:
            return None
        actual = self.composite_etag(md5s)
        if actual != wanted:
            if log_fn:
                log_fn(f"CHECKSUM MISMATCH for {path}: {actual} != {wanted}")
            return False
        if log_fn:
            log_fn(f"checksum ok (multipart ETag, {len(parts)} parts): {path}")
        return True

    def head_with_checksum(self, key) -> dict:
        """
        head_object asking for the stored checksum.

        ChecksumMode is a newer parameter, and S3-compatible backends that
        reject it would otherwise turn every download's HEAD into an error, so
        an unrecognised-parameter failure retries without it.
        """
        try:
            return self.client.head_object(
                Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        except TransferCancelled:
            raise
        except Exception as exc:
            if self._is_region_error(exc):
                raise
            return self.client.head_object(Bucket=self.bucket, Key=key)

    @staticmethod
    def stored_checksum(head) -> tuple:
        """
        (algorithm, value) from a head_object response, or ("", "").

        A checksum this machine can recompute is preferred over one it
        cannot: an object carrying only a CRC64NVME on a build without awscrt
        must not shadow an ETag that is still perfectly comparable. The
        uncomputable one is still returned when it is all there is, so the
        caller can say why nothing was compared.
        """
        head = head or {}
        for name in CHECKSUM_ALGORITHMS:
            value = head.get(f"Checksum{name}")
            if value:
                return name, str(value)
        for name in KNOWN_CHECKSUM_ALGORITHMS:
            if name in CHECKSUM_DIGESTS:
                continue
            value = head.get(f"Checksum{name}")
            if value:
                return name, str(value)
        return "", ""

    def _verify_by_parts(self, path, key, etag, algorithm, log_fn=None):
        """
        Settle a multipart digest using the object's real part boundaries.

        Returns True/False, or None when the backend cannot supply them — in
        which case the caller falls back to reporting "not comparable".
        """
        if not key:
            return None
        try:
            parts = self.get_object_parts(key)
        except Exception as exc:
            if log_fn:
                log_fn(f"could not read the parts of '{key}': {exc}")
            return None
        return self.verify_parts(
            path, parts, etag=etag, algorithm=algorithm, log_fn=log_fn)

    def list_object_versions(self, key: str) -> list:
        """
        Return every stored version and delete marker for a single object key,
        newest first. Each entry is a dict:
          {version_id, last_modified, size, is_latest, storage_class,
           is_delete_marker, etag}
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if not key or key.endswith("/"):
            raise ValueError("Versions are only available for a single object")

        def _fetch():
            out = []
            paginator = self.client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                for v in (page.get("Versions", []) or []):
                    if v.get("Key") != key:
                        continue
                    out.append({
                        "version_id": v.get("VersionId") or "null",
                        "last_modified": v.get("LastModified"),
                        "size": int(v.get("Size") or 0),
                        "is_latest": bool(v.get("IsLatest")),
                        "storage_class": v.get("StorageClass") or "STANDARD",
                        "is_delete_marker": False,
                        "etag": (v.get("ETag") or "").replace('"', ""),
                    })
                for dm in (page.get("DeleteMarkers", []) or []):
                    if dm.get("Key") != key:
                        continue
                    out.append({
                        "version_id": dm.get("VersionId") or "null",
                        "last_modified": dm.get("LastModified"),
                        "size": 0,
                        "is_latest": bool(dm.get("IsLatest")),
                        "storage_class": "",
                        "is_delete_marker": True,
                        "etag": "",
                    })
            # Newest first; entries without a timestamp sink to the bottom.
            out.sort(
                key=lambda e: (e["last_modified"] is not None, e["last_modified"]),
                reverse=True,
            )
            return out

        try:
            return _fetch()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            self.rebind_bucket()
            return _fetch()

    @staticmethod
    def _sse_args(head) -> dict:
        """
        The encryption a copy onto itself has to restate in order to keep it.

        CopyObject does not inherit the source's encryption: a copy that names
        none is encrypted with the bucket's DEFAULT. An object written under
        its own KMS key therefore came back SSE-S3 after a storage-class
        change, a metadata edit or a version restore -- readable by anyone
        with s3:GetObject, with no kms:Decrypt on the key standing in the way
        any more. SSE-C is not covered: its HEAD needs the customer key, which
        this app never has, so such an object keeps behaving as before.
        """
        head = head or {}
        algorithm = str(head.get("ServerSideEncryption") or "")
        if not algorithm:
            return {}
        args = {"ServerSideEncryption": algorithm}
        # Both are KMS-only; sending either beside AES256 is refused.
        if algorithm.startswith("aws:kms"):
            if head.get("SSEKMSKeyId"):
                args["SSEKMSKeyId"] = str(head["SSEKMSKeyId"])
            if head.get("BucketKeyEnabled"):
                args["BucketKeyEnabled"] = True
        return args

    def _head_for_rewrite(self, key, version_id="") -> dict:
        """
        HEAD of an object about to be copied onto itself, or {}.

        Best effort, like _copy_create_args: a HEAD the credentials cannot
        make leaves the copy exactly as it was rather than refusing it. A
        region error is the exception -- it is let out so the caller's
        rebind-and-retry sees it, instead of the retry going out with no
        encryption because the first HEAD went to the wrong place.
        """
        params = {"Bucket": self.bucket, "Key": key}
        if version_id:
            params["VersionId"] = version_id
        try:
            return self.client.head_object(**params) or {}
        except Exception as exc:
            if self._is_region_error(exc):
                raise
            return {}

    def make_version_current(self, key, version_id, log_fn=None):
        """
        Promote an older version to be the current one by copying that version
        onto the same key server-side. This creates a new current version with
        the older version's data (the standard S3 "restore a previous version"
        technique); nothing is deleted.

        The promoted version keeps the encryption it was stored with; see
        _sse_args for what an unqualified copy would do instead.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        copy_source = {"Bucket": self.bucket, "Key": key, "VersionId": version_id}

        def _do():
            sse = self._sse_args(self._head_for_rewrite(key, version_id))
            self.client.copy_object(
                CopySource=copy_source, Bucket=self.bucket, Key=key, **sse
            )

        try:
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error restoring version of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def presigned_get_url(self, key: str, expires_sec: int = 3600) -> str:
        """Return a temporary download (HTTP GET) URL for an object."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_sec,
        )

    def presigned_put_url(self, key: str, expires_sec: int = 3600) -> str:
        """Return a temporary upload (HTTP PUT) URL for an object key.

        Anyone with the link can upload to this exact key until it expires,
        e.g. `curl --upload-file localfile "<url>"`.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        return self.client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_sec,
        )

    def make_object_public(self, key: str) -> tuple[bool, str | None]:
        """
        Try to make the object publicly readable.

        Returns:
          (True, None) if ACL applied
          (False, reason) if ACL could not be applied (e.g., MinIO NotImplemented)
        """
        self._guard_write()
        if not self.bucket:
            return False, "Bucket is empty; select a bucket first"

        try:
            self.client.put_object_acl(Bucket=self.bucket, Key=key, ACL="public-read")
            return True, None
        except botocore.exceptions.ClientError as exc:
            err = exc.response.get("Error", {})
            code = err.get("Code") or ""
            msg = err.get("Message") or str(exc)

            # MinIO commonly returns NotImplemented for PutObjectAcl
            if code == "NotImplemented":
                return False, "Storage backend does not support ACLs (MinIO NotImplemented)."
            return False, f"{code}: {msg}".strip(": ")
        except Exception as exc:
            return False, str(exc)

    def direct_object_url(self, key: str) -> str:
        """
        Construct a direct (unsigned) URL for an object.

        After binding a bucket, self.endpoint_url may be virtual-host style
        (e.g. https://mybucket.s3.region.amazonaws.com) — in that case the
        bucket name is already part of the host, so we must NOT add it again
        as a path segment (that produced .../mybucket/mybucket/key).

        The key is percent-encoded on the way in. It is an arbitrary byte
        string, and pasting one containing "#" or "?" straight into a URL
        truncates the link at that character: "notes#2.txt" addressed the
        object "notes" with a fragment. Slashes stay literal — they are the
        path separators the object hierarchy is built from.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        key = quote((key or "").lstrip("/"), safe="/")
        # A CDN or custom domain is mapped at the bucket root, so the bucket
        # name is never part of the path there. Presigned links deliberately
        # do NOT go through this: their signature covers the host.
        if self.public_base_url:
            return f"{self.public_base_url}/{key}"
        ep = self.endpoint_url.rstrip("/")
        if self._endpoint_has_bucket(ep, self.bucket):
            # virtual-host style: bucket already in the hostname
            return f"{ep}/{key}"
        return f"{ep}/{self.bucket}/{key}"

    def object_properties(self, key: str):
        """
        Return object metadata without opening a body stream.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty")

        return self.client.head_object(Bucket=self.bucket, Key=key)

    def get_object_lock_state(self, key: str, version_id: str = "") -> dict:
        """
        Retention and legal-hold state for one object.

        Returns ``{"mode", "retain_until", "legal_hold", "supported"}``;
        supported is False on a bucket (or backend) without Object Lock, which
        is the common case and not an error.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        state = {"mode": "", "retain_until": "", "legal_hold": "",
                 "supported": True}
        params = {"Bucket": self.bucket, "Key": key}
        if version_id:
            params["VersionId"] = version_id
        try:
            retention = self.client.get_object_retention(**params)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("InvalidRequest", "NotImplemented",
                        "ObjectLockConfigurationNotFoundError"):
                state["supported"] = False
                return state
            if code not in ("NoSuchObjectLockConfiguration",
                            "NoSuchObjectLockConfigurationError"):
                raise
            retention = {}
        node = (retention or {}).get("Retention") or {}
        state["mode"] = str(node.get("Mode") or "")
        retain = node.get("RetainUntilDate")
        state["retain_until"] = "" if retain is None else str(retain)
        try:
            hold = self.client.get_object_legal_hold(**params)
            state["legal_hold"] = str(
                (hold.get("LegalHold") or {}).get("Status") or "")
        except botocore.exceptions.ClientError:
            # A bucket with retention but no hold set answers with an error;
            # an absent hold is simply OFF.
            state["legal_hold"] = state["legal_hold"] or ""
        return state

    def set_object_legal_hold(self, key: str, on: bool, version_id: str = "",
                              log_fn=None):
        """Place or clear a legal hold on one object."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        params = {"Bucket": self.bucket, "Key": key,
                  "LegalHold": {"Status": "ON" if on else "OFF"}}
        if version_id:
            params["VersionId"] = version_id
        self.client.put_object_legal_hold(**params)
        if log_fn:
            log_fn(f"legal hold {'ON' if on else 'OFF'}: {key}")

    def list_multipart_uploads(self, prefix: str = "", *, bucket_name: str = None,
                               client_obj=None, with_sizes: bool = False,
                               cancel_event=None, log_fn=None) -> list:
        """
        Return in-flight (incomplete) multipart uploads, newest first. Each
        entry is a dict:
          {key, upload_id, initiated, storage_class, size}

        These are invisible to normal listings but keep billing for the parts
        already uploaded, and they block DeleteBucket. 'size' is the summed
        part size, only fetched when with_sizes (one ListParts call each).
        """
        bucket = bucket_name or self.bucket
        if not bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        wanted = prefix or ""

        def _fetch():
            client = client_obj if client_obj is not None else self.client
            out = []
            paginator = client.get_paginator("list_multipart_uploads")
            # Prefix is filtered HERE, not by the server. MinIO treats the
            # ListMultipartUploads Prefix as an exact key match: asking for
            # "photos/" returns nothing while "photos/big.bin" returns that
            # one upload. Sending it therefore reported "no incomplete
            # uploads" for every prefix — which is where the dialog is
            # opened from, so the feature quietly did nothing everywhere
            # except a bucket root. The list is bounded by in-flight uploads,
            # not by objects, so listing them all is cheap.
            for page in paginator.paginate(Bucket=bucket):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for up in (page.get("Uploads", []) or []):
                    key = up.get("Key")
                    upload_id = up.get("UploadId")
                    if not key or not upload_id:
                        continue
                    if wanted and not key.startswith(wanted):
                        continue
                    out.append({
                        "key": key,
                        "upload_id": upload_id,
                        "initiated": up.get("Initiated"),
                        "storage_class": up.get("StorageClass") or "",
                        "size": None,
                    })

            if with_sizes:
                for entry in out:
                    if cancel_event is not None and cancel_event.is_set():
                        raise TransferCancelled("cancelled")
                    try:
                        entry["size"] = self._multipart_upload_size(
                            client, bucket, entry["key"], entry["upload_id"]
                        )
                    except Exception as exc:
                        if log_fn:
                            log_fn(f"Could not size upload {entry['key']}: {exc}")

            # Newest first; entries without a timestamp sink to the bottom.
            out.sort(
                key=lambda e: (e["initiated"] is not None, e["initiated"]),
                reverse=True,
            )
            return out

        try:
            return _fetch()
        except TransferCancelled:
            raise
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error listing multipart uploads: {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return _fetch()

    def abort_multipart_upload(self, key: str, upload_id: str,
                               bucket_name: str = None, log_fn=None):
        """Abort one in-flight multipart upload and free its stored parts."""
        self._guard_write()
        bucket = bucket_name or self.bucket
        if not bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        def _do():
            self.client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id
            )

        try:
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error aborting upload of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def update_object_tags(self, key: str, add=None, remove=None,
                           replace=False) -> list:
        """Read-modify-write one object's tags. Returns the stored TagSet."""
        self._guard_write()
        current = [] if replace else self.get_object_tags(key)
        tags = self.merge_tags(current, add=add, remove=remove, replace=replace)
        self.put_object_tags(key, tags)
        return tags

    def get_object_tags(self, key: str) -> list:
        """Return the TagSet for an object as a list of {'Key': k, 'Value': v} dicts."""
        resp = self.client.get_object_tagging(Bucket=self.bucket, Key=key)
        return resp.get("TagSet", [])

    def put_object_tags(self, key: str, tags: list):
        """Replace the full TagSet for an object."""
        self._guard_write()
        self.client.put_object_tagging(
            Bucket=self.bucket,
            Key=key,
            Tagging={"TagSet": tags},
        )

    def restore_object(self, key: str, days: int = 7,
                       tier: str = "Standard") -> tuple[bool, str | None]:
        """
        Initiate a restore of an archived (Glacier / Deep Archive) object.

        Returns:
          (True, None)      restore initiated
          (False, reason)   could not initiate (already running, unsupported, …)
        """
        self._guard_write()
        if not self.bucket:
            return False, "Bucket is empty; select a bucket first"
        req = {"Days": int(days)}
        if tier:
            req["GlacierJobParameters"] = {"Tier": tier}
        try:
            self.client.restore_object(
                Bucket=self.bucket, Key=key, RestoreRequest=req
            )
            return True, None
        except botocore.exceptions.ClientError as exc:
            err = exc.response.get("Error", {})
            code = err.get("Code") or ""
            msg = err.get("Message") or str(exc)
            if code == "RestoreAlreadyInProgress":
                return False, "Restore is already in progress for this object."
            if code == "NotImplemented":
                return False, "Storage backend does not support restore (NotImplemented)."
            return False, f"{code}: {msg}".strip(": ")
        except Exception as exc:
            return False, str(exc)

    def current_storage_class(self, key: str) -> str:
        """An object's storage class, with S3's silence read as STANDARD.

        HeadObject omits StorageClass entirely for a STANDARD object rather
        than naming it, so an absent field and "STANDARD" are the same
        answer.
        """
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        return head.get("StorageClass") or "STANDARD"

    def change_storage_class(self, key: str, storage_class: str, log_fn=None):
        """
        Change an object's storage class via a server-side copy onto itself.

        Returns True when the object moved, False when it was already in the
        requested class. S3 refuses a copy onto itself that changes nothing —
        "This copy request is illegal…", an InvalidRequest, on AWS and MinIO
        alike — so normalising a selection to STANDARD failed on every object
        already in STANDARD. That is a no-op, not an error.

        The happy path costs a HEAD and the copy. It used to be the copy
        alone, and that copy re-encrypted every object under the bucket's
        default -- see _sse_args. The same HEAD answers "already there?"
        when the service objects, so the no-op path costs no more than before.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        copy_source = {"Bucket": self.bucket, "Key": key}
        head = {}

        def _do():
            # Read on every attempt: after a rebind, the HEAD the first try
            # made may have been the request that went to the wrong place.
            head.clear()
            head.update(self._head_for_rewrite(key))
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=key,
                StorageClass=storage_class,
                MetadataDirective="COPY",
                **self._sse_args(head),
            )

        def _already_there():
            if head:
                return (head.get("StorageClass") or "STANDARD") == storage_class
            try:
                return self.current_storage_class(key) == storage_class
            except Exception:
                return False

        try:
            _do()
            return True
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            # Order matters: an object already in the requested class also
            # answers InvalidRequest, and rewriting a 50 GiB object in parts
            # to change nothing is an expensive way to say "no-op".
            if code == "InvalidRequest" and _already_there():
                if log_fn:
                    log_fn(f"{key} is already {storage_class}")
                return False
            oversized = self._oversized_copy_source(exc, key)
            if oversized:
                # A copy onto itself, in parts. The class is the whole point
                # of the copy, so it is the one thing the source must not
                # supply back.
                if log_fn:
                    log_fn(f"'{key}' is over the single-copy limit; "
                           f"re-writing it in parts as {storage_class}")
                self._copy_multipart(
                    key, key, size=oversized,
                    create_args=self._copy_create_args(
                        key, extra={"StorageClass": storage_class}),
                    log_fn=log_fn)
                return True
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error changing storage class of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()
            return True
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error changing storage class of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()
            return True

    def get_object_metadata(self, key: str) -> dict:
        """
        Return editable metadata for an object:
          {content_type, cache_control, content_disposition, content_encoding,
           storage_class, metadata (user x-amz-meta-* dict),
           sse (the encryption arguments a rewrite must restate)}
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self.client.head_object(Bucket=self.bucket, Key=key)
        return {
            "content_type": resp.get("ContentType", "") or "",
            "cache_control": resp.get("CacheControl", "") or "",
            "content_disposition": resp.get("ContentDisposition", "") or "",
            "content_encoding": resp.get("ContentEncoding", "") or "",
            "storage_class": resp.get("StorageClass") or "STANDARD",
            "metadata": dict(resp.get("Metadata", {}) or {}),
            # Read here so a caller about to rewrite the object can hand it
            # back to set_object_metadata instead of paying a second HEAD.
            "sse": self._sse_args(resp),
        }

    def set_object_metadata(self, key: str, *, content_type=None,
                            cache_control=None, content_disposition=None,
                            content_encoding=None, metadata=None,
                            storage_class=None, sse=None, log_fn=None):
        """
        Replace an object's system + user metadata via a server-side copy onto
        itself with MetadataDirective=REPLACE. The current storage class is
        preserved when passed in (a REPLACE copy would otherwise reset it to
        STANDARD).

        So is its encryption: *sse* is what get_object_metadata returned under
        "sse". When it is not passed the object is HEADed for it, because a
        REPLACE copy that names no encryption takes the bucket's default.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        params = {
            "CopySource": {"Bucket": self.bucket, "Key": key},
            "Bucket": self.bucket,
            "Key": key,
            "MetadataDirective": "REPLACE",
        }
        # ContentType must always be sent on a REPLACE copy or S3 defaults it
        # to binary/octet-stream.
        params["ContentType"] = content_type or ""
        if cache_control:
            params["CacheControl"] = cache_control
        if content_disposition:
            params["ContentDisposition"] = content_disposition
        if content_encoding:
            params["ContentEncoding"] = content_encoding
        if metadata is not None:
            params["Metadata"] = {str(k): str(v) for k, v in metadata.items()}
        if storage_class and storage_class != "STANDARD":
            params["StorageClass"] = storage_class
        if sse is not None:
            params.update(sse)

        def _do():
            if sse is None:
                # Inside the attempt, so a retry after a rebind reads it again
                # from the right place. What lands in params also reaches the
                # in-parts route below, which builds its upload from them.
                params.update(self._sse_args(self._head_for_rewrite(key)))
            self.client.copy_object(**params)

        try:
            _do()
        except botocore.exceptions.ClientError as exc:
            oversized = self._oversized_copy_source(exc, key)
            if oversized:
                # The same REPLACE, in parts. CreateMultipartUpload takes the
                # replacement fields directly, so the source is not consulted
                # at all here — a REPLACE copy was never going to keep what
                # the caller did not pass.
                if log_fn:
                    log_fn(f"'{key}' is over the single-copy limit; "
                           f"re-writing its metadata in parts")
                create = {field: value for field, value in params.items()
                          if field not in ("CopySource", "Bucket", "Key",
                                           "MetadataDirective")}
                self._copy_multipart(key, key, size=oversized,
                                     create_args=create, log_fn=log_fn)
                return
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error setting metadata of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error setting metadata of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def retype_object(self, key: str, log_fn=None):
        """
        Re-stamp one existing object's Content-Type from its key.

        Returns ``(changed, content_type)``. A key the tables cannot name is
        left alone rather than stamped with the fallback: rewriting every
        extensionless object to binary/octet-stream costs a copy and changes
        nothing.
        """
        self._guard_write()
        guessed = self.guess_content_type(key)
        if not guessed:
            return False, ""
        current = self.get_object_metadata(key)
        if (current.get("content_type") or "") == guessed:
            return False, guessed
        self.set_object_metadata(
            key,
            content_type=guessed,
            cache_control=current.get("cache_control"),
            content_disposition=current.get("content_disposition"),
            content_encoding=current.get("content_encoding"),
            metadata=current.get("metadata"),
            storage_class=current.get("storage_class"),
            sse=current.get("sse"),
            log_fn=log_fn,
        )
        return True, guessed

    def list_object_digests(self, prefix: str = "", cancel_event=None,
                            log_fn=None) -> list:
        """
        Return ``[(key, size, etag, last_modified), ...]`` for every object
        under 'prefix'. ETag and size both come back with ListObjectsV2, so
        content comparison costs no extra request.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        def _fetch():
            out = []
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix or ""):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key") or ""
                    if not key or key.endswith("/"):
                        continue
                    out.append((
                        key,
                        int(obj.get("Size") or 0),
                        self.normalize_etag(obj.get("ETag")),
                        obj.get("LastModified"),
                    ))
            return out

        try:
            return _fetch()
        except TransferCancelled:
            raise
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error scanning '{prefix}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return _fetch()

    def object_fingerprint(self, key: str, version_id: str = "") -> tuple:
        """
        A content fingerprint built from metadata alone, as ``(kind, value)``.

        kind "full"  — a whole-object digest: equal means the same bytes, and
                       different means different bytes.
        kind "parts" — per-part digests: equal still means the same bytes, but
                       different is inconclusive, since identical content split
                       at other boundaries digests differently.
        kind ""      — the service will not answer without the bytes.

        This is what lets the duplicate finder settle a "same size, ETags
        cannot compare" candidate without downloading either copy.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        params = {
            "Bucket": self.bucket,
            "Key": key,
            "ObjectAttributes": ["ETag", "Checksum", "ObjectParts"],
            "MaxParts": 1000,
        }
        if version_id:
            params["VersionId"] = version_id
        try:
            resp = self.client.get_object_attributes(**params)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NotImplemented", "InvalidRequest", "MethodNotAllowed",
                        "AccessDenied", "InvalidArgument",
                        "UnsupportedOperation"):
                return "", ""
            raise
        except botocore.exceptions.ParamValidationError:
            return "", ""

        checksum = resp.get("Checksum") or {}
        # Every algorithm S3 may have stored counts here, not only the ones
        # this build can recompute: comparing two stored digests to each
        # other needs no local implementation of the algorithm at all.
        for name in KNOWN_CHECKSUM_ALGORITHMS:
            value = checksum.get(f"Checksum{name}")
            if value and not self.checksum_is_composite(value):
                return "full", f"{name}:{value}"

        etag = self.normalize_etag(resp.get("ETag"))
        if etag and self.etag_is_md5(etag):
            # A single-part object's ETag is the MD5 of its bytes.
            return "full", f"MD5:{etag}"

        node = resp.get("ObjectParts") or {}
        parts = []
        for part in node.get("Parts") or []:
            digest = ""
            for name in KNOWN_CHECKSUM_ALGORITHMS:
                if part.get(f"Checksum{name}"):
                    digest = f"{name}:{part[f'Checksum{name}']}"
                    break
            if not digest:
                return "", ""
            parts.append(f"{int(part.get('Size') or 0)}:{digest}")
        if parts and not node.get("IsTruncated"):
            return "parts", "|".join(parts)
        return "", ""
