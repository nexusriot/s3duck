"""Deleting objects, versions, prefixes and whole buckets.
"""

import botocore.exceptions
from core.errors import TransferCancelled
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime


class DeleteMixin:
    """Deleting objects, versions, prefixes and whole buckets.
    """

    # Codes a backend answers DeleteObjects with when it has no multi-object
    # delete at all. Every caller falls back to one DeleteObject per key.
    MULTI_DELETE_UNSUPPORTED_CODES = ("NotImplemented", "MethodNotAllowed")

    # How far a delete marker may predate the recorded start of a delete and
    # still be taken as that delete's own. LastModified and the Date header
    # both have one-second resolution, and the request that wrote the marker
    # was in flight for a moment after the start was recorded.
    UNDO_SLACK = timedelta(seconds=5)

    def delete_bucket_recursive(self, bucket_name: str, *, batch_size: int = 1000,
                                cancel_event=None, log_fn=None):
        """
        Delete bucket even if non-empty: first remove all objects (recursive),
        then delete the bucket itself.

        Also aborts any in-flight multipart uploads: their orphaned parts are
        invisible to ListObjectsV2 and keep DeleteBucket failing.
        """
        self._guard_write()
        bucket_name = (bucket_name or "").strip()
        if not bucket_name:
            raise ValueError("bucket_name is empty")

        # Use a client that can actually access this bucket (endpoint/style probing)
        bucket_client, _endpoint, _region, _use_path = self._try_bind_bucket(bucket_name)

        self._purge_bucket_contents(
            bucket_name, bucket_client, batch_size=batch_size,
            cancel_event=cancel_event, log_fn=log_fn,
        )

        # If we're currently "in" that bucket, reset view state first
        if self.bucket == bucket_name:
            self.bucket = ""
            self.current_folder = ""
            self.prev_folder = ""

        # Delete with the *bound* client: a profile/root client raises
        # PermanentRedirect for buckets in another region, which would leave
        # the bucket emptied but not deleted.
        bucket_client.delete_bucket(Bucket=bucket_name)

    def empty_bucket(self, bucket_name: str, *, batch_size: int = 1000,
                     cancel_event=None, log_fn=None):
        """Remove everything inside a bucket but keep the bucket itself."""
        self._guard_write()
        bucket_name = (bucket_name or "").strip()
        if not bucket_name:
            raise ValueError("bucket_name is empty")
        bucket_client, _endpoint, _region, _use_path = self._try_bind_bucket(bucket_name)
        self._purge_bucket_contents(
            bucket_name, bucket_client, batch_size=batch_size,
            cancel_event=cancel_event, log_fn=log_fn,
        )
        if self.bucket == bucket_name:
            self.current_folder = ""
            self.prev_folder = ""

    def _purge_bucket_contents(self, bucket_name, bucket_client, *,
                               batch_size=1000, cancel_event=None, log_fn=None):
        """
        Delete every object, noncurrent version, delete marker and in-flight
        multipart upload in a bucket, leaving the bucket empty.
        """
        def _check_cancel():
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")

        _check_cancel()

        paginator = bucket_client.get_paginator("list_objects_v2")

        pending = []
        deleted = 0
        # Set once the backend has refused DeleteObjects outright, so every
        # later batch goes straight to single deletes instead of being
        # refused again first. Deleting a folder already fell back like this
        # (_delete_batch); emptying the whole bucket did not, so on the same
        # backend a folder could be deleted and its bucket could not.
        one_by_one = False

        def flush():
            nonlocal pending, deleted, one_by_one
            if not pending:
                return
            batch, pending = pending, []
            resp = None
            if not one_by_one:
                try:
                    resp = bucket_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": batch, "Quiet": True},
                    )
                except botocore.exceptions.ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    if code not in self.MULTI_DELETE_UNSUPPORTED_CODES:
                        raise
                    one_by_one = True
                    if log_fn:
                        log_fn(f"{bucket_name}: batch delete unsupported here; "
                               "deleting one by one")
            if one_by_one:
                for entry in batch:
                    _check_cancel()
                    # entry is {"Key"} or {"Key", "VersionId"} -- exactly
                    # DeleteObject's own arguments.
                    bucket_client.delete_object(Bucket=bucket_name, **entry)
            # Counted before the check so the log says how far the purge got;
            # a refused key would otherwise be reported as deleted and the
            # DeleteBucket that follows would fail with an unexplained
            # BucketNotEmpty.
            deleted += len(batch)
            if log_fn:
                log_fn(f"{bucket_name}: deleted {deleted} object(s)")
            self.raise_on_delete_errors(resp, log_fn=log_fn)

        # List everything
        for page in paginator.paginate(Bucket=bucket_name, Prefix=""):
            _check_cancel()
            for obj in (page.get("Contents", []) or []):
                k = obj.get("Key")
                if not k:
                    continue
                pending.append({"Key": k})
                if len(pending) >= int(batch_size or 1000):
                    flush()

        flush()

        # Second pass: purge any remaining noncurrent versions and delete
        # markers. On a versioning-enabled bucket these survive the pass above
        # and AWS refuses DeleteBucket until they are gone. Backends that do
        # not implement the versions API (some S3-compatible stores) simply
        # report NotImplemented, which we ignore.
        try:
            vpaginator = bucket_client.get_paginator("list_object_versions")
            for page in vpaginator.paginate(Bucket=bucket_name):
                _check_cancel()
                for v in (page.get("Versions", []) or []):
                    if not v.get("Key") or not v.get("VersionId"):
                        continue
                    pending.append({"Key": v["Key"], "VersionId": v["VersionId"]})
                    if len(pending) >= int(batch_size or 1000):
                        flush()
                for dm in (page.get("DeleteMarkers", []) or []):
                    if not dm.get("Key") or not dm.get("VersionId"):
                        continue
                    pending.append({"Key": dm["Key"], "VersionId": dm["VersionId"]})
                    if len(pending) >= int(batch_size or 1000):
                        flush()
            flush()
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("NotImplemented", "NoSuchBucket"):
                raise

        # In-flight multipart uploads hold parts that ListObjectsV2 never
        # reports, and S3 refuses DeleteBucket while they exist.
        _check_cancel()
        try:
            for up in self.list_multipart_uploads(bucket_name=bucket_name,
                                                  client_obj=bucket_client):
                _check_cancel()
                if log_fn:
                    log_fn(f"{bucket_name}: aborting multipart upload {up['key']}")
                bucket_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=up["key"], UploadId=up["upload_id"],
                )
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("NotImplemented", "NoSuchBucket"):
                raise

    def delete_bucket(self, bucket_name: str):
        """
        Delete a bucket. Must be empty.

        Probe the bucket for its real endpoint/region first (like every other
        bucket op), then use that bound client for BOTH the emptiness check and
        the DeleteBucket call. Using the profile-default client here failed for
        buckets that live in a non-default region (PermanentRedirect).
        """
        self._guard_write()
        # Bind to a client that can actually reach this bucket.
        bucket_client, _endpoint, _region, _use_path = self._try_bind_bucket(bucket_name)

        # emptiness check
        paginator = bucket_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix="", Delimiter="/")
        for page in pages:
            if page.get("Contents"):
                raise Exception(
                    f"Bucket '{bucket_name}' is not empty. Please empty it first."
                )
            if page.get("CommonPrefixes"):
                raise Exception(
                    f"Bucket '{bucket_name}' is not empty. Please empty it first."
                )

        # If we're currently "in" that bucket, reset view first
        if self.bucket == bucket_name:
            self.bucket = ""
            self.current_folder = ""
            self.prev_folder = ""

        bucket_client.delete_bucket(Bucket=bucket_name)

    def _delete_one(self, key, log_fn=None):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error deleting '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            self.client.delete_object(Bucket=self.bucket, Key=key)

    def _delete_batch(self, keys, log_fn=None):
        """
        Remove up to DELETE_BATCH_SIZE keys in one DeleteObjects call, falling
        back to individual deletes on backends that don't implement it.
        """
        if not keys:
            return
        payload = {"Objects": [{"Key": k} for k in keys], "Quiet": True}

        def _do():
            resp = self.client.delete_objects(
                Bucket=self.bucket, Delete=payload)
            self.raise_on_delete_errors(resp, log_fn=log_fn)

        try:
            _do()
            return
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in self.MULTI_DELETE_UNSUPPORTED_CODES:
                if log_fn:
                    log_fn("Batch delete unsupported here; deleting one by one")
                for k in keys:
                    self._delete_one(k, log_fn=log_fn)
                return
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error batch-deleting: {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def delete(self, key, log_fn=None, cancel_event=None) -> bool:
        """Delete one object, or every object under a prefix (batched)."""
        self._guard_write()
        if key.endswith("/"):
            batch = []
            for k, _ in self.get_keys(key, log_fn=log_fn):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if not k:
                    continue
                batch.append(k)
                if len(batch) >= self.DELETE_BATCH_SIZE:
                    self._delete_batch(batch, log_fn=log_fn)
                    batch = []
            self._delete_batch(batch, log_fn=log_fn)
        else:
            self._delete_one(key, log_fn=log_fn)
        return True

    def delete_object_version(self, key, version_id, log_fn=None):
        """Permanently delete a single object version (or a delete marker)."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        def _do():
            self.client.delete_object(
                Bucket=self.bucket, Key=key, VersionId=version_id
            )

        try:
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error deleting version of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    @classmethod
    def _undo_cutoff(cls, since, page):
        """
        The oldest LastModified a marker may carry and still belong to a
        delete that started at *since* (an aware datetime, a naive local one,
        or epoch seconds), on the SERVER's clock.

        *since* was read off this machine's clock and LastModified is the
        service's. The page's Date header says how far apart the two are now,
        which is what keeps a skewed clock from either missing the delete's
        own markers or sweeping in older ones.
        """
        if isinstance(since, (int, float)):
            since = datetime.fromtimestamp(since, timezone.utc)
        else:
            since = since.astimezone(timezone.utc)
        skew = timedelta(0)
        headers = ((page or {}).get("ResponseMetadata") or {}).get(
            "HTTPHeaders") or {}
        if headers.get("date"):
            try:
                skew = (parsedate_to_datetime(headers["date"])
                        - datetime.now(timezone.utc))
            except (TypeError, ValueError):
                skew = timedelta(0)
        return since + skew - cls.UNDO_SLACK

    def undelete(self, key: str, log_fn=None, since=None) -> int:
        """
        Undo a delete on a versioning-enabled bucket by removing the delete
        markers it left behind. Returns how many objects came back.

        *since* is when the delete being undone started. Only markers written
        at or after it are removed: for a prefix ('foo/') the newest marker
        of EVERY key underneath used to go, so undoing today's folder delete
        also resurrected whatever had been deliberately deleted from that
        folder a year ago. Without *since* nothing tells the two apart and
        every newest marker is still removed.

        On a bucket without versioning there are no markers and the delete was
        permanent, so this reports 0 rather than pretending to succeed.

        A marker whose version id is "null" is left alone and not counted.
        That is a delete made while versioning was suspended, and S3 removes
        the object's unversioned copy outright when it writes one: dropping
        the marker cannot bring that copy back, only promote whatever older
        version happens to sit beneath it, under a "restored" that would not
        be true.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        exact = not key.endswith("/")

        def _fetch_markers():
            markers = []
            unrecoverable = []
            cutoff = None
            paginator = self.client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                if since is not None and cutoff is None:
                    cutoff = self._undo_cutoff(since, page)
                for dm in (page.get("DeleteMarkers", []) or []):
                    marker_key = dm.get("Key")
                    version_id = dm.get("VersionId")
                    if not marker_key or not version_id:
                        continue
                    # Only the newest marker resurrects the object; older ones
                    # belong to earlier delete/restore cycles.
                    if not dm.get("IsLatest"):
                        continue
                    if exact and marker_key != key:
                        continue
                    if cutoff is not None:
                        written = dm.get("LastModified")
                        if written is None:
                            # Cannot be shown to be this delete's.
                            continue
                        if written.tzinfo is None:
                            written = written.replace(tzinfo=timezone.utc)
                        if written < cutoff:
                            continue
                    if version_id == "null":
                        unrecoverable.append(marker_key)
                        continue
                    markers.append((marker_key, version_id))
            return markers, unrecoverable

        try:
            markers, unrecoverable = _fetch_markers()
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "NotImplemented":
                if log_fn:
                    log_fn("Backend does not support versions; nothing to undo")
                return 0
            if not self._is_region_error(exc):
                raise
            self.rebind_bucket(log_fn=log_fn)
            markers, unrecoverable = _fetch_markers()

        if unrecoverable and log_fn:
            shown = ", ".join(unrecoverable[:5])
            if len(unrecoverable) > 5:
                shown += f" and {len(unrecoverable) - 5} more"
            log_fn(f"cannot restore {len(unrecoverable)} object(s): {shown} -- "
                   "deleted while versioning was suspended, which removes the "
                   "unversioned copy outright. Any older versions are listed "
                   "under Versions.")

        restored = 0
        for marker_key, version_id in markers:
            self.client.delete_object(
                Bucket=self.bucket, Key=marker_key, VersionId=version_id)
            restored += 1
            if log_fn:
                log_fn(f"restored {marker_key}")
        return restored
