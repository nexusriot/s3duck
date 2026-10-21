"""Shared fixtures for the unit suite: fakes, stubs and helpers.

These sat in the middle of a 16,000-line test_units.py, where a fake
written for one area was invisible to the next and got written twice.
"""

import botocore.session
import contextlib
import hashlib
import io
import main_window
import os
import sys
import threading
import types
from PyQt6.QtGui import QIconEngine, QPixmap
from PyQt6.QtWidgets import QApplication
from core.profile import ProfileSettings
from datetime import datetime, timezone
from model import FSObjectType, Item, Model, ReadOnlyError
from unittest.mock import MagicMock


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Directories that are not the application: the venv, build output, and the
# suite and tooling that inspect it.
_NON_APP_DIRS = {".venv", "__pycache__", ".git", "build", "dist", "tests",
                 "tools", ".idea", ".github", "icons", "resources", "DEBIAN"}
# A floor for every audit below. The guards in this file discover their own
# inputs, and a discovery that silently returns nothing turns each of them into
# a test that passes by looking at no code at all -- which is exactly what a
# flat `os.listdir` did once the sources moved into packages. Asserting the
# floor makes that failure loud.
MIN_APP_SOURCES = 8
def app_source_files(root=PROJECT_ROOT):
    """Every application .py file, recursively, newest layout or oldest.

    Deliberately not a hardcoded tuple of filenames: each audit that carried
    its own list stopped covering anything the moment a module moved, and a
    missing name reads exactly like a clean run.
    """
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames
                       if d not in _NON_APP_DIRS and not d.startswith(".")]
        found += [os.path.join(dirpath, name)
                  for name in sorted(filenames) if name.endswith(".py")]
    return sorted(found)
def app_source_names(root=PROJECT_ROOT):
    """``app_source_files`` as paths relative to the project root."""
    return [os.path.relpath(p, root) for p in app_source_files(root)]
def read_app_sources(root=PROJECT_ROOT):
    """``[(relative_path, text)]`` for every application source."""
    out = []
    for path in app_source_files(root):
        with open(path, encoding="utf-8") as handle:
            out.append((os.path.relpath(path, root), handle.read()))
    return out
# The packages that make up the application, for patch_app_wide below.
APP_MODULE_ROOTS = {
    "main_window", "model", "utils", "s3duck", "settings", "settings_store",
    "properties_window", "profile_switcher", "theme", "diagnostics",
    "core", "ui", "workers", "version",
}
@contextlib.contextmanager
def patch_app_wide(attr, replacement=None):
    """Replace a name in every application module that imported it.

    ``from x import Y`` binds Y into each importing module, so patching one
    module's Y leaves every other module's copy untouched. While the app was
    two big files that distinction did not arise; now a method and the test
    that stubs its dialogs can sit in different modules, and a patch that
    misses means a real modal dialog in an offscreen test -- which hangs the
    suite rather than failing it.

    A test that patches QMessageBox means "no message box anywhere". This
    says that, and keeps saying it when the method moves again.
    """
    if replacement is None:
        replacement = MagicMock()
    targets = [module for name, module in list(sys.modules.items())
               if module is not None
               and name.split(".")[0] in APP_MODULE_ROOTS
               and attr in vars(module)]
    assert targets, f"no application module imports {attr!r}"
    saved = [(module, getattr(module, attr)) for module in targets]
    try:
        for module in targets:
            setattr(module, attr, replacement)
        yield replacement
    finally:
        for module, original in saved:
            setattr(module, attr, original)
def make_main_window(current_dir, qsettings, **profile_fields):
    """A MainWindow for a test, with the profile spelled by name.

    These calls used to pass a positional tuple that MainWindow padded to
    twenty, so a test that cared about one late field had to count the ones
    before it. The defaults here are what nearly every call passed.
    """
    fields = dict(profile_name="prof", url="https://s3.amazonaws.com",
                  region="us-east-1", bucket="", access_key="AK",
                  secret_key="SK")
    fields.update(profile_fields)
    return main_window.MainWindow(current_dir=current_dir,
                                  qsettings=qsettings,
                                  profile=ProfileSettings(**fields))
def _ensure_qapp():
    """Return the shared QApplication, creating it once (offscreen)."""
    # Qt reads this when the application is constructed, not on import, so
    # setting it here still works. tests/__init__.py sets it earlier, but only
    # when the suite is imported as a package; asking again costs nothing and
    # keeps a stray invocation from core-dumping on a headless machine.
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication([])
class _FakePaginator:
    """Minimal boto3-style paginator returning preset pages."""

    def __init__(self, pages):
        self._pages = list(pages or [])

    def paginate(self, **_kwargs):
        return list(self._pages)
class FakeS3Client:
    """
    Records every call and returns canned responses. Enough of the boto3 S3
    surface to exercise the model's version / storage-class / preview paths
    fully offline.
    """

    def __init__(self, *, versions_pages=None, list_pages=None,
                 get_object_resp=None, head_object_resp=None,
                 restore_error=None, mpu_pages=None, parts_pages=None):
        self._paginators = {
            "list_object_versions": _FakePaginator(versions_pages),
            "list_objects_v2": _FakePaginator(list_pages),
            "list_multipart_uploads": _FakePaginator(mpu_pages),
            "list_parts": _FakePaginator(parts_pages),
        }
        self._get_object_resp = get_object_resp
        self._head_object_resp = head_object_resp or {}
        self._restore_error = restore_error
        self.calls = []

    def get_paginator(self, name):
        self.calls.append(("get_paginator", {"name": name}))
        return self._paginators[name]

    def copy_object(self, **kw):
        self.calls.append(("copy_object", kw))
        return {}

    def delete_object(self, **kw):
        self.calls.append(("delete_object", kw))
        return {}

    def delete_objects(self, **kw):
        self.calls.append(("delete_objects", kw))
        return {}

    def list_objects_v2(self, **kw):
        """The one-key existence probe. Answered from the same canned pages
        the paginator serves, honouring Prefix and MaxKeys so the probe can
        be told apart from a full listing."""
        self.calls.append(("list_objects_v2", kw))
        prefix = kw.get("Prefix") or ""
        limit = int(kw.get("MaxKeys") or 1000)
        hits = []
        for page in self._paginators["list_objects_v2"]._pages:
            for obj in (page.get("Contents") or []):
                if str(obj.get("Key", "")).startswith(prefix):
                    hits.append(obj)
                    if len(hits) >= limit:
                        break
            if len(hits) >= limit:
                break
        return {"Contents": hits, "KeyCount": len(hits)}

    def delete_bucket(self, **kw):
        self.calls.append(("delete_bucket", kw))
        return {}

    def abort_multipart_upload(self, **kw):
        self.calls.append(("abort_multipart_upload", kw))
        return {}

    def download_file(self, *a, **kw):
        self.calls.append(("download_file", {"args": a, "kwargs": kw}))

    def upload_file(self, *a, **kw):
        self.calls.append(("upload_file", {"args": a, "kwargs": kw}))

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        return self._head_object_resp

    def get_object(self, **kw):
        self.calls.append(("get_object", kw))
        if self._get_object_resp is None:
            raise AssertionError("get_object was not configured for this test")
        return self._get_object_resp

    def put_object(self, **kw):
        self.calls.append(("put_object", kw))
        return {}

    def put_object_acl(self, **kw):
        self.calls.append(("put_object_acl", kw))
        return {}

    def get_bucket_versioning(self, **kw):
        self.calls.append(("get_bucket_versioning", kw))
        return {"Status": "Enabled"}

    def put_bucket_versioning(self, **kw):
        self.calls.append(("put_bucket_versioning", kw))
        return {}

    def generate_presigned_url(self, op, Params=None, ExpiresIn=None):
        self.calls.append(("generate_presigned_url",
                           {"op": op, "Params": Params, "ExpiresIn": ExpiresIn}))
        return f"https://example.test/{op}?exp={ExpiresIn}"

    def restore_object(self, **kw):
        self.calls.append(("restore_object", kw))
        if self._restore_error is not None:
            raise self._restore_error
        return {}

    def calls_of(self, method):
        return [kw for (name, kw) in self.calls if name == method]
def _dt(day):
    return datetime(2026, 1, day, 12, 0, 0, tzinfo=timezone.utc)
def make_model(**overrides):
    """A Model is safe to construct offline; __init__ only builds a boto3
    Session object and does not perform any network I/O."""
    kw = dict(
        endpoint_url="https://s3.amazonaws.com",
        region_name="us-east-1",
        access_key="AK",
        secret_key="SK",
        bucket="",
        no_ssl_check=False,
        use_path=False,
    )
    kw.update(overrides)
    return Model(**kw)
class _StubClient:
    """Enough of a boto3 client for _make_client to finish: it registers
    event handlers on whatever comes back, so a bare object() would make
    these tests fail for a reason that has nothing to do with tokens."""

    def __init__(self):
        self.registered = []
        self.meta = types.SimpleNamespace(
            events=types.SimpleNamespace(
                register=lambda name, handler: self.registered.append(name)))
class _CopyPartClient(FakeS3Client):
    """A server that refuses an oversized CopyObject and records the
    multipart copy that follows."""

    def __init__(self, source_size=0, copy_error=None, fail_on_part=None,
                 head=None, **kw):
        super().__init__(**kw)
        self.source_size = int(source_size)
        self._copy_error = copy_error
        self.fail_on_part = fail_on_part
        self._head = dict(head or {})
        self.created = []
        self.ranges = []
        self.completed = []
        self.aborted = []

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        return dict(self._head, ContentLength=self.source_size)

    def copy_object(self, **kw):
        self.calls.append(("copy_object", kw))
        if self._copy_error is not None:
            raise self._copy_error
        return {}

    def create_multipart_upload(self, Bucket, Key, **kw):
        self.created.append((Bucket, Key, kw))
        return {"UploadId": "mpu-1"}

    def upload_part_copy(self, **kw):
        self.calls.append(("upload_part_copy", kw))
        number = kw["PartNumber"]
        self.ranges.append((number, kw["CopySourceRange"], kw["CopySource"]))
        if self.fail_on_part == number:
            raise RuntimeError("part copy failed")
        return {"CopyPartResult": {"ETag": f'"part{number}"'}}

    def complete_multipart_upload(self, **kw):
        self.completed.append(kw)
        return {"ETag": '"assembled"'}

    def abort_multipart_upload(self, **kw):
        self.aborted.append(kw)
        return {}
class _RangedClient:
    """Serves an in-memory object over ranged GETs, optionally failing once."""

    def __init__(self, payload, fail_on_chunk=None):
        self.payload = payload
        self.fail_on_chunk = fail_on_chunk
        self.ranges = []
        self.lock = threading.Lock()

    def head_object(self, **kw):
        return {"ContentLength": len(self.payload),
                "ETag": '"' + hashlib.md5(self.payload).hexdigest() + '"'}

    def get_object(self, **kw):
        rng = kw["Range"]
        start, end = rng.replace("bytes=", "").split("-")
        start, end = int(start), int(end)
        with self.lock:
            self.ranges.append((start, end))
            if self.fail_on_chunk is not None and start == self.fail_on_chunk:
                self.fail_on_chunk = None      # only fail once
                raise RuntimeError("connection reset")
        return {"Body": io.BytesIO(self.payload[start:end + 1])}
class _FakePresignModel:
    bucket = "b"

    def __init__(self):
        self.calls = []

    def presigned_get_url(self, key, expires_sec=3600):
        self.calls.append(("get", key, expires_sec))
        return f"https://get/{key}?e={expires_sec}"

    def presigned_put_url(self, key, expires_sec=3600):
        self.calls.append(("put", key, expires_sec))
        return f"https://put/{key}?e={expires_sec}"
class _PrefixDownloadClient(FakeS3Client):
    """A server holding a few objects under one prefix, able to hand back
    whole files or byte ranges."""

    def __init__(self, bodies, corrupt=()):
        pages = [{"Contents": [{"Key": k, "Size": len(v)}
                               for k, v in bodies.items()]}]
        super().__init__(list_pages=pages)
        self.bodies = dict(bodies)
        # Keys whose advertised ETag will not match what they serve.
        self.corrupt = set(corrupt)
        self.ranges = []

    def _etag(self, key):
        if key in self.corrupt:
            return '"' + "0" * 32 + '"'
        return '"' + hashlib.md5(self.bodies[key]).hexdigest() + '"'

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        key = kw["Key"]
        return {"ContentLength": len(self.bodies[key]),
                "ETag": self._etag(key)}

    def download_file(self, bucket, key, out_path, **kw):
        self.calls.append(("download_file", {"args": (bucket, key, out_path),
                                             "kwargs": kw}))
        with open(out_path, "wb") as handle:
            handle.write(self.bodies[key])

    def get_object(self, **kw):
        self.calls.append(("get_object", kw))
        key = kw["Key"]
        body = self.bodies[key]
        rng = kw.get("Range")
        if rng:
            start, end = (int(n) for n in rng.split("=")[1].split("-"))
            self.ranges.append((key, start, end))
            body = body[start:end + 1]
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}
class _FakeMainWindow:
    """Stands in for MainWindow in dialog tests (log + status bar only)."""

    def __init__(self):
        self.logs = []

    def log(self, msg):
        self.logs.append(msg)

    def statusBar(self):
        class _SB:
            def showMessage(self, *_a, **_kw):
                pass
        return _SB()
class _FakeUploadsModel:
    bucket = "b"

    def __init__(self, uploads=None, fail=False):
        self._uploads = uploads or []
        self._fail = fail
        self.aborted = []
        self.cancel_events = []

    def clone_for_worker(self):
        return self

    def list_multipart_uploads(self, prefix="", with_sizes=False,
                               cancel_event=None, **_kw):
        self.cancel_events.append(cancel_event)
        if self._fail:
            raise RuntimeError("denied")
        return [u for u in self._uploads if u["key"].startswith(prefix or "")]

    def abort_multipart_upload(self, key, upload_id, **_kw):
        self.aborted.append((key, upload_id))
class _FakeVersionsModel:
    bucket = "b"

    def __init__(self, versions=None, fail=False):
        self._versions = versions or []
        self._fail = fail

    def clone_for_worker(self):
        return self

    def list_object_versions(self, key):
        if self._fail:
            raise RuntimeError("denied")
        return self._versions

    def get_bucket_versioning_status(self):
        return "Enabled"
class _FakePropsModel:
    bucket = "b"
    endpoint_url = "https://s3.amazonaws.com"

    def __init__(self, head=None, size=0, head_error=False, lock=None):
        self._head = head or {}
        self._size = size
        self._head_error = head_error
        self._lock = lock
        self.head_calls = []
        self.size_calls = []
        self.lock_calls = []

    def clone_for_worker(self):
        return self

    def object_properties(self, key):
        self.head_calls.append(key)
        if self._head_error:
            raise RuntimeError("no such object")
        return self._head

    def get_size(self, key):
        self.size_calls.append(key)
        return self._size

    @staticmethod
    def parse_restore_status(v):
        return Model.parse_restore_status(v)

    def get_object_lock_state(self, key):
        self.lock_calls.append(key)
        if self._lock is None:
            raise RuntimeError("not supported")
        return self._lock

    def direct_object_url(self, key):
        return f"{self.endpoint_url}/{self.bucket}/{key}"

    def _endpoint_has_bucket(self, ep, b):
        return False
class _StubModel(Model):
    """Model subclass that answers listings from memory (no network)."""

    def list_buckets(self):
        return [Item("bkt", FSObjectType.BUCKET, "", 0)]

    def list(self, fld, page_cb=None, max_items=0):
        return []

    def existing_keys(self, keys):
        return set()

    def enter_bucket(self, bucket_name):
        self.bucket = bucket_name
        self.current_folder = ""
        self.prev_folder = ""
class _FakeRunningThread:
    """Stands in for a QThread that is still running."""

    def __init__(self):
        self.quit_calls = 0
        self.wait_calls = []

    def isRunning(self):
        return True

    def quit(self):
        self.quit_calls += 1

    def wait(self, msecs=None):
        self.wait_calls.append(msecs)
        return True
class _HollowIconEngine(QIconEngine):
    """An icon that exists but draws nothing — what a theme entry with no
    usable file behaves like, and the reason isNull() is not enough."""

    def pixmap(self, size, mode, state):
        return QPixmap()

    def paint(self, painter, rect, mode, state):
        pass

    def clone(self):
        return _HollowIconEngine()
class _MultipartClient:
    """Records a manual multipart upload, optionally failing one part once."""

    def __init__(self, fail_on_part=None, page_size=None, cancel_on_part=None,
                 cancel_event=None):
        self.cancel_on_part = cancel_on_part
        self.cancel_event = cancel_event
        self.uploads = {}          # upload_id -> {key, parts}
        self.completed = {}        # key -> assembled bytes
        self.fail_on_part = fail_on_part
        self.page_size = page_size
        self.created = 0
        self.part_calls = []
        self.managed = []          # upload_file() calls (the boto3 fast path)
        self.aborted = []
        self.completed_kw = {}
        # Raised once by complete_multipart_upload, for the conditional paths.
        self.fail_complete = None

    def create_multipart_upload(self, Bucket, Key, **kw):
        self.created += 1
        upload_id = f"upload-{self.created}"
        self.uploads[upload_id] = {"key": Key, "parts": {}, "kw": kw}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body, **kw):
        self.part_calls.append((PartNumber, kw.get("ChecksumAlgorithm")))
        if self.cancel_on_part == PartNumber and self.cancel_event is not None:
            self.cancel_event.set()
        if self.fail_on_part == PartNumber:
            self.fail_on_part = None
            raise RuntimeError("connection reset")
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        out = {"ETag": f'"etag{PartNumber}"'}
        if kw.get("ChecksumAlgorithm") == "CRC32":
            out["ChecksumCRC32"] = f"crc{PartNumber}"
        return out

    def list_parts(self, Bucket, Key, UploadId, PartNumberMarker=0):
        stored = sorted(self.uploads.get(UploadId, {}).get("parts", {}).items())
        rows = [{"PartNumber": n, "ETag": f'"etag{n}"', "Size": len(b)}
                for n, b in stored if n > PartNumberMarker]
        if self.page_size and len(rows) > self.page_size:
            head = rows[:self.page_size]
            return {"Parts": head, "IsTruncated": True,
                    "NextPartNumberMarker": head[-1]["PartNumber"]}
        return {"Parts": rows, "IsTruncated": False}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload,
                                  **kw):
        self.completed_kw = kw
        if self.fail_complete is not None:
            error, self.fail_complete = self.fail_complete, None
            raise error
        parts = self.uploads[UploadId]["parts"]
        order = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        self.completed[Key] = b"".join(parts[n] for n in order)
        self.completed_parts = MultipartUpload["Parts"]
        return {"ETag": '"assembled"'}

    def upload_file(self, local, Bucket, Key, **kw):
        self.managed.append(Key)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append(UploadId)
class _RecordingDest:
    """A stand-in destination model that records what was written to it."""

    def __init__(self, bucket="dst", read_only=False):
        self.bucket = bucket
        self.read_only = read_only
        self.upload_extra_args = {}
        self.transfer_cfg_upload = None
        self.uploads = []
        self.folders = []
        self.client = self

    def _guard_write(self):
        if self.read_only:
            raise ReadOnlyError("profile is read-only")

    def upload_fileobj(self, body, bucket, key, ExtraArgs=None, Callback=None,
                       Config=None):
        self.uploads.append((bucket, key, body.read(), ExtraArgs))

    def create_folder(self, key, log_fn=None):
        self.folders.append(key)
class _ConditionalPutClient(_MultipartClient):
    """Records PutObject, optionally answering the precondition."""

    def __init__(self, put_error=None, **kw):
        super().__init__(**kw)
        self.put_error = put_error
        self.puts = []
        # Called with the running byte count as the body is consumed.
        self.on_body_read = None

    def put_object(self, **kw):
        self.puts.append(kw)
        body = kw.get("Body")
        if hasattr(body, "read"):
            # A real service reads the body; cancellation and progress are
            # only observable to a fake that does the same.
            sofar = 0
            while True:
                block = body.read(8)
                if not block:
                    break
                sofar += len(block)
                if self.on_body_read is not None:
                    self.on_body_read(sofar)
        if self.put_error is not None:
            error, self.put_error = self.put_error, None
            raise error
        return {"ETag": '"written"'}
def _client_error(code, status=400, op="PutObject"):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code},
         "ResponseMetadata": {"HTTPStatusCode": status}}, op)
class _FakeSyncModel:
    bucket = "b"

    def __init__(self, remote=None):
        self._remote = remote or {}

    def clone_for_worker(self):
        return self

    def list_tree(self, prefix="", cancel_event=None, log_fn=None):
        return dict(self._remote)
class _FakeCompareModel:
    """
    The data model behind the pane comparison: a listing per bucket and a
    HEAD per key. ``trees`` is ``{bucket: {key: (size, mtime, etag)}}`` and
    ``heads`` is ``{(bucket, key): head-dict-or-exception}``.
    """

    DEFAULT_MULTIPART_CHUNKSIZE_MB = Model.DEFAULT_MULTIPART_CHUNKSIZE_MB

    def __init__(self, trees=None, heads=None, part_size=8 * 1024 * 1024):
        self.bucket = ""
        self.trees = trees or {}
        self.heads = heads or {}
        self.multipart_chunksize_bytes = part_size
        self.head_calls = []
        self.client = self

    def clone_for_worker(self):
        clone = _FakeCompareModel(self.trees, self.heads,
                                  self.multipart_chunksize_bytes)
        clone.bucket = self.bucket
        clone.head_calls = self.head_calls
        return clone

    def list_tree(self, prefix="", cancel_event=None, log_fn=None,
                  with_etag=False):
        out = {}
        for key, (size, mtime, etag) in self.trees.get(self.bucket, {}).items():
            if key.startswith(prefix or ""):
                entry = (size, mtime, etag) if with_etag else (size, mtime)
                out[key[len(prefix or ""):]] = entry
        return out

    def head_object(self, Bucket, Key, **_kw):
        self.head_calls.append((Bucket, Key))
        answer = self.heads.get((Bucket, Key), {})
        if isinstance(answer, Exception):
            raise answer
        return answer
class _FakeDuplicateModel:
    bucket = "bkt"

    def __init__(self, rows=None, fail=False):
        self._rows = rows or []
        self._fail = fail
        self.prefixes = []
        self.cancel_events = []

    def clone_for_worker(self):
        return self

    def list_object_digests(self, prefix="", cancel_event=None, log_fn=None):
        self.prefixes.append(prefix)
        self.cancel_events.append(cancel_event)
        if self._fail:
            raise RuntimeError("denied")
        return list(self._rows)
class _BucketAdminClient(FakeS3Client):
    """FakeS3Client plus the bucket-level documents."""

    def __init__(self, **kw):
        self.lifecycle = kw.pop("lifecycle", None)
        self.cors = kw.pop("cors", None)
        self.policy = kw.pop("policy", None)
        self.lock = kw.pop("lock", None)
        self.retention = kw.pop("retention", None)
        self.legal_hold = kw.pop("legal_hold", None)
        super().__init__(**kw)

    @staticmethod
    def _missing(code, operation):
        return botocore.exceptions.ClientError(
            {"Error": {"Code": code, "Message": "absent"}}, operation)

    def get_bucket_lifecycle_configuration(self, **kw):
        self.calls.append(("get_bucket_lifecycle_configuration", kw))
        if self.lifecycle is None:
            raise self._missing("NoSuchLifecycleConfiguration", "GetLifecycle")
        return {"Rules": self.lifecycle}

    def put_bucket_lifecycle_configuration(self, **kw):
        self.calls.append(("put_bucket_lifecycle_configuration", kw))
        return {}

    def delete_bucket_lifecycle(self, **kw):
        self.calls.append(("delete_bucket_lifecycle", kw))
        return {}

    def get_bucket_cors(self, **kw):
        self.calls.append(("get_bucket_cors", kw))
        if self.cors is None:
            raise self._missing("NoSuchCORSConfiguration", "GetBucketCors")
        return {"CORSRules": self.cors}

    def put_bucket_cors(self, **kw):
        self.calls.append(("put_bucket_cors", kw))
        return {}

    def delete_bucket_cors(self, **kw):
        self.calls.append(("delete_bucket_cors", kw))
        return {}

    def get_bucket_policy(self, **kw):
        self.calls.append(("get_bucket_policy", kw))
        if self.policy is None:
            raise self._missing("NoSuchBucketPolicy", "GetBucketPolicy")
        return {"Policy": self.policy}

    def put_bucket_policy(self, **kw):
        self.calls.append(("put_bucket_policy", kw))
        return {}

    def delete_bucket_policy(self, **kw):
        self.calls.append(("delete_bucket_policy", kw))
        return {}

    def get_bucket_policy_status(self, **kw):
        self.calls.append(("get_bucket_policy_status", kw))
        return {"PolicyStatus": {"IsPublic": True}}

    def get_object_lock_configuration(self, **kw):
        self.calls.append(("get_object_lock_configuration", kw))
        if self.lock is None:
            raise self._missing(
                "ObjectLockConfigurationNotFoundError", "GetObjectLock")
        return {"ObjectLockConfiguration": self.lock}

    def get_object_retention(self, **kw):
        self.calls.append(("get_object_retention", kw))
        if self.retention is None:
            raise self._missing("InvalidRequest", "GetObjectRetention")
        return {"Retention": self.retention}

    def get_object_legal_hold(self, **kw):
        self.calls.append(("get_object_legal_hold", kw))
        if self.legal_hold is None:
            raise self._missing("NoSuchObjectLockConfiguration", "GetHold")
        return {"LegalHold": {"Status": self.legal_hold}}

    def put_object_legal_hold(self, **kw):
        self.calls.append(("put_object_legal_hold", kw))
        return {}
class _AttributesClient(FakeS3Client):
    """FakeS3Client that answers GetObjectAttributes."""

    def __init__(self, attributes=None, error=None, **kw):
        self.attributes = attributes or {}
        self.attributes_error = error
        super().__init__(**kw)

    def get_object_attributes(self, **kw):
        self.calls.append(("get_object_attributes", kw))
        if self.attributes_error is not None:
            raise self.attributes_error
        key = kw.get("Key")
        payload = self.attributes.get(key, {})
        if callable(payload):
            return payload(kw)
        return payload
class _DocumentClient(FakeS3Client):
    """FakeS3Client answering the remaining bucket documents."""

    def __init__(self, **kw):
        self.docs = kw.pop("docs", {})
        super().__init__(**kw)

    def _absent(self, code):
        return botocore.exceptions.ClientError(
            {"Error": {"Code": code, "Message": "absent"}}, "Get")

    def _doc(self, name, code, **kw):
        self.calls.append((name, kw))
        if name not in self.docs:
            raise self._absent(code)
        return self.docs[name]

    def get_bucket_encryption(self, **kw):
        return self._doc("get_bucket_encryption",
                         "ServerSideEncryptionConfigurationNotFoundError", **kw)

    def put_bucket_encryption(self, **kw):
        self.calls.append(("put_bucket_encryption", kw))
        return {}

    def delete_bucket_encryption(self, **kw):
        self.calls.append(("delete_bucket_encryption", kw))
        return {}

    def get_bucket_tagging(self, **kw):
        return self._doc("get_bucket_tagging", "NoSuchTagSet", **kw)

    def put_bucket_tagging(self, **kw):
        self.calls.append(("put_bucket_tagging", kw))
        return {}

    def delete_bucket_tagging(self, **kw):
        self.calls.append(("delete_bucket_tagging", kw))
        return {}

    def get_bucket_website(self, **kw):
        return self._doc("get_bucket_website", "NoSuchWebsiteConfiguration", **kw)

    def put_bucket_website(self, **kw):
        self.calls.append(("put_bucket_website", kw))
        return {}

    def delete_bucket_website(self, **kw):
        self.calls.append(("delete_bucket_website", kw))
        return {}

    def get_bucket_notification_configuration(self, **kw):
        return self._doc("get_bucket_notification_configuration",
                         "NotImplemented", **kw)

    def get_bucket_accelerate_configuration(self, **kw):
        return self._doc("get_bucket_accelerate_configuration",
                         "UnsupportedOperation", **kw)

    def put_bucket_accelerate_configuration(self, **kw):
        self.calls.append(("put_bucket_accelerate_configuration", kw))
        return {}
