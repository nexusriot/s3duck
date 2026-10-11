"""What the S3 layer's pieces share.

Model's mixins all need these -- the checksum table, the progress adapters,
the rate limiter, the local-path safety rules -- and none of them can import
Model to get them without a cycle. model.py re-exports the names its own
callers ask it for, so `from model import run_parallel` and friends keep
working; a name nothing imports from model is reached from here instead.
"""

import os
import threading
import time
import types
import zlib
from concurrent.futures import ThreadPoolExecutor

import botocore.session

from core.checksums import _CrcDigest, _HashDigest
from core.entries import PrefixDownloadPlan
from core.errors import TransferCancelled

try:
    from awscrt import checksums as _crt_checksums
except ImportError:
    _crt_checksums = None

try:
    from botocore.httpchecksum import (
        _SUPPORTED_CHECKSUM_ALGORITHMS as _BOTOCORE_CHECKSUM_NAMES)
except ImportError:
    _BOTOCORE_CHECKSUM_NAMES = ("crc32", "sha1", "sha256")


def run_parallel(items, fn, workers, cancel_event=None):
    """
    Apply fn to every item, up to *workers* at a time.

    Sequential when workers <= 1 (which keeps ordering deterministic). The
    first exception stops further work and is re-raised once the in-flight
    calls have drained, so a failure never leaves threads running.
    """
    items = list(items)
    if not items:
        return
    if workers is None or int(workers) <= 1:
        for item in items:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            fn(item)
        return

    first_exc = None
    lock = threading.Lock()

    def _wrapped(item):
        nonlocal first_exc
        if first_exc is not None:
            return
        if cancel_event is not None and cancel_event.is_set():
            return
        try:
            fn(item)
        except Exception as exc:  # recorded, re-raised by the caller below
            with lock:
                if first_exc is None:
                    first_exc = exc

    with ThreadPoolExecutor(max_workers=int(workers)) as pool:
        for future in [pool.submit(_wrapped, item) for item in items]:
            future.result()

    if first_exc is not None:
        raise first_exc
    if cancel_event is not None and cancel_event.is_set():
        raise TransferCancelled("cancelled")
def _checksum_digests() -> dict:
    """Algorithm name -> factory for something that digests blocks."""
    out = {
        "CRC32": lambda: _CrcDigest(zlib.crc32, 4),
        "SHA1": lambda: _HashDigest("sha1"),
        "SHA256": lambda: _HashDigest("sha256"),
    }
    if _crt_checksums is not None:
        sendable = {str(name).upper() for name in _BOTOCORE_CHECKSUM_NAMES}
        for name, attr, width in (("CRC32C", "crc32c", 4),
                                  ("CRC64NVME", "crc64nvme", 8)):
            fn = getattr(_crt_checksums, attr, None)
            if fn is not None and name in sendable:
                out[name] = (lambda fn=fn, width=width:
                             _CrcDigest(fn, width))
    return out
CHECKSUM_DIGESTS = _checksum_digests()
# What the UI offers and the order a stored checksum is looked for in.
CHECKSUM_ALGORITHMS = tuple(
    name for name in ("CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256")
    if name in CHECKSUM_DIGESTS
)
# Every checksum S3 may have stored, computable here or not. Recognising one
# we cannot compute is what lets a download say why it was not verified
# instead of reporting a comparison that never happened.
KNOWN_CHECKSUM_ALGORITHMS = ("CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256")
# CRCs compose across parts, so S3 can keep a whole-object checksum for a
# multipart upload instead of a per-part composite. CRC64NVME is full-object
# only -- it has no composite form at all.
FULL_OBJECT_CHECKSUMS = ("CRC32", "CRC32C", "CRC64NVME")
# What an object gets when nothing better is known. S3's own default for a
# body uploaded without a Content-Type.
FALLBACK_CONTENT_TYPE = "binary/octet-stream"
# Extensions the stdlib table either misses entirely or answers differently
# depending on the platform's /etc/mime.types. Applied before mimetypes so the
# result does not depend on the machine doing the upload.
BUILTIN_CONTENT_TYPES = {
    "md": "text/markdown",
    "markdown": "text/markdown",
    "yml": "application/yaml",
    "yaml": "application/yaml",
    "toml": "application/toml",
    "wasm": "application/wasm",
    "webmanifest": "application/manifest+json",
    "avif": "image/avif",
    "webp": "image/webp",
    "heic": "image/heic",
    "jxl": "image/jxl",
    "opus": "audio/opus",
    "flac": "audio/flac",
    "mkv": "video/x-matroska",
    # .ts is deliberately absent: an HLS segment and a TypeScript source
    # share it, and guessing wrong breaks video playback. The stdlib answer
    # (video/mp2t) stands unless the user overrides it.
    "tsx": "application/typescript",
    "jsx": "text/javascript",
    "mjs": "text/javascript",
    "cjs": "text/javascript",
    "map": "application/json",
    "lock": "text/plain",
    "log": "text/plain",
    "env": "text/plain",
    "sql": "application/sql",
    "epub": "application/epub+zip",
    "zst": "application/zstd",
    "7z": "application/x-7z-compressed",
    "apk": "application/vnd.android.package-archive",
    "deb": "application/vnd.debian.binary-package",
    "rpm": "application/x-rpm",
    "dmg": "application/x-apple-diskimage",
    "iso": "application/x-iso9660-image",
    "parquet": "application/vnd.apache.parquet",
    "ndjson": "application/x-ndjson",
    "jsonl": "application/x-ndjson",
}
def prefix_of(key) -> str:
    """
    A folder key as a listing prefix — always trailing-slashed.

    The empty key is the bucket root, and the root's prefix is the empty
    string. Answering "/" instead looked harmless and was not: as a listing
    filter it matches only keys that literally begin with a slash, so a caller
    that reached here with "" quietly operated on nothing at all rather than
    on everything.
    """
    text = str(key or "")
    if not text:
        return ""
    return text if text.endswith("/") else text + "/"
# One path component's limit, in BYTES, on every mainstream filesystem:
# ext4, APFS and NTFS all stop at 255. S3 allows a 1024-byte key with no
# such per-component rule, so a perfectly legal key can name a file the
# local filesystem cannot create.
MAX_LOCAL_NAME_BYTES = 255
def local_name_is_writable(rel) -> bool:
    """
    True when every component of *rel* is short enough to exist locally.

    A 300-character component is a legal S3 key and an ENAMETOOLONG on
    ext4. Checked in BYTES, not characters, because that is what the
    filesystem counts — 200 emoji are 800 bytes and would pass a character
    test while failing the real one.
    """
    for part in str(rel or "").replace("\\", "/").split("/"):
        if len(part.encode("utf-8", "surrogatepass")) > MAX_LOCAL_NAME_BYTES:
            return False
    return True
def safe_local_path(base_dir, rel) -> str:
    """
    Where *rel* lands under *base_dir*, or "" when it cannot go there.

    Two ways a key fails to be a local path, and both end in "". First,
    escape: "../../.bashrc" and "/etc/passwd" are legal keys that anyone
    who can write to the bucket can create, and joining one onto a download
    folder writes outside the folder the user picked. Second, length: a
    component over the filesystem's limit is an ENAMETOOLONG rather than a
    file. plan_prefix_download has always refused the first for a
    whole-folder download; this is the same rule for every other place a key
    becomes a local path (sync, the second pane, search results).
    """
    base = os.path.abspath(base_dir or ".")
    text = str(rel or "").replace("\\", "/").lstrip("/")
    if not text:
        return ""
    if not local_name_is_writable(text):
        return ""
    candidate = os.path.abspath(os.path.join(base, *text.split("/")))
    # Strictly below the base: landing *on* it ("." or "a/..") names the
    # directory, which is not somewhere an object can be written either.
    if not candidate.startswith(base + os.sep):
        return ""
    return candidate
def plan_prefix_download(key, folder_path, entries) -> PrefixDownloadPlan:
    """
    Map the objects under a prefix to the local paths a download would write.

    Pure, so the overwrite check in the UI and the download itself derive
    their destinations from the same rules and cannot drift. *entries* is an
    iterable of ``(key, size)``.
    """
    prefix = prefix_of(key)
    base_dir = os.path.join(folder_path, os.path.basename(prefix.rstrip("/")))
    root = os.path.abspath(base_dir) + os.sep
    dirs, files, unsafe = [], [], []
    for k, size in entries:
        if not k:
            continue
        rel = os.path.relpath(k, prefix)
        if rel == ".":
            continue
        out_path = os.path.join(base_dir, rel)
        # Keys may contain ".." segments; never write outside base_dir.
        if not os.path.abspath(out_path).startswith(root):
            unsafe.append(k)
            continue
        # A component over the filesystem's limit is an ENAMETOOLONG, not a
        # file — and because the whole-prefix download fans out through
        # run_parallel, which re-raises the first error, ONE such key used to
        # abort the entire folder. Skipping it costs that object and saves
        # the other nine hundred.
        if not local_name_is_writable(rel):
            unsafe.append(k)
            continue
        if str(k).endswith("/"):
            dirs.append(out_path)
        else:
            files.append((k, out_path, size))
    return PrefixDownloadPlan(base_dir, dirs, files, unsafe)
class RateLimiter:
    """
    Token bucket shared by every transfer so the ceiling is a *total*, not
    per-file. consume() blocks until the requested bytes are affordable.

    time_fn/sleep_fn are injectable so the behaviour can be tested without
    actually waiting.
    """

    def __init__(self, rate_bps, *, time_fn=time.monotonic, sleep_fn=time.sleep,
                 capacity=None):
        self.rate_bps = float(rate_bps or 0)
        self._time = time_fn
        self._sleep = sleep_fn
        # One second of burst by default, so small chunks are not serialised.
        self.capacity = float(capacity if capacity is not None else self.rate_bps)
        self._tokens = self.capacity
        self._last = self._time()
        self._lock = threading.Lock()

    def consume(self, amount) -> float:
        """Charge *amount* bytes, sleeping as needed. Returns seconds slept."""
        amount = int(amount or 0)
        if self.rate_bps <= 0 or amount <= 0:
            return 0.0
        with self._lock:
            now = self._time()
            self._tokens = min(
                self.capacity, self._tokens + (now - self._last) * self.rate_bps)
            self._last = now
            if self._tokens >= amount:
                self._tokens -= amount
                return 0.0
            deficit = amount - self._tokens
            delay = deficit / self.rate_bps
            self._tokens = 0.0
            self._last = now + delay
        self._sleep(delay)
        return delay
class _BotoProgressAdapter:
    """
    Adapt boto3 Callback(bytes_amount) -> progress_cb(total, current, key).
    Also supports cooperative cancellation.
    """
    def __init__(self, total, key, cb, cancel_event=None, limiter=None):
        self.total = max(1, int(total or 0))
        self.key = key
        self.cb = cb
        self.cancel_event = cancel_event
        self.limiter = limiter
        self._sofar = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # If Cancel, abort transfer ASAP.
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise TransferCancelled("cancelled")

        inc = int(bytes_amount or 0)
        if self.limiter is not None:
            self.limiter.consume(inc)

        if self.cb is None:
            return
        with self._lock:
            self._sofar += inc
            cur = self._sofar

        if cur > self.total:
            cur = self.total

        self.cb(self.total, cur, self.key)
class _ProgressReader:
    """
    A request body that paces, reports progress and can be cancelled while
    it is being read.

    The managed transfer gets all three from its Callback; a request this
    file sends itself has no callback, so it gets them from the body. Bytes
    are only charged and reported the first time they go past: botocore may
    rewind and re-read a seekable body — to compute a checksum, or to retry
    a request — and billing the shared bandwidth ceiling twice for one
    upload is exactly the bug that would otherwise hide in here.
    """

    def __init__(self, handle, total, key, cb, cancel_event=None,
                 limiter=None):
        self._handle = handle
        self.total = max(1, int(total or 0))
        self.key = key
        self.cb = cb
        self.cancel_event = cancel_event
        self.limiter = limiter
        self._high = 0

    def read(self, size=-1):
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise TransferCancelled("cancelled")
        block = self._handle.read(-1 if size is None else size)
        if not block:
            return block
        position = self._handle.tell()
        fresh = position - self._high
        if fresh > 0:
            self._high = position
            if self.limiter is not None:
                self.limiter.consume(fresh)
            if self.cb is not None:
                self.cb(self.total, min(position, self.total), self.key)
        return block

    def seek(self, offset, whence=0):
        return self._handle.seek(offset, whence)

    def tell(self):
        return self._handle.tell()

    def seekable(self):
        return True
def client_accepts(client, operation: str, parameter: str) -> bool:
    """
    Whether this client's botocore knows ``parameter`` on ``operation``.

    boto3 validates arguments client-side against the service model it ships
    with, so a parameter added to S3 after the installed botocore was built
    raises ParamValidationError before a request is ever sent. That is not a
    ClientError, so it slips past the fallbacks that handle a backend saying
    "not implemented" — asking the model first is what keeps a too-old
    botocore from turning into a failed transfer.

    Not cached here: botocore already caches the operation model, which
    leaves this a dict lookup, and an answer cached per parameter alone
    would be one client's answer handed to another.

    Anything that cannot be asked answers True: a model this build cannot
    describe should not disable a feature the service does have.
    """
    try:
        shape = client.meta.service_model.operation_model(
            operation).input_shape
    except Exception:
        return True
    return shape is None or parameter in shape.members
def installed_sdk_accepts(operation: str, parameter: str) -> bool:
    """
    The same question asked of the installed botocore rather than of a live
    client, so Diagnostics can answer it with no profile connected.
    """
    try:
        return client_accepts(
            types.SimpleNamespace(meta=types.SimpleNamespace(
                service_model=botocore.session.get_session(
                ).get_service_model("s3"))),
            operation, parameter)
    except Exception:
        return True
def client_knows_checksum(client, algorithm: str) -> bool:
    """Whether this botocore accepts ``algorithm`` as a ChecksumAlgorithm."""
    try:
        shape = client.meta.service_model.operation_model(
            "PutObject").input_shape.members["ChecksumAlgorithm"]
    except Exception:
        return True
    allowed = getattr(shape, "enum", None)
    return not allowed or algorithm in allowed
def drop_unsendable_checksum_args(client, args: dict) -> dict:
    """
    Remove checksum arguments this botocore cannot send.

    The same trap as IfNoneMatch: ChecksumType and the CRC64NVME algorithm
    are recent additions, and boto3 refuses an unknown one client-side
    before any request goes out — so an upload with checksums turned on
    failed outright rather than falling back. Dropping them costs the
    whole-object checksum; keeping them costs the upload.

    Mutates and returns ``args``, which is already a per-call copy.
    """
    if not args:
        return args
    if "ChecksumType" in args and not client_accepts(
            client, "CreateMultipartUpload", "ChecksumType"):
        args.pop("ChecksumType", None)
    algorithm = args.get("ChecksumAlgorithm")
    if algorithm and not client_knows_checksum(client, algorithm):
        # Without an algorithm the type means nothing either.
        args.pop("ChecksumAlgorithm", None)
        args.pop("ChecksumType", None)
    return args
