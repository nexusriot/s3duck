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
# Characters Windows refuses in a file or folder name ('/' and '\' already
# split a key into components). ':' is the dangerous one: every ISO
# timestamp in a key has two, and NTFS does not refuse "a:b" -- it writes
# stream "b" of a file "a", so the download "succeeds" into a place nobody
# will ever look.
_WINDOWS_ILLEGAL_CHARS = frozenset('<>:"|?*') | frozenset(map(chr, range(32)))
# DOS device names, reserved in any case and with any extension ("nul.txt"
# is NUL). The list CPython's ntpath.isreserved uses, superscript digits
# included.
_WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"}
    | {dev + n for dev in ("COM", "LPT") for n in "123456789¹²³"})
def _local_names_follow_windows_rules() -> bool:
    """
    Whether names must also pass Windows' rules to be created locally.

    Only on Windows: ':' and '?' are ordinary characters on Linux and macOS,
    and refusing them there would skip files that download perfectly well.
    A function rather than a constant so a test can ask the question as a
    Windows machine would.
    """
    return os.name == "nt"
def local_name_problem(rel) -> str:
    """
    Why some component of *rel* cannot be created locally, or "" if none.

    A 300-character component is a legal S3 key and an ENAMETOOLONG on
    ext4. Checked in BYTES, not characters, because that is what the
    filesystem counts — 200 emoji are 800 bytes and would pass a character
    test while failing the real one. On Windows a key also has to be a legal
    Windows name, which a great many legal keys are not.
    """
    windows = _local_names_follow_windows_rules()
    for part in str(rel or "").replace("\\", "/").split("/"):
        if part in ("", ".", ".."):
            # Not names: an empty segment collapses, and whether a ".."
            # escapes is the containment check's question, not this one's.
            continue
        if len(part.encode("utf-8", "surrogatepass")) > MAX_LOCAL_NAME_BYTES:
            return (f"'{part[:40]}…' is longer than {MAX_LOCAL_NAME_BYTES} "
                    "bytes, the most one name can have on this filesystem")
        if "\0" in part:
            return "contains a NUL character, which no filesystem allows"
        if not windows:
            continue
        illegal = sorted(set(part) & _WINDOWS_ILLEGAL_CHARS)
        if illegal:
            shown = " ".join(repr(c) for c in illegal)
            return f"'{part}' contains {shown}, which Windows does not allow"
        if part[-1] in ". ":
            # Windows strips them, so the file would land under another
            # name -- possibly one a different key also maps to.
            return f"'{part}' ends in a dot or a space, which Windows strips"
        if part.split(".", 1)[0].rstrip(" ").upper() in _WINDOWS_RESERVED_NAMES:
            return f"'{part}' is a reserved device name on Windows"
    return ""
def local_name_is_writable(rel) -> bool:
    """True when every component of *rel* can exist locally."""
    return not local_name_problem(rel)
def safe_local_path(base_dir, rel) -> str:
    """
    Where *rel* lands under *base_dir*, or "" when it cannot go there.

    Two ways a key fails to be a local path, and both end in "". First,
    escape: "../../.bashrc" and "/etc/passwd" are legal keys that anyone
    who can write to the bucket can create, and joining one onto a download
    folder writes outside the folder the user picked. Second, a name the
    filesystem cannot hold: a component over its length limit, or on Windows
    a ':' (an alternate data stream, not a file), a '?' or a CON.
    plan_prefix_download has always refused the first for a
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
    if not local_destination_is_safe(base, candidate):
        return ""
    return candidate


def local_destination_is_safe(base_dir, target) -> bool:
    """Contain a local write and refuse symlinks below the chosen root.

    The chosen root itself may be a symlink (as /tmp is on some platforms).
    Links introduced by an object key, including the output file, are refused.
    Call again immediately before creating or replacing a file.
    """
    base, target = os.path.abspath(base_dir), os.path.abspath(target)
    try:
        if target == base or os.path.commonpath((base, target)) != base:
            return False
        real_base, real_target = os.path.realpath(base), os.path.realpath(target)
        if os.path.commonpath((real_base, real_target)) != real_base:
            return False
    except ValueError:
        return False
    current = base
    for component in os.path.relpath(target, base).split(os.sep):
        current = os.path.join(current, component)
        if os.path.islink(current):
            return False
    return True


def require_safe_local_destination(base_dir, target):
    """Raise unless *target* is a contained, symlink-free local destination."""
    if not local_destination_is_safe(base_dir, target):
        raise ValueError(
            f"Unsafe local destination (outside the chosen folder, or "
            f"reached through a symlink): {target}")


def plan_prefix_download(key, folder_path, entries) -> PrefixDownloadPlan:
    """
    Map the objects under a prefix to the local paths a download would write.

    Pure, so the overwrite check in the UI and the download itself derive
    their destinations from the same rules and cannot drift. *entries* is an
    iterable of ``(key, size)``.
    """
    prefix = prefix_of(key)
    base_dir = os.path.join(folder_path, os.path.basename(prefix.rstrip("/")))
    dirs, wanted, unsafe, reasons = [], [], [], {}

    def _refuse(k, why):
        unsafe.append(k)
        reasons[k] = why

    for k, size in entries:
        if not k:
            continue
        rel = os.path.relpath(k, prefix)
        if rel == ".":
            continue
        out_path = os.path.join(base_dir, rel)
        # Keys may contain ".." segments; never write outside base_dir.
        # Containment alone is not enough: a symlink anywhere below the
        # chosen root sends the write wherever it points, so the planner
        # uses the same check the writer re-runs just before it creates
        # the file.
        if not local_destination_is_safe(base_dir, out_path):
            _refuse(k, "escapes the folder being downloaded into, or is "
                       "reached through a symlink")
            continue
        # A name the filesystem cannot hold is an error, not a file — and
        # because the whole-prefix download fans out through run_parallel,
        # which re-raises the first error, ONE such key used to abort the
        # entire folder. Skipping it costs that object and saves the other
        # nine hundred.
        problem = local_name_problem(rel)
        if problem:
            _refuse(k, problem)
            continue
        if str(k).endswith("/"):
            dirs.append((rel, out_path))
        else:
            wanted.append((k, rel, out_path, size))

    # S3 has no folders, only keys, so "logs" and "logs/2024.txt" are both
    # legal at once; on disk one path cannot be a file and a folder. Writing
    # both gave IsADirectoryError or NotADirectoryError depending on which
    # came first. The folder wins whatever the listing order: it may hold
    # thousands of objects and the file is one. normcase, because on Windows
    # "Logs" and "logs" are the same folder.
    folders = set()
    for rel in [rel for rel, _p in dirs] + [os.path.dirname(w[1]) for w in wanted]:
        while rel:
            folders.add(os.path.normcase(rel))
            rel = os.path.dirname(rel)
    files = []
    for k, rel, out_path, size in wanted:
        if os.path.normcase(rel) in folders:
            _refuse(k, "a folder of the same name is also being downloaded")
            continue
        files.append((k, out_path, size))
    return PrefixDownloadPlan(
        base_dir, [p for _r, p in dirs], files, unsafe, reasons)
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
def precondition_failed(exc) -> bool:
    """
    Whether a request was refused because its If-Match (or
    x-amz-copy-source-if-match) no longer named the object: it was replaced
    after the ETag being pinned to was read.
    """
    response = getattr(exc, "response", None) or {}
    code = str((response.get("Error") or {}).get("Code", ""))
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return code in ("PreconditionFailed", "412") or status == 412
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
