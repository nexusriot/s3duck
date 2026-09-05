from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from enum import Enum
from typing import NamedTuple
import base64
import fnmatch
import hashlib
import struct
import zlib
import json
import mimetypes
import platform
import re
import time
import uuid
import os
import boto3
import botocore
import threading
from urllib.parse import urlencode, urlparse

from boto3.s3.transfer import TransferConfig


class TransferCancelled(Exception):
    pass


class ReadOnlyError(Exception):
    """Raised when a write is attempted on a profile marked read-only."""


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


def as_epoch(value) -> float:
    """
    Best-effort epoch seconds for an S3 LastModified / local mtime.
    Naive datetimes are read as UTC; anything unusable becomes 0.0.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return float(value.timestamp())
    except Exception:
        return 0.0


# Checksums S3 can store that we can also recompute locally. CRC32C is
# deliberately absent: it needs a third-party module, and an algorithm we
# cannot verify would pass every download unchecked.
CHECKSUM_ALGORITHMS = ("CRC32", "SHA1", "SHA256")

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
    """A folder key as a listing prefix — always trailing-slashed."""
    text = str(key or "")
    return text if text.endswith("/") else text + "/"


class PrefixDownloadPlan(NamedTuple):
    base_dir: str
    dirs: list
    files: list      # [(key, out_path, size)]
    unsafe: list     # keys that would escape base_dir


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
        if str(k).endswith("/"):
            dirs.append(out_path)
        else:
            files.append((k, out_path, size))
    return PrefixDownloadPlan(base_dir, dirs, files, unsafe)


class FSObjectType(Enum):
    FILE = 1
    FOLDER = 2
    BUCKET = 3  # top-level S3 bucket


class Item:
    def __init__(self, name, type_, modified, size, storage_class="", etag=""):
        self.name = name
        self.type_ = type_
        self.modified = modified
        self.size = size
        # Both come free with ListObjectsV2, so the optional listing columns
        # cost no extra request.
        self.storage_class = storage_class
        self.etag = etag

    def __repr__(self):
        return "name: %s; type_: %d(%s), modified: %s size: %d" % (
            self.name,
            self.type_,
            "file"
            if self.type_ == FSObjectType.FILE
            else ("dir" if self.type_ == FSObjectType.FOLDER else "bucket"),
            self.modified,
            self.size,
        )


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


class Model:
    # Parallel multipart threads per transfer; user-tunable at runtime via
    # set_transfer_concurrency ("Transfer settings…" in the UI).
    DEFAULT_TRANSFER_CONCURRENCY = 4
    MAX_TRANSFER_CONCURRENCY = 16
    # Files in flight at once. Total connections ≈ parallel_files × concurrency.
    DEFAULT_PARALLEL_FILES = 4
    MAX_PARALLEL_FILES = 32
    # Downloads this size or larger go through the resumable ranged path.
    RESUME_THRESHOLD = 16 * 1024 * 1024

    # Multipart tuning. Non-AWS backends often want a different part size, and
    # the part size also decides how a resumable upload is chunked.
    # S3 operations that accept RequestPayer. Bucket-level calls (ListBuckets,
    # GetBucketVersioning, …) do not, and sending it there is a hard
    # ParamValidationError rather than something the service ignores.
    REQUESTER_PAYS_OPERATIONS = (
        "GetObject", "HeadObject", "PutObject", "DeleteObject",
        "DeleteObjects", "CopyObject", "ListObjects", "ListObjectsV2",
        "ListObjectVersions", "ListMultipartUploads", "ListParts",
        "CreateMultipartUpload", "UploadPart", "UploadPartCopy",
        "CompleteMultipartUpload", "AbortMultipartUpload", "RestoreObject",
        "GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging",
        "GetObjectAcl", "PutObjectAcl", "GetObjectAttributes",
    )

    DEFAULT_MULTIPART_THRESHOLD_MB = 16
    DEFAULT_MULTIPART_CHUNKSIZE_MB = 8
    MIN_MULTIPART_CHUNKSIZE_MB = 5      # S3's own floor for a non-final part
    MAX_MULTIPART_CHUNKSIZE_MB = 512
    PART_SUFFIX = ".s3duckpart"

    def __init__(
        self,
        endpoint_url,
        region_name,
        access_key,
        secret_key,
        bucket,
        no_ssl_check,
        use_path,
        timeout=3,
        retries=3,
        read_timeout=60,
        transfer_concurrency=None,
        session_token=None,
        parallel_files=None,
        read_only=False,
        requester_pays=False,
        public_base_url="",
        proxy_url="",
        ca_bundle="",
    ):
        self.session = boto3.session.Session()
        self._client = None
        # Serialises rebind_bucket: parallel transfers can hit a region error
        # at the same moment and would otherwise race on _client/endpoint.
        self._rebind_lock = threading.RLock()

        # navigation state
        self.current_folder = ""
        self.prev_folder = ""

        # connection state (mutable while navigating buckets)
        self.endpoint_url = endpoint_url

        # keep original/root settings for when we leave a bucket (used by bucket list view)
        self.profile_endpoint_url = endpoint_url
        self.profile_use_path = use_path

        # region that came from the profile (stable "home" region)
        self.profile_region = region_name
        # region currently in use (can change per bucket)
        self.region_name = region_name

        self.access_key = access_key
        self.secret_key = secret_key
        # Set for temporary credentials (STS / SSO / assumed role / MFA).
        self.session_token = session_token or ""
        self.bucket = bucket or ""  # may be empty (bucket list mode)
        self.no_ssl_check = no_ssl_check
        self.use_path = use_path  # True -> path-style, False -> virtual-host style
        self.timeout = timeout
        self.retries = retries
        self.read_timeout = read_timeout

        # bucket -> (endpoint_url, region, use_path) that was proven to work.
        # Avoids re-probing every addressing-style/region combination on each
        # bucket open; persisted by the UI across sessions.
        self.binding_cache = {}

        # Extra args applied to every upload (storage class, encryption).
        self.upload_extra_args = {}

        # boto3 never guesses a content type, so without this every uploaded
        # object is stored as binary/octet-stream and a browser opening its
        # public URL downloads it instead of rendering it.
        self.detect_content_type = True
        # extension (no dot, lower-case) -> content type; beats the tables.
        self.content_type_overrides = {}

        # Per-destination upload rules: [{pattern, storage_class, tags}].
        # "Anything under archive/ goes to Glacier" is a policy, and applying
        # it by hand on every upload is how it stops being one.
        self.upload_rules = []

        # Writes are refused entirely when the profile is read-only.
        self.read_only = bool(read_only)

        # Requester-pays buckets refuse every read that does not opt in to
        # being billed for it, so without this such a bucket is unusable.
        self.requester_pays = bool(requester_pays)

        # A CDN or custom domain in front of this bucket. Public and presigned
        # links are shown against it instead of the raw endpoint.
        self.public_base_url = str(public_base_url or "").rstrip("/")

        # An outbound HTTP proxy, and a CA bundle to trust. The bundle is the
        # right answer to a corporate MITM certificate or a self-signed MinIO
        # one; no_ssl_check turns verification off entirely and is not.
        self.proxy_url = str(proxy_url or "").strip()
        self.ca_bundle = str(ca_bundle or "").strip()

        # Shared across all transfers so the cap is a total, not per file.
        self.rate_limiter = None

        # Set by list() when a cap stopped the walk before the end.
        self.last_listing_truncated = False

        # Compare the downloaded bytes against the object's ETag afterwards.
        self.verify_downloads = False
        self.checksum_algorithm = ""
        # Downloads at or above this size use the resumable ranged path.
        self.resume_threshold = self.RESUME_THRESHOLD

        # How many *files* move at once (transfer_concurrency parallelises the
        # chunks within one file).
        self.set_parallel_files(
            self.DEFAULT_PARALLEL_FILES if parallel_files is None else parallel_files
        )

        self.multipart_threshold_mb = self.DEFAULT_MULTIPART_THRESHOLD_MB
        self.multipart_chunksize_mb = self.DEFAULT_MULTIPART_CHUNKSIZE_MB
        # Ranged downloads chunk at the part size by default; kept as its own
        # attribute so a test can use a size the MiB setting cannot express.
        self.resume_chunk_size = self.multipart_chunksize_bytes
        # Part size actually used when chunking an upload; its own attribute
        # for the same reason as resume_chunk_size — a test needs a size the
        # MiB setting cannot express.
        self.upload_chunk_size = self.multipart_chunksize_bytes
        self.resumable_uploads = True
        # Resume records must outlive the process, so they live in the user's
        # config tree rather than a temp dir. Overridable for tests.
        self.upload_state_dir = os.path.join(
            os.path.expanduser("~"), ".config", "s3duck", "uploads")
        self.set_transfer_concurrency(
            self.DEFAULT_TRANSFER_CONCURRENCY
            if transfer_concurrency is None else transfer_concurrency
        )

    def set_transfer_concurrency(self, n) -> int:
        """
        Rebuild both TransferConfigs with 'n' parallel multipart threads
        (clamped to 1..MAX_TRANSFER_CONCURRENCY). Returns the value applied.
        Transfers already running keep the config object they started with;
        new transfers pick this up.
        """
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = self.DEFAULT_TRANSFER_CONCURRENCY
        n = max(1, min(n, self.MAX_TRANSFER_CONCURRENCY))
        self.transfer_concurrency = n
        self._rebuild_transfer_configs()
        return n

    def _rebuild_transfer_configs(self):
        """Both TransferConfigs from the current concurrency and part sizes."""
        cfg = dict(
            multipart_threshold=self.multipart_threshold_bytes,
            multipart_chunksize=self.multipart_chunksize_bytes,
            io_chunksize=256 * 1024,
            max_concurrency=self.transfer_concurrency,
            use_threads=True,
        )
        self.transfer_cfg_download = TransferConfig(**cfg)
        self.transfer_cfg_upload = TransferConfig(**cfg)

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

    def set_parallel_files(self, n) -> int:
        """How many files transfer simultaneously (1 = strictly sequential)."""
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = self.DEFAULT_PARALLEL_FILES
        self.parallel_files = max(1, min(n, self.MAX_PARALLEL_FILES))
        return self.parallel_files

    def set_rate_limit(self, bytes_per_sec):
        """Cap total transfer throughput; 0/None removes the limit."""
        try:
            rate = float(bytes_per_sec or 0)
        except (TypeError, ValueError):
            rate = 0.0
        self.rate_limiter = RateLimiter(rate) if rate > 0 else None
        return self.rate_limiter

    def _guard_write(self):
        if self.read_only:
            raise ReadOnlyError(
                "This profile is read-only; the operation was blocked."
            )

    # Server-side encryption modes offered in the UI.
    SSE_MODES = ("", "AES256", "aws:kms")

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
            if algorithm == "CRC32":
                # Without this a multipart upload stores a per-part composite,
                # which is as unverifiable as a multipart ETag. Only the CRC
                # algorithms support a whole-object checksum.
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

    @staticmethod
    def parse_content_type_overrides(text) -> dict:
        """
        Read the user's ``ext = type`` lines into a mapping.

        Unparseable lines are dropped rather than raising: this is edited in a
        free-text box, and a half-typed line must not cost the whole table.
        """
        overrides = {}
        for line in str(text or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            separator = "=" if "=" in line else (":" if ":" in line else "")
            if not separator:
                continue
            ext, _, value = line.partition(separator)
            ext = ext.strip().lstrip("*").lstrip(".").lower()
            value = value.strip()
            if ext and value:
                overrides[ext] = value
        return overrides

    @staticmethod
    def format_content_type_overrides(overrides) -> str:
        return "\n".join(
            f"{ext} = {value}"
            for ext, value in sorted(dict(overrides or {}).items()))

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

    @staticmethod
    def parse_upload_rules(text) -> list:
        """
        Read ``glob -> class=X, tag:k=v`` lines into rules.

        Unparseable lines are dropped rather than raising: this is edited in a
        free-text box, and a half-typed rule must not cost the whole table.
        """
        rules = []
        for line in str(text or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "->" not in line:
                continue
            pattern, _, rest = line.partition("->")
            pattern = pattern.strip()
            if not pattern:
                continue
            rule = {"pattern": pattern, "storage_class": "", "tags": {}}
            for part in rest.split(","):
                name, _, value = part.strip().partition("=")
                name = name.strip().lower()
                value = value.strip()
                if not value:
                    continue
                if name == "class":
                    rule["storage_class"] = value.upper()
                elif name.startswith("tag:"):
                    tag = name[4:].strip()
                    if tag:
                        rule["tags"][tag] = value
            if rule["storage_class"] or rule["tags"]:
                rules.append(rule)
        return rules

    @staticmethod
    def format_upload_rules(rules) -> str:
        lines = []
        for rule in list(rules or []):
            parts = []
            if rule.get("storage_class"):
                parts.append(f"class={rule['storage_class']}")
            for key, value in sorted(dict(rule.get("tags") or {}).items()):
                parts.append(f"tag:{key}={value}")
            if parts:
                lines.append(f"{rule.get('pattern', '')} -> "
                             + ", ".join(parts))
        return "\n".join(lines)

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
        return args

    def clone_for_worker(self):
        """
        Build a lightweight copy of this model that owns its own boto3 session
        and client cache, so background workers can call list()/list_buckets()
        without racing the main thread on shared client/region/endpoint state.

        Connection state and current navigation state are snapshotted by value;
        the cached _client is intentionally not shared.
        """
        cls = type(self)
        m = cls.__new__(cls)
        m.session = boto3.session.Session()
        m._client = None
        m.current_folder = self.current_folder
        m.prev_folder = self.prev_folder
        m.endpoint_url = self.endpoint_url
        m.profile_endpoint_url = self.profile_endpoint_url
        m.profile_use_path = self.profile_use_path
        m.profile_region = self.profile_region
        m.region_name = self.region_name
        m.access_key = self.access_key
        m.secret_key = self.secret_key
        m.session_token = self.session_token
        m.read_only = self.read_only
        m.requester_pays = self.requester_pays
        m.public_base_url = self.public_base_url
        m.proxy_url = self.proxy_url
        m.ca_bundle = self.ca_bundle
        m.parallel_files = self.parallel_files
        m.rate_limiter = self.rate_limiter
        m.verify_downloads = self.verify_downloads
        m.checksum_algorithm = self.checksum_algorithm
        m.multipart_threshold_mb = self.multipart_threshold_mb
        m.multipart_chunksize_mb = self.multipart_chunksize_mb
        m.resume_chunk_size = self.resume_chunk_size
        m.upload_chunk_size = self.upload_chunk_size
        m.resumable_uploads = self.resumable_uploads
        m.upload_state_dir = self.upload_state_dir
        m.resume_threshold = self.resume_threshold
        m._rebind_lock = threading.RLock()
        m.bucket = self.bucket
        # Shared by reference on purpose: a binding a worker discovers is worth
        # keeping for the whole app.
        m.binding_cache = self.binding_cache
        m.last_listing_truncated = False
        m.upload_extra_args = dict(self.upload_extra_args)
        m.detect_content_type = self.detect_content_type
        m.content_type_overrides = dict(self.content_type_overrides)
        m.upload_rules = [dict(rule) for rule in self.upload_rules]
        m.no_ssl_check = self.no_ssl_check
        m.use_path = self.use_path
        m.timeout = self.timeout
        m.retries = self.retries
        m.read_timeout = self.read_timeout
        m.transfer_concurrency = self.transfer_concurrency
        m.transfer_cfg_download = self.transfer_cfg_download
        m.transfer_cfg_upload = self.transfer_cfg_upload
        return m

    def proxy_settings(self) -> dict:
        """
        The profile's proxy as botocore expects it, or {} when unset.

        One URL covers both schemes: a proxy configured for an S3 endpoint is
        the way out of the network, not a per-scheme choice.
        """
        url = str(self.proxy_url or "").strip()
        if not url:
            return {}
        if "://" not in url:
            url = "http://" + url
        return {"http": url, "https": url}

    def check_tls_settings(self) -> str:
        """
        A human-readable problem with the TLS settings, or "".

        A CA bundle that is not there produces an SSLError deep inside the
        first request, which reads like the server is broken.
        """
        if self.no_ssl_check and self.ca_bundle:
            return ("Certificate verification is switched off, so the CA "
                    "bundle is ignored.")
        if self.ca_bundle and not os.path.isfile(self.ca_bundle):
            return f"CA bundle not found: {self.ca_bundle}"
        return ""

    @staticmethod
    def get_os_family():
        return platform.system()

    def _make_client(
        self,
        *,
        endpoint_url=None,
        region=None,
        use_path=None,
    ):
        """
        Build (but do NOT cache) an S3 client from overrides or current object state.
        """
        endpoint_url = endpoint_url if endpoint_url is not None else self.endpoint_url
        region = region if region is not None else self.region_name
        use_path = self.use_path if use_path is None else use_path

        params = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
        }
        if self.session_token:
            params["aws_session_token"] = self.session_token
        if region:
            params.update({"region_name": region})

        s3_config = (
            {"addressing_style": "virtual"}
            if not use_path
            else {"addressing_style": "path"}
        )
        if self.no_ssl_check:
            params.update({"verify": False})
        elif self.ca_bundle:
            # A path, not a boolean: trusting one extra CA is not the same as
            # trusting anything that answers.
            params.update({"verify": self.ca_bundle})

        config_args = {
            "s3": s3_config,
            "connect_timeout": self.timeout,
            "read_timeout": self.read_timeout,
            "retries": {"max_attempts": self.retries, "mode": "standard"},
        }
        proxies = self.proxy_settings()
        if proxies:
            config_args["proxies"] = proxies
        params.update({"config": botocore.config.Config(**config_args)})
        client = self.session.client("s3", **params)
        if self.requester_pays:
            self._register_requester_pays(client)
        return client

    def _register_requester_pays(self, client):
        """
        Add ``RequestPayer=requester`` to every call that accepts it.

        Threading the argument through forty call sites would mean forgetting
        one, and the symptom of forgetting is a bare AccessDenied on a bucket
        that looked fine a moment ago. botocore's parameter hook covers the
        managed transfer paths too, which never see our keyword arguments at
        all.
        """
        def _add(params, **_kwargs):
            if isinstance(params, dict):
                params.setdefault("RequestPayer", "requester")

        for operation in self.REQUESTER_PAYS_OPERATIONS:
            client.meta.events.register(
                f"provide-client-params.s3.{operation}", _add)
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._make_client()
        return self._client

    def _endpoint_has_bucket(self, endpoint_url: str, bucket_name: str) -> bool:
        """
        Return True if the hostname in endpoint_url already appears to be
        bucket-specific (contains *this* bucket name).
        """
        try:
            host = urlparse(endpoint_url).hostname or ""
        except Exception:
            host = endpoint_url or ""
        host = host.lower()
        bucket_name = bucket_name.lower()
        return host.startswith(bucket_name + ".") or (("." + bucket_name + ".") in ("." + host + "."))

    def _extract_leftmost_label(self, endpoint_url: str) -> str:
        """
        Best-effort extraction of the leftmost DNS label from endpoint host.
        Used only for diagnostics / mismatch error messages.
        """
        try:
            host = (urlparse(endpoint_url).hostname or "").strip().lower()
        except Exception:
            host = (endpoint_url or "").strip().lower()
        if not host:
            return ""
        return host.split(".")[0]

    def _redirect_conflicts_with_bucket(self, endpoint_url: str,
                                        bucket_name: str) -> bool:
        """
        True when endpoint_url's host is a virtual-host-style address for a
        DIFFERENT bucket (e.g. 'other.s3.region.amazonaws.com' while binding
        'mybucket'). Service-level hosts such as 's3.region.amazonaws.com' or
        'minio.local' are not bucket-bound and must not be rejected.
        """
        try:
            host = (urlparse(endpoint_url).hostname or "").strip().lower()
        except Exception:
            host = (endpoint_url or "").strip().lower()
        parts = [p for p in host.split(".") if p]
        if len(parts) < 3:
            return False
        if parts[0] == (bucket_name or "").strip().lower():
            return False
        second = parts[1]
        return second == "s3" or second.startswith("s3-")

    def _can_list_bucket(self, client_obj, bucket_name: str) -> bool:
        """True if this client can list a single page of *bucket_name*."""
        try:
            paginator = client_obj.get_paginator("list_objects_v2")
            iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix="",
                Delimiter="/",
                PaginationConfig={"MaxItems": 1},
            )
            for _ in iterator:
                break
            return True
        except Exception:
            return False

    def _try_bind_bucket(self, bucket_name: str):
        """
        Resolve a working (client, endpoint, region, use_path) for this bucket.

        A previously proven combination is tried first — the full probe below
        costs several round trips per bucket open, which is the single most
        expensive thing this app does on an off-region bucket. A stale entry is
        dropped and the probe re-runs.
        """
        cached = self.binding_cache.get(bucket_name)
        if cached:
            endpoint, region, use_path = cached
            client = self._make_client(
                endpoint_url=endpoint, region=region, use_path=use_path
            )
            if self._can_list_bucket(client, bucket_name):
                return client, endpoint, region, use_path
            self.binding_cache.pop(bucket_name, None)

        result = self._probe_bucket_binding(bucket_name)
        self.binding_cache[bucket_name] = (result[1], result[2], result[3])
        return result

    def _probe_bucket_binding(self, bucket_name: str):
        """
        Try different combinations (endpoint_url, path/virtual style) until
        ListObjectsV2 works on this bucket.

        Returns (client_ok, endpoint_url, region, use_path)

        Raises last seen error on total failure.
        """

        def can_list(c):
            """
            Try to list a single page to validate this client for this bucket.
            Returns (ok:bool, permanent_redirect_endpoint:str|None, error:Exception|None)

            Logic:
            - If it lists fine: ok=True
            - If we get PermanentRedirect: ok=False + endpoint hint
            - Any other ClientError is treated as a real failure (ok=False + error)
            """
            paginator = c.get_paginator("list_objects_v2")
            try:
                iterator = paginator.paginate(
                    Bucket=bucket_name,
                    Prefix="",
                    Delimiter="/",
                    PaginationConfig={"MaxItems": 1},
                )
                for _ in iterator:
                    break
                return True, None, None
            except botocore.exceptions.ClientError as exc:
                err_code = exc.response.get("Error", {}).get("Code", "")

                # Region/endpoint redirect from S3
                if err_code == "PermanentRedirect":
                    ep = exc.response.get("Error", {}).get("Endpoint")
                    return False, ep, exc

                return False, None, exc

            except Exception as exc:
                return False, None, exc

        last_err = None

        # Strategy A: current endpoint + current style
        cA = self._make_client(
            endpoint_url=self.endpoint_url,
            region=self.region_name,
            use_path=self.use_path,
        )
        ok, ep_hint, err = can_list(cA)
        if ok:
            return (cA, self.endpoint_url, self.region_name, self.use_path)
        last_err = err

        # Strategy B: current endpoint + flipped style
        cB = self._make_client(
            endpoint_url=self.endpoint_url,
            region=self.region_name,
            use_path=(not self.use_path),
        )
        ok, ep_hint2, err2 = can_list(cB)
        if ok:
            return (cB, self.endpoint_url, self.region_name, not self.use_path)
        last_err = err2 or last_err

        # Strategy C: endpoint hint(s) from PermanentRedirect
        endpoint_candidates = []
        if ep_hint:
            endpoint_candidates.append(ep_hint)
        if ep_hint2 and ep_hint2 not in endpoint_candidates:
            endpoint_candidates.append(ep_hint2)

        for ep_raw in endpoint_candidates:
            # If it's bare hostname, prepend scheme
            if "://" not in ep_raw:
                endpoint_fixed = "https://" + ep_raw
            else:
                endpoint_fixed = ep_raw

            # If the hinted endpoint is virtual-host-bound to a DIFFERENT
            # bucket, error out explicitly. (Service-level hosts like
            # s3.<region>.amazonaws.com must still be tried below.)
            if self._redirect_conflicts_with_bucket(endpoint_fixed, bucket_name):
                leftmost = self._extract_leftmost_label(endpoint_fixed)
                raise RuntimeError(
                    f"Endpoint redirect '{endpoint_fixed}' appears bound to bucket '{leftmost}', "
                    f"which does not match requested bucket '{bucket_name}'. "
                    f"Please verify endpoint and addressing style."
                )

            has_bucket_already = self._endpoint_has_bucket(endpoint_fixed, bucket_name)

            styles_to_try = (
                [True] if has_bucket_already else [self.use_path, not self.use_path]
            )

            for style in styles_to_try:
                cGuess = self._make_client(
                    endpoint_url=endpoint_fixed,
                    region=self.region_name,
                    use_path=style,
                )
                ok3, _ep_ignore, _err3 = can_list(cGuess)
                if ok3:
                    return (cGuess, endpoint_fixed, self.region_name, style)
                last_err = _err3 or last_err

        # nothing worked
        if last_err:
            raise last_err
        raise Exception("Cannot access bucket")

    @staticmethod
    def _is_region_error(exc: Exception) -> bool:
        """
        Return True if *exc* is a recoverable region/endpoint mismatch that
        can be fixed by re-probing the bucket for the right endpoint/region.
        """
        if not isinstance(exc, botocore.exceptions.ClientError):
            return False
        code = (exc.response.get("Error") or {}).get("Code", "")
        if code in ("AuthorizationHeaderMalformed", "PermanentRedirect"):
            return True
        msg = ((exc.response.get("Error") or {}).get("Message") or "").lower()
        return "expecting" in msg and "region" in msg

    def rebind_bucket(self, log_fn=None):
        """
        Re-probe the current bucket to find the correct endpoint/region combo
        and update self in-place.  Mirrors the retry logic in BucketEnterWorker
        so all operations (upload/download/delete/list) benefit from the same
        automatic region correction.

        log_fn: optional callable(str) — receives progress messages
                (e.g. Worker.progress.emit).
        Raises on total failure.
        """
        bucket = self.bucket
        if not bucket:
            return
        with self._rebind_lock:
            self._rebind_bucket_locked(bucket, log_fn)

    def _rebind_bucket_locked(self, bucket, log_fn=None):
        region_hint = endpoint_hint = None
        try:
            region_hint, endpoint_hint = self.get_bucket_hints(bucket)
        except Exception as hint_exc:
            if log_fn:
                log_fn(f"While probing hints for '{bucket}': {hint_exc}")

        if log_fn:
            if region_hint:
                log_fn(f"Hint: bucket '{bucket}' region may be '{region_hint}'")
            else:
                log_fn(f"Hint: bucket '{bucket}' region unknown (no header)")
            if endpoint_hint:
                log_fn(f"Hint: suggested endpoint for '{bucket}': {endpoint_hint}")

        if region_hint:
            base_endpoint = self.profile_endpoint_url or self.endpoint_url
            swapped = self.build_region_swapped_endpoint(base_endpoint, region_hint)
            candidate_endpoint = endpoint_hint or swapped
            if candidate_endpoint:
                old = (self.endpoint_url, self.region_name, self.use_path, self._client)
                try:
                    if log_fn:
                        log_fn(
                            f"Retry: temporarily switching endpoint to "
                            f"'{candidate_endpoint}' and region to "
                            f"'{region_hint}' for bucket '{bucket}'"
                        )
                    self.endpoint_url = candidate_endpoint
                    self.region_name = region_hint
                    self._client = None
                    self.enter_bucket(bucket)   # validates + commits new state
                    return                      # ← success
                except Exception as rexc:
                    if log_fn:
                        log_fn(f"Retry failed for '{bucket}': {rexc}")
                    self.endpoint_url, self.region_name, self.use_path, self._client = old

        client_ok, new_ep, new_region, new_use_path = self._try_bind_bucket(bucket)
        self.endpoint_url = new_ep
        self.region_name = new_region
        self.use_path = new_use_path
        self._client = client_ok
        if log_fn:
            log_fn(f"Re-bound: endpoint={new_ep} region={new_region}")

    def enter_bucket(self, bucket_name: str):
        """
        Transactional bucket entry:
        - Probe working combo for this bucket.
        - If probe succeeds, commit new client + nav state.
        - If it fails, raise without touching current state.
        """
        client_ok, new_endpoint, new_region, new_use_path = self._try_bind_bucket(
            bucket_name
        )

        # Success -> commit new working config for THIS bucket
        self.bucket = bucket_name
        self.endpoint_url = new_endpoint
        self.region_name = new_region          # region_name may become bucket-specific
        self.use_path = new_use_path
        self._client = client_ok               # working client for this bucket

        # reset navigation inside bucket
        self.current_folder = ""
        self.prev_folder = ""

    def list_buckets(self):
        """
        Return all buckets visible to the credentials.

        Adaptive region logic for ListBuckets:
        - Some backends demand a specific signing region and tell us via
          AuthorizationHeaderMalformed "... expecting '<region>'".
        - We'll chase that hint before giving up.

        IMPORTANT: always use the *profile/root* endpoint for ListBuckets so we
        don't accidentally call a bucket-scoped host after leaving a bucket.
        """
        # build initial region candidates, dedupe while preserving order
        initial_candidates = ["us-east-1", self.profile_region, self.region_name]
        queue = []
        for r in initial_candidates:
            if r and r not in queue:
                queue.append(r)

        tried = set()
        last_err = None
        buckets_resp = None

        ATTEMPT_LIMIT = 10
        attempts = 0

        while queue and attempts < ATTEMPT_LIMIT:
            attempts += 1
            candidate_region = queue.pop(0)
            if candidate_region in tried:
                continue
            tried.add(candidate_region)

            try:
                # Pin to the saved root endpoint
                tmp_client = self._make_client(
                    region=candidate_region,
                    endpoint_url=self.profile_endpoint_url
                )
                buckets_resp = tmp_client.list_buckets()
                last_err = None
                break  # success
            except botocore.exceptions.ClientError as exc:
                last_err = exc
                err_code = exc.response.get("Error", {}).get("Code", "")
                if err_code == "AuthorizationHeaderMalformed":
                    msg = exc.response.get("Error", {}).get("Message", "") or str(exc)
                    expecting_region = None
                    marker = "expecting '"
                    idx = msg.find(marker)
                    if idx != -1:
                        rest = msg[idx + len(marker):]
                        endq = rest.find("'")
                        if endq != -1:
                            expecting_region = rest[:endq].strip()
                    if expecting_region and expecting_region not in tried and expecting_region not in queue:
                        queue.append(expecting_region)
            except Exception as exc:
                last_err = exc

        if buckets_resp is None:
            raise last_err if last_err else Exception("Cannot list buckets")

        items = []
        for b in buckets_resp.get("Buckets", []):
            items.append(Item(b["Name"], FSObjectType.BUCKET, "", 0))
        return items

    def create_bucket(self, bucket_name: str):
        """
        Create a new bucket.
        We'll try to create it in the profile_region.
        For AWS S3:
          - us-east-1 is special: you cannot/should not pass LocationConstraint.
        """
        self._guard_write()
        params = {"Bucket": bucket_name}

        region = self.profile_region or "us-east-1"
        if region and region != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": region
            }

        root_client = self._make_client(region=region)
        root_client.create_bucket(**params)

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

        def flush():
            nonlocal pending, deleted
            if not pending:
                return
            bucket_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": pending, "Quiet": True},
            )
            deleted += len(pending)
            if log_fn:
                log_fn(f"{bucket_name}: deleted {deleted} object(s)")
            pending = []

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

    def _list_bucket_once(self, client_obj, bucket_name, prefix,
                          page_cb=None, max_items=0):
        """
        Try to list 'prefix' in 'bucket_name' using client_obj once.

        Returns tuple: (ok, items, expecting_region, fatal_exc, truncated)

        ok=True  -> listing succeeded
        items    -> list[Item] if ok
        expecting_region -> str or None; if not None, server hinted "use this region instead"
        fatal_exc -> Exception or None if this attempt should be considered a hard failure
        truncated -> True when max_items stopped the walk early

        ``page_cb(count)`` is called after every page so a caller can show
        progress: a prefix with a hundred thousand direct children used to
        take a long, completely silent minute.
        """
        paginator = client_obj.get_paginator("list_objects_v2")
        try:
            pages = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix or "",
                Delimiter="/",
            )
        except botocore.exceptions.ClientError as exc:
            err_code = exc.response.get("Error", {}).get("Code", "")

            # AuthorizationHeaderMalformed: maybe wrong region
            if err_code == "AuthorizationHeaderMalformed":
                msg = exc.response.get("Error", {}).get("Message", "") or str(exc)
                expecting_region = None
                marker = "expecting '"
                idx = msg.find(marker)
                if idx != -1:
                    rest = msg[idx + len(marker):]
                    endq = rest.find("'")
                    if endq != -1:
                        expecting_region = rest[:endq].strip()
                return False, [], expecting_region, None, False

            # Access/NoSuchKey/etc: treat as **fatal** now (surface real boto3 message)
            if err_code in ("NoSuchKey", "AccessDenied", "AllAccessDisabled"):
                return False, [], None, exc, False

            # other real failure
            return False, [], None, exc, False

        except Exception as exc:
            # unknown non-ClientError; treat as fatal
            return False, [], None, exc, False

        # We got a paginator successfully. Now accumulate.
        items = []
        truncated = False
        limit = int(max_items or 0)
        try:
            for page in pages:
                folders = [fld2["Prefix"] for fld2 in page.get("CommonPrefixes", [])]
                objects = [obj for obj in page.get("Contents", [])]

                for folder in folders:
                    s = folder.split("/")
                    if len(s) > 1:
                        folder_name = s[-2]
                    else:
                        folder_name = folder.rstrip("/")
                    items.append(Item(folder_name, FSObjectType.FOLDER, "", 0))

                for obj in objects:
                    key = obj["Key"]
                    if key == (prefix or ""):
                        continue
                    filename = key.split("/")[-1]
                    items.append(
                        Item(
                            filename,
                            FSObjectType.FILE,
                            obj["LastModified"],
                            obj["Size"],
                            storage_class=obj.get("StorageClass") or "STANDARD",
                            etag=(obj.get("ETag") or "").replace('"', ""),
                        )
                    )

                if page_cb is not None:
                    page_cb(len(items))
                if limit and len(items) >= limit:
                    # Stop rather than build a model the view cannot render;
                    # the caller says so instead of pretending this is all.
                    del items[limit:]
                    truncated = True
                    break
        except botocore.exceptions.ClientError as exc:
            err_code = exc.response.get("Error", {}).get("Code", "")
            if err_code in ("NoSuchKey", "AccessDenied", "AllAccessDisabled"):
                return False, [], None, exc, False
            return False, [], None, exc, False
        except Exception as exc:
            return False, [], None, exc, False

        return True, items, None, None, truncated

    def list(self, fld, page_cb=None, max_items=0):
        """
        List objects/prefixes in the currently selected bucket under prefix 'fld'.

        Adaptive region logic like list_buckets(), but bucket-scoped:
        - First try with the current active client/region.
        - If we get "AuthorizationHeaderMalformed ... expecting '<region>'",
          we retry with that region.
        - If that retry succeeds, we *promote* that region/client to become
          our active client for this bucket so the rest of the UI keeps working.
        """
        prefix = fld or ""
        self.last_listing_truncated = False

        # 1. Try with the current live client
        ok, items, expecting_region, fatal_exc, cut = self._list_bucket_once(
            self.client, self.bucket, prefix, page_cb=page_cb,
            max_items=max_items
        )
        if ok:
            self.last_listing_truncated = cut
            return items
        if fatal_exc:
            # true fatal -> raise original error with details
            raise fatal_exc

        # 2. If server told us a better region, try that region
        if expecting_region:
            tmp_client = self._make_client(region=expecting_region)
            (ok2, items2, expecting_region2,
             fatal_exc2, cut2) = self._list_bucket_once(
                tmp_client, self.bucket, prefix, page_cb=page_cb,
                max_items=max_items
            )
            if ok2:
                # success in new region -> adopt it permanently for this bucket
                self.region_name = expecting_region
                self._client = tmp_client
                self.last_listing_truncated = cut2
                return items2
            if fatal_exc2:
                raise fatal_exc2

        # 3. Nothing worked
        raise Exception(
            f"Cannot list bucket '{self.bucket}' at prefix '{prefix}' with available regions."
        )

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
            os.makedirs(plan.base_dir, exist_ok=True)
            if log_fn:
                for k in plan.unsafe:
                    log_fn(f"skipped unsafe key (escapes target dir): {k}")
            for out_path in plan.dirs:
                os.makedirs(out_path, exist_ok=True)
            for _k, out_path, _size in plan.files:
                os.makedirs(os.path.dirname(out_path), exist_ok=True)

            # Whole-prefix downloads are one job entry, so without this the
            # parallel-files setting would do nothing for "download a folder".
            run_parallel(
                plan.files,
                lambda item: _download_one(self.bucket, item[0], item[1], item[2]),
                self.parallel_files,
                cancel_event=cancel_event,
            )
            return

        size = None
        etag = ""
        head = None
        try:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            head = self.head_with_checksum(key)
            size = head.get("ContentLength")
            etag = self.normalize_etag(head.get("ETag"))
        except TransferCancelled:
            raise
        except Exception as exc:
            if self._is_region_error(exc):
                if log_fn:
                    log_fn(f"Region error on head_object for '{key}': {exc}")
                self.rebind_bucket(log_fn=log_fn)
                try:
                    head = self.head_with_checksum(key)
                    size = head.get("ContentLength")
                    etag = self.normalize_etag(head.get("ETag"))
                except Exception:
                    pass  # non-fatal; proceed without size

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        # ensure parent dir exists (cheap safety)
        try:
            parent = os.path.dirname(local_name)
            if parent:
                os.makedirs(parent, exist_ok=True)
        except Exception:
            pass

        # Large downloads take the ranged path so an interruption can resume
        # instead of starting over; small ones stay on the managed transfer.
        if size is not None and int(size) >= int(self.resume_threshold):
            self._download_ranged(
                key, local_name, size, etag, progress_cb=progress_cb,
                cancel_event=cancel_event, log_fn=log_fn,
            )
            return

        _download_one(self.bucket, key, local_name, size)

        if self.verify_downloads and (etag or head):
            if not self.verify_download(local_name, etag, head=head,
                                        log_fn=log_fn, key=key):
                raise Exception(
                    f"Checksum mismatch after downloading '{key}'; the local "
                    "file does not match the object's stored digest"
                )

    @staticmethod
    def normalize_etag(etag) -> str:
        return (etag or "").replace('"', "").strip()

    @classmethod
    def etag_is_md5(cls, etag) -> bool:
        """
        True when the ETag is a plain MD5 we can reproduce locally. Multipart
        uploads use a '<md5-of-md5s>-<parts>' form that cannot be checked
        without knowing the original part boundaries.
        """
        tag = cls.normalize_etag(etag)
        return bool(tag) and "-" not in tag and len(tag) == 32

    @staticmethod
    def file_md5(path, chunk_size: int = 1024 * 1024) -> str:
        digest = hashlib.md5()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(chunk_size), b""):
                digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def file_checksum(path, algorithm, chunk_size: int = 1024 * 1024) -> str:
        """
        The object checksum S3 would report for this file, base64-encoded.

        Only algorithms computable from the standard library are offered —
        CRC32C would need a third-party module, and a checksum we cannot
        recompute locally is worse than none because it would silently pass.
        """
        name = str(algorithm or "").upper()
        if name not in CHECKSUM_ALGORITHMS:
            raise ValueError(f"unsupported checksum algorithm: {algorithm}")
        if name == "CRC32":
            crc = 0
            with open(path, "rb") as handle:
                for block in iter(lambda: handle.read(chunk_size), b""):
                    crc = zlib.crc32(block, crc)
            return base64.b64encode(struct.pack(">I", crc & 0xFFFFFFFF)).decode()
        digest = hashlib.new("sha1" if name == "SHA1" else "sha256")
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(chunk_size), b""):
                digest.update(block)
        return base64.b64encode(digest.digest()).decode()

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
        crc = 0
        digest = None
        if name in ("SHA1", "SHA256"):
            digest = hashlib.new("sha1" if name == "SHA1" else "sha256")
        remaining = int(length)
        while remaining > 0:
            block = handle.read(min(1024 * 1024, remaining))
            if not block:
                break
            remaining -= len(block)
            md5.update(block)
            if name == "CRC32":
                crc = zlib.crc32(block, crc)
            elif digest is not None:
                digest.update(block)
        if name == "CRC32":
            checksum = base64.b64encode(
                struct.pack(">I", crc & 0xFFFFFFFF)).decode()
        elif digest is not None:
            checksum = base64.b64encode(digest.digest()).decode()
        else:
            checksum = ""
        return checksum, md5.digest()

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

    @staticmethod
    def composite_etag(md5_digests) -> str:
        """
        Rebuild a multipart ETag: MD5 over the concatenated part MD5s, then
        "-<count>". This is how S3 forms one, so it is how it is checked.
        """
        digests = list(md5_digests)
        if not digests:
            return ""
        combined = hashlib.md5(b"".join(digests)).hexdigest()
        return f"{combined}-{len(digests)}"

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
    def checksum_is_composite(value) -> bool:
        """
        Whether a stored checksum is a per-part composite ('<digest>-<parts>').

        A composite cannot be reproduced without the original part boundaries,
        exactly like a multipart ETag. Full-object checksums (what CRC32 with
        ChecksumType=FULL_OBJECT produces) have no suffix and are comparable.
        """
        text = str(value or "")
        head, sep, tail = text.rpartition("-")
        return bool(sep and head and tail.isdigit())

    @staticmethod
    def stored_checksum(head) -> tuple:
        """(algorithm, value) from a head_object response, or ("", "")."""
        for name in CHECKSUM_ALGORITHMS:
            value = (head or {}).get(f"Checksum{name}")
            if value:
                return name, str(value)
        return "", ""

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
        if algorithm and expected:
            if self.checksum_is_composite(expected):
                verdict = self._verify_by_parts(
                    path, key, etag, algorithm, log_fn=log_fn)
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

    def _resume_paths(self, out_path):
        part = out_path + self.PART_SUFFIX
        return part, part + ".meta"

    def _download_ranged(self, key, out_path, size, etag, progress_cb=None,
                         cancel_event=None, log_fn=None):
        """
        Download in fixed-size ranges into a '.s3duckpart' file, recording which
        chunks landed. An interrupted transfer resumes from the sidecar instead
        of restarting, and the chunks still download in parallel.
        """
        size = int(size)
        # Follows the configured part size, so one setting governs how both
        # uploads and resumable downloads are chunked. A sidecar written with a
        # different size is discarded below rather than misread.
        chunk_size = int(self.resume_chunk_size)
        total_chunks = max(1, (size + chunk_size - 1) // chunk_size)
        part_path, meta_path = self._resume_paths(out_path)
        etag = self.normalize_etag(etag)

        done = set()
        if os.path.exists(part_path) and os.path.exists(meta_path):
            try:
                with open(meta_path) as handle:
                    saved = json.load(handle)
                if (saved.get("etag") == etag
                        and int(saved.get("size", -1)) == size
                        and int(saved.get("chunk_size", -1)) == chunk_size):
                    done = {int(i) for i in saved.get("chunks", [])}
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

        state_lock = threading.Lock()
        transferred = len(done) * chunk_size

        def _save_meta():
            tmp = meta_path + ".tmp"
            with open(tmp, "w") as handle:
                json.dump({"etag": etag, "size": size,
                           "chunk_size": chunk_size,
                           "chunks": sorted(done)}, handle)
            os.replace(tmp, meta_path)

        handle = open(part_path, "r+b")
        try:
            def _fetch(index):
                nonlocal transferred
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                start = index * chunk_size
                end = min(size, start + chunk_size) - 1
                resp = self.client.get_object(
                    Bucket=self.bucket, Key=key, Range=f"bytes={start}-{end}")
                data = resp["Body"].read()
                if self.rate_limiter is not None:
                    self.rate_limiter.consume(len(data))
                with state_lock:
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
            run_parallel(pending, _fetch, self.parallel_files,
                         cancel_event=cancel_event)
        finally:
            handle.close()

        os.replace(part_path, out_path)
        if os.path.exists(meta_path):
            os.remove(meta_path)

        if self.verify_downloads and not self.verify_download(
                out_path, etag, log_fn=log_fn, key=key):
            raise Exception(
                f"Checksum mismatch after downloading '{key}'; the local file "
                "does not match the object's ETag"
            )

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

    def get_keys(self, prefix, log_fn=None):
        """
        Return [(Key, Size), ...] for ALL objects under 'prefix', paginated.
        Includes 'folder placeholder' keys (ending with '/').
        Automatically retries once after rebinding on region/endpoint errors.
        """
        def _fetch():
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            result = []
            for page in pages:
                for obj in page.get("Contents", []) or []:
                    result.append((obj.get("Key"), obj.get("Size")))
            return result

        try:
            return _fetch()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error listing '{prefix}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return _fetch()

    def get_keys_for_bucket(self, bucket_name: str, prefix: str = ""):
        """
        Return [(Key, Size, StorageClass), ...] for objects under 'prefix' in
        *bucket_name*.

        Unlike get_keys(), this does NOT require (and does not modify) the
        current navigation state (self.bucket / current_folder).

        It uses the same adaptive endpoint/region logic as open-bucket, and
        it also retries with region/endpoint hints like the UI does when entering a bucket.
        """
        if not bucket_name:
            return []

        def _list_with_client(client_obj):
            result = []
            paginator = client_obj.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix or "")
            for page in pages:
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key")
                    if not key:
                        continue
                    size = int(obj.get("Size") or 0)
                    result.append(
                        (key, size, obj.get("StorageClass") or "STANDARD"))
            return result

        # First attempt: existing logic
        try:
            client_obj, _endpoint, _region, _use_path = self._try_bind_bucket(bucket_name)
            return _list_with_client(client_obj)
        except Exception as first_exc:
            # Retry using the same “hints” approach as entering buckets in UI
            try:
                region_hint, endpoint_hint = self.get_bucket_hints(bucket_name)
            except Exception:
                region_hint, endpoint_hint = None, None

            # If we got nothing useful, keep original error
            if not region_hint and not endpoint_hint:
                raise first_exc

            base_endpoint = self.profile_endpoint_url or self.endpoint_url

            candidate_endpoint = endpoint_hint
            if not candidate_endpoint and region_hint:
                try:
                    candidate_endpoint = self.build_region_swapped_endpoint(base_endpoint, region_hint)
                except Exception:
                    candidate_endpoint = None

            # If we still have no endpoint, we can still try with region-only (some setups ignore endpoint)
            old_endpoint = self.endpoint_url
            old_region = self.region_name
            old_use_path = self.use_path
            old_client = self._client

            try:
                if candidate_endpoint:
                    self.endpoint_url = candidate_endpoint
                if region_hint:
                    self.region_name = region_hint
                self._client = None  # force rebuild with seeded values

                client_obj, _endpoint, _region, _use_path = self._try_bind_bucket(bucket_name)
                return _list_with_client(client_obj)
            except Exception as retry_exc:
                # Preserve the most useful message
                raise retry_exc
            finally:
                self.endpoint_url = old_endpoint
                self.region_name = old_region
                self.use_path = old_use_path
                self._client = old_client

    DELETE_BATCH_SIZE = 1000

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
            self.client.delete_objects(Bucket=self.bucket, Delete=payload)

        try:
            _do()
            return
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NotImplemented", "MethodNotAllowed"):
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

    def get_bucket_versioning_status(self) -> str:
        """Return 'Enabled', 'Suspended', or '' (never configured / unknown)."""
        if not self.bucket:
            return ""
        try:
            resp = self.client.get_bucket_versioning(Bucket=self.bucket)
            return resp.get("Status", "") or ""
        except Exception:
            return ""

    def set_bucket_versioning(self, status: str, log_fn=None):
        """Enable or suspend versioning on the current bucket.

        status must be 'Enabled' or 'Suspended'.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        status = (status or "").strip().capitalize()
        if status not in ("Enabled", "Suspended"):
            raise ValueError("status must be 'Enabled' or 'Suspended'")

        def _do():
            self.client.put_bucket_versioning(
                Bucket=self.bucket,
                VersioningConfiguration={"Status": status},
            )

        try:
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error setting versioning on '{self.bucket}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

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

    def undelete(self, key: str, log_fn=None) -> int:
        """
        Undo a delete on a versioning-enabled bucket by removing the delete
        markers it left behind. For a prefix ('foo/') every marker underneath
        is removed. Returns how many objects came back.

        On a bucket without versioning there are no markers and the delete was
        permanent, so this reports 0 rather than pretending to succeed.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

        exact = not key.endswith("/")

        def _fetch_markers():
            markers = []
            paginator = self.client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
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
                    markers.append((marker_key, version_id))
            return markers

        try:
            markers = _fetch_markers()
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "NotImplemented":
                if log_fn:
                    log_fn("Backend does not support versions; nothing to undo")
                return 0
            if not self._is_region_error(exc):
                raise
            self.rebind_bucket(log_fn=log_fn)
            markers = _fetch_markers()

        restored = 0
        for marker_key, version_id in markers:
            self.client.delete_object(
                Bucket=self.bucket, Key=marker_key, VersionId=version_id)
            restored += 1
            if log_fn:
                log_fn(f"restored {marker_key}")
        return restored

    def make_version_current(self, key, version_id, log_fn=None):
        """
        Promote an older version to be the current one by copying that version
        onto the same key server-side. This creates a new current version with
        the older version's data (the standard S3 "restore a previous version"
        technique); nothing is deleted.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        copy_source = {"Bucket": self.bucket, "Key": key, "VersionId": version_id}

        def _do():
            self.client.copy_object(
                CopySource=copy_source, Bucket=self.bucket, Key=key
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
                          cancel_event=None, log_fn=None):
        """
        Upload a large file part by part so an interruption can resume.

        boto3's managed upload restarts from zero, which on a slow link makes a
        big file effectively untransferable. A failure or cancellation here
        deliberately leaves the multipart upload OPEN and its record on disk,
        because that is exactly what the next attempt resumes from — the
        Incomplete-uploads tool is how an abandoned one gets cleaned up.
        """
        chunk = int(self.upload_chunk_size)
        total_parts = max(1, (int(size) + chunk - 1) // chunk)
        mtime = os.path.getmtime(local_file)
        state_path = self._upload_state_path(key, local_file)
        extra = self.upload_args_for(local_file, key)

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
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": parts})
        # Only now is the record useless.
        try:
            os.remove(state_path)
        except OSError:
            pass
        if log_fn:
            log_fn(f"uploaded {key} in {total_parts} part(s)")

    def upload_file(self, local_file, key, progress_cb=None, cancel_event=None, log_fn=None):
        """
        Upload a file (with progress) or create a folder placeholder if local_file is None.
        Automatically retries once on region/endpoint errors.
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

    def check_profile(self):
        """
        Profile check.
        - If bucket is empty: verify credentials by listing buckets.
        - If bucket is set: verify write/delete by creating a temp folder key.
        """
        if not self.bucket:
            try:
                self.list_buckets()
                return True, None
            except Exception as exc:
                return False, str(exc)

        res_c = res_d = False
        reason = None
        key = str(uuid.uuid4())
        try:
            try:
                res_c = self.create_folder(key)
            finally:
                res_d = self.delete(key)
        except botocore.exceptions.ClientError as exc:
            reason = exc.response["Error"]["Message"]
        except Exception as exc:
            reason = str(exc)
        return bool(res_c) and res_d, reason

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

    def public_access_summary(self) -> dict:
        """
        Explain why object ACLs may be refused: Block Public Access settings
        and bucket ownership (ACLs are disabled under BucketOwnerEnforced).

        Returns {"block": {...} | None, "ownership": str, "reasons": [str]}.
        """
        out = {"block": None, "ownership": "", "reasons": []}
        if not self.bucket:
            return out
        try:
            resp = self.client.get_public_access_block(Bucket=self.bucket)
            cfg = resp.get("PublicAccessBlockConfiguration", {}) or {}
            out["block"] = cfg
            if cfg.get("BlockPublicAcls"):
                out["reasons"].append(
                    "Block Public Access: BlockPublicAcls is on, so public-read "
                    "ACLs are rejected."
                )
            if cfg.get("IgnorePublicAcls"):
                out["reasons"].append(
                    "Block Public Access: IgnorePublicAcls is on, so public "
                    "ACLs are ignored even once set."
                )
        except Exception:
            pass  # unsupported or no configuration -> nothing to report
        try:
            resp = self.client.get_bucket_ownership_controls(Bucket=self.bucket)
            rules = (resp.get("OwnershipControls", {}) or {}).get("Rules", []) or []
            if rules:
                ownership = rules[0].get("ObjectOwnership", "") or ""
                out["ownership"] = ownership
                if ownership == "BucketOwnerEnforced":
                    out["reasons"].append(
                        "Object Ownership is BucketOwnerEnforced, which disables "
                        "ACLs entirely — use a bucket policy to grant public read."
                    )
        except Exception:
            pass
        return out

    def existing_keys(self, keys) -> set:
        """
        Return the subset of *keys* that already exist, using one recursive
        listing per distinct parent prefix rather than a HEAD per key.
        """
        wanted = {k for k in keys if k}
        if not wanted or not self.bucket:
            return set()
        prefixes = set()
        for key in wanted:
            base = key.rstrip("/")
            prefixes.add(base.rsplit("/", 1)[0] + "/" if "/" in base else "")
        found = set()
        for prefix in prefixes:
            for k, _size in self.get_keys(prefix):
                if k in wanted:
                    found.add(k)
                elif k and k.endswith("/") is False:
                    # A folder target conflicts if anything lives under it.
                    for w in wanted:
                        if w.endswith("/") and k.startswith(w):
                            found.add(w)
        return found

    def direct_object_url(self, key: str) -> str:
        """
        Construct a direct (unsigned) URL for an object.

        After binding a bucket, self.endpoint_url may be virtual-host style
        (e.g. https://mybucket.s3.region.amazonaws.com) — in that case the
        bucket name is already part of the host, so we must NOT add it again
        as a path segment (that produced .../mybucket/mybucket/key).
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        key = (key or "").lstrip("/")
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

    def get_size(self, key):
        # A bare file key is also a *prefix* of sibling keys that merely share
        # the name (e.g. "a.txt" vs "a.txt.bak"), so match it exactly.
        if key and not str(key).endswith("/"):
            for k, s in self.get_keys(key):
                if k == key:
                    return int(s or 0)
            return 0
        total = 0
        for k, s in self.get_keys(key):
            if not k.endswith("/"):
                total += int(s or 0)
        return total

    def object_properties(self, key: str):
        """
        Return object metadata without opening a body stream.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty")

        return self.client.head_object(Bucket=self.bucket, Key=key)

    def get_bucket_lifecycle(self) -> list:
        """
        The bucket's lifecycle rules, or [] when it has none.

        A bucket with no configuration answers with an error rather than an
        empty document, which is not a failure worth showing.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        try:
            resp = self.client.get_bucket_lifecycle_configuration(
                Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchLifecycleConfiguration",
                        "NoSuchLifecycleConfigurationError"):
                return []
            raise
        return list(resp.get("Rules") or [])

    def put_bucket_lifecycle(self, rules, log_fn=None):
        """Replace the bucket's lifecycle rules; an empty list removes them."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if not rules:
            self.client.delete_bucket_lifecycle(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed the lifecycle configuration of {self.bucket}")
            return
        self.client.put_bucket_lifecycle_configuration(
            Bucket=self.bucket,
            LifecycleConfiguration={"Rules": list(rules)})
        if log_fn:
            log_fn(f"wrote {len(rules)} lifecycle rule(s) to {self.bucket}")

    @staticmethod
    def lifecycle_rule_scope(rule) -> str:
        """The prefix a rule applies to, across both filter shapes."""
        if "Prefix" in rule and rule.get("Prefix") is not None:
            return str(rule.get("Prefix") or "")
        node = rule.get("Filter") or {}
        if "Prefix" in node:
            return str(node.get("Prefix") or "")
        combined = node.get("And") or {}
        return str(combined.get("Prefix") or "")

    @staticmethod
    def lifecycle_rule_extras(rule) -> dict:
        """
        The parts of a filter that are NOT a prefix: tags and size bounds.

        Returns ``{"tags": [(key, value)], "min_size": int|None,
        "max_size": int|None}``. These narrow which objects a rule touches, so
        a summary that omits them describes a rule that deletes far more than
        the real one does.
        """
        node = dict((rule or {}).get("Filter") or {})
        combined = dict(node.get("And") or {})
        tags = list(combined.get("Tags") or [])
        if node.get("Tag"):
            tags = [node["Tag"]] + tags
        pairs = [(str(tag.get("Key") or ""), str(tag.get("Value") or ""))
                 for tag in tags if isinstance(tag, dict)]
        minimum = node.get("ObjectSizeGreaterThan",
                           combined.get("ObjectSizeGreaterThan"))
        maximum = node.get("ObjectSizeLessThan",
                           combined.get("ObjectSizeLessThan"))
        return {
            "tags": pairs,
            "min_size": None if minimum is None else int(minimum),
            "max_size": None if maximum is None else int(maximum),
        }

    @classmethod
    def lifecycle_filter_is_simple(cls, rule) -> bool:
        """
        Whether the prefix editor can represent this rule's filter without
        losing part of it.

        A rule filtered by tag or size that is rebuilt from a prefix form comes
        back matching every object under that prefix — which for an expiration
        rule means deleting things it never used to touch.
        """
        extras = cls.lifecycle_rule_extras(rule)
        return not (extras["tags"] or extras["min_size"] is not None
                    or extras["max_size"] is not None)

    @classmethod
    def summarize_lifecycle_rule(cls, rule) -> str:
        """One readable line for a rule, for a list the user can scan."""
        rule = dict(rule or {})
        parts = []
        scope = cls.lifecycle_rule_scope(rule)
        parts.append(f"prefix '{scope}'" if scope else "whole bucket")
        extras = cls.lifecycle_rule_extras(rule)
        for key, value in extras["tags"]:
            parts.append(f"tagged {key}={value}" if value else f"tagged {key}")
        if extras["min_size"] is not None:
            parts.append(f"larger than {extras['min_size']} B")
        if extras["max_size"] is not None:
            parts.append(f"smaller than {extras['max_size']} B")
        for transition in rule.get("Transitions") or []:
            days = transition.get("Days")
            target = transition.get("StorageClass") or "?"
            if days is not None:
                parts.append(f"to {target} after {days}d")
            else:
                parts.append(f"to {target} on {transition.get('Date')}")
        expiration = rule.get("Expiration") or {}
        if expiration.get("Days") is not None:
            parts.append(f"delete after {expiration['Days']}d")
        elif expiration.get("Date"):
            parts.append(f"delete on {expiration['Date']}")
        if expiration.get("ExpiredObjectDeleteMarker"):
            parts.append("clean up delete markers")
        noncurrent = rule.get("NoncurrentVersionExpiration") or {}
        if noncurrent.get("NoncurrentDays") is not None:
            parts.append(
                f"delete old versions after {noncurrent['NoncurrentDays']}d")
        for transition in rule.get("NoncurrentVersionTransitions") or []:
            parts.append(
                f"old versions to {transition.get('StorageClass')} after "
                f"{transition.get('NoncurrentDays')}d")
        abort = rule.get("AbortIncompleteMultipartUpload") or {}
        if abort.get("DaysAfterInitiation") is not None:
            parts.append(
                "abort incomplete uploads after "
                f"{abort['DaysAfterInitiation']}d")
        if len(parts) == 1:
            parts.append("does nothing")
        return " · ".join(parts)

    @staticmethod
    def build_lifecycle_rule(rule_id, *, prefix="", enabled=True,
                             transition_days=None, transition_class="",
                             expire_days=None, noncurrent_days=None,
                             abort_days=None, expire_delete_markers=False):
        """
        Assemble a rule document from the fields the editor collects.

        Raises ValueError when the result would do nothing: S3 accepts such a
        rule and then silently never fires it.
        """
        rule = {
            "ID": str(rule_id or "").strip() or "rule",
            "Status": "Enabled" if enabled else "Disabled",
            # The modern shape. A bare "Prefix" is the deprecated one and is
            # rejected outright when mixed with Filter.
            "Filter": {"Prefix": str(prefix or "")},
        }
        if transition_days is not None and transition_class:
            rule["Transitions"] = [{"Days": int(transition_days),
                                    "StorageClass": str(transition_class)}]
        if expire_days is not None:
            rule["Expiration"] = {"Days": int(expire_days)}
        elif expire_delete_markers:
            rule["Expiration"] = {"ExpiredObjectDeleteMarker": True}
        if noncurrent_days is not None:
            rule["NoncurrentVersionExpiration"] = {
                "NoncurrentDays": int(noncurrent_days)}
        if abort_days is not None:
            rule["AbortIncompleteMultipartUpload"] = {
                "DaysAfterInitiation": int(abort_days)}
        if not any(key in rule for key in (
                "Transitions", "Expiration", "NoncurrentVersionExpiration",
                "AbortIncompleteMultipartUpload")):
            raise ValueError(
                "A lifecycle rule needs at least one action, or it will never "
                "do anything.")
        return rule

    def get_bucket_cors(self) -> list:
        """The bucket's CORS rules, or [] when it has none."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        try:
            resp = self.client.get_bucket_cors(Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchCORSConfiguration", "NoSuchCORSConfigurationError"):
                return []
            raise
        return list(resp.get("CORSRules") or [])

    def put_bucket_cors(self, rules, log_fn=None):
        """Replace the CORS document; an empty list removes it."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if not rules:
            self.client.delete_bucket_cors(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed the CORS configuration of {self.bucket}")
            return
        self.client.put_bucket_cors(
            Bucket=self.bucket, CORSConfiguration={"CORSRules": list(rules)})
        if log_fn:
            log_fn(f"wrote {len(rules)} CORS rule(s) to {self.bucket}")

    def get_bucket_policy(self) -> str:
        """The bucket policy as JSON text, or "" when there is none."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        try:
            resp = self.client.get_bucket_policy(Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchBucketPolicy", "NoSuchBucketPolicyError"):
                return ""
            raise
        return str(resp.get("Policy") or "")

    def put_bucket_policy(self, policy, log_fn=None):
        """
        Replace the bucket policy; an empty document removes it.

        The JSON is parsed here rather than at the server: a policy that fails
        to apply because of a typo is a confusing round trip, and a malformed
        one that DID apply would be worse.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        text = str(policy or "").strip()
        if not text:
            self.client.delete_bucket_policy(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed the bucket policy of {self.bucket}")
            return
        try:
            parsed = json.loads(text)
        except ValueError as exc:
            raise ValueError(f"The policy is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict) or "Statement" not in parsed:
            raise ValueError(
                "A bucket policy must be a JSON object with a Statement list.")
        self.client.put_bucket_policy(Bucket=self.bucket, Policy=text)
        if log_fn:
            log_fn(f"wrote a bucket policy to {self.bucket}")

    def get_bucket_policy_status(self) -> str:
        """"public", "not public", or "" when the server will not say."""
        if not self.bucket:
            return ""
        try:
            resp = self.client.get_bucket_policy_status(Bucket=self.bucket)
        except Exception:
            return ""
        status = (resp.get("PolicyStatus") or {}).get("IsPublic")
        if status is None:
            return ""
        return "public" if status else "not public"

    # Codes a backend returns for a bucket document that simply is not set.
    ABSENT_DOCUMENT_CODES = (
        "NoSuchLifecycleConfiguration", "NoSuchCORSConfiguration",
        "NoSuchBucketPolicy", "NoSuchTagSet", "NoSuchTagSetError",
        "NoSuchWebsiteConfiguration", "NoSuchConfiguration",
        "ServerSideEncryptionConfigurationNotFoundError",
        "ObjectLockConfigurationNotFoundError", "NotImplemented",
        "MethodNotAllowed", "InvalidRequest", "UnsupportedOperation",
    )

    def _optional_document(self, call, **kwargs):
        """
        Read a bucket document that may simply not be set.

        Every one of these answers with an error rather than an empty
        document, and on an S3-compatible backend the same call may not exist
        at all — neither is a failure worth showing.
        """
        try:
            return call(Bucket=self.bucket, **kwargs)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in self.ABSENT_DOCUMENT_CODES:
                return None
            raise
        except botocore.exceptions.ParamValidationError:
            return None
        except AttributeError:
            # An older botocore without the operation.
            return None

    def get_bucket_encryption(self) -> dict:
        """
        The bucket's default encryption, or {} when it has none.

        Returns ``{"sse": "AES256"|"aws:kms", "kms_key": str,
        "bucket_key": bool}``.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self._optional_document(self.client.get_bucket_encryption)
        if not resp:
            return {}
        rules = ((resp.get("ServerSideEncryptionConfiguration") or {})
                 .get("Rules") or [])
        for rule in rules:
            default = rule.get("ApplyServerSideEncryptionByDefault") or {}
            algorithm = default.get("SSEAlgorithm")
            if algorithm:
                return {
                    "sse": str(algorithm),
                    "kms_key": str(default.get("KMSMasterKeyID") or ""),
                    "bucket_key": bool(rule.get("BucketKeyEnabled")),
                }
        return {}

    def put_bucket_encryption(self, sse="", kms_key="", bucket_key=False,
                              log_fn=None):
        """Set or remove the bucket's default encryption."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if not sse:
            self.client.delete_bucket_encryption(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed default encryption from {self.bucket}")
            return
        if sse not in self.SSE_MODES or sse == "":
            raise ValueError(f"unsupported encryption mode: {sse}")
        default = {"SSEAlgorithm": sse}
        if sse == "aws:kms" and kms_key:
            default["KMSMasterKeyID"] = kms_key
        rule = {"ApplyServerSideEncryptionByDefault": default}
        if bucket_key:
            rule["BucketKeyEnabled"] = True
        self.client.put_bucket_encryption(
            Bucket=self.bucket,
            ServerSideEncryptionConfiguration={"Rules": [rule]})
        if log_fn:
            log_fn(f"default encryption on {self.bucket}: {sse}")

    def get_bucket_tags(self) -> dict:
        """The bucket's own tags (not an object's), or {}."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self._optional_document(self.client.get_bucket_tagging)
        if not resp:
            return {}
        return {str(tag.get("Key") or ""): str(tag.get("Value") or "")
                for tag in resp.get("TagSet") or [] if tag.get("Key")}

    def put_bucket_tags(self, tags, log_fn=None):
        """Replace the bucket's tag set; an empty mapping removes it."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        tags = {str(k).strip(): str(v).strip()
                for k, v in dict(tags or {}).items() if str(k).strip()}
        if not tags:
            self.client.delete_bucket_tagging(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed the tags from {self.bucket}")
            return
        self.client.put_bucket_tagging(
            Bucket=self.bucket,
            Tagging={"TagSet": [{"Key": k, "Value": v}
                                for k, v in sorted(tags.items())]})
        if log_fn:
            log_fn(f"wrote {len(tags)} tag(s) to {self.bucket}")

    def get_bucket_website(self) -> dict:
        """
        The static-website configuration, or {}.

        Returns ``{"index": str, "error": str, "redirect": str}``.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self._optional_document(self.client.get_bucket_website)
        if not resp:
            return {}
        redirect = resp.get("RedirectAllRequestsTo") or {}
        return {
            "index": str((resp.get("IndexDocument") or {}).get("Suffix") or ""),
            "error": str((resp.get("ErrorDocument") or {}).get("Key") or ""),
            "redirect": str(redirect.get("HostName") or ""),
        }

    def put_bucket_website(self, index="", error="", redirect="", log_fn=None):
        """
        Configure static-website hosting; everything blank removes it.

        A redirect-everything host and an index document are mutually
        exclusive in the API, so the redirect wins when both are given rather
        than sending a document the service rejects.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        index = str(index or "").strip()
        error = str(error or "").strip()
        redirect = str(redirect or "").strip()
        if not index and not redirect:
            self.client.delete_bucket_website(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed website hosting from {self.bucket}")
            return
        if redirect:
            config = {"RedirectAllRequestsTo": {"HostName": redirect}}
        else:
            config = {"IndexDocument": {"Suffix": index}}
            if error:
                config["ErrorDocument"] = {"Key": error}
        self.client.put_bucket_website(
            Bucket=self.bucket, WebsiteConfiguration=config)
        if log_fn:
            log_fn(f"website hosting configured on {self.bucket}")

    def get_bucket_notifications(self) -> list:
        """
        Event notifications as ``[(kind, target, events)]``, or [].

        Read-only on purpose: a notification points at a queue, topic or
        function whose ARN this app has no way to validate, and a wrong one
        silently stops every event.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self._optional_document(
            self.client.get_bucket_notification_configuration)
        if not resp:
            return []
        out = []
        for field, kind, arn_field in (
            ("TopicConfigurations", "SNS topic", "TopicArn"),
            ("QueueConfigurations", "SQS queue", "QueueArn"),
            ("LambdaFunctionConfigurations", "Lambda", "LambdaFunctionArn"),
            ("EventBridgeConfiguration", "EventBridge", ""),
        ):
            node = resp.get(field)
            if not node:
                continue
            if not arn_field:
                out.append((kind, "(enabled)", []))
                continue
            for entry in node:
                out.append((kind, str(entry.get(arn_field) or ""),
                            list(entry.get("Events") or [])))
        return out

    def get_bucket_acceleration(self) -> str:
        """"Enabled", "Suspended", or "" when the bucket has no setting."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        resp = self._optional_document(
            self.client.get_bucket_accelerate_configuration)
        if not resp:
            return ""
        return str(resp.get("Status") or "")

    def set_bucket_acceleration(self, enabled, log_fn=None):
        """Turn transfer acceleration on or off."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        status = "Enabled" if enabled else "Suspended"
        self.client.put_bucket_accelerate_configuration(
            Bucket=self.bucket,
            AccelerateConfiguration={"Status": status})
        if log_fn:
            log_fn(f"transfer acceleration {status.lower()} on {self.bucket}")

    def get_object_lock_configuration(self) -> dict:
        """
        The bucket's Object Lock defaults, or {} when it is not enabled.

        Returns ``{"enabled": bool, "mode": str, "days": int, "years": int}``.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        try:
            resp = self.client.get_object_lock_configuration(
                Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("ObjectLockConfigurationNotFoundError",
                        "ObjectLockConfigurationNotFound",
                        "NotImplemented", "InvalidRequest"):
                return {}
            raise
        config = resp.get("ObjectLockConfiguration") or {}
        rule = (config.get("Rule") or {}).get("DefaultRetention") or {}
        return {
            "enabled": config.get("ObjectLockEnabled") == "Enabled",
            "mode": str(rule.get("Mode") or ""),
            "days": int(rule.get("Days") or 0),
            "years": int(rule.get("Years") or 0),
        }

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

    def get_bucket_hints(self, bucket_name: str):
        """
        Best-effort hints when we fail to enter a bucket:
        - Try HEAD Bucket with both addressing styles to sniff:
          * x-amz-bucket-region from HTTP headers
          * Error.Endpoint (some S3-compatible backends include this)
        Returns: (region_hint:str|None, endpoint_hint:str|None)
        """
        region_hint = None
        endpoint_hint = None

        # try both addressing styles to maximize chances of getting headers back
        for style in [self.use_path, not self.use_path]:
            try:
                c = self._make_client(use_path=style)
                # This may succeed (rare) or throw ClientError (common on perms);
                # both paths can give us headers.
                try:
                    c.head_bucket(Bucket=bucket_name)
                    # If it actually succeeds, prefer the client's region
                    region_hint = region_hint or (
                                c.meta.region_name or self.region_name)
                    break
                except botocore.exceptions.ClientError as e:
                    resp = e.response or {}
                    headers = (resp.get("ResponseMetadata", {}) or {}).get(
                        "HTTPHeaders", {}) or {}
                    # Standard AWS header
                    region_hint = region_hint or headers.get(
                        "x-amz-bucket-region")
                    # Some implementations also stick hints here
                    err = resp.get("Error", {}) or {}
                    endpoint_hint = endpoint_hint or err.get("Endpoint")
                except Exception:
                    # ignore and try next style
                    pass
            except Exception:
                # ignore client construction issues and keep going
                pass

        return region_hint, endpoint_hint

    # A server-side copy fails with these when the destination is not reachable
    # from the source's client (another region, endpoint or account).
    CROSS_LOCATION_CODES = (
        "PermanentRedirect", "AuthorizationHeaderMalformed",
        "InvalidLocationConstraint", "CrossLocationLoggingProhibitted",
        "InvalidRequest", "NoSuchBucket",
    )

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
        extra = self._carried_extra_args(resp, dst_model.upload_extra_args)
        callback = None
        if progress_cb is not None:
            callback = _BotoProgressAdapter(
                resp.get("ContentLength"), dst_key, progress_cb,
                cancel_event=cancel_event, limiter=self.rate_limiter)
        dst_model.client.upload_fileobj(
            body, dst_model.bucket, dst_key,
            ExtraArgs=extra or None,
            Callback=callback,
            Config=dst_model.transfer_cfg_upload,
        )
        if log_fn:
            log_fn(f"copied {self.bucket}/{src_key} -> "
                   f"{dst_model.bucket}/{dst_key}")

    def copy_prefix_to_model(self, src_prefix: str, dst_model, dst_prefix: str,
                             progress_cb=None, cancel_event=None, log_fn=None):
        """Copy a whole prefix into another profile, preserving its shape."""
        dst_model._guard_write()
        source = prefix_of(src_prefix)
        target = prefix_of(dst_prefix) if dst_prefix else ""
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
                     log_fn=None):
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
        extra = self._carried_extra_args(resp, self.upload_extra_args)

        dst_client.upload_fileobj(
            body, dst_bucket, dst_key,
            ExtraArgs=extra or None,
            Config=self.transfer_cfg_upload,
        )
        if log_fn:
            log_fn(f"streamed {src_key} -> {dst_bucket}/{dst_key}")

    def copy_object(self, src_key: str, dst_key: str, dst_bucket: str = None,
                    log_fn=None, allow_stream=True):
        """
        Copy one object, server-side when possible.

        A cross-region or cross-account destination cannot be reached by the
        source's client, so on those errors the bytes are streamed through this
        process instead of failing.
        """
        self._guard_write()
        dst_bucket = dst_bucket or self.bucket
        cross_bucket = dst_bucket != self.bucket
        copy_source = {"Bucket": self.bucket, "Key": src_key}

        def _do():
            self.client.copy_object(CopySource=copy_source, Bucket=dst_bucket, Key=dst_key)

        try:
            _do()
            return
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if (allow_stream and cross_bucket
                    and code in self.CROSS_LOCATION_CODES):
                if log_fn:
                    log_fn(
                        f"Server-side copy refused ({code}); streaming "
                        f"'{src_key}' to bucket '{dst_bucket}' instead"
                    )
                self._stream_copy(src_key, dst_key, dst_bucket, log_fn=log_fn)
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
        def _copy_one(key):
            rel = key[len(src_prefix):]
            dst_key = dst_prefix + rel
            if log_fn:
                log_fn(f"copying {key} -> {dst_key}")
            self.copy_object(key, dst_key, dst_bucket=dst_bucket, log_fn=log_fn)

        keys = [k for k, _ in self.get_keys(src_prefix, log_fn=log_fn) if k]
        run_parallel(keys, _copy_one, self.parallel_files,
                     cancel_event=cancel_event)

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

        def _fetch():
            client = client_obj if client_obj is not None else self.client
            out = []
            paginator = client.get_paginator("list_multipart_uploads")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for up in (page.get("Uploads", []) or []):
                    key = up.get("Key")
                    upload_id = up.get("UploadId")
                    if not key or not upload_id:
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

    @staticmethod
    def _multipart_upload_size(client, bucket, key, upload_id) -> int:
        """Sum the sizes of the parts already uploaded for one multipart upload."""
        total = 0
        paginator = client.get_paginator("list_parts")
        for page in paginator.paginate(Bucket=bucket, Key=key, UploadId=upload_id):
            for part in (page.get("Parts", []) or []):
                total += int(part.get("Size") or 0)
        return total

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

    @staticmethod
    def merge_tags(current, add=None, remove=None, replace=False) -> list:
        """
        Work out the new TagSet for an object.

        current/result are boto3's [{'Key':k,'Value':v}] form. With replace the
        result is exactly *add*; otherwise *add* is merged over the existing
        tags and any key in *remove* is dropped. Insertion order is kept so a
        rewrite does not shuffle unrelated tags.
        """
        add = dict(add or {})
        remove = {str(k) for k in (remove or [])}
        if replace:
            merged = dict(add)
        else:
            merged = {}
            for tag in current or []:
                key = tag.get("Key")
                if key is None:
                    continue
                merged[str(key)] = str(tag.get("Value") or "")
            merged.update({str(k): str(v) for k, v in add.items()})
            for key in remove:
                merged.pop(key, None)
        return [{"Key": k, "Value": v} for k, v in merged.items()]

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

    # Storage classes selectable in the UI. The first is the default "hot"
    # tier; the GLACIER/DEEP_ARCHIVE tiers require a restore before download.
    STORAGE_CLASSES = (
        "STANDARD",
        "STANDARD_IA",
        "ONEZONE_IA",
        "INTELLIGENT_TIERING",
        "GLACIER_IR",
        "GLACIER",
        "DEEP_ARCHIVE",
        "REDUCED_REDUNDANCY",
    )

    # Storage classes whose objects must be restored before they can be read.
    ARCHIVE_STORAGE_CLASSES = ("GLACIER", "DEEP_ARCHIVE")

    # Indicative us-east-1 list price in USD per GiB-month, storage only. A
    # bundled table can never be a bill: prices change, every region differs,
    # and the real invoice also carries requests, retrieval and transfer. It
    # is enough to answer "is this bucket costing me pennies or hundreds",
    # which is the question a size figure alone cannot.
    STORAGE_PRICES_USD = {
        "STANDARD": 0.023,
        "REDUCED_REDUNDANCY": 0.024,
        "INTELLIGENT_TIERING": 0.023,
        "STANDARD_IA": 0.0125,
        "ONEZONE_IA": 0.01,
        "GLACIER_IR": 0.004,
        "GLACIER": 0.0036,
        "DEEP_ARCHIVE": 0.00099,
    }
    # Classes worth suggesting for bytes that are sitting in STANDARD, with
    # the catch that makes each one a real decision rather than free money.
    COLDER_CLASSES = (
        ("STANDARD_IA", "30-day minimum, per-GB retrieval fee"),
        ("GLACIER_IR", "90-day minimum, per-GB retrieval fee"),
        ("DEEP_ARCHIVE", "180-day minimum, restore takes hours"),
    )
    PRICING_BASE = 1024 ** 3

    def is_aws_endpoint(self) -> bool:
        """
        Whether this profile talks to AWS itself.

        The bundled prices are AWS list prices, so quoting them at a MinIO or
        Ceph endpoint would be inventing a number.
        """
        try:
            host = (urlparse(self.endpoint_url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("amazonaws.com")

    @classmethod
    def estimate_storage_cost(cls, by_class) -> dict:
        """
        Monthly storage cost for a ``{storage_class: bytes}`` breakdown.

        Returns ``{"total": usd, "by_class": {cls: usd}, "unpriced": [cls]}``.
        A class with no price is reported rather than silently costed at zero.
        """
        priced = {}
        unpriced = []
        total = 0.0
        for name, size in (by_class or {}).items():
            key = str(name or "STANDARD").upper()
            price = cls.STORAGE_PRICES_USD.get(key)
            if price is None:
                unpriced.append(key)
                continue
            cost = (int(size or 0) / cls.PRICING_BASE) * price
            priced[key] = cost
            total += cost
        return {"total": total, "by_class": priced,
                "unpriced": sorted(set(unpriced))}

    @classmethod
    def colder_class_options(cls, by_class) -> list:
        """
        What the STANDARD bytes would cost in a colder class.

        Returns ``[(class, monthly_usd, monthly_saving, caveat)]``, empty when
        there is nothing in STANDARD to move.
        """
        hot = int((by_class or {}).get("STANDARD", 0) or 0)
        if hot <= 0:
            return []
        gib = hot / cls.PRICING_BASE
        current = gib * cls.STORAGE_PRICES_USD["STANDARD"]
        options = []
        for name, caveat in cls.COLDER_CLASSES:
            cost = gib * cls.STORAGE_PRICES_USD[name]
            options.append((name, cost, current - cost, caveat))
        return options

    @staticmethod
    def parse_restore_status(restore_header: str) -> str:
        """
        Turn the raw x-amz-restore header into a short human status.
          None / ""                                  -> ""  (not restored/archived)
          'ongoing-request="true"'                   -> "in-progress"
          'ongoing-request="false", expiry-date=...' -> "available until <date>"
        """
        if not restore_header:
            return ""
        if 'ongoing-request="true"' in restore_header:
            return "in-progress"
        if 'ongoing-request="false"' in restore_header:
            marker = 'expiry-date="'
            idx = restore_header.find(marker)
            if idx != -1:
                rest = restore_header[idx + len(marker):]
                end = rest.find('"')
                if end != -1:
                    return f"available until {rest[:end]}"
            return "available"
        return restore_header

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

    def change_storage_class(self, key: str, storage_class: str, log_fn=None):
        """Change an object's storage class via a server-side copy onto itself."""
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        copy_source = {"Bucket": self.bucket, "Key": key}

        def _do():
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=key,
                StorageClass=storage_class,
                MetadataDirective="COPY",
            )

        try:
            _do()
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error changing storage class of '{key}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            _do()

    def get_object_metadata(self, key: str) -> dict:
        """
        Return editable metadata for an object:
          {content_type, cache_control, content_disposition, content_encoding,
           storage_class, metadata (user x-amz-meta-* dict)}
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
        }

    def set_object_metadata(self, key: str, *, content_type=None,
                            cache_control=None, content_disposition=None,
                            content_encoding=None, metadata=None,
                            storage_class=None, log_fn=None):
        """
        Replace an object's system + user metadata via a server-side copy onto
        itself with MetadataDirective=REPLACE. The current storage class is
        preserved when passed in (a REPLACE copy would otherwise reset it to
        STANDARD).
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

        def _do():
            self.client.copy_object(**params)

        try:
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
            log_fn=log_fn,
        )
        return True, guessed

    @staticmethod
    def build_search_matcher(query: str = "", *, regex: bool = False,
                            case_sensitive: bool = False,
                            min_size=None, max_size=None,
                            modified_after=None, modified_before=None,
                            extensions=None):
        """
        Compile the search filters once into ``match(key, size, modified)``.

        Raises ValueError for an invalid regular expression so the UI can
        report it instead of failing mid-listing.
        """
        needle = query or ""
        compiled = None
        if regex and needle:
            try:
                compiled = re.compile(
                    needle, 0 if case_sensitive else re.IGNORECASE)
            except re.error as exc:
                raise ValueError(f"Invalid regular expression: {exc}") from exc
        elif not case_sensitive:
            needle = needle.lower()

        wanted_exts = None
        if extensions:
            wanted_exts = set()
            for raw in extensions:
                ext = (raw or "").strip().lower()
                if not ext:
                    continue
                wanted_exts.add(ext if ext.startswith(".") else "." + ext)
            if not wanted_exts:
                wanted_exts = None

        after = as_epoch(modified_after) if modified_after is not None else None
        before = as_epoch(modified_before) if modified_before is not None else None

        def _match(key, size, modified):
            if compiled is not None:
                if not compiled.search(key):
                    return False
            elif needle:
                haystack = key if case_sensitive else key.lower()
                if needle not in haystack:
                    return False
            if min_size is not None and int(size or 0) < int(min_size):
                return False
            if max_size is not None and int(size or 0) > int(max_size):
                return False
            if wanted_exts is not None:
                if os.path.splitext(key)[1].lower() not in wanted_exts:
                    return False
            if after is not None or before is not None:
                stamp = as_epoch(modified)
                if after is not None and stamp < after:
                    return False
                if before is not None and stamp > before:
                    return False
            return True

        return _match

    def search_keys(self, prefix: str, query: str, cancel_event=None,
                    max_results: int = 1000, log_fn=None, **filters) -> list:
        """
        Recursively list objects under 'prefix' and return
        [(key, size, last_modified), ...] matching 'query' and any extra
        filters (see build_search_matcher). S3 only filters by prefix, so
        matching happens client-side over the full listing. Stops early at
        max_results (0/None means unlimited).
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        matches = self.build_search_matcher(query, **filters)

        def _fetch():
            res = []
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix or ""):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key") or ""
                    if not key or key.endswith("/"):
                        continue
                    size = int(obj.get("Size") or 0)
                    modified = obj.get("LastModified")
                    if not matches(key, size, modified):
                        continue
                    res.append((key, size, modified))
                    if max_results and len(res) >= int(max_results):
                        return res
            return res

        try:
            return _fetch()
        except TransferCancelled:
            raise
        except Exception as exc:
            if not self._is_region_error(exc):
                raise
            if log_fn:
                log_fn(f"Region error searching '{prefix}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return _fetch()

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
        for name in CHECKSUM_ALGORITHMS:
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
            for name in CHECKSUM_ALGORITHMS:
                if part.get(f"Checksum{name}"):
                    digest = f"{name}:{part[f'Checksum{name}']}"
                    break
            if not digest:
                return "", ""
            parts.append(f"{int(part.get('Size') or 0)}:{digest}")
        if parts and not node.get("IsTruncated"):
            return "parts", "|".join(parts)
        return "", ""

    @staticmethod
    def compare_fingerprints(fingerprints) -> str:
        """
        "same", "different" or "unknown" for a group of ``(kind, value)``.

        Only a whole-object digest can prove two objects differ; per-part
        digests can prove they match but never that they do not.
        """
        entries = list(fingerprints or [])
        if len(entries) < 2 or any(not kind for kind, _v in entries):
            return "unknown"
        values = {value for _kind, value in entries}
        if len(values) == 1:
            return "same"
        if all(kind == "full" for kind, _v in entries):
            return "different"
        return "unknown"

    def list_tree(self, prefix: str = "", cancel_event=None, log_fn=None) -> dict:
        """
        Map every object under 'prefix' to ``{relative_path: (size, mtime)}``
        for sync comparison. Folder placeholders are skipped; mtime is epoch
        seconds.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        base = prefix or ""

        def _fetch():
            out = {}
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=base):
                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key") or ""
                    if not key or key.endswith("/"):
                        continue
                    rel = key[len(base):] if key.startswith(base) else key
                    if not rel:
                        continue
                    out[rel] = (
                        int(obj.get("Size") or 0),
                        as_epoch(obj.get("LastModified")),
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
                log_fn(f"Region error listing tree '{base}': {exc}")
            self.rebind_bucket(log_fn=log_fn)
            return _fetch()

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

    # Codes an S3-compatible backend returns for a conditional PUT it does
    # not implement. On these the write is retried unconditionally, after the
    # ETag has been re-checked by hand.
    PRECONDITION_UNSUPPORTED_CODES = (
        "NotImplemented", "InvalidRequest", "InvalidArgument",
        "BadRequest", "MethodNotAllowed",
    )

    class PreconditionFailed(Exception):
        """The object changed since it was read, so the write was refused."""

    def put_object_body(self, key: str, data: bytes, *, if_match="",
                        content_type="", log_fn=None) -> str:
        """
        Replace an object's body, refusing when it changed since it was read.

        Returns the new ETag. The precondition is what makes in-app editing
        safe; a backend without conditional writes falls back to comparing the
        ETag first, which is racy but still catches the case that happens in
        practice — someone saved while the editor was open.
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        expected = self.normalize_etag(if_match)
        params = dict(self.upload_extra_args)
        params.pop("ChecksumAlgorithm", None)
        params.pop("ChecksumType", None)
        guessed = content_type or self.guess_content_type(key)
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

        try:
            resp = _put(bool(expected))
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

    def build_region_swapped_endpoint(self, base_endpoint: str,
                                      new_region: str) -> str:
        """
        Best-effort rewrite of an AWS-style endpoint to another region.
        Examples:
          https://s3.eu-central-1.amazonaws.com   -> https://s3.eu-north-1.amazonaws.com
          http://s3.us-west-2.amazonaws.com       -> http://s3.eu-north-1.amazonaws.com
          https://s3.amazonaws.com (no region)    -> https://s3.eu-north-1.amazonaws.com
        If base_endpoint doesn't look AWS-ish, returns None to signal "don't touch".
        """
        try:
            parsed = urlparse(base_endpoint)
            scheme = parsed.scheme or "https"
            host = (parsed.hostname or "").lower()
            if not host:
                return None

            # only handle AWS classic patterns
            # s3.<region>.amazonaws.com OR s3.amazonaws.com
            if host == "s3.amazonaws.com":
                new_host = f"s3.{new_region}.amazonaws.com"
            elif host.startswith("s3.") and host.endswith(".amazonaws.com"):
                # s3.<something>.amazonaws.com -> replace the middle with new_region
                parts = host.split(".")
                # parts: ["s3", "<region>", "amazonaws", "com"] or longer for china/gov (not covered fully)
                if len(parts) >= 4 and parts[0] == "s3" and parts[-2:] == [
                    "amazonaws", "com"]:
                    parts[1] = new_region
                    new_host = ".".join(parts)
                else:
                    return None
            else:
                return None

            # preserve port if any
            netloc = new_host
            if parsed.port:
                netloc = f"{new_host}:{parsed.port}"

            # keep path/query/fragment as-is (normally empty for endpoints)
            return f"{scheme}://{netloc}{parsed.path or ''}"
        except Exception:
            return None
