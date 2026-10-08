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
import botocore.session
import threading
import types
from urllib.parse import quote, urlencode, urlparse

from boto3.s3.transfer import TransferConfig

from core import lifecycle as _lifecycle
from core import pricing as _pricing
from core.checksums import _CrcDigest, _HashDigest
from core.errors import (  # noqa: F401  (re-exported: model is the front door)
    DeleteRefused, ObjectExists, ReadOnlyError, TransferCancelled,
)
from core.naming import as_epoch  # noqa: F401  (re-exported)
from s3.common import (  # noqa: F401  (re-exported: model is the front door)
    BUILTIN_CONTENT_TYPES, CHECKSUM_ALGORITHMS, CHECKSUM_DIGESTS,
    FALLBACK_CONTENT_TYPE, FULL_OBJECT_CHECKSUMS, KNOWN_CHECKSUM_ALGORITHMS,
    MAX_LOCAL_NAME_BYTES, RateLimiter, _BotoProgressAdapter, _ProgressReader,
    client_accepts, client_knows_checksum, drop_unsendable_checksum_args,
    installed_sdk_accepts, local_name_is_writable, plan_prefix_download,
    prefix_of, run_parallel, safe_local_path,
)
from s3.binding import BindingMixin
from s3.bucket_admin import BucketAdminMixin
from s3.copy import CopyMixin
from s3.delete import DeleteMixin
from s3.download import DownloadMixin
from s3.listing import ListingMixin
from s3.object_admin import ObjectAdminMixin
from s3.upload import UploadMixin
from core.entries import (  # noqa: F401  (re-exported)
    FSObjectType, Item, PrefixDownloadPlan,
)
from core import endpoints as _endpoints
from core import integrity as _integrity
from core import objects as _objects
from core import parts as _parts
from core import search as _search
from core import upload_rules as _upload_rules


# Checksums S3 can store, and how to recompute one here. An algorithm we
# cannot recompute locally is worse than none at all, because verification
# would then pass every download unchecked -- so what the UI offers is built
# from what this machine can actually compute, not from what S3 accepts.
#
# CRC32, SHA1 and SHA256 come from the standard library. CRC32C and CRC64NVME
# need awscrt, which is also the module botocore itself computes them with, so
# the two halves -- sending the checksum and verifying it afterwards -- appear
# and disappear together rather than one silently outliving the other.
try:
    from awscrt import checksums as _crt_checksums
except Exception:  # pragma: no cover - depends on what is installed
    _crt_checksums = None

# Which algorithms the installed botocore can actually send. Asking it rather
# than assuming keeps one out of the UI on an SDK that would reject the
# parameter outright. The name is private, hence the guard and the fallback
# to the set every supported botocore has had.
try:
    from botocore.httpchecksum import (
        _SUPPORTED_CHECKSUM_ALGORITHMS as _BOTOCORE_CHECKSUM_NAMES)
except Exception:  # pragma: no cover - private name, guarded on purpose
    _BOTOCORE_CHECKSUM_NAMES = ("crc32", "sha1", "sha256")


class Model(
    BindingMixin,
    ListingMixin,
    UploadMixin,
    DownloadMixin,
    CopyMixin,
    DeleteMixin,
    BucketAdminMixin,
    ObjectAdminMixin,
):
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
    # DeleteObjectTagging is the odd one out among the object calls: it takes
    # Bucket, Key, VersionId and ExpectedBucketOwner and nothing else.
    REQUESTER_PAYS_OPERATIONS = (
        "GetObject", "HeadObject", "PutObject", "DeleteObject",
        "DeleteObjects", "CopyObject", "ListObjects", "ListObjectsV2",
        "ListObjectVersions", "ListMultipartUploads", "ListParts",
        "CreateMultipartUpload", "UploadPart", "UploadPartCopy",
        "CompleteMultipartUpload", "AbortMultipartUpload", "RestoreObject",
        "GetObjectTagging", "PutObjectTagging",
        "GetObjectAcl", "PutObjectAcl", "GetObjectAttributes",
    )

    DEFAULT_MULTIPART_THRESHOLD_MB = 16
    DEFAULT_MULTIPART_CHUNKSIZE_MB = 8
    MIN_MULTIPART_CHUNKSIZE_MB = 5      # S3's own floor for a non-final part
    MAX_MULTIPART_CHUNKSIZE_MB = 512
    PART_SUFFIX = ".s3duckpart"

    # CopyObject refuses a source bigger than this. Past it the only
    # server-side route is a multipart upload whose parts are UploadPartCopy
    # ranges of the source — still server-side, still no bytes through this
    # process, but a different call that has to be made deliberately.
    MAX_SINGLE_COPY_BYTES = 5 * 1024 * 1024 * 1024

    # What a by-hand multipart upload names when the user configured no
    # checksum of their own. It is not a preference: botocore 1.36 and newer
    # put exactly this on every UploadPart whether we ask or not, so saying
    # it out loud on CreateMultipartUpload is what keeps the two agreeing.
    DEFAULT_PART_CHECKSUM = "CRC32"

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










    @staticmethod
    def get_os_family():
        return platform.system()




























    DELETE_BATCH_SIZE = 1000







































    # Codes a backend returns for a bucket document that simply is not set.
    ABSENT_DOCUMENT_CODES = (
        "NoSuchLifecycleConfiguration", "NoSuchCORSConfiguration",
        "NoSuchBucketPolicy", "NoSuchTagSet", "NoSuchTagSetError",
        "NoSuchWebsiteConfiguration", "NoSuchConfiguration",
        "ServerSideEncryptionConfigurationNotFoundError",
        "ObjectLockConfigurationNotFoundError", "NotImplemented",
        "MethodNotAllowed", "InvalidRequest", "UnsupportedOperation",
    )















    # A server-side copy fails with these when the destination is not reachable
    # from the source's client (another region, endpoint or account).
    CROSS_LOCATION_CODES = (
        "PermanentRedirect", "AuthorizationHeaderMalformed",
        "InvalidLocationConstraint", "CrossLocationLoggingProhibitted",
        "InvalidRequest", "NoSuchBucket",
    )





    # Codes a service answers when CopyObject is refused for the source's
    # size. None of them is distinctive — AWS says InvalidRequest, which is
    # also what a copy onto itself that changes nothing returns — so the
    # source gets measured rather than the message parsed.
    COPY_TOO_LARGE_CODES = (
        "InvalidRequest", "EntityTooLarge", "InvalidArgument",
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

    # Priced and summarised in core/, which needs neither a client nor a
    # profile to do it. Bound here so Model stays the one front door: every
    # Model.f(), self.f() and model.f() call site is unchanged.
    STORAGE_PRICES_USD = _pricing.STORAGE_PRICES_USD
    COLDER_CLASSES = _pricing.COLDER_CLASSES
    PRICING_BASE = _pricing.PRICING_BASE
    estimate_storage_cost = staticmethod(_pricing.estimate_storage_cost)
    colder_class_options = staticmethod(_pricing.colder_class_options)

    lifecycle_rule_scope = staticmethod(_lifecycle.lifecycle_rule_scope)
    lifecycle_rule_extras = staticmethod(_lifecycle.lifecycle_rule_extras)
    lifecycle_filter_is_simple = staticmethod(
        _lifecycle.lifecycle_filter_is_simple)
    summarize_lifecycle_rule = staticmethod(
        _lifecycle.summarize_lifecycle_rule)
    build_lifecycle_rule = staticmethod(_lifecycle.build_lifecycle_rule)

    normalize_etag = staticmethod(_integrity.normalize_etag)
    etag_is_md5 = staticmethod(_integrity.etag_is_md5)
    composite_etag = staticmethod(_integrity.composite_etag)
    checksum_is_composite = staticmethod(_integrity.checksum_is_composite)
    compare_fingerprints = staticmethod(_integrity.compare_fingerprints)
    file_md5 = staticmethod(_integrity.file_md5)

    parse_upload_rules = staticmethod(_upload_rules.parse_upload_rules)
    format_upload_rules = staticmethod(_upload_rules.format_upload_rules)
    parse_content_type_overrides = staticmethod(
        _upload_rules.parse_content_type_overrides)
    format_content_type_overrides = staticmethod(
        _upload_rules.format_content_type_overrides)

    build_region_swapped_endpoint = staticmethod(
        _endpoints.build_region_swapped_endpoint)
    _endpoint_has_bucket = staticmethod(_endpoints._endpoint_has_bucket)
    _extract_leftmost_label = staticmethod(_endpoints._extract_leftmost_label)
    _redirect_conflicts_with_bucket = staticmethod(
        _endpoints._redirect_conflicts_with_bucket)

    MIN_COPY_PART_BYTES = _parts.MIN_COPY_PART_BYTES
    MAX_MULTIPART_PARTS = _parts.MAX_MULTIPART_PARTS
    copy_part_size = staticmethod(_parts.copy_part_size)
    _multipart_upload_size = staticmethod(_parts._multipart_upload_size)

    parse_restore_status = staticmethod(_objects.parse_restore_status)
    merge_tags = staticmethod(_objects.merge_tags)
    _object_already_there = staticmethod(_objects._object_already_there)
    raise_on_delete_errors = staticmethod(_objects.raise_on_delete_errors)

    build_search_matcher = staticmethod(_search.build_search_matcher)













    # Codes an S3-compatible backend returns for a conditional PUT it does
    # not implement. On these the write is retried unconditionally, after the
    # ETag has been re-checked by hand.
    PRECONDITION_UNSUPPORTED_CODES = (
        "NotImplemented", "InvalidRequest", "InvalidArgument",
        "BadRequest", "MethodNotAllowed",
    )

    class PreconditionFailed(Exception):
        """The object changed since it was read, so the write was refused."""


