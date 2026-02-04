from enum import Enum
import platform
import uuid
import os
import boto3
import botocore
import threading
from urllib.parse import urlparse

from boto3.s3.transfer import TransferConfig


class TransferCancelled(Exception):
    pass


class FSObjectType(Enum):
    FILE = 1
    FOLDER = 2
    BUCKET = 3  # top-level S3 bucket


class Item:
    def __init__(self, name, type_, modified, size):
        self.name = name
        self.type_ = type_
        self.modified = modified
        self.size = size

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


class _BotoProgressAdapter:
    """
    Adapt boto3 Callback(bytes_amount) -> progress_cb(total, current, key).
    Also supports cooperative cancellation.
    """
    def __init__(self, total, key, cb, cancel_event=None):
        self.total = max(1, int(total or 0))
        self.key = key
        self.cb = cb
        self.cancel_event = cancel_event
        self._sofar = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # If Cancel, abort transfer ASAP.
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise TransferCancelled("cancelled")

        if self.cb is None:
            return

        inc = int(bytes_amount or 0)
        with self._lock:
            self._sofar += inc
            cur = self._sofar

        if cur > self.total:
            cur = self.total

        self.cb(self.total, cur, self.key)


class Model:
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
    ):
        self.session = boto3.session.Session()
        self._client = None

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
        self.bucket = bucket or ""  # may be empty (bucket list mode)
        self.no_ssl_check = no_ssl_check
        self.use_path = use_path  # True -> path-style, False -> virtual-host style
        self.timeout = timeout
        self.retries = retries
        self.read_timeout = read_timeout

        self.transfer_cfg = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,   # 8MB
            multipart_chunksize=1 * 1024 * 1024,   # 1MB parts
            io_chunksize=256 * 1024,               # 256KB read size
            max_concurrency=2,
            use_threads=True,
        )

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
        if region:
            params.update({"region_name": region})

        s3_config = (
            {"addressing_style": "virtual"}
            if not use_path
            else {"addressing_style": "path"}
        )
        if self.no_ssl_check:
            params.update({"verify": False})

        params.update(
            {
                "config": botocore.config.Config(
                    s3=s3_config,
                    connect_timeout=self.timeout,
                    read_timeout=self.read_timeout,
                    retries={
                        "max_attempts": self.retries,
                        "mode": "standard",
                    },
                ),
            }
        )
        return self.session.client("s3", **params)

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

    def _try_bind_bucket(self, bucket_name: str):
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

            # If the hinted endpoint looks bucket-bound but to a DIFFERENT bucket, error out explicitly.
            leftmost = self._extract_leftmost_label(endpoint_fixed)
            if leftmost and leftmost != bucket_name.lower() and self._endpoint_has_bucket(endpoint_fixed, leftmost):
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
        params = {"Bucket": bucket_name}

        region = self.profile_region or "us-east-1"
        if region and region != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": region
            }

        root_client = self._make_client(region=region)
        root_client.create_bucket(**params)

    def delete_bucket(self, bucket_name: str):
        """
        Delete a bucket. Must be empty.

        We'll check emptiness with the current (possibly bucket-tuned) client,
        then actually call DeleteBucket using a client bound to profile_region.
        """
        # emptiness check
        paginator = self.client.get_paginator("list_objects_v2")
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

        root_client = self._make_client(region=self.profile_region or "us-east-1")
        root_client.delete_bucket(Bucket=bucket_name)

    def _list_bucket_once(self, client_obj, bucket_name, prefix):
        """
        Try to list 'prefix' in 'bucket_name' using client_obj once.

        Returns tuple: (ok, items, expecting_region, fatal_exc)

        ok=True  -> listing succeeded
        items    -> list[Item] if ok
        expecting_region -> str or None; if not None, server hinted "use this region instead"
        fatal_exc -> Exception or None if this attempt should be considered a hard failure
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
                return False, [], expecting_region, None

            # Access/NoSuchKey/etc: treat as **fatal** now (surface real boto3 message)
            if err_code in ("NoSuchKey", "AccessDenied", "AllAccessDisabled"):
                return False, [], None, exc

            # other real failure
            return False, [], None, exc

        except Exception as exc:
            # unknown non-ClientError; treat as fatal
            return False, [], None, exc

        # We got a paginator successfully. Now accumulate.
        items = []
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
                        )
                    )
        except botocore.exceptions.ClientError as exc:
            err_code = exc.response.get("Error", {}).get("Code", "")
            if err_code in ("NoSuchKey", "AccessDenied", "AllAccessDisabled"):
                return False, [], None, exc
            return False, [], None, exc
        except Exception as exc:
            return False, [], None, exc

        return True, items, None, None

    def list(self, fld):
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

        # 1. Try with the current live client
        ok, items, expecting_region, fatal_exc = self._list_bucket_once(
            self.client, self.bucket, prefix
        )
        if ok:
            return items
        if fatal_exc:
            # true fatal -> raise original error with details
            raise fatal_exc

        # 2. If server told us a better region, try that region
        if expecting_region:
            tmp_client = self._make_client(region=expecting_region)
            ok2, items2, expecting_region2, fatal_exc2 = self._list_bucket_once(
                tmp_client, self.bucket, prefix
            )
            if ok2:
                # success in new region -> adopt it permanently for this bucket
                self.region_name = expecting_region
                self._client = tmp_client
                return items2
            if fatal_exc2:
                raise fatal_exc2

        # 3. Nothing worked
        raise Exception(
            f"Cannot list bucket '{self.bucket}' at prefix '{prefix}' with available regions."
        )

    def download_file(self, key: str, local_name: str, folder_path: str,
                      progress_cb=None, cancel_event=None):
        """
        Download a single file or a whole prefix.
        - If local_name is truthy: single object -> local_name
        - Else: prefix -> recreate directory tree under folder_path/<basename(prefix)>/
        Supports cancellation via cancel_event (threading.Event).
        """

        # folder/prefix download mode
        if not local_name:
            prefix = key if str(key).endswith("/") else (str(key) + "/")
            base_name = os.path.basename(prefix.rstrip("/"))
            base_dir = os.path.join(folder_path, base_name)
            os.makedirs(base_dir, exist_ok=True)

            for k, size in self.get_keys(prefix):
                if not k:
                    continue

                if cancel_event is not None and cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                # keep your original relative layout
                rel = os.path.relpath(k, prefix)
                if rel == ".":
                    continue

                out_path = os.path.join(base_dir, rel)

                if k.endswith("/"):
                    os.makedirs(out_path, exist_ok=True)
                    continue

                os.makedirs(os.path.dirname(out_path), exist_ok=True)

                self.client.download_file(
                    self.bucket,
                    k,
                    out_path,
                    Callback=_BotoProgressAdapter(size, k, progress_cb,
                                                  cancel_event=cancel_event),
                    Config=self.transfer_cfg,
                )
            return

        # single object download mode
        size = None
        try:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            head = self.client.head_object(Bucket=self.bucket, Key=key)
            size = head.get("ContentLength")
        except TransferCancelled:
            raise
        except Exception:
            pass

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        self.client.download_file(
            self.bucket,
            key,
            local_name,
            Callback=_BotoProgressAdapter(size, key, progress_cb,
                                          cancel_event=cancel_event),
            Config=self.transfer_cfg,
        )

    def create_folder(self, key):
        return self.client.put_object(Bucket=self.bucket, Key=key)

    def get_keys(self, prefix):
        """
        Return [(Key, Size), ...] for ALL objects under 'prefix', paginated.
        Includes 'folder placeholder' keys (ending with '/').
        """
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        result = []
        for page in pages:
            for obj in page.get("Contents", []) or []:
                result.append((obj.get("Key"), obj.get("Size")))
        return result

    def delete(self, key) -> bool:
        if key.endswith("/"):
            for k, _ in self.get_keys(key):
                self.client.delete_object(Bucket=self.bucket, Key=k)
        else:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        return True

    def upload_file(self, local_file, key, progress_cb=None,
                    cancel_event=None):
        """
        Upload a file (with progress) or create a folder placeholder if local_file is None.
        """
        if local_file is None:
            self.create_folder("%s/" % key)
            return

        if cancel_event is not None and cancel_event.is_set():
            raise TransferCancelled("cancelled")

        try:
            total = os.path.getsize(local_file)
        except Exception:
            total = None

        self.client.upload_file(
            local_file,
            self.bucket,
            key,
            Callback=_BotoProgressAdapter(total, key, progress_cb,
                                          cancel_event=cancel_event),
            Config=self.transfer_cfg,
        )

    def check_bucket(self):
        """
        Validate that self.bucket currently exists.
        """
        try:
            res = self.client.list_buckets()
            for b in res.get("Buckets", []):
                if b["Name"] == self.bucket:
                    return True, None
            reason = "bucket not found"
        except botocore.exceptions.ClientError as exc:
            reason = exc.response["Error"]["Message"]
        except Exception as exc:
            reason = str(exc)
        return False, reason

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
        """Return a temporary download URL for an object."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        return self.client.generate_presigned_url(
            "get_object",
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
        """Construct a direct (unsigned) URL for an object (path-style)."""
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        ep = self.endpoint_url.rstrip("/")
        return f"{ep}/{self.bucket}/{key}"

    def get_size(self, key):
        total = 0
        for k, s in self.get_keys(key):
            if not k.endswith("/"):
                total += int(s or 0)
        return total

    def bucket_properties(self):
        """
        Return lightweight info about the current bucket for Properties
        when no concrete key is selected.
        """
        return {
            "Bucket": self.bucket,
            "SizeBytes": None,
            "ETag": None,
            "Key": "",
            "IsBucketRoot": True,
        }

    def object_properties(self, key: str):
        """
        Return object metadata without opening a body stream.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty")

        return self.client.head_object(Bucket=self.bucket, Key=key)

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
