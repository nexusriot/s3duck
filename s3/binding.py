"""Getting a working client: addressing style, region, and re-binding.

A provider that answers path-style and not virtual-host, or that redirects
to the bucket's real region, is normal rather than exceptional, so binding a
bucket is a probe rather than a constructor argument.
"""

import boto3
import botocore.config
import botocore.exceptions
import os
import threading
import uuid
from boto3.s3.transfer import TransferConfig
from urllib.parse import urlparse


class BindingMixin:
    """Getting a working client: addressing style, region, and re-binding.

A provider that answers path-style and not virtual-host, or that redirects
to the bucket's real region, is normal rather than exceptional, so binding a
bucket is a probe rather than a constructor argument.
    """

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
            # Pinned, not left to the resolver. Against a custom endpoint
            # botocore presigns with SIGNATURE V2 unless told otherwise —
            # even though the client reports "s3v4" — and a SigV2 presigned
            # PUT is refused outright by MinIO (403 SignatureDoesNotMatch)
            # and by every AWS region launched after 2014. The upload-link
            # feature was therefore dead anywhere but a legacy region.
            "signature_version": "s3v4",
            "connect_timeout": self.timeout,
            "read_timeout": self.read_timeout,
            "retries": {"max_attempts": self.retries, "mode": "standard"},
        }
        proxies = self.proxy_settings()
        if proxies:
            config_args["proxies"] = proxies
        params.update({"config": botocore.config.Config(**config_args)})
        client = self.session.client("s3", **params)
        self._register_ranged_checksum_guard(client)
        if self.requester_pays:
            self._register_requester_pays(client)
        return client

    def _register_ranged_checksum_guard(self, client):
        # before-call, because botocore resolves the checksum context just
        # before emitting it — the one point where the decision is made and
        # the request headers are both visible.
        client.meta.events.register(
            "before-call.s3.GetObject", self._skip_range_response_checksum)
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

    @staticmethod
    def _list_one_page(client_obj, bucket_name: str) -> bool:
        """List a single page of *bucket_name*, letting the failure out.

        The cheapest proof that a client can read a bucket. _can_list_bucket
        wants the yes/no; check_profile wants the reason, so the raising form
        is the one that is written down.
        """
        paginator = client_obj.get_paginator("list_objects_v2")
        for _page in paginator.paginate(
                Bucket=bucket_name,
                Prefix="",
                Delimiter="/",
                PaginationConfig={"MaxItems": 1},
        ):
            break
        return True

    def _can_list_bucket(self, client_obj, bucket_name: str) -> bool:
        """True if this client can list a single page of *bucket_name*."""
        try:
            return self._list_one_page(client_obj, bucket_name)
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

    def check_profile(self):
        """
        Profile check.
        - If bucket is empty: verify credentials by listing buckets.
        - If the profile is read-only: verify the bucket can be listed.
        - Otherwise: verify write/delete by creating a temp folder key.

        A read-only profile gets the read check and not the write one. The
        write probe goes through _guard_write, so asking a read-only profile
        to prove it can write reported every correctly-configured one as
        "Check failed: This profile is read-only" — the check contradicting
        the setting the user had just chosen.
        """
        if not self.bucket:
            try:
                self.list_buckets()
                return True, None
            except Exception as exc:
                return False, str(exc)

        if self.read_only:
            try:
                self._list_one_page(self.client, self.bucket)
                return True, None
            except botocore.exceptions.ClientError as exc:
                return False, exc.response["Error"]["Message"]
            except Exception as exc:
                return False, str(exc)

        created = removed = False
        reason = None
        key = str(uuid.uuid4())
        try:
            self.create_folder(key)
        except botocore.exceptions.ClientError as exc:
            reason = exc.response["Error"]["Message"]
        except Exception as exc:
            reason = str(exc)
        else:
            # Not having raised is the success: the old test was whether
            # PutObject's RESPONSE was truthy, which is a property of the
            # backend's reply rather than of the write. A service answering
            # a bare body would have failed a profile that works.
            created = True
            # Only once the probe key exists is there anything to clean up,
            # and the cleanup must not overwrite the reason the create gave:
            # a delete that fails for its own reason used to be the only
            # failure reported.
            try:
                self.delete(key)
            except botocore.exceptions.ClientError as exc:
                reason = exc.response["Error"]["Message"]
            except Exception as exc:
                reason = str(exc)
            else:
                removed = True
        return created and removed, reason

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
