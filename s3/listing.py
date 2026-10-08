"""Reading what is in a bucket or under a prefix.
"""

import botocore.session
from core.entries import FSObjectType, Item
from core.errors import TransferCancelled
from core.naming import as_epoch


class ListingMixin:
    """Reading what is in a bucket or under a prefix.
    """

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
                # Strictly past the limit, not merely at it: a prefix holding
                # exactly *limit* entries is complete, and reporting it as
                # truncated told the user to raise a cap that was not
                # hiding anything. Costs at most one more page at the
                # boundary, which is what it takes to know the difference.
                if limit and len(items) > limit:
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

    def prefix_has_anything(self, prefix: str) -> bool:
        """
        Whether a single object exists under *prefix*, asked as cheaply as
        the protocol allows.

        MaxKeys=1 and no pagination: the question is existence, and reading
        a whole folder to answer it is what made the overwrite check the
        most expensive operation in the app.
        """
        if not prefix or not self.bucket:
            return False
        resp = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return int(resp.get("KeyCount") or 0) > 0 or bool(resp.get("Contents"))

    def existing_keys(self, keys) -> set:
        """
        Return the subset of *keys* that already exist.

        File targets are settled with one recursive listing per distinct
        parent prefix rather than a HEAD per key. Folder targets are settled
        with a one-key probe of the folder itself, which fixes two things
        the shared listing got wrong: a top-level folder like "photos/" has
        no parent, so its "parent prefix" was the bucket root and the check
        listed EVERY object in the bucket to decide one question; and a
        folder holding nothing but sub-folder placeholders was reported as
        free, because the old scan only counted non-placeholder keys.
        """
        wanted = {k for k in keys if k}
        if not wanted or not self.bucket:
            return set()

        folders = {k for k in wanted if k.endswith("/")}
        files = wanted - folders

        found = {folder for folder in folders
                 if self.prefix_has_anything(folder)}

        prefixes = set()
        for key in files:
            prefixes.add(key.rsplit("/", 1)[0] + "/" if "/" in key else "")
        for prefix in prefixes:
            for k, _size in self.get_keys(prefix):
                if k in files:
                    found.add(k)
        return found

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
