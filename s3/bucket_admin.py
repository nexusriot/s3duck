"""A bucket's own settings: policy, CORS, lifecycle, encryption, locking.
"""

import botocore.exceptions
import json


class BucketAdminMixin:
    """A bucket's own settings: policy, CORS, lifecycle, encryption, locking.
    """

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

    def _lifecycle_minimum_object_size(self) -> str:
        """
        The configuration's TransitionDefaultMinimumObjectSize, or "".

        It lives beside the rules rather than in them, and a PUT that omits it
        resets it to the service default -- so saving the rules, even
        unchanged, quietly re-enabled transitions the bucket owner had chosen
        to apply to small objects too. "" when there is no configuration yet
        or the backend (or the installed botocore) does not know the field;
        any other failure to read is raised rather than written over blind.
        """
        try:
            resp = self._optional_document(
                self.client.get_bucket_lifecycle_configuration)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "NoSuchLifecycleConfigurationError":
                return ""
            raise
        return str((resp or {}).get("TransitionDefaultMinimumObjectSize") or "")

    def put_bucket_lifecycle(self, rules, log_fn=None):
        """
        Replace the bucket's lifecycle rules; an empty list removes them.

        The rules are replaced; the bucket-wide minimum object size stored
        beside them is carried over (see _lifecycle_minimum_object_size).
        """
        self._guard_write()
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if not rules:
            self.client.delete_bucket_lifecycle(Bucket=self.bucket)
            if log_fn:
                log_fn(f"removed the lifecycle configuration of {self.bucket}")
            return
        params = {"Bucket": self.bucket,
                  "LifecycleConfiguration": {"Rules": list(rules)}}
        minimum = self._lifecycle_minimum_object_size()
        if minimum:
            params["TransitionDefaultMinimumObjectSize"] = minimum
        self.client.put_bucket_lifecycle_configuration(**params)
        if log_fn:
            log_fn(f"wrote {len(rules)} lifecycle rule(s) to {self.bucket}")

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

        Read-modify-write: the stored document is fetched first and only the
        three fields edited here are replaced. PutBucketWebsite replaces the
        whole document, and rebuilding it from those three fields deleted the
        site's RoutingRules and turned an https redirect into one that follows
        whatever protocol the visitor came in on. A read that fails is raised
        rather than written over blind.
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
        stored = self._optional_document(self.client.get_bucket_website) or {}
        if redirect:
            # Protocol is the one field beside HostName; the routing rules
            # and documents cannot coexist with a redirect-everything host.
            target = dict(stored.get("RedirectAllRequestsTo") or {})
            target["HostName"] = redirect
            config = {"RedirectAllRequestsTo": target}
        else:
            config = {"IndexDocument": {"Suffix": index}}
            if error:
                config["ErrorDocument"] = {"Key": error}
            if stored.get("RoutingRules"):
                config["RoutingRules"] = list(stored["RoutingRules"])
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
