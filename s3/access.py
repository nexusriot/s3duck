"""Who may reach a bucket and its objects: ACLs, Block Public Access, access
logging, replication, Object Lock retention and browser upload forms.
"""

import botocore.exceptions

from core.acl import build_access_control_policy, parse_acl
from core.replication import summarize_replication
from core.retention import MODES

PUBLIC_ACCESS_FLAGS = ("BlockPublicAcls", "IgnorePublicAcls",
                       "BlockPublicPolicy", "RestrictPublicBuckets")


class AccessMixin:
    """Who may reach a bucket and its objects."""

    def _require_bucket(self):
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")

    def get_public_access_block(self):
        """
        The four Block Public Access flags, or None when none are set.

        None and "all four off" are different answers: the first means the
        bucket falls back to the account-level setting, which this app cannot
        see, so the tab says so rather than showing four unticked boxes.
        """
        self._require_bucket()
        try:
            resp = self.client.get_public_access_block(Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchPublicAccessBlockConfiguration",
                        *self.ABSENT_DOCUMENT_CODES):
                return None
            raise
        cfg = resp.get("PublicAccessBlockConfiguration") or {}
        return {flag: bool(cfg.get(flag)) for flag in PUBLIC_ACCESS_FLAGS}

    def put_public_access_block(self, flags, log_fn=None):
        self._guard_write()
        self._require_bucket()
        config = {flag: bool((flags or {}).get(flag)) for flag in PUBLIC_ACCESS_FLAGS}
        self.client.put_public_access_block(
            Bucket=self.bucket, PublicAccessBlockConfiguration=config)
        if log_fn:
            on = [flag for flag, value in config.items() if value]
            log_fn(f"Block Public Access on {self.bucket}: "
                   f"{', '.join(on) if on else 'all off'}")

    def delete_public_access_block(self, log_fn=None):
        self._guard_write()
        self._require_bucket()
        self.client.delete_public_access_block(Bucket=self.bucket)
        if log_fn:
            log_fn(f"removed the Block Public Access settings of {self.bucket}")

    def get_bucket_logging(self) -> dict:
        """``{"target_bucket", "target_prefix"}``, or {} when logging is off."""
        self._require_bucket()
        resp = self._optional_document(self.client.get_bucket_logging)
        enabled = (resp or {}).get("LoggingEnabled") or {}
        if not enabled:
            return {}
        return {"target_bucket": str(enabled.get("TargetBucket") or ""),
                "target_prefix": str(enabled.get("TargetPrefix") or "")}

    def put_bucket_logging(self, target_bucket="", target_prefix="", log_fn=None):
        """Turn server access logging on (to *target_bucket*) or off ("")."""
        self._guard_write()
        self._require_bucket()
        target_bucket = str(target_bucket or "").strip()
        status = {}
        if target_bucket:
            status["LoggingEnabled"] = {
                "TargetBucket": target_bucket,
                "TargetPrefix": str(target_prefix or ""),
            }
        self.client.put_bucket_logging(
            Bucket=self.bucket, BucketLoggingStatus=status)
        if log_fn:
            log_fn(f"access logging on {self.bucket}: "
                   + (f"to {target_bucket}/{target_prefix}" if target_bucket
                      else "off"))

    def get_bucket_replication(self) -> dict:
        """``{"role", "rules"}``; no rules when replication is not set up."""
        self._require_bucket()
        try:
            resp = self.client.get_bucket_replication(Bucket=self.bucket)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("ReplicationConfigurationNotFoundError",
                        "ReplicationConfigurationNotFound",
                        *self.ABSENT_DOCUMENT_CODES):
                return {"role": "", "rules": []}
            raise
        return summarize_replication(resp)

    def get_bucket_acl(self) -> dict:
        self._require_bucket()
        return parse_acl(self.client.get_bucket_acl(Bucket=self.bucket))

    def put_bucket_acl(self, owner, grants, log_fn=None):
        self._guard_write()
        self._require_bucket()
        policy = build_access_control_policy(owner, grants)
        self.client.put_bucket_acl(Bucket=self.bucket,
                                   AccessControlPolicy=policy)
        if log_fn:
            log_fn(f"wrote {len(policy['Grants'])} grant(s) to the ACL of "
                   f"{self.bucket}")

    def get_object_acl(self, key, version_id="") -> dict:
        self._require_bucket()
        params = {"Bucket": self.bucket, "Key": key}
        if version_id:
            params["VersionId"] = version_id
        return parse_acl(self.client.get_object_acl(**params))

    def put_object_acl_grants(self, key, owner, grants, version_id="",
                              log_fn=None):
        self._guard_write()
        self._require_bucket()
        params = {"Bucket": self.bucket, "Key": key,
                  "AccessControlPolicy": build_access_control_policy(owner, grants)}
        if version_id:
            params["VersionId"] = version_id
        self.client.put_object_acl(**params)
        if log_fn:
            log_fn(f"wrote {len(grants or [])} grant(s) to the ACL of {key}")

    def set_object_retention(self, key, mode, retain_until, version_id="",
                             bypass_governance=False, log_fn=None):
        """
        Set an object's retention mode and date.

        The bypass is sent only when asked for. Sending it on every call
        would turn the one request that needs a deliberate permission into a
        default, which is the opposite of what GOVERNANCE is for.
        """
        self._guard_write()
        self._require_bucket()
        if mode not in MODES:
            raise ValueError(f"Retention mode must be one of {', '.join(MODES)}")
        params = {"Bucket": self.bucket, "Key": key,
                  "Retention": {"Mode": mode, "RetainUntilDate": retain_until}}
        if version_id:
            params["VersionId"] = version_id
        if bypass_governance:
            params["BypassGovernanceRetention"] = True
        self.client.put_object_retention(**params)
        if log_fn:
            log_fn(f"retention {mode} until {retain_until} on {key}"
                   + (" (governance bypassed)" if bypass_governance else ""))

    def presigned_post(self, key, expires_sec=3600, max_bytes=0,
                       content_type="", any_key_under_prefix=False) -> dict:
        """
        (A form is a write: a read-only profile cannot mint one, the same as
        it cannot mint a presigned PUT. Nor can a session with an SSE-C key,
        whose form would land objects without that key's encryption.)

        A browser-upload form: ``{"url", "fields"}`` for a multipart POST.

        Unlike a presigned PUT, the form's policy is enforced by the service:
        the size range and Content-Type are conditions it checks, so a link
        handed to someone else cannot be used for a 5 GB upload or to drop an
        executable where an image was expected. ``any_key_under_prefix`` lets
        the uploader choose the name, but only under *key* (which must then
        be a prefix).
        """
        self._guard_write()
        self._require_bucket()
        self.refuse_presign_with_sse_c()
        conditions, fields = [], {}
        if max_bytes and int(max_bytes) > 0:
            conditions.append(["content-length-range", 0, int(max_bytes)])
        if content_type:
            fields["Content-Type"] = content_type
            conditions.append({"Content-Type": content_type})
        # botocore turns a key ending in ${filename} into its own
        # starts-with condition on the part before it.
        object_key = key + "${filename}" if any_key_under_prefix else key
        return self.client.generate_presigned_post(
            Bucket=self.bucket, Key=object_key, Fields=fields or None,
            Conditions=conditions or None, ExpiresIn=int(expires_sec))
