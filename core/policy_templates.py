"""Reviewed bucket-policy snippets, merged into an existing document.

A blank JSON box is where public buckets come from: the statement people
paste from a forum grants ``s3:*`` to ``"*"``. Each template here produces one
narrowly scoped statement with a fixed ``Sid``, so inserting it twice replaces
the first copy instead of stacking duplicates, and the rest of the policy is
left exactly as it was.
"""

import hashlib
import json
import re

_ACCOUNT = re.compile(r"^\d{12}$")


def _arn(bucket, prefix=""):
    prefix = str(prefix or "").lstrip("/")
    return f"arn:aws:s3:::{bucket}/{prefix}*"


def _sid(text, scope=None):
    """
    A statement id: the readable part, plus a digest of what it applies to.

    The id is what makes re-inserting a template replace its own earlier
    copy, so two templates for different prefixes must never share one.
    Stripping punctuation alone made "a-b/" and "ab/" collide, and an
    account's grant for invoices/ replaced its grant for reports/.
    """
    base = re.sub(r"[^A-Za-z0-9]", "", text)[:48] or "Statement"
    if scope is None:
        return base
    return base + hashlib.sha256(str(scope).encode()).hexdigest()[:10]


def public_read_prefix(bucket, prefix=""):
    """Anyone may GET objects under *prefix* (and nothing else)."""
    return {
        "Sid": _sid("PublicRead", prefix or ""),
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": _arn(bucket, prefix),
    }


def allow_account(bucket, account_id, write=False, prefix=""):
    """Another AWS account may read (and optionally write) under *prefix*."""
    account_id = str(account_id or "").strip()
    if not _ACCOUNT.match(account_id):
        raise ValueError("An AWS account ID is 12 digits.")
    actions = ["s3:GetObject"]
    if write:
        actions += ["s3:PutObject", "s3:DeleteObject"]
    statements = [{
        "Sid": _sid(f"Account{account_id}{'ReadWrite' if write else 'Read'}",
                    prefix or ""),
        "Effect": "Allow",
        "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
        "Action": actions,
        "Resource": _arn(bucket, prefix),
    }, {
        "Sid": _sid(f"Account{account_id}List", prefix or ""),
        "Effect": "Allow",
        "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
        "Action": "s3:ListBucket",
        "Resource": f"arn:aws:s3:::{bucket}",
    }]
    if prefix:
        statements[1]["Condition"] = {
            "StringLike": {"s3:prefix": [str(prefix).lstrip("/") + "*"]}}
    return statements


def deny_insecure_transport(bucket):
    """Refuse every request that does not arrive over TLS."""
    return {
        "Sid": "DenyInsecureTransport",
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    }


TEMPLATES = {
    "public_read": "Make a prefix publicly readable",
    "allow_account": "Allow another AWS account",
    "deny_http": "Refuse plain-HTTP requests",
}


def build_template(name, bucket, prefix="", account_id="", write=False) -> list:
    """The statements one template produces, as a list."""
    if not bucket:
        raise ValueError("Open a bucket first.")
    if name == "public_read":
        return [public_read_prefix(bucket, prefix)]
    if name == "allow_account":
        return allow_account(bucket, account_id, write=write, prefix=prefix)
    if name == "deny_http":
        return [deny_insecure_transport(bucket)]
    raise ValueError(f"Unknown template: {name}")


def merge_statements(policy_text, statements) -> str:
    """
    *policy_text* with *statements* added, replacing any with the same Sid.

    An empty text starts a new policy. A document that does not parse is
    refused rather than overwritten, because the alternative is silently
    discarding whatever the user had typed.
    """
    text = str(policy_text or "").strip()
    if text:
        try:
            policy = json.loads(text)
        except ValueError as exc:
            raise ValueError(f"The current policy is not valid JSON: {exc}") from exc
        if not isinstance(policy, dict):
            raise ValueError("A bucket policy must be a JSON object.")
    else:
        policy = {"Version": "2012-10-17", "Statement": []}
    existing = policy.get("Statement") or []
    if isinstance(existing, dict):
        existing = [existing]
    incoming = {s.get("Sid") for s in statements if s.get("Sid")}
    kept = [s for s in existing
            if not (isinstance(s, dict) and s.get("Sid") in incoming)]
    policy.setdefault("Version", "2012-10-17")
    policy["Statement"] = kept + list(statements)
    return json.dumps(policy, indent=2)


def grants_public_access(statements) -> bool:
    """Whether any Allow statement names ``"*"`` as its principal."""
    for statement in statements or []:
        if statement.get("Effect") != "Allow":
            continue
        principal = statement.get("Principal")
        if principal == "*" or (isinstance(principal, dict)
                                and principal.get("AWS") == "*"):
            return True
    return False
