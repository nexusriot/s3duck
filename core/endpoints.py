"""Reading an S3 endpoint URL: region, bucket-in-host, redirect conflicts.

A provider that answers one addressing style and not the other, or that
redirects to the bucket's real region, is normal. Working out what the URL is
actually saying is plain string work and belongs away from the client.
"""

from urllib.parse import urlparse


def _endpoint_has_bucket(endpoint_url: str, bucket_name: str) -> bool:
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


def _extract_leftmost_label(endpoint_url: str) -> str:
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


def _redirect_conflicts_with_bucket(endpoint_url: str,
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


def build_region_swapped_endpoint(base_endpoint: str,
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
