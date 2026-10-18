"""``bucket/prefix`` as a string, and the rows the switcher shows."""


def location_entries(buckets, bookmarks, current_bucket="") -> list:
    """
    ``(label, hint, (bucket, prefix))`` rows for the quick-open switcher.

    Bookmarks come first because a saved place is almost always what someone
    is reaching for; the bucket that is already open is dropped, since jumping
    to where you already are is not a useful result.
    """
    rows, seen = [], set()
    for entry in bookmarks or []:
        bucket = str((entry or {}).get("bucket") or "").strip()
        if not bucket:
            continue
        prefix = str((entry or {}).get("prefix") or "").strip()
        label = str((entry or {}).get("name") or "").strip() or \
            serialize_location(bucket, prefix)
        key = (bucket, prefix)
        if key in seen:
            continue
        seen.add(key)
        rows.append((label, serialize_location(bucket, prefix), key))
    for name in buckets or []:
        bucket = str(name or "").strip()
        if not bucket or bucket == str(current_bucket or "").strip():
            continue
        key = (bucket, "")
        if key in seen:
            continue
        seen.add(key)
        rows.append((bucket, "bucket", key))
    return rows
def build_profile_sync_job(actions, source_prefix, dest_prefix) -> list:
    """
    Map a sync plan onto ``(operation, src_key, dst_key)`` rows.

    The planner is reused unchanged by feeding it the two profiles' trees as
    source and destination, so its "upload"/"delete_remote" verbs mean
    "copy to the other profile" and "delete over there" here.
    """
    rows = []
    for action in actions or []:
        rel = str(action.get("rel") or "")
        if not rel:
            continue
        kind = action.get("action")
        if kind == "upload":
            rows.append(("copy", source_prefix + rel, dest_prefix + rel))
        elif kind == "delete_remote":
            rows.append(("delete", "", dest_prefix + rel))
    return rows
# Jobs that hold a live connection to another profile, so they cannot be
# reconstructed from persisted history.
CROSS_PROFILE_METHODS = ("copy_to_profile", "sync_to_profile")
def serialize_location(bucket, prefix) -> str:
    """
    A browsing location as one settings string, or "" in bucket-list mode.

    Its own codec rather than reusing the s3:// parser, whose meaning depends
    on which bucket happens to be open when it runs.
    """
    name = str(bucket or "").strip().strip("/")
    if not name:
        return ""
    inner = str(prefix or "").strip().strip("/")
    return f"{name}/{inner}/" if inner else name
def parse_location(text) -> tuple:
    """Reverse serialize_location as (bucket, prefix); prefix ends with "/"."""
    raw = str(text or "").strip().lstrip("/")
    if not raw:
        return "", ""
    name, _, inner = raw.partition("/")
    inner = inner.strip("/")
    return name.strip(), (inner + "/" if inner else "")


def parse_s3_location(text: str, current_bucket: str = ""):
    """
    Parse 's3://bucket/prefix', 'bucket/prefix' or a bare 'prefix' into
    (bucket, prefix). A bare prefix keeps the current bucket. The returned
    prefix is '' or ends with '/'.
    """
    raw = (text or "").strip()
    if not raw:
        return "", ""
    for scheme in ("s3://", "s3a://", "S3://"):
        if raw.lower().startswith(scheme.lower()):
            raw = raw[len(scheme):]
            bucket, _, prefix = raw.partition("/")
            prefix = prefix.strip("/")
            return bucket.strip("/"), (prefix + "/" if prefix else "")
    raw = raw.lstrip("/")
    if current_bucket:
        prefix = raw.strip("/")
        return current_bucket, (prefix + "/" if prefix else "")
    bucket, _, prefix = raw.partition("/")
    prefix = prefix.strip("/")
    return bucket.strip("/"), (prefix + "/" if prefix else "")
