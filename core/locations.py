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
def build_profile_copy_job(items, dest_prefix) -> list:
    """
    ``(src_key, dst_key, is_folder)`` rows copying *items* under *dest_prefix*.

    ``items`` are the ``(name, key, is_folder)`` targets every selection
    produces; a folder lands as ``dest_prefix + name/``.
    """
    prefix = str(dest_prefix or "")
    rows = []
    for name, src_key, is_folder in items or []:
        name = str(name or "").rstrip("/")
        if not name:
            continue
        if is_folder:
            rows.append((src_key, prefix + name + "/", True))
        else:
            rows.append((src_key, prefix + name, False))
    return rows
def same_store(left, right) -> bool:
    """
    Whether two connections address the same bucket of the same account.

    Two profiles can do that: an alias with the same keys, or the window
    switched to the profile the second pane is already showing. A "move to
    another profile" between them is a move within one bucket, and has to
    obey the same overlap rule as any other.
    """
    def ident(model):
        endpoint = (getattr(model, "profile_endpoint_url", "")
                    or getattr(model, "endpoint_url", "") or "")
        return (str(endpoint).rstrip("/").lower(),
                str(getattr(model, "bucket", "") or ""),
                str(getattr(model, "access_key", "") or ""))
    return ident(left) == ident(right)


def move_overlap(rows):
    """
    The first ``(src, dst)`` of a move whose destination overlaps a source.

    Copying a folder into itself and then deleting the source deletes the
    copy with it; moving an object onto its own key copies it in place and
    then deletes the only copy. ``rows`` are ``(src, dst, is_folder, …)``.
    """
    rows = list(rows or [])
    sources = {row[0] for row in rows}
    folders = [row[0] for row in rows if row[2]]
    for src, dst, is_folder, *_rest in rows:
        if dst in sources:
            return src, dst
        inside = next((key for key in folders if dst.startswith(key)), None)
        if inside is not None:
            return inside, dst
        if is_folder and any(key.startswith(dst) for key in sources):
            return src, dst
    return None


# Jobs that hold a live connection to another profile, so they cannot be
# reconstructed from persisted history.
CROSS_PROFILE_METHODS = ("copy_to_profile", "sync_to_profile",
                         "move_to_profile")
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
