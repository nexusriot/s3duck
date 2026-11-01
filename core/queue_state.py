"""The transfer queue as something that survives the window closing.

A job used to exist only as a live ``_QEntry``: closing the window, a crash or
a profile switch dropped everything still waiting, and nothing said so. What
is stored here is the part of an entry that can be rebuilt from JSON -- the
method, the job rows, the bucket and connection it was queued against and the
user's overwrite answer -- never the models or threads it ran on.

A job is also stored *minus what already finished*. That is what makes a
pause more than a cancel: the items a worker reported done are dropped, so a
resumed 500-file upload continues at file 251 instead of starting over, and
the file that was in flight picks up from its own resume record.
"""

import base64
import hashlib
import json

from core.locations import CROSS_PROFILE_METHODS

# Methods whose job is a plan worked out from both sides as they were then --
# including what to delete. Replayed after a restart (days later, maybe), its
# delete rows would remove what has changed since; history refuses to re-run
# one for the same reason. Run Sync again for a fresh comparison instead.
NOT_PERSISTED_METHODS = ("sync", "sync_to_profile")

# Statuses worth keeping. "running" is stored as queued -- whatever was
# running when the window went away has stopped -- and "pausing" as paused.
PENDING_STATUSES = ("queued", "running", "paused", "pausing")

# A job bigger than this is not persisted. Settings live in one file, and a
# 100 000-row prefix walk written on every queue change would make every
# save pay for it; such a job is announced in the log instead.
PENDING_JOB_LIMIT = 10000

# Methods whose items are independent, so finishing one is permanent and a
# resume may skip it. A move is absent on purpose: its sources are deleted in
# a second pass after every copy, so "this row is done" means nothing until
# the whole job is.
ITEM_RESUMABLE_METHODS = ("upload", "download", "sync", "delete", "copy",
                          "set_tags", "set_storage_class", "set_content_type",
                          "restore", "restore_versions")


def sse_c_md5(key) -> str:
    """The MD5 S3 reports for a customer key, or "" for none. Not the key."""
    if not key:
        return ""
    return base64.b64encode(hashlib.md5(bytes(key)).digest()).decode()


def remaining_job(job, done_indices):
    """The rows of *job* that have not been reported finished."""
    done = set(done_indices or ())
    return [item for index, item in enumerate(job or []) if index not in done]


def _jsonable(job):
    try:
        return json.loads(json.dumps(job))
    except (TypeError, ValueError):
        return None


def serialize_pending(entries, limit=PENDING_JOB_LIMIT):
    """
    ``(records, skipped)`` for every unfinished entry, oldest first.

    ``entries`` are objects with the ``_QEntry`` attributes. Each record holds
    what a restore needs and nothing that names a live object; ``skipped``
    lists the labels of entries that could not be stored (another profile's
    connection, a job too large, rows that are not JSON) so the caller can
    say so rather than lose them quietly.
    """
    records, skipped = [], []
    for entry in sorted(entries, key=lambda e: e.entry_id):
        status = getattr(entry, "status", "")
        if status not in PENDING_STATUSES:
            continue
        label = getattr(entry, "label", "") or entry.method
        if (entry.method in CROSS_PROFILE_METHODS
                or entry.method in NOT_PERSISTED_METHODS):
            skipped.append(label)
            continue
        job = entry.job or []
        if entry.method in ITEM_RESUMABLE_METHODS:
            job = remaining_job(job, getattr(entry, "done_items", ()))
        if not job:
            continue
        if len(job) > limit:
            skipped.append(label)
            continue
        rows = _jsonable(job)
        if rows is None:
            skipped.append(label)
            continue
        records.append({
            "method": entry.method,
            "label": label,
            "job": rows,
            "need_refresh": bool(getattr(entry, "need_refresh", True)),
            "source_bucket": getattr(entry, "source_bucket", "") or "",
            # The bucket the job was made in; None (unknown) means the one
            # open when it is restored.
            "bucket": getattr(entry, "bucket", None),
            "if_none_match": bool(getattr(entry, "if_none_match", False)),
            "origin": dict(getattr(entry, "origin", None) or {}),
            "paused": status in ("paused", "pausing"),
            "sse_c_md5": getattr(entry, "sse_c_md5", "") or "",
        })
    return records, skipped


def parse_pending(raw) -> list:
    """Records from their stored JSON, dropping anything malformed."""
    try:
        data = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    out = []
    for record in data:
        if not isinstance(record, dict):
            continue
        method = record.get("method")
        job = record.get("job")
        if not isinstance(method, str) or not method:
            continue
        if not isinstance(job, list) or not job:
            continue
        if method in CROSS_PROFILE_METHODS or method in NOT_PERSISTED_METHODS:
            continue
        out.append(record)
    return out


def split_restorable(records, origin):
    """
    ``(restorable, foreign)``: records queued against *origin*, and the rest.

    The origin is the profile, endpoint, region and a hash of the access key.
    A record from the same profile name pointing somewhere else -- the
    profile was edited, or its keys rotated -- must not be replayed against
    the new account, for the same reason a history re-run refuses it.
    """
    restorable, foreign = [], []
    for record in records or []:
        if record.get("origin") and record.get("origin") == dict(origin or {}):
            restorable.append(record)
        else:
            foreign.append(record)
    return restorable, foreign


def restore_rows(job):
    """JSON turned tuples into lists; the workers unpack either, but the
    window's own job builders compare tuples."""
    return [tuple(item) if isinstance(item, list) else item for item in job]


def describe_pending(records) -> str:
    """One line for the restore notice: how many jobs, how many items."""
    jobs = len(records or [])
    items = sum(len(r.get("job") or []) for r in records or [])
    paused = sum(1 for r in records or [] if r.get("paused"))
    text = (f"{jobs} unfinished transfer{'s' if jobs != 1 else ''} "
            f"({items} item{'s' if items != 1 else ''}) from the last session")
    if paused:
        text += f", {paused} paused"
    return text
