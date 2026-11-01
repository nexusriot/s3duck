"""Two-way sync: what changed on which side since the last time they agreed.

A one-way sync only needs the two trees; a two-way one cannot work from them
alone, because "a file is here and not there" means either "it is new here"
or "it was deleted there", and the two need opposite actions. The answer is
a small state file written after every run: for each path the two sides
agreed on, the local size and modification time and the remote size and
ETag. Against that, each side's change is visible on its own:

* changed on one side only -> copy it to the other;
* deleted on one side, untouched on the other -> delete it there too;
* changed on both sides, or changed on one and deleted on the other ->
  a conflict, which is listed and never resolved silently.

A path with no state (the first run, or a path added since) is never a
deletion: it is copied to the side that lacks it, and a path present on both
sides with no history is settled by content where that is cheap to prove,
and is a conflict otherwise.
"""

import hashlib
import json
import os

from core.sync_plan import SYNC_MTIME_TOLERANCE_SEC, build_exclude_matcher

UPLOAD, DOWNLOAD = "upload", "download"
DELETE_LOCAL, DELETE_REMOTE = "delete_local", "delete_remote"
CONFLICT, SKIP = "conflict", "skip"


def _local_changed(now, then, tolerance):
    if then is None:
        return True
    size, mtime = now
    old_size, old_mtime = then
    return int(size) != int(old_size) or \
        abs(float(mtime) - float(old_mtime)) > tolerance


def _remote_changed(now, then):
    if then is None:
        return True
    return int(now[0]) != int(then[0]) or str(now[1]) != str(then[1])


def plan_two_way(local, remote, state, *, exclude=None, identical=None,
                 tolerance=SYNC_MTIME_TOLERANCE_SEC) -> list:
    """
    The actions that bring a local tree and a remote tree back into step.

    ``local`` is ``{rel: (size, mtime)}``, ``remote`` ``{rel: (size, etag)}``
    and ``state`` ``{rel: {"local": [size, mtime], "remote": [size, etag]}}``
    from the last run. ``identical(rel)`` may answer True/False/None for a
    path present on both sides with no state; None (or no callable) makes it
    a conflict. Returns ``[{"action", "rel", "size", "reason"}]``.
    """
    state = state or {}
    is_excluded = exclude if callable(exclude) else build_exclude_matcher(exclude)
    actions = []
    for rel in sorted(set(local) | set(remote) | set(state)):
        if is_excluded(rel):
            continue
        here, there = local.get(rel), remote.get(rel)
        known = state.get(rel)
        seen_here = known.get("local") if known else None
        seen_there = known.get("remote") if known else None

        def act(kind, reason, size=None):
            if size is None:
                size = (here or there or (0,))[0]
            actions.append({"action": kind, "rel": rel,
                            "size": int(size or 0), "reason": reason})

        if here is None and there is None:
            continue
        if here is not None and there is not None:
            if known is None:
                same = identical(rel) if identical is not None else None
                if same:
                    act(SKIP, "same content on both sides")
                else:
                    act(CONFLICT, "exists on both sides with no shared history"
                        if same is None else "differs on both sides")
                continue
            changed_here = _local_changed(here, seen_here, tolerance)
            changed_there = _remote_changed(there, seen_there)
            if changed_here and changed_there:
                act(CONFLICT, "changed on both sides")
            elif changed_here:
                act(UPLOAD, "changed locally", here[0])
            elif changed_there:
                act(DOWNLOAD, "changed in the bucket", there[0])
            else:
                act(SKIP, "unchanged")
            continue
        if here is not None:
            if known is None:
                act(UPLOAD, "new locally", here[0])
            elif _local_changed(here, seen_here, tolerance):
                act(CONFLICT, "changed locally, deleted in the bucket")
            else:
                act(DELETE_LOCAL, "deleted in the bucket", here[0])
            continue
        if known is None:
            act(DOWNLOAD, "new in the bucket", there[0])
        elif _remote_changed(there, seen_there):
            act(CONFLICT, "changed in the bucket, deleted locally")
        else:
            act(DELETE_REMOTE, "deleted locally", there[0])
    return actions


def resolve_conflict(action, keep) -> dict:
    """A conflict turned into a copy that keeps the ``"local"`` or ``"remote"``
    side; a side that no longer has the file becomes a delete of the other."""
    if action.get("action") != CONFLICT:
        return dict(action)
    reason = action.get("reason", "")
    out = dict(action)
    if keep == "local":
        deleted_here = reason.startswith("changed in the bucket, deleted locally")
        out["action"] = DELETE_REMOTE if deleted_here else UPLOAD
    else:
        deleted_there = reason.startswith("changed locally, deleted in")
        out["action"] = DELETE_LOCAL if deleted_there else DOWNLOAD
    out["reason"] = f"conflict resolved: kept {keep}"
    return out


def build_state(local, remote, keep_old=None, old_state=None) -> dict:
    """
    The state to record after a run, from fresh scans of both sides.

    Only a path present on both sides with the same size is recorded as
    agreed. ``keep_old`` names paths (open conflicts) whose previous entry
    is carried over unchanged, so the next run still sees them as changed.
    """
    old_state = old_state or {}
    keep_old = set(keep_old or ())
    # Carried over whichever side still has the file: a conflict that is
    # "changed in the bucket, deleted locally" exists only remotely, and
    # dropping its entry made the next run download it as new.
    out = {rel: old_state[rel] for rel in keep_old if rel in old_state}
    for rel, (size, mtime) in local.items():
        if rel in keep_old:
            continue
        theirs = remote.get(rel)
        if theirs is None or int(theirs[0]) != int(size):
            continue
        out[rel] = {"local": [int(size), float(mtime)],
                    "remote": [int(theirs[0]), str(theirs[1])]}
    return out


def state_path(state_dir, endpoint, bucket, prefix, local_dir) -> str:
    """One state file per pairing of a local folder and a remote prefix."""
    ident = "\n".join((str(endpoint or "").rstrip("/"), str(bucket or ""),
                       str(prefix or ""), os.path.abspath(local_dir or "")))
    name = hashlib.sha256(ident.encode()).hexdigest()[:32] + ".json"
    return os.path.join(state_dir, name)


def load_state(path) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return {}
    paths = data.get("paths") if isinstance(data, dict) else None
    return paths if isinstance(paths, dict) else {}


def save_state(path, paths, meta=None):
    """Written to a temporary file and renamed, so a crash cannot truncate it."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump({"meta": dict(meta or {}), "paths": paths}, handle)
    os.replace(tmp, path)


def md5_matches(path, etag, limit=256 * 1024 * 1024) -> object:
    """True/False when a single-part ETag can settle it, None when it cannot."""
    etag = str(etag or "").strip('"')
    if len(etag) != 32 or "-" in etag:
        return None
    try:
        if os.path.getsize(path) > limit:
            return None
        digest = hashlib.md5()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError:
        return None
    return digest.hexdigest() == etag.lower()
