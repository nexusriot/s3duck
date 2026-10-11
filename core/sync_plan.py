"""Comparing two trees and deciding what has to move.

One planner serves local-to-remote sync, remote-to-remote sync between two
profiles, and the read-only pane comparison: they differ only in what is fed
in as "left" and "right".
"""

import fnmatch
import re


# Filesystem and S3 timestamps disagree by up to a couple of seconds
# (second resolution remotely, 2s granularity on some local filesystems), so
# only a larger difference counts as "newer".
SYNC_MTIME_TOLERANCE_SEC = 2.0
def build_exclude_matcher(patterns):
    """
    Compile sync exclude patterns into ``match(rel_path) -> bool``.

    A pattern matches if it globs the whole relative path or just the file
    name, so `*.tmp` works at any depth. A pattern ending in `/` (or naming a
    directory) excludes everything beneath it, which is what `node_modules/`
    is expected to do.

    A single string is split here rather than being iterated character by
    character — passing the raw text from an input box is the obvious mistake,
    and its symptom is the lone "*" excluding absolutely everything.
    """
    if isinstance(patterns, str):
        patterns = re.split(r"[,\s]+", patterns)
    cleaned = [p.strip() for p in (patterns or []) if p and p.strip()]
    if not cleaned:
        return lambda _rel: False

    dir_prefixes = []
    globs = []
    for pattern in cleaned:
        if pattern.endswith("/"):
            dir_prefixes.append(pattern.rstrip("/"))
        else:
            globs.append(pattern)

    def _match(rel):
        rel = (rel or "").strip("/")
        if not rel:
            return False
        parts = rel.split("/")
        for prefix in dir_prefixes:
            if rel == prefix or rel.startswith(prefix + "/"):
                return True
            if prefix in parts[:-1]:
                return True
        name = parts[-1]
        for pattern in globs:
            if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern):
                return True
            # a bare directory name also excludes its contents
            if pattern in parts[:-1]:
                return True
        return False

    return _match
def build_sync_plan(local_entries, remote_entries, *, direction,
                    delete_extra=False, tolerance=SYNC_MTIME_TOLERANCE_SEC,
                    exclude=None):
    """
    Compare two ``{rel_path: (size, mtime_epoch)}`` maps and return the list of
    actions needed to make the destination match the source.

    direction "upload" treats local as the source, "download" treats remote as
    the source. Each action is a dict with keys: action
    (upload/download/delete_remote/delete_local/skip), rel, size, reason.
    """
    if direction not in ("upload", "download"):
        raise ValueError("direction must be 'upload' or 'download'")

    is_excluded = exclude if callable(exclude) else build_exclude_matcher(exclude)

    if direction == "upload":
        source, dest = local_entries, remote_entries
        transfer, delete_action = "upload", "delete_remote"
    else:
        source, dest = remote_entries, local_entries
        transfer, delete_action = "download", "delete_local"

    actions = []
    for rel in sorted(source):
        if is_excluded(rel):
            continue
        src_size, src_mtime = source[rel]
        if rel not in dest:
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "missing at destination"})
            continue
        dst_size, dst_mtime = dest[rel]
        if int(src_size) != int(dst_size):
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "size differs"})
        elif float(src_mtime) - float(dst_mtime) > tolerance:
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "source is newer"})
        else:
            actions.append({"action": "skip", "rel": rel,
                            "size": src_size, "reason": "up to date"})

    for rel in sorted(dest):
        if rel in source or is_excluded(rel):
            continue
        size = dest[rel][0]
        if delete_extra:
            actions.append({"action": delete_action, "rel": rel,
                            "size": size, "reason": "not at source"})
        else:
            actions.append({"action": "skip", "rel": rel, "size": size,
                            "reason": "extra at destination (kept)"})
    return actions
def build_compare_plan(left, right, *, tolerance=SYNC_MTIME_TOLERANCE_SEC,
                       exclude=None) -> list:
    """
    Diff two ``{rel_path: (size, mtime)}`` maps without picking a direction.

    Returns ``[{rel, status, left_size, right_size}]`` where status is
    "only_left", "only_right", "differs" or "same". Direction-free on purpose:
    the same comparison drives a copy either way, and calling one side "the
    source" before the user has said so is how a compare turns into a sync
    nobody asked for.
    """
    is_excluded = exclude if callable(exclude) else build_exclude_matcher(exclude)
    rows = []
    for rel in sorted(set(left or {}) | set(right or {})):
        if is_excluded(rel):
            continue
        here = (left or {}).get(rel)
        there = (right or {}).get(rel)
        if here is None:
            rows.append({"rel": rel, "status": "only_right",
                         "left_size": None, "right_size": int(there[0])})
            continue
        if there is None:
            rows.append({"rel": rel, "status": "only_left",
                         "left_size": int(here[0]), "right_size": None})
            continue
        same_size = int(here[0]) == int(there[0])
        close_enough = abs(float(here[1]) - float(there[1])) <= tolerance
        rows.append({
            "rel": rel,
            "status": "same" if (same_size and close_enough) else "differs",
            "left_size": int(here[0]),
            "right_size": int(there[0]),
        })
    return rows
def summarize_compare_plan(rows) -> dict:
    """Count a compare plan by status."""
    counts = {"only_left": 0, "only_right": 0, "differs": 0, "same": 0}
    for row in rows or []:
        status = row.get("status", "same")
        counts[status] = counts.get(status, 0) + 1
    return counts
def summarize_sync_plan(actions) -> dict:
    """Count actions by kind and total the bytes that would move."""
    counts = {}
    total_bytes = 0
    for entry in actions or []:
        kind = entry.get("action", "skip")
        counts[kind] = counts.get(kind, 0) + 1
        if kind in ("upload", "download"):
            total_bytes += int(entry.get("size") or 0)
    counts["bytes"] = total_bytes
    return counts
