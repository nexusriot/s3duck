"""Comparing two trees and deciding what has to move.

Two planners over the same ``{rel: (size, mtime)}`` trees: build_sync_plan is
directional (local sync either way, the watch folder, and sync to another
profile), build_compare_plan is direction-free (the pane comparison).
"""

import fnmatch
import os
import re
from core.errors import TransferCancelled
from core.integrity import (
    etag_is_md5, etag_is_opaque, file_matches_etag, normalize_etag,
)
from core.parts import upload_part_size


# Filesystem and S3 timestamps disagree by up to a couple of seconds
# (second resolution remotely, 2s granularity on some local filesystems), so
# only a larger difference counts as "newer".
SYNC_MTIME_TOLERANCE_SEC = 2.0


# Hashing a local file is the only way to put it against a remote ETag, and a
# pane comparison should not sit reading one enormous file for minutes. A
# file over this is reported "unverified" rather than guessed at either way.
COMPARE_HASH_LIMIT = 512 * 1024 * 1024
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
def listed_etag(entry) -> str:
    """The ETag a tree entry carries as its third element; "" for none."""
    return normalize_etag(entry[2]) if len(entry) > 2 else ""
def compare_listed_etags(first, second):
    """
    What two listed ETags say about content: True (the same bytes), False
    (different bytes) or None (they cannot tell).

    Equal ETags of any form mean the same bytes -- a multipart one pins every
    part's MD5, so two that match are the same content split the same way.
    Unequal ones prove a difference only when both are plain MD5s: one file
    uploaded twice at different part sizes gets two different multipart
    ETags. A listing cannot see encryption, though, and under SSE-KMS or
    SSE-C a 32-hex ETag is no digest of the bytes, so False here is still a
    claim for whoever holds a client to confirm (see build_compare_plan).
    """
    first, second = normalize_etag(first), normalize_etag(second)
    if not first or not second:
        return None
    if first == second:
        return True
    if etag_is_md5(first) and etag_is_md5(second):
        return False
    return None
def build_compare_plan(left, right, *, tolerance=SYNC_MTIME_TOLERANCE_SEC,
                       exclude=None, content_check=None) -> list:
    """
    Diff two ``{rel_path: (size, mtime[, etag])}`` maps without picking a
    direction.

    Returns ``[{rel, status, left_size, right_size}]`` where status is
    "only_left", "only_right", "differs", "unverified" or "same". Direction-
    free on purpose: the same comparison drives a copy either way, and
    calling one side "the source" before the user has said so is how a
    compare turns into a sync nobody asked for.

    Timestamps say little about a pair here. S3's LastModified is when the
    object was written and a download does not set the local mtime, so a
    folder compared with the prefix it was just copied to -- or a prefix
    with its server-side copy -- disagrees in time on every file, and judging
    by size and mtime called all of them "differs". A same-size pair is
    settled on content wherever that is possible instead: by the ETags when
    both entries carry one (a remote listing does), else by
    ``content_check(rel, here, there)``, the caller's True/False/None --
    hashing a local file against the remote ETag, or confirming with a HEAD
    that a False from compare_listed_etags is not an encrypted object's
    opaque ETag. Only when nothing can tell does time decide: "same" within
    *tolerance* (the size-and-mtime rule build_sync_plan still uses), and
    otherwise "unverified" -- same size, different time, content unknown --
    rather than a "differs" nothing has shown.
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
        if int(here[0]) != int(there[0]):
            status = "differs"
        else:
            status = _same_size_status(rel, here, there, tolerance,
                                       content_check)
        rows.append({
            "rel": rel,
            "status": status,
            "left_size": int(here[0]),
            "right_size": int(there[0]),
        })
    return rows
def _same_size_status(rel, here, there, tolerance, content_check) -> str:
    """build_compare_plan's verdict on two entries of equal size."""
    if int(here[0]) == 0:
        return "same"           # every empty file holds the same bytes
    close = abs(float(here[1]) - float(there[1])) <= tolerance
    verdict = compare_listed_etags(listed_etag(here), listed_etag(there))
    if verdict is None and close:
        # Nothing cheap to compare and the clocks agree: the old rule, which
        # spares hashing a local file that has not been touched since.
        return "same"
    if verdict is not True and content_check is not None:
        verdict = content_check(rel, here, there)
    if verdict is True:
        return "same"
    if verdict is False:
        return "differs"
    return "same" if close else "unverified"
def make_content_check(left, right, models, *, part_sizes=(), cancel=None):
    """
    build_compare_plan's ``content_check`` for two pane sides, each
    ``("local", path)`` or ``("remote", bucket, prefix)``.

    Local against remote hashes the file against the listed ETag; remote
    against remote has nothing to add to what the listing already said.
    Either way a "they differ" resting on a plain-looking ETag is confirmed
    with a HEAD of each remote object first: a listing cannot see
    encryption, and under SSE-KMS or SSE-C that ETag is no digest of the
    bytes, so every intact copy of such an object would otherwise be
    reported changed.

    *models* holds a model bound to each remote side's bucket (None for a
    local side). *part_sizes* are the multipart chunk sizes uploads may have
    used -- a multipart ETag can only be rebuilt at the right one. *cancel*
    stops the check between files.
    """
    sides = (left, right)
    remote = [index for index in (0, 1) if sides[index][0] == "remote"]

    def _check(rel, here, there):
        if cancel is not None and cancel.is_set():
            raise TransferCancelled("cancelled")
        entries = (here, there)
        if len(remote) == 2:
            verdict = compare_listed_etags(listed_etag(here),
                                           listed_etag(there))
        elif len(remote) == 1:
            theirs = remote[0]
            local_root = sides[1 - theirs][1]
            verdict = _local_verdict(
                os.path.join(local_root, rel.replace("/", os.sep)),
                entries[theirs], part_sizes)
        else:
            return None
        if verdict is False and not all(
                _etag_is_digest(models[index], (sides[index][2] or "") + rel)
                for index in remote):
            return None
        return verdict

    return _check
def _local_verdict(path, entry, part_sizes):
    """
    Whether the local file at *path* holds the bytes of the remote *entry*
    ``(size, mtime, etag)``; None when that cannot be told cheaply.

    Each chunk size is raised to fit S3's 10 000 parts the way uploads raise
    it, so the guess is the part size the upload really used.
    """
    size = int(entry[0])
    etag = listed_etag(entry)
    if not etag or size > COMPARE_HASH_LIMIT:
        return None
    guesses = [upload_part_size(size, chunk) for chunk in part_sizes if chunk]
    try:
        return file_matches_etag(path, etag, guesses)
    except OSError:
        return None
def _etag_is_digest(model, key) -> bool:
    """Whether a HEAD shows *key*'s ETag is a digest of its bytes."""
    try:
        head = model.client.head_object(Bucket=model.bucket, Key=key)
    except Exception:
        # SSE-C refuses a HEAD without its key, and its ETag is opaque
        # anyway; any other failure leaves the question open too, and an
        # open question is not a "differs".
        return False
    return not etag_is_opaque(head)
def summarize_compare_plan(rows) -> dict:
    """Count a compare plan by status."""
    counts = {"only_left": 0, "only_right": 0, "differs": 0,
              "unverified": 0, "same": 0}
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
