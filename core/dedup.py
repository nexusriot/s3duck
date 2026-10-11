"""Finding objects that hold the same bytes, and choosing which to drop."""

from core.naming import _to_epoch


def _etag_is_comparable(etag) -> bool:
    """
    True when an ETag pins the content on its own.

    A single-part upload's ETag is the MD5 of the body. A multipart ETag is
    '<md5-of-part-md5s>-<n>': still deterministic, so two objects sharing one
    are identical — but two objects with *different* multipart ETags may still
    hold the same bytes if they were uploaded with different part sizes.
    """
    return bool(etag) and "-" not in etag
def find_duplicate_groups(entries, *, min_size=1) -> list:
    """
    Group objects that hold the same content.

    ``entries`` is an iterable of ``(key, size, etag, last_modified)``.
    Returns groups sorted by reclaimable bytes, each a dict of
    ``{size, etag, members, count, wasted, confirmed}`` where members are
    ``(key, last_modified)`` sorted by key.

    Objects sharing a size *and* an ETag are confirmed duplicates. Objects
    sharing only a size are reported as unconfirmed **only** when their ETags
    cannot settle it (a multipart or missing ETag is involved); two distinct
    plain-MD5 ETags prove the contents differ, so those are never grouped.
    Zero-byte objects are excluded by default — every empty file matches every
    other, which is noise rather than a finding.
    """
    by_size = {}
    for entry in entries or []:
        key, size, etag, modified = (list(entry) + [None] * 4)[:4]
        if not key or str(key).endswith("/"):
            continue
        size = int(size or 0)
        if size < int(min_size):
            continue
        by_size.setdefault(size, []).append(
            (str(key), (etag or "").replace('"', "").strip(), modified))

    groups = []
    for size, rows in by_size.items():
        by_etag = {}
        for key, etag, modified in rows:
            by_etag.setdefault(etag, []).append((key, modified))

        leftovers = []
        for etag, members in by_etag.items():
            if etag and len(members) >= 2:
                groups.append({
                    "size": size,
                    "etag": etag,
                    "members": sorted(members, key=lambda m: m[0]),
                    "count": len(members),
                    "wasted": size * (len(members) - 1),
                    "confirmed": True,
                })
            else:
                leftovers.extend((key, etag, modified)
                                 for key, modified in members)

        # Same size, unresolved ETags: only a real candidate when at least one
        # side cannot be compared by ETag at all.
        if len(leftovers) >= 2 and any(
                not _etag_is_comparable(etag) for _k, etag, _m in leftovers):
            members = sorted(((key, modified) for key, _e, modified in leftovers),
                             key=lambda m: m[0])
            groups.append({
                "size": size,
                "etag": "",
                "members": members,
                "count": len(members),
                "wasted": size * (len(members) - 1),
                "confirmed": False,
            })

    groups.sort(key=lambda g: (-g["wasted"], -g["size"], g["members"][0][0]))
    return groups
def summarize_duplicate_groups(groups) -> dict:
    """Totals for the duplicate report header."""
    groups = list(groups or [])
    confirmed = [g for g in groups if g.get("confirmed")]
    return {
        "groups": len(groups),
        "confirmed_groups": len(confirmed),
        "redundant": sum(g["count"] - 1 for g in groups),
        "wasted": sum(g["wasted"] for g in confirmed),
    }
def select_redundant_keys(groups, keep="newest") -> set:
    """
    Which keys to delete so one copy of each group survives.

    keep is "newest" or "oldest" by last-modified; entries without a timestamp
    sort oldest so a dated copy is preferred as the survivor. Unconfirmed
    groups are never auto-selected — their members are not proven identical.
    """
    if keep not in ("newest", "oldest"):
        raise ValueError("keep must be 'newest' or 'oldest'")
    chosen = set()
    for group in groups or []:
        if not group.get("confirmed"):
            continue
        members = list(group.get("members") or [])
        if len(members) < 2:
            continue
        ordered = sorted(
            members,
            key=lambda m: (m[1] is not None, _to_epoch(m[1]), m[0]),
        )
        survivor = ordered[-1] if keep == "newest" else ordered[0]
        for key, _modified in members:
            if key != survivor[0]:
                chosen.add(key)
    return chosen
