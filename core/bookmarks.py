"""Saved places: their labels, and the string they are stored as."""

import re


def bookmark_label(bucket, prefix) -> str:
    """Default display name for a saved location."""
    bucket = bucket or ""
    prefix = prefix or ""
    return f"{bucket}/{prefix}" if prefix else (bucket or "/")
def parse_bookmarks(raw) -> list:
    """
    Read the stored bookmark list.

    One tab-separated `name<TAB>bucket<TAB>prefix` record per line; malformed
    lines are dropped rather than losing the whole list.
    """
    out = []
    for line in str(raw or "").splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        name, bucket, prefix = (p.strip() for p in parts)
        if not bucket:
            continue
        out.append({
            "name": name or bookmark_label(bucket, prefix),
            "bucket": bucket,
            "prefix": prefix,
        })
    return out
def serialize_bookmarks(bookmarks) -> str:
    """Inverse of parse_bookmarks. Tabs and newlines in a name are stripped so
    a record can never span lines."""
    lines = []
    for entry in bookmarks or []:
        bucket = (entry.get("bucket") or "").strip()
        if not bucket:
            continue
        name = re.sub(r"[\t\r\n]+", " ", entry.get("name") or "").strip()
        prefix = (entry.get("prefix") or "").strip()
        lines.append("\t".join(
            [name or bookmark_label(bucket, prefix), bucket, prefix]))
    return "\n".join(lines)
def add_bookmark_to(bookmarks, name, bucket, prefix) -> tuple:
    """
    Append a bookmark unless that exact location is already saved.

    Returns ``(new_list, added)`` so the caller can report a duplicate rather
    than silently growing the menu.
    """
    entries = list(bookmarks or [])
    for entry in entries:
        if entry.get("bucket") == bucket and (entry.get("prefix") or "") == (prefix or ""):
            return entries, False
    entries.append({
        "name": name or bookmark_label(bucket, prefix),
        "bucket": bucket,
        "prefix": prefix or "",
    })
    return entries, True
