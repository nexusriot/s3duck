"""The manifest file: writing a listing down, and checking it later."""

import csv
import io


MANIFEST_HEADER = "# s3duck-manifest v1"
MANIFEST_COLUMNS = ("key", "size", "etag", "modified")
def build_manifest(entries, bucket="", prefix="", stamp="") -> str:
    """
    A CSV record of what a prefix held, from one listing.

    Size and ETag come back with ListObjectsV2, so a manifest of a million
    objects costs no more than browsing them — which is the point: an
    integrity record nobody can afford to take is not taken.
    """
    buffer = io.StringIO()
    buffer.write(f"{MANIFEST_HEADER}\n")
    buffer.write(f"# bucket: {bucket}\n")
    buffer.write(f"# prefix: {prefix}\n")
    buffer.write(f"# taken: {stamp}\n")
    writer = csv.writer(buffer)
    writer.writerow(MANIFEST_COLUMNS)
    for entry in sorted(entries or [], key=lambda row: str(row[0])):
        key, size, etag, modified = (list(entry) + [None] * 4)[:4]
        writer.writerow([str(key), int(size or 0), str(etag or ""),
                         "" if modified is None else str(modified)])
    return buffer.getvalue()
def parse_manifest(text) -> tuple:
    """
    Read a manifest back as ``(meta, {key: (size, etag)})``.

    Raises ValueError on anything that is not one of ours — verifying against
    an arbitrary CSV would report every object as changed.
    """
    raw = str(text or "")
    if not raw.startswith(MANIFEST_HEADER):
        raise ValueError("This is not an s3duck manifest.")
    meta = {}
    # Only the LEADING comment block is metadata, and the CSV body is handed
    # to the reader whole rather than pre-split into lines. Splitting first
    # tore a record whose key contains a newline in half, and treating any
    # later '#' line as metadata swallowed a key that simply starts with one
    # — both made verification report the object as missing AND added.
    _header_line, _sep, rest = raw.partition("\n")
    while rest.startswith("#"):
        line, sep, following = rest.partition("\n")
        label, _, value = line[1:].partition(":")
        meta[label.strip()] = value.strip()
        rest = following
        if not sep:
            break
    if not rest.strip():
        raise ValueError("The manifest has no entries.")
    reader = csv.reader(io.StringIO(rest))
    header = next(reader, [])
    if [column.strip() for column in header] != list(MANIFEST_COLUMNS):
        raise ValueError("The manifest columns are not the expected ones.")
    records = {}
    for row in reader:
        if len(row) < 3 or not row[0]:
            continue
        try:
            size = int(row[1])
        except (TypeError, ValueError):
            continue
        records[row[0]] = (size, str(row[2] or ""))
    return meta, records
def compare_manifest(stored, current) -> dict:
    """
    Diff a manifest against a fresh listing.

    Returns ``{"missing", "added", "changed", "same"}`` — changed carries
    ``(key, reason)`` so "the size is the same but the content is not" reads
    differently from "it grew".
    """
    stored = dict(stored or {})
    current = dict(current or {})
    missing = sorted(set(stored) - set(current))
    added = sorted(set(current) - set(stored))
    changed = []
    same = 0
    for key in sorted(set(stored) & set(current)):
        was_size, was_etag = stored[key]
        now_size, now_etag = current[key]
        if int(was_size) != int(now_size):
            changed.append((key, f"size {was_size} → {now_size}"))
        elif was_etag and now_etag and was_etag != now_etag:
            changed.append((key, "same size, different content"))
        else:
            same += 1
    return {"missing": missing, "added": added, "changed": changed,
            "same": same}
