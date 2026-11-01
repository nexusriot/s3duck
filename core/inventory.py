"""S3 Inventory reports: finding the latest one, and reading it.

Listing a bucket with a hundred million objects is a hundred thousand
requests. S3 Inventory writes the same facts out once a day or week as CSV
files plus a ``manifest.json`` naming them, so the usage report and the
duplicate finder can be fed from a handful of GETs instead. Only the CSV
format is read; ORC and Parquet need libraries this app does not ship.
"""

import csv
import gzip
import io
import json
from urllib.parse import unquote_plus

from core.dedup import find_duplicate_groups, summarize_duplicate_groups


def summarize_configuration(raw) -> dict:
    """One ListBucketInventoryConfigurations entry as the dialog shows it."""
    raw = raw or {}
    dest = (raw.get("Destination") or {}).get("S3BucketDestination") or {}
    arn = str(dest.get("Bucket") or "")
    return {
        "id": str(raw.get("Id") or ""),
        "enabled": bool(raw.get("IsEnabled")),
        "destination_bucket": arn.rsplit(":", 1)[-1] if arn else "",
        "destination_prefix": str(dest.get("Prefix") or ""),
        "format": str(dest.get("Format") or ""),
        "frequency": str((raw.get("Schedule") or {}).get("Frequency") or ""),
        "fields": list(raw.get("OptionalFields") or []),
        "versions": str(raw.get("IncludedObjectVersions") or ""),
    }


def manifest_search_prefix(config, source_bucket) -> str:
    """Where a configuration's manifests live: prefix/source-bucket/config-id/."""
    prefix = str(config.get("destination_prefix") or "").strip("/")
    parts = [p for p in (prefix, source_bucket, config.get("id")) if p]
    return "/".join(parts) + "/"


def latest_manifest_key(keys) -> str:
    """
    The newest ``<date>/manifest.json`` among *keys*, or "".

    The date folders are ISO-like (``2026-10-27T01-00Z``), so they sort as
    text; the ``hive/`` symlink tree beside them holds no manifest.
    """
    manifests = sorted(k for k in keys or []
                       if str(k).endswith("/manifest.json") and "/hive/" not in str(k))
    return manifests[-1] if manifests else ""


def parse_manifest(text) -> dict:
    """``{"source_bucket", "destination_bucket", "format", "schema", "files"}``."""
    try:
        data = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"The manifest is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("The manifest is not a JSON object.")
    fmt = str(data.get("fileFormat") or "")
    schema = [c.strip() for c in str(data.get("fileSchema") or "").split(",")
              if c.strip()]
    files = [str(f.get("key") or "") for f in data.get("files") or []
             if isinstance(f, dict) and f.get("key")]
    dest = str(data.get("destinationBucket") or "")
    return {
        "source_bucket": str(data.get("sourceBucket") or ""),
        "destination_bucket": dest.rsplit(":", 1)[-1] if dest else "",
        "format": fmt,
        "schema": schema,
        "files": files,
        "created": str(data.get("creationTimestamp") or ""),
    }


def read_inventory_file(body, schema, gzipped=True) -> list:
    """
    Rows of one CSV data file as dicts keyed by the schema's column names.

    Keys are URL-encoded in the report (a space is ``+``); they are decoded
    here so they compare with keys from a listing.
    """
    if gzipped:
        body = gzip.decompress(body)
    text = body.decode("utf-8")
    rows = []
    for values in csv.reader(io.StringIO(text)):
        if not values:
            continue
        row = dict(zip(schema, values))
        if "Key" in row:
            row["Key"] = unquote_plus(row["Key"])
        rows.append(row)
    return rows


def _size(row):
    try:
        return int(row.get("Size") or 0)
    except ValueError:
        return 0


def summarize_inventory(rows, prefix="") -> dict:
    """
    Totals a usage report needs: objects, bytes, and both by storage class.

    Delete markers and noncurrent versions (in a versions report) are left
    out of the totals, because the listing-based report counts only current
    objects and the two must agree.
    """
    count = total = 0
    by_class = {}
    for row in rows or []:
        key = row.get("Key") or ""
        if prefix and not key.startswith(prefix):
            continue
        if str(row.get("IsDeleteMarker", "")).lower() == "true":
            continue
        if str(row.get("IsLatest", "true")).lower() == "false":
            continue
        size = _size(row)
        count += 1
        total += size
        cls = row.get("StorageClass") or "STANDARD"
        bucket = by_class.setdefault(cls, [0, 0])
        bucket[0] += 1
        bucket[1] += size
    return {"objects": count, "bytes": total,
            "by_class": {k: {"objects": v[0], "bytes": v[1]}
                         for k, v in sorted(by_class.items())}}


def inventory_duplicates(rows, prefix="") -> dict:
    """The duplicate finder's summary from an inventory with ETag and Size."""
    entries = []
    for row in rows or []:
        key = row.get("Key") or ""
        if prefix and not key.startswith(prefix):
            continue
        if str(row.get("IsLatest", "true")).lower() == "false":
            continue
        entries.append((key, _size(row), row.get("ETag") or "",
                        row.get("LastModifiedDate") or ""))
    groups = find_duplicate_groups(entries)
    return {"groups": groups, "summary": summarize_duplicate_groups(groups)}
