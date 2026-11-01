"""Reading what the service already computed: inventory reports, and S3
Select queries run next to the data.
"""

import botocore.exceptions

from core.errors import TransferCancelled
from core.inventory import (
    latest_manifest_key, manifest_search_prefix, parse_manifest,
    read_inventory_file, summarize_configuration,
)

# Formats S3 Select reads; the preview offers a query only for these.
SELECT_FORMATS = {".csv": "csv", ".tsv": "tsv", ".json": "json",
                  ".jsonl": "lines", ".ndjson": "lines"}


def trim_partial_utf8(data) -> bytes:
    """*data* without a multi-byte character the byte cap cut in half."""
    for cut in range(min(3, len(data)) + 1):
        candidate = data[:len(data) - cut] if cut else data
        try:
            candidate.decode("utf-8")
        except UnicodeDecodeError as exc:
            if exc.start < len(candidate) - 3:
                return data
            continue
        return candidate
    return data


class ReportsMixin:
    """Inventory reports and S3 Select."""

    def list_inventory_configurations(self) -> list:
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        out, token = [], None
        while True:
            params = {"Bucket": self.bucket}
            if token:
                params["ContinuationToken"] = token
            try:
                resp = self.client.list_bucket_inventory_configurations(**params)
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in self.ABSENT_DOCUMENT_CODES:
                    return []
                raise
            out.extend(summarize_configuration(c) for c in
                       resp.get("InventoryConfigurationList") or [])
            if not resp.get("IsTruncated"):
                return out
            token = resp.get("NextContinuationToken")

    def find_latest_inventory(self, config) -> str:
        """The newest manifest key a configuration has delivered, or ""."""
        bucket = config.get("destination_bucket") or ""
        if not bucket:
            return ""
        prefix = manifest_search_prefix(config, self.bucket)
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents") or [])
        return latest_manifest_key(keys)

    def load_inventory(self, bucket, manifest_key, cancel_event=None,
                       log_fn=None) -> dict:
        """``{"manifest", "rows"}`` for one delivered report (CSV only)."""
        body = self.client.get_object(
            Bucket=bucket, Key=manifest_key)["Body"].read()
        manifest = parse_manifest(body.decode("utf-8"))
        if manifest["format"].upper() != "CSV":
            raise ValueError(
                f"This report is {manifest['format'] or 'in an unknown format'}"
                "; only CSV inventory reports can be read here.")
        rows = []
        for index, key in enumerate(manifest["files"], 1):
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            if log_fn:
                log_fn(f"inventory file {index}/{len(manifest['files'])}: {key}")
            data = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
            rows.extend(read_inventory_file(
                data, manifest["schema"], gzipped=key.endswith(".gz")))
        return {"manifest": manifest, "rows": rows}

    def select_object(self, key, sql, input_format="csv", csv_header=True,
                      max_bytes=1024 * 1024) -> dict:
        """
        Run *sql* over one CSV or JSON object; ``{"text", "scanned", "truncated"}``.

        The result is returned as CSV for CSV input and as JSON lines for JSON
        input, and is cut off at *max_bytes* -- a ``SELECT *`` over a 40 GB
        log is still 40 GB of answer.
        """
        if not self.bucket:
            raise ValueError("Bucket is empty; select a bucket first")
        if input_format in ("csv", "tsv"):
            serialization = {"CSV": {
                "FileHeaderInfo": "USE" if csv_header else "NONE",
                "FieldDelimiter": "\t" if input_format == "tsv" else ","}}
            output = {"CSV": {}}
        else:
            serialization = {"JSON": {
                "Type": "LINES" if input_format == "lines" else "DOCUMENT"}}
            output = {"JSON": {}}
        resp = self.client.select_object_content(
            Bucket=self.bucket, Key=key, Expression=sql,
            ExpressionType="SQL", InputSerialization=serialization,
            OutputSerialization=output)
        chunks, size, truncated, scanned = [], 0, False, 0
        stream = resp["Payload"]
        try:
            for event in stream:
                if "Records" in event:
                    payload = event["Records"]["Payload"]
                    if size + len(payload) > max_bytes:
                        chunks.append(payload[:max(0, max_bytes - size)])
                        truncated = True
                        break
                    chunks.append(payload)
                    size += len(payload)
                elif "Stats" in event:
                    scanned = int(
                        event["Stats"]["Details"].get("BytesScanned") or 0)
        finally:
            # Abandoning the stream at the cap would otherwise keep the
            # connection checked out of the pool.
            close = getattr(stream, "close", None)
            if close is not None:
                close()
        data = b"".join(chunks)
        if truncated:
            data = trim_partial_utf8(data)
        return {"text": data.decode("utf-8", errors="replace"),
                "scanned": scanned, "truncated": truncated}
