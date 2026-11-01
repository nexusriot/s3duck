"""Inventory reports and S3 Select, against a server.

MinIO does not *produce* inventory reports, so the report here is written by
hand in the layout S3 delivers (manifest.json plus gzipped CSV under
``prefix/source-bucket/config-id/date/``); what is being tested is that the
app finds and reads one through a real client. S3 Select is a capability
check: a backend may answer with rows or refuse with a code, and either has
to reach the user as something they can act on.
"""

import gzip
import io
import json

import botocore.exceptions

from tests.e2e.harness import ServerCase


class InventoryTests(ServerCase):

    SCHEMA = "Bucket, Key, Size, ETag, StorageClass"

    def _deliver(self, config_id="daily", stamp="2026-10-27T01-00Z"):
        rows = [(self.bucket, "data/a.txt", 10, "e1", "STANDARD"),
                (self.bucket, "data/copy+of+a.txt", 10, "e1", "STANDARD"),
                (self.bucket, "data/b.bin", 99, "e2", "STANDARD_IA")]
        body = io.StringIO()
        for row in rows:
            body.write(",".join(f'"{v}"' for v in row) + "\n")
        data_key = f"inv/data/{stamp}.csv.gz"
        self.put(data_key, gzip.compress(body.getvalue().encode()))
        manifest_key = f"inv/{self.bucket}/{config_id}/{stamp}/manifest.json"
        self.put(manifest_key, json.dumps({
            "sourceBucket": self.bucket,
            "destinationBucket": f"arn:aws:s3:::{self.bucket}",
            "fileFormat": "CSV", "fileSchema": self.SCHEMA,
            "creationTimestamp": "1761526800000",
            "files": [{"key": data_key, "size": 1}]}).encode())
        return manifest_key

    def test_the_latest_report_is_found_and_read(self):
        self._deliver(stamp="2026-10-20T01-00Z")
        newest = self._deliver(stamp="2026-10-27T01-00Z")
        config = {"id": "daily", "destination_bucket": self.bucket,
                  "destination_prefix": "inv"}
        self.assertEqual(self.model.find_latest_inventory(config), newest)
        report = self.model.load_inventory(self.bucket, newest)
        keys = sorted(row["Key"] for row in report["rows"])
        self.assertEqual(keys, ["data/a.txt", "data/b.bin", "data/copy of a.txt"])

    def test_listing_configurations_answers_or_is_empty(self):
        try:
            configs = self.model.list_inventory_configurations()
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(exc.response.get("Error", {}).get("Code"))
            return
        self.assertEqual(configs, [])


class SelectTests(ServerCase):

    def test_a_query_returns_rows_or_a_code(self):
        self.put("q/people.csv", b"name,age\nann,31\nbob,45\n")
        try:
            result = self.model.select_object(
                "q/people.csv", "SELECT s.name FROM s3object s WHERE "
                "CAST(s.age AS INT) > 40")
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(exc.response.get("Error", {}).get("Code"),
                            "S3 Select refused with nothing to show")
            return
        self.assertEqual(result["text"].strip(), "bob")
