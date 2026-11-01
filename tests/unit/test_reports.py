"""Inventory reports, S3 Select and the QR encoder."""

import gzip
import hashlib
import io
import json
import unittest

from PyQt6.QtWidgets import QMessageBox

from core import qr
from core.inventory import (
    inventory_duplicates, latest_manifest_key, manifest_search_prefix,
    parse_manifest, read_inventory_file, summarize_configuration,
    summarize_inventory,
)
from tests.support import _ensure_qapp, make_model
from ui.dialogs.presigned import QR_BORDER, QrCodeDialog, qr_image, qr_level_for
from ui.dialogs.reports import InventoryDialog, SelectDialog, select_format
from unittest.mock import patch

SCHEMA = "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass"


def _csv(rows):
    out = io.StringIO()
    for row in rows:
        out.write(",".join(f'"{v}"' for v in row) + "\n")
    return gzip.compress(out.getvalue().encode())


ROWS = [
    ("src", "a/one.txt", "10", "2026-01-01T00:00:00.000Z", "e1", "STANDARD"),
    ("src", "a/two+copy.txt", "10", "2026-01-02T00:00:00.000Z", "e1", "STANDARD"),
    ("src", "b/cold.bin", "100", "2026-01-03T00:00:00.000Z", "e2", "GLACIER"),
]


class InventoryCoreTests(unittest.TestCase):

    def test_configuration_summary_and_paths(self):
        config = summarize_configuration({
            "Id": "daily", "IsEnabled": True,
            "Destination": {"S3BucketDestination": {
                "Bucket": "arn:aws:s3:::reports", "Prefix": "inv/",
                "Format": "CSV"}},
            "Schedule": {"Frequency": "Daily"},
            "OptionalFields": ["Size", "ETag"],
            "IncludedObjectVersions": "Current"})
        self.assertEqual(config["destination_bucket"], "reports")
        self.assertEqual(manifest_search_prefix(config, "src"), "inv/src/daily/")
        self.assertEqual(manifest_search_prefix(
            dict(config, destination_prefix=""), "src"), "src/daily/")

    def test_latest_manifest(self):
        keys = ["p/2026-10-01T01-00Z/manifest.json",
                "p/2026-10-27T01-00Z/manifest.json",
                "p/2026-10-27T01-00Z/manifest.checksum",
                "p/hive/dt=2026-10-28-01-00/manifest.json"]
        self.assertEqual(latest_manifest_key(keys),
                         "p/2026-10-27T01-00Z/manifest.json")
        self.assertEqual(latest_manifest_key([]), "")

    def test_manifest_and_rows(self):
        manifest = parse_manifest(json.dumps({
            "sourceBucket": "src", "destinationBucket": "arn:aws:s3:::reports",
            "fileFormat": "CSV", "fileSchema": SCHEMA,
            "creationTimestamp": "1700000000000",
            "files": [{"key": "data/1.csv.gz"}, {"nokey": 1}]}))
        self.assertEqual(manifest["files"], ["data/1.csv.gz"])
        self.assertEqual(manifest["schema"][1], "Key")
        rows = read_inventory_file(_csv(ROWS), manifest["schema"])
        self.assertEqual(rows[1]["Key"], "a/two copy.txt", "keys are URL-decoded")
        with self.assertRaises(ValueError):
            parse_manifest("{nope")

    def test_summary_and_duplicates(self):
        rows = read_inventory_file(_csv(ROWS), [c.strip() for c in SCHEMA.split(",")])
        summary = summarize_inventory(rows)
        self.assertEqual(summary["objects"], 3)
        self.assertEqual(summary["bytes"], 120)
        self.assertEqual(summary["by_class"]["GLACIER"], {"objects": 1, "bytes": 100})
        self.assertEqual(summarize_inventory(rows, "a/")["objects"], 2)
        dup = inventory_duplicates(rows)["summary"]
        self.assertEqual(dup["groups"], 1)
        self.assertEqual(dup["wasted"], 10)

    def test_versions_reports_count_only_current_objects(self):
        rows = [{"Key": "k", "Size": "5", "IsLatest": "false"},
                {"Key": "k", "Size": "6", "IsLatest": "true"},
                {"Key": "m", "Size": "0", "IsDeleteMarker": "true"}]
        self.assertEqual(summarize_inventory(rows)["bytes"], 6)


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _ReportClient:
    def __init__(self, objects=None, configs=None, events=None, error=None):
        self.objects = objects or {}
        self.configs = configs or []
        self.events = events or []
        self.error = error
        self.calls = []

    def get_object(self, Bucket, Key, **_kw):
        return {"Body": _Body(self.objects[(Bucket, Key)])}

    def list_bucket_inventory_configurations(self, **kw):
        self.calls.append(kw)
        return {"InventoryConfigurationList": self.configs, "IsTruncated": False}

    def get_paginator(self, _name):
        objects = self.objects

        class _Pages:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [{"Key": k} for (b, k) in objects
                                      if b == Bucket and k.startswith(Prefix)]}]
        return _Pages()

    def select_object_content(self, **kw):
        self.calls.append(kw)
        if self.error:
            raise self.error
        return {"Payload": iter(self.events)}


class ReportsModelTests(unittest.TestCase):

    def _model(self, client):
        model = make_model(bucket="src")
        model._client = client
        return model

    def test_load_latest_inventory(self):
        manifest = json.dumps({"sourceBucket": "src", "fileFormat": "CSV",
                               "fileSchema": SCHEMA,
                               "files": [{"key": "inv/data/1.csv.gz"}]}).encode()
        client = _ReportClient(objects={
            ("reports", "inv/src/daily/2026-10-27T01-00Z/manifest.json"): manifest,
            ("reports", "inv/data/1.csv.gz"): _csv(ROWS)},
            configs=[{"Id": "daily", "IsEnabled": True, "Destination": {
                "S3BucketDestination": {"Bucket": "arn:aws:s3:::reports",
                                        "Prefix": "inv", "Format": "CSV"}}}])
        model = self._model(client)
        [config] = model.list_inventory_configurations()
        key = model.find_latest_inventory(config)
        self.assertTrue(key.endswith("manifest.json"))
        report = model.load_inventory("reports", key)
        self.assertEqual(len(report["rows"]), 3)

    def test_a_parquet_report_is_refused(self):
        client = _ReportClient(objects={("r", "m.json"): json.dumps(
            {"fileFormat": "Parquet", "files": []}).encode()})
        with self.assertRaises(ValueError) as caught:
            self._model(client).load_inventory("r", "m.json")
        self.assertIn("only CSV", str(caught.exception))

    def test_select_streams_records_and_caps_them(self):
        events = [{"Records": {"Payload": b"a,1\n"}},
                  {"Records": {"Payload": b"b,2\n"}},
                  {"Stats": {"Details": {"BytesScanned": 99}}}]
        client = _ReportClient(events=events)
        result = self._model(client).select_object("t.csv", "SELECT 1")
        self.assertEqual(result, {"text": "a,1\nb,2\n", "scanned": 99,
                                  "truncated": False})
        request = client.calls[-1]
        self.assertEqual(request["InputSerialization"]["CSV"]["FileHeaderInfo"], "USE")
        capped = self._model(_ReportClient(events=list(events))).select_object(
            "t.csv", "SELECT 1", max_bytes=5)
        self.assertTrue(capped["truncated"])
        self.assertEqual(capped["text"], "a,1\nb")

    def test_select_json_and_tsv(self):
        client = _ReportClient()
        self._model(client).select_object("t.jsonl", "S", input_format="lines")
        self.assertEqual(client.calls[-1]["InputSerialization"],
                         {"JSON": {"Type": "LINES"}})
        self._model(client).select_object("t.tsv", "S", input_format="tsv",
                                          csv_header=False)
        csv_in = client.calls[-1]["InputSerialization"]["CSV"]
        self.assertEqual((csv_in["FieldDelimiter"], csv_in["FileHeaderInfo"]),
                         ("\t", "NONE"))


class QrTests(unittest.TestCase):
    """Golden symbols, checked module for module against the reference
    ``qrcode`` library when this encoder was written, at fixed masks."""

    GOLDEN = (
        ("https://example.com/a", qr.MEDIUM, 2, 25,
         "a4333f7db0bd1e57970ea6398e2f9d609b25b1af72d7557d09a777cb35d3c376"),
        ("x" * 300, qr.LOW, 5, 61,
         "a81c88a14aa3571eeeaa90187d9866805c34f7c432615fa8706eac7c52b34e50"),
        ("https://s3.amazonaws.com/b/k?" + "Q" * 1100, qr.LOW, 0, 113,
         "80eedb38b888cb32286ae82eab17f1628dc2837668c908e185db5f6ccb97e69d"),
    )

    @staticmethod
    def _digest(matrix):
        return hashlib.sha256("".join(
            "1" if c else "0" for row in matrix for c in row).encode()).hexdigest()

    def test_golden_symbols(self):
        for text, ecl, mask, size, digest in self.GOLDEN:
            with self.subTest(size=size):
                matrix = qr.encode(text, ecl, mask=mask)
                self.assertEqual(len(matrix), size)
                self.assertEqual(self._digest(matrix), digest)

    def test_finder_patterns_and_auto_mask(self):
        matrix = qr.encode("hello")
        self.assertEqual(len(matrix), 21)
        for x0, y0 in ((0, 0), (14, 0), (0, 14)):
            ring = [matrix[y0][x0 + i] for i in range(7)]
            self.assertEqual(ring, [True] * 7)
            self.assertFalse(matrix[y0 + 1][x0 + 1])
            self.assertTrue(matrix[y0 + 3][x0 + 3])

    def test_capacity(self):
        self.assertEqual(qr.byte_capacity(1, qr.LOW), 17)
        self.assertEqual(qr.byte_capacity(40, qr.LOW), 2953)
        self.assertEqual(qr.choose_version(17, qr.LOW), 1)
        self.assertEqual(qr.choose_version(18, qr.LOW), 2)
        with self.assertRaises(ValueError):
            qr.choose_version(3000, qr.LOW)


class ReportDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_qr_image_has_a_quiet_zone(self):
        image = qr_image("https://example.com/a", scale=1)
        self.assertEqual(image.width(), 25 + 2 * QR_BORDER)
        self.assertEqual(image.pixelColor(0, 0).name(), "#ffffff")
        self.assertEqual(image.pixelColor(QR_BORDER, QR_BORDER).name(), "#000000")
        self.assertEqual(qr_level_for("short"), qr.MEDIUM)
        self.assertEqual(qr_level_for("x" * 1500), qr.LOW)

    def test_qr_dialog_explains_a_link_too_long(self):
        dlg = QrCodeDialog(None, "x" * 4000)
        self.assertIsNone(dlg.image.pixmap().toImage().bits())

    def test_select_format(self):
        self.assertEqual(select_format("a/b.CSV"), "csv")
        self.assertEqual(select_format("log.ndjson"), "lines")
        self.assertEqual(select_format("noext"), "")
        self.assertEqual(select_format("img.png"), "")

    def test_select_dialog_reports_an_unsupported_server(self):
        class _Model:
            bucket = "b"

            def clone_for_worker(self):
                return self

        dlg = SelectDialog(None, _Model(), "data.csv")
        with patch("ui.dialogs.reports.run_with_progress",
                   return_value=(None, RuntimeError("NotImplemented"))):
            self.assertFalse(dlg.run())
        self.assertIn("does not implement S3 Select", dlg.status.text())
        with patch("ui.dialogs.reports.run_with_progress",
                   return_value=({"text": "a,b", "scanned": 2048,
                                  "truncated": True}, None)):
            self.assertTrue(dlg.run())
        self.assertEqual(dlg.output.toPlainText(), "a,b")
        self.assertIn("cut off", dlg.status.text())

    def test_inventory_dialog_renders_a_report(self):
        class _Model:
            bucket = "src"
            current_folder = ""

            def clone_for_worker(self):
                return self

        rows = read_inventory_file(_csv(ROWS), [c.strip() for c in SCHEMA.split(",")])
        report = {"rows": rows, "manifest": {
            "created": "today", "source_bucket": "src",
            "schema": [c.strip() for c in SCHEMA.split(",")]}}
        dlg = InventoryDialog(None, _Model(), [])
        self.assertIn("No inventory configuration", dlg.status.text())
        with patch("ui.dialogs.reports.run_with_progress",
                   return_value=(report, None)):
            self.assertTrue(dlg.load("reports", "m.json"))
        self.assertIn("3 object(s)", dlg.status.text())
        self.assertEqual(dlg.table.rowCount(), 2)
        self.assertIn("1 group(s)", dlg.duplicates.text())
        with patch("ui.dialogs.reports.run_with_progress",
                   return_value=(None, RuntimeError("AccessDenied"))), \
                patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(dlg.load("reports", "m.json"))
        warn.assert_called_once()
        self.assertFalse(dlg.load("", ""))


if __name__ == "__main__":
    unittest.main()
