"""
Unit tests for pure-logic helpers (no network / no QApplication needed).

Run with:  python -m unittest discover -s tests   (from the project root)
or:        .venv/bin/python -m unittest discover -s tests

Several tests are explicit regression guards for previously fixed bugs and
are marked with "REGRESSION:" in their docstrings.
"""

import io
import os
import sys
import unittest
from datetime import datetime, timezone, timedelta

# Widget tests need a Qt platform plugin; use the headless one before any Qt
# import so the suite runs without a display.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import botocore.exceptions

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import str_to_bool
from main_window import _to_epoch, categorize_key, _human_bytes, _scaled_bar_values
from model import Model


def _ensure_qapp():
    """Return the shared QApplication, creating it once (offscreen)."""
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


class _FakePaginator:
    """Minimal boto3-style paginator returning preset pages."""

    def __init__(self, pages):
        self._pages = list(pages or [])

    def paginate(self, **_kwargs):
        return list(self._pages)


class FakeS3Client:
    """
    Records every call and returns canned responses. Enough of the boto3 S3
    surface to exercise the model's version / storage-class / preview paths
    fully offline.
    """

    def __init__(self, *, versions_pages=None, list_pages=None,
                 get_object_resp=None, head_object_resp=None,
                 restore_error=None):
        self._paginators = {
            "list_object_versions": _FakePaginator(versions_pages),
            "list_objects_v2": _FakePaginator(list_pages),
        }
        self._get_object_resp = get_object_resp
        self._head_object_resp = head_object_resp or {}
        self._restore_error = restore_error
        self.calls = []

    def get_paginator(self, name):
        self.calls.append(("get_paginator", {"name": name}))
        return self._paginators[name]

    def copy_object(self, **kw):
        self.calls.append(("copy_object", kw))
        return {}

    def delete_object(self, **kw):
        self.calls.append(("delete_object", kw))
        return {}

    def delete_objects(self, **kw):
        self.calls.append(("delete_objects", kw))
        return {}

    def delete_bucket(self, **kw):
        self.calls.append(("delete_bucket", kw))
        return {}

    def download_file(self, *a, **kw):
        self.calls.append(("download_file", {"args": a, "kwargs": kw}))

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        return self._head_object_resp

    def get_object(self, **kw):
        self.calls.append(("get_object", kw))
        if self._get_object_resp is None:
            raise AssertionError("get_object was not configured for this test")
        return self._get_object_resp

    def get_bucket_versioning(self, **kw):
        self.calls.append(("get_bucket_versioning", kw))
        return {"Status": "Enabled"}

    def put_bucket_versioning(self, **kw):
        self.calls.append(("put_bucket_versioning", kw))
        return {}

    def generate_presigned_url(self, op, Params=None, ExpiresIn=None):
        self.calls.append(("generate_presigned_url",
                           {"op": op, "Params": Params, "ExpiresIn": ExpiresIn}))
        return f"https://example.test/{op}?exp={ExpiresIn}"

    def restore_object(self, **kw):
        self.calls.append(("restore_object", kw))
        if self._restore_error is not None:
            raise self._restore_error
        return {}

    def calls_of(self, method):
        return [kw for (name, kw) in self.calls if name == method]


def _dt(day):
    return datetime(2026, 1, day, 12, 0, 0, tzinfo=timezone.utc)


def make_model(**overrides):
    """A Model is safe to construct offline; __init__ only builds a boto3
    Session object and does not perform any network I/O."""
    kw = dict(
        endpoint_url="https://s3.amazonaws.com",
        region_name="us-east-1",
        access_key="AK",
        secret_key="SK",
        bucket="",
        no_ssl_check=False,
        use_path=False,
    )
    kw.update(overrides)
    return Model(**kw)


class StrToBoolTests(unittest.TestCase):
    def test_true_values(self):
        self.assertIs(str_to_bool("true"), True)
        self.assertIs(str_to_bool("True"), True)
        self.assertIs(str_to_bool("TRUE"), True)

    def test_false_values(self):
        self.assertIs(str_to_bool("false"), False)
        self.assertIs(str_to_bool("False"), False)

    def test_garbage_and_none_default_false(self):
        self.assertIs(str_to_bool("yes"), False)
        self.assertIs(str_to_bool(""), False)
        self.assertIs(str_to_bool(None), False)
        self.assertIs(str_to_bool(1), False)


class ToEpochTests(unittest.TestCase):
    def test_none_and_empty(self):
        self.assertEqual(_to_epoch(None), 0)
        self.assertEqual(_to_epoch(""), 0)
        self.assertEqual(_to_epoch("   "), 0)

    def test_numeric_passthrough(self):
        self.assertEqual(_to_epoch(1700000000), 1700000000)
        self.assertEqual(_to_epoch(1700000000.9), 1700000000)

    def test_datetime_object(self):
        dt = datetime(2026, 2, 8, 18, 59, 33, tzinfo=timezone.utc)
        self.assertEqual(_to_epoch(dt), int(dt.timestamp()))

    def test_plain_formats(self):
        self.assertGreater(_to_epoch("2026-02-08 18:59:33"), 0)
        self.assertGreater(_to_epoch("2026-02-08T18:59:33"), 0)
        self.assertGreater(_to_epoch("2026-02-08 18:59:33.123"), 0)

    def test_garbage_string(self):
        self.assertEqual(_to_epoch("not-a-date"), 0)

    def test_tz_aware_string_is_parsed(self):
        """REGRESSION: boto3 LastModified stringified as
        '2026-02-08 18:59:33+00:00' previously returned 0 (every file
        compared equal), so the Modified column would not sort."""
        self.assertGreater(_to_epoch("2026-02-08 18:59:33+00:00"), 0)
        self.assertGreater(_to_epoch("2026-02-08 18:59:33.123456+00:00"), 0)
        self.assertGreater(_to_epoch("2026-02-08T18:59:33Z"), 0)

    def test_tz_aware_strings_order_correctly(self):
        """REGRESSION: the Modified-column sort must produce a strict
        ordering for real S3-style timestamps."""
        older = str(datetime(2026, 2, 8, 18, 59, 33, tzinfo=timezone.utc))
        newer = str(datetime(2026, 2, 8, 19, 0, 0, tzinfo=timezone.utc))
        self.assertLess(_to_epoch(older), _to_epoch(newer))

    def test_tz_offset_respected(self):
        a = _to_epoch("2026-02-08 18:59:33+00:00")
        b = _to_epoch("2026-02-08 18:59:33+01:00")  # same wall clock, 1h earlier
        self.assertEqual(a - b, 3600)


class CategorizeKeyTests(unittest.TestCase):
    def test_documents(self):
        for k in ("a/b/report.pdf", "x.DOCX", "notes.txt", "data.csv"):
            self.assertEqual(categorize_key(k), "Documents", k)

    def test_media(self):
        for k in ("img.JPG", "clip.mp4", "song.flac", "a/b/pic.png"):
            self.assertEqual(categorize_key(k), "Media", k)

    def test_other(self):
        self.assertEqual(categorize_key("archive.zip"), "Other")
        self.assertEqual(categorize_key("no_extension"), "Other")
        self.assertEqual(categorize_key(""), "Other")
        self.assertEqual(categorize_key(None), "Other")


class HumanBytesTests(unittest.TestCase):
    def test_zero_and_none(self):
        self.assertEqual(_human_bytes(0), "0.0 B")
        self.assertEqual(_human_bytes(None), "0.0 B")

    def test_scaling(self):
        self.assertEqual(_human_bytes(512), "512.0 B")
        self.assertEqual(_human_bytes(1024), "1.0 KB")
        self.assertEqual(_human_bytes(1024 * 1024), "1.0 MB")
        self.assertEqual(_human_bytes(1024 ** 3), "1.0 GB")

    def test_caps_at_tb(self):
        self.assertTrue(_human_bytes(1024 ** 5).endswith(" TB"))


class ScaledBarValuesTests(unittest.TestCase):
    INT32_MAX = 2147483647

    def test_normal_progress_halfway(self):
        self.assertEqual(_scaled_bar_values(50, 100), (1000, 500))

    def test_zero_and_complete(self):
        self.assertEqual(_scaled_bar_values(0, 100), (1000, 0))
        self.assertEqual(_scaled_bar_values(100, 100), (1000, 1000))

    def test_unknown_total_is_indeterminate(self):
        self.assertEqual(_scaled_bar_values(0, 0), (0, 0))
        self.assertEqual(_scaled_bar_values(123, -1), (0, 0))

    def test_value_never_exceeds_range(self):
        # done > total (e.g. slightly over-reported) must still clamp to full
        self.assertEqual(_scaled_bar_values(150, 100), (1000, 1000))

    def test_large_transfer_stays_within_int32(self):
        """REGRESSION: a multi-file download whose byte total exceeds ~2.1 GB
        overflowed QProgressBar.setRange (C++ int), raising OverflowError.
        The scaled range and value must always fit in a 32-bit signed int."""
        done = 3 * 1024 ** 3          # 3 GB downloaded
        total = 10 * 1024 ** 3        # 10 GB total  (> INT32_MAX)
        range_max, value = _scaled_bar_values(done, total)
        self.assertLessEqual(range_max, self.INT32_MAX)
        self.assertLessEqual(value, self.INT32_MAX)
        self.assertEqual((range_max, value), (1000, 300))

    def test_petabyte_scale_does_not_overflow(self):
        done = 5 * 1024 ** 5          # 5 PB
        total = 8 * 1024 ** 5         # 8 PB
        range_max, value = _scaled_bar_values(done, total)
        self.assertLessEqual(range_max, self.INT32_MAX)
        self.assertLessEqual(value, self.INT32_MAX)
        self.assertEqual((range_max, value), (1000, 625))


class BuildRegionSwappedEndpointTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_bare_s3(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("https://s3.amazonaws.com", "eu-north-1"),
            "https://s3.eu-north-1.amazonaws.com",
        )

    def test_regioned_s3(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("https://s3.eu-central-1.amazonaws.com", "eu-north-1"),
            "https://s3.eu-north-1.amazonaws.com",
        )

    def test_scheme_and_port_preserved(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("http://s3.us-west-2.amazonaws.com:9000", "eu-north-1"),
            "http://s3.eu-north-1.amazonaws.com:9000",
        )

    def test_non_aws_returns_none(self):
        self.assertIsNone(self.m.build_region_swapped_endpoint("https://minio.local:9000", "eu-north-1"))
        self.assertIsNone(self.m.build_region_swapped_endpoint("", "eu-north-1"))


class EndpointHasBucketTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_virtual_host_prefix(self):
        self.assertTrue(self.m._endpoint_has_bucket("https://mybucket.s3.amazonaws.com", "mybucket"))
        self.assertTrue(self.m._endpoint_has_bucket("https://mybucket.s3.amazonaws.com", "MYBUCKET"))

    def test_not_present(self):
        self.assertFalse(self.m._endpoint_has_bucket("https://s3.amazonaws.com", "mybucket"))
        self.assertFalse(self.m._endpoint_has_bucket("https://other.s3.amazonaws.com", "mybucket"))

    def test_extract_leftmost_label(self):
        self.assertEqual(self.m._extract_leftmost_label("https://mybucket.s3.amazonaws.com"), "mybucket")
        self.assertEqual(self.m._extract_leftmost_label("https://s3.amazonaws.com"), "s3")
        self.assertEqual(self.m._extract_leftmost_label(""), "")


class ParseRestoreStatusTests(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(Model.parse_restore_status(None), "")
        self.assertEqual(Model.parse_restore_status(""), "")

    def test_in_progress(self):
        self.assertEqual(
            Model.parse_restore_status('ongoing-request="true"'),
            "in-progress",
        )

    def test_available_with_expiry(self):
        header = ('ongoing-request="false", '
                  'expiry-date="Fri, 01 Jan 2027 00:00:00 GMT"')
        self.assertEqual(
            Model.parse_restore_status(header),
            "available until Fri, 01 Jan 2027 00:00:00 GMT",
        )

    def test_available_without_expiry(self):
        self.assertEqual(
            Model.parse_restore_status('ongoing-request="false"'),
            "available",
        )


class ListObjectVersionsTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(versions_pages=[{
            "Versions": [
                {"Key": "a/f.txt", "VersionId": "v2", "IsLatest": True,
                 "LastModified": _dt(2), "Size": 20, "StorageClass": "STANDARD",
                 "ETag": '"etag2"'},
                {"Key": "a/f.txt", "VersionId": "v1", "IsLatest": False,
                 "LastModified": _dt(1), "Size": 10, "StorageClass": "STANDARD",
                 "ETag": '"etag1"'},
                {"Key": "a/other.txt", "VersionId": "x", "IsLatest": True,
                 "LastModified": _dt(3), "Size": 99, "StorageClass": "STANDARD"},
            ],
            "DeleteMarkers": [
                {"Key": "a/f.txt", "VersionId": "dm1", "IsLatest": False,
                 "LastModified": _dt(3)},
            ],
        }])

    def test_filters_to_exact_key(self):
        self.m._client = self._client()
        out = self.m.list_object_versions("a/f.txt")
        # v2, v1, dm1 -> the 'a/other.txt' entry must be excluded
        self.assertEqual({e["version_id"] for e in out}, {"v1", "v2", "dm1"})

    def test_newest_first_and_flags(self):
        self.m._client = self._client()
        out = self.m.list_object_versions("a/f.txt")
        # dm1 (day 3) newest, then v2 (day 2), then v1 (day 1)
        self.assertEqual([e["version_id"] for e in out], ["dm1", "v2", "v1"])
        dm = out[0]
        self.assertTrue(dm["is_delete_marker"])
        self.assertEqual(dm["size"], 0)
        latest = next(e for e in out if e["version_id"] == "v2")
        self.assertTrue(latest["is_latest"])
        self.assertEqual(latest["size"], 20)
        self.assertEqual(latest["etag"], "etag2")

    def test_rejects_folder_key(self):
        self.m._client = self._client()
        with self.assertRaises(ValueError):
            self.m.list_object_versions("a/")


class VersionMutationTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_make_version_current_copies_with_version_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.make_version_current("a/f.txt", "v1")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(
            copies[0]["CopySource"],
            {"Bucket": "b", "Key": "a/f.txt", "VersionId": "v1"},
        )
        self.assertEqual(copies[0]["Key"], "a/f.txt")
        self.assertEqual(copies[0]["Bucket"], "b")

    def test_delete_object_version_passes_version_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.delete_object_version("a/f.txt", "dm1")
        dels = c.calls_of("delete_object")
        self.assertEqual(dels, [{"Bucket": "b", "Key": "a/f.txt", "VersionId": "dm1"}])


class StorageClassTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_change_storage_class_copies_onto_itself(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.change_storage_class("a/f.txt", "GLACIER")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["CopySource"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(copies[0]["Key"], "a/f.txt")
        self.assertEqual(copies[0]["StorageClass"], "GLACIER")
        self.assertEqual(copies[0]["MetadataDirective"], "COPY")

    def test_restore_object_success(self):
        c = FakeS3Client()
        self.m._client = c
        ok, reason = self.m.restore_object("a/f.txt", days=3, tier="Bulk")
        self.assertTrue(ok)
        self.assertIsNone(reason)
        req = c.calls_of("restore_object")[0]["RestoreRequest"]
        self.assertEqual(req["Days"], 3)
        self.assertEqual(req["GlacierJobParameters"], {"Tier": "Bulk"})

    def test_restore_object_already_in_progress(self):
        err = botocore.exceptions.ClientError(
            {"Error": {"Code": "RestoreAlreadyInProgress", "Message": "busy"}},
            "RestoreObject",
        )
        self.m._client = FakeS3Client(restore_error=err)
        ok, reason = self.m.restore_object("a/f.txt")
        self.assertFalse(ok)
        self.assertIn("already in progress", reason.lower())


class GetObjectPreviewTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_ranged_reports_total_and_truncation(self):
        resp = {
            "Body": io.BytesIO(b"hello"),
            "ContentType": "text/plain",
            "ContentLength": 5,
            "ContentRange": "bytes 0-4/10",
        }
        c = FakeS3Client(get_object_resp=resp)
        self.m._client = c
        out = self.m.get_object_preview("a/f.txt", max_bytes=5)
        self.assertEqual(out["data"], b"hello")
        self.assertEqual(out["content_type"], "text/plain")
        self.assertEqual(out["size"], 10)          # from Content-Range total
        self.assertTrue(out["truncated"])          # 5 fetched < 10 total
        # A Range header must be sent when max_bytes is set.
        self.assertEqual(c.calls_of("get_object")[0]["Range"], "bytes=0-4")

    def test_full_object_not_truncated(self):
        resp = {
            "Body": io.BytesIO(b"abc"),
            "ContentType": "application/octet-stream",
            "ContentLength": 3,
        }
        self.m._client = FakeS3Client(get_object_resp=resp)
        out = self.m.get_object_preview("a/f.bin", max_bytes=1024)
        self.assertEqual(out["size"], 3)
        self.assertFalse(out["truncated"])

    def test_empty_object_retries_without_range(self):
        """REGRESSION: a zero-byte object can't satisfy a byte range and must
        fall back to an unranged read instead of surfacing 416."""

        class RangeRejectingClient:
            def __init__(self):
                self.range_seen = []

            def get_object(self, **kw):
                self.range_seen.append("Range" in kw)
                if "Range" in kw:
                    raise botocore.exceptions.ClientError(
                        {"Error": {"Code": "InvalidRange", "Message": "n/a"}},
                        "GetObject",
                    )
                return {"Body": io.BytesIO(b""), "ContentType": "text/plain",
                        "ContentLength": 0}

        c = RangeRejectingClient()
        self.m._client = c
        out = self.m.get_object_preview("a/empty.txt", max_bytes=1024)
        self.assertEqual(out["data"], b"")
        self.assertFalse(out["truncated"])
        # first call ranged (rejected), second call unranged (ok)
        self.assertEqual(c.range_seen, [True, False])


class RenameBuildingBlocksTests(unittest.TestCase):
    """In-place rename reuses server-side copy + delete; verify those calls."""

    def setUp(self):
        self.m = make_model(bucket="b")

    def test_file_rename_copies_then_deletes(self):
        c = FakeS3Client()
        self.m._client = c
        # what MainWindow.rename_selected drives for a file in prefix "a/"
        self.m.copy_object("a/old.txt", "a/new.txt")
        self.m.delete("a/old.txt")
        copies = c.calls_of("copy_object")
        self.assertEqual(copies[0]["CopySource"], {"Bucket": "b", "Key": "a/old.txt"})
        self.assertEqual(copies[0]["Key"], "a/new.txt")
        dels = c.calls_of("delete_object")
        self.assertEqual(dels, [{"Bucket": "b", "Key": "a/old.txt"}])


class DeleteBucketPurgesVersionsTests(unittest.TestCase):
    """REGRESSION: recursive bucket delete must also purge noncurrent
    versions and delete markers, or AWS refuses DeleteBucket."""

    def test_versions_and_markers_are_deleted(self):
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "cur.txt"}]}],
            versions_pages=[{
                "Versions": [
                    {"Key": "cur.txt", "VersionId": "v1"},
                    {"Key": "old.txt", "VersionId": "v0"},
                ],
                "DeleteMarkers": [
                    {"Key": "gone.txt", "VersionId": "dm0"},
                ],
            }],
        )
        # Bypass network-bound client construction.
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.delete_bucket_recursive("mybucket")

        deleted = []
        for kw in c.calls_of("delete_objects"):
            deleted.extend(kw["Delete"]["Objects"])

        # current object (no VersionId) purged in pass 1
        self.assertIn({"Key": "cur.txt"}, deleted)
        # versions + delete markers purged in pass 2 (with VersionId)
        self.assertIn({"Key": "cur.txt", "VersionId": "v1"}, deleted)
        self.assertIn({"Key": "old.txt", "VersionId": "v0"}, deleted)
        self.assertIn({"Key": "gone.txt", "VersionId": "dm0"}, deleted)
        # and the bucket itself is finally removed
        self.assertEqual(len(c.calls_of("delete_bucket")), 1)


class SetBucketVersioningTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_enable_sends_correct_config(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_bucket_versioning("Enabled")
        put = c.calls_of("put_bucket_versioning")[0]
        self.assertEqual(put["Bucket"], "b")
        self.assertEqual(put["VersioningConfiguration"], {"Status": "Enabled"})

    def test_lowercase_is_normalized(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_bucket_versioning("suspended")
        put = c.calls_of("put_bucket_versioning")[0]
        self.assertEqual(put["VersioningConfiguration"], {"Status": "Suspended"})

    def test_invalid_status_rejected(self):
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.set_bucket_versioning("Nope")


class ObjectMetadataTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_get_metadata_maps_head_fields(self):
        head = {
            "ContentType": "text/plain",
            "CacheControl": "max-age=60",
            "ContentDisposition": 'attachment; filename="x.txt"',
            "ContentEncoding": "gzip",
            "StorageClass": "STANDARD_IA",
            "Metadata": {"owner": "vlad"},
        }
        self.m._client = FakeS3Client(head_object_resp=head)
        meta = self.m.get_object_metadata("a/f.txt")
        self.assertEqual(meta["content_type"], "text/plain")
        self.assertEqual(meta["cache_control"], "max-age=60")
        self.assertEqual(meta["content_encoding"], "gzip")
        self.assertEqual(meta["storage_class"], "STANDARD_IA")
        self.assertEqual(meta["metadata"], {"owner": "vlad"})

    def test_set_metadata_replace_copy(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_object_metadata(
            "a/f.txt",
            content_type="application/json",
            cache_control="no-cache",
            metadata={"k": "v"},
            storage_class="GLACIER",
        )
        cp = c.calls_of("copy_object")[0]
        self.assertEqual(cp["MetadataDirective"], "REPLACE")
        self.assertEqual(cp["CopySource"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(cp["Key"], "a/f.txt")
        self.assertEqual(cp["ContentType"], "application/json")
        self.assertEqual(cp["CacheControl"], "no-cache")
        self.assertEqual(cp["Metadata"], {"k": "v"})
        # non-STANDARD storage class must be preserved on the REPLACE copy
        self.assertEqual(cp["StorageClass"], "GLACIER")

    def test_set_metadata_always_sends_content_type(self):
        """REGRESSION: a REPLACE copy without ContentType makes S3 reset it to
        binary/octet-stream, so it must always be present."""
        c = FakeS3Client()
        self.m._client = c
        self.m.set_object_metadata("a/f.txt", content_type="")
        cp = c.calls_of("copy_object")[0]
        self.assertIn("ContentType", cp)
        # STANDARD is the default and must NOT be pinned on the copy
        self.assertNotIn("StorageClass", cp)


class SearchKeysTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(list_pages=[
            {"Contents": [
                {"Key": "docs/Report.pdf", "Size": 10},
                {"Key": "docs/notes.txt", "Size": 20},
                {"Key": "docs/sub/", "Size": 0},          # folder placeholder
                {"Key": "docs/sub/report_final.txt", "Size": 30},
            ]},
        ])

    def test_case_insensitive_substring_match(self):
        self.m._client = self._client()
        hits = self.m.search_keys("docs/", "report")
        keys = [k for k, _ in hits]
        self.assertIn("docs/Report.pdf", keys)          # case-insensitive
        self.assertIn("docs/sub/report_final.txt", keys)  # recursive
        self.assertNotIn("docs/notes.txt", keys)

    def test_skips_folder_placeholders(self):
        self.m._client = self._client()
        hits = self.m.search_keys("docs/", "sub")
        keys = [k for k, _ in hits]
        # the "docs/sub/" placeholder must not be returned, but the file under
        # it (whose key contains "sub") is a valid match
        self.assertNotIn("docs/sub/", keys)
        self.assertIn("docs/sub/report_final.txt", keys)

    def test_max_results_caps_output(self):
        self.m._client = self._client()
        hits = self.m.search_keys("docs/", "", max_results=2)  # empty q matches all files
        self.assertEqual(len(hits), 2)


class WorkerBulkStorageTests(unittest.TestCase):
    """Worker.set_storage_class / restore must fan a folder target out to every
    concrete object under it, and act directly on file targets."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _worker(self, job):
        from main_window import Worker

        class FakeWorkerModel:
            def __init__(self):
                self.storage_calls = []
                self.restore_calls = []

            def get_keys(self, prefix, log_fn=None):
                # two files + a placeholder under any folder prefix
                return [(prefix + "a.txt", 1), (prefix + "b.txt", 2),
                        (prefix, 0)]

            def change_storage_class(self, key, storage_class, log_fn=None):
                self.storage_calls.append((key, storage_class))

            def restore_object(self, key, days=7, tier="Standard"):
                self.restore_calls.append((key, days, tier))
                return True, None

        model = FakeWorkerModel()
        return Worker(model, job), model

    def test_storage_class_recurses_folder(self):
        job = [("f.txt", False, "GLACIER"), ("dir/", True, "GLACIER")]
        worker, model = self._worker(job)
        worker.set_storage_class()
        keys = [k for k, _cls in model.storage_calls]
        self.assertIn("f.txt", keys)                 # file target
        self.assertIn("dir/a.txt", keys)             # folder fanned out
        self.assertIn("dir/b.txt", keys)
        self.assertNotIn("dir/", keys)               # placeholder skipped
        self.assertTrue(all(c == "GLACIER" for _k, c in model.storage_calls))

    def test_restore_recurses_folder(self):
        job = [("dir/", True, 5, "Bulk")]
        worker, model = self._worker(job)
        worker.restore()
        keys = [k for k, _d, _t in model.restore_calls]
        self.assertEqual(set(keys), {"dir/a.txt", "dir/b.txt"})
        self.assertTrue(all((d, t) == (5, "Bulk") for _k, d, t in model.restore_calls))


class PresignedUrlModelTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_put_url_uses_put_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.presigned_put_url("a/f.txt", 120)
        call = c.calls_of("generate_presigned_url")[0]
        self.assertEqual(call["op"], "put_object")
        self.assertEqual(call["Params"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(call["ExpiresIn"], 120)

    def test_get_url_uses_get_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.presigned_get_url("a/f.txt", 300)
        call = c.calls_of("generate_presigned_url")[0]
        self.assertEqual(call["op"], "get_object")
        self.assertEqual(call["ExpiresIn"], 300)


class _FakePresignModel:
    bucket = "b"

    def __init__(self):
        self.calls = []

    def presigned_get_url(self, key, expires_sec=3600):
        self.calls.append(("get", key, expires_sec))
        return f"https://get/{key}?e={expires_sec}"

    def presigned_put_url(self, key, expires_sec=3600):
        self.calls.append(("put", key, expires_sec))
        return f"https://put/{key}?e={expires_sec}"


class PresignedLinkDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self):
        from main_window import PresignedLinkDialog
        return PresignedLinkDialog(None, _FakePresignModel(), "a/f.txt"), None

    def test_default_is_one_hour_get(self):
        from main_window import PresignedLinkDialog
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        self.assertEqual(dlg._expires_sec(), 3600)          # 1 Hour default
        self.assertEqual(model.calls[-1], ("get", "a/f.txt", 3600))
        self.assertTrue(dlg._url.text().startswith("https://get/"))

    def test_switch_to_put(self):
        from main_window import PresignedLinkDialog
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        dlg._type.setCurrentIndex(1)  # Upload (PUT) -> triggers regenerate
        self.assertEqual(model.calls[-1][0], "put")
        self.assertTrue(dlg._url.text().startswith("https://put/"))

    def test_expiry_units_and_clamp(self):
        from main_window import PresignedLinkDialog
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        dlg._unit.setCurrentIndex(2)   # Days
        dlg._amount.setValue(2)
        self.assertEqual(dlg._expires_sec(), 172800)        # 2 days
        self.assertEqual(model.calls[-1][2], 172800)
        # 10 days exceeds the 7-day SigV4 cap -> clamped in the generated call
        dlg._amount.setValue(10)
        self.assertEqual(model.calls[-1][2], PresignedLinkDialog.MAX_EXPIRES)


class BreadcrumbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _segments(self, bc):
        from PyQt6.QtWidgets import QToolButton
        out = []
        for i in range(bc._lay.count()):
            w = bc._lay.itemAt(i).widget()
            if isinstance(w, QToolButton):
                out.append(w)
        return out

    def test_bucket_list_mode_only_home(self):
        from main_window import Breadcrumb
        bc = Breadcrumb()
        bc.set_location("", "", True)
        segs = self._segments(bc)
        self.assertEqual([s.text() for s in segs], ["Buckets"])
        self.assertFalse(segs[0].isEnabled())  # current location, not clickable

    def test_nested_prefix_segments(self):
        from main_window import Breadcrumb
        bc = Breadcrumb()
        bc.set_location("mybucket", "a/b/", False)
        segs = self._segments(bc)
        self.assertEqual([s.text() for s in segs], ["Buckets", "mybucket", "a", "b"])
        # only the last ("b") is the current location
        self.assertTrue(segs[0].isEnabled())    # Buckets
        self.assertTrue(segs[1].isEnabled())    # mybucket
        self.assertTrue(segs[2].isEnabled())    # a
        self.assertFalse(segs[3].isEnabled())   # b (current)

    def test_segment_click_emits_prefix(self):
        from main_window import Breadcrumb
        bc = Breadcrumb()
        bc.set_location("mybucket", "a/b/", False)
        got = []
        bc.go.connect(lambda p: got.append(p))
        segs = self._segments(bc)
        # click "a" -> navigate to "a/"; click bucket -> navigate to ""
        segs[2].click()
        segs[1].click()
        self.assertEqual(got, ["a/", ""])

    def test_home_click_emits_home(self):
        from main_window import Breadcrumb
        bc = Breadcrumb()
        bc.set_location("mybucket", "", False)
        fired = []
        bc.home.connect(lambda: fired.append(True))
        self._segments(bc)[0].click()  # "Buckets"
        self.assertEqual(fired, [True])


class ThemeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_apply_returns_normalized_name(self):
        import theme
        self.assertEqual(theme.apply_theme(self._app, "dark"), "dark")
        self.assertEqual(theme.apply_theme(self._app, "LIGHT"), "light")
        self.assertEqual(theme.apply_theme(self._app, "bogus"), "system")

    def test_dark_palette_is_dark(self):
        import theme
        from PyQt6.QtGui import QPalette
        theme.apply_theme(self._app, "dark")
        window = self._app.palette().color(QPalette.ColorRole.Window)
        self.assertLess(window.lightness(), 128)
        # restore for other tests
        theme.apply_theme(self._app, "system")


if __name__ == "__main__":
    unittest.main()
