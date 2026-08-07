"""
Unit tests for pure-logic helpers (no network / no QApplication needed).

Run with:  python -m unittest discover -s tests   (from the project root)
or:        .venv/bin/python -m unittest discover -s tests

Several tests are explicit regression guards for previously fixed bugs and
are marked with "REGRESSION:" in their docstrings.
"""

import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

# Widget tests need a Qt platform plugin; use the headless one before any Qt
# import so the suite runs without a display.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import botocore.exceptions
from PyQt6.QtCore import QSettings
from PyQt6.QtGui import QAction, QPalette
from PyQt6.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QMessageBox, QToolButton,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main_window
import theme
from utils import (
    str_to_bool, load_aws_profiles, scan_local_tree,
    export_profile_bundle, import_profile_bundle, BundleError,
)
from main_window import (
    _to_epoch, categorize_key, _human_bytes, _scaled_bar_values,
    _dest_inside_source, _build_upload_job_for_path, _listing_summary,
    bulk_rename_plan, build_sync_plan, summarize_sync_plan, build_exclude_matcher,
    collect_shortcuts, LIST_COLUMNS, LIST_OPTIONAL_COLUMNS,
    format_completion_notification, BULK_RENAME_FIND, BULK_RENAME_TEMPLATE,
    Breadcrumb, BulkRenameDialog, CopyMoveDialog, IncompleteUploadsDialog,
    MetadataDialog, OverwriteDialog, PresignedLinkDialog, SyncDialog,
    TagsDialog, TransferSettingsDialog, VersionsDialog, Worker,
)
import model as model_module
from model import (
    Model, Item, FSObjectType, TransferCancelled, ReadOnlyError, run_parallel,
    RateLimiter,
)
from properties_window import PropertiesWindow


def _ensure_qapp():
    """Return the shared QApplication, creating it once (offscreen)."""
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
                 restore_error=None, mpu_pages=None, parts_pages=None):
        self._paginators = {
            "list_object_versions": _FakePaginator(versions_pages),
            "list_objects_v2": _FakePaginator(list_pages),
            "list_multipart_uploads": _FakePaginator(mpu_pages),
            "list_parts": _FakePaginator(parts_pages),
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

    def abort_multipart_upload(self, **kw):
        self.calls.append(("abort_multipart_upload", kw))
        return {}

    def download_file(self, *a, **kw):
        self.calls.append(("download_file", {"args": a, "kwargs": kw}))

    def upload_file(self, *a, **kw):
        self.calls.append(("upload_file", {"args": a, "kwargs": kw}))

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        return self._head_object_resp

    def get_object(self, **kw):
        self.calls.append(("get_object", kw))
        if self._get_object_resp is None:
            raise AssertionError("get_object was not configured for this test")
        return self._get_object_resp

    def put_object(self, **kw):
        self.calls.append(("put_object", kw))
        return {}

    def put_object_acl(self, **kw):
        self.calls.append(("put_object_acl", kw))
        return {}

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


class DeleteBucketUsesBoundClientTests(unittest.TestCase):
    """REGRESSION: the recursive delete emptied the bucket and then issued
    DeleteBucket on a profile/root client, which raises PermanentRedirect for
    a bucket in another region — leaving the bucket wiped but still present."""

    def _model_with_split_clients(self):
        m = make_model(bucket="")
        bound = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "k.txt"}]}],
            versions_pages=[{}],
            mpu_pages=[{}],
        )
        root = FakeS3Client()
        m._try_bind_bucket = lambda name: (bound, "ep", "eu-north-1", True)
        m._make_client = lambda **kw: root
        return m, bound, root

    def test_recursive_delete_uses_the_bound_client(self):
        m, bound, root = self._model_with_split_clients()
        m.delete_bucket_recursive("offregion")
        self.assertEqual(
            bound.calls_of("delete_bucket"), [{"Bucket": "offregion"}])
        self.assertEqual(root.calls_of("delete_bucket"), [])

    def test_plain_delete_uses_the_bound_client(self):
        m = make_model(bucket="")
        bound = FakeS3Client(list_pages=[{}])
        root = FakeS3Client()
        m._try_bind_bucket = lambda name: (bound, "ep", "eu-north-1", True)
        m._make_client = lambda **kw: root
        m.delete_bucket("offregion")
        self.assertEqual(
            bound.calls_of("delete_bucket"), [{"Bucket": "offregion"}])
        self.assertEqual(root.calls_of("delete_bucket"), [])

    def test_recursive_delete_honours_cancellation(self):
        m, bound, _root = self._model_with_split_clients()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            m.delete_bucket_recursive("offregion", cancel_event=cancel)
        # nothing destructive may run once cancellation is already requested
        self.assertEqual(bound.calls_of("delete_objects"), [])
        self.assertEqual(bound.calls_of("delete_bucket"), [])


class SessionTokenTests(unittest.TestCase):
    """Temporary credentials (STS / SSO / assumed role) need a session token;
    without one those credentials cannot be used at all."""

    def test_token_is_passed_to_the_client(self):
        captured = {}
        m = make_model(session_token="TOKEN")

        class FakeSession:
            def client(self, _name, **kw):
                captured.update(kw)
                return object()

        m.session = FakeSession()
        m._make_client()
        self.assertEqual(captured["aws_session_token"], "TOKEN")

    def test_absent_token_is_omitted(self):
        captured = {}
        m = make_model()

        class FakeSession:
            def client(self, _name, **kw):
                captured.update(kw)
                return object()

        m.session = FakeSession()
        m._make_client()
        self.assertNotIn("aws_session_token", captured)

    def test_clone_carries_the_token(self):
        m = make_model(session_token="TOKEN")
        self.assertEqual(m.clone_for_worker().session_token, "TOKEN")


class LoadAwsProfilesTests(unittest.TestCase):
    def _write(self, tmp, name, text):
        path = os.path.join(tmp, name)
        with open(path, "w") as handle:
            handle.write(text)
        return path

    def test_credentials_and_config_are_merged(self):
        with tempfile.TemporaryDirectory() as tmp:
            creds = self._write(tmp, "credentials", """
[default]
aws_access_key_id = AKDEFAULT
aws_secret_access_key = SECRET1

[work]
aws_access_key_id = AKWORK
aws_secret_access_key = SECRET2
aws_session_token = TOKEN2
""")
            config = self._write(tmp, "config", """
[default]
region = eu-north-1

[profile work]
region = us-west-2
endpoint_url = https://minio.local:9000
""")
            profiles = load_aws_profiles(creds, config)
            self.assertEqual(set(profiles), {"default", "work"})
            self.assertEqual(profiles["default"]["access_key"], "AKDEFAULT")
            self.assertEqual(profiles["default"]["region"], "eu-north-1")
            self.assertEqual(profiles["default"]["session_token"], "")
            self.assertEqual(profiles["work"]["session_token"], "TOKEN2")
            self.assertEqual(profiles["work"]["region"], "us-west-2")
            self.assertEqual(
                profiles["work"]["endpoint_url"], "https://minio.local:9000")

    def test_config_only_profiles_are_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self._write(tmp, "config", "[profile empty]\nregion = x\n")
            self.assertEqual(load_aws_profiles(os.path.join(tmp, "none"), config), {})

    def test_missing_files_are_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                load_aws_profiles(os.path.join(tmp, "a"), os.path.join(tmp, "b")),
                {},
            )


class BindingCacheTests(unittest.TestCase):
    """Opening an off-region bucket probes several endpoint/style combos; a
    proven binding must be reused instead of re-probed."""

    def setUp(self):
        self.m = make_model()
        self.made = []

        def _make_client(**kw):
            self.made.append(kw)
            return FakeS3Client(list_pages=[{}])

        self.m._make_client = _make_client

    def test_probe_result_is_cached_and_reused(self):
        probes = []

        def _probe(name):
            probes.append(name)
            return FakeS3Client(list_pages=[{}]), "https://ep", "eu-north-1", True

        self.m._probe_bucket_binding = _probe

        first = self.m._try_bind_bucket("b1")
        self.assertEqual(first[1:], ("https://ep", "eu-north-1", True))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(
            self.m.binding_cache["b1"], ("https://ep", "eu-north-1", True))

        # second open: cached combo is used, no re-probe
        again = self.m._try_bind_bucket("b1")
        self.assertEqual(again[1:], ("https://ep", "eu-north-1", True))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(self.made[-1]["endpoint_url"], "https://ep")

    def test_stale_entry_is_dropped_and_reprobed(self):
        probes = []

        def _probe(name):
            probes.append(name)
            return FakeS3Client(list_pages=[{}]), "https://new", "us-east-1", False

        self.m._probe_bucket_binding = _probe
        self.m.binding_cache["b1"] = ("https://stale", "eu-west-1", True)
        # a cached client that cannot list forces a re-probe
        self.m._can_list_bucket = lambda client, bucket: False

        result = self.m._try_bind_bucket("b1")
        self.assertEqual(result[1:], ("https://new", "us-east-1", False))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(
            self.m.binding_cache["b1"], ("https://new", "us-east-1", False))

    def test_clone_shares_the_cache(self):
        self.m.binding_cache["b1"] = ("https://ep", "r", True)
        clone = self.m.clone_for_worker()
        clone.binding_cache["b2"] = ("https://ep2", "r2", False)
        # discoveries made by a worker are not lost
        self.assertIn("b2", self.m.binding_cache)


class BatchDeleteTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_prefix_delete_uses_one_batched_call(self):
        keys = [f"dir/f{i}.txt" for i in range(5)]
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": k, "Size": 1} for k in keys]}])
        self.m._client = c
        self.m.delete("dir/")
        batches = c.calls_of("delete_objects")
        self.assertEqual(len(batches), 1)
        self.assertEqual(
            [o["Key"] for o in batches[0]["Delete"]["Objects"]], keys)
        self.assertEqual(c.calls_of("delete_object"), [])  # no per-object calls

    def test_single_object_uses_delete_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.delete("dir/f.txt")
        self.assertEqual(
            c.calls_of("delete_object"), [{"Bucket": "b", "Key": "dir/f.txt"}])
        self.assertEqual(c.calls_of("delete_objects"), [])

    def test_batches_are_chunked(self):
        keys = [f"dir/f{i}.txt" for i in range(2500)]
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": k, "Size": 1} for k in keys]}])
        self.m._client = c
        self.m.delete("dir/")
        sizes = [len(kw["Delete"]["Objects"]) for kw in c.calls_of("delete_objects")]
        self.assertEqual(sizes, [1000, 1000, 500])

    def test_falls_back_when_batch_delete_unsupported(self):
        class NoBatch(FakeS3Client):
            def delete_objects(self, **kw):
                self.calls.append(("delete_objects", kw))
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "DeleteObjects",
                )

        c = NoBatch(list_pages=[{"Contents": [
            {"Key": "dir/a", "Size": 1}, {"Key": "dir/b", "Size": 1}]}])
        self.m._client = c
        self.m.delete("dir/")
        self.assertEqual(
            [kw["Key"] for kw in c.calls_of("delete_object")], ["dir/a", "dir/b"])

    def test_prefix_delete_is_cancellable(self):
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "dir/a", "Size": 1}]}])
        self.m._client = c
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self.m.delete("dir/", cancel_event=cancel)
        self.assertEqual(c.calls_of("delete_objects"), [])


class EmptyBucketTests(unittest.TestCase):
    def test_contents_purged_but_bucket_kept(self):
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "a.txt"}]}],
            versions_pages=[{"Versions": [{"Key": "a.txt", "VersionId": "v1"}]}],
            mpu_pages=[{"Uploads": [
                {"Key": "half.bin", "UploadId": "u1", "Initiated": _dt(1)}]}],
        )
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.empty_bucket("mybucket")

        deleted = []
        for kw in c.calls_of("delete_objects"):
            deleted.extend(kw["Delete"]["Objects"])
        self.assertIn({"Key": "a.txt"}, deleted)
        self.assertIn({"Key": "a.txt", "VersionId": "v1"}, deleted)
        self.assertEqual(len(c.calls_of("abort_multipart_upload")), 1)
        # the bucket itself must survive
        self.assertEqual(c.calls_of("delete_bucket"), [])

    def test_current_prefix_is_reset_for_the_open_bucket(self):
        m = make_model(bucket="mybucket")
        m.current_folder = "deep/prefix/"
        c = FakeS3Client(list_pages=[{}], versions_pages=[{}], mpu_pages=[{}])
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m.empty_bucket("mybucket")
        self.assertEqual(m.current_folder, "")
        self.assertEqual(m.bucket, "mybucket")  # still inside it

    def test_rejects_empty_name(self):
        with self.assertRaises(ValueError):
            make_model().empty_bucket("")


class UploadOptionsTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_default_sends_no_extra_args(self):
        self.assertEqual(self.m.upload_extra_args, {})

    def test_storage_class_and_sse(self):
        args = self.m.set_upload_options(storage_class="GLACIER", sse="AES256")
        self.assertEqual(args, {
            "StorageClass": "GLACIER", "ServerSideEncryption": "AES256"})

    def test_standard_class_is_not_pinned(self):
        self.assertEqual(self.m.set_upload_options(storage_class="STANDARD"), {})

    def test_kms_key_only_with_kms_mode(self):
        self.assertEqual(
            self.m.set_upload_options(sse="aws:kms", kms_key_id="key-1"),
            {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "key-1"},
        )
        # a key without kms mode is meaningless and must be dropped
        self.assertEqual(
            self.m.set_upload_options(sse="AES256", kms_key_id="key-1"),
            {"ServerSideEncryption": "AES256"},
        )

    def test_extra_args_reach_upload_file(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_upload_options(storage_class="STANDARD_IA")
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.txt")
            with open(path, "w") as handle:
                handle.write("x")
            self.m.upload_file(path, "k.txt")
        self.assertEqual(
            c.calls_of("upload_file")[0]["kwargs"]["ExtraArgs"],
            {"StorageClass": "STANDARD_IA"},
        )


class ExistingKeysTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_reports_only_keys_that_exist(self):
        self.m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "dst/a.txt", "Size": 1},
            {"Key": "dst/c.txt", "Size": 1},
        ]}])
        found = self.m.existing_keys(["dst/a.txt", "dst/b.txt"])
        self.assertEqual(found, {"dst/a.txt"})

    def test_folder_target_conflicts_when_anything_lives_under_it(self):
        self.m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "dst/sub/deep.txt", "Size": 1},
        ]}])
        self.assertEqual(self.m.existing_keys(["dst/sub/"]), {"dst/sub/"})

    def test_empty_input(self):
        self.m._client = FakeS3Client(list_pages=[{}])
        self.assertEqual(self.m.existing_keys([]), set())


class PublicAccessSummaryTests(unittest.TestCase):
    def test_reasons_explain_a_refused_acl(self):
        m = make_model(bucket="b")

        class Client:
            def get_public_access_block(self, **_kw):
                return {"PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True, "IgnorePublicAcls": False}}

            def get_bucket_ownership_controls(self, **_kw):
                return {"OwnershipControls": {"Rules": [
                    {"ObjectOwnership": "BucketOwnerEnforced"}]}}

        m._client = Client()
        out = m.public_access_summary()
        self.assertEqual(out["ownership"], "BucketOwnerEnforced")
        self.assertEqual(len(out["reasons"]), 2)
        self.assertTrue(any("BlockPublicAcls" in r for r in out["reasons"]))
        self.assertTrue(any("ACLs" in r for r in out["reasons"]))

    def test_unsupported_backend_reports_nothing(self):
        m = make_model(bucket="b")

        class Client:
            def get_public_access_block(self, **_kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "GetPublicAccessBlock")

            def get_bucket_ownership_controls(self, **_kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "GetBucketOwnershipControls")

        m._client = Client()
        out = m.public_access_summary()
        self.assertEqual(out["reasons"], [])


class RunParallelTests(unittest.TestCase):
    def test_sequential_when_single_worker(self):
        seen = []
        run_parallel([1, 2, 3], seen.append, 1)
        self.assertEqual(seen, [1, 2, 3])   # order preserved

    def test_all_items_processed_in_parallel(self):
        seen = []
        lock = threading.Lock()

        def _fn(item):
            with lock:
                seen.append(item)

        run_parallel(range(50), _fn, 8)
        self.assertEqual(sorted(seen), list(range(50)))

    def test_first_error_is_reraised_and_work_stops(self):
        started = []
        lock = threading.Lock()

        def _fn(item):
            with lock:
                started.append(item)
            raise RuntimeError(f"boom {item}")

        with self.assertRaises(RuntimeError):
            run_parallel(range(20), _fn, 4)
        # the pool drains rather than leaving threads running
        self.assertLessEqual(len(started), 20)

    def test_cancel_event_stops_and_raises(self):
        cancel = threading.Event()
        cancel.set()
        seen = []
        with self.assertRaises(TransferCancelled):
            run_parallel([1, 2, 3], seen.append, 4, cancel_event=cancel)
        self.assertEqual(seen, [])

    def test_cancel_checked_between_sequential_items(self):
        cancel = threading.Event()
        seen = []

        def _fn(item):
            seen.append(item)
            cancel.set()

        with self.assertRaises(TransferCancelled):
            run_parallel([1, 2, 3], _fn, 1, cancel_event=cancel)
        self.assertEqual(seen, [1])

    def test_empty_input(self):
        run_parallel([], lambda _i: None, 4)  # must not raise


class ParallelFilesSettingTests(unittest.TestCase):
    def test_default_and_clamping(self):
        m = make_model()
        self.assertEqual(m.parallel_files, Model.DEFAULT_PARALLEL_FILES)
        self.assertEqual(m.set_parallel_files(0), 1)
        self.assertEqual(m.set_parallel_files(999), Model.MAX_PARALLEL_FILES)
        self.assertEqual(m.set_parallel_files("junk"),
                         Model.DEFAULT_PARALLEL_FILES)

    def test_clone_inherits(self):
        m = make_model(parallel_files=7)
        self.assertEqual(m.clone_for_worker().parallel_files, 7)

    def test_prefix_download_runs_in_parallel(self):
        """A whole-folder download is a single job entry, so the fan-out has
        to happen inside download_file or the setting does nothing for it."""
        m = make_model(bucket="b", parallel_files=4)
        keys = [{"Key": f"dir/f{i}.txt", "Size": 1} for i in range(12)]
        c = FakeS3Client(list_pages=[{"Contents": keys}])
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("dir/", None, tmp)
        self.assertEqual(len(c.calls_of("download_file")), 12)

    def test_prefix_copy_runs_in_parallel(self):
        m = make_model(bucket="b", parallel_files=4)
        keys = [{"Key": f"src/f{i}.txt", "Size": 1} for i in range(10)]
        c = FakeS3Client(list_pages=[{"Contents": keys}])
        m._client = c
        m.copy_prefix("src/", "dst/")
        copied = {kw["Key"] for kw in c.calls_of("copy_object")}
        self.assertEqual(copied, {f"dst/f{i}.txt" for i in range(10)})


class ReadOnlyProfileTests(unittest.TestCase):
    """A read-only profile must refuse every write at the model layer, not
    just hide buttons."""

    def setUp(self):
        self.m = make_model(bucket="b", read_only=True)
        self.m._client = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "a.txt", "Size": 1}]}],
            versions_pages=[{}], mpu_pages=[{}],
        )

    def test_writes_are_refused(self):
        cases = {
            "delete": lambda: self.m.delete("a.txt"),
            "delete prefix": lambda: self.m.delete("dir/"),
            "create_folder": lambda: self.m.create_folder("x/"),
            "upload": lambda: self.m.upload_file(None, "x"),
            "copy_object": lambda: self.m.copy_object("a", "b"),
            "copy_prefix": lambda: self.m.copy_prefix("a/", "b/"),
            "create_bucket": lambda: self.m.create_bucket("nb"),
            "delete_bucket": lambda: self.m.delete_bucket("b"),
            "delete_bucket_recursive": lambda: self.m.delete_bucket_recursive("b"),
            "empty_bucket": lambda: self.m.empty_bucket("b"),
            "versioning": lambda: self.m.set_bucket_versioning("Enabled"),
            "storage class": lambda: self.m.change_storage_class("a", "GLACIER"),
            "metadata": lambda: self.m.set_object_metadata("a"),
            "tags": lambda: self.m.put_object_tags("a", []),
            "make public": lambda: self.m.make_object_public("a"),
            "delete version": lambda: self.m.delete_object_version("a", "v1"),
            "make current": lambda: self.m.make_version_current("a", "v1"),
            "abort upload": lambda: self.m.abort_multipart_upload("a", "u1"),
            "presigned put": lambda: self.m.presigned_put_url("a"),
            "undelete": lambda: self.m.undelete("a.txt"),
            "restore": lambda: self.m.restore_object("a"),
        }
        for label, call in cases.items():
            with self.subTest(operation=label):
                with self.assertRaises(ReadOnlyError):
                    call()

    def test_reads_still_work(self):
        self.assertEqual(
            [k for k, _s in self.m.get_keys("")], ["a.txt"])
        self.assertTrue(self.m.presigned_get_url("a.txt"))

    def test_writable_profile_is_unaffected(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client()
        m.create_folder("x/")  # must not raise
        self.assertEqual(len(m._client.calls_of("put_object")), 1)

    def test_clone_keeps_the_flag(self):
        self.assertTrue(self.m.clone_for_worker().read_only)


class UndeleteTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(versions_pages=[{
            "DeleteMarkers": [
                {"Key": "dir/a.txt", "VersionId": "dm-new", "IsLatest": True},
                {"Key": "dir/a.txt", "VersionId": "dm-old", "IsLatest": False},
                {"Key": "dir/b.txt", "VersionId": "dm-b", "IsLatest": True},
            ],
            "Versions": [{"Key": "dir/a.txt", "VersionId": "v1"}],
        }])

    def test_single_key_removes_only_its_latest_marker(self):
        c = self._client()
        self.m._client = c
        restored = self.m.undelete("dir/a.txt")
        self.assertEqual(restored, 1)
        self.assertEqual(
            c.calls_of("delete_object"),
            [{"Bucket": "b", "Key": "dir/a.txt", "VersionId": "dm-new"}],
        )

    def test_prefix_restores_everything_underneath(self):
        c = self._client()
        self.m._client = c
        restored = self.m.undelete("dir/")
        self.assertEqual(restored, 2)
        keys = {kw["Key"] for kw in c.calls_of("delete_object")}
        self.assertEqual(keys, {"dir/a.txt", "dir/b.txt"})

    def test_unversioned_bucket_restores_nothing(self):
        self.m._client = FakeS3Client(versions_pages=[{}])
        self.assertEqual(self.m.undelete("dir/a.txt"), 0)

    def test_backend_without_versions_api(self):
        class NoVersions(FakeS3Client):
            def get_paginator(self, name):
                if name == "list_object_versions":
                    raise botocore.exceptions.ClientError(
                        {"Error": {"Code": "NotImplemented", "Message": "no"}},
                        "ListObjectVersions")
                return super().get_paginator(name)

        self.m._client = NoVersions()
        self.assertEqual(self.m.undelete("a.txt"), 0)

    def test_requires_a_bucket(self):
        m = make_model(bucket="")
        with self.assertRaises(ValueError):
            m.undelete("a.txt")


class CrossLocationCopyTests(unittest.TestCase):
    """CopyObject is server-side and cannot reach a bucket in another region
    or account; those copies must stream through instead of failing."""

    def setUp(self):
        self.m = make_model(bucket="src")

    class Client(FakeS3Client):
        def __init__(self, copy_error=None, **kw):
            super().__init__(**kw)
            self._copy_error = copy_error
            self.uploaded = []

        def copy_object(self, **kw):
            self.calls.append(("copy_object", kw))
            if self._copy_error is not None:
                raise self._copy_error
            return {}

        def get_object(self, **kw):
            self.calls.append(("get_object", kw))
            return {"Body": io.BytesIO(b"payload"), "ContentType": "text/plain",
                    "Metadata": {"owner": "vlad"}}

        def upload_fileobj(self, body, bucket, key, **kw):
            self.uploaded.append((bucket, key, body.read(), kw.get("ExtraArgs")))

    @staticmethod
    def _error(code):
        return botocore.exceptions.ClientError(
            {"Error": {"Code": code, "Message": "nope"}}, "CopyObject")

    def test_same_bucket_copy_stays_server_side(self):
        c = self.Client()
        self.m._client = c
        self.m.copy_object("a.txt", "b.txt")
        self.assertEqual(len(c.calls_of("copy_object")), 1)
        self.assertEqual(c.uploaded, [])

    def test_cross_location_error_falls_back_to_streaming(self):
        c = self.Client(copy_error=self._error("InvalidRequest"))
        self.m._client = c
        self.m._try_bind_bucket = lambda name: (c, "ep", "eu-north-1", True)
        logs = []
        self.m.copy_object("a.txt", "a.txt", dst_bucket="other",
                           log_fn=logs.append)
        self.assertEqual(len(c.uploaded), 1)
        bucket, key, data, extra = c.uploaded[0]
        self.assertEqual((bucket, key, data), ("other", "a.txt", b"payload"))
        # content type and user metadata survive the streamed copy
        self.assertEqual(extra["ContentType"], "text/plain")
        self.assertEqual(extra["Metadata"], {"owner": "vlad"})
        self.assertTrue(any("streaming" in line for line in logs))

    def test_permanent_redirect_cross_bucket_streams(self):
        c = self.Client(copy_error=self._error("PermanentRedirect"))
        self.m._client = c
        self.m._try_bind_bucket = lambda name: (c, "ep", "eu-north-1", True)
        self.m.copy_object("a.txt", "a.txt", dst_bucket="other")
        self.assertEqual(len(c.uploaded), 1)

    def test_same_bucket_redirect_still_rebinds_instead_of_streaming(self):
        c = self.Client(copy_error=self._error("PermanentRedirect"))
        self.m._client = c
        rebound = []
        self.m.rebind_bucket = lambda log_fn=None: rebound.append(True)
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "b.txt")  # same bucket
        self.assertEqual(rebound, [True])
        self.assertEqual(c.uploaded, [])

    def test_unrelated_error_is_not_swallowed(self):
        c = self.Client(copy_error=self._error("AccessDenied"))
        self.m._client = c
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "a.txt", dst_bucket="other")
        self.assertEqual(c.uploaded, [])

    def test_streaming_can_be_disabled(self):
        c = self.Client(copy_error=self._error("InvalidRequest"))
        self.m._client = c
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "a.txt", dst_bucket="other",
                               allow_stream=False)


class ChecksumTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_etag_is_md5_detection(self):
        self.assertTrue(Model.etag_is_md5('"' + "a" * 32 + '"'))
        self.assertFalse(Model.etag_is_md5("a" * 32 + "-3"))  # multipart
        self.assertFalse(Model.etag_is_md5(""))
        self.assertFalse(Model.etag_is_md5("short"))

    def test_matching_file_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"hello world")
            digest = hashlib.md5(b"hello world").hexdigest()
            self.assertTrue(self.m.verify_download(path, f'"{digest}"'))

    def test_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"hello world")
            logs = []
            self.assertFalse(
                self.m.verify_download(path, "b" * 32, log_fn=logs.append))
            self.assertTrue(any("MISMATCH" in line for line in logs))

    def test_multipart_etag_cannot_be_checked_and_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"x")
            logs = []
            self.assertTrue(
                self.m.verify_download(path, "a" * 32 + "-2", log_fn=logs.append))
            self.assertTrue(any("skipped" in line for line in logs))


class _RangedClient:
    """Serves an in-memory object over ranged GETs, optionally failing once."""

    def __init__(self, payload, fail_on_chunk=None):
        self.payload = payload
        self.fail_on_chunk = fail_on_chunk
        self.ranges = []
        self.lock = threading.Lock()

    def head_object(self, **kw):
        return {"ContentLength": len(self.payload),
                "ETag": '"' + hashlib.md5(self.payload).hexdigest() + '"'}

    def get_object(self, **kw):
        rng = kw["Range"]
        start, end = rng.replace("bytes=", "").split("-")
        start, end = int(start), int(end)
        with self.lock:
            self.ranges.append((start, end))
            if self.fail_on_chunk is not None and start == self.fail_on_chunk:
                self.fail_on_chunk = None      # only fail once
                raise RuntimeError("connection reset")
        return {"Body": io.BytesIO(self.payload[start:end + 1])}


class ResumableDownloadTests(unittest.TestCase):
    """REGRESSION-proofing: a large download that dies part-way must resume
    from the bytes already on disk instead of starting over."""

    def _model(self, payload, fail_on_chunk=None):
        m = make_model(bucket="b")
        m.RESUME_CHUNK_SIZE = 1024
        m.resume_threshold = 1
        m.parallel_files = 1          # deterministic chunk order
        m._client = _RangedClient(payload, fail_on_chunk)
        return m

    def test_full_download_assembles_the_object(self):
        payload = os.urandom(4096)
        m = self._model(payload)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            m.download_file("big.bin", out, tmp)
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)
            # the sidecar and part file are cleaned up on success
            self.assertEqual(os.listdir(tmp), ["big.bin"])

    def test_interrupted_download_resumes_from_the_partial(self):
        payload = os.urandom(4096)
        m = self._model(payload, fail_on_chunk=2048)   # 3rd chunk explodes
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(RuntimeError):
                m.download_file("big.bin", out, tmp)
            part = out + Model.PART_SUFFIX
            self.assertTrue(os.path.exists(part))
            self.assertTrue(os.path.exists(part + ".meta"))
            first_pass = len(m._client.ranges)

            m._client.ranges.clear()
            m.download_file("big.bin", out, tmp)      # resume
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)
            # only the chunks that were missing are re-fetched
            self.assertLess(len(m._client.ranges), first_pass)
            self.assertEqual(
                {start for start, _e in m._client.ranges}, {2048, 3072})

    def test_changed_object_discards_the_stale_partial(self):
        payload = os.urandom(4096)
        m = self._model(payload, fail_on_chunk=2048)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(RuntimeError):
                m.download_file("big.bin", out, tmp)
            # the object is replaced remotely -> a different ETag
            m._client = _RangedClient(os.urandom(4096))
            m.download_file("big.bin", out, tmp)
            starts = {start for start, _e in m._client.ranges}
            self.assertEqual(starts, {0, 1024, 2048, 3072})  # all re-fetched

    def test_small_files_skip_the_ranged_path(self):
        m = make_model(bucket="b")
        m.resume_threshold = 10 * 1024
        c = FakeS3Client(head_object_resp={"ContentLength": 10, "ETag": '"x"'})
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("small.bin", os.path.join(tmp, "small.bin"), tmp)
        self.assertEqual(len(c.calls_of("download_file")), 1)

    def test_verification_failure_after_ranged_download(self):
        payload = os.urandom(2048)
        m = self._model(payload)
        m.verify_downloads = True
        # corrupt what the server hands back so the ETag will not match
        m._client.payload = payload
        original_head = m._client.head_object
        m._client.head_object = lambda **kw: {
            "ContentLength": len(payload), "ETag": '"' + "0" * 32 + '"'}
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(Exception) as ctx:
                m.download_file("big.bin", out, tmp)
            self.assertIn("Checksum mismatch", str(ctx.exception))
        m._client.head_object = original_head


class RateLimiterTests(unittest.TestCase):
    """Deterministic: the limiter takes injectable clock and sleep."""

    def _limiter(self, rate, **kw):
        self.now = 0.0
        self.slept = []

        def _time():
            return self.now

        def _sleep(seconds):
            self.slept.append(seconds)
            self.now += seconds

        return RateLimiter(rate, time_fn=_time, sleep_fn=_sleep, **kw)

    def test_zero_rate_never_blocks(self):
        limiter = self._limiter(0)
        self.assertEqual(limiter.consume(10 ** 9), 0.0)
        self.assertEqual(self.slept, [])

    def test_burst_within_capacity_is_free(self):
        limiter = self._limiter(1000)          # 1 s of burst = 1000 bytes
        self.assertEqual(limiter.consume(1000), 0.0)
        self.assertEqual(self.slept, [])

    def test_exceeding_the_bucket_sleeps_the_difference(self):
        limiter = self._limiter(1000)
        limiter.consume(1000)                  # drains the bucket
        delay = limiter.consume(500)           # needs another half second
        self.assertAlmostEqual(delay, 0.5, places=6)
        self.assertEqual(len(self.slept), 1)

    def test_tokens_refill_over_time(self):
        limiter = self._limiter(1000)
        limiter.consume(1000)
        self.now += 2.0                        # plenty of time passes
        self.assertEqual(limiter.consume(1000), 0.0)

    def test_sustained_rate_is_respected(self):
        limiter = self._limiter(1000, capacity=0)
        for _ in range(4):
            limiter.consume(500)
        # 2000 bytes at 1000 B/s with no burst allowance ≈ 2 s of waiting
        self.assertAlmostEqual(sum(self.slept), 2.0, places=6)

    def test_negative_and_zero_amounts_ignored(self):
        limiter = self._limiter(1000)
        self.assertEqual(limiter.consume(0), 0.0)
        self.assertEqual(limiter.consume(-5), 0.0)

    def test_model_wires_the_limiter_and_shares_it_with_clones(self):
        m = make_model()
        self.assertIsNone(m.rate_limiter)
        m.set_rate_limit(2048)
        self.assertIsNotNone(m.rate_limiter)
        self.assertIs(m.clone_for_worker().rate_limiter, m.rate_limiter)
        m.set_rate_limit(0)
        self.assertIsNone(m.rate_limiter)

    def test_progress_adapter_charges_the_limiter(self):
        limiter = self._limiter(1000)
        seen = []
        adapter = model_module._BotoProgressAdapter(
            100, "k", lambda t, c, k: seen.append(c), limiter=limiter)
        adapter(60)
        adapter(40)
        self.assertEqual(seen, [60, 100])
        # everything transferred was charged against the bucket
        self.assertAlmostEqual(limiter._tokens, 900.0, places=6)


class ListingColumnDataTests(unittest.TestCase):
    """Storage class and ETag come back with ListObjectsV2, so the optional
    columns must cost no extra request."""

    def test_listing_captures_storage_class_and_etag(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{
            "CommonPrefixes": [{"Prefix": "dir/sub/"}],
            "Contents": [{
                "Key": "dir/a.txt", "Size": 12, "LastModified": _dt(1),
                "StorageClass": "GLACIER", "ETag": '"abc123"',
            }],
        }])
        items = m.list("dir/")
        by_name = {i.name: i for i in items}
        self.assertEqual(by_name["a.txt"].storage_class, "GLACIER")
        self.assertEqual(by_name["a.txt"].etag, "abc123")
        # folders carry no object metadata
        self.assertEqual(by_name["sub"].storage_class, "")

    def test_storage_class_defaults_to_standard(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 1, "LastModified": _dt(1)}]}])
        self.assertEqual(m.list("")[0].storage_class, "STANDARD")

    def test_usage_listing_returns_storage_class(self):
        m = make_model(bucket="b")
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 5, "StorageClass": "STANDARD_IA"}]}])
        m._try_bind_bucket = lambda name: (c, "ep", "r", True)
        self.assertEqual(
            m.get_keys_for_bucket("b", ""), [("a.txt", 5, "STANDARD_IA")])


class ProfileBundleTests(unittest.TestCase):
    """Export must not write credentials in the clear, and import must reject
    a wrong passphrase or a tampered file."""

    PROFILES = [
        {"name": "prod", "url": "https://s3.amazonaws.com",
         "access_key": "AKPROD", "secret_key": "SUPERSECRET",
         "session_token": "", "read_only": "true"},
        {"name": "dev", "url": "https://minio.local", "access_key": "AKDEV",
         "secret_key": "devsecret", "session_token": "tok"},
    ]

    def test_round_trip(self):
        blob = export_profile_bundle(self.PROFILES, "correct horse")
        restored = import_profile_bundle(blob, "correct horse")
        self.assertEqual(restored, self.PROFILES)

    def test_secrets_are_not_in_the_file(self):
        blob = export_profile_bundle(self.PROFILES, "pw")
        self.assertNotIn(b"SUPERSECRET", blob)
        self.assertNotIn(b"AKPROD", blob)
        # the envelope itself stays readable JSON
        document = json.loads(blob.decode())
        self.assertEqual(document["version"], 1)
        self.assertIn("salt", document)

    def test_wrong_passphrase_is_rejected(self):
        blob = export_profile_bundle(self.PROFILES, "right")
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(blob, "wrong")
        self.assertIn("passphrase", str(ctx.exception).lower())

    def test_tampered_payload_is_rejected(self):
        blob = export_profile_bundle(self.PROFILES, "pw")
        document = json.loads(blob.decode())
        document["data"] = document["data"][:-6] + "AAAAA="
        with self.assertRaises(BundleError):
            import_profile_bundle(json.dumps(document).encode(), "pw")

    def test_salt_differs_between_exports(self):
        first = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        second = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        self.assertNotEqual(first["salt"], second["salt"])

    def test_garbage_and_missing_fields(self):
        with self.assertRaises(BundleError):
            import_profile_bundle(b"not json at all", "pw")
        with self.assertRaises(BundleError):
            import_profile_bundle(b'{"hello": 1}', "pw")

    def test_unsupported_version(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["version"] = 99
        with self.assertRaises(BundleError):
            import_profile_bundle(json.dumps(document).encode(), "pw")

    def test_export_requires_a_passphrase(self):
        with self.assertRaises(BundleError):
            export_profile_bundle(self.PROFILES, "")


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
        keys = [k for k, _s, _m in hits]
        self.assertIn("docs/Report.pdf", keys)          # case-insensitive
        self.assertIn("docs/sub/report_final.txt", keys)  # recursive
        self.assertNotIn("docs/notes.txt", keys)

    def test_skips_folder_placeholders(self):
        self.m._client = self._client()
        hits = self.m.search_keys("docs/", "sub")
        keys = [k for k, _s, _m in hits]
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

    def test_default_is_one_hour_get(self):
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        self.assertEqual(dlg._expires_sec(), 3600)          # 1 Hour default
        self.assertEqual(model.calls[-1], ("get", "a/f.txt", 3600))
        self.assertTrue(dlg._url.text().startswith("https://get/"))

    def test_switch_to_put(self):
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        dlg._type.setCurrentIndex(1)  # Upload (PUT) -> triggers regenerate
        self.assertEqual(model.calls[-1][0], "put")
        self.assertTrue(dlg._url.text().startswith("https://put/"))

    def test_expiry_units_and_clamp(self):
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
        out = []
        for i in range(bc._lay.count()):
            w = bc._lay.itemAt(i).widget()
            if isinstance(w, QToolButton):
                out.append(w)
        return out

    def test_bucket_list_mode_only_home(self):
        bc = Breadcrumb()
        bc.set_location("", "", True)
        segs = self._segments(bc)
        self.assertEqual([s.text() for s in segs], ["Buckets"])
        self.assertFalse(segs[0].isEnabled())  # current location, not clickable

    def test_nested_prefix_segments(self):
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
        bc = Breadcrumb()
        bc.set_location("mybucket", "", False)
        fired = []
        bc.home.connect(lambda: fired.append(True))
        self._segments(bc)[0].click()  # "Buckets"
        self.assertEqual(fired, [True])


class GetSizeTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_file_key_matches_exactly(self):
        """REGRESSION: Properties size for 'a.txt' also counted sibling keys
        that merely share the name as a string prefix (e.g. 'a.txt.bak')."""
        self.m._client = FakeS3Client(list_pages=[{
            "Contents": [
                {"Key": "docs/a.txt", "Size": 10},
                {"Key": "docs/a.txt.bak", "Size": 90},
            ],
        }])
        self.assertEqual(self.m.get_size("docs/a.txt"), 10)

    def test_missing_file_key_is_zero(self):
        self.m._client = FakeS3Client(list_pages=[{"Contents": []}])
        self.assertEqual(self.m.get_size("docs/none.txt"), 0)

    def test_folder_key_sums_recursively(self):
        self.m._client = FakeS3Client(list_pages=[{
            "Contents": [
                {"Key": "docs/", "Size": 0},
                {"Key": "docs/a.txt", "Size": 10},
                {"Key": "docs/sub/b.txt", "Size": 5},
            ],
        }])
        self.assertEqual(self.m.get_size("docs/"), 15)


class RedirectConflictTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_service_endpoint_is_not_a_conflict(self):
        """REGRESSION: service-level redirect hints such as
        's3.us-west-2.amazonaws.com' were rejected as "bound to bucket 's3'"
        (the old check compared the leftmost label against itself), which
        broke PermanentRedirect recovery."""
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://s3.us-west-2.amazonaws.com", "mybucket"))

    def test_same_bucket_endpoint_is_not_a_conflict(self):
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://mybucket.s3.us-west-2.amazonaws.com", "mybucket"))

    def test_other_bucket_endpoint_conflicts(self):
        self.assertTrue(self.m._redirect_conflicts_with_bucket(
            "https://other.s3.us-west-2.amazonaws.com", "mybucket"))
        # legacy dash-style regional host
        self.assertTrue(self.m._redirect_conflicts_with_bucket(
            "https://other.s3-us-west-2.amazonaws.com", "mybucket"))

    def test_non_aws_hosts_are_not_conflicts(self):
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://minio.company.local", "mybucket"))
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://localhost:9000", "mybucket"))


class DownloadTraversalGuardTests(unittest.TestCase):
    """REGRESSION: keys containing '..' segments must never write outside the
    download target directory."""

    def test_unsafe_keys_are_skipped(self):
        m = make_model(bucket="b")
        c = FakeS3Client(list_pages=[{
            "Contents": [
                {"Key": "docs/ok.txt", "Size": 1},
                {"Key": "docs/../../evil.txt", "Size": 1},
                {"Key": "docs/../evil2/", "Size": 0},
            ],
        }])
        m._client = c
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("docs/", None, tmp, log_fn=logs.append)
            base = os.path.abspath(os.path.join(tmp, "docs"))
            downloaded = [kw["args"][2] for kw in c.calls_of("download_file")]
            self.assertEqual(len(downloaded), 1)
            self.assertEqual(os.path.abspath(downloaded[0]),
                             os.path.join(base, "ok.txt"))
            # the placeholder dir with '..' must not be created outside base
            self.assertFalse(os.path.exists(os.path.join(tmp, "evil2")))
        self.assertTrue(any("unsafe" in ln for ln in logs))


class CopyPrefixGuardTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_copy_into_own_subtree_rejected(self):
        """REGRESSION: 'move photos/ -> photos/2024/' copied the tree into
        itself and the follow-up delete of the source prefix then wiped the
        fresh copy (data loss)."""
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.copy_prefix("photos/", "photos/2024/photos/")

    def test_copy_onto_itself_rejected(self):
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.copy_prefix("photos/", "photos/")

    def test_sibling_prefix_sharing_name_is_allowed(self):
        c = FakeS3Client(list_pages=[
            {"Contents": [{"Key": "photos/a.jpg", "Size": 1}]},
        ])
        self.m._client = c
        self.m.copy_prefix("photos/", "photos-old/")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["Key"], "photos-old/a.jpg")

    def test_same_prefix_in_other_bucket_is_allowed(self):
        c = FakeS3Client(list_pages=[
            {"Contents": [{"Key": "photos/a.jpg", "Size": 1}]},
        ])
        self.m._client = c
        self.m.copy_prefix("photos/", "photos/", dst_bucket="b2")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["Bucket"], "b2")


class DestInsideSourceTests(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(_dest_inside_source("a/f.txt", "a/f.txt", False))
        self.assertTrue(_dest_inside_source("a/", "a/", True))

    def test_folder_nesting(self):
        self.assertTrue(
            _dest_inside_source("photos/", "photos/2024/photos/", True))
        self.assertFalse(
            _dest_inside_source("photos/", "archive/photos/", True))
        self.assertFalse(
            _dest_inside_source("photos/", "photos-old/", True))

    def test_files_do_not_nest(self):
        # 'a/f.txt.bak' starts with 'a/f.txt' as a string, but a file is not
        # a prefix — copying next to it must stay allowed.
        self.assertFalse(_dest_inside_source("a/f.txt", "a/f.txt.bak", False))


class BuildUploadJobTests(unittest.TestCase):
    def test_directory_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "proj")
            os.makedirs(os.path.join(root, "sub"))
            with open(os.path.join(root, "a.txt"), "w") as f:
                f.write("x")
            with open(os.path.join(root, "sub", "b.txt"), "w") as f:
                f.write("y")
            job = _build_upload_job_for_path(root, "dest/")
            entries = dict(job)
            # placeholder entries for each directory level (local side None)
            self.assertIsNone(entries["dest/proj"])
            self.assertIsNone(entries["dest/proj/sub"])
            self.assertEqual(entries["dest/proj/a.txt"],
                             os.path.join(root, "a.txt"))
            self.assertEqual(entries["dest/proj/sub/b.txt"],
                             os.path.join(root, "sub", "b.txt"))
            self.assertEqual(len(job), 4)

    def test_single_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "f.bin")
            open(p, "wb").close()
            self.assertEqual(_build_upload_job_for_path(p, ""), [("f.bin", p)])
            self.assertEqual(_build_upload_job_for_path(p, "a/b/"),
                             [("a/b/f.bin", p)])

    def test_trailing_separator_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "d")
            os.makedirs(root)
            job = _build_upload_job_for_path(root + os.sep, "pre/")
            self.assertEqual(job, [("pre/d", None)])

    def test_empty_path_yields_nothing(self):
        self.assertEqual(_build_upload_job_for_path("", "dest/"), [])


class TransferConcurrencyTests(unittest.TestCase):
    def test_default_applied_to_both_configs(self):
        m = make_model()
        self.assertEqual(m.transfer_concurrency,
                         Model.DEFAULT_TRANSFER_CONCURRENCY)
        self.assertEqual(m.transfer_cfg_download.max_concurrency,
                         m.transfer_concurrency)
        self.assertEqual(m.transfer_cfg_upload.max_concurrency,
                         m.transfer_concurrency)

    def test_explicit_value_and_clamping(self):
        m = make_model(transfer_concurrency=8)
        self.assertEqual(m.transfer_concurrency, 8)
        self.assertEqual(m.transfer_cfg_download.max_concurrency, 8)
        self.assertEqual(m.set_transfer_concurrency(0), 1)
        self.assertEqual(m.set_transfer_concurrency(999),
                         Model.MAX_TRANSFER_CONCURRENCY)
        self.assertEqual(m.set_transfer_concurrency("garbage"),
                         Model.DEFAULT_TRANSFER_CONCURRENCY)
        self.assertEqual(m.transfer_cfg_upload.max_concurrency,
                         Model.DEFAULT_TRANSFER_CONCURRENCY)

    def test_clone_inherits_concurrency(self):
        m = make_model(transfer_concurrency=6)
        c = m.clone_for_worker()
        self.assertEqual(c.transfer_concurrency, 6)
        self.assertEqual(c.transfer_cfg_download.max_concurrency, 6)


class ListingSummaryTests(unittest.TestCase):
    def test_counts_and_size(self):
        items = [
            Item("a", FSObjectType.FOLDER, "", 0),
            Item("b.txt", FSObjectType.FILE, "", 1024),
            Item("c.txt", FSObjectType.FILE, "", 1024),
        ]
        self.assertEqual(_listing_summary(items),
                         "1 dir(s), 2 file(s), 2.0 KB")

    def test_empty_and_none(self):
        self.assertEqual(_listing_summary([]), "0 dir(s), 0 file(s), 0.0 B")
        self.assertEqual(_listing_summary(None), "0 dir(s), 0 file(s), 0.0 B")

    def test_buckets_are_not_counted(self):
        items = [Item("bk", FSObjectType.BUCKET, "", 0)]
        self.assertEqual(_listing_summary(items),
                         "0 dir(s), 0 file(s), 0.0 B")


class MultipartUploadTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(
            mpu_pages=[{
                "Uploads": [
                    {"Key": "a/old.bin", "UploadId": "u1",
                     "Initiated": _dt(1), "StorageClass": "STANDARD"},
                    {"Key": "a/new.bin", "UploadId": "u2",
                     "Initiated": _dt(5), "StorageClass": "STANDARD"},
                    {"Key": "a/broken.bin"},  # no UploadId -> skipped
                ],
            }],
            parts_pages=[{"Parts": [{"Size": 100}, {"Size": 50}]}],
        )

    def test_lists_newest_first_and_skips_incomplete_entries(self):
        self.m._client = self._client()
        out = self.m.list_multipart_uploads()
        self.assertEqual([u["key"] for u in out], ["a/new.bin", "a/old.bin"])
        self.assertEqual(out[0]["upload_id"], "u2")
        self.assertIsNone(out[0]["size"])  # sizes not requested

    def test_with_sizes_sums_parts(self):
        c = self._client()
        self.m._client = c
        out = self.m.list_multipart_uploads(with_sizes=True)
        self.assertTrue(all(u["size"] == 150 for u in out))
        self.assertEqual(
            [kw["name"] for kw in c.calls_of("get_paginator")].count("list_parts"),
            2,
        )

    def test_abort_passes_upload_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.abort_multipart_upload("a/old.bin", "u1")
        self.assertEqual(
            c.calls_of("abort_multipart_upload"),
            [{"Bucket": "b", "Key": "a/old.bin", "UploadId": "u1"}],
        )

    def test_requires_a_bucket(self):
        m = make_model(bucket="")
        m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            m.list_multipart_uploads()
        with self.assertRaises(ValueError):
            m.abort_multipart_upload("k", "u")

    def test_recursive_bucket_delete_aborts_uploads(self):
        """REGRESSION: orphaned multipart parts are invisible to
        ListObjectsV2 and make DeleteBucket fail, so the recursive delete
        must abort them too."""
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "cur.txt"}]}],
            versions_pages=[{}],
            mpu_pages=[{"Uploads": [
                {"Key": "half.bin", "UploadId": "u9", "Initiated": _dt(1)},
            ]}],
        )
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.delete_bucket_recursive("mybucket")

        self.assertEqual(
            c.calls_of("abort_multipart_upload"),
            [{"Bucket": "mybucket", "Key": "half.bin", "UploadId": "u9"}],
        )
        self.assertEqual(len(c.calls_of("delete_bucket")), 1)


class DeleteBucketsWorkerTests(unittest.TestCase):
    """Bucket deletion runs through the transfer queue so the UI stays live."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self, failing=()):
            self.recursive = []
            self.plain = []
            self._failing = set(failing)

        def delete_bucket_recursive(self, name, cancel_event=None, log_fn=None):
            if name in self._failing:
                raise RuntimeError("bucket not empty")
            self.recursive.append(name)

        def delete_bucket(self, name):
            if name in self._failing:
                raise RuntimeError("bucket not empty")
            self.plain.append(name)

    def _run(self, job, failing=()):
        model = self.FakeModel(failing)
        worker = Worker(model, job)
        errors, finished = [], []
        worker.error.connect(errors.append)
        worker.finished.connect(finished.append)
        worker.delete_buckets()
        return model, errors, finished

    def test_routes_recursive_and_plain(self):
        model, errors, finished = self._run([("a", True), ("b", False)])
        self.assertEqual(model.recursive, ["a"])
        self.assertEqual(model.plain, ["b"])
        self.assertEqual(errors, [])
        self.assertEqual(finished, [False])

    def test_one_failure_does_not_abort_the_batch(self):
        model, errors, finished = self._run(
            [("a", True), ("bad", True), ("c", True)], failing={"bad"})
        # every healthy bucket still processed
        self.assertEqual(model.recursive, ["a", "c"])
        self.assertEqual(len(errors), 1)
        self.assertIn("bad", errors[0])
        self.assertEqual(finished, [False])

    def test_cancel_stops_early(self):
        model = self.FakeModel()
        worker = Worker(model, [("a", True), ("b", True)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.delete_buckets()
        self.assertEqual(model.recursive, [])
        self.assertEqual(finished, [True])  # cancelled


class CrossBucketCopyMoveTests(unittest.TestCase):
    """copy/move jobs carry a destination bucket (None = same bucket)."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.copies = []
            self.prefixes = []
            self.deletes = []

        def copy_object(self, src, dst, dst_bucket=None, log_fn=None):
            self.copies.append((src, dst, dst_bucket))

        def copy_prefix(self, src, dst, dst_bucket=None, log_fn=None,
                        cancel_event=None):
            self.prefixes.append((src, dst, dst_bucket))

        def delete(self, key, log_fn=None):
            self.deletes.append(key)

    def test_copy_forwards_dst_bucket(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "a.txt", False, "other"),
                       ("d/", "d/", True, "other")]).copy()
        self.assertEqual(model.copies, [("a.txt", "a.txt", "other")])
        self.assertEqual(model.prefixes, [("d/", "d/", "other")])
        self.assertEqual(model.deletes, [])

    def test_same_bucket_passes_none(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "b.txt", False, None)]).copy()
        self.assertEqual(model.copies, [("a.txt", "b.txt", None)])

    def test_move_deletes_sources_after_all_copies(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "a.txt", False, "other"),
                       ("b.txt", "b.txt", False, "other")]).move()
        self.assertEqual(len(model.copies), 2)
        self.assertEqual(model.deletes, ["a.txt", "b.txt"])

    def test_move_failure_leaves_sources_intact(self):
        """A copy error must not fall through to deleting the originals."""
        class Failing(self.FakeModel):
            def copy_object(self, src, dst, dst_bucket=None, log_fn=None):
                raise RuntimeError("access denied")

        model = Failing()
        worker = Worker(model, [("a.txt", "a.txt", False, "other")])
        errors = []
        worker.error.connect(errors.append)
        worker.move()
        self.assertEqual(model.deletes, [])
        self.assertEqual(len(errors), 1)


class CopyMoveDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        bucket = "src"

        def clone_for_worker(self):
            return self

        def list_buckets(self):
            raise RuntimeError("offline")  # dialog must still work

    def _dlg(self):
        dlg = CopyMoveDialog(None, self.FakeModel(), 2, "a/")
        # The dialog loads buckets on a QThread; close() joins it so the
        # thread never outlives the widget.
        self.addCleanup(dlg.close)
        return dlg

    def test_defaults_to_source_bucket(self):
        dlg = self._dlg()
        self.assertEqual(dlg.destination_bucket(), "src")
        self.assertFalse(dlg.is_cross_bucket())
        self.assertEqual(dlg.destination(), "a/")

    def test_typing_another_bucket_is_cross_bucket(self):
        dlg = self._dlg()
        dlg.bucket_combo.setCurrentText("other")
        self.assertTrue(dlg.is_cross_bucket())
        self.assertEqual(dlg.destination_bucket(), "other")

    def test_blank_bucket_falls_back_to_source(self):
        dlg = self._dlg()
        dlg.bucket_combo.setCurrentText("   ")
        self.assertEqual(dlg.destination_bucket(), "src")
        self.assertFalse(dlg.is_cross_bucket())


class _FakeMainWindow:
    """Stands in for MainWindow in dialog tests (log + status bar only)."""

    def __init__(self):
        self.logs = []

    def log(self, msg):
        self.logs.append(msg)

    def statusBar(self):
        class _SB:
            def showMessage(self, *_a, **_kw):
                pass
        return _SB()


class _FakeUploadsModel:
    bucket = "b"

    def __init__(self, uploads=None, fail=False):
        self._uploads = uploads or []
        self._fail = fail
        self.aborted = []
        self.cancel_events = []

    def clone_for_worker(self):
        return self

    def list_multipart_uploads(self, prefix="", with_sizes=False,
                               cancel_event=None, **_kw):
        self.cancel_events.append(cancel_event)
        if self._fail:
            raise RuntimeError("denied")
        return [u for u in self._uploads if u["key"].startswith(prefix or "")]

    def abort_multipart_upload(self, key, upload_id, **_kw):
        self.aborted.append((key, upload_id))


class IncompleteUploadsDialogTests(unittest.TestCase):
    """The dialog loads on a worker thread, so each test pumps the event loop
    until the load settles."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model, prefix=""):
        dlg = IncompleteUploadsDialog(None, _FakeMainWindow(), model, prefix)
        self.addCleanup(dlg.close)
        self._settle(dlg)
        return dlg

    def _uploads(self):
        return [
            {"key": "a/one.bin", "upload_id": "u1", "initiated": _dt(1),
             "storage_class": "STANDARD", "size": 1024},
            {"key": "a/two.bin", "upload_id": "u2", "initiated": _dt(2),
             "storage_class": "STANDARD", "size": 2048},
        ]

    def test_lists_uploads_and_reports_wasted_bytes(self):
        dlg = self._dlg(_FakeUploadsModel(self._uploads()))
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertIn("2 incomplete upload(s)", dlg._info.text())
        self.assertIn("3.0 KB", dlg._info.text())  # 1024 + 2048 still billed

    def test_empty_state(self):
        dlg = self._dlg(_FakeUploadsModel([]))
        self.assertEqual(dlg._table.rowCount(), 0)
        self.assertIn("No incomplete multipart uploads", dlg._info.text())

    def test_load_failure_is_reported_not_raised(self):
        dlg = self._dlg(_FakeUploadsModel(fail=True))
        self.assertIn("Could not list", dlg._info.text())
        self.assertEqual(dlg._table.rowCount(), 0)

    def test_abort_selected_calls_model_for_each_row(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        dlg._table.selectAll()
        with patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.Yes):
            dlg._abort_selected()
        self._settle(dlg)
        self.assertEqual(sorted(model.aborted),
                         [("a/one.bin", "u1"), ("a/two.bin", "u2")])

    def test_abort_is_cancellable(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        dlg._table.selectAll()
        with patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.No):
            dlg._abort_selected()
        self.assertEqual(model.aborted, [])

    def test_abort_older_than_filters_by_age(self):
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        uploads = [
            {"key": "old.bin", "upload_id": "u1", "initiated": _dt(1),
             "storage_class": "", "size": 10},
            {"key": "recent.bin", "upload_id": "u2", "initiated": recent,
             "storage_class": "", "size": 10},
        ]
        model = _FakeUploadsModel(uploads)
        dlg = self._dlg(model)
        with patch("main_window.QInputDialog.getInt", return_value=(7, True)), \
             patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.Yes):
            dlg._abort_older_than()
        self._settle(dlg)
        # only the 2026-01-01 upload is older than 7 days
        self.assertEqual(model.aborted, [("old.bin", "u1")])

    def test_scan_receives_a_cancel_event(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        self.assertTrue(model.cancel_events)
        self.assertIsNotNone(model.cancel_events[0])

    def test_closing_mid_scan_cancels_instead_of_orphaning_the_thread(self):
        """Sizing each upload costs a ListParts call, so a scan can run for
        minutes. Closing must signal cancellation; otherwise the join times out
        and the dialog is destroyed with its thread still running (SIGABRT)."""
        started = threading.Event()
        captured = []

        class Blocking(_FakeUploadsModel):
            def list_multipart_uploads(self, prefix="", with_sizes=False,
                                       cancel_event=None, **_kw):
                captured.append(cancel_event)
                started.set()
                while not cancel_event.is_set():   # emulate a long scan
                    time.sleep(0.005)
                raise TransferCancelled("cancelled")

        dlg = IncompleteUploadsDialog(None, _FakeMainWindow(), Blocking([]), "")
        self.addCleanup(dlg.close)
        self.assertTrue(started.wait(5))
        self.assertFalse(captured[0].is_set())

        began = time.monotonic()
        dlg.close()
        elapsed = time.monotonic() - began
        self.assertTrue(captured[0].is_set())
        # a cancelled scan joins promptly; a timed-out join would take ~2s
        self.assertLess(elapsed, 1.5)

    def test_prefix_scope_limits_the_scan(self):
        uploads = self._uploads() + [
            {"key": "elsewhere/x.bin", "upload_id": "u9", "initiated": _dt(3),
             "storage_class": "", "size": 5},
        ]
        dlg = self._dlg(_FakeUploadsModel(uploads), prefix="a/")
        self.assertTrue(dlg._scope.isChecked())
        self.assertEqual(dlg._table.rowCount(), 2)  # 'elsewhere/' excluded


class _FakeVersionsModel:
    bucket = "b"

    def __init__(self, versions=None, fail=False):
        self._versions = versions or []
        self._fail = fail

    def clone_for_worker(self):
        return self

    def list_object_versions(self, key):
        if self._fail:
            raise RuntimeError("denied")
        return self._versions

    def get_bucket_versioning_status(self):
        return "Enabled"


class VersionsDialogAsyncTests(unittest.TestCase):
    """REGRESSION: the version list and bucket versioning status were fetched
    on the main thread, freezing the dialog on buckets with many versions."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._list_thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model):
        dlg = VersionsDialog(None, _FakeMainWindow(), model, "a/f.txt")
        self.addCleanup(dlg.close)
        return dlg

    def test_rows_appear_after_the_background_load(self):
        versions = [
            {"version_id": "v2", "last_modified": _dt(2), "size": 20,
             "is_latest": True, "storage_class": "STANDARD",
             "is_delete_marker": False, "etag": "e2"},
            {"version_id": "v1", "last_modified": _dt(1), "size": 10,
             "is_latest": False, "storage_class": "STANDARD",
             "is_delete_marker": False, "etag": "e1"},
        ]
        dlg = self._dlg(_FakeVersionsModel(versions))
        # constructor must not block on the network
        self.assertEqual(dlg._info.text(), "Loading versions…")
        self._settle(dlg)
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertIn("2 version(s)", dlg._info.text())
        self.assertIn("Enabled", dlg._info.text())

    def test_load_failure_renders_empty(self):
        dlg = self._dlg(_FakeVersionsModel(fail=True))
        with patch("main_window.QMessageBox.warning", return_value=None):
            self._settle(dlg)
        self.assertEqual(dlg._table.rowCount(), 0)
        self.assertEqual(dlg._versions, [])


class _FakePropsModel:
    bucket = "b"
    endpoint_url = "https://s3.amazonaws.com"

    def __init__(self, head=None, size=0, head_error=False):
        self._head = head or {}
        self._size = size
        self._head_error = head_error
        self.head_calls = []
        self.size_calls = []

    def clone_for_worker(self):
        return self

    def object_properties(self, key):
        self.head_calls.append(key)
        if self._head_error:
            raise RuntimeError("no such object")
        return self._head

    def get_size(self, key):
        self.size_calls.append(key)
        return self._size

    @staticmethod
    def parse_restore_status(v):
        return Model.parse_restore_status(v)

    def direct_object_url(self, key):
        return f"{self.endpoint_url}/{self.bucket}/{key}"

    def _endpoint_has_bucket(self, ep, b):
        return False


class PropertiesWindowTests(unittest.TestCase):
    """REGRESSION: Properties recursively listed the prefix on the main thread
    (freezing the app on large folders) and re-fetched a file's size that
    head_object had already returned."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._size_thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model, key):
        dlg = PropertiesWindow(None, settings=(model, key))
        self.addCleanup(dlg.close)
        return dlg

    def test_file_size_comes_from_head_without_a_listing(self):
        model = _FakePropsModel(head={"ContentLength": 4096, "ETag": '"abc"'})
        dlg = self._dlg(model, "a/f.txt")
        self.assertEqual(dlg.size.text(), "4096 Bytes")
        self.assertEqual(model.head_calls, ["a/f.txt"])
        self.assertEqual(model.size_calls, [])  # no extra recursive listing
        self.assertEqual(dlg.eTag.text(), "abc")

    def test_folder_size_is_computed_off_the_main_thread(self):
        model = _FakePropsModel(size=1234)
        dlg = self._dlg(model, "a/dir/")
        # must not block the constructor
        self.assertEqual(dlg.size.text(), "Calculating…")
        self.assertEqual(model.head_calls, [])  # folders have no object to HEAD
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "1234 Bytes")
        self.assertEqual(model.size_calls, ["a/dir/"])

    def test_missing_object_falls_back_to_a_listing(self):
        model = _FakePropsModel(head_error=True, size=77)
        dlg = self._dlg(model, "a/implicit")
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "77 Bytes")

    def test_size_failure_reports_na(self):
        class Failing(_FakePropsModel):
            def get_size(self, key):
                raise RuntimeError("denied")

        dlg = self._dlg(Failing(head_error=True), "a/dir/")
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "N/A")


class DialogLoadFailureTests(unittest.TestCase):
    """REGRESSION: when the initial load fails, Save must be disabled —
    saving would replace the object's real tags/metadata with the empty
    dialog contents."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_tags_dialog_disables_save_on_load_failure(self):
        class FailingModel:
            def get_object_tags(self, key):
                raise RuntimeError("boom")

        with patch("main_window.QMessageBox.warning", return_value=None):
            dlg = TagsDialog(None, FailingModel(), "a/f.txt")
        save = dlg._btns.button(QDialogButtonBox.StandardButton.Save)
        self.assertFalse(save.isEnabled())

    def test_tags_dialog_save_enabled_on_success(self):
        class OkModel:
            def get_object_tags(self, key):
                return [{"Key": "k", "Value": "v"}]

        dlg = TagsDialog(None, OkModel(), "a/f.txt")
        save = dlg._btns.button(QDialogButtonBox.StandardButton.Save)
        self.assertTrue(save.isEnabled())
        self.assertEqual(dlg._table.rowCount(), 1)

    def test_metadata_dialog_disables_save_on_load_failure(self):
        class FailingModel:
            def get_object_metadata(self, key):
                raise RuntimeError("boom")

        with patch("main_window.QMessageBox.warning", return_value=None):
            dlg = MetadataDialog(None, FailingModel(), "a/f.txt")
        save = dlg.buttonBox.button(QDialogButtonBox.StandardButton.Save)
        self.assertFalse(save.isEnabled())


class _StubModel(Model):
    """Model subclass that answers listings from memory (no network)."""

    def list_buckets(self):
        return [Item("bkt", FSObjectType.BUCKET, "", 0)]

    def list(self, fld):
        return []


class _FakeRunningThread:
    """Stands in for a QThread that is still running."""

    def isRunning(self):
        return True


class MainWindowGuardTests(unittest.TestCase):
    """Window-level regressions: the shared-model race during bucket entry and
    QAction's 'checked' argument leaking into an optional slot parameter."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        # A private QSettings scope so tests never touch the real config.
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                self.settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(self.win.close)
        self._settle()

    def _settle(self, timeout=5.0):
        # navigate() disables the list view and _on_navigation_finished
        # re-enables it, so that flag (not _nav_thread, which is only cleared
        # by the *next* navigate) marks the end of a navigation.
        deadline = time.monotonic() + timeout
        while (not self.win.listview.isEnabled()
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._app.processEvents()

    def test_navigation_is_blocked_while_entering_a_bucket(self):
        """BucketEnterWorker mutates the shared model, so cloning it mid-entry
        would snapshot half-updated connection state."""
        self.win._bucket_enter_thread = _FakeRunningThread()
        self.assertTrue(self.win.bucket_enter_active())
        seq_before = self.win._nav_seq
        self.win.navigate()
        self.assertEqual(self.win._nav_seq, seq_before)  # no navigation started

        self.win._bucket_enter_thread = None
        self.assertFalse(self.win.bucket_enter_active())
        self.win.navigate()
        self.assertEqual(self.win._nav_seq, seq_before + 1)
        self._settle()

    def test_refresh_action_does_not_pass_checked_as_restore_name(self):
        """REGRESSION: triggered=self.navigate handed QAction's 'checked' bool
        to navigate(restore_name=...)."""
        self.win.btnRefresh.trigger()
        self.assertIsNone(self.win._nav_pending_restore_name)
        self._settle()

    def test_upload_actions_do_not_pass_checked_as_folder(self):
        """Same trap for upload(folder=...) / upload_folder(folder=...): the
        bool must never reach the destination-prefix parameter."""
        seen = []
        # Both actions are disabled in bucket-list mode; this test is about
        # argument passing, not enablement.
        self.win.btnUpload.setEnabled(True)
        self.win.btnUploadFolder.setEnabled(True)
        with patch.object(main_window.MainWindow, "upload",
                          lambda _s, folder=None: seen.append(("upload", folder))):
            self.win.btnUpload.trigger()
        with patch.object(main_window.MainWindow, "upload_folder",
                          lambda _s, folder=None: seen.append(("folder", folder))):
            self.win.btnUploadFolder.trigger()
        self.assertEqual(seen, [("upload", None), ("folder", None)])


class TransferSettingsDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self, **kw):
        opts = dict(
            concurrency=4, max_concurrency=16,
            storage_classes=("",) + tuple(Model.STORAGE_CLASSES),
            sse_modes=Model.SSE_MODES,
        )
        opts.update(kw)
        dlg = TransferSettingsDialog(None, **opts)
        self.addCleanup(dlg.close)
        return dlg

    def test_round_trips_existing_values(self):
        dlg = self._dlg(concurrency=7, storage_class="GLACIER", sse="AES256")
        self.assertEqual(dlg.concurrency(), 7)
        self.assertEqual(dlg.storage_class(), "GLACIER")
        self.assertEqual(dlg.sse(), "AES256")

    def test_kms_field_only_enabled_for_kms(self):
        dlg = self._dlg(sse="AES256", kms_key_id="key-1")
        self.assertFalse(dlg._kms.isEnabled())
        self.assertEqual(dlg.kms_key_id(), "")   # dropped for non-kms mode
        dlg._sse.setCurrentIndex(dlg._sse.findData("aws:kms"))
        self.assertTrue(dlg._kms.isEnabled())
        self.assertEqual(dlg.kms_key_id(), "key-1")

    def test_defaults_are_empty(self):
        dlg = self._dlg()
        self.assertEqual(dlg.storage_class(), "")
        self.assertEqual(dlg.sse(), "")

    def test_parallel_files_and_verify_round_trip(self):
        dlg = self._dlg(parallel_files=9, verify_downloads=True)
        self.assertEqual(dlg.parallel_files(), 9)
        self.assertTrue(dlg.verify_downloads())
        dlg._verify.setChecked(False)
        self.assertFalse(dlg.verify_downloads())


class OverwriteDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_choices(self):
        dlg = OverwriteDialog(None, ["a", "b"], total=5, what="file")
        self.addCleanup(dlg.close)
        self.assertIsNone(dlg.choice())
        dlg._pick(OverwriteDialog.OVERWRITE)
        self.assertEqual(dlg.choice(), OverwriteDialog.OVERWRITE)


class ParseS3LocationTests(unittest.TestCase):
    def test_full_uri(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://bkt/a/b/"),
            ("bkt", "a/b/"),
        )
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://bkt"), ("bkt", ""))

    def test_uri_wins_over_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://other/x", "cur"),
            ("other", "x/"),
        )

    def test_bare_prefix_keeps_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("a/b", "cur"),
            ("cur", "a/b/"),
        )
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("/a/b/", "cur"),
            ("cur", "a/b/"),
        )

    def test_bucket_slash_prefix_without_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("bkt/a", ""), ("bkt", "a/"))

    def test_empty(self):
        self.assertEqual(main_window.MainWindow._parse_s3_location("", "cur"),
                         ("", ""))
        self.assertEqual(main_window.MainWindow._parse_s3_location("   "),
                         ("", ""))


class QueueRetryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_retry_button_only_for_unfinished_jobs(self):
        entry = main_window._QEntry(1, "upload", [("k", "/tmp/f")], label="Upload")
        row = main_window._QueueRow(entry)
        self.addCleanup(row.deleteLater)
        self.assertFalse(row._retry_btn.isVisible())
        row.set_status("done")
        self.assertFalse(row._retry_btn.isVisible())
        row.set_status("error")
        self.assertTrue(row._retry_btn.isVisibleTo(row))
        row.set_status("cancelled")
        self.assertTrue(row._retry_btn.isVisibleTo(row))


class MainWindowBatchTests(unittest.TestCase):
    """Window-level behaviour of the new batch: selection summary, view-state
    persistence, binding-cache round-trip and queue retry."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _make_window(self, settings=None):
        settings = settings or QSettings("s3duck-tests", "s3duck-tests")
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False, "TOKEN",
            ))
        self.addCleanup(win.close)
        deadline = time.monotonic() + 5
        while not win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        return win

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        self.win = self._make_window(self.settings)

    def test_session_token_reaches_the_model(self):
        self.assertEqual(self.win.data_model.session_token, "TOKEN")

    def test_selection_summary_counts_multiple_rows(self):
        # bucket-list mode has a single bucket row -> no summary for one row
        self.assertEqual(self.win._selection_summary(), "")
        self.win.listview.selectAll()
        self.assertEqual(self.win._selection_summary(), "")  # still one bucket

    def test_binding_cache_survives_a_restart(self):
        self.win.data_model.binding_cache["bkt"] = (
            "https://s3.eu-north-1.amazonaws.com", "eu-north-1", True)
        self.win.close()          # writeSettings persists the cache
        again = self._make_window(self.settings)
        self.assertEqual(
            again.data_model.binding_cache.get("bkt"),
            ("https://s3.eu-north-1.amazonaws.com", "eu-north-1", True),
        )

    def test_view_state_survives_a_restart(self):
        self.win.listview.header().resizeSection(0, 411)
        self.win.close()
        again = self._make_window(self.settings)
        self.assertEqual(again.listview.header().sectionSize(0), 411)

    def test_retry_requeues_a_failed_entry(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation("upload", [("k", "/tmp/f")])
            self.assertEqual(len(started), 1)
            entry = started[0]
            entry.status = "error"
            self.win._on_queue_retry_requested(entry.entry_id)
        self.assertEqual(len(started), 2)
        self.assertEqual(started[1].job, entry.job)
        self.assertNotEqual(started[1].entry_id, entry.entry_id)

    def test_failed_transfer_is_marked_error_not_done(self):
        """REGRESSION: a transfer that raised still finished as "done", which
        also meant the queue offered no retry for it."""
        class FailingUpload:
            def upload_file(self, *_a, **_kw):
                raise RuntimeError("boom")

        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: FailingUpload()), \
             patch.object(main_window.QMessageBox, "critical",
                          lambda *_a, **_kw: None):
            self.win.assign_thread_operation(
                "upload", [("k", "/nonexistent/f")], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()

        entries = list(self.win._queue_entries.values())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].status, "error")
        self.assertIn("boom", entries[0].error or "")

    def test_retry_ignores_a_successful_entry(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation("upload", [("k", "/tmp/f")])
            started[0].status = "done"
            self.win._on_queue_retry_requested(started[0].entry_id)
        self.assertEqual(len(started), 1)

    def test_download_skips_existing_files_when_asked(self):
        job = [("a", "/tmp/exists", 1, "/tmp"), ("b", "/tmp/fresh", 1, "/tmp")]
        conflicts = {"/tmp/exists"}
        with patch.object(main_window, "OverwriteDialog") as dlg_cls:
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.choice.return_value = main_window.OverwriteDialog.SKIP
            remaining = self.win._resolve_overwrites(
                job, conflicts, what="file", index_of=lambda e: e[1])
        self.assertEqual([e[1] for e in remaining], ["/tmp/fresh"])

    def test_overwrite_choice_keeps_everything(self):
        job = [("a", "/tmp/exists", 1, "/tmp")]
        with patch.object(main_window, "OverwriteDialog") as dlg_cls:
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.choice.return_value = main_window.OverwriteDialog.OVERWRITE
            remaining = self.win._resolve_overwrites(
                job, {"/tmp/exists"}, what="file", index_of=lambda e: e[1])
        self.assertEqual(remaining, job)

    def test_cancelling_the_prompt_aborts(self):
        job = [("a", "/tmp/exists", 1, "/tmp")]
        with patch.object(main_window, "OverwriteDialog") as dlg_cls:
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Rejected
            self.assertIsNone(self.win._resolve_overwrites(
                job, {"/tmp/exists"}, what="file", index_of=lambda e: e[1]))

    def test_bulk_rename_builds_a_move_job(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = "pre/"
        targets = [("a.txt", "pre/a.txt", False), ("dir", "pre/dir/", True)]
        started = []
        with patch.object(main_window.MainWindow, "_collect_selected_targets",
                          lambda _s: targets), \
             patch.object(main_window, "BulkRenameDialog") as dlg_cls, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.plan.return_value = [("a.txt", "b.txt"), ("dir", "folder")]
            self.win.bulk_rename()

        self.assertEqual(len(started), 1)
        method, job = started[0]
        self.assertEqual(method, "move")
        # folders keep their trailing slash on both sides; files do not
        self.assertEqual(job, [
            ("pre/a.txt", "pre/b.txt", False, None),
            ("pre/dir/", "pre/folder/", True, None),
        ])

    def test_bulk_rename_respects_overwrite_skip(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = ""
        targets = [("a.txt", "a.txt", False), ("b.txt", "b.txt", False)]
        started = []
        with patch.object(main_window.MainWindow, "_collect_selected_targets",
                          lambda _s: targets), \
             patch.object(main_window, "BulkRenameDialog") as dlg_cls, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"x.txt"}), \
             patch.object(main_window, "OverwriteDialog") as ow_cls, \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Accepted
            dlg_cls.return_value.plan.return_value = [
                ("a.txt", "x.txt"), ("b.txt", "y.txt")]
            ow_cls.return_value.exec.return_value = QDialog.DialogCode.Accepted
            ow_cls.return_value.choice.return_value = OverwriteDialog.SKIP
            self.win.bulk_rename()

        self.assertEqual([e[1] for e in started[0][1]], ["y.txt"])

    def test_start_sync_maps_actions_to_local_paths(self):
        started = []
        actions = [
            {"action": "upload", "rel": "a/b.txt", "size": 5},
            {"action": "delete_remote", "rel": "old.txt", "size": 1},
        ]
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.start_sync(actions, "/tmp/local", "pre/", "upload")
        method, job = started[0]
        self.assertEqual(method, "sync")
        self.assertEqual(job[0][0], "upload")
        self.assertEqual(job[0][3], "pre/a/b.txt")
        self.assertEqual(job[0][2], os.path.join("/tmp/local", "a", "b.txt"))
        self.assertEqual(job[1][3], "pre/old.txt")

    def test_queue_drain_reports_the_batch_once(self):
        self.win._batch_stats = {"done": 2, "cancelled": 0, "error": 1}
        shown = []
        with patch.object(main_window.MainWindow, "_ensure_tray_icon",
                          lambda _s: None):
            self.win.statusBar().showMessage = lambda msg, *a: shown.append(msg)
            self.win._queue_start_next()
        self.assertTrue(shown)
        self.assertIn("failed", shown[-1])
        # stats reset, so a second drain stays quiet
        shown.clear()
        self.win._queue_start_next()
        self.assertEqual(shown, [])

    def test_read_only_disables_write_actions_and_marks_the_title(self):
        self.win.data_model.read_only = True
        self.win.enable_action_buttons()
        self.win.update_window_title()
        self.assertFalse(self.win.btnRemove.isEnabled())
        self.assertFalse(self.win.btnCreateFolder.isEnabled())
        self.assertFalse(self.win.btnUpload.isEnabled())
        self.assertFalse(self.win.btnUploadFolder.isEnabled())
        self.assertIn("read-only", self.win.windowTitle())
        # An armed undo is itself a write, so read-only must take it away
        # again — check it is genuinely enabled first, or this proves nothing.
        self.win.data_model.bucket = "bkt"
        self.win._undo_delete = {"bucket": "bkt", "keys": ["a.txt"]}
        self.win.data_model.read_only = False
        self.win.enable_action_buttons()
        self.assertTrue(self.win.btnUndoDelete.isEnabled())
        self.win.data_model.read_only = True
        self.win.enable_action_buttons()
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        # reading stays available
        self.assertTrue(self.win.btnDownload.isEnabled())

    def test_writable_profile_keeps_write_actions(self):
        self.win.data_model.read_only = False
        self.win.data_model.bucket = "bkt"
        self.win.enable_action_buttons()
        self.win.update_window_title()
        self.assertTrue(self.win.btnRemove.isEnabled())
        self.assertTrue(self.win.btnUpload.isEnabled())
        self.assertNotIn("read-only", self.win.windowTitle())

    def test_a_real_delete_arms_undo(self):
        """The arming happens in the transfer-finished handler, so drive an
        actual delete job rather than calling the recorder directly."""
        class FakeDeleter:
            def delete(self, key, log_fn=None, cancel_event=None):
                return True

        self.win.data_model.bucket = "bkt"
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: FakeDeleter()):
            self.win.assign_thread_operation(
                "delete", ["a.txt", "dir/"], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()

        self.assertIsNotNone(self.win._undo_delete)
        self.assertEqual(self.win._undo_delete["keys"], ["a.txt", "dir/"])
        self.assertTrue(self.win.btnUndoDelete.isEnabled())

    def test_delete_arms_undo_and_undo_queues_a_job(self):
        self.win.data_model.bucket = "bkt"
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        self.win._record_undoable_delete(["a.txt", "dir/"])
        self.assertTrue(self.win.btnUndoDelete.isEnabled())

        started = []
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.undo_delete()
        self.assertEqual(started, [("undelete", [("a.txt",), ("dir/",)])])
        # a one-shot action: it disarms once used
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_undo_is_dropped_when_the_bucket_changes(self):
        self.win.data_model.bucket = "bkt"
        self.win._record_undoable_delete(["a.txt"])
        self.win._return_to_bucket_list_mode()
        self.assertIsNone(self.win._undo_delete)
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_undo_refuses_when_the_bucket_no_longer_matches(self):
        self.win.data_model.bucket = "bkt"
        self.win._record_undoable_delete(["a.txt"])
        self.win.data_model.bucket = "other"
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.undo_delete()
        self.assertEqual(started, [])

    def test_undo_stays_disabled_on_a_read_only_profile(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.read_only = True
        self.win._record_undoable_delete(["a.txt"])
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_no_conflicts_runs_untouched(self):
        job = [("a", "/tmp/fresh", 1, "/tmp")]
        self.assertEqual(
            self.win._resolve_overwrites(
                job, set(), what="file", index_of=lambda e: e[1]),
            job,
        )


class BulkRenamePlanTests(unittest.TestCase):
    def test_find_replace_only_changed_entries(self):
        items = [("IMG_1.jpg", False), ("IMG_2.jpg", False), ("notes.txt", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_FIND, find="IMG_", replace="photo-")
        self.assertEqual(plan, [("IMG_1.jpg", "photo-1.jpg"),
                                ("IMG_2.jpg", "photo-2.jpg")])
        self.assertEqual(problems, [])

    def test_find_replace_is_case_sensitive_by_default(self):
        items = [("img_1.jpg", False)]
        plan, _ = bulk_rename_plan(items, find="IMG_", replace="x")
        self.assertEqual(plan, [])
        plan, _ = bulk_rename_plan(
            items, find="IMG_", replace="x", case_sensitive=False)
        self.assertEqual(plan, [("img_1.jpg", "x1.jpg")])

    def test_regex_with_backreference(self):
        items = [("2024-report.pdf", False)]
        plan, problems = bulk_rename_plan(
            items, find=r"(\d{4})-(\w+)", replace=r"\2-\1", regex=True)
        self.assertEqual(plan, [("2024-report.pdf", "report-2024.pdf")])
        self.assertEqual(problems, [])

    def test_invalid_regex_is_reported_not_raised(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="([", replace="", regex=True)
        self.assertEqual(plan, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("Invalid regular expression", problems[0])

    def test_literal_replacement_keeps_backslashes(self):
        plan, _ = bulk_rename_plan(
            [("a b.txt", False)], find=" ", replace="\\", case_sensitive=False)
        self.assertEqual(plan, [("a b.txt", "a\\b.txt")])

    def test_template_numbering_and_extension(self):
        items = [("b.txt", False), ("a.log", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_TEMPLATE,
            template="file-{n}{ext}", start=7, padding=3)
        self.assertEqual(plan, [("b.txt", "file-007.txt"),
                                ("a.log", "file-008.log")])
        self.assertEqual(problems, [])

    def test_template_uses_whole_name_for_folders(self):
        plan, _ = bulk_rename_plan(
            [("v1.2", True)], mode=BULK_RENAME_TEMPLATE,
            template="{name}-old{ext}")
        # a dotted folder name must not be split into stem/extension
        self.assertEqual(plan, [("v1.2", "v1.2-old")])

    def test_unknown_placeholder_reported(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False)], mode=BULK_RENAME_TEMPLATE, template="{nope}")
        self.assertEqual(plan, [])
        self.assertIn("Unknown placeholder", problems[0])

    def test_duplicate_targets_are_flagged(self):
        items = [("a.txt", False), ("b.txt", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_TEMPLATE, template="same.txt")
        self.assertEqual(len(plan), 2)
        self.assertEqual(len(problems), 1)
        self.assertIn("same.txt", problems[0])

    def test_slash_and_empty_results_rejected(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False), ("b.txt", False)],
            mode=BULK_RENAME_TEMPLATE, template="{name}")
        # "{name}" for b.txt -> "b" is fine; check slash/empty separately
        self.assertEqual(len(plan), 2)

        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="a.txt", replace="sub/a.txt")
        self.assertEqual(plan, [])
        self.assertIn("cannot contain '/'", problems[0])

        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="a.txt", replace="")
        self.assertEqual(plan, [])
        self.assertIn("empty", problems[0])

    def test_empty_find_is_a_no_op(self):
        self.assertEqual(
            bulk_rename_plan([("a.txt", False)], find="", replace="x"),
            ([], []),
        )


class BuildSyncPlanTests(unittest.TestCase):
    def test_upload_direction(self):
        local = {"new.txt": (10, 100.0), "same.txt": (5, 100.0),
                 "bigger.txt": (20, 100.0), "newer.txt": (5, 500.0)}
        remote = {"same.txt": (5, 100.0), "bigger.txt": (10, 100.0),
                  "newer.txt": (5, 100.0), "extra.txt": (7, 100.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        by_rel = {a["rel"]: a for a in actions}
        self.assertEqual(by_rel["new.txt"]["action"], "upload")
        self.assertEqual(by_rel["new.txt"]["reason"], "missing at destination")
        self.assertEqual(by_rel["bigger.txt"]["reason"], "size differs")
        self.assertEqual(by_rel["newer.txt"]["reason"], "source is newer")
        self.assertEqual(by_rel["same.txt"]["action"], "skip")
        # extras are kept unless explicitly asked for
        self.assertEqual(by_rel["extra.txt"]["action"], "skip")

    def test_delete_extra_opt_in(self):
        actions = build_sync_plan(
            {}, {"gone.txt": (1, 1.0)}, direction="upload", delete_extra=True)
        self.assertEqual(
            [(a["action"], a["rel"]) for a in actions],
            [("delete_remote", "gone.txt")],
        )

    def test_download_direction_mirrors(self):
        local = {"old.txt": (5, 100.0), "onlylocal.txt": (1, 1.0)}
        remote = {"old.txt": (5, 500.0), "fresh.txt": (9, 100.0)}
        actions = build_sync_plan(
            local, remote, direction="download", delete_extra=True)
        by_rel = {a["rel"]: a for a in actions}
        self.assertEqual(by_rel["fresh.txt"]["action"], "download")
        self.assertEqual(by_rel["old.txt"]["action"], "download")
        self.assertEqual(by_rel["old.txt"]["reason"], "source is newer")
        self.assertEqual(by_rel["onlylocal.txt"]["action"], "delete_local")

    def test_mtime_tolerance_absorbs_clock_granularity(self):
        local = {"a.txt": (5, 101.0)}
        remote = {"a.txt": (5, 100.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        self.assertEqual(actions[0]["action"], "skip")  # 1s < 2s tolerance

    def test_older_source_is_not_transferred(self):
        local = {"a.txt": (5, 100.0)}
        remote = {"a.txt": (5, 900.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        self.assertEqual(actions[0]["action"], "skip")

    def test_bad_direction_rejected(self):
        with self.assertRaises(ValueError):
            build_sync_plan({}, {}, direction="sideways")

    def test_summary_counts_and_bytes(self):
        actions = build_sync_plan(
            {"a": (10, 5.0), "b": (20, 5.0)}, {}, direction="upload")
        summary = summarize_sync_plan(actions)
        self.assertEqual(summary["upload"], 2)
        self.assertEqual(summary["bytes"], 30)


class ExcludeMatcherTests(unittest.TestCase):
    def test_no_patterns_excludes_nothing(self):
        match = build_exclude_matcher([])
        self.assertFalse(match("anything/at/all.txt"))
        self.assertFalse(build_exclude_matcher(None)("x"))

    def test_glob_matches_at_any_depth(self):
        match = build_exclude_matcher(["*.tmp"])
        self.assertTrue(match("a.tmp"))
        self.assertTrue(match("deep/nested/b.tmp"))
        self.assertFalse(match("a.txt"))

    def test_directory_pattern_excludes_contents(self):
        match = build_exclude_matcher(["node_modules/"])
        self.assertTrue(match("node_modules/pkg/index.js"))
        self.assertTrue(match("app/node_modules/pkg/index.js"))
        self.assertFalse(match("src/app.js"))
        # a file merely starting with the same characters is kept
        self.assertFalse(match("node_modules_notes.txt"))

    def test_bare_directory_name_also_excludes_contents(self):
        match = build_exclude_matcher([".git"])
        self.assertTrue(match(".git/config"))
        self.assertTrue(match("sub/.git/HEAD"))

    def test_path_glob(self):
        match = build_exclude_matcher(["build/*.o"])
        self.assertTrue(match("build/main.o"))
        self.assertFalse(match("build/main.c"))

    def test_blank_patterns_ignored(self):
        match = build_exclude_matcher(["", "  ", "*.log"])
        self.assertTrue(match("x.log"))
        self.assertFalse(match("x.txt"))


class SyncPlanExcludeTests(unittest.TestCase):
    def test_excluded_paths_never_appear_in_the_plan(self):
        local = {"keep.txt": (1, 1.0), "skip.tmp": (1, 1.0),
                 "node_modules/a.js": (1, 1.0)}
        actions = build_sync_plan(
            local, {}, direction="upload",
            exclude=["*.tmp", "node_modules/"])
        self.assertEqual([a["rel"] for a in actions], ["keep.txt"])

    def test_excluded_extras_are_not_deleted(self):
        """The dangerous case: with delete_extra on, an excluded remote file
        must not be swept away just because it is absent locally."""
        actions = build_sync_plan(
            {}, {"secrets.env": (1, 1.0), "junk.txt": (1, 1.0)},
            direction="upload", delete_extra=True, exclude=["*.env"])
        self.assertEqual(
            [(a["action"], a["rel"]) for a in actions],
            [("delete_remote", "junk.txt")],
        )

    def test_callable_exclude_is_accepted(self):
        actions = build_sync_plan(
            {"a.txt": (1, 1.0), "b.txt": (1, 1.0)}, {},
            direction="upload", exclude=lambda rel: rel == "a.txt")
        self.assertEqual([a["rel"] for a in actions], ["b.txt"])


class ScanLocalTreeTests(unittest.TestCase):
    def test_relative_paths_sizes_and_mtimes(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "sub"))
            with open(os.path.join(tmp, "a.txt"), "w") as handle:
                handle.write("12345")
            with open(os.path.join(tmp, "sub", "b.txt"), "w") as handle:
                handle.write("x")
            tree = scan_local_tree(tmp)
            self.assertEqual(set(tree), {"a.txt", "sub/b.txt"})
            self.assertEqual(tree["a.txt"][0], 5)
            self.assertGreater(tree["a.txt"][1], 0)

    def test_missing_directory_is_empty(self):
        self.assertEqual(scan_local_tree("/nonexistent/path/xyz"), {})


class SearchMatcherTests(unittest.TestCase):
    def _match(self, **kw):
        return Model.build_search_matcher(**kw)

    def test_substring_is_case_insensitive_by_default(self):
        m = self._match(query="report")
        self.assertTrue(m("docs/Report.pdf", 10, None))
        m = self._match(query="report", case_sensitive=True)
        self.assertFalse(m("docs/Report.pdf", 10, None))

    def test_regex_mode(self):
        m = self._match(query=r"^docs/\d+\.txt$", regex=True)
        self.assertTrue(m("docs/12.txt", 1, None))
        self.assertFalse(m("docs/ab.txt", 1, None))

    def test_invalid_regex_raises_valueerror(self):
        with self.assertRaises(ValueError):
            self._match(query="([", regex=True)

    def test_size_bounds(self):
        m = self._match(min_size=100, max_size=200)
        self.assertFalse(m("a", 99, None))
        self.assertTrue(m("a", 100, None))
        self.assertTrue(m("a", 200, None))
        self.assertFalse(m("a", 201, None))

    def test_extension_filter_normalizes_dots(self):
        m = self._match(extensions=["txt", ".LOG"])
        self.assertTrue(m("a/b.txt", 1, None))
        self.assertTrue(m("a/b.log", 1, None))
        self.assertFalse(m("a/b.pdf", 1, None))

    def test_date_bounds(self):
        m = self._match(modified_after=_dt(10), modified_before=_dt(20))
        self.assertFalse(m("a", 1, _dt(5)))
        self.assertTrue(m("a", 1, _dt(15)))
        self.assertFalse(m("a", 1, _dt(25)))

    def test_filters_combine(self):
        m = self._match(query="log", extensions=["log"], min_size=10)
        self.assertTrue(m("x/app.log", 50, None))
        self.assertFalse(m("x/app.log", 5, None))     # too small
        self.assertFalse(m("x/app.txt", 50, None))    # wrong extension

    def test_empty_matcher_accepts_everything(self):
        m = self._match()
        self.assertTrue(m("anything", 0, None))


class SearchKeysFilterTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")
        self.m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "docs/a.txt", "Size": 10, "LastModified": _dt(1)},
            {"Key": "docs/big.log", "Size": 5000, "LastModified": _dt(9)},
            {"Key": "docs/sub/", "Size": 0, "LastModified": _dt(2)},
        ]}])

    def test_results_carry_modified(self):
        hits = self.m.search_keys("docs/", "a.txt")
        self.assertEqual(len(hits), 1)
        key, size, modified = hits[0]
        self.assertEqual((key, size), ("docs/a.txt", 10))
        self.assertEqual(modified, _dt(1))

    def test_filters_are_forwarded(self):
        hits = self.m.search_keys("docs/", "", min_size=1000)
        self.assertEqual([k for k, _s, _m in hits], ["docs/big.log"])
        hits = self.m.search_keys("docs/", "", extensions=["txt"])
        self.assertEqual([k for k, _s, _m in hits], ["docs/a.txt"])

    def test_invalid_regex_surfaces_before_listing(self):
        with self.assertRaises(ValueError):
            self.m.search_keys("docs/", "([", regex=True)


class ListTreeTests(unittest.TestCase):
    def test_relative_keys_sizes_and_epochs(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "pre/a.txt", "Size": 3, "LastModified": _dt(1)},
            {"Key": "pre/sub/b.txt", "Size": 4, "LastModified": _dt(2)},
            {"Key": "pre/", "Size": 0, "LastModified": _dt(1)},
        ]}])
        tree = m.list_tree("pre/")
        self.assertEqual(set(tree), {"a.txt", "sub/b.txt"})
        self.assertEqual(tree["a.txt"][0], 3)
        self.assertEqual(tree["a.txt"][1], _dt(1).timestamp())

    def test_requires_a_bucket(self):
        with self.assertRaises(ValueError):
            make_model(bucket="").list_tree("x/")


class CompletionNotificationTests(unittest.TestCase):
    def test_success(self):
        title, body = format_completion_notification({"done": 3})
        self.assertIn("finished", title)
        self.assertEqual(body, "3 completed")

    def test_failure_wins_the_title(self):
        title, body = format_completion_notification({"done": 1, "error": 2})
        self.assertIn("failed", title)
        self.assertEqual(body, "1 completed, 2 failed")

    def test_cancelled_only(self):
        title, body = format_completion_notification({"cancelled": 2})
        self.assertIn("cancelled", title)
        self.assertEqual(body, "2 cancelled")

    def test_nothing(self):
        _title, body = format_completion_notification({})
        self.assertEqual(body, "nothing to do")


class BulkRenameDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self, items):
        dlg = BulkRenameDialog(None, items)
        self.addCleanup(dlg.close)
        return dlg

    def _ok(self, dlg):
        return dlg._btns.button(QDialogButtonBox.StandardButton.Ok)

    def test_ok_disabled_until_something_changes(self):
        dlg = self._dlg([("a.txt", False)])
        self.assertFalse(self._ok(dlg).isEnabled())
        dlg._find.setText("a")
        dlg._replace.setText("b")
        self.assertTrue(self._ok(dlg).isEnabled())
        self.assertEqual(dlg.plan(), [("a.txt", "b.txt")])

    def test_preview_table_mirrors_the_plan(self):
        dlg = self._dlg([("x1.log", False), ("x2.log", False)])
        dlg._find.setText("x")
        dlg._replace.setText("y")
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertEqual(dlg._table.item(0, 1).text(), "y1.log")

    def test_ok_blocked_on_duplicate_targets(self):
        dlg = self._dlg([("a.txt", False), ("b.txt", False)])
        dlg._mode_template.setChecked(True)
        dlg._template.setText("same.txt")
        self.assertFalse(self._ok(dlg).isEnabled())
        self.assertIn("Cannot apply", dlg._info.text())

    def test_invalid_regex_blocks_ok(self):
        dlg = self._dlg([("a.txt", False)])
        dlg._regex.setChecked(True)
        dlg._find.setText("([")
        self.assertFalse(self._ok(dlg).isEnabled())
        self.assertIn("Invalid regular expression", dlg._info.text())

    def test_mode_switch_toggles_the_forms(self):
        dlg = self._dlg([("a.txt", False)])
        self.assertTrue(dlg._find_box.isVisibleTo(dlg))
        self.assertFalse(dlg._tpl_box.isVisibleTo(dlg))
        dlg._mode_template.setChecked(True)
        self.assertFalse(dlg._find_box.isVisibleTo(dlg))
        self.assertTrue(dlg._tpl_box.isVisibleTo(dlg))


class _FakeSyncModel:
    bucket = "b"

    def __init__(self, remote=None):
        self._remote = remote or {}

    def clone_for_worker(self):
        return self

    def list_tree(self, prefix="", cancel_event=None, log_fn=None):
        return dict(self._remote)


class SyncDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, remote=None, prefix="pre/"):
        mw = _FakeMainWindow()
        mw.started = []
        mw.start_sync = lambda *a: mw.started.append(a)
        dlg = SyncDialog(None, mw, _FakeSyncModel(remote), prefix)
        self.addCleanup(dlg.close)
        return dlg, mw

    def test_preview_needs_an_existing_folder(self):
        dlg, _mw = self._dlg()
        dlg._local.setText("/nonexistent/xyz")
        dlg._preview()
        self.assertIn("existing local folder", dlg._info.text())
        self.assertFalse(dlg._btn_run.isEnabled())

    def test_dry_run_lists_actions_without_transferring(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "new.txt"), "w") as handle:
                handle.write("12345")
            dlg, mw = self._dlg(remote={"old.txt": (3, 10.0)})
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            actions = {a["action"] for a in dlg._actions}
            self.assertIn("upload", actions)      # new.txt is missing remotely
            self.assertIn("skip", actions)        # old.txt kept (no delete_extra)
            self.assertTrue(dlg._btn_run.isEnabled())
            self.assertEqual(mw.started, [])      # dry run transfers nothing

    def test_delete_extra_turns_skips_into_deletes(self):
        with tempfile.TemporaryDirectory() as tmp:
            dlg, _mw = self._dlg(remote={"gone.txt": (3, 10.0)})
            dlg._local.setText(tmp)
            dlg._delete_extra.setChecked(True)
            dlg._preview()
            self._settle(dlg)
            self.assertEqual(
                [(a["action"], a["rel"]) for a in dlg.actionable()],
                [("delete_remote", "gone.txt")],
            )

    def test_direction_switch(self):
        dlg, _mw = self._dlg()
        self.assertEqual(dlg.direction(), "upload")
        dlg._down.setChecked(True)
        self.assertEqual(dlg.direction(), "download")

    def test_exclude_patterns_are_parsed_and_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("keep.txt", "drop.tmp"):
                with open(os.path.join(tmp, name), "w") as handle:
                    handle.write("x")
            dlg, _mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._exclude.setText("*.tmp, *.bak")
            self.assertEqual(dlg.exclude_patterns(), ["*.tmp", "*.bak"])
            dlg._preview()
            self._settle(dlg)
            self.assertEqual([a["rel"] for a in dlg._actions], ["keep.txt"])

    def test_run_hands_the_plan_to_the_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(len(mw.started), 1)
            actions, local_dir, prefix, direction = mw.started[0]
            self.assertEqual(local_dir, tmp)
            self.assertEqual(prefix, "pre/")
            self.assertEqual(direction, "upload")
            self.assertEqual([a["rel"] for a in actions], ["f.txt"])

    def test_declining_the_confirmation_does_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.No):
                dlg._run()
            self.assertEqual(mw.started, [])


class SyncWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.uploads = []
            self.downloads = []
            self.deletes = []

        def upload_file(self, local, key, progress_cb=None, cancel_event=None,
                        log_fn=None):
            self.uploads.append((local, key))

        def download_file(self, key, local, folder, progress_cb=None,
                          cancel_event=None, log_fn=None):
            self.downloads.append((key, local))

        def delete(self, key, log_fn=None):
            self.deletes.append(key)

    def test_each_action_routes_to_the_right_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            victim = os.path.join(tmp, "gone.txt")
            with open(victim, "w") as handle:
                handle.write("x")
            up_path = os.path.join(tmp, "a.txt")
            # a nested target proves the parent directory gets created
            down_path = os.path.join(tmp, "nested", "b.txt")
            job = [
                ("upload", "a.txt", up_path, "pre/a.txt", 5),
                ("download", "nested/b.txt", down_path, "pre/b.txt", 7),
                ("delete_remote", "c.txt", os.path.join(tmp, "c.txt"),
                 "pre/c.txt", 0),
                ("delete_local", "gone.txt", victim, "pre/gone.txt", 0),
            ]
            model = self.FakeModel()
            worker = Worker(model, job)
            finished = []
            worker.finished.connect(finished.append)
            worker.sync()
            self.assertEqual(model.uploads, [(up_path, "pre/a.txt")])
            self.assertEqual(model.downloads, [("pre/b.txt", down_path)])
            self.assertTrue(os.path.isdir(os.path.join(tmp, "nested")))
            self.assertEqual(model.deletes, ["pre/c.txt"])
            self.assertFalse(os.path.exists(victim))
            self.assertEqual(finished, [False])

    def test_cancel_stops_before_any_work(self):
        model = self.FakeModel()
        worker = Worker(model, [("upload", "a", "/l/a", "k/a", 1)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.sync()
        self.assertEqual(model.uploads, [])
        self.assertEqual(finished, [True])

    def test_failure_is_reported(self):
        class Failing(self.FakeModel):
            def upload_file(self, *_a, **_kw):
                raise RuntimeError("nope")

        worker = Worker(Failing(), [("upload", "a", "/l/a", "k/a", 1)])
        errors = []
        worker.error.connect(errors.append)
        worker.sync()
        self.assertEqual(errors, ["nope"])


class CollectShortcutsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _action(self, text, keys=""):
        from PyQt6.QtGui import QAction, QKeySequence
        act = QAction(text)
        if keys:
            act.setShortcut(QKeySequence(keys))
        self.addCleanup(act.deleteLater)
        return act

    def test_only_actions_with_shortcuts_are_listed(self):
        rows = collect_shortcuts([
            self._action("Download (Ctrl+D)", "Ctrl+D"),
            self._action("No shortcut here"),
        ])
        self.assertEqual(rows, [("Ctrl+D", "Download")])

    def test_parenthetical_hint_is_stripped(self):
        rows = collect_shortcuts([self._action("Refresh (F5, Ctrl+R)", "F5")])
        self.assertEqual(rows[0][1], "Refresh")

    def test_duplicates_removed_and_sorted_by_label(self):
        rows = collect_shortcuts([
            self._action("Zebra", "Ctrl+Z"),
            self._action("Alpha", "Ctrl+A"),
            self._action("Alpha", "Ctrl+A"),
        ])
        self.assertEqual([label for _k, label in rows], ["Alpha", "Zebra"])

    def test_extra_rows_are_appended_verbatim(self):
        rows = collect_shortcuts([], [("Enter", "Open")])
        self.assertEqual(rows, [("Enter", "Open")])


class UsageDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_result_renders_counts_classes_and_largest(self):
        dlg = main_window.BucketUsageDialog("bkt", "pre/")
        self.addCleanup(dlg.close)
        dlg.set_result(
            "bkt", "pre/", 3072, {"Documents": 1024, "Media": 2048, "Other": 0},
            {"docs": 1024}, count=7,
            by_class={"STANDARD": 1024, "GLACIER": 2048},
            largest=[(2048, "big.mp4"), (1024, "doc.pdf")],
        )
        self.assertIn("7", dlg.total_lbl.text())
        self.assertIn("GLACIER", dlg.by_class.text())
        self.assertIn("big.mp4", dlg.largest.text())

    def test_empty_result_says_so(self):
        dlg = main_window.BucketUsageDialog("bkt")
        self.addCleanup(dlg.close)
        dlg.set_result("bkt", "", 0, {}, {}, count=0, by_class={}, largest=[])
        self.assertIn("(empty)", dlg.by_class.text())
        self.assertIn("(empty)", dlg.largest.text())


class TransferHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                self.settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False, "", False,
            ))
        self.addCleanup(self.win.close)
        deadline = time.monotonic() + 5
        while not self.win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _entry(self, method="upload", job=None, status="done"):
        entry = main_window._QEntry(1, method, job or [("k", "/tmp/f")],
                                    label="Upload 1 item(s)")
        entry.status = status
        return entry

    def test_completed_job_is_recorded_newest_first(self):
        self.win._record_history(self._entry(), done_bytes=2048)
        self.win._record_history(
            self._entry(method="download", status="error"), done_bytes=10)
        history = self.win.load_transfer_history()
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["method"], "download")
        self.assertEqual(history[0]["status"], "error")
        self.assertEqual(history[1]["bytes"], 2048)

    def test_small_jobs_are_rerunnable(self):
        self.win._record_history(self._entry())
        self.assertIn("job", self.win.load_transfer_history()[0])

    def test_huge_jobs_are_not_stored_for_rerun(self):
        big = [("k%d" % i, "/tmp/f%d" % i)
               for i in range(main_window.MainWindow.HISTORY_JOB_LIMIT + 1)]
        self.win._record_history(self._entry(job=big))
        record = self.win.load_transfer_history()[0]
        self.assertNotIn("job", record)
        self.assertEqual(record["items"], len(big))

    def test_history_is_capped(self):
        limit = main_window.MainWindow.HISTORY_LIMIT
        entries = [{"when": str(i), "method": "upload", "label": "x",
                    "items": 1, "bytes": 0, "status": "done"}
                   for i in range(limit + 25)]
        self.win._save_transfer_history(entries)
        self.assertEqual(len(self.win.load_transfer_history()), limit)

    def test_rerun_requeues_the_stored_job(self):
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.rerun_history_entry(
                {"method": "upload", "job": [["k", "/tmp/f"]]})
        # JSON turned the tuples into lists; they come back as tuples
        self.assertEqual(started, [("upload", [("k", "/tmp/f")])])

    def test_rerun_ignores_entries_without_a_job(self):
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.rerun_history_entry({"method": "upload"})
        self.assertEqual(started, [])

    def test_clear_empties_the_log(self):
        self.win._record_history(self._entry())
        self.win.clear_transfer_history()
        self.assertEqual(self.win.load_transfer_history(), [])

    def test_corrupt_history_is_ignored(self):
        self.settings.beginGroup("common")
        self.settings.setValue("transfer_history", "{not json")
        self.settings.endGroup()
        self.assertEqual(self.win.load_transfer_history(), [])

    def test_optional_columns_hidden_by_default_and_toggleable(self):
        for index in LIST_OPTIONAL_COLUMNS:
            self.assertTrue(self.win.listview.isColumnHidden(index))
        self.assertEqual(self.win.model.columnCount(), len(LIST_COLUMNS))
        self.win.listview.setColumnHidden(3, False)
        self.win.close()

        again_settings = self.settings
        with patch.object(main_window, "DataModel", _StubModel):
            again = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                again_settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False, "", False,
            ))
        self.addCleanup(again.close)
        # visibility survives a restart
        self.assertFalse(again.listview.isColumnHidden(3))
        self.assertTrue(again.listview.isColumnHidden(4))

    def test_shortcut_help_includes_actions_and_listview_keys(self):
        """REGRESSION: most QActions are parentless, so collecting only
        findChildren(QAction) left the toolbar out of the cheat sheet."""
        actions = list(self.win.tBar.actions()) + self.win.findChildren(QAction)
        rows = collect_shortcuts(actions, main_window.LISTVIEW_KEY_HELP)
        labels = [label for _k, label in rows]
        all_keys = " | ".join(k for k, _l in rows)
        for expected in ("Ctrl+D", "Ctrl+U", "Ctrl+Z", "Ctrl+L"):
            self.assertIn(expected, all_keys)  # real toolbar actions
        # Refresh binds two keys; both must be listed, not just the first
        self.assertIn("F5", all_keys)
        self.assertIn("Ctrl+R", all_keys)
        self.assertIn("Backspace", all_keys)   # an event-filter key
        self.assertIn("Ctrl+E", all_keys)      # a bare QShortcut
        self.assertTrue(any("Download" in l for l in labels))
        # labels lose their "(Ctrl+D)" hint
        self.assertNotIn("Download (Ctrl+D)", labels)


class ThemeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_apply_returns_normalized_name(self):
        self.assertEqual(theme.apply_theme(self._app, "dark"), "dark")
        self.assertEqual(theme.apply_theme(self._app, "LIGHT"), "light")
        self.assertEqual(theme.apply_theme(self._app, "bogus"), "system")

    def test_dark_palette_is_dark(self):
        theme.apply_theme(self._app, "dark")
        window = self._app.palette().color(QPalette.ColorRole.Window)
        self.assertLess(window.lightness(), 128)
        # restore for other tests
        theme.apply_theme(self._app, "system")


if __name__ == "__main__":
    unittest.main()
