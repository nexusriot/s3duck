"""
Unit tests for pure-logic helpers (no network / no QApplication needed).

Run with:  python -m unittest discover -s tests -t .   (from the project root)
or:        .venv/bin/python -m unittest discover -s tests -t .

Several tests are explicit regression guards for previously fixed bugs and
are marked with "REGRESSION:" in their docstrings.
"""

import ast
import base64
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import struct
import zlib
import sys
import tempfile
import threading
import time
import types
import unittest
import weakref
import zipfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import botocore.exceptions
from PyQt6 import sip
from PyQt6.QtCore import (
    QByteArray, QEvent, QRect, QSettings, Qt, QThread, QUrl,
)
from PyQt6.QtGui import (
    QAction, QColor, QFont, QFontMetrics, QIcon, QIconEngine, QKeyEvent,
    QKeySequence, QPainter, QPalette, QPixmap,
)
from PyQt6.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QMessageBox, QStyle,
    QStyleOptionViewItem, QToolButton, QWidgetAction,
)

import diagnostics
import main_window
from main_window import command_entries, filter_commands, palette_score
import profile_switcher
import theme
import utils
from cryptography.fernet import Fernet
import s3duck
from s3duck import (
    selected_row_index, SettingsItem, load_profile_secrets, profile_summary,
    profile_badges, preselect_row,
)
from utils import (
    Crypto, CredentialError, require_crypto, TempWorkspace, pid_is_alive,
    normalize_accent, themed_icon, icon_is_visible, bundled_icon,
)
from utils import (
    str_to_bool, load_aws_profiles, scan_local_tree,
    export_profile_bundle, import_profile_bundle, BundleError,
)
from main_window import (
    _to_epoch, categorize_key, _human_bytes, _scaled_bar_values,
    _dest_inside_source, _build_upload_job_for_path, _listing_summary,
    bulk_rename_plan, build_sync_plan, summarize_sync_plan, build_exclude_matcher,
    collect_shortcuts, LIST_COLUMNS, LIST_OPTIONAL_COLUMNS,
    serialize_location, parse_location, build_profile_sync_job,
    location_entries,
    build_paste_job, hex_dump, bookmark_label, parse_bookmarks,
    find_duplicate_groups, summarize_duplicate_groups, select_redundant_keys,
    serialize_bookmarks, add_bookmark_to,
    format_completion_notification, BULK_RENAME_FIND, BULK_RENAME_TEMPLATE,
    Breadcrumb, BulkRenameDialog, CopyMoveDialog, IncompleteUploadsDialog,
    MetadataDialog, OverwriteDialog, PresignedLinkDialog, SyncDialog,
    TagsDialog, TransferSettingsDialog, VersionsDialog, Worker,
)
import model as model_module
from model import (
    Model, Item, FSObjectType, TransferCancelled, ReadOnlyError, run_parallel,
    RateLimiter, plan_prefix_download, prefix_of, CHECKSUM_ALGORITHMS,
)
from properties_window import PropertiesWindow
from settings import SettingsWindow


def _ensure_qapp():
    """Return the shared QApplication, creating it once (offscreen)."""
    # Qt reads this when the application is constructed, not on import, so
    # setting it here still works. tests/__init__.py sets it earlier, but only
    # when the suite is imported as a package; asking again costs nothing and
    # keeps a stray invocation from core-dumping on a headless machine.
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
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
        m.resume_chunk_size = 1024
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

    @staticmethod
    def _forge(payload, passphrase="pw"):
        """A well-formed, correctly-encrypted bundle wrapping arbitrary bytes,
        so the checks *after* decryption can be reached."""
        salt = b"0123456789abcdef"
        token = Fernet(utils._bundle_key(passphrase, salt)).encrypt(payload)
        return json.dumps({
            "version": utils.PROFILE_BUNDLE_VERSION,
            "salt": base64.b64encode(salt).decode(),
            "data": token.decode(),
        }).encode()

    def test_non_string_data_field_is_rejected(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["data"] = 12345
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(json.dumps(document).encode(), "pw")
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_payload_that_is_not_json_is_rejected(self):
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(self._forge(b"\x00not json"), "pw")
        self.assertIn("corrupt", str(ctx.exception).lower())

    def test_payload_that_is_not_a_list_is_rejected(self):
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(self._forge(b'{"name": "prod"}'), "pw")
        self.assertIn("list of profiles", str(ctx.exception))

    def test_corrupt_salt_is_rejected(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["salt"] = "!!! not base64 !!!"
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(json.dumps(document).encode(), "pw")
        self.assertIn("salt", str(ctx.exception).lower())

    def test_unicode_passphrase_round_trips(self):
        blob = export_profile_bundle(self.PROFILES, "паролü-🦆")
        self.assertEqual(import_profile_bundle(blob, "паролü-🦆"), self.PROFILES)
        with self.assertRaises(BundleError):
            import_profile_bundle(blob, "паролu-🦆")

    def test_bundle_key_is_bound_to_the_salt(self):
        """Why every export re-salts: same passphrase must not yield the same
        key twice, or two bundles would be interchangeable ciphertexts."""
        salt = b"0123456789abcdef"
        self.assertEqual(utils._bundle_key("pw", salt), utils._bundle_key("pw", salt))
        self.assertNotEqual(utils._bundle_key("pw", salt),
                            utils._bundle_key("pw", b"fedcba9876543210"))
        self.assertNotEqual(utils._bundle_key("pw", salt),
                            utils._bundle_key("other", salt))

    def test_key_derivation_is_not_weakened(self):
        """A security parameter, not an implementation detail — a careless edit
        that drops it turns the passphrase into a fast offline guess."""
        self.assertGreaterEqual(utils.PROFILE_BUNDLE_ITERATIONS, 480000)
        self.assertEqual(len(base64.urlsafe_b64decode(
            utils._bundle_key("pw", b"0123456789abcdef"))), 32)

    def test_a_bundle_carries_profiles_to_another_installation(self):
        """The end-to-end story the bundle exists for: credentials encrypted
        under this machine's Fernet key are exported under a passphrase and
        re-encrypted under a *different* machine's key, still readable."""
        source_key, target_key = Crypto.generate_key(), Crypto.generate_key()
        self.assertNotEqual(source_key, target_key)
        source = Crypto(source_key)
        stored = SettingsItem(
            name="prod", url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="b", enc_access_key=source.encrypt("AKPROD"),
            enc_secret_key=source.encrypt("SUPERSECRET"), no_ssl_check="false",
            use_path="false", enc_session_token=source.encrypt("tok"))

        access, secret, token = load_profile_secrets(source_key, stored)
        blob = export_profile_bundle(
            [{"name": stored.name, "access_key": access,
              "secret_key": secret, "session_token": token}], "transfer pw")
        self.assertNotIn(b"SUPERSECRET", blob)

        restored = import_profile_bundle(blob, "transfer pw")[0]
        target = Crypto(target_key)
        migrated = SettingsItem(
            name=restored["name"], url="", region="", bucket_name="",
            enc_access_key=target.encrypt(restored["access_key"]),
            enc_secret_key=target.encrypt(restored["secret_key"]),
            no_ssl_check="false", use_path="false",
            enc_session_token=target.encrypt(restored["session_token"]))

        self.assertEqual(load_profile_secrets(target_key, migrated),
                         ("AKPROD", "SUPERSECRET", "tok"))
        # and the old key is now useless against the migrated copy
        with self.assertRaises(CredentialError):
            load_profile_secrets(source_key, migrated)


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

    def enter_bucket(self, bucket_name):
        self.bucket = bucket_name
        self.current_folder = ""
        self.prev_folder = ""


class _FakeRunningThread:
    """Stands in for a QThread that is still running."""

    def __init__(self):
        self.quit_calls = 0
        self.wait_calls = []

    def isRunning(self):
        return True

    def quit(self):
        self.quit_calls += 1

    def wait(self, msecs=None):
        self.wait_calls.append(msecs)
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

    def test_entering_a_bucket_actually_loads_its_listing(self):
        """REGRESSION: the guard that blocks navigation during bucket entry
        also blocked the navigation the success handler itself starts — the
        worker emits success() before the thread stops running — so opening a
        bucket left the view on the bucket list."""
        self.assertTrue(self.win.in_bucket_list_mode())
        seq_before = self.win._nav_seq

        self.win.enter_bucket_async("bkt")
        deadline = time.monotonic() + 5
        while (self.win._nav_seq == seq_before
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._settle()

        self.assertEqual(self.win.data_model.bucket, "bkt")
        self.assertFalse(self.win.in_bucket_list_mode())
        self.assertGreater(self.win._nav_seq, seq_before)
        # the listing rendered: a bucket view always carries the [..] row
        self.assertIsNotNone(self.win.ix_by_name(main_window.UP_ENTRY_LABEL))
        self.assertTrue(self.win.listview.isEnabled())

    def test_double_clicking_a_bucket_row_opens_it(self):
        """The end-to-end path a user actually takes."""
        row = self.win.proxy.index(0, 0)
        self.assertEqual(row.data(), "bkt")
        seq_before = self.win._nav_seq

        self.win.list_doubleClicked(row)
        deadline = time.monotonic() + 5
        while (self.win._nav_seq == seq_before
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._settle()

        self.assertEqual(self.win.data_model.bucket, "bkt")
        self.assertFalse(self.win.in_bucket_list_mode())

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

    def test_a_superseded_navigation_thread_is_not_forgotten(self):
        """FINDING (crash): navigate() dropped the previous nav QThread on the
        floor whether or not it had finished. The thread is a child of this
        window, so closing while a listing was still in flight had Qt destroy
        a running QThread, which aborts the process — an intermittent CI
        core dump with no failing test to show for it."""
        stale = _FakeRunningThread()
        self.win._nav_thread = stale

        self.win.navigate()
        self.addCleanup(self._settle)

        self.assertIn(stale, self.win._nav_orphan_threads)
        self.assertIsNot(self.win._nav_thread, stale)

    def test_closing_joins_a_superseded_navigation_thread(self):
        """Parking it is only half the fix: shutdown has to drain the list,
        otherwise the window still outlives a thread it owns."""
        stale = _FakeRunningThread()
        self.win._nav_orphan_threads = [stale]

        self.win._shutdown_threads()

        self.assertEqual(stale.quit_calls, 1)
        self.assertEqual(stale.wait_calls, [3000])

    def test_a_finished_navigation_thread_is_pruned(self):
        """The parking list must not grow for the lifetime of the window."""
        class _Finished(_FakeRunningThread):
            def isRunning(self):
                return False

        done = _Finished()
        self.win._nav_orphan_threads = [done]
        self.win._park_nav_thread()
        self.assertNotIn(done, self.win._nav_orphan_threads)

    def test_navigation_pins_its_worker_until_swept(self):
        """navigate() must route its NavigationWorker through the pin
        registry — the deleteLater it replaced is the deadlock pattern."""
        keys_before = set(utils._LIVE_WORKERS)
        self.win.navigate()
        self._settle()

        new = {k: v for k, v in utils._LIVE_WORKERS.items()
               if k not in keys_before}
        self.assertTrue(
            any(type(worker).__name__ == "NavigationWorker"
                for worker, _th in new.values()),
            f"no NavigationWorker pinned; new entries: {list(new)}")

        # Once its thread reports finished, a sweep lets it go.
        deadline = time.monotonic() + 5.0
        while any(k in utils._LIVE_WORKERS for k in new) \
                and time.monotonic() < deadline:
            self._app.processEvents()
            utils.reap_finished_workers()
        for k in new:
            self.assertNotIn(k, utils._LIVE_WORKERS)

    def test_shutdown_sweeps_pinned_workers(self):
        """_shutdown_threads is the last sweep point: a window must not exit
        leaving finished threads' workers pinned in the registry."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self.assertTrue(th.wait(5000))
        self.assertIn(id(wk), utils._LIVE_WORKERS)

        self.win._shutdown_threads()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

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


class _HollowIconEngine(QIconEngine):
    """An icon that exists but draws nothing — what a theme entry with no
    usable file behaves like, and the reason isNull() is not enough."""

    def pixmap(self, size, mode, state):
        return QPixmap()

    def paint(self, painter, rect, mode, state):
        pass

    def clone(self):
        return _HollowIconEngine()


class QuickOpenTests(unittest.TestCase):
    """Ctrl+P: reach a bucket or saved place by name instead of clicking back
    to the bucket list."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    BOOKMARKS = [
        {"name": "logs 2026", "bucket": "logs", "prefix": "2026/"},
        {"name": "", "bucket": "media", "prefix": ""},
    ]

    def test_bookmarks_come_before_buckets(self):
        """A saved place is almost always what someone is reaching for."""
        rows = location_entries(["assets", "logs"], self.BOOKMARKS)
        self.assertEqual([label for label, _h, _t in rows][:2],
                         ["logs 2026", "media"])

    def test_a_nameless_bookmark_falls_back_to_its_location(self):
        rows = location_entries([], [{"bucket": "media", "prefix": "raw/"}])
        self.assertEqual(rows, [("media/raw/", "media/raw/", ("media", "raw/"))])

    def test_the_open_bucket_is_not_offered(self):
        """Jumping to where you already are is not a useful result."""
        rows = location_entries(["assets", "logs"], [], current_bucket="logs")
        self.assertEqual([label for label, _h, _t in rows], ["assets"])

    def test_a_bookmark_for_the_open_bucket_is_still_offered(self):
        """Its prefix is a different place, even in the same bucket."""
        rows = location_entries([], self.BOOKMARKS, current_bucket="logs")
        self.assertIn("logs 2026", [label for label, _h, _t in rows])

    def test_duplicates_collapse(self):
        rows = location_entries(
            ["media", "media"], [{"bucket": "media", "prefix": ""}])
        self.assertEqual(len(rows), 1)

    def test_blank_entries_are_ignored(self):
        rows = location_entries(["", None], [{"bucket": ""}, None])
        self.assertEqual(rows, [])

    def test_targets_carry_the_bucket_and_prefix(self):
        rows = location_entries([], self.BOOKMARKS)
        self.assertEqual(rows[0][2], ("logs", "2026/"))

    def test_the_dialog_filters_with_the_palette_matcher(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], self.BOOKMARKS))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.count(), 4)
        dlg._query.setText("med")
        self.assertEqual(dlg._list.count(), 1)
        self.assertEqual(dlg.chosen_location(), ("media", ""))

    def test_arrow_keys_move_the_selection(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], []))
        self.addCleanup(dlg.close)
        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Down,
                          Qt.KeyboardModifier.NoModifier)
        self.assertTrue(dlg.eventFilter(dlg._query, event))
        self.assertEqual(dlg._list.currentRow(), 1)

    def test_the_highlighted_row_is_what_opens(self):
        """Not simply the first match — the arrows move the selection."""
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], []))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg.chosen_location(), ("assets", ""))
        dlg._list.setCurrentRow(1)
        self.assertEqual(dlg.chosen_location(), ("logs", ""))

    def test_nothing_is_chosen_when_nothing_matches(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets"], []))
        self.addCleanup(dlg.close)
        dlg._query.setText("zzzz")
        self.assertIsNone(dlg.chosen_location())

    def test_the_window_binds_ctrl_p(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        self.assertEqual(win._quick_open_shortcut.key(), QKeySequence("Ctrl+P"))

    def test_choosing_a_location_navigates_there(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        win._bookmarks = list(self.BOOKMARKS)
        jumped = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (["logs", "media"], None)), \
             patch.object(main_window.QuickOpenDialog, "exec",
                          lambda _s: QDialog.DialogCode.Accepted), \
             patch.object(main_window.QuickOpenDialog, "chosen_location",
                          lambda _s: ("logs", "2026/")), \
             patch.object(main_window.MainWindow, "go_to_bookmark",
                          lambda _s, entry: jumped.append(entry)):
            win.show_quick_open()
        self.assertEqual(jumped, [{"bucket": "logs", "prefix": "2026/"}])

    def test_a_failed_bucket_listing_still_offers_bookmarks(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        win._bookmarks = list(self.BOOKMARKS)
        shown = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, RuntimeError("no route"))), \
             patch.object(main_window, "QuickOpenDialog") as dlg_cls:
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Rejected
            win.show_quick_open()
            shown = list(dlg_cls.call_args.args[1])
        self.assertEqual([label for label, _h, _t in shown],
                         ["logs 2026", "media"])

    def test_cancelling_the_bucket_listing_opens_nothing(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        # Bookmarks exist, so "nothing to show" cannot stand in for the
        # cancellation check the way an empty list would.
        win._bookmarks = list(self.BOOKMARKS)
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, None)), \
             patch.object(main_window, "QuickOpenDialog") as dlg_cls:
            win.show_quick_open()
        dlg_cls.assert_not_called()


class CrossProfileSyncTests(unittest.TestCase):
    """Cross-profile copy landed first; syncing reuses the same planner by
    feeding it the two profiles' trees as source and destination."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_plan_becomes_copy_and_delete_rows(self):
        actions = [
            {"action": "upload", "rel": "a.txt", "size": 1, "reason": "missing"},
            {"action": "skip", "rel": "b.txt", "size": 1, "reason": "same"},
            {"action": "delete_remote", "rel": "old.txt", "size": 0,
             "reason": "not at source"},
        ]
        self.assertEqual(
            build_profile_sync_job(actions, "src/", "dst/"),
            [("copy", "src/a.txt", "dst/a.txt"),
             ("delete", "", "dst/old.txt")])

    def test_skips_never_become_work(self):
        actions = [{"action": "skip", "rel": "a", "size": 1, "reason": "same"}]
        self.assertEqual(build_profile_sync_job(actions, "", ""), [])

    def test_a_root_to_root_sync_keeps_bare_keys(self):
        actions = [{"action": "upload", "rel": "a/b.txt", "size": 1,
                    "reason": "missing"}]
        self.assertEqual(build_profile_sync_job(actions, "", ""),
                         [("copy", "a/b.txt", "a/b.txt")])

    def test_rows_without_a_path_are_ignored(self):
        self.assertEqual(
            build_profile_sync_job([{"action": "upload", "rel": ""}], "s/", "d/"),
            [])
        self.assertEqual(build_profile_sync_job(None, "s/", "d/"), [])

    def test_the_worker_copies_and_deletes_against_the_right_models(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        dest = _RecordingDest(bucket="dst")
        dest.deleted = []
        dest.delete = lambda key, log_fn=None, cancel_event=None: \
            dest.deleted.append(key)
        copied = []
        worker = main_window.Worker(
            source,
            [("copy", "src/a.txt", "dst/a.txt"), ("delete", "", "dst/old.txt")],
            dest_model=dest)
        with patch.object(Model, "copy_to_model",
                          lambda _s, src, model, dst, **kw:
                          copied.append((src, dst, model))):
            worker.sync_to_profile()
        self.assertEqual(copied, [("src/a.txt", "dst/a.txt", dest)])
        self.assertEqual(dest.deleted, ["dst/old.txt"])

    def test_the_worker_refuses_without_a_destination(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        worker = main_window.Worker(source, [("copy", "a", "b")])
        errors = []
        worker.error.connect(errors.append)
        worker.sync_to_profile()
        self.assertIn("destination", errors[0])

    def test_cancelling_stops_before_the_next_item(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        dest = _RecordingDest(bucket="dst")
        worker = main_window.Worker(
            source, [("copy", "a", "a"), ("copy", "b", "b")], dest_model=dest)
        worker._cancel_event.set()
        done = []
        worker.finished.connect(lambda cancelled: done.append(cancelled))
        with patch.object(Model, "copy_to_model",
                          lambda *a, **kw: self.fail("copied after cancel")):
            worker.sync_to_profile()
        self.assertEqual(done, [True])

    def test_a_pull_reads_from_the_other_profile(self):
        """The queue entry carries its own source model, because a pull does
        not read from the window's model at all."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        other = Model("https://b", "us-east-1", "AK", "SK", "other", False, False)
        entry = main_window._QEntry(
            entry_id=1, method="sync_to_profile", job=[], source_model=other)
        self.assertIs(win._worker_model_for(entry), other)

    def test_an_ordinary_entry_still_gets_a_clone(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                root, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        entry = main_window._QEntry(entry_id=1, method="upload", job=[])
        model = win._worker_model_for(entry)
        self.assertIsNot(model, win.data_model)
        self.assertIsInstance(model, Model)


class LocationRestoreTests(unittest.TestCase):
    """Bookmarks landed earlier; this is the other half — come back to where
    you were, in the profile you were using."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def test_a_location_round_trips(self):
        for bucket, prefix in (("logs", "2026/08/"), ("logs", ""),
                               ("logs", "deep/nested/path/")):
            with self.subTest(bucket=bucket, prefix=prefix):
                text = serialize_location(bucket, prefix)
                self.assertEqual(parse_location(text), (bucket, prefix))

    def test_bucket_list_mode_stores_nothing(self):
        self.assertEqual(serialize_location("", ""), "")
        self.assertEqual(serialize_location(None, "x/"), "")
        self.assertEqual(parse_location(""), ("", ""))
        self.assertEqual(parse_location(None), ("", ""))

    def test_stray_slashes_do_not_change_the_meaning(self):
        self.assertEqual(parse_location("/logs/a/b/"), ("logs", "a/b/"))
        self.assertEqual(serialize_location("/logs/", "/a/"), "logs/a/")

    def test_a_prefix_without_a_trailing_slash_is_normalised(self):
        """Navigation treats a prefix as a folder, so the slash is not
        cosmetic — a hand-edited setting must still work."""
        self.assertEqual(parse_location("logs/a/b"), ("logs", "a/b/"))
        self.assertEqual(parse_location("logs"), ("logs", ""))

    def _win(self, profile="prof", settings=None):
        settings = settings or QSettings("s3duck-tests", "s3duck-tests")
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                self.ROOT, settings, profile, "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        return win

    def test_the_location_is_saved_per_profile(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = self._win("prod", settings)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/"
        win._save_last_location()
        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/2026/")
        self.assertIsNone(settings.value("last_location/dev"))
        settings.endGroup()

    def test_closing_the_window_persists_the_location(self):
        """Saved through writeSettings, so the hook itself is covered rather
        than only the helper it calls."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = self._win("prod", settings)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "deep/"
        win.close()
        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/deep/")
        settings.endGroup()

    def test_the_stored_location_is_reopened(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        settings.beginGroup("common")
        settings.setValue("last_location/prod", "logs/2026/")
        settings.endGroup()
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                          entered.append((name, target_prefix))):
            win = self._win("prod", settings)
            entered.clear()          # ignore whatever startup already did
            self.assertEqual(win.restore_last_location(), "logs")
        self.assertEqual(entered, [("logs", "2026/")])

    def test_no_stored_location_stays_on_the_bucket_list(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                          entered.append(name)):
            win = self._win("prod", settings)
            entered.clear()
            self.assertEqual(win.restore_last_location(), "")
        self.assertEqual(entered, [])

    def test_preselect_falls_back_when_the_profile_is_gone(self):
        """A deleted or renamed profile must not leave the list unselected."""
        items = [SettingsItem("dev", "u", "r", "", b"", b"", "false", "false",
                              b"", "false", ""),
                 SettingsItem("prod", "u", "r", "", b"", b"", "false", "false",
                              b"", "false", "")]
        self.assertEqual(preselect_row(items, "prod"), 1)
        self.assertEqual(preselect_row(items, "dev"), 0)
        self.assertEqual(preselect_row(items, "vanished"), 0)
        # An unnamed profile must not be matched by an empty remembered name.
        # It sits at index 1 on purpose: at index 0 the fallback and the bug
        # would both answer 0, and the test would prove nothing.
        unnamed = SettingsItem("", "u", "r", "", b"", b"", "false", "false",
                               b"", "false", "")
        mixed = [items[0], unnamed, items[1]]
        self.assertEqual(preselect_row(mixed, ""), 0)
        self.assertEqual(preselect_row(mixed, "prod"), 2)
        self.assertEqual(preselect_row(items, ""), 0)
        self.assertEqual(preselect_row(items, None), 0)
        self.assertEqual(preselect_row([], "prod"), -1)

    def test_the_launcher_preselects_the_remembered_profile(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = s3duck.Profiles()
        self.addCleanup(win.close)
        win.settings = settings
        win.items = [
            SettingsItem("dev", "u", "r", "", b"", b"", "false", "false",
                         b"", "false", ""),
            SettingsItem("prod", "u", "r", "", b"", b"", "false", "false",
                         b"", "false", ""),
        ]
        win.populate_list()
        win.listWidget.setCurrentIndex(
            win.listWidget.model().index(preselect_row(win.items, "prod"), 0))
        self.assertEqual(win.listWidget.currentRow(), 1)


class DiagnosticsTests(unittest.TestCase):
    """Exists because a blank-icon report took three rounds to explain: the
    venv's PyQt6 wheel bundles QtSvg, the distribution package does not, and
    nothing in the app said so."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def test_the_report_names_the_facts_that_explain_icon_trouble(self):
        sections = diagnostics.collect(self.ROOT, version="9.9.9")
        text = diagnostics.format_report(sections)
        for label in ("SVG icons render", "QtSvg module",
                      "QtPdf module (PDF preview)", "Icon theme",
                      "Would render blank"):
            self.assertIn(label, text)

    def test_the_version_reaches_the_report(self):
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9"))
        self.assertIn("9.9.9", text)

    def test_svg_support_is_measured_by_rendering_not_by_format_list(self):
        """REGRESSION: the report read "svg" out of supportedImageFormats and
        said SVG was unreadable on a Qt where every SVG icon drew fine — QIcon
        reaches SVG through the icon-engine plugin, not the image format."""
        self.assertTrue(diagnostics.svg_supported(self.ROOT))
        # The format list alone must not decide it.
        with patch.object(diagnostics, "image_formats",
                          staticmethod(lambda: ["png"])):
            self.assertTrue(diagnostics.svg_supported(self.ROOT))
        # A Qt that cannot draw the sample reports no support.
        with patch.object(diagnostics, "icon_is_visible",
                          staticmethod(lambda *a, **kw: False)):
            self.assertFalse(diagnostics.svg_supported(self.ROOT))

    def test_svg_support_falls_back_to_the_format_list_without_a_sample(self):
        with patch.object(diagnostics.os.path, "exists",
                          staticmethod(lambda _p: False)), \
             patch.object(diagnostics, "image_formats",
                          staticmethod(lambda: ["png"])):
            self.assertFalse(diagnostics.svg_supported(self.ROOT))

    def test_icon_status_counts_every_requested_icon(self):
        status = diagnostics.icon_status(self.ROOT)
        self.assertGreater(status["total"], 30)
        self.assertEqual(status["blank"], 0)
        self.assertEqual(status["themed"] + status["bundled"] + status["blank"],
                         status["total"])

    def test_icon_status_counts_hollow_theme_entries(self):
        """The Mint case: the theme claims the name, so no fallback is used,
        yet nothing is painted. Counting these is what points at the cause."""
        hollow = QIcon(_HollowIconEngine())
        with patch.object(diagnostics.QIcon, "hasThemeIcon",
                          staticmethod(lambda name: True)), \
             patch.object(diagnostics.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: hollow)):
            status = diagnostics.icon_status(self.ROOT)
        self.assertEqual(status["hollow"], status["total"])
        self.assertEqual(status["themed"], 0)
        self.assertEqual(status["blank"], 0, "bundled art should cover them")

    def test_icon_status_reports_blanks_when_nothing_can_render(self):
        """The symptom the user actually saw."""
        with patch.object(diagnostics, "bundled_icon",
                          staticmethod(lambda *a, **kw: QIcon())):
            status = diagnostics.icon_status(self.ROOT)
        self.assertEqual(status["bundled"], 0)
        self.assertGreater(status["blank"], 0)

    def test_icon_calls_reads_multi_line_calls(self):
        """A line-oriented regex reported a present fallback as missing."""
        source = (
            'x = themed_icon("go-home", os.path.join(\n'
            '    self.current_dir,\n'
            '    "icons",\n'
            '    "home_24px.svg",\n'
            '))\n'
            'y = themed_icon("no-fallback")\n')
        self.assertEqual(
            sorted(diagnostics.icon_calls(source)),
            [("go-home", "home_24px.svg"), ("no-fallback", "")])

    def test_profile_and_transfer_settings_are_included(self):
        model = Model("https://minio.local", "us-east-1", "AK", "SK",
                      "bkt", True, True)
        model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        model.read_only = True
        sections = diagnostics.collect(
            self.ROOT, model=model, profile_name="prod")
        text = diagnostics.format_report(sections)
        self.assertIn("prod", text)
        self.assertIn("32 MiB", text)
        self.assertIn("64 MiB", text)
        # no_ssl_check=True means verification is OFF; the report must not
        # invert a security-relevant flag
        rows = dict(next(r for t, r in sections if t == "Profile"))
        self.assertEqual(rows["TLS verification"], "no")
        self.assertEqual(rows["Read-only"], "yes")

    def test_a_model_is_optional(self):
        """The launcher may want a report before any profile is opened."""
        titles = [title for title, _rows in diagnostics.collect(self.ROOT)]
        self.assertNotIn("Profile", titles)
        self.assertIn("Qt", titles)

    def test_the_report_is_aligned_plain_text(self):
        text = diagnostics.format_report(
            [("Bit", [("a", "1"), ("longer label", "2")])])
        self.assertEqual(
            text, "== Bit ==\na            : 1\nlonger label : 2\n")

    def test_the_dialog_copies_the_report_to_the_clipboard(self):
        dlg = main_window.DiagnosticsDialog(None, "== X ==\nk : v\n")
        self.addCleanup(dlg.close)
        dlg.copy_report()
        self.assertEqual(QApplication.clipboard().text(), "== X ==\nk : v\n")

    def test_the_action_is_reachable_from_the_command_palette(self):
        """Tools-menu actions are descendants of the window, so findChildren
        reaches them; this pins that the new entry is genuinely offered."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                self.ROOT, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        labels = [label for label, _k, _a in
                  command_entries(win._live_actions())]
        self.assertIn("Diagnostics", labels)


class _MultipartClient:
    """Records a manual multipart upload, optionally failing one part once."""

    def __init__(self, fail_on_part=None, page_size=None, cancel_on_part=None,
                 cancel_event=None):
        self.cancel_on_part = cancel_on_part
        self.cancel_event = cancel_event
        self.uploads = {}          # upload_id -> {key, parts}
        self.completed = {}        # key -> assembled bytes
        self.fail_on_part = fail_on_part
        self.page_size = page_size
        self.created = 0
        self.part_calls = []
        self.managed = []          # upload_file() calls (the boto3 fast path)
        self.aborted = []

    def create_multipart_upload(self, Bucket, Key, **kw):
        self.created += 1
        upload_id = f"upload-{self.created}"
        self.uploads[upload_id] = {"key": Key, "parts": {}, "kw": kw}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body, **kw):
        self.part_calls.append((PartNumber, kw.get("ChecksumAlgorithm")))
        if self.cancel_on_part == PartNumber and self.cancel_event is not None:
            self.cancel_event.set()
        if self.fail_on_part == PartNumber:
            self.fail_on_part = None
            raise RuntimeError("connection reset")
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        out = {"ETag": f'"etag{PartNumber}"'}
        if kw.get("ChecksumAlgorithm") == "CRC32":
            out["ChecksumCRC32"] = f"crc{PartNumber}"
        return out

    def list_parts(self, Bucket, Key, UploadId, PartNumberMarker=0):
        stored = sorted(self.uploads.get(UploadId, {}).get("parts", {}).items())
        rows = [{"PartNumber": n, "ETag": f'"etag{n}"', "Size": len(b)}
                for n, b in stored if n > PartNumberMarker]
        if self.page_size and len(rows) > self.page_size:
            head = rows[:self.page_size]
            return {"Parts": head, "IsTruncated": True,
                    "NextPartNumberMarker": head[-1]["PartNumber"]}
        return {"Parts": rows, "IsTruncated": False}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        parts = self.uploads[UploadId]["parts"]
        order = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        self.completed[Key] = b"".join(parts[n] for n in order)
        self.completed_parts = MultipartUpload["Parts"]
        return {"ETag": '"assembled"'}

    def upload_file(self, local, Bucket, Key, **kw):
        self.managed.append(Key)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append(UploadId)


class ResumableUploadTests(unittest.TestCase):
    """boto3's managed upload restarts from zero, so on a slow link a large
    file was effectively untransferable."""

    PART = 1024

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.payload = os.urandom(self.PART * 3 + 17)   # 4 parts, last short
        self.path = os.path.join(self.dir, "big.bin")
        with open(self.path, "wb") as handle:
            handle.write(self.payload)

    def _model(self, client):
        model = Model("https://s3.amazonaws.com", "us-east-1",
                      "AK", "SK", "bkt", False, False)
        model.upload_chunk_size = self.PART
        model.multipart_threshold_mb = 0        # every file takes this path
        model.transfer_concurrency = 1          # deterministic part order
        model.upload_state_dir = os.path.join(self.dir, "state")
        model._client = client
        return model

    def _state_files(self):
        directory = os.path.join(self.dir, "state")
        return sorted(os.listdir(directory)) if os.path.isdir(directory) else []

    def test_a_large_file_is_assembled_byte_for_byte(self):
        client = _MultipartClient()
        self._model(client).upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(sorted(n for n, _c in client.part_calls),
                         [1, 2, 3, 4])

    def test_the_record_is_removed_once_the_upload_completes(self):
        self._model(_MultipartClient()).upload_file(self.path, "big.bin")
        self.assertEqual(self._state_files(), [])

    def test_an_interrupted_upload_resumes_instead_of_restarting(self):
        """THE POINT: the second attempt re-sends only what is missing."""
        client = _MultipartClient(fail_on_part=3)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        self.assertEqual(len(self._state_files()), 1, "no resume record kept")
        first_round = list(client.part_calls)

        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")

        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(client.created, 1, "started a second multipart upload")
        resent = sorted(n for n, _c in client.part_calls)
        self.assertNotIn(1, resent, "re-sent a part the server already had")
        self.assertIn(3, resent, "did not re-send the failed part")
        self.assertLess(len(client.part_calls), len(first_round))

    def test_cancelling_mid_upload_keeps_the_record_so_it_can_resume(self):
        """Cancel is not an abort: the open upload and its record are exactly
        what the next attempt continues from."""
        cancel = threading.Event()
        client = _MultipartClient(cancel_on_part=2, cancel_event=cancel)
        model = self._model(client)
        with self.assertRaises(TransferCancelled):
            model.upload_file(self.path, "big.bin", cancel_event=cancel)
        self.assertEqual(len(self._state_files()), 1)
        self.assertEqual(client.aborted, [], "aborted the resumable upload")
        self.assertNotIn("big.bin", client.completed)

        cancel.clear()
        client.cancel_on_part = None
        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(client.created, 1)
        self.assertNotIn(1, [n for n, _c in client.part_calls])

    def test_cancelling_before_anything_starts_creates_no_upload(self):
        client = _MultipartClient()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self._model(client).upload_file(
                self.path, "big.bin", cancel_event=cancel)
        self.assertEqual(client.created, 0)
        self.assertEqual(self._state_files(), [])

    def test_a_changed_local_file_starts_a_fresh_upload(self):
        """Parts already on the server no longer line up, so reusing them
        would assemble a corrupt object."""
        client = _MultipartClient(fail_on_part=2)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        with open(self.path, "wb") as handle:
            handle.write(os.urandom(self.PART * 2))
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a stale upload id")

    def _seed_record(self, client, parts=(), size=None, mtime=None,
                     chunk=None, upload_id=None):
        """A prior attempt: an open upload with some parts already stored."""
        model = self._model(client)
        if upload_id is None:
            upload_id = client.create_multipart_upload("bkt", "big.bin")["UploadId"]
        for number in parts:
            start = (number - 1) * self.PART
            client.uploads[upload_id]["parts"][number] = \
                self.payload[start:start + self.PART]
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            upload_id,
            len(self.payload) if size is None else size,
            os.path.getmtime(self.path) if mtime is None else mtime,
            self.PART if chunk is None else chunk)
        return model

    def test_only_a_size_change_invalidates_the_record(self):
        """Each identity field is checked on its own; the others cannot be
        relied on to notice."""
        client = _MultipartClient()
        mtime = os.path.getmtime(self.path)
        model = self._seed_record(client, parts=[1], mtime=mtime)
        with open(self.path, "wb") as handle:
            handle.write(self.payload + b"appended")
        os.utime(self.path, (mtime, mtime))          # size differs, mtime same
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a record for a resized file")

    def test_only_an_mtime_change_invalidates_the_record(self):
        client = _MultipartClient()
        mtime = os.path.getmtime(self.path)
        model = self._seed_record(client, parts=[1], mtime=mtime)
        os.utime(self.path, (mtime + 60, mtime + 60))   # same bytes, new mtime
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a record for a touched file")

    def test_only_a_part_size_change_invalidates_the_record(self):
        """Parts on the server would no longer line up with what we send."""
        client = _MultipartClient()
        model = self._seed_record(client, parts=[1])
        model.upload_chunk_size = self.PART * 2
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a differently chunked upload")

    def test_completion_lists_parts_in_number_order(self):
        """A resumed upload learns of server parts before it sends the missing
        ones, so insertion order is not part order — and S3 assembles the
        object in the order given."""
        client = _MultipartClient()
        model = self._seed_record(client, parts=[3])
        model.upload_file(self.path, "big.bin")
        numbers = [p["PartNumber"] for p in client.completed_parts]
        self.assertEqual(numbers, [1, 2, 3, 4])
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_cancelling_during_the_last_part_does_not_complete(self):
        """Every part is in flight when the cancel lands, so the parallel run
        finishes normally and only a final check can stop the completion."""
        cancel = threading.Event()
        client = _MultipartClient(cancel_on_part=4, cancel_event=cancel)
        model = self._model(client)
        with self.assertRaises(TransferCancelled):
            model.upload_file(self.path, "big.bin", cancel_event=cancel)
        self.assertNotIn("big.bin", client.completed)
        self.assertEqual(len(self._state_files()), 1)

    def test_a_stored_part_of_the_wrong_length_is_resent(self):
        client = _MultipartClient()
        model = self._model(client)
        upload_id = client.create_multipart_upload("bkt", "big.bin")["UploadId"]
        client.uploads[upload_id]["parts"][1] = b"truncated"
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            upload_id, len(self.payload), os.path.getmtime(self.path),
            self.PART)
        model.upload_file(self.path, "big.bin")
        self.assertIn(1, [n for n, _c in client.part_calls])
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_an_unusable_upload_id_falls_back_to_a_new_upload(self):
        """The record survives but the service has forgotten the upload."""
        client = _MultipartClient()
        model = self._model(client)
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            "gone", len(self.payload), os.path.getmtime(self.path), self.PART)

        def _boom(**kw):
            raise RuntimeError("NoSuchUpload")

        client.list_parts = _boom
        logs = []
        model.upload_file(self.path, "big.bin", log_fn=logs.append)
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertTrue(any("starting over" in m for m in logs))

    def test_paginated_part_listings_are_followed(self):
        client = _MultipartClient(fail_on_part=4, page_size=1)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual([n for n, _c in client.part_calls], [4])

    def test_progress_is_reported_and_never_exceeds_the_total(self):
        seen = []
        client = _MultipartClient()
        self._model(client).upload_file(
            self.path, "big.bin",
            progress_cb=lambda total, cur, key: seen.append((total, cur)))
        self.assertTrue(seen)
        self.assertTrue(all(cur <= total for total, cur in seen))
        self.assertEqual(seen[-1][1], len(self.payload))

    def test_small_files_stay_on_the_managed_upload(self):
        client = _MultipartClient()
        model = self._model(client)
        model.multipart_threshold_mb = 16
        model.upload_file(self.path, "small.bin")
        self.assertEqual(client.managed, ["small.bin"])
        self.assertEqual(client.created, 0)

    def test_the_feature_can_be_switched_off(self):
        client = _MultipartClient()
        model = self._model(client)
        model.resumable_uploads = False
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.managed, ["big.bin"])

    def test_upload_options_reach_the_multipart_creation(self):
        client = _MultipartClient()
        model = self._model(client)
        model.set_upload_options(storage_class="GLACIER", sse="AES256")
        model.upload_file(self.path, "big.bin")
        created = client.uploads["upload-1"]["kw"]
        self.assertEqual(created["StorageClass"], "GLACIER")
        self.assertEqual(created["ServerSideEncryption"], "AES256")

    def test_a_checksum_algorithm_is_applied_per_part_and_at_completion(self):
        """Otherwise choosing a checksum would silently stop applying to
        exactly the large objects it matters most for."""
        client = _MultipartClient()
        model = self._model(client)
        model.set_upload_options(checksum_algorithm="CRC32")
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.uploads["upload-1"]["kw"]["ChecksumAlgorithm"],
                         "CRC32")
        self.assertTrue(all(alg == "CRC32" for _n, alg in client.part_calls))
        self.assertTrue(
            all("ChecksumCRC32" in part for part in client.completed_parts))

    def test_a_read_only_profile_refuses(self):
        client = _MultipartClient()
        model = self._model(client)
        model.read_only = True
        with self.assertRaises(ReadOnlyError):
            model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 0)

    def test_completion_sends_parts_in_order_without_the_size_field(self):
        """complete_multipart_upload rejects an unexpected Size member."""
        client = _MultipartClient(fail_on_part=2)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        model.upload_file(self.path, "big.bin")
        numbers = [p["PartNumber"] for p in client.completed_parts]
        self.assertEqual(numbers, sorted(numbers))
        self.assertTrue(all("Size" not in p for p in client.completed_parts))


class MultipartSizeTests(unittest.TestCase):
    """The part size and the multipart threshold were hardcoded at 8/16 MiB;
    non-AWS backends often need different ones, and the part size also decides
    how a resumable transfer is chunked."""

    def setUp(self):
        self.model = Model("https://s3.amazonaws.com", "us-east-1",
                           "AK", "SK", "bkt", False, False)

    def test_defaults_match_the_previous_hardcoded_values(self):
        self.assertEqual(self.model.multipart_threshold_mb, 16)
        self.assertEqual(self.model.multipart_chunksize_mb, 8)
        self.assertEqual(self.model.multipart_chunksize_bytes, 8 * 1024 * 1024)

    def test_the_sizes_reach_the_transfer_config(self):
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        for cfg in (self.model.transfer_cfg_upload,
                    self.model.transfer_cfg_download):
            self.assertEqual(cfg.multipart_threshold, 64 * 1024 * 1024)
            self.assertEqual(cfg.multipart_chunksize, 32 * 1024 * 1024)

    def test_the_part_size_cannot_go_below_s3s_floor(self):
        """A non-final part under 5 MiB makes every multipart upload fail at
        completion, so the value is clamped rather than trusted."""
        self.model.set_multipart_sizes(chunksize_mb=1)
        self.assertEqual(self.model.multipart_chunksize_mb, 5)

    def test_the_part_size_is_capped(self):
        self.model.set_multipart_sizes(chunksize_mb=99999)
        self.assertEqual(self.model.multipart_chunksize_mb,
                         Model.MAX_MULTIPART_CHUNKSIZE_MB)

    def test_the_threshold_never_falls_below_the_part_size(self):
        """Otherwise boto3 is asked to split a file into a single part."""
        self.model.set_multipart_sizes(threshold_mb=8, chunksize_mb=32)
        self.assertEqual(self.model.multipart_threshold_mb, 32)

    def test_garbage_falls_back_to_the_defaults(self):
        self.model.set_multipart_sizes(threshold_mb="abc", chunksize_mb="xyz")
        self.assertEqual(self.model.multipart_chunksize_mb, 8)
        self.assertEqual(self.model.multipart_threshold_mb, 16)

    def test_ranged_downloads_follow_the_part_size(self):
        """One setting governs how uploads and resumable downloads chunk."""
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=16)
        self.assertEqual(self.model.resume_chunk_size, 16 * 1024 * 1024)
        self.assertEqual(self.model.resume_threshold, 64 * 1024 * 1024)

    def test_the_sizes_survive_a_worker_clone(self):
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        clone = self.model.clone_for_worker()
        self.assertEqual(clone.multipart_threshold_mb, 64)
        self.assertEqual(clone.multipart_chunksize_mb, 32)
        self.assertEqual(clone.resume_chunk_size, 32 * 1024 * 1024)

    def test_changing_concurrency_keeps_the_sizes(self):
        """Both settings rebuild the same TransferConfig pair, so one must not
        reset the other."""
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        self.model.set_transfer_concurrency(9)
        self.assertEqual(
            self.model.transfer_cfg_upload.multipart_chunksize, 32 * 1024 * 1024)
        self.assertEqual(self.model.transfer_cfg_upload.max_concurrency, 9)


class CodeStyleTests(unittest.TestCase):
    """
    The project's standing style rules, enforced rather than re-checked by
    hand: every import at the top of its module, and no decorative
    section-divider comments.
    """

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SKIP_DIRS = {".venv", "__pycache__", ".git", "build", "dist"}
    # Runs of punctuation are what makes a comment a divider rather than prose.
    BANNER = re.compile(r"^\s*#\s*[-=*~_#+]{3,}\s*$|^\s*#\s*[-=*~_#]{2,}.*[-=*~_#]{2,}\s*$")

    def _python_files(self):
        found = []
        for dirpath, dirnames, filenames in os.walk(self.ROOT):
            dirnames[:] = [d for d in dirnames if d not in self.SKIP_DIRS]
            found += [os.path.join(dirpath, name)
                      for name in sorted(filenames) if name.endswith(".py")]
        return sorted(found)

    def _rel(self, path):
        return os.path.relpath(path, self.ROOT)

    def test_no_imports_inside_functions_or_classes(self):
        """A deferred import hides a dependency and can fail mid-operation."""
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                         ast.ClassDef)):
                    continue
                for sub in ast.walk(node):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        offenders.append(f"{self._rel(path)}:{sub.lineno}")
        self.assertEqual(offenders, [], "imports must be at module top")

    def test_no_module_level_import_sits_below_code(self):
        """The stricter half of the same rule. Setup that genuinely must run
        first (a Qt platform hint, a sys.path insert) belongs in a _bootstrap
        module that is simply imported first, so the imports stay contiguous.
        A module-level `try: import x / except ImportError:` still counts as
        top-level — that is the optional-dependency idiom, not a late import.
        """
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                tree = ast.parse(handle.read())
            first_code = None
            for node in tree.body:
                docstring = (isinstance(node, ast.Expr)
                             and isinstance(node.value, ast.Constant)
                             and isinstance(node.value.value, str))
                plain = isinstance(node, (ast.Import, ast.ImportFrom))
                guarded = isinstance(node, ast.Try) and any(
                    isinstance(sub, (ast.Import, ast.ImportFrom))
                    for sub in ast.walk(node))
                if docstring or plain or guarded:
                    continue
                first_code = node.lineno
                break
            if first_code is None:
                continue
            for node in tree.body:
                if (isinstance(node, (ast.Import, ast.ImportFrom))
                        and node.lineno > first_code):
                    offenders.append(f"{self._rel(path)}:{node.lineno}")
        self.assertEqual(offenders, [], "import below code at module level")

    def test_no_banner_comments(self):
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                for number, line in enumerate(handle.read().splitlines(), 1):
                    if self.BANNER.match(line):
                        offenders.append(f"{self._rel(path)}:{number}")
        self.assertEqual(offenders, [], "decorative divider comment")

    def test_the_banner_pattern_recognises_real_dividers(self):
        """Guard the guard: a pattern that matches nothing would pass the test
        above no matter what the tree contained."""
        for divider in ("# ------------", "#####", "# ==== Section ====",
                        "    # ---- helpers ----", "# ~~~~~~~~"):
            with self.subTest(divider=divider):
                self.assertTrue(self.BANNER.match(divider))
        for prose in ("# store settings in ~/.config/s3duck",
                      "# ~75% top / ~25% bottom",
                      "#!/usr/bin/python",
                      "# navigation state",
                      "# land in navigate(restore_name=...)"):
            with self.subTest(prose=prose):
                self.assertFalse(self.BANNER.match(prose))


class DebianPackagingTests(unittest.TestCase):
    """REGRESSION (blank icons on an installed .deb): the package did not
    depend on python3-pyqt6.qtsvg, so Qt could not render the bundled SVG
    icons and every theme-less button came out empty. A new third-party
    import must not be able to ship a broken package again."""

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Third-party module -> the Debian package that provides it.
    PACKAGES = {
        "PyQt6": "python3-pyqt6",
        "boto3": "python3-boto3",
        "botocore": "python3-botocore",
        "cryptography": "python3-cryptography",
        "urllib3": "python3-urllib3",
    }
    # Needed at runtime without being imported: Qt loads these as plugins.
    RUNTIME_ONLY = {"python3-pyqt6.qtsvg"}

    def _control(self):
        with open(os.path.join(self.ROOT, "DEBIAN", "control")) as handle:
            return handle.read()

    def _depends(self):
        for line in self._control().splitlines():
            if line.startswith("Depends:"):
                return {part.split()[0].strip()
                        for part in line[len("Depends:"):].split(",")
                        if part.strip()}
        return set()

    def _third_party_imports(self):
        found = set()
        local = {name[:-3] for name in os.listdir(self.ROOT)
                 if name.endswith(".py")}
        for name in sorted(os.listdir(self.ROOT)):
            if not name.endswith(".py"):
                continue
            with open(os.path.join(self.ROOT, name)) as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    found.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    found.add(node.module.split(".")[0])
        return {m for m in found
                if m not in sys.stdlib_module_names and m not in local}

    def test_every_third_party_import_is_a_declared_dependency(self):
        depends = self._depends()
        unmapped, undeclared = [], []
        for module in sorted(self._third_party_imports()):
            package = self.PACKAGES.get(module)
            if package is None:
                unmapped.append(module)
            elif package not in depends:
                undeclared.append(f"{module} -> {package}")
        self.assertEqual(unmapped, [], "no Debian package mapped for these "
                                       "imports; add them to PACKAGES")
        self.assertEqual(undeclared, [], "imported but missing from Depends")

    def test_the_svg_plugin_is_a_dependency(self):
        """It is never imported, so the import scan above cannot catch it —
        and without it every bundled icon renders blank."""
        self.assertTrue(self.RUNTIME_ONLY <= self._depends())

    def test_the_pdf_module_is_a_dependency(self):
        """The preview import-guards QtPdf, so a missing package degrades
        silently rather than failing loudly."""
        self.assertIn("python3-pyqt6.qtpdf", self._depends())

    def test_the_version_and_size_are_placeholders_for_the_build(self):
        """Both are filled in by build_deb.sh; a literal here is a value that
        will drift, which is how the package once shipped mislabelled."""
        control = self._control()
        self.assertIn("Version: _version_", control)
        self.assertIn("Installed-Size: _size_", control)
        self.assertIn("Architecture: _arch_", control)

    def test_the_build_script_fills_in_every_placeholder(self):
        with open(os.path.join(self.ROOT, "build_deb.sh")) as handle:
            script = handle.read()
        for token in ("_version_", "_arch_", "_size_"):
            self.assertIn(f"s/{token}/", script, f"{token} never substituted")


class WorkerLifetimeTests(unittest.TestCase):
    """release_worker_on_finish — the fix for the intermittent whole-suite
    freeze: a worker deleteLater'd via its own signal is destroyed on the
    worker thread, whose destructor takes a pooled signal-slot mutex and then
    the GIL, deadlocking against a GUI thread that holds the GIL and touches
    any connect/disconnect (Qt pools those mutexes, so unrelated objects
    collide)."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _drain(self, th):
        deadline = time.monotonic() + 5.0
        while not th.isFinished() and time.monotonic() < deadline:
            self._app.processEvents()
        th.wait(2000)

    def test_worker_is_pinned_until_its_thread_finishes(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)

        utils.release_worker_on_finish(th, wk)
        key = id(wk)
        self.assertIn(key, utils._LIVE_WORKERS)

        # Not started yet, so a sweep must not release it: isFinished is the
        # guard that keeps a fresh thread's worker from being swept early.
        utils.reap_finished_workers()
        self.assertIn(key, utils._LIVE_WORKERS)

        th.start()
        self._drain(th)
        utils.reap_finished_workers()

        self.assertNotIn(key, utils._LIVE_WORKERS)
        self.assertFalse(th.isRunning())
        # No deleteLater was queued anywhere: the C++ object is still alive
        # because this test holds the last reference, on the GUI thread.
        self.assertFalse(sip.isdeleted(wk))

    def test_next_pin_sweeps_finished_predecessors(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        th2 = QThread()
        wk2 = utils.FuncWorker(lambda _w: None)
        utils.release_worker_on_finish(th2, wk2)

        self.assertNotIn(id(wk), utils._LIVE_WORKERS)
        self.assertIn(id(wk2), utils._LIVE_WORKERS)
        utils._LIVE_WORKERS.pop(id(wk2), None)

    def test_join_qthread_sweeps_too(self):
        """Dialogs join in their done/close handlers; the sweep must ride
        along so a dialog's last worker is not pinned until the next one."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        utils.join_qthread(th)
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_deleted_thread_counts_as_finished(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        utils.release_worker_on_finish(th, wk)
        sip.delete(th)
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_thread_finished_fallback_without_sip(self):
        """A dead wrapper must count as finished even where sip is missing:
        isFinished on it raises RuntimeError, which means the thread cannot
        run its worker any more."""
        class _Gone:
            def isFinished(self):
                raise RuntimeError("wrapped C/C++ object has been deleted")

        class _Busy:
            def isFinished(self):
                return False

        with patch.object(utils, "sip", None):
            self.assertTrue(utils._thread_finished(_Gone()))
            self.assertFalse(utils._thread_finished(_Busy()))

        # With sip present, a non-wrapper argument makes isdeleted raise;
        # that must fall through to isFinished, not blow up the sweep.
        self.assertFalse(utils._thread_finished(_Busy()))
        self.assertTrue(utils._thread_finished(_Gone()))

    def test_join_qthread_tolerates_none_and_running(self):
        utils.join_qthread(None)   # close paths pass whatever they have

        dead = QThread()
        sip.delete(dead)
        utils.join_qthread(dead)   # wrapper outlived its C++ object

        gate = threading.Event()
        th = QThread()
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        gate.set()
        utils.join_qthread(th, timeout_ms=5000)
        self.assertTrue(th.isFinished())
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_cancelled_run_with_progress_keeps_the_worker_pinned(self):
        """Cancel returns while fn is still executing on the thread — the
        exact case where freeing the worker early is a use-after-free. It must
        stay pinned until the thread really finishes, then be released."""
        class _CancelledDialog:
            def __init__(self, *_a, **_k):
                pass

            def __getattr__(self, _name):
                return lambda *_a, **_k: None

            def wasCanceled(self):
                return True

        gate = threading.Event()
        self.addCleanup(gate.set)
        with patch.object(utils, "QProgressDialog", _CancelledDialog):
            result, exc = utils.run_with_progress(
                None, "t", lambda _w: gate.wait(10))
        self.assertEqual((result, exc), (None, None))

        pinned = [(k, w, th) for k, (w, th) in utils._LIVE_WORKERS.items()
                  if isinstance(w, utils.FuncWorker) and not th.isFinished()]
        self.assertEqual(len(pinned), 1, "cancelled worker not pinned")
        key, worker, th = pinned[0]

        utils.reap_finished_workers()
        self.assertIn(key, utils._LIVE_WORKERS)   # still running: keep it

        gate.set()
        deadline = time.monotonic() + 5.0
        while not th.isFinished() and time.monotonic() < deadline:
            self._app.processEvents()
        utils.reap_finished_workers()
        self.assertNotIn(key, utils._LIVE_WORKERS)

        wref = weakref.ref(worker)
        del worker, pinned
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wref())

    def test_running_thread_keeps_its_worker_pinned(self):
        """A sweep while the worker is mid-run must not release it — that
        would destroy the object whose method is currently executing."""
        gate = threading.Event()
        th = QThread()
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        try:
            th.start()
            deadline = time.monotonic() + 5.0
            while not th.isRunning() and time.monotonic() < deadline:
                time.sleep(0.01)

            utils.reap_finished_workers()
            self.assertIn(id(wk), utils._LIVE_WORKERS)
        finally:
            gate.set()
        self.assertTrue(th.wait(5000))
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_reaper_is_a_singleton_and_flush_drains_batches(self):
        reaper = utils._reaper()
        self.assertIs(reaper, utils._reaper())

        class _Dummy:
            pass

        a, b = _Dummy(), _Dummy()
        wa, wb = weakref.ref(a), weakref.ref(b)
        reaper.bury((a, None))
        reaper.bury((b, None))
        del a, b
        self.assertIsNotNone(wa())
        self.assertIsNotNone(wb())

        deadline = time.monotonic() + 5.0
        while (wa() is not None or wb() is not None) \
                and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wa())
        self.assertIsNone(wb())

    def test_pending_delivery_still_runs_before_the_release(self):
        """REGRESSION (segfault): the exact crash sequence — the worker emits,
        finishes, and the sweep runs before the GUI queue drains. The queued
        delivery must still reach a live object; only then may the worker (and
        the callables its connections hold) be freed."""
        hits = []
        th = QThread()
        wk = utils.FuncWorker(lambda w: w.progress.emit(7, 9))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.progress.connect(lambda cur, total: hits.append((cur, total)))
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)

        th.start()
        # Deliberately no processEvents: the progress delivery is still queued
        # when the thread reports finished and the sweep runs.
        self.assertTrue(th.wait(5000))
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

        wref = weakref.ref(wk)
        del wk
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertEqual(hits, [(7, 9)])
        self.assertIsNone(wref())

    def test_run_with_progress_releases_its_worker(self):
        """End to end through the most-used call path: the worker must not
        outlive the call by more than the reaper's one queue trip."""
        created = []

        class _Recording(utils.FuncWorker):
            def __init__(self, fn):
                super().__init__(fn)
                created.append(self)

        with patch.object(utils, "FuncWorker", _Recording):
            result, exc = utils.run_with_progress(None, "t", lambda _w: 7)
        self.assertEqual((result, exc), (7, None))
        self.assertEqual(len(created), 1)

        wref = weakref.ref(created[0])
        created.clear()
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
            utils.reap_finished_workers()
        self.assertIsNone(wref())

    def test_release_waits_out_pending_deliveries(self):
        """REGRESSION (segfault): dropping the last worker reference the
        moment its thread finished freed the callables its connections held
        while a delivery for one of them was still queued. The reaper must
        keep the worker alive until the event queue has cycled once."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        wref = weakref.ref(wk)
        del wk
        utils.reap_finished_workers()
        # Out of the registry, but the reaper still holds it: nothing that
        # was queued before the flush can call into a freed object.
        self.assertIsNone(utils._LIVE_WORKERS.get(wref and id(wref())))
        self.assertIsNotNone(wref())

        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wref())


class WorkerTeardownPatternTests(unittest.TestCase):
    """REGRESSION (deadlock): no sip QObject may be destroyed in a worker
    thread. `x.moveToThread(t)` plus `something.connect(x.deleteLater)` runs
    x's C++ destructor on the worker thread; ~1 full-suite run in 25 froze at
    the GIL. Every such site must use release_worker_on_finish instead."""

    FILES = ("main_window.py", "utils.py", "properties_window.py",
             "settings.py", "s3duck.py", "profile_switcher.py")

    def test_no_deleteLater_on_moved_workers(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        offenders = []
        for fname in self.FILES:
            path = os.path.join(root, fname)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as fh:
                src = fh.read()
            moved = set(re.findall(r"([A-Za-z_][\w.]*)\.moveToThread\(", src))
            for name in moved:
                esc = re.escape(name)
                if re.search(rf"\.connect\({esc}\.deleteLater\)", src):
                    offenders.append(f"{fname}: .connect({name}.deleteLater)")
                # Direct calls are only checkable for names that are
                # unmistakably workers; short names like `w` also belong to
                # widgets in these files.
                if "worker" in name.lower() and re.search(
                        rf"{esc}\.deleteLater\(\)", src):
                    offenders.append(f"{fname}: {name}.deleteLater()")
        self.assertEqual(offenders, [])


class IconFallbackTests(unittest.TestCase):
    """REGRESSION (blank toolbar buttons on Linux Mint): every icon went
    through QIcon.fromTheme, which uses its fallback only when the theme has NO
    entry for the name. A theme that registers the name but ships nothing
    drawable at our size therefore rendered an empty button, and 19 call sites
    passed no fallback at all."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ICONS = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "icons")

    def _bundled(self, name="home_24px.svg"):
        return os.path.join(self.ICONS, name)

    def _renderable(self, name="home_24px.svg"):
        """
        A bundled icon path that this Qt can actually draw.

        Distributions ship Qt without SVG support, so a test that needs "an
        icon which renders" must not assume the .svg does — that assumption is
        the very bug this class exists to guard.
        """
        path = self._bundled(name)
        if icon_is_visible(QIcon(path)):
            return path
        twin = os.path.splitext(path)[0] + ".png"
        self.assertTrue(icon_is_visible(QIcon(twin)),
                        f"neither {name} nor its PNG twin renders")
        return twin

    def _require_svg(self):
        if not icon_is_visible(QIcon(self._bundled("pie_24px.svg"))):
            self.skipTest("this Qt has no SVG support")

    def test_a_bundled_icon_is_used_when_the_theme_has_nothing(self):
        icon = themed_icon("definitely-not-a-theme-icon", self._bundled())
        self.assertFalse(icon.isNull())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def setUp(self):
        # The resolver caches, so each test starts from a clean slate.
        utils._icon_cache.clear()
        self.addCleanup(utils._icon_cache.clear)

    def test_a_transparent_theme_icon_is_rejected(self):
        """The remaining Mint case: the theme resolves the name to a fully
        transparent placeholder. It is not null, its pixmap is not null, and it
        still paints an empty button — only real alpha proves otherwise."""
        blank = QPixmap(24, 24)
        blank.fill(Qt.GlobalColor.transparent)
        invisible = QIcon(blank)
        self.assertFalse(invisible.isNull())
        self.assertFalse(invisible.pixmap(24, 24).isNull())
        self.assertFalse(icon_is_visible(invisible))
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: invisible)):
            icon = themed_icon("view-statistics", self._bundled())
        self.assertTrue(icon_is_visible(icon))

    def test_visibility_accepts_a_real_icon(self):
        self.assertTrue(icon_is_visible(QIcon(self._renderable())))
        self.assertFalse(icon_is_visible(QIcon()))
        self.assertFalse(icon_is_visible(None))

    def test_every_bundled_icon_paints_visible_pixels(self):
        """A fallback that renders nothing is no fallback at all. Asserted
        through bundled_icon, so a Qt without SVG passes on the PNG twins —
        which is the whole point of having them."""
        invisible = []
        for name in sorted(os.listdir(self.ICONS)):
            if not name.endswith(".svg"):
                continue
            if not icon_is_visible(bundled_icon(os.path.join(self.ICONS, name))):
                invisible.append(name)
        self.assertEqual(invisible, [])

    @staticmethod
    @contextlib.contextmanager
    def no_svg_support():
        """
        Simulate a Qt build with no SVG plugin.

        Debian/Mint's python3-pyqt6 ships no QtSvg, and there QIcon("x.svg")
        is NOT null — it simply paints nothing. That is why the venv (whose
        PyQt6 wheel bundles QtSvg) showed everything fine while the packaged
        app had blank buttons.
        """
        real = utils.QIcon

        class SvgBlindIcon(real):
            def __init__(self, *args):
                if args and isinstance(args[0], str) and args[0].endswith(".svg"):
                    super().__init__()          # unreadable: paints nothing
                else:
                    super().__init__(*args)

        with patch.object(utils, "QIcon", SvgBlindIcon):
            yield

    def test_a_png_twin_is_used_when_qt_cannot_read_svg(self):
        """THE PACKAGED-APP BUG: SVG fallbacks silently painted nothing."""
        with self.no_svg_support():
            icon = bundled_icon(self._bundled("pie_24px.svg"))
        self.assertTrue(icon_is_visible(icon))

    def test_svg_is_preferred_when_it_works(self):
        """The PNG twin is a fixed 48px raster; the SVG stays crisp at any
        size and DPI, so it must be tried first."""
        self._require_svg()
        tried = []
        real = utils.QIcon

        class Recording(real):
            def __init__(self, *args):
                if args and isinstance(args[0], str):
                    tried.append(os.path.basename(args[0]))
                super().__init__(*args)

        with patch.object(utils, "QIcon", Recording):
            icon = bundled_icon(self._bundled("pie_24px.svg"))
        self.assertTrue(icon_is_visible(icon))
        self.assertEqual(tried[0], "pie_24px.svg")
        self.assertNotIn("pie_24px.png", tried)

    def test_an_empty_path_yields_an_empty_icon(self):
        self.assertTrue(bundled_icon("").isNull())
        self.assertTrue(bundled_icon(None).isNull())

    def test_every_bundled_svg_has_a_png_twin(self):
        """The twin is what keeps the app usable without the SVG plugin."""
        missing = [
            name for name in sorted(os.listdir(self.ICONS))
            if name.endswith(".svg")
            and not os.path.exists(
                os.path.join(self.ICONS, name[:-4] + ".png"))
        ]
        self.assertEqual(missing, [])

    def test_every_png_twin_paints_visible_pixels(self):
        invisible = [
            name for name in sorted(os.listdir(self.ICONS))
            if name.endswith(".png")
            and not icon_is_visible(QIcon(os.path.join(self.ICONS, name)))
        ]
        self.assertEqual(invisible, [])

    def test_no_toolbar_icon_is_blank_without_svg_support(self):
        """The end-to-end check against the reported environment: build the
        real window on an SVG-blind Qt and assert every button still draws."""
        with self.no_svg_support():
            win = self._win()
            blank, checked = [], 0
            for action in win.tBar.actions():
                if action.isSeparator():
                    continue
                holder = action
                if isinstance(action, QWidgetAction):
                    widget = action.defaultWidget()
                    if widget is None:
                        continue
                    holder = widget
                checked += 1
                if not icon_is_visible(holder.icon()):
                    blank.append(action.text() or type(holder).__name__)
        self.assertEqual(blank, [], f"blank without SVG: {blank}")
        self.assertGreaterEqual(checked, 15)

    def test_the_cache_keys_on_the_name(self):
        """Two names sharing one fallback must not collide — otherwise the
        first answer is reused for every later icon."""
        blank = QPixmap(24, 24)
        blank.fill(Qt.GlobalColor.transparent)
        answers = {"hollow-name": QIcon(blank),
                   "good-name": QIcon(self._renderable("bucket_24px.svg"))}
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda n, *a, **kw: answers[n])):
            first = themed_icon("hollow-name", self._bundled("home_24px.svg"))
            second = themed_icon("good-name", self._bundled("home_24px.svg"))
        self.assertIsNot(first, second)
        self.assertIs(second, answers["good-name"])

    def test_results_are_cached_so_menus_stay_cheap(self):
        """Context menus rebuild their icons on every right-click."""
        calls = []
        real = utils.QIcon.fromTheme

        def counting(*args, **kwargs):
            calls.append(args[:1])
            return real(*args, **kwargs)

        with patch.object(utils.QIcon, "fromTheme", staticmethod(counting)):
            themed_icon("go-home", self._bundled())
            themed_icon("go-home", self._bundled())
            themed_icon("go-home", self._bundled())
        self.assertEqual(len(calls), 1)

    def test_a_theme_icon_that_draws_nothing_is_rejected(self):
        """The actual Mint failure: the theme HAS the name, so the icon is not
        null, but nothing is drawable at the size we paint. isNull() alone
        cannot see that, which is why the pixmap is probed."""
        hollow = QIcon(_HollowIconEngine())
        self.assertFalse(hollow.isNull())          # the trap: it looks fine
        self.assertTrue(hollow.pixmap(24, 24).isNull())
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: hollow)):
            icon = themed_icon("go-home", self._bundled())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def test_a_null_theme_icon_is_also_rejected(self):
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: QIcon())):
            icon = themed_icon("go-home", self._bundled())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def test_a_usable_theme_icon_is_preferred(self):
        """Desktop integration still wins when the theme actually delivers."""
        real = QIcon(self._renderable("bucket_24px.svg"))
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: real)):
            icon = themed_icon("go-home", self._bundled("home_24px.svg"))
        self.assertIs(icon, real)

    def test_no_fallback_and_no_theme_yields_an_explicit_empty_icon(self):
        self.assertTrue(themed_icon("nope-not-here", "").isNull())

    def test_every_bundled_icon_file_referenced_actually_exists(self):
        """A typo'd filename is a silent blank, since QIcon(bad_path) is
        simply null."""
        missing = []
        for source in ("main_window.py", "s3duck.py", "settings.py",
                       "properties_window.py", "profile_switcher.py"):
            path = os.path.join(os.path.dirname(self.ICONS), source)
            with open(path) as handle:
                referenced = re.findall(r'"icons",\s*"([^"]+)"', handle.read())
            for name in referenced:
                if not os.path.exists(os.path.join(self.ICONS, name)):
                    missing.append(f"{source}: {name}")
        self.assertEqual(missing, [])

    def test_nothing_calls_fromTheme_without_going_through_the_helper(self):
        """Any direct fromTheme call re-opens the hole, because it trusts the
        theme's claim that the name exists."""
        offenders = []
        for source in ("main_window.py", "s3duck.py", "settings.py",
                       "properties_window.py", "profile_switcher.py"):
            path = os.path.join(os.path.dirname(self.ICONS), source)
            with open(path) as handle:
                lines = handle.read().splitlines()
            for i, line in enumerate(lines, 1):
                if "QIcon.fromTheme" in line:
                    offenders.append(f"{source}:{i}")
        self.assertEqual(offenders, [])

    def _win(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(self.ICONS), settings, "prof",
                "https://s3.amazonaws.com", "us-east-1", "", "AK", "SK",
                False, False,
            ))
        self.addCleanup(win.close)
        return win

    def test_every_toolbar_button_draws_something(self):
        """The failure the screenshots showed: buttons present, icons blank.

        A QWidgetAction (the Theme / Bookmarks / Tools dropdowns) carries no
        icon itself — the QToolButton it wraps does — so those are checked
        through their widget rather than skipped.
        """
        win = self._win()
        blank, checked = [], 0
        for action in win.tBar.actions():
            if action.isSeparator():
                continue
            holder = action
            if isinstance(action, QWidgetAction):
                widget = action.defaultWidget()
                if widget is None:
                    continue
                holder = widget
            checked += 1
            if holder.icon().pixmap(24, 24).isNull():
                blank.append(action.text() or type(holder).__name__)
        self.assertEqual(blank, [], f"blank toolbar icons: {blank}")
        # Guard the guard: an empty toolbar would pass the assertion above.
        self.assertGreaterEqual(checked, 15)


class CommandPaletteTests(unittest.TestCase):
    """The action surface outgrew the toolbar: toolbar + Tools menu + several
    context menus + ~20 shortcuts, with no way to reach a rare command."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    @staticmethod
    def _action(text, keys=(), enabled=True, visible=True, separator=False):
        action = QAction(text)
        action.setShortcuts([QKeySequence(k) for k in keys])
        action.setEnabled(enabled)
        action.setVisible(visible)
        action.setSeparator(separator)
        return action

    def test_entries_are_built_from_live_actions(self):
        entries = command_entries([
            self._action("Refresh", ["F5", "Ctrl+R"]),
            self._action("Upload…"),
        ])
        self.assertEqual([(label, keys) for label, keys, _a in entries],
                         [("Refresh", "F5, Ctrl+R"), ("Upload", "")])

    def test_unusable_actions_are_left_out(self):
        """Running a disabled or hidden action would do nothing. Qt reports a
        hidden action as disabled too, so one check covers both."""
        hidden = self._action("Hidden", visible=False)
        self.assertFalse(hidden.isEnabled())
        entries = command_entries([
            self._action("Disabled", enabled=False),
            hidden,
            self._action("Real"),
        ])
        self.assertEqual([label for label, _k, _a in entries], ["Real"])

    def test_separators_are_left_out_even_when_they_carry_text(self):
        """A separator is enabled and visible, so only the separator flag
        keeps it out of the list."""
        entries = command_entries([
            self._action("Divider", separator=True),
            self._action("Real"),
        ])
        self.assertEqual([label for label, _k, _a in entries], ["Real"])

    def test_labels_are_cleaned_up(self):
        entries = command_entries([self._action("&Copy (Ctrl+C)…")])
        self.assertEqual(entries[0][0], "Copy")

    def test_duplicate_labels_collapse(self):
        """The same action reaches the palette from the toolbar and from
        findChildren; it should be offered once."""
        entries = command_entries(
            [self._action("Refresh", ["F5"]), self._action("refresh")])
        self.assertEqual(len(entries), 1)

    def test_matches_are_ranked_not_left_in_input_order(self):
        """Deliberately fed worst-first, so returning the input order fails."""
        entries = [("Clean old parts", "", None), ("Recopy files", "", None),
                   ("Copy", "", None), ("Delete", "", None)]
        self.assertEqual(
            [label for label, _k, _a in filter_commands(entries, "co")],
            ["Copy", "Recopy files", "Clean old parts"])

    def test_a_contiguous_match_beats_a_scattered_one(self):
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Clean old parts", "co"))

    def test_a_contiguous_match_wins_even_when_it_is_the_longer_label(self):
        """Guards the tier itself: 'Clo' is shorter, and would sort first if
        substring and subsequence shared a tier."""
        self.assertLess(palette_score("Incomplete uploads", "co"),
                        palette_score("Clo", "co"))
        self.assertEqual(
            [label for label, _k, _a in filter_commands(
                [("Clo", "", None), ("Incomplete uploads", "", None)], "co")],
            ["Incomplete uploads", "Clo"])

    def test_an_earlier_match_wins_within_a_tier(self):
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Recopy files", "co"))

    def test_position_beats_alphabetical_order(self):
        """Guards the position component: 'Abort copy' sorts first
        alphabetically but matches later."""
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Abort copy", "co"))
        self.assertEqual(
            [label for label, _k, _a in filter_commands(
                [("Abort copy", "", None), ("Copy", "", None)], "co")],
            ["Copy", "Abort copy"])

    def test_a_subsequence_must_be_in_order(self):
        """'yc' appears in 'Copy' only out of order, and must not match."""
        self.assertIsNone(palette_score("Copy", "yc"))
        self.assertIsNotNone(palette_score("Copy", "cy"))

    def test_an_empty_query_keeps_everything(self):
        entries = [("B", "", None), ("A", "", None)]
        self.assertEqual(len(filter_commands(entries, "")), 2)
        self.assertEqual(len(filter_commands(entries, "   ")), 2)

    def test_matching_ignores_case(self):
        entries = [("Upload folder", "", None)]
        self.assertEqual(len(filter_commands(entries, "UPLOAD")), 1)

    def test_a_query_that_matches_nothing_returns_nothing(self):
        entries = [("Copy", "", None)]
        self.assertEqual(filter_commands(entries, "zzz"), [])
        self.assertIsNone(palette_score("Copy", "zzz"))

    def test_the_dialog_runs_the_selected_action(self):
        fired = []
        action = self._action("Refresh", ["F5"])
        action.triggered.connect(lambda: fired.append(1))
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([action, self._action("Delete")]))
        self.addCleanup(dlg.close)
        dlg._query.setText("refr")
        chosen = dlg.chosen_action()
        self.assertIs(chosen, action)
        chosen.trigger()
        self.assertEqual(fired, [1])

    def test_the_highlighted_row_is_what_runs(self):
        """Not simply the first match — the arrows move the selection."""
        wanted = self._action("Download")
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Delete"), wanted]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.currentRow(), 0)
        dlg._list.setCurrentRow(1)
        self.assertIs(dlg.chosen_action(), wanted)

    def test_typing_narrows_the_list(self):
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Copy"), self._action("Delete"),
            self._action("Download")]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.count(), 3)
        dlg._query.setText("del")
        self.assertEqual(dlg._list.count(), 1)
        dlg._query.setText("")
        self.assertEqual(dlg._list.count(), 3)

    def test_arrow_keys_move_the_selection_while_typing(self):
        """The cursor stays in the box, so the list needs the arrows."""
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Copy"), self._action("Delete"),
            self._action("Download")]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.currentRow(), 0)
        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Down,
                          Qt.KeyboardModifier.NoModifier)
        self.assertTrue(dlg.eventFilter(dlg._query, event))
        self.assertEqual(dlg._list.currentRow(), 1)
        up = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Up,
                       Qt.KeyboardModifier.NoModifier)
        dlg.eventFilter(dlg._query, up)
        self.assertEqual(dlg._list.currentRow(), 0)

    def test_the_selection_cannot_run_off_the_ends(self):
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([self._action("Copy")]))
        self.addCleanup(dlg.close)
        for key in (Qt.Key.Key_Up, Qt.Key.Key_Down, Qt.Key.Key_Down):
            dlg.eventFilter(dlg._query, QKeyEvent(
                QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier))
            self.assertEqual(dlg._list.currentRow(), 0)

    def test_nothing_is_chosen_when_the_query_matches_nothing(self):
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([self._action("Copy")]))
        self.addCleanup(dlg.close)
        dlg._query.setText("zzzz")
        self.assertEqual(dlg._list.count(), 0)
        self.assertIsNone(dlg.chosen_action())

    def test_the_window_binds_ctrl_k(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        self.assertEqual(win._palette_shortcut.key(), QKeySequence("Ctrl+K"))

    def test_the_palette_offers_the_windows_real_actions(self):
        """Built from the live QActions, so it cannot drift from the app —
        and most are created parentless, which findChildren alone misses."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        labels = [label for label, _k, _a in
                  command_entries(win._live_actions())]
        self.assertTrue(labels)
        self.assertTrue(any("refresh" in label.lower() for label in labels),
                        f"toolbar actions missing from {labels}")


class ProfileAccentTests(unittest.TestCase):
    """The launcher's badges only help while picking a profile; once a window
    is open nothing distinguished prod from dev."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_normalize_accepts_hex_and_rejects_everything_else(self):
        self.assertEqual(normalize_accent("#C62828"), "#c62828")
        self.assertEqual(normalize_accent("#abc"), "#abc")
        self.assertEqual(normalize_accent("  #abc  "), "#abc")
        for bad in ("", None, "red", "c62828", "#12345", "#gggggg",
                    "#abc; drop table", 42):
            with self.subTest(value=bad):
                self.assertEqual(normalize_accent(bad), "")

    def _win(self, accent=""):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        base = (os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com", "us-east-1", "",
                "AK", "SK", False, False, "", "false")
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=base + (accent,))
        self.addCleanup(win.close)
        return win

    def test_an_accent_bands_the_window(self):
        win = self._win("#c62828")
        self.assertEqual(win.accent, "#c62828")
        self.assertFalse(win.accent_bar.isHidden())
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#c62828"))

    def test_no_accent_hides_the_band(self):
        win = self._win("")
        self.assertEqual(win.accent, "")
        self.assertTrue(win.accent_bar.isHidden())

    def test_a_malformed_accent_hides_the_band(self):
        """Settings are user-editable text; garbage must not paint a band that
        looks like a rendering fault."""
        win = self._win("not-a-colour")
        self.assertEqual(win.accent, "")
        self.assertTrue(win.accent_bar.isHidden())

    def test_the_band_uses_the_palette_not_a_stylesheet(self):
        """Any stylesheet makes a widget stop following the palette, which is
        how theming breaks elsewhere in this app."""
        win = self._win("#1565c0")
        self.assertEqual(win.accent_bar.styleSheet(), "")
        self.assertTrue(win.accent_bar.autoFillBackground())

    def test_the_accent_can_be_changed_at_runtime(self):
        """Switching profiles without restarting must re-mark the window."""
        win = self._win("#c62828")
        self.assertEqual(win.set_accent("#2e7d32"), "#2e7d32")
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#2e7d32"))
        win.set_accent("")
        self.assertTrue(win.accent_bar.isHidden())
        # Back to a colour: the band was explicitly hidden above, so this is
        # the only path where showing it again actually matters.
        win.set_accent("#1565c0")
        self.assertFalse(win.accent_bar.isHidden())
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#1565c0"))

    def test_a_profile_without_an_accent_still_opens(self):
        """Profiles saved before this feature pass a shorter tuple."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        self.assertEqual(win.accent, "")

    def test_the_switcher_carries_the_colour(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        crypto = Crypto(Fernet.generate_key().decode())
        settings.beginGroup("common")
        settings.setValue("key", crypto.key)
        settings.endGroup()
        raw = {"name": "prod", "url": "u", "region": "r", "bucket_name": "b",
               "access_key": crypto.encrypt("AK"),
               "secret_key": crypto.encrypt("SK"),
               "no_ssl_check": "false", "use_path": "false",
               "session_token": "", "read_only": "false", "color": "#C62828"}
        profile = profile_switcher.decrypt_profile(settings, raw)
        self.assertEqual(profile.color, "#c62828")

    def test_the_switcher_normalizes_a_bad_colour(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        crypto = Crypto(Fernet.generate_key().decode())
        settings.beginGroup("common")
        settings.setValue("key", crypto.key)
        settings.endGroup()
        raw = {"name": "p", "url": "u", "region": "r", "bucket_name": "",
               "access_key": crypto.encrypt("AK"),
               "secret_key": crypto.encrypt("SK"),
               "no_ssl_check": "false", "use_path": "false",
               "session_token": "", "read_only": "false", "color": "bogus"}
        self.assertEqual(
            profile_switcher.decrypt_profile(settings, raw).color, "")

    def test_the_profile_form_round_trips_the_colour(self):
        dlg = SettingsWindow(settings=(
            "n", "u", "r", "b", "AK", "SK", "false", "true", "", "false",
            "#2e7d32"))
        self.addCleanup(dlg.close)
        dlg.setRetVal()
        self.assertEqual(dlg.retrunVal[10], "#2e7d32")

    def test_the_profile_form_defaults_to_no_colour(self):
        dlg = SettingsWindow()
        self.addCleanup(dlg.close)
        dlg.setRetVal()
        self.assertEqual(dlg.retrunVal[10], "")


class LauncherAccentTests(unittest.TestCase):
    """The same colour marks the row in the picker and the open window."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, items):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests", "s3duck-tests")
        win.items = list(items)
        win.populate_list()
        self.addCleanup(win.close)
        return win

    @staticmethod
    def _item(color=""):
        return SettingsItem(
            "prod", "https://s3.amazonaws.com", "us-east-1", "", b"", b"",
            "false", "false", b"", "false", color)

    def test_the_row_carries_the_colour(self):
        win = self._launcher([self._item("#c62828")])
        self.assertEqual(
            win.listWidget.item(0).data(Qt.ItemDataRole.UserRole + 1),
            "#c62828")

    def _swatches(self, win):
        filled = []
        delegate = win.listWidget.itemDelegate()
        pixmap = QPixmap(400, 60)
        pixmap.fill()
        painter = QPainter(pixmap)
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, 400, 60)
        option.font = win.listWidget.font()
        option.palette = win.listWidget.palette()
        with patch.object(QPainter, "fillRect",
                          lambda _s, rect, colour: filled.append(
                              (rect.width(), QColor(colour).name()))):
            delegate.paint(painter, option,
                           win.listWidget.model().index(0, 0))
        painter.end()
        return filled

    def test_a_coloured_profile_gets_a_swatch(self):
        win = self._launcher([self._item("#c62828")])
        self.assertIn((4, "#c62828"), self._swatches(win))

    def test_an_uncoloured_profile_gets_no_swatch(self):
        win = self._launcher([self._item("")])
        self.assertEqual(self._swatches(win), [])

    def test_a_malformed_colour_paints_nothing(self):
        win = self._launcher([self._item("nonsense")])
        self.assertEqual(self._swatches(win), [])


class _RecordingDest:
    """A stand-in destination model that records what was written to it."""

    def __init__(self, bucket="dst", read_only=False):
        self.bucket = bucket
        self.read_only = read_only
        self.upload_extra_args = {}
        self.transfer_cfg_upload = None
        self.uploads = []
        self.folders = []
        self.client = self

    def _guard_write(self):
        if self.read_only:
            raise ReadOnlyError("profile is read-only")

    def upload_fileobj(self, body, bucket, key, ExtraArgs=None, Callback=None,
                       Config=None):
        self.uploads.append((bucket, key, body.read(), ExtraArgs))

    def create_folder(self, key, log_fn=None):
        self.folders.append(key)


class CrossProfileCopyTests(unittest.TestCase):
    """Server-side copy cannot use two sets of credentials, so moving objects
    between accounts or providers had no route at all."""

    def setUp(self):
        self.src = Model("https://s3.amazonaws.com", "us-east-1",
                         "AK", "SK", "srcbkt", False, False)
        self.dest = _RecordingDest()

    def _source_object(self, body=b"payload", **extra):
        resp = {"Body": io.BytesIO(body), "ContentLength": len(body)}
        resp.update(extra)
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: resp)
        return resp

    def test_an_object_is_read_here_and_written_there(self):
        self._source_object(b"hello")
        self.src.copy_to_model("a/b.txt", self.dest, "into/b.txt")
        self.assertEqual(self.dest.uploads,
                         [("dst", "into/b.txt", b"hello", None)])

    def test_content_type_and_metadata_survive_the_stream(self):
        """Streaming loses everything CopyObject would have preserved."""
        self._source_object(ContentType="text/csv",
                            Metadata={"owner": "vlad"},
                            ContentEncoding="gzip")
        self.src.copy_to_model("a.csv", self.dest, "a.csv")
        extra = self.dest.uploads[0][3]
        self.assertEqual(extra["ContentType"], "text/csv")
        self.assertEqual(extra["Metadata"], {"owner": "vlad"})
        self.assertEqual(extra["ContentEncoding"], "gzip")

    def test_the_destinations_upload_policy_wins(self):
        """Storage class and encryption belong to where the object lands."""
        self._source_object(ContentType="text/plain")
        self.dest.upload_extra_args = {"StorageClass": "GLACIER"}
        self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(self.dest.uploads[0][3]["StorageClass"], "GLACIER")

    def test_a_read_only_destination_is_refused(self):
        """The source profile's flag says nothing about the target's."""
        self._source_object()
        self.dest.read_only = True
        with self.assertRaises(ReadOnlyError):
            self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(self.dest.uploads, [])

    def test_a_read_only_source_can_still_copy_out(self):
        """Read-only protects the source's data; copying it elsewhere does not
        modify it, so it must not be blocked."""
        self.src.read_only = True
        self._source_object(b"x")
        self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(len(self.dest.uploads), 1)

    def test_cancelling_before_the_read_writes_nothing(self):
        self._source_object()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self.src.copy_to_model("a.txt", self.dest, "a.txt",
                                   cancel_event=cancel)
        self.assertEqual(self.dest.uploads, [])

    def test_a_prefix_keeps_its_shape(self):
        listed = [("src/a.txt", 1), ("src/sub/", 0), ("src/sub/b.txt", 2)]
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": io.BytesIO(b"z"),
                                     "ContentLength": 1})
        with patch.object(Model, "get_keys", lambda _s, p, **kw: listed):
            self.src.copy_prefix_to_model("src/", self.dest, "into/")
        self.assertEqual([key for _b, key, _d, _e in self.dest.uploads],
                         ["into/a.txt", "into/sub/b.txt"])
        self.assertEqual(self.dest.folders, ["into/sub/"])

    def test_a_prefix_can_land_at_the_bucket_root(self):
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": io.BytesIO(b"z"),
                                     "ContentLength": 1})
        with patch.object(Model, "get_keys",
                          lambda _s, p, **kw: [("src/a.txt", 1)]):
            self.src.copy_prefix_to_model("src/", self.dest, "")
        self.assertEqual(self.dest.uploads[0][1], "a.txt")

    def test_a_read_only_destination_refuses_a_whole_prefix(self):
        """And refuses it BEFORE listing: the per-file guard would also stop
        the copy, but only after a full recursive listing has been paid for."""
        self.dest.read_only = True
        listed = []
        with patch.object(Model, "get_keys",
                          lambda _s, p, **kw: listed.append(p) or [("a", 1)]):
            with self.assertRaises(ReadOnlyError):
                self.src.copy_prefix_to_model("src/", self.dest, "into/")
        self.assertEqual(self.dest.uploads, [])
        self.assertEqual(listed, [], "listed the prefix before refusing")

    def test_the_worker_refuses_a_job_with_no_destination(self):
        """A queued entry that lost its destination model must fail loudly
        rather than silently doing nothing."""
        worker = main_window.Worker(self.src, [("a.txt", "a.txt", False)])
        errors = []
        worker.error.connect(errors.append)
        worker.copy_to_profile()
        self.assertTrue(errors)
        self.assertIn("destination", errors[0])

    def test_the_worker_dispatches_files_and_folders(self):
        seen = []
        worker = main_window.Worker(
            self.src,
            [("f.txt", "to/f.txt", False), ("d/", "to/d/", True)],
            dest_model=self.dest)
        with patch.object(Model, "copy_to_model",
                          lambda _s, k, m, t, **kw: seen.append(("file", k, t))), \
             patch.object(Model, "copy_prefix_to_model",
                          lambda _s, k, m, t, **kw: seen.append(("dir", k, t))):
            worker.copy_to_profile()
        self.assertEqual(seen, [("file", "f.txt", "to/f.txt"),
                                ("dir", "d/", "to/d/")])


class AdditionalChecksumTests(unittest.TestCase):
    """Multipart ETags are unverifiable, so verification silently passed for
    exactly the large objects most worth checking. An additional full-object
    checksum stays comparable."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "payload.bin")
        self.data = b"duck" * 5000
        with open(self.path, "wb") as fh:
            fh.write(self.data)
        self.model = Model("https://s3.amazonaws.com", "us-east-1",
                           "AK", "SK", "bkt", False, False)

    def _expected(self, algorithm):
        if algorithm == "CRC32":
            return base64.b64encode(
                struct.pack(">I", zlib.crc32(self.data) & 0xFFFFFFFF)).decode()
        digest = hashlib.new(algorithm.lower(), self.data).digest()
        return base64.b64encode(digest).decode()

    def test_file_checksum_matches_what_s3_would_store(self):
        for algorithm in CHECKSUM_ALGORITHMS:
            with self.subTest(algorithm=algorithm):
                self.assertEqual(Model.file_checksum(self.path, algorithm),
                                 self._expected(algorithm))

    def test_an_algorithm_we_cannot_compute_is_refused(self):
        """CRC32C needs a third-party module; accepting it would mean
        'verified' downloads that were never actually checked."""
        for bad in ("CRC32C", "MD5", "", None):
            with self.subTest(algorithm=bad):
                with self.assertRaises(ValueError):
                    Model.file_checksum(self.path, bad)

    def test_composite_checksums_are_recognised(self):
        self.assertTrue(Model.checksum_is_composite("abcDEF==-4"))
        self.assertFalse(Model.checksum_is_composite("abcDEF=="))
        self.assertFalse(Model.checksum_is_composite("abc-DEF"))
        self.assertFalse(Model.checksum_is_composite(""))
        self.assertFalse(Model.checksum_is_composite(None))

    def test_a_matching_checksum_verifies(self):
        head = {"ChecksumSHA256": self._expected("SHA256")}
        self.assertTrue(self.model.verify_download(self.path, "", head=head))

    def test_a_mismatched_checksum_fails_even_with_a_good_etag(self):
        """The checksum is the stronger signal and must win."""
        head = {"ChecksumSHA256": base64.b64encode(b"x" * 32).decode()}
        etag = hashlib.md5(self.data).hexdigest()
        self.assertFalse(
            self.model.verify_download(self.path, etag, head=head))

    def test_a_full_object_checksum_verifies_a_multipart_object(self):
        """THE POINT: a multipart ETag cannot be checked, but a full-object
        CRC32 on the same object can."""
        multipart_etag = hashlib.md5(self.data).hexdigest() + "-4"
        self.assertFalse(Model.etag_is_md5(multipart_etag))
        head = {"ChecksumCRC32": self._expected("CRC32")}
        logs = []
        self.assertTrue(self.model.verify_download(
            self.path, multipart_etag, head=head, log_fn=logs.append))
        self.assertTrue(any("checksum ok (CRC32)" in m for m in logs))

    def test_a_per_part_checksum_falls_back_to_the_etag(self):
        head = {"ChecksumCRC32": self._expected("CRC32") + "-4"}
        etag = hashlib.md5(self.data).hexdigest()
        logs = []
        self.assertTrue(self.model.verify_download(
            self.path, etag, head=head, log_fn=logs.append))
        self.assertTrue(any("per-part" in m for m in logs))
        self.assertTrue(any("checksum ok" in m for m in logs))

    def test_no_checksum_still_uses_the_etag(self):
        etag = hashlib.md5(self.data).hexdigest()
        self.assertTrue(self.model.verify_download(self.path, etag, head={}))
        self.assertFalse(
            self.model.verify_download(self.path, "0" * 32, head={}))

    def test_stored_checksum_reads_the_head_response(self):
        self.assertEqual(
            Model.stored_checksum({"ChecksumSHA256": "abc"}), ("SHA256", "abc"))
        self.assertEqual(Model.stored_checksum({}), ("", ""))
        self.assertEqual(Model.stored_checksum(None), ("", ""))

    def test_uploads_request_the_checksum(self):
        args = self.model.set_upload_options(checksum_algorithm="SHA256")
        self.assertEqual(args["ChecksumAlgorithm"], "SHA256")
        self.assertEqual(self.model.checksum_algorithm, "SHA256")

    def test_crc32_uploads_ask_for_a_whole_object_checksum(self):
        """Without ChecksumType a multipart upload stores a per-part
        composite, which is as unverifiable as the ETag it replaces."""
        args = self.model.set_upload_options(checksum_algorithm="CRC32")
        self.assertEqual(args["ChecksumType"], "FULL_OBJECT")

    def test_sha_uploads_do_not_ask_for_full_object(self):
        """S3 only supports FULL_OBJECT for the CRC algorithms."""
        args = self.model.set_upload_options(checksum_algorithm="SHA256")
        self.assertNotIn("ChecksumType", args)

    def test_an_unsupported_algorithm_is_not_sent(self):
        args = self.model.set_upload_options(checksum_algorithm="CRC32C")
        self.assertNotIn("ChecksumAlgorithm", args)
        self.assertEqual(self.model.checksum_algorithm, "")

    def test_the_setting_survives_a_worker_clone(self):
        self.model.set_upload_options(checksum_algorithm="SHA1")
        self.assertEqual(
            self.model.clone_for_worker().checksum_algorithm, "SHA1")

    def test_head_asks_for_the_checksum(self):
        calls = []

        class _Client:
            def head_object(self, **kw):
                calls.append(kw)
                return {"ContentLength": 1}

        self.model._client = _Client()
        self.model.head_with_checksum("k")
        self.assertEqual(calls[0].get("ChecksumMode"), "ENABLED")

    def test_a_backend_rejecting_checksum_mode_still_works(self):
        """Older S3-compatible servers reject the parameter; without the retry
        every download's HEAD would fail."""
        calls = []

        class _Client:
            def head_object(self, **kw):
                calls.append(kw)
                if "ChecksumMode" in kw:
                    raise botocore.exceptions.ParamValidationError(
                        report="unknown parameter ChecksumMode")
                return {"ContentLength": 7}

        self.model._client = _Client()
        self.assertEqual(
            self.model.head_with_checksum("k"), {"ContentLength": 7})
        self.assertEqual(len(calls), 2)


class TempWorkspaceTests(unittest.TestCase):
    """REGRESSION (data left behind): previews, 'open with default app' and
    drag-out staging wrote decrypted object payloads into the system temp
    directory and nothing ever removed them."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def test_directories_are_created_under_one_session_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        first, second = ws.make(prefix="preview_"), ws.make(prefix="drag_")
        self.assertTrue(os.path.isdir(first))
        self.assertTrue(os.path.isdir(second))
        self.assertEqual(os.path.dirname(first), ws.session_dir)
        self.assertEqual(os.path.dirname(second), ws.session_dir)
        self.assertTrue(
            os.path.basename(ws.session_dir).startswith("s3duck_1234_"))

    def test_cleanup_removes_everything_including_staged_payloads(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        staged = os.path.join(ws.make(), "secret.txt")
        with open(staged, "w") as fh:
            fh.write("payload")
        session = ws.session_dir
        ws.cleanup()
        self.assertFalse(os.path.exists(staged))
        self.assertFalse(os.path.exists(session))

    def test_cleanup_is_safe_to_repeat(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        ws.make()
        ws.cleanup()
        ws.cleanup()          # must not raise on an already-removed root

    def test_a_later_make_after_cleanup_starts_a_new_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        first = ws.session_dir
        ws.cleanup()
        self.assertNotEqual(ws.session_dir, first)
        self.assertTrue(os.path.isdir(ws.session_dir))

    def test_sweep_reclaims_roots_whose_owner_is_gone(self):
        os.makedirs(os.path.join(self.root, "s3duck_4242_abc"))
        ws = TempWorkspace(root=self.root, pid=1234)
        removed = ws.sweep(is_alive=lambda pid: False)
        self.assertEqual(
            removed, [os.path.join(self.root, "s3duck_4242_abc")])
        self.assertFalse(os.path.exists(removed[0]))

    def test_sweep_never_touches_a_running_instance(self):
        """A second s3duck may be using its own staging right now."""
        live = os.path.join(self.root, "s3duck_4242_abc")
        os.makedirs(live)
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: True), [])
        self.assertTrue(os.path.isdir(live))

    def test_sweep_never_touches_our_own_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        mine = ws.session_dir
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])
        self.assertTrue(os.path.isdir(mine))

    def test_sweep_ignores_unrelated_directories(self):
        # "aaaaaaa9999_x" is the dangerous shape: drop the prefix check and
        # its 8th character onward parses as a PID, so another application's
        # temp directory would be deleted.
        names = ("not-ours", "s3duck_notanumber_x", "s3duck_", "aaaaaaa9999_x")
        for name in names:
            os.makedirs(os.path.join(self.root, name))
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])
        for name in names:
            self.assertTrue(os.path.isdir(os.path.join(self.root, name)),
                            f"{name} was swept")

    def test_owner_pid_only_reads_our_own_names(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.owner_pid("s3duck_42_abc"), 42)
        self.assertIsNone(ws.owner_pid("aaaaaaa9999_x"))
        self.assertIsNone(ws.owner_pid("s3duck_nope_x"))

    def test_sweep_survives_a_missing_root(self):
        ws = TempWorkspace(root=os.path.join(self.root, "gone"), pid=1)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])

    def test_pid_liveness_errs_towards_keeping_files(self):
        """An unknown answer must never authorise a delete."""
        self.assertTrue(pid_is_alive(os.getpid()))
        self.assertTrue(pid_is_alive("not-a-pid"))
        self.assertTrue(pid_is_alive(None))
        with patch.object(utils.os, "kill", side_effect=PermissionError):
            self.assertTrue(pid_is_alive(1))
        with patch.object(utils.os, "kill", side_effect=ProcessLookupError):
            self.assertFalse(pid_is_alive(999999))

    def test_the_window_cleans_up_on_close(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        win.temp_workspace = TempWorkspace(root=self.root, pid=1234)
        staged = win.temp_workspace.make()
        win.close()
        self.assertFalse(os.path.exists(staged))


class PrefixDownloadPlanTests(unittest.TestCase):
    """The destination mapping shared by the download itself and the overwrite
    check, so the two cannot disagree about what a folder download writes."""

    def test_maps_keys_under_the_prefix_basename(self):
        plan = plan_prefix_download(
            "pics/2024/", "/out", [("pics/2024/a.jpg", 10),
                                   ("pics/2024/sub/b.jpg", 20)])
        self.assertEqual(plan.base_dir, os.path.join("/out", "2024"))
        self.assertEqual(
            [(k, p) for k, p, _s in plan.files],
            [("pics/2024/a.jpg", os.path.join("/out", "2024", "a.jpg")),
             ("pics/2024/sub/b.jpg",
              os.path.join("/out", "2024", "sub", "b.jpg"))])

    def test_a_key_without_a_trailing_slash_still_works(self):
        plan = plan_prefix_download("pics", "/out", [("pics/a.jpg", 1)])
        self.assertEqual(plan.base_dir, os.path.join("/out", "pics"))
        self.assertEqual(len(plan.files), 1)

    def test_directory_markers_become_dirs_not_files(self):
        plan = plan_prefix_download(
            "p/", "/out", [("p/sub/", 0), ("p/sub/f.txt", 3)])
        self.assertEqual(plan.dirs, [os.path.join("/out", "p", "sub")])
        self.assertEqual(len(plan.files), 1)

    def test_the_prefix_itself_is_not_a_file(self):
        plan = plan_prefix_download("p/", "/out", [("p/", 0)])
        self.assertEqual(plan.files, [])
        self.assertEqual(plan.dirs, [])

    def test_keys_escaping_the_target_are_refused(self):
        """S3 keys may contain '..'; a download must not write outside the
        chosen directory."""
        plan = plan_prefix_download(
            "p/", "/out", [("p/../../etc/passwd", 1), ("p/ok.txt", 1)])
        self.assertEqual(plan.unsafe, ["p/../../etc/passwd"])
        self.assertEqual([k for k, _p, _s in plan.files], ["p/ok.txt"])

    def test_blank_keys_are_ignored(self):
        plan = plan_prefix_download("p/", "/out", [("", 0), (None, 0)])
        self.assertEqual((plan.files, plan.dirs, plan.unsafe), ([], [], []))

    def test_prefix_of_always_trailing_slashes(self):
        """Listings keyed on a bare name also match sibling prefixes — 'pics'
        matches 'pics-old/…' — so the slash is load-bearing."""
        self.assertEqual(prefix_of("pics"), "pics/")
        self.assertEqual(prefix_of("pics/"), "pics/")
        self.assertEqual(prefix_of(""), "/")
        self.assertEqual(prefix_of(None), "/")


class UploadOverwriteTests(unittest.TestCase):
    """REGRESSION (silent data loss): download, copy/move, paste and rename all
    checked their destination, but every upload path replaced remote objects
    with no prompt."""

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
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(self.win.close)
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = ""

    @staticmethod
    @contextlib.contextmanager
    def answering(choice, accepted=True):
        """
        Drive the REAL OverwriteDialog's exec/choice.

        Patching the whole class instead would make `OverwriteDialog.SKIP` in
        the code under test a mock attribute, so a Skip assertion would pass
        even when the code took the Overwrite branch.
        """
        verdict = (QDialog.DialogCode.Accepted if accepted
                   else QDialog.DialogCode.Rejected)
        with patch.object(main_window.OverwriteDialog, "exec",
                          lambda _s: verdict), \
             patch.object(main_window.OverwriteDialog, "choice",
                          lambda _s: choice):
            yield

    def _run_upload(self, files, conflicts, choice=None, cancel_prompt=False):
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set(conflicts)), \
             self.answering(choice, accepted=not cancel_prompt), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (files, "All (*)")
            self.win.upload()
        return started

    def test_upload_skips_existing_objects_when_asked(self):
        started = self._run_upload(
            ["/tmp/a.txt", "/tmp/b.txt"], conflicts={"a.txt"},
            choice=OverwriteDialog.SKIP)
        self.assertEqual(len(started), 1)
        self.assertEqual([k for k, _local in started[0][1]], ["b.txt"])

    def test_upload_overwrite_keeps_everything(self):
        started = self._run_upload(
            ["/tmp/a.txt", "/tmp/b.txt"], conflicts={"a.txt"},
            choice=OverwriteDialog.OVERWRITE)
        self.assertEqual([k for k, _local in started[0][1]], ["a.txt", "b.txt"])

    def test_cancelling_the_prompt_uploads_nothing(self):
        self.assertEqual(
            self._run_upload(["/tmp/a.txt"], conflicts={"a.txt"},
                             choice=OverwriteDialog.OVERWRITE,
                             cancel_prompt=True),
            [])

    def test_no_conflicts_means_no_prompt(self):
        started = []
        shown = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.OverwriteDialog, "exec",
                          lambda _s: shown.append(1)), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (
                ["/tmp/a.txt"], "All (*)")
            self.win.upload()
        self.assertEqual(shown, [])
        self.assertEqual(len(started), 1)

    def test_a_failed_destination_scan_aborts(self):
        """_destination_conflicts returns None when the lookup failed or was
        cancelled — uploading anyway would be the silent overwrite again."""
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: None), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (
                ["/tmp/a.txt"], "All (*)")
            self.win.upload()
        self.assertEqual(started, [])

    def test_folder_markers_are_not_treated_as_conflicts(self):
        """A directory placeholder only creates a prefix, so it must never be
        offered as an object about to be overwritten."""
        asked = []

        def _conflicts(_s, keys, bucket=None):
            asked.append(list(keys))
            return set()

        job = [("dir/", None), ("dir/f.txt", "/tmp/f.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          _conflicts):
            self.win._guard_upload(job)
        self.assertEqual(asked, [["dir/f.txt"]])

    def test_skipping_every_file_uploads_nothing(self):
        """Only folder markers left is not a job worth queueing."""
        job = [("dir/", None), ("dir/f.txt", "/tmp/f.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"dir/f.txt"}), \
             self.answering(OverwriteDialog.SKIP):
            self.assertIsNone(self.win._guard_upload(job))

    def test_upload_folder_is_guarded(self):
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window, "_build_upload_job_for_path",
                          lambda path, dest: [("a.txt", "/tmp/a.txt"),
                                              ("b.txt", "/tmp/b.txt")]), \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.SKIP), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.getExistingDirectory.return_value = "/tmp/src"
            self.win.upload_folder()
        self.assertEqual([k for k, _l in started[0][1]], ["b.txt"])

    def test_drag_and_drop_is_guarded(self):
        """The third upload entry point — dropping files onto the list."""
        started = []
        event = types.SimpleNamespace(
            source=lambda: None,
            setDropAction=lambda _a: None,
            accept=lambda: None,
            ignore=lambda: None,
            mimeData=lambda: types.SimpleNamespace(
                hasUrls=lambda: True,
                urls=lambda: [QUrl.fromLocalFile("/tmp/a.txt")]),
        )
        with patch.object(main_window, "_build_upload_job_for_path",
                          lambda path, dest: [("a.txt", "/tmp/a.txt")]), \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.SKIP), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.listview.dropEvent(event)
        self.assertEqual(started, [])


class FolderDownloadOverwriteTests(unittest.TestCase):
    """REGRESSION: the download prompt covered file rows only, so downloading a
    folder overwrote a whole local tree with no warning."""

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
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(self.win.close)

    def test_download_target_resolves_folders_to_their_local_root(self):
        self.assertEqual(
            self.win._download_target(("k/f.txt", "/out/f.txt", 1, "/out")),
            "/out/f.txt")
        self.assertEqual(
            self.win._download_target(("pics/2024/", None, None, "/out")),
            os.path.join("/out", "2024"))

    def test_a_folder_with_existing_local_files_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "pics"))
            with open(os.path.join(tmp, "pics", "a.jpg"), "w") as fh:
                fh.write("x")
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: [("pics/a.jpg", 1)]):
                hits = self.win._folder_download_conflicts(
                    [("pics/", None, None, tmp)])
        self.assertEqual(hits, {os.path.join(tmp, "pics")})

    def test_a_folder_whose_files_are_absent_is_not_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "pics"))
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: [("pics/a.jpg", 1)]):
                hits = self.win._folder_download_conflicts(
                    [("pics/", None, None, tmp)])
        self.assertEqual(hits, set())

    def test_no_folders_means_no_scan(self):
        called = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: called.append(t) or (set(), None)):
            self.assertEqual(self.win._folder_download_conflicts([]), set())
        self.assertEqual(called, [])

    def test_a_cancelled_scan_aborts_the_download(self):
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, None)):
            self.assertIsNone(self.win._folder_download_conflicts(
                [("pics/", None, None, "/out")]))

    def test_the_scan_lists_a_trailing_slashed_prefix(self):
        """Listing 'pics' instead of 'pics/' also matches the sibling prefix
        'pics-old/', so the check would scan the wrong objects."""
        asked = []
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: asked.append(p) or []):
                self.win._folder_download_conflicts(
                    [("pics", None, None, tmp)])
        self.assertEqual(asked, ["pics/"])

    def _download_folder(self, choice, folder_hits):
        """Drive download() with a single folder row selected."""
        started = []
        index = types.SimpleNamespace(column=lambda: 0)
        item = types.SimpleNamespace(size=0)
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(main_window, "QFileDialog") as fd, \
                 patch.object(self.win.listview, "selectionModel",
                              lambda: types.SimpleNamespace(
                                  selectedIndexes=lambda: [index])), \
                 patch.object(main_window.MainWindow, "get_row_primary_item",
                              lambda _s, _ix: (item, "pics",
                                               FSObjectType.FOLDER)), \
                 patch.object(main_window.MainWindow,
                              "_folder_download_conflicts",
                              lambda _s, entries: {
                                  os.path.join(tmp, "pics")} if folder_hits
                              else set()), \
                 patch.object(main_window.OverwriteDialog, "exec",
                              lambda _s: QDialog.DialogCode.Accepted), \
                 patch.object(main_window.OverwriteDialog, "choice",
                              lambda _s: choice), \
                 patch.object(main_window.MainWindow,
                              "assign_thread_operation",
                              lambda _s, m, j, **kw: started.append((m, j))):
                fd.getExistingDirectory.return_value = tmp
                self.win.data_model.bucket = "bkt"   # else download() no-ops
                self.win.data_model.current_folder = ""
                self.win.download()
        return started

    def test_downloading_a_folder_onto_existing_files_can_be_skipped(self):
        """END-TO-END: the whole point — a folder download used to overwrite a
        local tree with no prompt at all."""
        self.assertEqual(
            self._download_folder(OverwriteDialog.SKIP, folder_hits=True), [])

    def test_downloading_a_folder_proceeds_when_overwrite_is_chosen(self):
        started = self._download_folder(
            OverwriteDialog.OVERWRITE, folder_hits=True)
        self.assertEqual(len(started), 1)
        self.assertEqual(started[0][1][0][0], "pics/")

    def test_a_clean_destination_downloads_without_a_prompt(self):
        started = self._download_folder(OverwriteDialog.SKIP, folder_hits=False)
        self.assertEqual(len(started), 1)


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

    def test_progress_helper_returns_when_the_work_finishes_instantly(self):
        """REGRESSION (hang): the helper used QProgressDialog.exec(), but a
        worker that finished before exec() was entered had already called
        reset() — leaving the app frozen behind a dialog nothing would close.
        A small bucket made destination checks fast enough to hit it."""
        result, exc = self.win._run_with_progress("t", lambda _w: 42)
        self.assertEqual(result, 42)
        self.assertIsNone(exc)

    def test_progress_helper_surfaces_worker_errors(self):
        def _boom(_w):
            raise RuntimeError("nope")

        result, exc = self.win._run_with_progress("t", _boom)
        self.assertIsNone(result)
        self.assertIsInstance(exc, RuntimeError)

    def test_progress_helper_handles_slow_work(self):
        def _slow(_w):
            time.sleep(0.2)
            return "done"

        result, exc = self.win._run_with_progress("t", _slow)
        self.assertEqual(result, "done")
        self.assertIsNone(exc)

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


class CredentialGuardTests(unittest.TestCase):
    """REGRESSION (crash): a missing, malformed or mismatched encryption key
    raised AttributeError / ValueError / InvalidToken straight out of a Qt slot
    (onStart, onEdit, check_profile, export), and PyQt6 turns an exception that
    escapes a slot into a process ABORT — the launcher died instead of saying
    the credentials could not be read. profile_switcher.py already handled the
    same failure gracefully; this path did not."""

    def _crypto(self):
        return Crypto(Fernet.generate_key().decode())

    def _item(self, crypto):
        return SettingsItem(
            "prod", "https://s3.amazonaws.com", "us-east-1", "",
            crypto.encrypt("AKID"), crypto.encrypt("SECRET"),
            "false", "true", crypto.encrypt("TOKEN"), "false",
        )

    def test_missing_key_reports_instead_of_crashing(self):
        """A missing key and a corrupt key need different remedies, so the two
        messages must stay distinct rather than collapsing into one."""
        item = self._item(self._crypto())
        for missing in (None, ""):
            with self.subTest(key=missing):
                with self.assertRaises(CredentialError) as ctx:
                    load_profile_secrets(missing, item)
                self.assertIn("missing", str(ctx.exception).lower())
                self.assertIn("common/key", str(ctx.exception))

    def test_malformed_key_reports_instead_of_crashing(self):
        item = self._item(self._crypto())
        with self.assertRaises(CredentialError) as ctx:
            load_profile_secrets("this-is-not-a-fernet-key", item)
        # not the "missing key" wording — the key is present but unusable
        self.assertNotIn("missing", str(ctx.exception).lower())

    def test_wrong_key_reports_instead_of_crashing(self):
        """Settings restored from another machine, or a regenerated key."""
        item = self._item(self._crypto())
        other = Fernet.generate_key().decode()
        with self.assertRaises(CredentialError) as ctx:
            load_profile_secrets(other, item)
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_corrupt_blob_reports_instead_of_crashing(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_secret_key = b"not-a-token"
        with self.assertRaises(CredentialError):
            load_profile_secrets(crypto.key, item)

    def test_absent_blob_reports_instead_of_crashing(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_access_key = None
        with self.assertRaises(CredentialError):
            load_profile_secrets(crypto.key, item)

    def test_good_key_round_trips(self):
        crypto = self._crypto()
        item = self._item(crypto)
        self.assertEqual(
            load_profile_secrets(crypto.key, item), ("AKID", "SECRET", "TOKEN"))

    def test_optional_token_absent_is_not_an_error(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_session_token = ""
        self.assertEqual(load_profile_secrets(crypto.key, item)[2], "")

    def test_encrypting_with_a_bad_key_reports_instead_of_crashing(self):
        for bad in (None, "", "not-a-fernet-key"):
            with self.subTest(key=bad):
                with self.assertRaises(CredentialError):
                    Crypto(bad).encrypt("secret")

    def test_require_crypto_validates_eagerly(self):
        """The write paths must fail before a half-written profile is stored."""
        with self.assertRaises(CredentialError):
            require_crypto(None)
        with self.assertRaises(CredentialError):
            require_crypto("not-a-fernet-key")
        good = Fernet.generate_key().decode()
        self.assertEqual(require_crypto(good).decrypt(
            require_crypto(good).encrypt("x")), "x")


class SharedCryptoTests(unittest.TestCase):
    """The launcher and the runtime profile switcher must use ONE Crypto.
    They previously had separate classes: only the launcher's was hardened,
    and the switcher's coerced values differently."""

    def test_one_class_shared_by_every_module(self):
        self.assertIs(s3duck.Crypto, utils.Crypto)
        self.assertIs(profile_switcher.Crypto, utils.Crypto)
        self.assertIs(s3duck.CredentialError, utils.CredentialError)

    def test_decrypt_accepts_bytes_str_and_qbytearray(self):
        """QSettings hands back bytes or QByteArray depending on the backend.
        REGRESSION: the switcher coerced with `str(value).encode()`, which for
        a QByteArray yields the literal repr b\"b'…'\" and never decrypts."""
        crypto = Crypto(Fernet.generate_key().decode())
        token = crypto.encrypt("secret")
        self.assertEqual(crypto.decrypt(token), "secret")
        self.assertEqual(crypto.decrypt(token.decode()), "secret")
        self.assertEqual(crypto.decrypt(QByteArray(token)), "secret")

    def test_missing_value_is_an_error_for_a_required_field(self):
        """An absent stored value and a key mismatch need different remedies,
        so blaming the key for an empty field would misdirect the user."""
        crypto = Crypto(Fernet.generate_key().decode())
        for empty in (None, ""):
            with self.subTest(value=empty):
                with self.assertRaises(CredentialError) as ctx:
                    crypto.decrypt(empty)
                self.assertIn("empty", str(ctx.exception).lower())
                self.assertNotIn("different one", str(ctx.exception))

    def test_decrypt_optional_is_shared_and_forgiving(self):
        crypto = Crypto(Fernet.generate_key().decode())
        self.assertEqual(utils.decrypt_optional(crypto, None), "")
        self.assertEqual(utils.decrypt_optional(crypto, b"garbage"), "")
        self.assertEqual(
            utils.decrypt_optional(crypto, crypto.encrypt("tok")), "tok")


class ProfileSummaryTests(unittest.TestCase):
    """The launcher list showed names only, so profiles differing only by
    endpoint/region were indistinguishable and the read-only and
    TLS-unverified flags were invisible until after connecting."""

    @staticmethod
    def _item(**kw):
        fields = dict(
            name="prod", url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="", enc_access_key=b"", enc_secret_key=b"",
            no_ssl_check="false", use_path="false", enc_session_token=b"",
            read_only="false")
        fields.update(kw)
        return SettingsItem(**fields)

    def test_endpoint_and_region(self):
        self.assertEqual(profile_summary(self._item()),
                         "https://s3.amazonaws.com · us-east-1")

    def test_bucket_is_included_when_pinned(self):
        self.assertIn("logs", profile_summary(self._item(bucket_name="logs")))

    def test_flags_are_badges_not_summary_text(self):
        """A safety flag appended to the dim endpoint line reads as more
        metadata, so the flags live beside the name instead."""
        item = self._item(read_only="true", no_ssl_check="true")
        self.assertEqual(profile_badges(item), ["read-only", "TLS unverified"])
        summary = profile_summary(item)
        self.assertNotIn("read-only", summary)
        self.assertNotIn("TLS unverified", summary)

    def test_badges_are_absent_when_flags_are_off(self):
        self.assertEqual(profile_badges(self._item()), [])

    def test_each_flag_stands_alone(self):
        self.assertEqual(profile_badges(self._item(read_only="true")),
                         ["read-only"])
        self.assertEqual(profile_badges(self._item(no_ssl_check="true")),
                         ["TLS unverified"])

    def test_missing_endpoint_is_called_out(self):
        self.assertIn("(no endpoint)", profile_summary(self._item(url="")))

    def test_blank_optional_fields_leave_no_empty_separators(self):
        summary = profile_summary(self._item(region="", bucket_name=""))
        self.assertEqual(summary, "https://s3.amazonaws.com")
        self.assertNotIn("··", summary)
        self.assertFalse(summary.endswith("·"))


class LauncherListTests(unittest.TestCase):
    """Rows, keyboard handling and the non-blocking connect probe."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, items=()):
        win = s3duck.Profiles()
        # Never write to the real config from a test.
        win.settings = QSettings("s3duck-tests", "s3duck-tests")
        win.items = list(items)
        win.populate_list()
        if win.items:
            win.listWidget.setCurrentRow(0)
        self.addCleanup(win.close)
        return win

    @staticmethod
    def _item(name="prod", **kw):
        fields = dict(
            name=name, url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="", enc_access_key=b"", enc_secret_key=b"",
            no_ssl_check="false", use_path="false", enc_session_token=b"",
            read_only="false")
        fields.update(kw)
        return SettingsItem(**fields)

    def test_rows_show_the_name_and_the_summary(self):
        win = self._launcher([self._item(read_only="true")])
        row = win.listWidget.item(0)
        first, _, second = row.text().partition("\n")
        self.assertEqual(first, "prod")
        self.assertIn("us-east-1", second)

    def test_rows_carry_their_badges_for_the_delegate(self):
        win = self._launcher([self._item(read_only="true", no_ssl_check="true"),
                              self._item("plain")])
        self.assertEqual(
            win.listWidget.item(0).data(Qt.ItemDataRole.UserRole),
            ["read-only", "TLS unverified"])
        self.assertEqual(
            win.listWidget.item(1).data(Qt.ItemDataRole.UserRole), [])

    def test_the_tooltip_still_carries_everything(self):
        """The row elides; the tooltip is the fallback for the full truth."""
        win = self._launcher([self._item(read_only="true")])
        tip = win.listWidget.item(0).toolTip()
        self.assertIn("s3.amazonaws.com", tip)
        self.assertIn("us-east-1", tip)
        self.assertIn("read-only", tip)

    def _paint_row(self, win, row, selected=False, width=400):
        """Drive the delegate directly. Qt swallows a Python error raised
        inside a virtual call into a process abort, so calling paint() from
        the test is what turns a mistake there into a test failure."""
        delegate = win.listWidget.itemDelegate()
        pixmap = QPixmap(width, 60)
        pixmap.fill()
        painter = QPainter(pixmap)
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, width, 60)
        option.font = win.listWidget.font()
        option.palette = win.listWidget.palette()
        if selected:
            option.state |= QStyle.StateFlag.State_Selected
        try:
            delegate.paint(painter, option,
                           win.listWidget.model().index(row, 0))
        finally:
            painter.end()
        return delegate, option

    def test_rows_paint_in_both_states(self):
        """REGRESSION: paint() referenced a name the launcher never imported;
        every unit test passed while the real window crashed on first draw."""
        win = self._launcher([self._item(read_only="true"),
                              self._item("dev", url="", region="")])
        for row in (0, 1):
            for selected in (False, True):
                with self.subTest(row=row, selected=selected):
                    self._paint_row(win, row, selected)

    def test_the_summary_font_is_smaller_but_stays_legible(self):
        base = QFont()
        base.setPointSizeF(11.0)
        self.assertLess(s3duck._summary_font(base).pointSizeF(), 11.0)

        tiny = QFont()
        tiny.setPointSizeF(7.0)
        self.assertGreaterEqual(s3duck._summary_font(tiny).pointSizeF(), 7.0)

    def test_a_pixel_sized_font_is_left_alone(self):
        """pointSizeF() is -1 for a pixel-sized font; subtracting from it
        would ask Qt for a negative point size."""
        pixels = QFont()
        pixels.setPixelSize(14)
        self.assertEqual(s3duck._summary_font(pixels).pixelSize(), 14)

    def test_the_list_uses_the_profile_delegate(self):
        win = self._launcher([self._item()])
        self.assertIsInstance(win.listWidget.itemDelegate(),
                              s3duck.ProfileRowDelegate)

    def test_badges_paint_and_keep_their_own_colours(self):
        win = self._launcher([self._item(read_only="true", no_ssl_check="true")])
        for selected in (False, True):
            with self.subTest(selected=selected):
                self._paint_row(win, 0, selected)

        palette = QPalette()
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        read_only = s3duck.badge_color("read-only", palette, False)
        insecure = s3duck.badge_color("TLS unverified", palette, False)
        self.assertNotEqual(read_only, insecure)
        self.assertEqual(read_only, s3duck.BADGE_COLORS["read-only"])

    def test_a_selected_badge_stays_readable_on_the_highlight(self):
        """Its own colour would sit on the highlight brush and lose contrast."""
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        self.assertEqual(s3duck.badge_color("read-only", palette, True),
                         QColor("#ffffff"))

    def test_a_long_name_cannot_push_a_badge_off_the_row(self):
        """A truncated safety flag is worse than a truncated name, so badge
        width is reserved before the name is elided."""
        long_name = "a" * 400
        win = self._launcher([self._item(long_name, read_only="true")])
        badge_width = QFontMetrics(
            s3duck._summary_font(win.listWidget.font())
        ).horizontalAdvance("[read-only]")

        painted = []
        with patch.object(QPainter, "drawText",
                          lambda self, x, y, text: painted.append((x, text))):
            _, option = self._paint_row(win, 0, width=240)

        drawn = [t for _, t in painted]
        self.assertIn("[read-only]", drawn)
        name_text = painted[0][1]
        self.assertTrue(name_text.endswith("…"), f"name not elided: {name_text}")
        # The badge must end inside the row. paint() insets by 6px a side, so
        # the usable right edge is rect.right() - 6.
        badge_x = next(x for x, t in painted if t == "[read-only]")
        self.assertLessEqual(badge_x + badge_width, option.rect.right() - 6)

    def test_a_selected_row_uses_the_highlight_pen(self):
        """Without the switch the name is drawn in the ordinary text colour on
        top of the highlight brush — dark on dark on most themes."""
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Text, QColor("#101010"))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        self.assertEqual(s3duck.row_text_color(palette, True),
                         QColor("#ffffff"))
        self.assertEqual(s3duck.row_text_color(palette, False),
                         QColor("#101010"))

    def test_a_row_is_tall_enough_for_both_lines(self):
        win = self._launcher([self._item()])
        delegate, option = self._paint_row(win, 0)
        index = win.listWidget.model().index(0, 0)
        two_line = delegate.sizeHint(option, index).height()
        single = QFontMetrics(win.listWidget.font()).height()
        self.assertGreater(two_line, single * 1.5)

    def test_a_row_without_a_summary_stays_compact(self):
        win = self._launcher([self._item()])
        win.listWidget.item(0).setText("just-a-name")
        delegate, option = self._paint_row(win, 0)
        index = win.listWidget.model().index(0, 0)
        bare = delegate.sizeHint(option, index).height()
        win.listWidget.item(0).setText("just-a-name\nwith a summary")
        tall = delegate.sizeHint(option, index).height()
        self.assertLess(bare, tall)

    def test_buttons_do_not_steal_enter(self):
        """REGRESSION: QDialog buttons are autoDefault, so Enter on the list
        fired Add — the first button — instead of opening the profile."""
        win = self._launcher([self._item()])
        for button in (win.btnRun, win.btnAdd, win.btnEdit, win.btnDelete):
            with self.subTest(button=button.text()):
                self.assertFalse(button.autoDefault())
                self.assertFalse(button.isDefault())

    def _press(self, win, key):
        event = QKeyEvent(QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier)
        return win.eventFilter(win.listWidget, event)

    def test_keys_drive_the_matching_actions(self):
        for key, handler in ((Qt.Key.Key_Return, "onStart"),
                             (Qt.Key.Key_Enter, "onStart"),
                             (Qt.Key.Key_Delete, "onDelete"),
                             (Qt.Key.Key_F2, "onEdit")):
            with self.subTest(key=key):
                win = self._launcher([self._item()])
                with patch.object(s3duck.Profiles, handler) as called:
                    self.assertTrue(self._press(win, key))
                called.assert_called_once_with()

    def test_other_keys_are_left_to_the_list(self):
        """Arrow keys and type-ahead must keep working."""
        win = self._launcher([self._item()])
        for key in (Qt.Key.Key_Down, Qt.Key.Key_A, Qt.Key.Key_Escape):
            with self.subTest(key=key):
                self.assertFalse(self._press(win, key))

    def test_connect_probe_runs_off_the_gui_thread(self):
        """REGRESSION (freeze): list_buckets() ran inline in the slot, so an
        unreachable endpoint locked the launcher for the whole connect
        timeout with no way to cancel."""
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel") as model, \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(True, None)) as runner:
            win.onStart()
        runner.assert_called_once()
        self.assertIs(runner.call_args.args[0], win)
        # the probe must be handed over as a callable, not already invoked
        self.assertFalse(model.return_value.list_buckets.called)
        main.assert_called_once()

    def test_cancelling_the_probe_does_not_open_a_window(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, None)):
            win.onStart()
        main.assert_not_called()
        box.assert_not_called()          # a cancel is not an error

    def test_a_failed_probe_reports_and_does_not_open_a_window(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, RuntimeError("host down"))):
            win.onStart()
        main.assert_not_called()
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("host down", shown)

    def test_check_profile_runs_off_the_gui_thread(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel") as model, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=((False, "denied"), None)) as runner:
            win.check_profile()
        runner.assert_called_once()
        self.assertFalse(model.return_value.check_profile.called)
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("denied", shown)

    def test_check_profile_reports_a_raised_error(self):
        """run_with_progress returns the exception rather than raising it, and
        an unhandled one here would abort the process from a Qt slot."""
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, RuntimeError("no route"))):
            win.check_profile()
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("no route", shown)

    def test_cancelling_check_profile_reports_nothing(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, None)):
            win.check_profile()
        box.assert_not_called()


class EncryptionPropertiesTests(unittest.TestCase):
    """Properties a credential store must actually hold, as opposed to lines
    merely being executed: secrets must not be recoverable from the stored
    blob, and a value written in one process must read back in another."""

    SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    def test_generate_key_yields_a_usable_and_unique_key(self):
        first, second = Crypto.generate_key(), Crypto.generate_key()
        self.assertNotEqual(first, second)
        self.assertIsInstance(first, str)
        crypto = Crypto(first)
        self.assertEqual(crypto.decrypt(crypto.encrypt(self.SECRET)), self.SECRET)

    def test_plaintext_never_appears_in_the_stored_blob(self):
        """The whole point of encrypting credentials at rest."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt(self.SECRET)
        self.assertNotIn(self.SECRET.encode(), token)
        self.assertNotIn(b"wJalrXUt", token)

    def test_the_same_value_encrypts_differently_each_time(self):
        """Fernet randomises the IV; identical ciphertexts would leak that two
        profiles share a secret."""
        crypto = Crypto(Crypto.generate_key())
        first = crypto.encrypt(self.SECRET)
        second = crypto.encrypt(self.SECRET)
        self.assertNotEqual(first, second)
        self.assertEqual(crypto.decrypt(first), crypto.decrypt(second))

    def test_a_token_survives_a_new_crypto_instance(self):
        """Written on save, read back by a later process — the real lifecycle."""
        key = Crypto.generate_key()
        token = Crypto(key).encrypt(self.SECRET)
        self.assertEqual(Crypto(key).decrypt(token), self.SECRET)

    def test_a_bytes_key_is_accepted(self):
        key = Fernet.generate_key()          # bytes, not str
        crypto = Crypto(key)
        self.assertEqual(crypto.decrypt(crypto.encrypt("v")), "v")

    def test_round_trips_awkward_values(self):
        crypto = Crypto(Crypto.generate_key())
        for value in ("", "ünïcødé-Ключ-🦆", "x" * 4096, "line\nbreak\ttab",
                      "aws/session+token=="):
            with self.subTest(value=value[:20]):
                self.assertEqual(crypto.decrypt(crypto.encrypt(value)), value)

    def test_empty_plaintext_round_trips_but_an_empty_token_errors(self):
        """Two different things that both look 'empty': a stored empty secret
        is valid and returns ''; a missing stored value is an error."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt("")
        self.assertTrue(token)
        self.assertEqual(crypto.decrypt(token), "")
        with self.assertRaises(CredentialError):
            crypto.decrypt("")

    def test_encrypt_coerces_none_to_empty(self):
        crypto = Crypto(Crypto.generate_key())
        self.assertEqual(crypto.decrypt(crypto.encrypt(None)), "")

    def test_decrypt_falls_back_to_str_for_exotic_values(self):
        """QSettings-like wrappers that are neither bytes nor str and cannot
        be bytes()-converted still have to decrypt."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt(self.SECRET)

        class Wrapped:
            def __init__(self, raw):
                self._raw = raw

            def __str__(self):
                return self._raw.decode()

        with self.assertRaises(TypeError):
            bytes(Wrapped(token))            # the fallback really is needed
        self.assertEqual(crypto.decrypt(Wrapped(token)), self.SECRET)

    def test_a_token_is_worthless_without_the_key(self):
        token = Crypto(Crypto.generate_key()).encrypt(self.SECRET)
        with self.assertRaises(CredentialError):
            Crypto(Crypto.generate_key()).decrypt(token)


class ProfileSwitcherCryptoTests(unittest.TestCase):
    """The runtime profile switcher shows `str(exc)` in a warning box."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settings(self, key):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        settings.beginGroup("common")
        settings.setValue("key", key)
        settings.endGroup()
        return settings

    def _raw(self, crypto):
        return {
            "name": "prod", "url": "https://s3.amazonaws.com",
            "region": "us-east-1", "bucket_name": "",
            "access_key": crypto.encrypt("AKID"),
            "secret_key": crypto.encrypt("SECRET"),
            "no_ssl_check": "false", "use_path": "true",
            "session_token": crypto.encrypt("TOK"), "read_only": "false",
        }

    def test_good_key_round_trips(self):
        crypto = Crypto(Fernet.generate_key().decode())
        prof = profile_switcher.decrypt_profile(
            self._settings(crypto.key), self._raw(crypto))
        self.assertEqual(prof.access_key, "AKID")
        self.assertEqual(prof.secret_key, "SECRET")
        self.assertEqual(prof.session_token, "TOK")

    def test_wrong_key_error_message_is_not_blank(self):
        """REGRESSION: a mismatched key raised InvalidToken, whose str() is
        EMPTY — the switcher then showed a warning box with no text at all."""
        crypto = Crypto(Fernet.generate_key().decode())
        raw = self._raw(crypto)
        other = Fernet.generate_key().decode()
        with self.assertRaises(CredentialError) as ctx:
            profile_switcher.decrypt_profile(self._settings(other), raw)
        self.assertTrue(str(ctx.exception).strip())
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_missing_key_is_reported(self):
        crypto = Crypto(Fernet.generate_key().decode())
        with self.assertRaises(CredentialError) as ctx:
            profile_switcher.decrypt_profile(
                self._settings(""), self._raw(crypto))
        self.assertIn("missing", str(ctx.exception).lower())


class SelectedRowIndexTests(unittest.TestCase):
    """REGRESSION: Qt returns row -1 with no selection, and `items[-1]` would
    silently act on the LAST profile — copy/check operated on the wrong one."""

    def test_no_selection_returns_minus_one(self):
        self.assertEqual(selected_row_index(-1, 3), -1)
        self.assertEqual(selected_row_index(None, 3), -1)

    def test_stale_row_past_the_end_is_rejected(self):
        self.assertEqual(selected_row_index(5, 3), -1)
        self.assertEqual(selected_row_index(0, 0), -1)

    def test_valid_rows_pass_through(self):
        self.assertEqual(selected_row_index(0, 3), 0)
        self.assertEqual(selected_row_index(2, 3), 2)

    def test_empty_list_is_never_indexable(self):
        for row in (-1, 0, 1):
            self.assertEqual(selected_row_index(row, 0), -1)


class FindDuplicateGroupsTests(unittest.TestCase):
    MD5_A = "a" * 32
    MD5_B = "b" * 32

    def test_same_size_and_etag_is_a_confirmed_group(self):
        groups = find_duplicate_groups([
            ("one.txt", 100, self.MD5_A, _dt(1)),
            ("two.txt", 100, self.MD5_A, _dt(2)),
            ("other.txt", 100, self.MD5_B, _dt(3)),
        ])
        self.assertEqual(len(groups), 1)
        group = groups[0]
        self.assertTrue(group["confirmed"])
        self.assertEqual([k for k, _m in group["members"]],
                         ["one.txt", "two.txt"])
        self.assertEqual(group["wasted"], 100)

    def test_distinct_plain_etags_are_never_grouped(self):
        """Two different MD5s prove the contents differ — grouping them would
        invite deleting a file that is not a duplicate."""
        groups = find_duplicate_groups([
            ("one.txt", 100, self.MD5_A, _dt(1)),
            ("two.txt", 100, self.MD5_B, _dt(2)),
        ])
        self.assertEqual(groups, [])

    def test_multipart_etags_that_match_are_confirmed(self):
        etag = self.MD5_A + "-4"
        groups = find_duplicate_groups([
            ("one.bin", 10 ** 8, etag, _dt(1)),
            ("two.bin", 10 ** 8, etag, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertTrue(groups[0]["confirmed"])

    def test_multipart_vs_single_part_is_unconfirmed(self):
        """Identical bytes uploaded with different part sizes yield different
        ETags, so same-size-different-ETag is a candidate, not a finding."""
        groups = find_duplicate_groups([
            ("one.bin", 500, self.MD5_A, _dt(1)),
            ("two.bin", 500, self.MD5_B + "-2", _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(groups[0]["etag"], "")

    def test_two_blank_etags_are_never_confirmed(self):
        """A backend that returns no ETag tells us nothing about the content;
        grouping such objects as confirmed would make them auto-selectable for
        deletion on nothing more than a matching size."""
        groups = find_duplicate_groups([
            ("a.bin", 500, "", _dt(1)),
            ("b.bin", 500, "", _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(groups[0]["etag"], "")
        self.assertEqual(select_redundant_keys(groups), set())

    def test_missing_etag_makes_a_size_match_unconfirmed(self):
        groups = find_duplicate_groups([
            ("one.bin", 500, "", _dt(1)),
            ("two.bin", 500, self.MD5_A, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])

    def test_confirmed_and_unconfirmed_coexist_at_one_size(self):
        groups = find_duplicate_groups([
            ("a1.bin", 500, self.MD5_A, _dt(1)),
            ("a2.bin", 500, self.MD5_A, _dt(2)),
            ("b.bin", 500, self.MD5_B + "-3", _dt(3)),
            ("c.bin", 500, "", _dt(4)),
        ])
        kinds = sorted(g["confirmed"] for g in groups)
        self.assertEqual(kinds, [False, True])
        unconfirmed = next(g for g in groups if not g["confirmed"])
        self.assertEqual([k for k, _m in unconfirmed["members"]],
                         ["b.bin", "c.bin"])

    def test_zero_byte_objects_are_excluded_by_default(self):
        self.assertEqual(find_duplicate_groups([
            ("a", 0, self.MD5_A, _dt(1)),
            ("b", 0, self.MD5_A, _dt(2)),
        ]), [])

    def test_min_size_filter(self):
        entries = [("a", 10, self.MD5_A, _dt(1)), ("b", 10, self.MD5_A, _dt(2))]
        self.assertEqual(len(find_duplicate_groups(entries, min_size=10)), 1)
        self.assertEqual(find_duplicate_groups(entries, min_size=11), [])

    def test_folder_placeholders_are_ignored(self):
        self.assertEqual(find_duplicate_groups([
            ("dir/", 100, self.MD5_A, _dt(1)),
            ("dir2/", 100, self.MD5_A, _dt(2)),
        ]), [])

    def test_groups_sorted_by_reclaimable_bytes(self):
        groups = find_duplicate_groups([
            ("s1", 10, self.MD5_A, _dt(1)),
            ("s2", 10, self.MD5_A, _dt(2)),
            ("big1", 900, self.MD5_B, _dt(1)),
            ("big2", 900, self.MD5_B, _dt(2)),
        ])
        self.assertEqual([g["size"] for g in groups], [900, 10])

    def test_three_copies_report_two_wasted(self):
        groups = find_duplicate_groups([
            ("a", 50, self.MD5_A, _dt(1)),
            ("b", 50, self.MD5_A, _dt(2)),
            ("c", 50, self.MD5_A, _dt(3)),
        ])
        self.assertEqual(groups[0]["count"], 3)
        self.assertEqual(groups[0]["wasted"], 100)

    def test_quoted_etags_are_normalized(self):
        groups = find_duplicate_groups([
            ("a", 10, f'"{self.MD5_A}"', _dt(1)),
            ("b", 10, self.MD5_A, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)

    def test_empty_input(self):
        self.assertEqual(find_duplicate_groups([]), [])
        self.assertEqual(find_duplicate_groups(None), [])

    def test_summary_counts_only_confirmed_bytes(self):
        groups = find_duplicate_groups([
            ("a1", 100, self.MD5_A, _dt(1)),
            ("a2", 100, self.MD5_A, _dt(2)),
            ("u1", 700, "", _dt(1)),
            ("u2", 700, self.MD5_B, _dt(2)),
        ])
        summary = summarize_duplicate_groups(groups)
        self.assertEqual(summary["groups"], 2)
        self.assertEqual(summary["confirmed_groups"], 1)
        self.assertEqual(summary["redundant"], 2)
        # unconfirmed bytes are not promised as reclaimable
        self.assertEqual(summary["wasted"], 100)


class SelectRedundantKeysTests(unittest.TestCase):
    MD5 = "c" * 32

    def _groups(self):
        return find_duplicate_groups([
            ("old.txt", 10, self.MD5, _dt(1)),
            ("mid.txt", 10, self.MD5, _dt(2)),
            ("new.txt", 10, self.MD5, _dt(3)),
        ])

    def test_keep_newest_selects_the_rest(self):
        self.assertEqual(
            select_redundant_keys(self._groups(), keep="newest"),
            {"old.txt", "mid.txt"})

    def test_keep_oldest_selects_the_rest(self):
        self.assertEqual(
            select_redundant_keys(self._groups(), keep="oldest"),
            {"mid.txt", "new.txt"})

    def test_unconfirmed_groups_are_never_auto_selected(self):
        groups = find_duplicate_groups([
            ("a", 500, "", _dt(1)),
            ("b", 500, "d" * 32, _dt(2)),
        ])
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(select_redundant_keys(groups), set())

    def test_undated_members_are_treated_as_oldest(self):
        groups = find_duplicate_groups([
            ("dated.txt", 10, self.MD5, _dt(5)),
            ("undated.txt", 10, self.MD5, None),
        ])
        self.assertEqual(
            select_redundant_keys(groups, keep="newest"), {"undated.txt"})

    def test_every_group_keeps_one_survivor(self):
        groups = find_duplicate_groups([
            ("g1a", 10, self.MD5, _dt(1)), ("g1b", 10, self.MD5, _dt(2)),
            ("g2a", 20, "e" * 32, _dt(1)), ("g2b", 20, "e" * 32, _dt(2)),
        ])
        selected = select_redundant_keys(groups)
        for group in groups:
            survivors = [k for k, _m in group["members"] if k not in selected]
            self.assertEqual(len(survivors), 1)

    def test_bad_keep_value_rejected(self):
        with self.assertRaises(ValueError):
            select_redundant_keys([], keep="sideways")


class ListObjectDigestsTests(unittest.TestCase):
    def test_returns_key_size_etag_and_modified(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 5, "ETag": '"abc"', "LastModified": _dt(1)},
            {"Key": "dir/", "Size": 0, "ETag": '""', "LastModified": _dt(1)},
        ]}])
        rows = m.list_object_digests("")
        self.assertEqual(rows, [("a.txt", 5, "abc", _dt(1))])

    def test_requires_a_bucket(self):
        with self.assertRaises(ValueError):
            make_model(bucket="").list_object_digests("")


class BookmarkStorageTests(unittest.TestCase):
    def test_round_trip(self):
        entries = [
            {"name": "prod logs", "bucket": "bkt", "prefix": "logs/2026/"},
            {"name": "root", "bucket": "other", "prefix": ""},
        ]
        self.assertEqual(parse_bookmarks(serialize_bookmarks(entries)), entries)

    def test_malformed_lines_are_dropped_not_fatal(self):
        raw = "good\tbkt\tp/\nbroken line\n\t\t\nalso\tb2\t"
        parsed = parse_bookmarks(raw)
        self.assertEqual([b["bucket"] for b in parsed], ["bkt", "b2"])

    def test_tabs_and_newlines_in_a_name_cannot_split_a_record(self):
        raw = serialize_bookmarks(
            [{"name": "a\tb\nc", "bucket": "bkt", "prefix": "p/"}])
        self.assertEqual(len(raw.splitlines()), 1)
        self.assertEqual(len(parse_bookmarks(raw)), 1)

    def test_missing_name_falls_back_to_the_location(self):
        parsed = parse_bookmarks("\tbkt\tlogs/")
        self.assertEqual(parsed[0]["name"], "bkt/logs/")

    def test_label_formatting(self):
        self.assertEqual(bookmark_label("bkt", "a/b/"), "bkt/a/b/")
        self.assertEqual(bookmark_label("bkt", ""), "bkt")
        self.assertEqual(bookmark_label("", ""), "/")

    def test_empty_input(self):
        self.assertEqual(parse_bookmarks(""), [])
        self.assertEqual(parse_bookmarks(None), [])
        self.assertEqual(serialize_bookmarks([]), "")

    def test_add_rejects_a_duplicate_location(self):
        entries, added = add_bookmark_to([], "one", "bkt", "p/")
        self.assertTrue(added)
        entries, added = add_bookmark_to(entries, "other name", "bkt", "p/")
        self.assertFalse(added)
        self.assertEqual(len(entries), 1)

    def test_add_allows_the_same_bucket_at_another_prefix(self):
        entries, _ = add_bookmark_to([], "root", "bkt", "")
        entries, added = add_bookmark_to(entries, "deep", "bkt", "a/")
        self.assertTrue(added)
        self.assertEqual(len(entries), 2)

    def test_add_defaults_the_name(self):
        entries, _ = add_bookmark_to([], "", "bkt", "a/")
        self.assertEqual(entries[0]["name"], "bkt/a/")


class BuildPasteJobTests(unittest.TestCase):
    def _clip(self, mode="copy", bucket="src", items=None):
        return {"mode": mode, "bucket": bucket,
                "items": items or [("a.txt", "from/a.txt", False),
                                   ("dir", "from/dir/", True)]}

    def test_same_bucket_paste_targets_the_current_prefix(self):
        job, skipped = build_paste_job(self._clip(), "src", "to/")
        self.assertEqual(job, [
            ("from/a.txt", "to/a.txt", False, None),
            ("from/dir/", "to/dir/", True, None),
        ])
        self.assertEqual(skipped, [])

    def test_cross_bucket_paste_carries_the_destination(self):
        job, _skipped = build_paste_job(self._clip(), "other", "to/")
        self.assertEqual({entry[3] for entry in job}, {"other"})

    def test_paste_into_the_same_place_is_skipped(self):
        job, skipped = build_paste_job(
            self._clip(items=[("a.txt", "to/a.txt", False)]), "src", "to/")
        self.assertEqual(job, [])
        self.assertEqual(skipped, ["a.txt"])

    def test_folder_cannot_be_pasted_into_itself(self):
        job, skipped = build_paste_job(
            self._clip(items=[("dir", "dir/", True)]), "src", "dir/sub/")
        self.assertEqual(job, [])
        self.assertEqual(skipped, ["dir"])

    def test_same_prefix_in_another_bucket_is_allowed(self):
        job, skipped = build_paste_job(
            self._clip(items=[("a.txt", "to/a.txt", False)]), "other", "to/")
        self.assertEqual(len(job), 1)
        self.assertEqual(skipped, [])

    def test_paste_at_bucket_root(self):
        job, _s = build_paste_job(
            self._clip(items=[("a.txt", "from/a.txt", False)]), "src", "")
        self.assertEqual(job[0][1], "a.txt")

    def test_empty_clipboard(self):
        self.assertEqual(build_paste_job(None, "b", "p/"), ([], []))
        self.assertEqual(build_paste_job({}, "b", "p/"), ([], []))


class MergeTagsTests(unittest.TestCase):
    CURRENT = [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "ops"}]

    def test_add_merges_over_existing(self):
        out = Model.merge_tags(self.CURRENT, add={"owner": "vlad"})
        self.assertEqual(
            {t["Key"]: t["Value"] for t in out},
            {"env": "prod", "team": "ops", "owner": "vlad"})

    def test_add_overwrites_the_same_key(self):
        out = Model.merge_tags(self.CURRENT, add={"env": "staging"})
        self.assertEqual(
            {t["Key"]: t["Value"] for t in out}["env"], "staging")

    def test_remove_drops_keys(self):
        out = Model.merge_tags(self.CURRENT, remove=["team", "absent"])
        self.assertEqual([t["Key"] for t in out], ["env"])

    def test_replace_discards_everything_else(self):
        out = Model.merge_tags(self.CURRENT, add={"only": "1"}, replace=True)
        self.assertEqual(out, [{"Key": "only", "Value": "1"}])

    def test_existing_order_is_preserved(self):
        out = Model.merge_tags(self.CURRENT, add={"z": "1"})
        self.assertEqual([t["Key"] for t in out], ["env", "team", "z"])

    def test_values_are_stringified(self):
        out = Model.merge_tags([], add={"n": 5})
        self.assertEqual(out, [{"Key": "n", "Value": "5"}])

    def test_update_reads_merges_and_writes(self):
        m = make_model(bucket="b")

        class TagClient(FakeS3Client):
            def get_object_tagging(self, **kw):
                self.calls.append(("get_object_tagging", kw))
                return {"TagSet": [{"Key": "env", "Value": "prod"}]}

            def put_object_tagging(self, **kw):
                self.calls.append(("put_object_tagging", kw))
                return {}

        c = TagClient()
        m._client = c
        m.update_object_tags("a.txt", add={"owner": "vlad"}, remove=["env"])
        put = c.calls_of("put_object_tagging")[0]
        self.assertEqual(put["Tagging"]["TagSet"],
                         [{"Key": "owner", "Value": "vlad"}])

    def test_replace_skips_the_read(self):
        m = make_model(bucket="b")

        class TagClient(FakeS3Client):
            def get_object_tagging(self, **kw):
                raise AssertionError("must not read when replacing")

            def put_object_tagging(self, **kw):
                self.calls.append(("put_object_tagging", kw))
                return {}

        m._client = TagClient()
        m.update_object_tags("a.txt", add={"a": "b"}, replace=True)

    def test_read_only_refuses(self):
        m = make_model(bucket="b", read_only=True)
        m._client = FakeS3Client()
        with self.assertRaises(ReadOnlyError):
            m.update_object_tags("a.txt", add={"a": "b"})


class StreamObjectTests(unittest.TestCase):
    def test_yields_chunks_and_charges_the_limiter(self):
        m = make_model(bucket="b")
        payload = b"x" * 4096

        class Client:
            def get_object(self, **kw):
                return {"Body": io.BytesIO(payload)}

        m._client = Client()
        m.set_rate_limit(10 ** 9)
        chunks = list(m.stream_object("k", chunk_size=1024))
        self.assertEqual(b"".join(chunks), payload)
        self.assertEqual(len(chunks), 4)

    def test_cancellation_stops_the_stream(self):
        m = make_model(bucket="b")

        class Client:
            def get_object(self, **kw):
                return {"Body": io.BytesIO(b"y" * 4096)}

        m._client = Client()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            list(m.stream_object("k", chunk_size=1024, cancel_event=cancel))

    def test_requires_a_bucket(self):
        with self.assertRaises(ValueError):
            list(make_model(bucket="").stream_object("k"))


class HexDumpTests(unittest.TestCase):
    def test_layout(self):
        out = hex_dump(b"AB\x00\xff")
        self.assertTrue(out.startswith("00000000  41 42 00 ff"))
        self.assertIn("|AB..|", out)

    def test_multiple_rows_and_offsets(self):
        out = hex_dump(bytes(range(48)), width=16)
        lines = out.splitlines()
        self.assertEqual(len(lines), 3)
        self.assertTrue(lines[1].startswith("00000010"))
        self.assertTrue(lines[2].startswith("00000020"))

    def test_truncation_is_announced(self):
        out = hex_dump(b"z" * 100, max_bytes=32)
        self.assertIn("68 more byte(s) not shown", out)

    def test_empty(self):
        self.assertEqual(hex_dump(b""), "")
        self.assertEqual(hex_dump(None), "")


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

    def test_non_transfer_ops_do_not_inherit_stale_byte_counts(self):
        """REGRESSION: a delete recorded right after a big download showed the
        download's byte total, because _smooth_done was read unconditionally."""
        class NoopDeleter:
            def delete(self, key, log_fn=None, cancel_event=None):
                return True

        self.win._smooth_done = 12345          # residue of an earlier transfer
        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: NoopDeleter()):
            self.win.assign_thread_operation(
                "delete", ["a.txt"], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()
        record = self.win.load_transfer_history()[0]
        self.assertEqual(record["method"], "delete")
        self.assertEqual(record["bytes"], 0)

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


class ZipDownloadWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.blobs = {"pre/a.txt": b"alpha", "pre/dir/b.txt": b"bravo"}

        def get_keys(self, prefix, log_fn=None):
            return [(k, len(v)) for k, v in self.blobs.items()
                    if k.startswith(prefix)]

        def get_size(self, key):
            return len(self.blobs.get(key, b""))

        def stream_object(self, key, chunk_size=1024 * 1024, cancel_event=None):
            yield self.blobs[key]

    def test_files_and_folders_land_in_one_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            job = [
                (zip_path, "pre/", "pre/a.txt", False),
                (zip_path, "pre/", "pre/dir/", True),
            ]
            worker = Worker(self.FakeModel(), job)
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [False])
            with zipfile.ZipFile(zip_path) as zf:
                # arcnames are relative to the browsed prefix
                self.assertEqual(sorted(zf.namelist()), ["a.txt", "dir/b.txt"])
                self.assertEqual(zf.read("dir/b.txt"), b"bravo")

    def test_cancelling_before_it_starts_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            worker = Worker(self.FakeModel(),
                            [(zip_path, "pre/", "pre/a.txt", False)])
            worker.cancel()
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [True])
            self.assertFalse(os.path.exists(zip_path))

    def test_cancelling_midway_deletes_the_partial_archive(self):
        """The archive is already open by then, so the cleanup path has to
        actually remove it — a truncated zip cannot be opened."""
        outer = self

        class CancelMidway(outer.FakeModel):
            def __init__(self):
                super().__init__()
                self.cancel_event = None

            def stream_object(self, key, chunk_size=1024 * 1024,
                              cancel_event=None):
                yield b"partial"
                self.cancel_event.set()
                raise TransferCancelled("cancelled")

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            model = CancelMidway()
            worker = Worker(model, [(zip_path, "pre/", "pre/a.txt", False)])
            model.cancel_event = worker._cancel_event
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [True])
            self.assertFalse(os.path.exists(zip_path))

    def test_failure_is_reported(self):
        class Failing(self.FakeModel):
            def stream_object(self, key, chunk_size=1024 * 1024,
                              cancel_event=None):
                raise RuntimeError("read error")
                yield b""      # pragma: no cover

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            worker = Worker(Failing(), [(zip_path, "pre/", "pre/a.txt", False)])
            errors = []
            worker.error.connect(errors.append)
            worker.zip_download()
            self.assertEqual(errors, ["read error"])


class SetTagsWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.calls = []

        def get_keys(self, prefix, log_fn=None):
            return [(prefix + "a.txt", 1), (prefix + "b.txt", 2), (prefix, 0)]

        def update_object_tags(self, key, add=None, remove=None, replace=False):
            self.calls.append((key, dict(add or {}), list(remove or []), replace))

    def test_folders_fan_out_to_every_object(self):
        model = self.FakeModel()
        job = [("f.txt", False, {"env": "prod"}, [], False),
               ("dir/", True, {"env": "prod"}, [], False)]
        worker = Worker(model, job)
        worker.set_tags()
        keys = [c[0] for c in model.calls]
        self.assertIn("f.txt", keys)
        self.assertIn("dir/a.txt", keys)
        self.assertIn("dir/b.txt", keys)
        self.assertNotIn("dir/", keys)       # placeholder skipped
        self.assertTrue(all(c[1] == {"env": "prod"} for c in model.calls))

    def test_remove_and_replace_are_forwarded(self):
        model = self.FakeModel()
        Worker(model, [("f.txt", False, {}, ["old"], True)]).set_tags()
        self.assertEqual(model.calls, [("f.txt", {}, ["old"], True)])

    def test_cancel_stops_early(self):
        model = self.FakeModel()
        worker = Worker(model, [("f.txt", False, {"a": "b"}, [], False)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.set_tags()
        self.assertEqual(model.calls, [])
        self.assertEqual(finished, [True])


class BulkTagsDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self):
        dlg = main_window.BulkTagsDialog(None, 3)
        self.addCleanup(dlg.close)
        return dlg

    def test_empty_dialog_is_a_noop(self):
        self.assertTrue(self._dlg().is_noop())

    def test_collects_tags_and_removals(self):
        dlg = self._dlg()
        dlg._table.item(0, 0).setText("env")
        dlg._table.item(0, 1).setText("prod")
        dlg._remove_keys.setText("old, stale")
        self.assertEqual(dlg.tags_to_add(), {"env": "prod"})
        self.assertEqual(dlg.tags_to_remove(), ["old", "stale"])
        self.assertFalse(dlg.is_noop())

    def test_blank_keys_are_dropped(self):
        dlg = self._dlg()
        dlg._table.item(0, 1).setText("value with no key")
        self.assertEqual(dlg.tags_to_add(), {})

    def test_replace_alone_is_meaningful(self):
        dlg = self._dlg()
        dlg._replace.setChecked(True)
        self.assertTrue(dlg.replace_all())
        self.assertFalse(dlg.is_noop())   # replace with no tags clears them


class PreviewRenderTests(unittest.TestCase):
    """PreviewDialog picks a page from the fetched bytes; drive _on_loaded
    directly so no network or worker thread is involved."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        bucket = "b"

        def clone_for_worker(self):
            return self

        def get_object_preview(self, key, max_bytes):
            return {"data": b"", "content_type": "", "size": 0,
                    "truncated": False}

    def _dlg(self, key):
        dlg = main_window.PreviewDialog(None, self.FakeModel(), key)
        self.addCleanup(dlg.close)
        deadline = time.monotonic() + 5
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        return dlg

    def test_binary_falls_back_to_a_hex_dump(self):
        dlg = self._dlg("a.bin")
        dlg._on_loaded({"data": b"\x00\x01ABC", "content_type": "",
                        "size": 5, "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 3)   # hex page
        self.assertIn("|..ABC|", dlg._hex.toPlainText())

    def test_code_gets_a_highlighter(self):
        dlg = self._dlg("script.py")
        dlg._on_loaded({"data": b"def f():\n    return 1\n",
                        "content_type": "text/plain", "size": 20,
                        "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 2)   # text page
        self.assertIsNotNone(dlg._highlighter)

    def test_plain_text_has_no_highlighter(self):
        dlg = self._dlg("notes.txt")
        dlg._on_loaded({"data": b"hello", "content_type": "text/plain",
                        "size": 5, "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 2)
        self.assertIsNone(dlg._highlighter)

    def test_truncated_pdf_explains_itself(self):
        dlg = self._dlg("doc.pdf")
        dlg._on_loaded({"data": b"%PDF-1.4 truncated", "content_type":
                        "application/pdf", "size": 10 ** 9, "truncated": True},
                       None)
        self.assertEqual(dlg._stack.currentIndex(), 0)   # status page
        self.assertIn("larger than the preview limit", dlg._status.text())

    def test_real_pdf_renders_when_qtpdf_is_available(self):
        if main_window.QPdfView is None:
            self.skipTest("QtPdf not available in this build")
        pdf = (b"%PDF-1.1\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
               b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
               b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 99 99]>>endobj\n"
               b"trailer<</Root 1 0 R>>\n")
        dlg = self._dlg("doc.pdf")
        dlg._on_loaded({"data": pdf, "content_type": "application/pdf",
                        "size": len(pdf), "truncated": False}, None)
        # either it rendered (page 4) or it reported why (page 0)
        self.assertIn(dlg._stack.currentIndex(), (0, 4))


class ClipboardTests(unittest.TestCase):
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
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = "from/"

    def _select(self, items):
        return patch.object(main_window.MainWindow, "_collect_selected_targets",
                            lambda _s: items)

    def test_copy_stores_the_selection_and_the_uris(self):
        items = [("a.txt", "from/a.txt", False)]
        with self._select(items):
            self.win.copy_to_clipboard()
        self.assertEqual(self.win._clipboard["mode"], "copy")
        self.assertEqual(self.win._clipboard["bucket"], "bkt")
        self.assertEqual(
            QApplication.clipboard().text(), "s3://bkt/from/a.txt")

    def test_cut_is_blocked_on_a_read_only_profile(self):
        self.win.data_model.read_only = True
        with self._select([("a.txt", "from/a.txt", False)]):
            self.win.copy_to_clipboard(cut=True)
        self.assertIsNone(self.win._clipboard)

    def test_paste_copies_into_the_current_folder(self):
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][0], "copy")
        self.assertEqual(started[0][1], [("from/a.txt", "to/a.txt", False, None)])
        # a copy stays on the clipboard for another paste
        self.assertIsNotNone(self.win._clipboard)

    def test_paste_after_cut_moves_and_clears_the_clipboard(self):
        started = []
        self.win._clipboard = {"mode": "cut", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][0], "move")
        self.assertIsNone(self.win._clipboard)

    def test_paste_across_buckets_sets_the_destination_bucket(self):
        """REGRESSION (critical): the copy/move workers resolve CopySource —
        and move's delete pass — against their model's bucket. A cross-bucket
        paste must therefore hand the SOURCE bucket to the transfer, or the
        copy reads from the destination bucket and a cut deletes an unrelated
        destination object that happens to share the key."""
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "other",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = ""
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j, kw))):
            self.win.paste_from_clipboard()
        method, job, kwargs = started[0]
        self.assertEqual(job[0][3], "bkt")                     # destination
        self.assertEqual(kwargs.get("source_bucket"), "other")  # source

    def test_same_bucket_paste_has_no_source_override(self):
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j, kw))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][2].get("source_bucket", ""), "")

    def test_worker_model_is_bound_to_the_source_bucket(self):
        """The entry's source_bucket must rebind the worker's model clone —
        bucket, navigation AND endpoint, since the clone inherits the
        destination bucket's (possibly virtual-host) endpoint binding."""
        self.win.data_model.current_folder = "deep/"
        entry = main_window._QEntry(
            1, "copy", [("from/a.txt", "a.txt", False, "bkt")],
            label="Copy", source_bucket="other")
        model = self.win._worker_model_for(entry)
        self.assertEqual(model.bucket, "other")
        self.assertEqual(model.current_folder, "")
        self.assertEqual(model.endpoint_url, model.profile_endpoint_url)
        self.assertIsNone(model._client)

        plain = main_window._QEntry(2, "copy", [("a", "b", False, None)],
                                    label="Copy")
        self.assertEqual(self.win._worker_model_for(plain).bucket, "bkt")

    def test_retry_keeps_the_source_bucket(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation(
                "copy", [("from/a.txt", "a.txt", False, "bkt")],
                source_bucket="other")
            started[0].status = "error"
            self.win._on_queue_retry_requested(started[0].entry_id)
        self.assertEqual(len(started), 2)
        self.assertEqual(started[1].source_bucket, "other")

    def test_profile_switch_clears_the_clipboard(self):
        """A clipboard from another profile points at another account's
        objects; pasting it through new credentials must be impossible."""
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        prof = types.SimpleNamespace(
            name="p2", url="https://s3.amazonaws.com", region="us-east-1",
            access_key="AK2", secret_key="SK2", no_ssl_check=False,
            use_path=False, session_token="", read_only=False)
        self.win.apply_profile(prof)
        deadline = time.monotonic() + 5
        while not self.win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        self.assertIsNone(self.win._clipboard)

    def test_paste_is_blocked_when_read_only(self):
        started = []
        self.win.data_model.read_only = True
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started, [])

    def test_bookmark_is_added_and_survives_a_restart(self):
        self.win.data_model.current_folder = "logs/2026/"
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("prod logs", True)):
            self.win.add_bookmark()
        self.assertEqual(len(self.win._bookmarks), 1)
        self.assertEqual(self.win._bookmarks[0]["prefix"], "logs/2026/")
        self.win.close()

        with patch.object(main_window, "DataModel", _StubModel):
            again = main_window.MainWindow(settings=(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                self.settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False, "", False,
            ))
        self.addCleanup(again.close)
        self.assertEqual(
            [b["name"] for b in again._bookmarks], ["prod logs"])

    def test_duplicate_location_is_not_bookmarked_twice(self):
        self.win.data_model.current_folder = "logs/"
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("one", True)):
            self.win.add_bookmark()
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("two", True)):
            self.win.add_bookmark()
        self.assertEqual(len(self.win._bookmarks), 1)

    def test_cancelling_the_name_prompt_adds_nothing(self):
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("", False)):
            self.win.add_bookmark()
        self.assertEqual(self.win._bookmarks, [])

    def test_bookmarks_need_an_open_bucket(self):
        self.win.data_model.bucket = ""
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("x", True)):
            self.win.add_bookmark()
        self.assertEqual(self.win._bookmarks, [])

    def test_same_bucket_bookmark_navigates_without_reopening(self):
        entered = []
        navigated = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                              entered.append((name, target_prefix))), \
             patch.object(main_window.MainWindow, "navigate",
                          lambda _s, **kw: navigated.append(kw)):
            self.win.go_to_bookmark(
                {"bucket": "bkt", "prefix": "logs/"})
        self.assertEqual(entered, [])
        self.assertEqual(self.win.data_model.current_folder, "logs/")
        self.assertEqual(len(navigated), 1)

    def test_other_bucket_bookmark_reopens_that_bucket(self):
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                              entered.append((name, target_prefix))):
            self.win.go_to_bookmark({"bucket": "other", "prefix": "deep/"})
        self.assertEqual(entered, [("other", "deep/")])

    def test_bookmark_menu_lists_saved_locations(self):
        self.win._bookmarks = [
            {"name": "one", "bucket": "bkt", "prefix": "a/"},
            {"name": "two", "bucket": "bkt2", "prefix": ""},
        ]
        self.win._rebuild_bookmark_menu()
        labels = [a.text() for a in self.win.bookmarkButton.menu().actions()
                  if not a.isSeparator()]
        self.assertIn("one", labels)
        self.assertIn("two", labels)
        self.assertTrue(any("Bookmark this location" in l for l in labels))

    def test_manage_dialog_edits_are_kept(self):
        self.win._bookmarks = [
            {"name": "one", "bucket": "bkt", "prefix": "a/"},
            {"name": "two", "bucket": "bkt2", "prefix": ""},
        ]
        dlg = main_window.BookmarksDialog(None, self.win._bookmarks)
        self.addCleanup(dlg.close)
        dlg._table.item(0, 0).setText("renamed")
        dlg._table.setCurrentCell(1, 0)
        dlg._remove()
        result = dlg.bookmarks()
        self.assertEqual([b["name"] for b in result], ["renamed"])
        self.assertEqual(result[0]["bucket"], "bkt")

    def test_paste_with_empty_clipboard_does_nothing(self):
        started = []
        self.win._clipboard = None
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started, [])


class _FakeDuplicateModel:
    bucket = "bkt"

    def __init__(self, rows=None, fail=False):
        self._rows = rows or []
        self._fail = fail
        self.prefixes = []
        self.cancel_events = []

    def clone_for_worker(self):
        return self

    def list_object_digests(self, prefix="", cancel_event=None, log_fn=None):
        self.prefixes.append(prefix)
        self.cancel_events.append(cancel_event)
        if self._fail:
            raise RuntimeError("denied")
        return list(self._rows)


class DuplicateFinderDialogTests(unittest.TestCase):
    MD5_A = "a" * 32
    MD5_B = "b" * 32

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, rows=None, prefix="pre/", fail=False, read_only=False):
        mw = _FakeMainWindow()
        mw.is_read_only = lambda: read_only
        mw.deleted = []
        mw.delete_duplicate_keys = lambda keys: mw.deleted.append(list(keys))
        model = _FakeDuplicateModel(rows, fail=fail)
        dlg = main_window.DuplicateFinderDialog(None, mw, model, prefix)
        self.addCleanup(dlg.close)
        return dlg, mw, model

    def _rows(self):
        return [
            ("pre/old.bin", 2 * 1024 * 1024, self.MD5_A, _dt(1)),
            ("pre/new.bin", 2 * 1024 * 1024, self.MD5_A, _dt(3)),
            ("pre/unique.bin", 5 * 1024 * 1024, self.MD5_B, _dt(2)),
        ]

    def test_scan_groups_duplicates_and_reports_reclaimable_space(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_size.setText("1")
        dlg._min_unit.setCurrentIndex(0)      # bytes
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(len(dlg._groups), 1)
        self.assertEqual(dlg._tree.topLevelItemCount(), 1)
        self.assertEqual(dlg._tree.topLevelItem(0).childCount(), 2)
        self.assertIn("2.0 MB", dlg._info.text())

    def test_nothing_is_preselected(self):
        """Deletion is irreversible; the user must choose explicitly."""
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(dlg.selected_keys(), [])
        self.assertFalse(dlg._btn_delete.isEnabled())

    def test_keep_newest_and_oldest_pick_opposite_survivors(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        self.assertEqual(dlg.selected_keys(), ["pre/old.bin"])
        dlg._auto_select("oldest")
        self.assertEqual(dlg.selected_keys(), ["pre/new.bin"])
        dlg._clear_selection()
        self.assertEqual(dlg.selected_keys(), [])

    def test_min_size_filter_is_applied(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_size.setText("3")
        dlg._min_unit.setCurrentIndex(2)      # MB — above the duplicate pair
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(dlg._groups, [])
        self.assertIn("No duplicates", dlg._info.text())

    def test_scope_checkbox_widens_the_scan_to_the_bucket(self):
        dlg, _mw, model = self._dlg(self._rows(), prefix="pre/")
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(model.prefixes[-1], "pre/")
        dlg._scope.setChecked(True)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(model.prefixes[-1], "")

    def test_scan_passes_a_cancel_event(self):
        dlg, _mw, model = self._dlg(self._rows())
        dlg._scan()
        self._settle(dlg)
        self.assertIsNotNone(model.cancel_events[0])

    def test_scan_failure_is_reported(self):
        dlg, _mw, _model = self._dlg(fail=True)
        dlg._scan()
        self._settle(dlg)
        self.assertIn("Scan failed", dlg._info.text())
        self.assertTrue(dlg._btn_scan.isEnabled())

    def test_delete_hands_the_selection_to_the_window(self):
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            dlg._delete_selected()
        self.assertEqual(mw.deleted, [["pre/old.bin"]])

    def test_declining_the_confirmation_deletes_nothing(self):
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            dlg._delete_selected()
        self.assertEqual(mw.deleted, [])

    def test_warns_when_a_whole_group_is_selected(self):
        """Checking every copy destroys the content instead of de-duplicating."""
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        for item in dlg._iter_key_items():
            item.setCheckState(0, Qt.CheckState.Checked)
        self.assertTrue(dlg._selection_would_empty_a_group())
        seen = {}
        with patch.object(main_window.QMessageBox, "question",
                          side_effect=lambda *a, **k: seen.update(text=a[2])
                          or QMessageBox.StandardButton.No):
            dlg._delete_selected()
        self.assertIn("WARNING", seen["text"])

    def test_read_only_profile_cannot_delete(self):
        dlg, _mw, _model = self._dlg(self._rows(), read_only=True)
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        self.assertFalse(dlg._btn_delete.isEnabled())

    def test_unconfirmed_group_is_labelled_and_not_auto_selected(self):
        rows = [
            ("pre/a.bin", 4096, self.MD5_A, _dt(1)),
            ("pre/b.bin", 4096, self.MD5_B + "-2", _dt(2)),
        ]
        dlg, _mw, _model = self._dlg(rows)
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(len(dlg._groups), 1)
        self.assertFalse(dlg._groups[0]["confirmed"])
        self.assertIn("cannot confirm", dlg._tree.topLevelItem(0).text(0))
        dlg._auto_select("newest")
        self.assertEqual(dlg.selected_keys(), [])

    def test_min_size_parsing_is_forgiving(self):
        dlg, _mw, _model = self._dlg()
        dlg._min_unit.setCurrentIndex(0)
        dlg._min_size.setText("not a number")
        self.assertEqual(dlg.min_size_bytes(), 1)
        dlg._min_size.setText("0")
        self.assertEqual(dlg.min_size_bytes(), 1)   # zero-byte objects excluded
        dlg._min_size.setText("2")
        dlg._min_unit.setCurrentIndex(1)            # KB
        self.assertEqual(dlg.min_size_bytes(), 2048)


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


class ReviewRegressionTests(unittest.TestCase):
    """Findings from the 0.16.0 review, each reproduced before being fixed."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @staticmethod
    def _region_error():
        return botocore.exceptions.ClientError(
            {"Error": {"Code": "PermanentRedirect",
                       "Message": "The bucket is in another region"}},
            "UploadPart")

    def _big_file(self, part=1024, parts=3):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        payload = os.urandom(part * parts)
        path = os.path.join(directory, "big.bin")
        with open(path, "wb") as handle:
            handle.write(payload)
        return path, payload, directory

    def _model(self, client, directory, part=1024):
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        model.upload_chunk_size = part
        model.multipart_threshold_mb = 0
        model.transfer_concurrency = 1
        model.upload_state_dir = os.path.join(directory, "state")
        model._client = client
        return model

    def test_a_resumable_upload_retries_after_a_region_error(self):
        """FINDING: the managed upload rebinds and retries once on a
        region/endpoint mismatch; the multipart path did not, so the SAME
        bucket failed for a large file and succeeded for a small one."""
        path, payload, directory = self._big_file()
        client = _MultipartClient()
        model = self._model(client, directory)

        real_upload_part = client.upload_part
        state = {"raised": False}

        def _once(**kw):
            if not state["raised"]:
                state["raised"] = True
                raise self._region_error()
            return real_upload_part(**kw)

        client.upload_part = _once
        rebinds = []

        def _rebind(inner, log_fn=None):
            # A real rebind MOVES the endpoint, which is what made the resume
            # record unfindable on the retry.
            inner.endpoint_url = "https://s3.eu-central-1.amazonaws.com"
            rebinds.append(1)

        with patch.object(Model, "rebind_bucket", _rebind):
            model.upload_file(path, "big.bin")
        self.assertEqual(len(rebinds), 1, "did not rebind and retry")
        self.assertEqual(client.completed["big.bin"], payload)
        self.assertEqual(client.created, 1,
                         "retry started a second multipart upload, orphaning "
                         "the first one's parts")

    def test_a_non_region_error_is_not_retried(self):
        """Retrying a genuine failure would double every upload."""
        path, _payload, directory = self._big_file()
        client = _MultipartClient(fail_on_part=1)
        model = self._model(client, directory)
        rebinds = []
        with patch.object(Model, "rebind_bucket",
                          lambda _s, log_fn=None: rebinds.append(1)):
            with self.assertRaises(Exception):
                model.upload_file(path, "big.bin")
        self.assertEqual(rebinds, [])

    def test_part_listing_cannot_loop_forever(self):
        """FINDING: a truncated response whose marker does not advance spun
        the transfer thread forever. S3-compatible backends differ in what
        they return, and this app exists to talk to them."""
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        calls = []

        class _Stuck:
            def list_parts(self, **kw):
                calls.append(kw.get("PartNumberMarker"))
                if len(calls) > 5:
                    raise RuntimeError("would have looped forever")
                # Truncated, but the marker never moves on.
                return {"Parts": [{"PartNumber": 1, "ETag": '"e1"', "Size": 4}],
                        "IsTruncated": True}

        model._client = _Stuck()
        parts = model._uploaded_parts("k", "upload-1")
        self.assertEqual(sorted(parts), [1])
        self.assertLessEqual(len(calls), 5)

    def _sync_dialog(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        dlg = main_window.CrossProfileSyncDialog(
            None, None, model, "pre/", settings, current_profile="prof")
        self.addCleanup(dlg.close)
        return dlg

    def test_changing_the_direction_invalidates_the_plan(self):
        """FINDING (data loss): the plan stayed runnable after its inputs
        changed, so a plan reviewed as a push could be executed after
        switching to pull — deleting at the side believed to be the source."""
        dlg = self._sync_dialog()
        dlg._actions = [{"action": "upload", "rel": "a", "size": 1,
                         "reason": "missing"}]
        dlg._source_prefix, dlg._dest_prefix = "pre/", "dst/"
        dlg._dest_model = object()
        dlg._ok_enabled(True)

        dlg._direction.setCurrentIndex(1)

        self.assertEqual(dlg._actions, [])
        self.assertEqual(dlg.plan()[2], [])
        self.assertFalse(dlg._buttons.button(
            QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_editing_any_input_invalidates_the_plan(self):
        for change in ("prefix", "exclude", "delete_extra"):
            with self.subTest(change=change):
                dlg = self._sync_dialog()
                dlg._actions = [{"action": "upload", "rel": "a", "size": 1,
                                 "reason": "missing"}]
                dlg._ok_enabled(True)
                if change == "prefix":
                    dlg.picker.prefix.setText("other/")
                elif change == "exclude":
                    dlg._exclude.setText("*.tmp")
                else:
                    dlg._delete_extra.setChecked(True)
                self.assertEqual(dlg._actions, [], f"{change} kept the plan")
                self.assertFalse(dlg._buttons.button(
                    QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_accepting_a_picker_dialog_stops_its_loader(self):
        """FINDING (crash): the bucket fetch was only stopped in closeEvent,
        which accept() never fires — so a dialog dismissed while the listing
        was in flight was destroyed with a live QThread, which aborts the
        process in PyQt6."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        for factory in (
            lambda: main_window.CrossProfileCopyDialog(
                None, settings, 1, current_profile="prof"),
            lambda: main_window.CrossProfileSyncDialog(
                None, None,
                Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False),
                "pre/", settings, current_profile="prof"),
        ):
            stopped = []
            with patch.object(main_window.ProfilePicker, "stop_loader",
                              lambda _s: stopped.append(1)):
                dlg = factory()
                self.addCleanup(dlg.close)
                dlg.accept()
            self.assertTrue(stopped, "loader left running after accept()")

    def _window(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                self.ROOT, settings, "prof", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        return win

    def test_retrying_a_cross_profile_job_keeps_its_models(self):
        """FINDING: retry re-queued the job without the destination model, so
        the worker refused it — the retry button could never succeed."""
        win = self._window()
        dest = object()
        source = object()
        started = []
        entry = main_window._QEntry(
            entry_id=1, method="copy_to_profile",
            job=[("a", "b", False)], dest_model=dest, source_model=source)
        entry.status = "error"
        win._queue_entries[1] = entry
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(kw)):
            win._on_queue_retry_requested(1)
        self.assertEqual(len(started), 1)
        self.assertIs(started[0].get("dest_model"), dest)
        self.assertIs(started[0].get("source_model"), source)

    def test_history_refuses_to_rerun_a_cross_profile_job(self):
        """A stored job cannot carry another profile's live credentials, so
        queueing it would produce a job that can only fail."""
        win = self._window()
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(method)), \
             patch.object(main_window, "QMessageBox") as box:
            win.rerun_history_entry(
                {"method": "sync_to_profile", "job": [["copy", "a", "b"]]})
        self.assertEqual(started, [])
        self.assertTrue(box.information.called or box.warning.called)

    def test_history_still_reruns_ordinary_jobs(self):
        win = self._window()
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(method)):
            win.rerun_history_entry(
                {"method": "upload", "job": [["k", "/tmp/f"]]})
        self.assertEqual(started, ["upload"])

    def test_switching_profiles_saves_where_the_old_one_was(self):
        """FINDING: the location is stored per profile, but a runtime switch
        reassigned profile_name first, so the profile being left behind never
        recorded where it was."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = main_window.MainWindow(settings=(
                self.ROOT, settings, "prod", "https://s3.amazonaws.com",
                "us-east-1", "", "AK", "SK", False, False,
            ))
        self.addCleanup(win.close)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/"

        profile = profile_switcher.Profile(
            name="dev", url="https://minio.local", region="us-east-1",
            bucket="", access_key="AK", secret_key="SK",
            no_ssl_check=False, use_path=True)
        win.apply_profile(profile)

        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/2026/")
        settings.endGroup()
        self.assertEqual(win.profile_name, "dev")
