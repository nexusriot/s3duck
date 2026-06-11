"""
Unit tests for pure-logic helpers (no network / no QApplication needed).

Run with:  python -m unittest discover -s tests   (from the project root)
or:        .venv/bin/python -m unittest discover -s tests

Several tests are explicit regression guards for previously fixed bugs and
are marked with "REGRESSION:" in their docstrings.
"""

import os
import sys
import unittest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import str_to_bool
from main_window import _to_epoch, categorize_key, _human_bytes
from model import Model


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


if __name__ == "__main__":
    unittest.main()
