"""s3/listing.py and the search that reads through it."""

import botocore.exceptions
import main_window
import os
import tempfile
import unittest
import unittest.mock
from model import Model
from s3.listing import expecting_region
from tests.support import FakeS3Client, _dt, _ensure_qapp, make_model
from utils import scan_local_tree


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

    def test_a_folder_target_is_a_one_key_probe_not_a_full_listing(self):
        """A top-level folder has no parent, so its "parent prefix" was the
        bucket root: deciding whether "photos/" existed listed EVERY object
        in the bucket, behind a modal dialog."""
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "photos/a.jpg", "Size": 1},
            {"Key": "elsewhere/b.jpg", "Size": 1},
        ]}])
        self.m._client = c
        self.assertEqual(self.m.existing_keys(["photos/"]), {"photos/"})
        probes = c.calls_of("list_objects_v2")
        self.assertEqual([p["Prefix"] for p in probes], ["photos/"])
        self.assertEqual([p["MaxKeys"] for p in probes], [1])
        self.assertEqual(
            [kw["name"] for kw in c.calls_of("get_paginator")], [],
            "a folder target must not trigger a recursive listing")

    def test_a_folder_holding_only_placeholders_still_counts_as_existing(self):
        """The old scan only looked at non-placeholder keys, so a folder
        whose contents were all sub-folder markers was reported free and the
        overwrite prompt never appeared."""
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "dst/sub/", "Size": 0},
        ]}])
        self.m._client = c
        self.assertEqual(self.m.existing_keys(["dst/"]), {"dst/"})

    def test_an_absent_folder_is_not_reported(self):
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "other/a.txt", "Size": 1},
        ]}])
        self.m._client = c
        self.assertEqual(self.m.existing_keys(["missing/"]), set())

    def test_files_and_folders_in_one_call(self):
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "dst/a.txt", "Size": 1},
            {"Key": "dst/sub/deep.txt", "Size": 1},
        ]}])
        self.m._client = c
        self.assertEqual(
            self.m.existing_keys(["dst/a.txt", "dst/gone.txt", "dst/sub/"]),
            {"dst/a.txt", "dst/sub/"})
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

    def test_a_complete_scan_refuses_a_missing_root(self):
        """Sync reads "no local files" as "every remote object is extra".
        An unmounted or renamed source folder must stop the comparison, not
        produce a plan that deletes the backup it was meant to feed."""
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(OSError):
                scan_local_tree(os.path.join(tmp, "missing"),
                                require_complete=True)

    def test_a_complete_scan_propagates_a_directory_error(self):
        def failed_walk(_root, *, onerror):
            onerror(PermissionError("unreadable subtree"))
            return iter(())

        with tempfile.TemporaryDirectory() as tmp, \
                unittest.mock.patch("utils.os.walk", failed_walk):
            with self.assertRaises(PermissionError):
                scan_local_tree(tmp, require_complete=True)

    def test_a_complete_scan_propagates_a_file_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "file")
            with open(path, "w") as handle:
                handle.write("x")
            real_stat = os.stat

            def stat(name, *args, **kwargs):
                if name == path:
                    raise PermissionError("unreadable file")
                return real_stat(name, *args, **kwargs)

            with unittest.mock.patch("utils.os.stat", stat):
                with self.assertRaises(PermissionError):
                    scan_local_tree(tmp, require_complete=True)

    def test_an_ordinary_scan_still_skips_what_it_cannot_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "file")
            with open(path, "w") as handle:
                handle.write("x")
            real_stat = os.stat

            def stat(name, *args, **kwargs):
                if name == path:
                    raise PermissionError("unreadable file")
                return real_stat(name, *args, **kwargs)

            with unittest.mock.patch("utils.os.stat", stat):
                self.assertEqual(scan_local_tree(tmp), {})
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
class IncrementalListingTests(unittest.TestCase):
    """A prefix with a hundred thousand direct children used to be a silent
    minute and then one enormous model."""

    def _pages(self, count, per_page=100):
        pages = []
        made = 0
        while made < count:
            size = min(per_page, count - made)
            pages.append({"Contents": [
                {"Key": f"p/{made + i}.txt", "Size": 1,
                 "LastModified": _dt(1), "ETag": '"x"'}
                for i in range(size)]})
            made += size
        return pages

    def test_every_page_reports_its_running_count(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(300))
        seen = []
        items = m.list("p/", page_cb=seen.append)
        self.assertEqual(len(items), 300)
        self.assertEqual(seen, [100, 200, 300])
        self.assertFalse(m.last_listing_truncated)

    def test_the_cap_stops_the_walk_and_says_so(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(500))
        items = m.list("p/", max_items=250)
        self.assertEqual(len(items), 250)
        self.assertTrue(m.last_listing_truncated)

    def test_a_listing_of_exactly_the_cap_is_not_truncated(self):
        """Stopping at >= the cap reported a complete prefix as truncated,
        which told the user to raise a limit that was hiding nothing."""
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(300))
        items = m.list("p/", max_items=300)
        self.assertEqual(len(items), 300)
        self.assertFalse(m.last_listing_truncated)

    def test_one_past_the_cap_is_still_truncated(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(301))
        items = m.list("p/", max_items=300)
        self.assertEqual(len(items), 300)
        self.assertTrue(m.last_listing_truncated)

    def test_no_cap_means_no_cap(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(500))
        self.assertEqual(len(m.list("p/", max_items=0)), 500)
        self.assertFalse(m.last_listing_truncated)

    def test_the_truncated_flag_is_reset_per_listing(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=self._pages(500))
        m.list("p/", max_items=10)
        self.assertTrue(m.last_listing_truncated)
        m._client = FakeS3Client(list_pages=self._pages(5))
        m.list("p/", max_items=10)
        self.assertFalse(m.last_listing_truncated)
class _LazyPaginator:
    """A paginator that behaves the way botocore's really does.

    ``paginate()`` sends nothing — it hands back a lazy iterator — so a
    failure, region hint and all, only surfaces on the first iteration. The
    FakeS3Client paginator returns a plain list, which is eager, and that is
    precisely why the bug this guards against lived through a green suite.
    """

    def __init__(self, pages=None, error=None):
        self._pages = list(pages or [])
        self._error = error
        self.paginate_calls = 0

    def paginate(self, **_kwargs):
        self.paginate_calls += 1

        def _walk():
            if self._error is not None:
                raise self._error
            for page in self._pages:
                yield page

        return _walk()


class _LazyClient:
    def __init__(self, paginator):
        self._paginator = paginator

    def get_paginator(self, _name):
        return self._paginator


def _wrong_region_error(expecting="eu-north-1"):
    return botocore.exceptions.ClientError(
        {"Error": {
            "Code": "AuthorizationHeaderMalformed",
            "Message": ("The authorization header is malformed; the region "
                        f"'us-east-1' is wrong; expecting '{expecting}'"),
        }},
        "ListObjectsV2")


class RegionHintListingTests(unittest.TestCase):
    """REGRESSION: a listing refused for the wrong signing region used to
    fail outright.

    The hint was read in the handler around ``paginator.paginate()``, which
    performs no I/O at all — so the mismatch arrived one line further down,
    where it was classified as a hard failure, and the retry in the region
    the server had just named never ran. "Automatic region detection" was
    therefore dead for every listing.
    """

    def test_the_hint_is_read_and_the_region_is_adopted(self):
        m = make_model(bucket="b", region_name="us-east-1")
        m._client = _LazyClient(_LazyPaginator(error=_wrong_region_error()))
        good = _LazyClient(_LazyPaginator(pages=[{"Contents": [
            {"Key": "p/a.txt", "Size": 1, "LastModified": _dt(1),
             "ETag": '"x"'}]}]))
        with unittest.mock.patch.object(
                Model, "_make_client", lambda _s, **kw: good):
            items = m.list("p/")
        self.assertEqual([i.name for i in items], ["a.txt"])
        # Promoted, so the next operation on this bucket starts there.
        self.assertEqual(m.region_name, "eu-north-1")
        self.assertIs(m._client, good)

    def test_a_refusal_in_the_hinted_region_too_is_raised(self):
        m = make_model(bucket="b", region_name="us-east-1")
        m._client = _LazyClient(_LazyPaginator(error=_wrong_region_error()))
        denied = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "ListObjectsV2")
        second = _LazyClient(_LazyPaginator(error=denied))
        with unittest.mock.patch.object(
                Model, "_make_client", lambda _s, **kw: second):
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                m.list("p/")
        self.assertEqual(
            caught.exception.response["Error"]["Code"], "AccessDenied")

    def test_an_ordinary_failure_is_still_fatal(self):
        """Only a named region is a retry; everything else keeps its own
        message rather than becoming "cannot list with available regions"."""
        m = make_model(bucket="b")
        denied = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "ListObjectsV2")
        m._client = _LazyClient(_LazyPaginator(error=denied))
        with self.assertRaises(botocore.exceptions.ClientError):
            m.list("p/")

    def test_expecting_region_reads_only_what_it_should(self):
        self.assertEqual(expecting_region(_wrong_region_error("ap-south-1")),
                         "ap-south-1")
        self.assertEqual(expecting_region(
            botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "expecting 'x'"}},
                "ListObjectsV2")), "")
        self.assertEqual(expecting_region(RuntimeError("expecting 'x'")), "")


class ListingFilterTests(unittest.TestCase):
    """The quick-find bar: a substring by default, a glob when it looks like
    one, and a count so an over-narrow filter is not mistaken for an empty
    folder."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _proxy(self):
        return main_window.UpTopProxyModel(main_window.UP_ENTRY_LABEL)

    def test_no_filter_matches_everything(self):
        proxy = self._proxy()
        self.assertTrue(proxy.matches("anything.txt"))

    def test_substring_is_case_insensitive(self):
        proxy = self._proxy()
        proxy.set_filter_text("LOG")
        self.assertTrue(proxy.matches("access.log"))
        self.assertTrue(proxy.matches("Logfile"))
        self.assertFalse(proxy.matches("data.csv"))

    def test_a_glob_pattern_is_anchored(self):
        proxy = self._proxy()
        proxy.set_filter_text("*.log")
        self.assertTrue(proxy.matches("access.log"))
        self.assertFalse(proxy.matches("access.log.gz"))
        self.assertFalse(proxy.matches("logbook"))

    def test_question_marks_and_classes_work(self):
        proxy = self._proxy()
        proxy.set_filter_text("2026-0?-*")
        self.assertTrue(proxy.matches("2026-01-report"))
        self.assertFalse(proxy.matches("2025-01-report"))
        proxy.set_filter_text("[ab]*.txt")
        self.assertTrue(proxy.matches("a1.txt"))
        self.assertFalse(proxy.matches("c1.txt"))

    def test_a_bracket_alone_stays_a_substring(self):
        """A bare "[" is far more often part of a name than the start of a
        character class; treating it as one would anchor the pattern."""
        proxy = self._proxy()
        proxy.set_filter_text("[draft")
        self.assertTrue(proxy.matches("report [draft].txt"))
