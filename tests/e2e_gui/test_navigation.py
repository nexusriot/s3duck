"""Navigation through the real worker thread, against a real server.

Every assertion here is about what the list view ends up showing, not about
what the model returned: the gap between those two is the navigation worker,
its model clone, and the queued signal that carries the result back onto the
UI thread. The unit suite also exercises these threads with in-memory
listings; this suite checks the requests and results against a real server.
"""

from contextlib import contextmanager
from unittest.mock import patch

from botocore.client import BaseClient
from PyQt6.QtCore import Qt
from PyQt6.QtTest import QTest

from tests.e2e_gui.harness import GuiServerCase


class ListingTests(GuiServerCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.s3.put_object(Bucket=cls.bucket, Key="top.txt", Body=b"top")
        cls.s3.put_object(Bucket=cls.bucket, Key="docs/a.txt", Body=b"a")
        cls.s3.put_object(Bucket=cls.bucket, Key="docs/b.txt", Body=b"bb")
        cls.s3.put_object(Bucket=cls.bucket, Key="docs/deep/c.txt", Body=b"ccc")
        cls.s3.put_object(Bucket=cls.bucket, Key="docs-old/x.txt", Body=b"x")

    def test_the_bucket_root_shows_its_folders_and_files(self):
        self.navigate("")
        self.assertEqual(self.listing(), ["[..]", "docs", "docs-old", "top.txt"])

    def test_a_folder_shows_only_its_own_children(self):
        self.navigate("docs/")
        self.assertEqual(self.listing(), ["[..]", "a.txt", "b.txt", "deep"])

    def test_a_sibling_prefix_is_not_swept_in(self):
        # 'docs' must not match 'docs-old/'. The delimiter is what separates
        # them, and getting it wrong shows a bucket's worth of unrelated keys.
        self.navigate("docs/")
        self.assertNotIn("x.txt", self.listing())

    def test_going_deeper_and_back_lands_where_it_started(self):
        self.navigate("docs/deep/")
        self.assertEqual(self.listing(), ["[..]", "c.txt"])
        self.navigate("docs/")
        self.assertIn("a.txt", self.listing())

    def test_the_listing_carries_the_size_the_server_reported(self):
        # The row text is the user's only evidence that the listing is real
        # rather than a cached shape, so it has to come back through the
        # worker with the entry.
        self.navigate("docs/")
        model = self.window.model
        rows = {model.item(i).text(): i for i in range(model.rowCount())}
        self.assertIn("b.txt", rows)
        row = rows["b.txt"]
        size_cell = model.item(row, 1)
        self.assertIsNotNone(size_cell, "the listing has no size column")
        self.assertTrue(size_cell.text().strip(),
                        "the size column came back empty")

    def test_an_object_added_behind_the_app_appears_on_refresh(self):
        # A refresh has to re-ask the server rather than redraw what it has;
        # this is the difference between a listing and a cache.
        self.navigate("docs/")
        self.assertNotIn("late.txt", self.listing())
        self.put("docs/late.txt", b"late")
        self.navigate("docs/")
        self.assertIn("late.txt", self.listing())

    def test_an_object_deleted_behind_the_app_disappears_on_refresh(self):
        self.put("docs/doomed.txt", b"x")
        self.navigate("docs/")
        self.assertIn("doomed.txt", self.listing())
        self.s3.delete_object(Bucket=self.bucket, Key="docs/doomed.txt")
        self.navigate("docs/")
        self.assertNotIn("doomed.txt", self.listing())


class EmptyPrefixTests(GuiServerCase):
    def test_an_empty_folder_lists_as_just_the_up_entry(self):
        # A prefix with nothing under it must still produce a usable view —
        # the way out has to be there, or the user is stranded.
        self.navigate("nothing-here/")
        self.assertEqual(self.listing(), ["[..]"])
        self.assertTrue(self.window.listview.isEnabled(),
                        "the list view was left disabled after an empty listing")


class NavigationCacheTests(GuiServerCase):
    """Observe real SDK calls without replacing their server responses."""

    def setUp(self):
        super().setUp()
        self.prefix = self._testMethodName + "/"
        self.child = self.prefix + "child/"
        self.put(self.child + "first.txt", b"first")
        self.put(self.child + "second.txt", b"second")
        self.navigate(self.prefix)

    @contextmanager
    def recording_api_calls(self):
        calls = []
        original = BaseClient._make_api_call

        def record(client, operation, params):
            calls.append(operation)
            return original(client, operation, params)

        with patch.object(BaseClient, "_make_api_call", record):
            yield calls

    def settle_navigation(self):
        self.pump(lambda: self.window.listing_settled()
                  and self.window.listview.isEnabled(),
                  what="the folder listing to settle")

    def test_cached_parent_and_child_navigation_sends_no_requests(self):
        with self.recording_api_calls() as calls:
            self.window.list_doubleClicked(self.window.ix_by_name("child"))
            self.settle_navigation()
            self.assertIn("ListObjectsV2", calls)
            calls.clear()
            self.window.goUp()
            self.settle_navigation()
            self.assertIn("child", self.listing())
            self.window.list_doubleClicked(self.window.ix_by_name("child"))
            self.settle_navigation()
            self.assertIn("first.txt", self.listing())
            self.assertEqual(calls, [])

    def test_refresh_fetches_changes_made_by_another_client(self):
        self.navigate(self.child)
        self.put(self.child + "external.txt", b"new")
        with self.recording_api_calls() as calls:
            self.window.open_location(self.bucket, self.child)
            self.settle_navigation()
            self.assertNotIn("external.txt", self.listing())
            self.assertEqual(calls, [])
            self.window.btnRefresh.trigger()
            self.settle_navigation()
            self.assertIn("ListObjectsV2", calls)
            self.assertIn("external.txt", self.listing())

    def test_delete_without_visible_refresh_invalidates_the_cached_folder(self):
        self.navigate(self.child)
        entry = self.window.assign_thread_operation(
            "delete", [self.child + "first.txt"], need_refresh=False)
        self.await_transfers()
        self.assertEqual(entry.status, "done")
        self.assertFalse(self.exists(self.child + "first.txt"))
        with self.recording_api_calls() as calls:
            self.window.open_location(self.bucket, self.child)
            self.settle_navigation()
            self.assertIn("ListObjectsV2", calls)
            self.assertNotIn("first.txt", self.listing())
            self.assertIn("second.txt", self.listing())

    def test_open_usage_window_only_scans_when_explicitly_requested(self):
        self.navigate(self.child)
        self.window.listview.setCurrentIndex(self.window.ix_by_name("first.txt"))
        with self.recording_api_calls() as calls:
            self.window.btnBucketUsage.trigger()
            self.addCleanup(self.window._bucket_usage_dialog.hide)
            self.pump(lambda: self.window._bucket_usage_thread is None,
                      what="the explicit usage calculation to finish")
            self.assertIn("ListObjectsV2", calls)
            self.assertTrue(self.window._bucket_usage_dialog.isVisible())
            self.assertEqual(self.window.statusBar().currentMessage(), "Usage calculated")
            calls.clear()
            for key in (Qt.Key.Key_Down, Qt.Key.Key_Up, Qt.Key.Key_Down):
                QTest.keyClick(self.window.listview, key)
                self.app.processEvents()
            self.assertEqual(calls, [], "highlighting loaded rows sent S3 requests")
            self.window.btnBucketUsage.trigger()
            self.pump(lambda: self.window._bucket_usage_thread is None,
                      what="a second explicit usage calculation to finish")
            self.assertIn("ListObjectsV2", calls)


class NavigationFailureTests(GuiServerCase):
    """A navigation that fails must hand the UI back, not strand it.

    The list view is disabled while a navigation runs. If the error path
    forgets to re-enable it the application is bricked without crashing, which
    is the worst shape of this bug and the one a mocked test is least likely to
    catch.
    """

    def test_a_vanished_bucket_reports_and_re_enables_the_view(self):
        self.navigate("")
        # Point the window at a bucket that does not exist, behind its back.
        self.window.data_model.bucket = self.bucket + "-gone"
        try:
            self.navigate("")
            self.assertTrue(
                self.window.listview.isEnabled(),
                "a failed navigation left the list view disabled")
        finally:
            self.window.data_model.bucket = self.bucket
            self.window.data_model.enter_bucket(self.bucket)
        # And the window still works afterwards.
        self.navigate("")
        self.assertIn("[..]", self.listing())


class BucketEntryTests(GuiServerCase):
    """Opening a bucket is its own worker, separate from navigation."""

    def test_entering_a_bucket_shows_its_contents(self):
        self.put("entered.txt", b"x")
        self.window.enter_bucket_async(self.bucket)
        self.pump(
            lambda: (not self.window.bucket_enter_active()
                     and self.window.listview.isEnabled()),
            what="the bucket-entry worker to finish",
        )
        self.navigate("")
        self.assertIn("entered.txt", self.listing())
        self.assertEqual(self.window.data_model.bucket, self.bucket)
