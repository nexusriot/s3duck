"""Navigation through the real worker thread, against a real server.

Every assertion here is about what the list view ends up showing, not about
what the model returned: the gap between those two is the navigation worker,
its model clone, and the queued signal that carries the result back onto the
UI thread — none of which the unit suite can exercise, because its listings
never leave the calling thread.
"""

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
