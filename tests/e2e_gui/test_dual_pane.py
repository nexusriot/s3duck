"""The dual pane against a real server: the local half as a source, and the
second pane pointed at another profile's account.

"Another profile" here is a second Model with its own client on a second
bucket. Same credentials, but nothing is shared between the two clients, which
is the property the cross-profile route depends on: the bytes must go through
this process because no server-side copy can see both.
"""

import os
import tempfile

from PyQt6.QtCore import QItemSelectionModel

from model import Model
from tests.e2e.harness import _env, endpoint, purge_bucket, unique_bucket
from tests.e2e_gui.harness import GuiServerCase


class DualPaneTests(GuiServerCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.other_bucket = unique_bucket("pane")
        cls.s3.create_bucket(Bucket=cls.other_bucket)

    @classmethod
    def tearDownClass(cls):
        purge_bucket(cls.s3, cls.other_bucket)
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pane = self.window.second_pane
        self.pane.show()
        self.addCleanup(self.pane.hide)

    def _other_model(self):
        return Model(
            endpoint(), _env("S3DUCK_TEST_REGION", "us-east-1"),
            _env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
            _env("S3DUCK_TEST_SECRET_KEY", "minioadmin"),
            self.other_bucket, True, True)

    def _await_pane(self):
        self.pump(lambda: self.pane._thread is None,
                  what="the second pane's listing")

    def _select_in_listing(self, *names):
        view = self.window.listview
        proxy = view.model()
        selection = view.selectionModel()
        selection.clearSelection()
        for row in range(proxy.rowCount()):
            index = proxy.index(row, 0)
            if index.data() in names:
                selection.select(
                    index, QItemSelectionModel.SelectionFlag.Select
                    | QItemSelectionModel.SelectionFlag.Rows)

    def _other_body(self, key):
        return self.s3.get_object(Bucket=self.other_bucket,
                                  Key=key)["Body"].read()

    def _other_exists(self, key):
        found = self.s3.list_objects_v2(Bucket=self.other_bucket, Prefix=key)
        return any(o["Key"] == key for o in found.get("Contents") or [])

    def test_a_copy_reaches_the_other_profiles_bucket(self):
        self.put("push/a.txt", b"pushed across")
        self.navigate("push/")
        self._select_in_listing("a.txt")
        self.pane.set_other_profile("other", self._other_model(),
                                    self.other_bucket, "in/")
        self._await_pane()
        with self.recording_messages() as shown:
            self.window._pane_profile_transfer(False, False)
            self.await_transfers()
        self.assertEqual(self.last_queue_entry().status, "done", shown)
        self.assertEqual(self._other_body("in/a.txt"), b"pushed across")
        self.assertTrue(self.exists("push/a.txt"), "a copy removed the original")

    def test_a_move_pulls_from_the_other_profile_and_removes_the_original(self):
        self.s3.put_object(Bucket=self.other_bucket, Key="src/b.txt",
                           Body=b"pulled over")
        self.navigate("pull/")
        self.pane.set_other_profile("other", self._other_model(),
                                    self.other_bucket, "src/")
        self._await_pane()
        self.assertIn("b.txt", [n for n, _d, _s in self.pane.entries()])
        self.pane.select_names(["b.txt"])
        with self.recording_messages() as shown, self.confirming():
            self.window._pane_profile_transfer(True, True)
            self.await_transfers()
        self.assertEqual(self.last_queue_entry().method, "move_to_profile")
        self.assertEqual(self.last_queue_entry().status, "done", shown)
        self.assertEqual(self.body("pull/b.txt"), b"pulled over")
        self.assertFalse(self._other_exists("src/b.txt"),
                         "a move left the original behind")

    def test_a_renamed_local_file_uploads_from_the_local_half(self):
        path = os.path.join(self.tmp.name, "draft.txt")
        with open(path, "wb") as handle:
            handle.write(b"from the local pane")
        self.navigate("local/")
        self.pane.set_local(self.tmp.name)
        self.assertTrue(self.pane.rename("draft.txt", "final.txt"))
        self.assertTrue(self.pane.make_folder("nested"))
        self.pane.select_names(["final.txt"])
        with self.recording_messages() as shown:
            self.window.pane_transfer_from_second()
            self.await_transfers()
        self.assertEqual(self.last_queue_entry().status, "done", shown)
        self.assertEqual(self.body("local/final.txt"), b"from the local pane")
