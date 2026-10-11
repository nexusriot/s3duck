"""Uploads, downloads and deletes driven through the window's own entry
points, against a real server.

These are the methods the menus and shortcuts call. The only things stubbed
are ``QFileDialog`` and ``QMessageBox`` — the operating system asking the user
something, which headless has nobody to answer. Everything below them is real:
the overwrite guard, the transfer queue, the worker thread, the model, and the
bucket the result is checked against.
"""

import os
import tempfile

from tests.e2e_gui.harness import GuiServerCase


class UploadTests(GuiServerCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")

    def _local(self, name, body=b"payload"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as handle:
            handle.write(body)
        return path

    def test_an_uploaded_file_arrives_byte_for_byte(self):
        path = self._local("hello.txt", b"hello from the gui")

        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()

        self.assertEqual(self.body("hello.txt"), b"hello from the gui")
        self.assertEqual(self.queue_statuses()[-1], "done")

    def test_an_upload_lands_under_the_folder_being_viewed(self):
        # The key is built from the current folder, so navigating and then
        # uploading has to put the object where the user is looking.
        self.navigate("inbox/")
        path = self._local("note.txt", b"note")

        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()

        self.assertIn("inbox/note.txt", self.keys("inbox/"))

    def test_several_files_in_one_gesture_all_arrive(self):
        paths = [self._local(f"m{i}.txt", f"body {i}".encode()) for i in range(3)]

        with self.choosing_files(*paths), self.confirming():
            self.window.upload()
        self.await_transfers()

        for i in range(3):
            self.assertEqual(self.body(f"m{i}.txt"), f"body {i}".encode())

    def test_an_empty_file_uploads(self):
        # Zero bytes is the boundary every transfer path gets wrong at least
        # once: an empty body is falsy, and a length check written as a truth
        # test skips it.
        path = self._local("empty.bin", b"")

        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()

        self.assertTrue(self.exists("empty.bin"))
        self.assertEqual(self.body("empty.bin"), b"")

    def test_cancelling_the_file_picker_uploads_nothing(self):
        before = self.keys()
        # An empty selection is what the dialog returns on Cancel.
        with self.choosing_files():
            self.window.upload()
        self.await_transfers()
        self.assertEqual(self.keys(), before)

    def test_the_listing_shows_the_upload_without_being_told_twice(self):
        # need_refresh is set for an upload, so the window re-lists itself
        # when the worker finishes. Without it the user sees a bucket that
        # does not contain the file they just sent.
        self.navigate("fresh/")
        path = self._local("shown.txt", b"x")

        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()
        self.pump(lambda: "shown.txt" in self.listing(),
                  what="the listing to refresh after the upload")


class DownloadTests(GuiServerCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.s3.put_object(Bucket=cls.bucket, Key="dl/one.txt", Body=b"first")
        cls.s3.put_object(Bucket=cls.bucket, Key="dl/two.txt", Body=b"second")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("dl/")

    def test_a_downloaded_object_is_written_to_disk(self):
        with self.choosing_directory(self.tmp.name), self.confirming():
            self.window.download_keys(["dl/one.txt"])
        self.await_transfers()

        landed = os.path.join(self.tmp.name, "dl", "one.txt")
        self.assertTrue(os.path.exists(landed), f"{landed} was not written")
        with open(landed, "rb") as handle:
            self.assertEqual(handle.read(), b"first")

    def test_downloading_several_keeps_each_body(self):
        with self.choosing_directory(self.tmp.name), self.confirming():
            self.window.download_keys(["dl/one.txt", "dl/two.txt"])
        self.await_transfers()

        base = os.path.join(self.tmp.name, "dl")
        with open(os.path.join(base, "one.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"first")
        with open(os.path.join(base, "two.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"second")

    def test_a_download_keeps_the_key_s_prefix(self):
        # Flattening would silently overwrite same-named objects from
        # different prefixes, which is why download_keys exists at all.
        with self.choosing_directory(self.tmp.name), self.confirming():
            self.window.download_keys(["dl/one.txt"])
        self.await_transfers()

        self.assertFalse(
            os.path.exists(os.path.join(self.tmp.name, "one.txt")),
            "the download flattened the key into the chosen folder")

    def test_cancelling_the_folder_picker_writes_nothing(self):
        with self.choosing_directory(""):
            self.window.download_keys(["dl/one.txt"])
        self.await_transfers()
        self.assertEqual(os.listdir(self.tmp.name), [])


class DeleteTests(GuiServerCase):
    def setUp(self):
        self.navigate("")

    def test_a_deleted_object_is_gone_from_the_bucket(self):
        self.put("doomed.txt", b"x")

        with self.confirming():
            self.window.delete_keys(["doomed.txt"])
        self.await_transfers()

        self.assertFalse(self.exists("doomed.txt"))

    def test_deleting_several_removes_all_of_them(self):
        keys = [self.put(f"batch-{i}.txt") for i in range(5)]

        with self.confirming():
            self.window.delete_keys(keys)
        self.await_transfers()

        for key in keys:
            self.assertFalse(self.exists(key), f"{key} survived the delete")

    def test_a_delete_leaves_its_neighbours_alone(self):
        self.put("keep-me.txt", b"keep")
        self.put("remove-me.txt", b"go")

        with self.confirming():
            self.window.delete_keys(["remove-me.txt"])
        self.await_transfers()

        self.assertTrue(self.exists("keep-me.txt"))
        self.assertFalse(self.exists("remove-me.txt"))


class QueueTests(GuiServerCase):
    """The queue is the part no unit test can prove: entries are started,
    finished and reported from a worker thread."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")

    def _local(self, name, body=b"x"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as handle:
            handle.write(body)
        return path

    def test_a_finished_transfer_is_recorded_as_done(self):
        path = self._local("queued.txt")
        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()

        entry = self.last_queue_entry()
        self.assertIsNotNone(entry, "the upload never reached the queue")
        self.assertEqual(entry.status, "done")
        self.assertIsNone(entry.error)

    def test_back_to_back_transfers_all_complete(self):
        # The second is queued behind the first, so this covers the hand-off
        # the queue does when a worker finishes — the path that strands every
        # later transfer if it is wrong.
        #
        # The queue keeps finished entries so a failed row can be re-run, so
        # count from where this test started rather than from zero.
        before = len(self.queue_statuses())
        for i in range(3):
            path = self._local(f"serial-{i}.txt", f"{i}".encode())
            with self.choosing_files(path), self.confirming():
                self.window.upload()
        self.await_transfers()

        statuses = self.queue_statuses()[before:]
        self.assertEqual(len(statuses), 3, f"queue holds {statuses}")
        self.assertEqual(set(statuses), {"done"}, f"statuses: {statuses}")
        for i in range(3):
            self.assertEqual(self.body(f"serial-{i}.txt"), f"{i}".encode())

    def test_the_queue_drains_so_navigation_is_allowed_again(self):
        # Navigation is blocked while transfers run. If the queue never
        # reports itself idle the application stays read-only forever.
        path = self._local("blocker.txt")
        with self.choosing_files(path), self.confirming():
            self.window.upload()
        self.await_transfers()

        self.assertFalse(self.window.transfers_active())
        self.navigate("")
        self.assertIn("blocker.txt", self.listing())
