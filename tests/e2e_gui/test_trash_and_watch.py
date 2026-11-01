"""Delete-to-trash and the download watch, driven through the real window.

Both are a plan the window builds and the queue executes, so only a server
shows the outcome: an object that moved rather than vanished, and a folder
filled from a bucket by a timer nobody pressed.
"""

import os
import tempfile

from tests.e2e_gui.harness import GuiServerCase


class TrashTests(GuiServerCase):

    def setUp(self):
        super().setUp()
        self.navigate("")
        self.window.set_trash_enabled(True)
        self.addCleanup(self.window.set_trash_enabled, False)

    def test_a_delete_moves_and_a_restore_brings_it_back(self):
        key = self.put("trash/doc.txt", b"keep me")
        with self.recording_messages(), self.confirming():
            self.window.delete_keys([key])
            self.await_transfers()
        self.assertFalse(self.exists(key))
        self.assertEqual(self.body(".trash/trash/doc.txt"), b"keep me")

        with self.recording_messages():
            self.assertTrue(self.window.restore_from_trash([".trash/trash/doc.txt"]))
            self.await_transfers()
        self.assertEqual(self.body(key), b"keep me")
        self.assertFalse(self.exists(".trash/trash/doc.txt"))

    def test_emptying_the_trash_deletes_for_good(self):
        self.put(".trash/old/a.txt", b"a")
        self.put(".trash/old/b.txt", b"b")
        with self.recording_messages(), self.confirming():
            self.assertTrue(self.window.empty_trash())
            self.await_transfers()
        self.assertEqual(self.keys(".trash/"), [])


class DownloadWatchTests(GuiServerCase):

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")
        self.addCleanup(lambda: self.window.watch_active()
                        and self.window.stop_watch())

    def _start(self, delete_extra=False):
        self.window.start_watch({
            "local": self.tmp.name, "interval": 3600, "exclude": "",
            "delete_extra": delete_extra, "direction": "download",
            "bucket": self.bucket, "prefix": "mirror/"})

    def _idle(self):
        self.pump(lambda: self.window._watch_thread is None,
                  what="the watch comparison")
        self.await_transfers()

    def test_a_pass_fetches_what_the_bucket_has(self):
        self.put("mirror/a.txt", b"from the bucket")
        self.put("mirror/deep/b.txt", b"deep")
        with self.recording_messages():
            self._start()
            self._idle()
        with open(os.path.join(self.tmp.name, "deep", "b.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"deep")
        self.assertTrue(self.window.watch_active())

    def test_a_pass_that_would_empty_the_folder_is_refused(self):
        for i in range(8):
            with open(os.path.join(self.tmp.name, f"local{i}.txt"), "wb") as handle:
                handle.write(b"mine")
        with self.recording_messages():
            self._start(delete_extra=True)
            self._idle()
        self.assertFalse(self.window.watch_active(),
                         "the watch kept going after refusing a mass delete")
        self.assertEqual(len(os.listdir(self.tmp.name)), 8,
                         "local files were deleted")
