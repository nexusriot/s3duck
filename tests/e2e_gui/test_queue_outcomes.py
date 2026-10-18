"""How a job ENDS, reported through the real queue.

A transfer has three outcomes and the user is told them apart by colour: done,
cancelled, failed. Which one a job gets is decided in the worker thread and
carried back by a queued signal, so nothing offline can prove the row the user
sees. The outcome also decides whether anything is said at all -- a
cancellation is silent on purpose, because the user asked for it.

That silence is why the distinction matters more than it looks. A failure
mistaken for a cancellation does not merely get the wrong colour: it loses the
error dialog, the red row and the log line along with it, and the job goes
quiet while the objects are still there. So these tests assert the dialog as
well as the status -- the dialog is the part the user actually notices.

The message boxes are held open across the WAIT, not just across the call:
the failure arrives on a queued signal while the queue is draining, and a
patch that has already exited lets a real modal open in a headless run, where
there is nobody to answer it.
"""

import os
import tempfile

from tests.e2e_gui.harness import GuiServerCase


class FailedJobTests(GuiServerCase):
    """A job that fails has to say so."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")

    def _vanished(self, name):
        """A local path that existed when the job was built and does not now.

        The realistic shape of an upload failure: the picker returned a file,
        and by the time the worker reaches it the file has moved.
        """
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as handle:
            handle.write(b"payload")
        os.remove(path)
        return path

    def _run_upload(self, key, path):
        """Queue one upload and drain, collecting whatever was shown."""
        with self.recording_messages() as shown:
            self.window.assign_thread_operation("upload", [(key, path)])
            self.await_transfers()
        return shown

    def test_a_failed_upload_is_recorded_as_an_error(self):
        path = self._vanished("ordinary.txt")
        shown = self._run_upload("queue/ordinary.txt", path)

        entry = self.last_queue_entry()
        self.assertIsNotNone(entry, "the upload never reached the queue")
        self.assertEqual(entry.status, "error")
        self.assertTrue(entry.error, "a failed job recorded no error")
        self.assertFalse(self.exists("queue/ordinary.txt"))
        self.assertTrue([m for m in shown if m[0] == "critical"],
                        f"the user was never told; shown: {shown}")

    def test_a_failure_whose_message_says_cancelled_is_still_an_error(self):
        """REGRESSION: the worker decided "was this a cancellation?" by
        looking for the word anywhere in str(exc), so a job that failed over
        a key or a path containing it was reported as the user's own
        cancellation -- silently. No dialog, no red row, nothing in the log
        about what actually went wrong.

        The filename here is the ordinary kind of name that collides with
        that test; the error is a real FileNotFoundError carrying the path.
        """
        path = self._vanished("cancelled-orders.csv")
        self.assertIn("cancelled", path)
        shown = self._run_upload("queue/cancelled-orders.csv", path)

        entry = self.last_queue_entry()
        self.assertIsNotNone(entry, "the upload never reached the queue")
        self.assertEqual(entry.status, "error",
                         "a failure was reported as a cancellation")
        self.assertTrue(entry.error, "the failure was swallowed")
        self.assertTrue(
            [m for m in shown if m[0] == "critical"],
            "a failure was silently swallowed as a cancellation")

    def test_a_failed_job_can_be_retried_from_the_queue(self):
        """The row stays actionable, which is the reason a failed entry is
        kept at all."""
        path = self._vanished("retryable.txt")
        self._run_upload("queue/retryable.txt", path)

        failed = self.window.failed_entries()
        self.assertTrue(failed, "the failure is not offered for retry")
        self.assertEqual(failed[-1].status, "error")


class CancelledJobTests(GuiServerCase):
    """A job the user stops is not a failure, and must not be reported as
    one: no error dialog, and a 'cancelled' row."""

    PAYLOAD = 4 * 1024 * 1024
    # Slow enough that the job is certainly still running when cancel is
    # pressed, fast enough that the suite is not waiting on it if it is not.
    RATE = 256 * 1024

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")
        self.addCleanup(self.window.data_model.set_rate_limit, 0)

    def _cancel_a_running_download(self, key):
        """Start a slow download, stop it once it is genuinely running, and
        return whatever the window showed."""
        self.put(key, os.urandom(self.PAYLOAD))
        self.window.data_model.set_rate_limit(self.RATE)
        with self.recording_messages() as shown:
            with self.choosing_directory(self.tmp.name):
                self.window.download_keys([key])
            # Cancelling before the worker starts would prove nothing about
            # the worker's own handling.
            self.pump(lambda: self.window.transfers_active(),
                      what="the download to start")
            self.window.cancel_transfers()
            self.await_transfers()
        return shown

    def test_cancelling_a_running_download_reports_a_cancellation(self):
        self._cancel_a_running_download("queue/slow.bin")

        entry = self.last_queue_entry()
        self.assertIsNotNone(entry, "the download never reached the queue")
        self.assertEqual(entry.status, "cancelled",
                         f"a cancellation was reported as {entry.status}")
        self.assertIsNone(entry.error,
                          "a cancellation must not report an error")

    def test_a_cancellation_shows_the_user_no_error(self):
        """The other half of the same promise: the user asked for this, so
        it is silent."""
        shown = self._cancel_a_running_download("queue/slow-quiet.bin")
        self.assertEqual([m for m in shown if m[0] == "critical"], [],
                         "a cancellation opened an error dialog")

    def test_the_queue_is_usable_again_afterwards(self):
        """A cancelled job still has to release the thread and re-enable the
        window, or the application is stuck read-only."""
        self._cancel_a_running_download("queue/slow-two.bin")

        self.assertFalse(self.window.transfers_active())
        self.window.data_model.set_rate_limit(0)
        self.navigate("")
        self.assertIn("queue", self.listing())


class ReadOnlyWindowTests(GuiServerCase):
    """A read-only profile blocks writes at the model, not just in the menus.

    Driven through the window on purpose: hiding a button and refusing the
    operation are different promises, and only the second one survives a
    shortcut, a drag or a command-palette entry.
    """

    PROFILE = {"read_only": True}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")

    def test_the_window_knows_it_is_read_only(self):
        self.assertTrue(self.window.is_read_only())
        self.assertTrue(self.window.data_model.read_only)

    def test_a_delete_leaves_the_object_where_it_is(self):
        key = self.put("readonly/keep.txt", b"still here")
        with self.recording_messages():
            with self.confirming():
                self.window.delete_keys([key])
            self.await_transfers()

        self.assertTrue(self.exists(key), "a read-only profile deleted it")
        self.assertEqual(self.body(key), b"still here")

    def test_an_upload_writes_nothing(self):
        path = os.path.join(self.tmp.name, "blocked.txt")
        with open(path, "wb") as handle:
            handle.write(b"should not arrive")
        with self.recording_messages():
            with self.choosing_files(path), self.confirming():
                self.window.upload()
            self.await_transfers()

        self.assertFalse(self.exists("blocked.txt"))

    def test_reading_still_works(self):
        """Read-only is a restriction on writes, not a broken profile."""
        self.put("readonly/visible.txt", b"x")
        self.navigate("readonly/")
        self.assertIn("visible.txt", self.listing())

    def test_a_download_still_works(self):
        key = self.put("readonly/fetch.txt", b"fetched")
        with self.recording_messages():
            with self.choosing_directory(self.tmp.name):
                self.window.download_keys([key])
            self.await_transfers()

        landed = os.path.join(self.tmp.name, "readonly", "fetch.txt")
        self.assertTrue(os.path.exists(landed), os.listdir(self.tmp.name))
        with open(landed, "rb") as handle:
            self.assertEqual(handle.read(), b"fetched")
