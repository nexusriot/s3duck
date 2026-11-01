"""Regressions from the review of the feat_func_improvements x main merge.

The two branches grew the transfer queue separately: one added automatic
retries, per-job bucket/profile and undo bounds, the other pause/resume and a
queue that survives a restart. Each was right on its own; these are the
places where the combination was not.
"""

import os
import shutil
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

import botocore.exceptions
from PyQt6.QtWidgets import QMessageBox

import main_window
import utils
from core.entries import Item
from core.queue_state import PENDING_JOB_LIMIT, parse_pending, serialize_pending
from model import FSObjectType
from tests.support import _CopyPartClient, _ensure_qapp, make_model
from tests.unit.test_queue_persistence import _QueueWindowCase
from ui.widgets.second_pane import SecondPane


def _ConnectionReset():
    """A dropped connection: transient, so the job is retried once."""
    return botocore.exceptions.ConnectionClosedError(
        endpoint_url="https://s3.example.test/bkt")


class _FlakyDeletes:
    """A worker model whose delete of each key in ``fail_once`` fails once."""

    def __init__(self, fail_once=(), bucket="bkt"):
        self.calls = []
        self.fail_once = set(fail_once)
        self.bucket = bucket
        self.sse_c_key = b""
        self.binding_cache = {}

    def delete(self, key, log_fn=None, cancel_event=None):
        self.calls.append(key)
        if key in self.fail_once:
            self.fail_once.discard(key)
            raise _ConnectionReset()

    def set_sse_c_key(self, key):
        self.sse_c_key = key


class _FailingJobCase(_QueueWindowCase):
    """A window whose failed jobs do not stop the test on a modal dialog."""

    def setUp(self):
        super().setUp()
        quiet = patch.object(QMessageBox, "critical",
                             lambda *a, **kw: QMessageBox.StandardButton.Ok)
        quiet.start()
        self.addCleanup(quiet.stop)


class RetryRunsOnlyWhatDidNotFinishTests(_FailingJobCase):
    """REGRESSION: a retry re-ran the whole job. A delete run again on a
    versioned bucket stacks a second marker on the rows it already deleted,
    and undo, which lifts one, then reported them restored."""

    def test_an_automatic_retry_skips_the_finished_rows(self):
        win = self._window()
        fake = _FlakyDeletes(fail_once={"c"})
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake), \
                patch("ui.window.transfers.AUTO_RETRY_DELAY_MS", 50):
            first = win.assign_thread_operation("delete", ["a", "b", "c"],
                                                need_refresh=False)
            self._pump(lambda: first.status == "error")
            self._pump(lambda: len(win._queue_entries) == 2 and
                       max(win._queue_entries.values(),
                           key=lambda e: e.entry_id).status == "done")
        self.assertEqual(fake.calls, ["a", "b", "c", "c"])
        retry = max(win._queue_entries.values(), key=lambda e: e.entry_id)
        self.assertEqual(retry.job, ["c"])
        # Undo still covers everything the two attempts deleted.
        self.assertEqual(retry.full_job, ["a", "b", "c"])


class PauseAllHoldsPendingRetriesTests(_FailingJobCase):
    """REGRESSION: "Pause all" during an automatic retry's wait left the
    timer armed, and the job started a few seconds later anyway."""

    def test_pause_all_stops_the_retry_timer(self):
        win = self._window()
        fake = _FlakyDeletes(fail_once={"z"})
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake), \
                patch("ui.window.transfers.AUTO_RETRY_DELAY_MS", 300):
            first = win.assign_thread_operation("delete", ["y", "z"],
                                                need_refresh=False)
            self._pump(lambda: first.status == "error")
            self.assertEqual(len(win._pending_retries), 1)
            win.pause_all_transfers()
            deadline = time.monotonic() + 0.8
            while time.monotonic() < deadline:
                self._app.processEvents()
                time.sleep(0.01)
        self.assertEqual(list(win._queue_entries), [first.entry_id])
        self.assertEqual(win._pending_retries, [])
        # Still a failure, and still the user's to retry by hand.
        self.assertFalse(first.retried)
        self.assertEqual(fake.calls, ["y", "z"])


class _ResetOnCancel(_FlakyDeletes):
    """Deleting ``block`` waits for the cancel, which then surfaces as a
    connection reset rather than TransferCancelled."""

    def __init__(self, block):
        super().__init__()
        self.block = block
        self.reached = threading.Event()
        self.released = False

    def delete(self, key, log_fn=None, cancel_event=None):
        self.calls.append(key)
        if key == self.block and not self.released:
            self.reached.set()
            while not cancel_event.is_set():
                time.sleep(0.01)
            raise _ConnectionReset()


class PauseEndingInAnErrorTests(_FailingJobCase):
    """REGRESSION: a pause whose cancellation surfaced as a transient error
    was treated as a failure, retried automatically, and ran to the end the
    user had paused."""

    def test_the_job_is_paused_not_retried(self):
        win = self._window()
        fake = _ResetOnCancel(block="b")
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake), \
                patch("ui.window.transfers.AUTO_RETRY_DELAY_MS", 50):
            entry = win.assign_thread_operation("delete", ["a", "b", "c"],
                                                need_refresh=False)
            self.assertTrue(fake.reached.wait(5))
            win.pause_entry(entry.entry_id)
            self._pump(lambda: entry.status == "paused")
            deadline = time.monotonic() + 0.3
            while time.monotonic() < deadline:
                self._app.processEvents()
                time.sleep(0.01)
            self.assertEqual(win._pending_retries, [])
            self.assertEqual(list(win._queue_entries), [entry.entry_id])
            self.assertIsNone(entry.error)
            fake.released = True
            self.assertTrue(win.resume_entry(entry.entry_id))
            self._pump(lambda: entry.status == "done")
        self.assertEqual(entry.job, ["b", "c"])


class ResumeAfterProfileSwitchTests(_QueueWindowCase):
    """REGRESSION (wrong account): a paused job too large to park stays on
    the panel through a profile switch. Resumed there, it ran through the old
    account but recorded its undo under the new one, so Ctrl+Z removed delete
    markers in the new account's bucket of the same name."""

    def _switch(self, win, name):
        win.apply_profile(types.SimpleNamespace(
            name=name, url="https://s3.amazonaws.com", region="us-east-1",
            access_key="AK2", secret_key="SK2", no_ssl_check=False,
            use_path=False, session_token="", read_only=False))

    def test_a_job_of_another_profile_is_not_resumed(self):
        win = self._window()
        win.data_model.bucket = "logs"
        fake = _FlakyDeletes(bucket="logs")
        keys = [f"k{i}" for i in range(PENDING_JOB_LIMIT + 1)]
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake), \
                patch.object(main_window.MainWindow, "transfers_active",
                             lambda _s: True):
            entry = win.assign_thread_operation("delete", keys,
                                                need_refresh=False)
        self.assertTrue(win.pause_entry(entry.entry_id))
        self._switch(win, "other-account")
        self.assertIn(entry.entry_id, win._queue_entries)
        win.data_model.bucket = "logs"
        self.assertFalse(win.resume_entry(entry.entry_id))
        self.assertEqual(entry.status, "paused")
        self.assertEqual(fake.calls, [])
        self.assertIsNone(getattr(win, "_undo_delete", None))


class SyncIsNotReplayedAfterARestartTests(unittest.TestCase):
    """REGRESSION (data loss): a sync plan -- its deletes included -- was
    written down and offered after a restart, days later maybe, against two
    sides that had changed since. History refuses to re-run one for the same
    reason."""

    def _entry(self, method, job):
        return types.SimpleNamespace(
            entry_id=1, method=method, job=job, status="queued", label=method,
            need_refresh=True, source_bucket="bkt", bucket="bkt",
            if_none_match=False, origin={}, done_items=set(), sse_c_md5="")

    def test_a_sync_is_skipped_and_said_so(self):
        job = [["delete_remote", "old.txt", "/tmp/x/old.txt", "old.txt", 1]]
        records, skipped = serialize_pending([self._entry("sync", job)])
        self.assertEqual(records, [])
        self.assertEqual(skipped, ["sync"])

    def test_a_stored_sync_is_not_offered(self):
        raw = ('[{"method": "sync", "job": [["upload", "a", "/a", "a", 1]]},'
               ' {"method": "delete", "job": ["a"]}]')
        self.assertEqual([r["method"] for r in parse_pending(raw)],
                         ["delete"])

    def test_other_jobs_are_still_kept(self):
        records, skipped = serialize_pending(
            [self._entry("delete", ["a", "b"])])
        self.assertEqual(skipped, [])
        self.assertEqual(records[0]["job"], ["a", "b"])
        self.assertEqual(records[0]["bucket"], "bkt")


class _ChangesAfterPartOne(_CopyPartClient):
    """The source is overwritten once the first part is copied, so every
    later part's CopySourceIfMatch is refused."""

    def upload_part_copy(self, **kw):
        if kw["PartNumber"] > 1:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "PreconditionFailed",
                           "Message": "At least one of the pre-conditions "
                                      "you specified did not hold"}},
                "UploadPartCopy")
        return super().upload_part_copy(**kw)


class SourceChangedStaysAClientErrorTests(unittest.TestCase):
    """REGRESSION: one branch turned the refused multipart copy into a bare
    Exception with a readable message; the other's callers (and its
    end-to-end suite) branch on the ClientError's PreconditionFailed code."""

    def test_the_error_keeps_its_code_and_reads_as_what_happened(self):
        model = make_model(bucket="b")
        client = _ChangesAfterPartOne(source_size=11)
        model._client = client
        model.copy_part_size = lambda *a: 5
        model.transfer_concurrency = 1
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            model._copy_multipart("key", "copy", size=11)
        self.assertEqual(caught.exception.response["Error"]["Code"],
                         "PreconditionFailed")
        self.assertIn("changed on the server", str(caught.exception))
        self.assertEqual(client.completed, [])
        self.assertEqual(len(client.aborted), 1)


@unittest.skipIf(os.name == "nt", "POSIX symlinks")
class StrictScanDanglingLinkTests(unittest.TestCase):
    """REGRESSION (data loss): a strict scan skipped a symlink whose target
    was missing -- a file on an unmounted drive -- as if it had vanished, so
    two-way sync, `sync --delete`, an upload watch and the Sync dialog
    deleted its remote copy."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        with open(os.path.join(self.root, "a.txt"), "w") as handle:
            handle.write("a")
        os.symlink(os.path.join(self.root, "unmounted", "b.txt"),
                   os.path.join(self.root, "b.txt"))

    def test_a_strict_scan_refuses_it(self):
        with self.assertRaises(OSError):
            utils.scan_local_tree(self.root, strict=True)

    def test_a_lenient_scan_still_skips_it(self):
        self.assertEqual(list(utils.scan_local_tree(self.root)), ["a.txt"])


class _GatedRemote:
    """A remote whose listing waits for ``gate``."""

    bucket = "b"
    current_folder = "p/"

    def __init__(self):
        self.gate = threading.Event()

    def clone_for_worker(self):
        return self

    def list(self, prefix):
        self.gate.wait(5)
        return [Item("remote.txt", FSObjectType.FILE, "", 1)]


class SecondPaneLocalRowsTests(unittest.TestCase):
    """REGRESSION (wrong object): Remote -> Local -> Remote kept the local
    rows on screen under the remote path while the new listing loaded, and
    F5/F6 resolved a selected local name as a key under the remote prefix."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _pump(self, predicate):
        deadline = time.monotonic() + 5
        while not predicate() and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def test_switching_back_to_remote_clears_the_local_rows(self):
        remote = _GatedRemote()
        window = types.SimpleNamespace(log=Mock(), settings=None,
                                       profile_name="me")
        pane = SecondPane(None, window, remote)
        self.addCleanup(pane.shutdown)
        self.addCleanup(remote.gate.set)
        remote.gate.set()
        pane.set_remote("b", "p/")
        self._pump(lambda: pane.entries())
        local = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, local, ignore_errors=True)
        open(os.path.join(local, "localonly.txt"), "w").close()
        remote.gate.clear()
        pane.set_local(local)
        self.assertEqual([e[0] for e in pane.entries()], ["localonly.txt"])
        pane.mode.setCurrentIndex(pane.mode.findData(SecondPane.REMOTE))
        self.assertEqual(pane.location(), "s3://b/p/")
        self.assertEqual(pane.entries(), [])
        remote.gate.set()
        self._pump(lambda: pane.entries())
        self.assertEqual([e[0] for e in pane.entries()], ["remote.txt"])


class ThumbnailsWaitForTheListingTests(_QueueWindowCase):
    """Thumbnail view switched on mid-navigation fetched the old rows' names
    under the new folder: objects nobody was looking at."""

    def test_nothing_is_fetched_while_the_listing_moves(self):
        win = self._window()
        win.data_model.bucket = "b"
        win.in_bucket_list_mode = lambda: False
        win.thumbnail_view_active = lambda: True
        win.modelToListView([Item("a.png", FSObjectType.FILE, "", 100)])
        win.data_model.current_folder = "elsewhere/"
        self.assertFalse(win.listing_settled())
        win.data_model.clone_for_worker = Mock(
            side_effect=AssertionError("fetched for an unsettled listing"))
        self.assertEqual(win.request_thumbnails(), 0)


if __name__ == "__main__":
    unittest.main()
