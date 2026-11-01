"""The transfer queue across a pause, a close and a restart."""

import json
import main_window
import threading
import time
import types
import unittest
import botocore.exceptions
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox
from core.queue_state import (
    PENDING_JOB_LIMIT, describe_pending, parse_pending, remaining_job,
    restore_rows, serialize_pending, split_restorable,
)
from model import TransferCancelled
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from ui.widgets.queue_panel import TransferQueuePanel, _QEntry
from unittest.mock import patch


def _entry(entry_id, method="upload", job=None, status="queued", **kw):
    entry = _QEntry(entry_id, method, job if job is not None else [("k", "/f")],
                    label=f"job {entry_id}", source_bucket="bkt",
                    origin={"profile": "p"}, **kw)
    entry.status = status
    return entry


class QueueStateTests(unittest.TestCase):
    """core/queue_state: what of an entry is written down, and what is not."""

    def test_only_unfinished_entries_are_stored_oldest_first(self):
        entries = [_entry(3), _entry(1, status="running"),
                   _entry(2, status="done"), _entry(4, status="paused"),
                   _entry(5, status="error"), _entry(6, status="cancelled")]
        records, skipped = serialize_pending(entries)
        self.assertEqual([r["label"] for r in records],
                         ["job 1", "job 3", "job 4"])
        self.assertEqual([r["paused"] for r in records], [False, False, True])
        self.assertEqual(skipped, [])

    def test_a_pausing_entry_is_stored_as_paused(self):
        records, _ = serialize_pending([_entry(1, status="pausing")])
        self.assertTrue(records[0]["paused"])

    def test_finished_rows_are_left_out(self):
        entry = _entry(1, job=[("a", "/a"), ("b", "/b"), ("c", "/c")])
        entry.done_items = {0, 2}
        records, _ = serialize_pending([entry])
        self.assertEqual(records[0]["job"], [["b", "/b"]])

    def test_a_move_is_stored_whole(self):
        """A move deletes its sources after every copy, so a row reported
        copied is not finished and must run again."""
        entry = _entry(1, method="move",
                       job=[("a", "x/a", False, None), ("b", "x/b", False, None)])
        entry.done_items = {0}
        records, _ = serialize_pending([entry])
        self.assertEqual(len(records[0]["job"]), 2)

    def test_an_entry_with_nothing_left_is_dropped(self):
        entry = _entry(1, job=[("a", "/a")])
        entry.done_items = {0}
        self.assertEqual(serialize_pending([entry]), ([], []))

    def test_cross_profile_and_oversized_and_unserialisable_are_reported(self):
        cross = _entry(1, method="copy_to_profile", job=[("a", "b", "c")])
        huge = _entry(2, job=[("k", "/f")] * (PENDING_JOB_LIMIT + 1))
        odd = _entry(3, job=[object()])
        records, skipped = serialize_pending([cross, huge, odd])
        self.assertEqual(records, [])
        self.assertEqual(skipped, ["job 1", "job 2", "job 3"])

    def test_the_overwrite_answer_and_bucket_travel_with_the_record(self):
        entry = _entry(1, if_none_match=True)
        record = serialize_pending([entry])[0][0]
        self.assertTrue(record["if_none_match"])
        self.assertEqual(record["source_bucket"], "bkt")
        self.assertEqual(record["origin"], {"profile": "p"})

    def test_parse_drops_malformed_records(self):
        raw = json.dumps([
            {"method": "upload", "job": [["k", "/f"]]},
            {"method": "", "job": [["k"]]},
            {"method": "upload", "job": []},
            {"method": "copy_to_profile", "job": [["a"]]},
            "nonsense",
        ])
        self.assertEqual(len(parse_pending(raw)), 1)
        self.assertEqual(parse_pending("{not json"), [])
        self.assertEqual(parse_pending(json.dumps({"a": 1})), [])
        self.assertEqual(parse_pending(""), [])

    def test_split_restorable_needs_the_same_origin(self):
        mine = {"origin": {"profile": "p", "credential_id": "1"}}
        other = {"origin": {"profile": "p", "credential_id": "2"}}
        legacy = {"origin": {}}
        ok, foreign = split_restorable(
            [mine, other, legacy], {"profile": "p", "credential_id": "1"})
        self.assertEqual(ok, [mine])
        self.assertEqual(foreign, [other, legacy])

    def test_helpers(self):
        self.assertEqual(remaining_job(["a", "b", "c"], {1}), ["a", "c"])
        self.assertEqual(restore_rows([["a", 1], "b"]), [("a", 1), "b"])
        text = describe_pending([{"job": [1, 2]}, {"job": [3], "paused": True}])
        self.assertIn("2 unfinished transfers", text)
        self.assertIn("3 items", text)
        self.assertIn("1 paused", text)


class QueuePanelPauseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_buttons_follow_the_status(self):
        panel = TransferQueuePanel()
        entry = _entry(1)
        panel.add_entry(entry)
        row = panel._rows[1]
        row.set_status("running")
        self.assertTrue(row._pause_btn.isVisibleTo(row))
        self.assertFalse(row._resume_btn.isVisibleTo(row))
        row.set_status("paused")
        self.assertFalse(row._pause_btn.isVisibleTo(row))
        self.assertTrue(row._resume_btn.isVisibleTo(row))
        self.assertTrue(row._cancel_btn.isEnabled())
        self.assertFalse(row._retry_btn.isVisibleTo(row))
        row.set_status("done")
        self.assertFalse(row._resume_btn.isVisibleTo(row))

    def test_clear_done_keeps_a_paused_row(self):
        panel = TransferQueuePanel()
        paused, done = _entry(1, status="paused"), _entry(2, status="done")
        for entry in (paused, done):
            panel.add_entry(entry)
            panel.update_status(entry)
        panel._clear_done()
        self.assertEqual(list(panel._rows), [1])

    def test_restore_offer(self):
        panel = TransferQueuePanel()
        panel.show_restore_offer("2 unfinished transfers")
        self.assertTrue(panel.restore_offer_visible())
        self.assertEqual(panel.restore_offer_text(), "2 unfinished transfers")
        panel.hide_restore_offer()
        self.assertFalse(panel.restore_offer_visible())


class _BlockingDeletes:
    """A worker model whose delete of ``block`` waits for the cancel."""

    def __init__(self, block):
        self.block = block
        self.deleted = []
        self.reached = threading.Event()
        self.bucket = "bkt"

    def delete(self, key, log_fn=None, cancel_event=None):
        if key == self.block and not getattr(self, "released", False):
            self.reached.set()
            while not cancel_event.is_set():
                time.sleep(0.01)
            raise TransferCancelled("cancelled")
        self.deleted.append(key)


class _QueueWindowCase(unittest.TestCase):
    """A real window with its own settings scope."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests-queue", "s3duck-tests-queue")
        self.settings.clear()
        self.addCleanup(self.settings.clear)

    def _window(self):
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(win.close)
        return win

    def _pump(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while not predicate() and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()
        self.assertTrue(predicate(), "timed out")

    def _stored(self, win):
        return win.load_pending_queue()



class WindowQueuePersistenceTests(_QueueWindowCase):
    """Pause, resume, and the restore offer, through a real window."""

    def test_a_queued_job_is_written_down_and_offered_after_a_restart(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation("delete", ["a", "b"],
                                        need_refresh=False)
        self.assertEqual(self._stored(win)[0]["job"], ["a", "b"])
        win.close()
        # Teardown cancels the waiting job; that must not erase the record.
        self.assertEqual(len(self._stored(win)), 1)

        again = self._window()
        self.assertTrue(again._queue_panel.restore_offer_visible())
        self.assertIn("1 unfinished transfer", again._queue_panel.restore_offer_text())
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.assertEqual(again.restore_pending_queue(), 1)
        self.assertEqual(started[0].method, "delete")
        self.assertEqual(started[0].job, ["a", "b"])
        self.assertEqual(started[0].source_bucket, win.data_model.bucket)
        self.assertFalse(again._queue_panel.restore_offer_visible())

    def test_discard_forgets_the_last_session(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation("delete", ["a"], need_refresh=False)
        win.close()
        again = self._window()
        again.discard_pending_queue()
        self.assertEqual(self._stored(again), [])
        self.assertFalse(again._queue_panel.restore_offer_visible())

    def test_a_record_from_another_connection_is_not_offered(self):
        record = {"method": "delete", "job": ["a"],
                  "origin": {"profile": "prof", "credential_id": "other"}}
        store_key = "pending_queue/prof"
        self.settings.beginGroup("common")
        self.settings.setValue(store_key, json.dumps([record]))
        self.settings.endGroup()
        win = self._window()
        self.assertFalse(win._queue_panel.restore_offer_visible())
        self.assertEqual(self._stored(win), [])

    def test_a_finished_queue_leaves_nothing_behind(self):
        win = self._window()
        fake = _BlockingDeletes(block=None)
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake):
            win.assign_thread_operation("delete", ["a"], need_refresh=False)
            self._pump(lambda: win._active_entry is None)
        self.assertEqual(fake.deleted, ["a"])
        self.assertEqual(self._stored(win), [])

    def test_pausing_a_running_job_keeps_what_it_finished(self):
        win = self._window()
        fake = _BlockingDeletes(block="b")
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake):
            entry = win.assign_thread_operation(
                "delete", ["a", "b", "c"], need_refresh=False)
            self.assertTrue(fake.reached.wait(5))
            self.assertTrue(win.pause_entry(entry.entry_id))
            self.assertEqual(entry.status, "pausing")
            self._pump(lambda: entry.status == "paused"
                       and win._active_entry is None)
            self.assertEqual(entry.done_items, {0})
            # Paused is not an outcome: nothing in history yet.
            self.assertEqual(win.load_transfer_history(), [])
            stored = self._stored(win)
            self.assertEqual(stored[0]["job"], ["b", "c"])
            self.assertTrue(stored[0]["paused"])

            fake.released = True
            self.assertTrue(win.resume_entry(entry.entry_id))
            self._pump(lambda: entry.status == "done")
        self.assertEqual(fake.deleted, ["a", "b", "c"])
        self.assertEqual(entry.job, ["b", "c"])
        self.assertEqual(self._stored(win), [])

    def test_pausing_a_queued_job_takes_it_out_of_line(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
            self.assertTrue(win.pause_entry(entry.entry_id))
            self.assertEqual(entry.status, "paused")
            self.assertNotIn(entry, win._transfer_queue)
            self.assertEqual(win.paused_entries(), [entry])
            self.assertTrue(win.resume_entry(entry.entry_id))
            self.assertIn(entry, win._transfer_queue)
            self.assertEqual(entry.status, "queued")

    def test_pause_all_and_resume_all(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            first = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
            second = win.assign_thread_operation("delete", ["b"],
                                                 need_refresh=False)
            win.pause_all_transfers()
            self.assertEqual({first.status, second.status}, {"paused"})
            win.resume_all_transfers()
            self.assertEqual(win._transfer_queue, [first, second])

    def test_cancelling_a_paused_job(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
            win.pause_entry(entry.entry_id)
        win._on_queue_cancel_requested(entry.entry_id)
        self.assertEqual(entry.status, "cancelled")
        self.assertEqual(self._stored(win), [])

    def test_a_job_that_cannot_be_kept_is_named_once(self):
        win = self._window()
        logged = []
        win.log = logged.append
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation("copy_to_profile", [("a", "b", False)],
                                        need_refresh=False,
                                        dest_model=types.SimpleNamespace())
            win.assign_thread_operation("delete", ["x"], need_refresh=False)
        notes = [m for m in logged if "not kept across a restart" in m]
        self.assertEqual(len(notes), 1, logged)
        self.assertEqual([r["method"] for r in self._stored(win)], ["delete"])

    def test_a_paused_restore_comes_back_paused(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
            win.pause_entry(entry.entry_id)
        win.close()
        again = self._window()
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: started.append(e)):
            again.restore_pending_queue()
        self.assertEqual(started, [])
        self.assertEqual(len(again.paused_entries()), 1)

    def test_a_profile_switch_parks_the_paused_jobs_with_their_profile(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
            win.pause_entry(entry.entry_id)
        self.assertEqual(win._park_paused_for_profile_switch(), 1)
        self.assertEqual(win.paused_entries(), [])
        self.assertEqual(len(self._stored(win)), 1)


class QueueEdgeTests(_QueueWindowCase):
    """Pause and restore requests that do not apply, and a settings backend
    that cannot sync."""

    def test_requests_that_do_not_apply_are_refused(self):
        win = self._window()
        self.assertFalse(win.pause_entry(999))
        self.assertFalse(win.resume_entry(999))
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
        self.assertFalse(win.resume_entry(entry.entry_id), "resumed a queued job")
        entry.status = "done"
        self.assertFalse(win.pause_entry(entry.entry_id), "paused a finished job")

    def test_resuming_while_busy_waits_in_line(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation(
                "delete", ["a", "b"], need_refresh=False, start_paused=True)
            entry.done_items = {0}
            self.assertTrue(win.resume_entry(entry.entry_id))
        self.assertEqual(entry.job, ["b"])
        self.assertIn(entry, win._transfer_queue)

    def test_cancel_all_writes_the_empty_queue(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation("delete", ["a"], need_refresh=False)
        self.assertEqual(len(self._stored(win)), 1)
        win.cancel_transfers()
        self.assertEqual(self._stored(win), [])

    def test_cancelling_a_waiting_job_updates_the_record(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            entry = win.assign_thread_operation("delete", ["a"],
                                                need_refresh=False)
        win._on_queue_cancel_requested(entry.entry_id)
        self.assertEqual(self._stored(win), [])

    def test_restore_with_nothing_offered(self):
        win = self._window()
        win._pending_restore = []
        self.assertEqual(win.restore_pending_queue(), 0)

    def test_a_sync_failure_is_not_fatal(self):
        win = self._window()
        with patch.object(win.settings, "sync", side_effect=RuntimeError("ro fs")), \
                patch.object(main_window.MainWindow, "transfers_active",
                             lambda _s: True):
            win.assign_thread_operation("delete", ["a"], need_refresh=False)
        self.assertEqual(len(self._stored(win)), 1)

    def test_nothing_is_written_while_closing(self):
        win = self._window()
        win._closing = True
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation("delete", ["a"], need_refresh=False)
        self.assertEqual(self._stored(win), [])
        win._closing = False


def _connection_reset():
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
            raise _connection_reset()

    def set_sse_c_key(self, key):
        self.sse_c_key = key


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
            raise _connection_reset()


class _FailingJobCase(_QueueWindowCase):
    """A window whose failed jobs do not stop the test on a modal dialog."""

    def setUp(self):
        super().setUp()
        quiet = patch.object(QMessageBox, "critical",
                             lambda *a, **kw: QMessageBox.StandardButton.Ok)
        quiet.start()
        self.addCleanup(quiet.stop)

    def _settle(self, seconds):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.01)


class RetryAndPauseTests(_FailingJobCase):
    """Automatic retries and pause/resume, which grew up apart."""

    def test_an_automatic_retry_skips_the_finished_rows(self):
        """REGRESSION: a retry re-ran the whole job. A delete run again on a
        versioned bucket stacks a second marker on the rows it already
        deleted, and undo, which lifts one, then reported them restored."""
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

    def test_pause_all_stops_a_pending_retry(self):
        """REGRESSION: "Pause all" during an automatic retry's wait left the
        timer armed, and the job started a few seconds later anyway."""
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
            self._settle(0.8)
        self.assertEqual(list(win._queue_entries), [first.entry_id])
        self.assertEqual(win._pending_retries, [])
        # Still a failure, and still the user's to retry by hand.
        self.assertFalse(first.retried)
        self.assertEqual(fake.calls, ["y", "z"])

    def test_a_pause_ending_in_an_error_is_paused_not_retried(self):
        """REGRESSION: a pause whose cancellation surfaced as a transient
        error was treated as a failure, retried automatically, and ran to
        the end the user had paused."""
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
            self._settle(0.3)
            self.assertEqual(win._pending_retries, [])
            self.assertEqual(list(win._queue_entries), [entry.entry_id])
            self.assertIsNone(entry.error)
            fake.released = True
            self.assertTrue(win.resume_entry(entry.entry_id))
            self._pump(lambda: entry.status == "done")
        self.assertEqual(entry.job, ["b", "c"])

    def test_a_job_of_another_profile_is_not_resumed(self):
        """REGRESSION (wrong account): a paused job too large to park stays
        on the panel through a profile switch. Resumed there, it ran through
        the old account but recorded its undo under the new one, so Ctrl+Z
        removed delete markers in the new account's same-named bucket."""
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
        win.apply_profile(types.SimpleNamespace(
            name="other-account", url="https://s3.amazonaws.com",
            region="us-east-1", access_key="AK2", secret_key="SK2",
            no_ssl_check=False, use_path=False, session_token="",
            read_only=False))
        self.assertIn(entry.entry_id, win._queue_entries)
        win.data_model.bucket = "logs"
        self.assertFalse(win.resume_entry(entry.entry_id))
        self.assertEqual(entry.status, "paused")
        self.assertEqual(fake.calls, [])
        self.assertIsNone(getattr(win, "_undo_delete", None))


class SyncNotPersistedTests(unittest.TestCase):
    """REGRESSION (data loss): a sync plan -- its deletes included -- was
    written down and offered after a restart, days later maybe, against two
    sides that had changed since. History refuses to re-run one for the
    same reason."""

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

    def test_other_jobs_are_still_kept_with_their_bucket(self):
        records, skipped = serialize_pending(
            [self._entry("delete", ["a", "b"])])
        self.assertEqual(skipped, [])
        self.assertEqual(records[0]["job"], ["a", "b"])
        self.assertEqual(records[0]["bucket"], "bkt")


class StoreKeyTests(unittest.TestCase):

    def test_the_key_is_per_profile(self):
        stub = types.SimpleNamespace(
            store=types.SimpleNamespace(profile_key=lambda n: f"{n}/x"))
        self.assertEqual(
            main_window.MainWindow._pending_queue_key(stub), "pending_queue/x")


if __name__ == "__main__":
    unittest.main()
