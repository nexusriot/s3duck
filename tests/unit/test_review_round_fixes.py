"""Regressions for the 2026-10-29 deep review of the v0.27.0 round."""

import io
import json
import main_window
import os
import tempfile
import threading
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox

import s3duck
import s3duck_cli
from core.entries import Item
from core.locations import build_profile_copy_job, move_overlap, same_store
from core.sync_plan import build_sync_plan, local_delete_guard, local_form
from core.policy_templates import build_template, merge_statements
from core.queue_state import sse_c_md5
from core.retention import assess_retention_change
from core.trash import TRASH_PREFIX
from model import FSObjectType
from s3.reports import trim_partial_utf8
from s3duck import SettingsItem
from tests.support import (
    PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window, make_model,
)
from tests.unit.test_cli import _FakeModel
from tests.unit.test_queue_persistence import _BlockingDeletes, _QueueWindowCase
from ui.dialogs.access import RetentionDialog
from ui.dialogs.presigned import post_form_curl
from ui.widgets.second_pane import SecondPane
from ui.window.watch import plan_watch_pass
from unittest.mock import Mock, patch
from workers.transfer import Worker


class AliasProfileMoveTests(unittest.TestCase):
    """REGRESSION (data loss): two profiles on one bucket of one account --
    an alias with the same keys, or the window switched to the profile the
    pane shows -- made "move data/ into data/" copy into data/data/ and then
    delete data/, copies included."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_overlap_rules(self):
        self.assertEqual(move_overlap([("data/", "data/data/", True)]),
                         ("data/", "data/data/"))
        self.assertEqual(move_overlap([("a.txt", "a.txt", False)]),
                         ("a.txt", "a.txt"))
        self.assertEqual(move_overlap([("data/data/", "data/", True)]),
                         ("data/data/", "data/"))
        self.assertIsNone(move_overlap([("a/", "b/a/", True),
                                        ("x.txt", "b/x.txt", False)]))

    def test_same_store(self):
        a = make_model(bucket="b")
        b = make_model(bucket="b")
        self.assertTrue(same_store(a, b))
        self.assertFalse(same_store(a, make_model(bucket="other")))
        self.assertFalse(same_store(a, make_model(bucket="b", access_key="AK2")))
        self.assertFalse(same_store(
            a, make_model(bucket="b", endpoint_url="http://minio:9000")))

    def _worker(self, src, dst, job):
        calls = []
        src.copy_to_model = lambda *a, **kw: calls.append(("copy",) + a[:1])
        src.copy_prefix_to_model = lambda *a, **kw: calls.append(("copy",) + a[:1])
        src.delete = lambda key, **kw: calls.append(("delete", key))
        worker = Worker(src, job, dest_model=dst)
        errors = []
        worker.error.connect(errors.append)
        worker.move_to_profile()
        return calls, errors

    def test_an_overlapping_move_on_one_store_writes_nothing(self):
        job = build_profile_copy_job([("data", "data/", True)], "data/")
        calls, errors = self._worker(make_model(bucket="b"),
                                     make_model(bucket="b"), job)
        self.assertEqual(calls, [])
        self.assertIn("overlaps", errors[0])

    def test_the_same_keys_in_another_bucket_move_normally(self):
        job = build_profile_copy_job([("data", "data/", True)], "data/")
        calls, errors = self._worker(make_model(bucket="b"),
                                     make_model(bucket="other"), job)
        self.assertEqual(errors, [])
        self.assertEqual(calls, [("copy", "data/"), ("delete", "data/")])


class _SlowRemote:
    def __init__(self):
        self.bucket = ""
        self.current_folder = ""
        self.release = threading.Event()

    def clone_for_worker(self):
        return self

    def list(self, prefix):
        self.release.wait(5)
        return [Item(f"from-{prefix or 'root'}.txt", FSObjectType.FILE, None, 1)]


class StaleListingTests(unittest.TestCase):
    """REGRESSION (wrong object): a listing still in flight landed in a pane
    that had moved on, and selected() mapped those rows onto the new target
    -- so F6 or Del acted on a different object than the one shown."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        with open(os.path.join(self.tmp.name, "local.txt"), "wb") as handle:
            handle.write(b"x")
        self.remote = _SlowRemote()
        self.pane = SecondPane(None, types.SimpleNamespace(
            log=Mock(), settings=None, profile_name="me"), self.remote)
        self.addCleanup(self.pane.shutdown)

    def _land(self):
        self.remote.release.set()
        deadline = time.monotonic() + 5
        while self.pane._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def names(self):
        return [n for n, _d, _s in self.pane.entries()]

    def test_a_late_remote_listing_does_not_replace_local_rows(self):
        self.pane.set_remote("bkt", "a/")
        self.pane.set_local(self.tmp.name)
        self.assertEqual(self.names(), ["local.txt"])
        self._land()
        self.assertEqual(self.names(), ["local.txt"])

    def test_moving_on_clears_the_old_rows_and_lists_the_new_target(self):
        self.pane.set_remote("bkt", "a/b/")
        self.pane.go_up()
        self.assertEqual(self.pane.entries(), [],
                         "rows from a/b/ stayed on screen for a/")
        self._land()
        self.assertEqual(self.names(), ["from-a/.txt"])

    def test_a_refresh_asked_for_while_listing_runs_is_not_lost(self):
        self.pane.set_remote("bkt", "a/")
        self.pane.refresh()
        self.assertTrue(self.pane._refresh_again)
        self._land()
        self.assertFalse(self.pane._refresh_again)
        self.assertEqual(self.names(), ["from-a/.txt"])


class WatchGuardExcludeTests(unittest.TestCase):
    """REGRESSION (data loss): the guard's share counted excluded files, so
    a large excluded cache/ let one pass delete every mirrored file."""

    def test_excluded_files_do_not_raise_the_limit(self):
        local = {f"cache/{i}": (1, 0) for i in range(300)}
        local.update({f"doc{i}.txt": (1, 0) for i in range(25)})
        result = plan_watch_pass(local, {}, {"exclude": "cache/",
                                             "delete_extra": True}, "download")
        self.assertEqual(result["local_count"], 25)
        allowed, deletes = local_delete_guard(result["plan"], result["local_count"])
        self.assertEqual((allowed, deletes), (False, 25))


class TrashOverwriteTests(unittest.TestCase):
    """REGRESSION (data loss): deleting a key a second time replaced the
    copy the first delete had put in the trash, which the prompt had
    promised could be restored."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-review", "s3duck-tests-review")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        win.assign_thread_operation = Mock()
        win.set_trash_enabled(True)
        return win

    def test_an_earlier_trashed_copy_is_asked_about(self):
        win = self._window()
        win._destination_conflicts = Mock(return_value={".trash/a.txt"})
        win._resolve_overwrites = Mock(return_value=None)
        self.assertEqual(win.queue_delete(["a.txt"]), (0, 0))
        win._destination_conflicts.assert_called_once_with([".trash/a.txt"])
        win.assign_thread_operation.assert_not_called()

    def test_skipping_the_conflict_still_deletes_the_rest(self):
        win = self._window()
        win._destination_conflicts = Mock(return_value=set())
        self.assertEqual(win.queue_delete(["a.txt", ".trash/b.txt"]), (1, 1))
        win._destination_conflicts = Mock(return_value={".trash/a.txt"})
        win._resolve_overwrites = Mock(return_value=[])
        win.assign_thread_operation.reset_mock()
        self.assertEqual(win.queue_delete(["a.txt", ".trash/b.txt"]), (0, 1))
        win.assign_thread_operation.assert_called_once_with(
            "delete", [".trash/b.txt"])

    def test_a_failed_check_deletes_nothing(self):
        win = self._window()
        win._destination_conflicts = Mock(return_value=None)
        self.assertEqual(win.queue_delete(["a.txt", ".trash/b.txt"]), (0, 0))
        win.assign_thread_operation.assert_not_called()

    def test_a_mixed_selection_says_part_is_permanent(self):
        win = self._window()
        verb = win.delete_verb(["a.txt", TRASH_PREFIX + "b.txt"])
        self.assertIn("deleted for good", verb)
        self.assertEqual(win.delete_verb(["a.txt"]), f"Move to {TRASH_PREFIX}")


class LandingPathPlanTests(unittest.TestCase):
    """REGRESSION (data loss): a key the local side writes under another
    name ("p//x" lands as "x") was downloaded and then deleted as an extra
    in the same sync."""

    def test_local_form(self):
        self.assertEqual(local_form("/x"), "x")
        self.assertEqual(local_form("a\\b"), "a/b")
        self.assertEqual(local_form("a//./b"), "a/b")
        self.assertEqual(local_form(""), "")

    def test_a_rewritten_rel_is_neither_deleted_nor_refetched(self):
        plan = build_sync_plan({"x": (3, 100)}, {"/x": (3, 100)},
                               direction="download", delete_extra=True)
        self.assertEqual([a["action"] for a in plan], ["skip"])
        plan = build_sync_plan({}, {"/x": (3, 100)}, direction="download",
                               delete_extra=True)
        self.assertEqual([(a["action"], a["rel"]) for a in plan],
                         [("download", "/x")])

    def test_upload_is_unchanged(self):
        plan = build_sync_plan({"x": (1, 0)}, {"/x": (1, 0)}, direction="upload",
                               delete_extra=True)
        self.assertEqual(sorted(a["action"] for a in plan),
                         ["delete_remote", "upload"])


class CliGuardTests(unittest.TestCase):
    """REGRESSION (data loss): an unattended `sync --delete` against a
    typo'd prefix or an unmounted folder deleted the whole destination and
    exited 0; and an exclude containing a space was split in two."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _run(self, model, *argv):
        out, err = io.StringIO(), io.StringIO()
        code = s3duck_cli.main(["--endpoint", "http://x", *argv], out=out,
                               err=err, model_factory=lambda bucket: model)
        return code, out.getvalue(), err.getvalue()

    def _local(self, count):
        for i in range(count):
            with open(os.path.join(self.tmp.name, f"f{i}.txt"), "wb") as handle:
                handle.write(b"x")

    def test_a_mass_local_delete_is_refused(self):
        self._local(50)
        code, _out, err = self._run(_FakeModel(), "sync", "--delete",
                                    "s3://b/typo/", self.tmp.name)
        self.assertEqual(code, 1)
        self.assertIn("refusing to delete 50 of 50", err)
        self.assertEqual(len(os.listdir(self.tmp.name)), 50)

    def test_a_mass_remote_delete_is_refused_and_force_overrides(self):
        empty = os.path.join(self.tmp.name, "mnt")
        os.makedirs(empty)
        model = _FakeModel({f"p/{i}": b"x" for i in range(20)})
        code, _out, _err = self._run(model, "sync", "--delete", empty, "s3://b/p/")
        self.assertEqual(code, 1)
        self.assertEqual(len(model.objects), 20)
        code, _out, _err = self._run(model, "sync", "--delete", "--force",
                                     empty, "s3://b/p/")
        self.assertEqual(code, 0)
        self.assertEqual(model.objects, {})

    def test_a_dry_run_is_never_refused(self):
        self._local(50)
        code, out, _ = self._run(_FakeModel(), "sync", "--delete", "--dry-run",
                                 "s3://b/typo/", self.tmp.name)
        self.assertEqual(code, 0)
        self.assertEqual(out.count("delete_local\t"), 50)

    def test_an_exclude_with_a_space_stays_one_pattern(self):
        folder = os.path.join(self.tmp.name, "My Documents")
        os.makedirs(folder)
        with open(os.path.join(folder, "keep.txt"), "wb") as handle:
            handle.write(b"k")
        code, out, _ = self._run(_FakeModel({"p/other": b"o"}), "sync",
                                 "--delete", "--dry-run", "--exclude",
                                 "My Documents/", "s3://b/p/", self.tmp.name)
        self.assertEqual(code, 0)
        self.assertNotIn("My Documents/keep.txt", out)


class LauncherFilteredLocationTests(unittest.TestCase):
    """REGRESSION (wrong account): a location request with a filter on
    started the profile at items[list row] instead of the highlighted one."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_highlighted_profile_is_the_one_started(self):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests-review2", "s3duck-tests-review2")
        win.settings.clear()
        self.addCleanup(win.settings.clear)
        self.addCleanup(win.close)
        win.items = [SettingsItem(n, "https://s3.amazonaws.com", "us-east-1",
                                  "", b"", b"", "false", "false", b"", "false")
                     for n in ("alpha", "beta", "prod-eu")]
        win.populate_list()
        win.filterEdit.setText("prod")
        win.listWidget.setCurrentRow(0)
        started = []
        with patch.object(s3duck.Profiles, "onStart",
                          lambda self_: started.append(
                              self_.items[self_._current_item_index()].name)):
            win.open_location_request("s3://bucket/")
        self.assertEqual(started, ["prod-eu"])


class SelectTruncationTests(unittest.TestCase):

    def test_a_cut_character_is_dropped_not_replaced(self):
        data = "aé€".encode()
        self.assertEqual(trim_partial_utf8(data[:2]), b"a")
        self.assertEqual(trim_partial_utf8(data[:5]), "aé".encode())
        self.assertEqual(trim_partial_utf8(data), data)
        self.assertEqual(trim_partial_utf8(b"\xffabc"), b"\xffabc")

    def test_the_stream_is_closed_when_the_cap_is_hit(self):
        closed = []

        class _Stream:
            def __iter__(self):
                yield {"Records": {"Payload": "é".encode() * 10}}
                yield {"Records": {"Payload": b"more"}}

            def close(self):
                closed.append(True)

        class _Client:
            def select_object_content(self, **_kw):
                return {"Payload": _Stream()}

        model = make_model(bucket="b")
        model._client = _Client()
        result = model.select_object("t.csv", "S", max_bytes=5)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["text"], "éé")
        self.assertEqual(closed, [True])


class QueueReviewTests(_QueueWindowCase):
    """REGRESSIONS from the queue review."""

    def _queued(self, win, job, **kw):
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            return win.assign_thread_operation("delete", job,
                                               need_refresh=False, **kw)

    def test_unanswered_records_survive_this_sessions_queue_changes(self):
        """The bar was still up while every queue change erased the saved
        jobs behind it; a restart then had nothing to offer."""
        win = self._window()
        self._queued(win, ["old"])
        win.close()
        again = self._window()
        self.assertTrue(again._queue_panel.restore_offer_visible())
        entry = self._queued(again, ["new"])
        again._on_queue_cancel_requested(entry.entry_id)
        self.assertEqual([r["job"] for r in self._stored(again)], [["old"]])
        again.close()
        third = self._window()
        self.assertTrue(third._queue_panel.restore_offer_visible())
        third.discard_pending_queue()
        self.assertEqual(self._stored(third), [])

    def test_restoring_keeps_the_record_until_the_job_is_live(self):
        win = self._window()
        self._queued(win, ["old"])
        win.close()
        again = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            again.restore_pending_queue()
        self.assertEqual([r["job"] for r in self._stored(again)], [["old"]])

    def _minted_window(self, access_key):
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, self.settings,
                                   access_key=access_key,
                                   credential_process="aws-sso-mint")
        self.addCleanup(win.close)
        return win

    def test_a_minted_profile_keeps_its_jobs_across_new_session_keys(self):
        win = self._minted_window("ASIA-FIRST-SESSION")
        self._queued(win, ["k"], start_paused=True)
        win.close()
        again = self._minted_window("ASIA-NEW-SESSION")
        self.assertTrue(again._queue_panel.restore_offer_visible(),
                        "a fresh SSO session threw the paused job away")

    def test_a_fixed_key_profile_with_another_key_is_still_refused(self):
        win = self._window()
        self._queued(win, ["k"])
        win.close()
        again = self._window()
        again.data_model.access_key = "SOMEONE-ELSE"
        self.assertFalse(again.offer_pending_restore())

    def test_an_unsaveable_paused_job_stays_through_a_profile_switch(self):
        win = self._window()
        huge = self._queued(win, ["k"] * 10001, start_paused=True)
        small = self._queued(win, ["s"], start_paused=True)
        self.assertEqual(win._park_paused_for_profile_switch(), 1)
        self.assertIn(huge.entry_id, win._queue_entries)
        self.assertNotIn(small.entry_id, win._queue_entries)
        self.assertEqual([r["job"] for r in self._stored(win)], [["s"]])

    def test_an_sse_c_job_comes_back_paused_and_needs_its_key(self):
        win = self._window()
        key = bytes(range(32))
        win.data_model.set_sse_c_key(key)
        self._queued(win, ["enc"])
        stored = self._stored(win)
        self.assertEqual(stored[0]["sse_c_md5"], sse_c_md5(key))
        self.assertNotIn("sse_c_key", json.dumps(stored))
        win.close()
        again = self._window()
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: started.append(e)):
            again.restore_pending_queue()
            entry = again.paused_entries()[0]
            self.assertEqual(started, [], "an SSE-C job restarted without its key")
            self.assertFalse(again.resume_entry(entry.entry_id))
            again.data_model.set_sse_c_key(bytes(32))
            self.assertFalse(again.resume_entry(entry.entry_id), "a wrong key was accepted")
            again.data_model.set_sse_c_key(key)
            self.assertTrue(again.resume_entry(entry.entry_id))
        self.assertEqual(started[0].source_model.sse_c_key, key)

    def test_cancel_while_pausing_cancels(self):
        win = self._window()
        fake = _BlockingDeletes(block="b")
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake):
            entry = win.assign_thread_operation("delete", ["a", "b"],
                                                need_refresh=False)
            self.assertTrue(fake.reached.wait(5))
            win.pause_entry(entry.entry_id)
            win._on_queue_cancel_requested(entry.entry_id)
            self._pump(lambda: win._active_entry is None)
        self.assertEqual(entry.status, "cancelled")
        self.assertEqual(self._stored(win), [])

    def test_undo_after_a_resume_covers_the_whole_delete(self):
        win = self._window()
        fake = _BlockingDeletes(block="b")
        recorded = []
        win._record_undoable_delete = recorded.append
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: fake):
            entry = win.assign_thread_operation("delete", ["a", "b", "c"],
                                                need_refresh=False)
            self.assertTrue(fake.reached.wait(5))
            win.pause_entry(entry.entry_id)
            self._pump(lambda: entry.status == "paused")
            fake.released = True
            win.resume_entry(entry.entry_id)
            self._pump(lambda: entry.status == "done")
        self.assertEqual(entry.job, ["b", "c"])
        self.assertEqual(recorded, [["a", "b", "c"]])
        self.assertEqual(win.load_transfer_history()[0]["items"], 3)


class StorageClassPauseTests(unittest.TestCase):
    """REGRESSION: a row whose objects failed was still reported finished,
    so a pause dropped it and the resumed job ended "done"."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_failed_row_is_not_marked_done(self):
        class Model:
            parallel_files = 1

            def change_storage_class(self, key, cls_, log_fn=None):
                if key == "bad":
                    raise RuntimeError("InvalidStorageClass")
                return True

        worker = Worker(Model(), [("good", False, "STANDARD_IA"),
                                  ("bad", False, "STANDARD_IA")])
        done = []
        worker.item_done.connect(done.append)
        worker.set_storage_class()
        self.assertEqual(done, [0])


class UploadWatchGuardTests(unittest.TestCase):
    """An upload watch with delete extras now has the same mass-delete
    guard: an unmounted local folder would otherwise empty the prefix."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_count_is_of_the_remote_side(self):
        remote = {f"o{i}": (1, 0) for i in range(30)}
        result = plan_watch_pass({}, remote, {"delete_extra": True}, "upload")
        self.assertEqual(result["local_count"], 30)
        self.assertEqual(local_delete_guard(result["plan"], 30,
                                            action="delete_remote"),
                         (False, 30))

    def test_the_watch_stops_instead_of_deleting(self):
        settings = QSettings("s3duck-tests-review3", "s3duck-tests-review3")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(main_window.MainWindow, "_watch_tick", lambda _s: None):
                win.start_watch({"local": folder, "interval": 60, "exclude": "",
                                 "delete_extra": True, "direction": "upload",
                                 "bucket": "bkt", "prefix": "p/"})
            with patch.object(main_window.MainWindow, "start_sync") as start:
                win._on_watch_planned({"plan": [
                    {"action": "delete_remote", "rel": str(i), "size": 1}
                    for i in range(30)], "local_count": 30}, None)
        start.assert_not_called()
        self.assertFalse(win.watch_active())


class AccessReviewTests(unittest.TestCase):
    """REGRESSIONS from the access-control review."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_read_only_profile_cannot_mint_an_upload_form(self):
        model = make_model(endpoint_url="http://localhost:9000", bucket="b",
                           use_path=True, read_only=True)
        with self.assertRaises(Exception) as caught:
            model.presigned_post("in/", 600, any_key_under_prefix=True)
        self.assertIn("read-only", str(caught.exception).lower())

    def test_no_link_is_minted_while_an_sse_c_key_is_set(self):
        model = make_model(endpoint_url="http://localhost:9000", bucket="b",
                           use_path=True)
        model.set_sse_c_key(bytes(32))
        for mint in (lambda: model.presigned_get_url("k", 60),
                     lambda: model.presigned_put_url("k", 60),
                     lambda: model.presigned_post("k", 60)):
            with self.assertRaises(ValueError) as caught:
                mint()
            self.assertIn("SSE-C", str(caught.exception))
        model.set_sse_c_key(b"")
        self.assertIn("X-Amz-Signature", model.presigned_get_url("k", 60))

    def test_select_carries_the_customer_key(self):
        model = make_model(endpoint_url="http://localhost:9", bucket="b",
                           use_path=True)
        model.set_sse_c_key(bytes(32))
        seen = {}

        def before_send(request, **_kw):
            seen.update(request.headers)
            raise RuntimeError("sent")

        model.client.meta.events.register("before-send.s3", before_send)
        with self.assertRaises(RuntimeError):
            model.select_object("t.csv", "SELECT 1")
        self.assertIn("x-amz-server-side-encryption-customer-key",
                      {k.lower() for k in seen})

    def test_a_dsse_kms_etag_is_not_compared(self):
        model = make_model(bucket="b")
        logged = []
        self.assertTrue(model.verify_download(
            __file__, "0" * 32, head={"ServerSideEncryption": "aws:kms:dsse"},
            log_fn=logged.append))
        self.assertIn("SSE-KMS", logged[-1])

    def test_templates_for_different_prefixes_do_not_replace_each_other(self):
        first = build_template("allow_account", "bkt", prefix="reports/",
                               account_id="123456789012")
        second = build_template("allow_account", "bkt", prefix="invoices/",
                                account_id="123456789012")
        merged = json.loads(merge_statements(merge_statements("", first), second))
        resources = [str(st.get("Resource")) for st in merged["Statement"]]
        self.assertTrue(any("reports/" in r for r in resources), resources)
        self.assertTrue(any("invoices/" in r for r in resources), resources)
        a = build_template("public_read", "bkt", prefix="a-b/")[0]["Sid"]
        b = build_template("public_read", "bkt", prefix="ab/")[0]["Sid"]
        self.assertNotEqual(a, b)
        again = merge_statements(merge_statements("", first), first)
        self.assertEqual(len(json.loads(again)["Statement"]), 2,
                         "re-inserting the same template stacked a copy")

    def test_a_bypass_no_longer_needed_is_not_sent(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state = {"mode": "GOVERNANCE",
                 "retain_until_dt": now + timedelta(days=30)}
        dlg = RetentionDialog(None, "k", state, now=now)
        dlg.until.setDateTime(dlg.until.dateTime().addDays(-20))
        dlg.bypass.setChecked(True)
        self.assertTrue(dlg.chosen()[2])
        dlg.until.setDateTime(dlg.until.dateTime().addDays(40))
        self.assertTrue(dlg.bypass.isHidden())
        self.assertFalse(dlg.chosen()[2], "a hidden bypass was still sent")

    def test_an_unchanged_date_with_fractions_is_not_a_shortening(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        current = datetime(2026, 2, 1, 0, 0, 7, 500000, tzinfo=timezone.utc)
        same = current.replace(microsecond=0)
        self.assertEqual(assess_retention_change("COMPLIANCE", current,
                                                 "COMPLIANCE", same, now=now),
                         ("", False, True))
        self.assertEqual(assess_retention_change("GOVERNANCE", current,
                                                 "GOVERNANCE", same, now=now),
                         ("", False, False))

    def test_curl_fields_are_literal(self):
        curl = post_form_curl({"url": "https://h/", "fields": {
            "key": "<notes.txt", "Content-Type": "a;type=x"}})
        self.assertIn("--form-string 'key=<notes.txt'", curl)
        self.assertIn("--form-string 'Content-Type=a;type=x'", curl)
        self.assertNotIn("-F 'key=", curl)
        self.assertIn("-F file=@/path/to/file ", curl)


if __name__ == "__main__":
    unittest.main()
