"""Edges of the v0.28.0 features that the feature tests did not reach:
concurrent saves from an external editor, stale and cancelled
point-in-time scans, cancel and pause of a restore job, queue follow-ups
that fail, conflict resolution on rows that are not conflicts, and
thumbnails that do not decode."""

import main_window
import os
import threading
import time
import types
import unittest
import ui.window.external_edit as external_edit
from datetime import timedelta
from PyQt6.QtCore import QDateTime, QSettings
from PyQt6.QtWidgets import QMessageBox
from core.point_in_time import DELETE, RESTORE
from core.two_way import CONFLICT, UPLOAD
from model import TransferCancelled
from tests.support import (
    PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window, make_model,
)
from tests.unit.test_external_edit_and_thumbnails import _EditModel, _inline
from tests.unit import test_point_in_time_and_search as pit_tests
from tests.unit.test_point_in_time_and_search import T0, _PlanModel
from tests.unit.test_queue_persistence import _QueueWindowCase
from ui.dialogs.compare import SyncDialog
from ui.dialogs.point_in_time import PointInTimeDialog
from ui.widgets.thumbnails import ThumbnailLoader
from unittest.mock import Mock, patch
from workers.transfer import Worker


def _pump(app, predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.005)
    app.processEvents()
    return predicate()


class _SlowEditModel(_EditModel):
    """Uploads wait for a release, so a second save lands mid-upload."""

    def __init__(self):
        super().__init__()
        self.release = threading.Event()
        self.entered = threading.Event()

    def put_object_body(self, key, data, if_match=""):
        self.entered.set()
        self.release.wait(5)
        return super().put_object_body(key, data, if_match)


class ExternalEditEdgeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, model):
        settings = QSettings("s3duck-tests-edge", "s3duck-tests-edge")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        win.data_model.bucket = "bkt"
        win.data_model.clone_for_worker = lambda: model
        self.addCleanup(lambda: (win.stop_external_edits(ask=False), win.close()))
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QDesktopServices, "openUrl"):
            path = win.edit_externally("doc.txt")
        return win, path

    def _write(self, path, body):
        time.sleep(0.01)
        with open(path, "wb") as handle:
            handle.write(body)

    def test_a_save_during_an_upload_is_sent_after_it_not_lost(self):
        """The second save arrives while the first PUT is still in flight;
        it must go out afterwards, with the ETag the first one produced."""
        model = _SlowEditModel()
        win, path = self._window(model)
        self._write(path, b"first")
        win._on_edit_file_changed(path)
        win.flush_external_edits()
        self.assertTrue(model.entered.wait(5))
        self._write(path, b"second")
        win._on_edit_file_changed(path)
        self.assertEqual(win.flush_external_edits(), 0,
                         "a second upload started while the first ran")
        model.release.set()
        self.assertTrue(_pump(self._app, lambda: model.body == b"second"),
                        f"puts: {model.puts}")
        self.assertEqual([p[2] for p in model.puts], ["e1", "e2"])

    def test_a_file_that_cannot_be_read_is_logged(self):
        model = _EditModel()
        win, path = self._window(model)
        logged = []
        win.log = logged.append
        self._write(path, b"changed")
        with patch("builtins.open", side_effect=OSError("locked")):
            win._on_edit_file_changed(path)
            win.flush_external_edits()
        self.assertTrue(any("could not read" in m for m in logged), logged)
        self.assertEqual(model.puts, [])

    def test_a_vanished_file_is_not_uploaded(self):
        model = _EditModel()
        win, path = self._window(model)
        os.remove(path)
        win._on_edit_file_changed(path)
        self.assertEqual(win.flush_external_edits(), 0)
        self.assertEqual(model.puts, [])

    def test_a_failed_upload_on_stop_is_reported(self):
        model = _EditModel()
        win, path = self._window(model)
        self._write(path, b"unsent")
        model.put_object_body = Mock(side_effect=RuntimeError("AccessDenied"))
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes), \
                patch.object(external_edit.QMessageBox, "warning") as warn:
            win.stop_external_edits()
        self.assertIn("was not saved", warn.call_args.args[2])

    def test_declining_on_stop_uploads_nothing(self):
        model = _EditModel()
        win, path = self._window(model)
        self._write(path, b"unsent")
        with patch.object(external_edit.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            win.stop_external_edits()
        self.assertEqual(model.puts, [])

    def test_a_cancelled_open_does_nothing(self):
        model = _EditModel()
        settings = QSettings("s3duck-tests-edge2", "s3duck-tests-edge2")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        win.data_model.clone_for_worker = lambda: model
        with patch.object(external_edit, "run_with_progress", return_value=(None, None)):
            self.assertIsNone(win.edit_externally("doc.txt"))
        self.assertEqual(win.external_edits(), {})

    def test_a_late_result_for_a_stopped_edit_is_ignored(self):
        model = _EditModel()
        win, path = self._window(model)
        thread = Mock()
        win.stop_external_edits(ask=False)
        win._on_edit_uploaded(path, (1, 1), "doc.txt", '"e9"', None, thread)


class _CancellableVersions(_PlanModel):
    """A history read that waits, so the dialog can move on meanwhile."""

    def __init__(self, versions):
        super().__init__(versions)
        self.release = threading.Event()
        self.cancelled = []

    def list_prefix_versions(self, prefix, cancel_event=None):
        self.release.wait(5)
        if cancel_event is not None and cancel_event.is_set():
            self.cancelled.append(True)
            raise TransferCancelled("cancelled")
        return self.versions


class PointInTimeEdgeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self, model):
        mw = types.SimpleNamespace(is_read_only=lambda: False,
                                   assign_thread_operation=Mock())
        dlg = PointInTimeDialog(None, mw, model, "p/", versioning="Enabled")
        self.addCleanup(dlg.close)
        dlg.moment.setDateTime(QDateTime.fromSecsSinceEpoch(
            int((T0 + timedelta(minutes=10)).timestamp())))
        return dlg, mw

    def test_a_plan_for_a_moment_since_changed_is_discarded(self):
        model = _CancellableVersions(pit_tests.PlanTests.VERSIONS)
        dlg, _ = self._dialog(model)
        self.assertTrue(dlg.preview())
        self.assertFalse(dlg.preview(), "a second scan started over the first")
        dlg.moment.setDateTime(dlg.moment.dateTime().addSecs(3600))
        model.release.set()
        _pump(self._app, lambda: dlg._thread is None)
        self.assertEqual(dlg.table.rowCount(), 0, "a stale plan was shown")
        self.assertFalse(dlg.run_btn.isEnabled())

    def test_closing_mid_scan_cancels_it(self):
        model = _CancellableVersions(pit_tests.PlanTests.VERSIONS)
        dlg, _ = self._dialog(model)
        dlg.preview()
        threading.Timer(0.05, model.release.set).start()
        dlg.stop_threads()
        self.assertEqual(model.cancelled, [True])
        self.assertIsNone(dlg._thread)

    def test_a_plan_with_nothing_to_do(self):
        dlg, mw = self._dialog(_PlanModel([
            {"key": "k", "version_id": "1", "last_modified": T0,
             "is_latest": True, "is_delete_marker": False, "size": 1}]))
        dlg.preview()
        _pump(self._app, lambda: dlg._thread is None)
        self.assertFalse(dlg.run_btn.isEnabled())
        self.assertIn("1 unchanged", dlg.info.text())
        self.assertFalse(dlg.run())
        mw.assign_thread_operation.assert_not_called()

    def test_a_prefix_walk_stops_on_cancel(self):
        class _Pages:
            def paginate(self, **_kw):
                return [{"Versions": []}, {"Versions": []}]

        model = make_model(bucket="b")
        model._client = types.SimpleNamespace(get_paginator=lambda n: _Pages())
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            model.list_prefix_versions("", cancel_event=cancel)


class RestoreJobEdgeTests(_QueueWindowCase):
    """The restore job is item-resumable: a pause keeps what it did."""

    def test_the_worker_stops_on_cancel_before_the_next_key(self):
        calls = []
        worker = None

        def restore(k, v, **kw):
            calls.append(k)
            worker.cancel()

        source = types.SimpleNamespace(parallel_files=1, make_version_current=restore,
                                       mark_deleted=lambda k: calls.append(("d", k)))
        worker = Worker(source, [("a", RESTORE, "1"), ("b", DELETE, "")])
        outcome = []
        worker.finished.connect(outcome.append)
        worker.restore_versions()
        self.assertEqual(calls, ["a"])
        self.assertEqual(outcome, [True])

    def test_a_paused_restore_resumes_with_the_remaining_keys(self):
        win = self._window()
        reached = threading.Event()
        release = threading.Event()
        restored = []

        class _Model:
            bucket = "bkt"

            def make_version_current(self, key, version_id, log_fn=None):
                if key == "b" and not release.is_set():
                    reached.set()
                    release.wait(5)
                    raise TransferCancelled("cancelled")
                restored.append(key)

            def mark_deleted(self, key):
                restored.append(("deleted", key))

        model = _Model()
        with patch.object(type(win.data_model), "clone_for_worker", lambda _s: model):
            entry = win.assign_thread_operation(
                "restore_versions", [("a", RESTORE, "1"), ("b", RESTORE, "2"),
                                     ("c", DELETE, "")], need_refresh=False)
            self.assertTrue(reached.wait(5))
            win.pause_entry(entry.entry_id)
            release.set()
            self._pump(lambda: entry.status == "paused")
            self.assertEqual(self._stored(win)[0]["job"],
                             [["b", RESTORE, "2"], ["c", DELETE, ""]])
            win.resume_entry(entry.entry_id)
            self._pump(lambda: entry.status == "done")
        self.assertEqual(restored, ["a", "b", ("deleted", "c")])

    def test_pause_all_parks_the_running_job_too(self):
        win = self._window()
        reached = threading.Event()

        class _Model:
            bucket = "bkt"

            def delete(self, key, log_fn=None, cancel_event=None):
                reached.set()
                while not cancel_event.is_set():
                    time.sleep(0.01)
                raise TransferCancelled("cancelled")

        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: _Model()):
            running = win.assign_thread_operation("delete", ["a"], need_refresh=False)
            waiting = win.assign_thread_operation("delete", ["b"], need_refresh=False)
            self.assertTrue(reached.wait(5))
            win.pause_all_transfers()
            self._pump(lambda: running.status == "paused")
        self.assertEqual(waiting.status, "paused")
        self.assertEqual(win._transfer_queue, [], "a job started after pause all")

    def test_a_failing_follow_up_is_logged_and_the_queue_moves_on(self):
        win = self._window()
        logged = []
        win.log = logged.append

        class _Model:
            bucket = "bkt"

            def delete(self, key, **kw):
                pass

        with patch.object(type(win.data_model), "clone_for_worker", lambda _s: _Model()):
            first = win.assign_thread_operation("delete", ["a"], need_refresh=False)
            first.on_finished = Mock(side_effect=RuntimeError("disk full"))
            second = win.assign_thread_operation("delete", ["b"], need_refresh=False)
            self._pump(lambda: second.status == "done")
        self.assertEqual(first.status, "done")
        self.assertTrue(any("follow-up failed: disk full" in m for m in logged), logged)


class SyncDialogEdgeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self, actions):
        mw = types.SimpleNamespace(two_way_state_file=lambda *a: "")
        dlg = SyncDialog(None, mw, types.SimpleNamespace(bucket="b"), "p/")
        self.addCleanup(dlg.close)
        dlg._actions = list(actions)
        dlg._render_plan()
        return dlg

    def test_resolving_leaves_non_conflicts_alone(self):
        dlg = self._dialog([
            {"action": UPLOAD, "rel": "a", "size": 1, "reason": "new locally"},
            {"action": CONFLICT, "rel": "b", "size": 1, "reason": "changed on both sides"}])
        dlg._table.selectAll()
        self.assertEqual(dlg.resolve_selected("remote"), 1)
        self.assertEqual([a["action"] for a in dlg._actions], [UPLOAD, "download"])
        dlg._table.clearSelection()
        self.assertEqual(dlg.resolve_selected("local"), 0)

    def test_skipped_keys_are_named_in_the_summary(self):
        dlg = self._dialog([])
        dlg._skipped_keys = ["/x", "a\\b"]
        dlg._render_plan()
        self.assertIn("2 object(s) skipped", dlg._info.text())


class ThumbnailEdgeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_an_image_that_did_not_decode_is_not_cached(self):
        settings = QSettings("s3duck-tests-edge3", "s3duck-tests-edge3")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        win._thumb_targets = {"k.png": ("b", "k.png", "e")}
        win._on_thumbnail_loaded(win._thumb_loader.generation, "k.png", None)
        self.assertEqual(len(win._thumb_cache), 0)
        self.assertNotIn("k.png", win._thumb_targets)
        win._on_thumbnail_loaded(win._thumb_loader.generation, "unknown.png",
                                 object())
        self.assertEqual(len(win._thumb_cache), 0)

    def test_a_truncated_body_gives_no_thumbnail(self):
        loader = ThumbnailLoader()
        self.addCleanup(loader.shutdown)
        got = []
        loader.loaded.connect(lambda g, k, image: got.append(image))
        loader.request(types.SimpleNamespace(get_object_preview=lambda k, c: {
            "data": b"\x89PNG partial", "truncated": True}), "big.png")
        _pump(self._app, lambda: got)
        self.assertEqual(got, [None])


if __name__ == "__main__":
    unittest.main()
