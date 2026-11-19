"""Regressions for the deep review round: the transfer queue, the trash,
external editing and the workers. Companion to test_deep_review_fixes.py,
which holds the ones that need no window.
"""

import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import botocore.exceptions
import main_window
import ui.window.external_edit as external_edit
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox
from tests.support import (
    PROJECT_ROOT, FakeS3Client, _StubModel, _ensure_qapp, make_main_window,
    make_model, patch_app_wide,
)
from tests.unit.test_external_edit_and_thumbnails import _EditModel, _inline
from workers.transfer import Worker


class _Store:
    """An in-memory bucket with the Model methods move and delete call."""

    bucket = "bkt"

    def __init__(self, keys):
        self.objs = dict.fromkeys(keys, b"x")

    def copy_object(self, src, dst, dst_bucket=None, **_kw):
        self.objs[dst] = self.objs[src]

    def copy_prefix(self, src, dst, dst_bucket=None, **_kw):
        keys = [k for k in self.objs if k.startswith(src)]
        for k in keys:
            self.objs[dst + k[len(src):]] = self.objs[k]
        return keys

    def delete(self, key, **_kw):
        if key.endswith("/"):
            for k in [k for k in self.objs if k.startswith(key)]:
                del self.objs[k]
        else:
            self.objs.pop(key, None)

    def delete_exact(self, keys, **_kw):
        for k in keys:
            self.objs.pop(k, None)


class _WindowCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests-deep", "s3duck-tests-deep")
        self.settings.clear()
        self.addCleanup(self.settings.clear)

    def _window(self, **kw):
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, self.settings, **kw)
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        return win

    def _pump(self, pred, timeout=5):
        end = time.monotonic() + timeout
        while not pred() and time.monotonic() < end:
            self.app.processEvents()
            time.sleep(0.005)
        self.app.processEvents()
        return pred()


class TrashNeverEatsWhatItJustTookTests(_WindowCase):
    """REGRESSION (everything selected gone for good): with the trash on, a
    selection that included ".trash/" itself -- Ctrl+A at the bucket root --
    queued the move into the trash and THEN the delete of the trash, under
    a prompt promising the objects could be restored."""

    def test_a_selection_holding_the_trash_keeps_what_it_moved(self):
        win = self._window()
        win.set_trash_enabled(True)
        store = _Store(["report.txt", "photos/1.jpg", ".trash/old.txt"])
        win._destination_conflicts = Mock(return_value=set())
        win._selected_storage_classes = lambda: {}
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: store):
            self.assertEqual(
                win.queue_delete(["report.txt", "photos/", ".trash/"]),
                (2, 1))
            self.assertTrue(self._pump(
                lambda: win._active_entry is None and not win._transfer_queue
                and all(e.status == "done"
                        for e in win._queue_entries.values())))
        self.assertEqual(sorted(store.objs),
                         [".trash/photos/1.jpg", ".trash/report.txt"])


class RetryKeepsTheFollowUpTests(_WindowCase):
    """REGRESSION (every file a conflict on the next run): a retried job
    did not inherit on_finished, so a two-way sync that succeeded on its
    second attempt never wrote its state."""

    def test_a_two_way_sync_records_its_state_after_a_retry(self):
        win = self._window()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        win.TWO_WAY_STATE_DIR = os.path.join(tmp.name, "state")
        local = os.path.join(tmp.name, "l")
        os.makedirs(local)
        with open(os.path.join(local, "a"), "wb") as handle:
            handle.write(b"NEW")
        calls = []

        class _Flaky:
            bucket = "bkt"
            parallel_files = 1

            def upload_file(self, *_a, **_kw):
                calls.append(1)
                if len(calls) == 1:
                    raise RuntimeError("boom")

        recorded = []
        win._record_two_way_state = lambda *a, **_kw: recorded.append(a)
        with patch.object(type(win.data_model), "clone_for_worker",
                          lambda _s: _Flaky()), \
                patch_app_wide("QMessageBox"):
            entry = win.start_two_way_sync(
                [{"action": "upload", "rel": "a", "size": 3}],
                local, "p/", {})
            self.assertTrue(self._pump(lambda: entry.status == "error"))
            win._on_queue_retry_requested(entry.entry_id)
            fresh = max(win._queue_entries.values(),
                        key=lambda e: e.entry_id)
            self.assertIsNot(fresh, entry)
            self.assertTrue(self._pump(lambda: fresh.status == "done"))
        self.assertIsNotNone(fresh.on_finished)
        self.assertTrue(recorded, "the successful retry recorded no state")


class RefreshedCredentialsReachQueuedJobsTests(_WindowCase):
    """REGRESSION (a retry that could only fail): each queued job holds a
    model cloned when it was queued, and a credential refresh changed only
    the window's. The retry of a job that died on ExpiredToken ran with the
    expired token."""

    MINTED = {"access_key": "NEW-AK", "secret_key": "NEW-SK",
              "session_token": "NEW-TOKEN", "expires": ""}

    def _refresh(self, win):
        with patch.object(type(win), "_mint_credentials",
                          lambda _s: dict(self.MINTED)), \
                patch.object(type(win), "_persist_refreshed_credentials",
                             lambda _s, _m: None):
            self.assertTrue(win.refresh_credentials())

    def test_a_failed_job_is_retried_with_the_new_keys(self):
        win = self._window()
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: started.append(e)):
            entry = win.assign_thread_operation(
                "delete", ["a"], need_refresh=False)
            entry.status = "error"
            self._refresh(win)
            win._on_queue_retry_requested(entry.entry_id)
        model = win._worker_model_for(started[-1])
        self.assertEqual((model.access_key, model.secret_key,
                          model.session_token),
                         ("NEW-AK", "NEW-SK", "NEW-TOKEN"))

    def test_another_profiles_model_is_left_alone(self):
        win = self._window()
        other = make_model(access_key="OTHER-AK", secret_key="OTHER-SK")
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: None):
            entry = win.assign_thread_operation(
                "delete", ["a"], need_refresh=False)
            entry.dest_model = other
            self._refresh(win)
        self.assertEqual(other.access_key, "OTHER-AK")
        self.assertEqual(entry.source_model.access_key, "NEW-AK")


class DeclinedRestoreKeepsTheOfferTests(_WindowCase):
    """REGRESSION (saved work silently erased): Restore cleared the pending
    records before queueing them. Declining the credentials prompt left
    nothing queued and nothing offered, and the next queue write removed
    the jobs from disk."""

    def test_jobs_that_could_not_be_queued_are_still_offered_and_stored(self):
        win = self._window()
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win.assign_thread_operation(
                "delete", ["a", "b"], need_refresh=False)
        win.close()
        again = self._window()
        self.assertTrue(again._queue_panel.restore_offer_visible())
        again._credentials_ok_for_transfer = lambda: False
        self.assertEqual(again.restore_pending_queue(), 0)
        self.assertTrue(again._queue_panel.restore_offer_visible())
        self.assertEqual(len(again._pending_restore), 1)
        # A job queued meanwhile must not write the offered ones away.
        again._credentials_ok_for_transfer = lambda: True
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: None):
            again.assign_thread_operation(
                "download", [("k", "/tmp/x", 1, "/tmp")], need_refresh=False)
        self.assertIn("delete",
                      [r["method"] for r in again.load_pending_queue()])
        # ... and a second try, with credentials, restores them.
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: None):
            self.assertEqual(again.restore_pending_queue(), 1)
        self.assertFalse(again._queue_panel.restore_offer_visible())


class ExternalEditTests(_WindowCase):
    """REGRESSIONS in "Edit in external app": a save stripped the object's
    metadata, class and tags; and an upload refused while the window was
    closing lost the user's edits with the temp folder."""

    def _editing(self, model):
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, self.settings)
        win.data_model.bucket = "bkt"
        win.data_model.clone_for_worker = lambda: model
        self.rescue = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.rescue, ignore_errors=True)
        win._unsaved_edits_dir = lambda: self.rescue
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QDesktopServices, "openUrl"):
            path = win.edit_externally("doc.txt")
        time.sleep(0.01)
        with open(path, "wb") as handle:
            handle.write(b"two hours of edits")
        return win, path

    def _close(self, win):
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes), \
                patch.object(external_edit.QMessageBox, "warning") as warn:
            win.close()
        return warn

    def test_edits_refused_at_close_are_kept_and_the_user_is_told_where(self):
        model = _EditModel()
        win, _path = self._editing(model)
        model.etag = "someone-else"          # changed on the server meanwhile
        warn = self._close(win)
        kept = os.listdir(self.rescue)
        self.assertEqual(len(kept), 1)
        with open(os.path.join(self.rescue, kept[0]), "rb") as handle:
            self.assertEqual(handle.read(), b"two hours of edits")
        self.assertIn(os.path.join(self.rescue, kept[0]),
                      warn.call_args.args[2])

    def test_a_save_that_succeeds_leaves_nothing_behind(self):
        model = _EditModel()
        win, _path = self._editing(model)
        warn = self._close(win)
        self.assertFalse(warn.called)
        self.assertEqual(os.listdir(self.rescue), [])
        self.assertEqual(model.body, b"two hours of edits")

    def test_every_save_route_asks_to_keep_the_objects_properties(self):
        seen = []

        class _Recording(_EditModel):
            def put_object_body(self, key, data, **kw):
                seen.append(kw.get("preserve_properties"))
                return super().put_object_body(key, data, **kw)

        win, _path = self._editing(_Recording())
        self._close(win)
        self.assertEqual(seen, [True])


class GlacierRestoreFailuresTests(unittest.TestCase):
    """REGRESSION (a failed job reported done): restore_object answers
    (False, reason) for any refusal, and the worker counted all of them as
    "skipped". A job whose every request was AccessDenied ended green."""

    @classmethod
    def setUpClass(cls):
        cls.app = _ensure_qapp()

    def _run(self, code):
        m = make_model(bucket="bkt")
        m._client = FakeS3Client(restore_error=botocore.exceptions.ClientError(
            {"Error": {"Code": code, "Message": "no"}}, "RestoreObject"))
        worker = Worker(m, [("a.bin", False, 7, "Standard"),
                            ("b.bin", False, 7, "Standard")])
        out = {"error": [], "done": []}
        worker.error.connect(out["error"].append)
        worker.item_done.connect(out["done"].append)
        worker.restore()
        return out

    def test_a_refusal_is_a_failure_and_its_rows_are_not_finished(self):
        out = self._run("AccessDenied")
        self.assertEqual(out["done"], [])
        self.assertEqual(len(out["error"]), 1)
        self.assertIn("2 object(s) could not be restored", out["error"][0])
        self.assertIn("a.bin", out["error"][0])

    def test_nothing_to_do_is_still_only_a_skip(self):
        for code in ("RestoreAlreadyInProgress", "InvalidObjectState"):
            out = self._run(code)
            self.assertEqual(out["error"], [], code)
            self.assertEqual(out["done"], [0, 1], code)


if __name__ == "__main__":
    unittest.main()
