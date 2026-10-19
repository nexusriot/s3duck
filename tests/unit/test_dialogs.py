"""ui/dialogs/."""

import ast
import io
import main_window
import os
import tempfile
import threading
import time
import types
import ui.dialogs.search as search_dialog_module
import unittest
import utils
from PyQt6.QtCore import QEvent, QSettings, Qt
from PyQt6.QtGui import QKeyEvent
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QDialog, QDialogButtonBox, QLabel, QMenu,
    QMessageBox, QTableWidget,
)
from datetime import datetime, timedelta, timezone
from main_window import (
    BulkRenameDialog, CopyMoveDialog, CrossProfileSyncDialog,
    IncompleteUploadsDialog, MetadataDialog, OverwriteDialog,
    PresignedLinkDialog, SyncDialog, TagsDialog, TransferSettingsDialog,
    VersionsDialog,
)
from model import Model, TransferCancelled
from properties_window import PropertiesWindow
from tests.support import (
    FakeS3Client, PROJECT_ROOT, _FakeDuplicateModel, _FakeMainWindow,
    _FakePresignModel, _FakePropsModel, _FakeSyncModel, _FakeUploadsModel,
    _FakeVersionsModel, _StubModel, _dt, _ensure_qapp, make_main_window,
    make_model, read_app_sources,
)
from unittest.mock import Mock, patch


class SearchRegexValidationTests(unittest.TestCase):
    """REGRESSION (found while splitting main_window.py): _run_search called
    ``Model.build_search_matcher`` to validate the regex before starting, but
    main_window.py imported the class as ``DataModel`` and never bound the
    bare name. So the validation raised NameError, which the surrounding
    ``except ValueError`` did not catch, and a bad regex took the dialog's
    slot down instead of printing a message. The suite never saw it because
    every test that touches this dialog patches _run_search out."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        dlg = main_window.SearchDialog(win, win, win.data_model, "logs/")
        self.addCleanup(dlg.close)
        return dlg

    def test_the_validating_name_is_actually_bound(self):
        """The import, not the behaviour -- this is what was missing."""
        self.assertTrue(hasattr(search_dialog_module, "Model"))
        self.assertIs(search_dialog_module.Model, Model)

    def test_a_bad_regex_is_reported_and_does_not_start_a_search(self):
        dlg = self._dialog()
        dlg._query.setText("[unclosed")
        dlg._regex.setChecked(True)
        started = []
        with patch.object(main_window.SearchDialog, "_on_results",
                          lambda *a, **kw: started.append(True)):
            dlg._run_search()          # must not raise
        self.assertIn("Invalid regular expression", dlg._info.text())
        self.assertEqual(started, [])
        # The button is only disabled once a search is genuinely under way.
        self.assertTrue(dlg._btn_search.isEnabled())

    def test_a_good_regex_passes_validation(self):
        dlg = self._dialog()
        dlg._query.setText("log[0-9]+")
        dlg._regex.setChecked(True)
        # The dialog's own close(), registered above, joins the search thread.
        dlg._run_search()
        self.assertNotIn("Invalid regular expression", dlg._info.text())
class PresignedLinkDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_default_is_one_hour_get(self):
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        self.assertEqual(dlg._expires_sec(), 3600)          # 1 Hour default
        self.assertEqual(model.calls[-1], ("get", "a/f.txt", 3600))
        self.assertTrue(dlg._url.text().startswith("https://get/"))

    def test_switch_to_put(self):
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        dlg._type.setCurrentIndex(1)  # Upload (PUT) -> triggers regenerate
        self.assertEqual(model.calls[-1][0], "put")
        self.assertTrue(dlg._url.text().startswith("https://put/"))

    def test_expiry_units_and_clamp(self):
        model = _FakePresignModel()
        dlg = PresignedLinkDialog(None, model, "a/f.txt")
        dlg._unit.setCurrentIndex(2)   # Days
        dlg._amount.setValue(2)
        self.assertEqual(dlg._expires_sec(), 172800)        # 2 days
        self.assertEqual(model.calls[-1][2], 172800)
        # 10 days exceeds the 7-day SigV4 cap -> clamped in the generated call
        dlg._amount.setValue(10)
        self.assertEqual(model.calls[-1][2], PresignedLinkDialog.MAX_EXPIRES)
class CopyMoveDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        bucket = "src"

        def clone_for_worker(self):
            return self

        def list_buckets(self):
            raise RuntimeError("offline")  # dialog must still work

    def _dlg(self):
        dlg = CopyMoveDialog(None, self.FakeModel(), 2, "a/")
        # The dialog loads buckets on a QThread; close() joins it so the
        # thread never outlives the widget.
        self.addCleanup(dlg.close)
        return dlg

    def test_defaults_to_source_bucket(self):
        dlg = self._dlg()
        self.assertEqual(dlg.destination_bucket(), "src")
        self.assertFalse(dlg.is_cross_bucket())
        self.assertEqual(dlg.destination(), "a/")

    def test_typing_another_bucket_is_cross_bucket(self):
        dlg = self._dlg()
        dlg.bucket_combo.setCurrentText("other")
        self.assertTrue(dlg.is_cross_bucket())
        self.assertEqual(dlg.destination_bucket(), "other")

    def test_blank_bucket_falls_back_to_source(self):
        dlg = self._dlg()
        dlg.bucket_combo.setCurrentText("   ")
        self.assertEqual(dlg.destination_bucket(), "src")
        self.assertFalse(dlg.is_cross_bucket())
class IncompleteUploadsDialogTests(unittest.TestCase):
    """The dialog loads on a worker thread, so each test pumps the event loop
    until the load settles."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model, prefix=""):
        dlg = IncompleteUploadsDialog(None, _FakeMainWindow(), model, prefix)
        self.addCleanup(dlg.close)
        self._settle(dlg)
        return dlg

    def _uploads(self):
        return [
            {"key": "a/one.bin", "upload_id": "u1", "initiated": _dt(1),
             "storage_class": "STANDARD", "size": 1024},
            {"key": "a/two.bin", "upload_id": "u2", "initiated": _dt(2),
             "storage_class": "STANDARD", "size": 2048},
        ]

    def test_lists_uploads_and_reports_wasted_bytes(self):
        dlg = self._dlg(_FakeUploadsModel(self._uploads()))
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertIn("2 incomplete upload(s)", dlg._info.text())
        self.assertIn("3.0 KB", dlg._info.text())  # 1024 + 2048 still billed

    def test_empty_state(self):
        dlg = self._dlg(_FakeUploadsModel([]))
        self.assertEqual(dlg._table.rowCount(), 0)
        self.assertIn("No incomplete multipart uploads", dlg._info.text())

    def test_load_failure_is_reported_not_raised(self):
        dlg = self._dlg(_FakeUploadsModel(fail=True))
        self.assertIn("Could not list", dlg._info.text())
        self.assertEqual(dlg._table.rowCount(), 0)

    def test_abort_selected_calls_model_for_each_row(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        dlg._table.selectAll()
        with patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.Yes):
            dlg._abort_selected()
        self._settle(dlg)
        self.assertEqual(sorted(model.aborted),
                         [("a/one.bin", "u1"), ("a/two.bin", "u2")])

    def test_abort_is_cancellable(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        dlg._table.selectAll()
        with patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.No):
            dlg._abort_selected()
        self.assertEqual(model.aborted, [])

    def test_abort_older_than_filters_by_age(self):
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        uploads = [
            {"key": "old.bin", "upload_id": "u1", "initiated": _dt(1),
             "storage_class": "", "size": 10},
            {"key": "recent.bin", "upload_id": "u2", "initiated": recent,
             "storage_class": "", "size": 10},
        ]
        model = _FakeUploadsModel(uploads)
        dlg = self._dlg(model)
        with patch("main_window.QInputDialog.getInt", return_value=(7, True)), \
             patch("main_window.QMessageBox.question",
                   return_value=QMessageBox.StandardButton.Yes):
            dlg._abort_older_than()
        self._settle(dlg)
        # only the 2026-01-01 upload is older than 7 days
        self.assertEqual(model.aborted, [("old.bin", "u1")])

    def test_scan_receives_a_cancel_event(self):
        model = _FakeUploadsModel(self._uploads())
        dlg = self._dlg(model)
        self.assertTrue(model.cancel_events)
        self.assertIsNotNone(model.cancel_events[0])

    def test_closing_mid_scan_cancels_instead_of_orphaning_the_thread(self):
        """Sizing each upload costs a ListParts call, so a scan can run for
        minutes. Closing must signal cancellation; otherwise the join times out
        and the dialog is destroyed with its thread still running (SIGABRT)."""
        started = threading.Event()
        captured = []

        class Blocking(_FakeUploadsModel):
            def list_multipart_uploads(self, prefix="", with_sizes=False,
                                       cancel_event=None, **_kw):
                captured.append(cancel_event)
                started.set()
                while not cancel_event.is_set():   # emulate a long scan
                    time.sleep(0.005)
                raise TransferCancelled("cancelled")

        dlg = IncompleteUploadsDialog(None, _FakeMainWindow(), Blocking([]), "")
        self.addCleanup(dlg.close)
        self.assertTrue(started.wait(5))
        self.assertFalse(captured[0].is_set())

        began = time.monotonic()
        dlg.close()
        elapsed = time.monotonic() - began
        self.assertTrue(captured[0].is_set())
        # a cancelled scan joins promptly; a timed-out join would take ~2s
        self.assertLess(elapsed, 1.5)

    def test_prefix_scope_limits_the_scan(self):
        uploads = self._uploads() + [
            {"key": "elsewhere/x.bin", "upload_id": "u9", "initiated": _dt(3),
             "storage_class": "", "size": 5},
        ]
        dlg = self._dlg(_FakeUploadsModel(uploads), prefix="a/")
        self.assertTrue(dlg._scope.isChecked())
        self.assertEqual(dlg._table.rowCount(), 2)  # 'elsewhere/' excluded
class VersionsDialogAsyncTests(unittest.TestCase):
    """REGRESSION: the version list and bucket versioning status were fetched
    on the main thread, freezing the dialog on buckets with many versions."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._list_thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model):
        dlg = VersionsDialog(None, _FakeMainWindow(), model, "a/f.txt")
        self.addCleanup(dlg.close)
        return dlg

    def test_rows_appear_after_the_background_load(self):
        versions = [
            {"version_id": "v2", "last_modified": _dt(2), "size": 20,
             "is_latest": True, "storage_class": "STANDARD",
             "is_delete_marker": False, "etag": "e2"},
            {"version_id": "v1", "last_modified": _dt(1), "size": 10,
             "is_latest": False, "storage_class": "STANDARD",
             "is_delete_marker": False, "etag": "e1"},
        ]
        dlg = self._dlg(_FakeVersionsModel(versions))
        # constructor must not block on the network
        self.assertEqual(dlg._info.text(), "Loading versions…")
        self._settle(dlg)
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertIn("2 version(s)", dlg._info.text())
        self.assertIn("Enabled", dlg._info.text())

    def test_load_failure_renders_empty(self):
        dlg = self._dlg(_FakeVersionsModel(fail=True))
        with patch("main_window.QMessageBox.warning", return_value=None):
            self._settle(dlg)
        self.assertEqual(dlg._table.rowCount(), 0)
        self.assertEqual(dlg._versions, [])
class PropertiesWindowTests(unittest.TestCase):
    """REGRESSION: Properties recursively listed the prefix on the main thread
    (freezing the app on large folders) and re-fetched a file's size that
    head_object had already returned."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._size_thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, model, key):
        dlg = PropertiesWindow(None, settings=(model, key))
        self.addCleanup(dlg.close)
        return dlg

    def test_file_size_comes_from_head_without_a_listing(self):
        model = _FakePropsModel(head={"ContentLength": 4096, "ETag": '"abc"'})
        dlg = self._dlg(model, "a/f.txt")
        self.assertEqual(dlg.size.text(), "4096 Bytes")
        self.assertEqual(model.head_calls, ["a/f.txt"])
        self.assertEqual(model.size_calls, [])  # no extra recursive listing
        self.assertEqual(dlg.eTag.text(), "abc")

    def test_escape_joins_the_probes_the_way_closing_does(self):
        """FINDING: the joins lived in closeEvent, which Escape never fires,
        so one keystroke left both probe threads running and still parented to
        the dialog — destroyed with the parent, which aborts the process."""
        model = _FakePropsModel(size=1234)
        dlg = self._dlg(model, "a/dir/")
        self.assertIsNotNone(dlg._size_thread)
        QApplication.sendEvent(dlg, QKeyEvent(
            QEvent.Type.KeyPress, Qt.Key.Key_Escape,
            Qt.KeyboardModifier.NoModifier))
        self._app.processEvents()
        self.assertIsNone(dlg._size_thread)
        self.assertIsNone(dlg._lock_thread)

    def test_rejecting_joins_them_too(self):
        model = _FakePropsModel(size=1234)
        dlg = self._dlg(model, "a/dir/")
        dlg.reject()
        self._app.processEvents()
        self.assertIsNone(dlg._size_thread)

    def test_folder_size_is_computed_off_the_main_thread(self):
        model = _FakePropsModel(size=1234)
        dlg = self._dlg(model, "a/dir/")
        # must not block the constructor
        self.assertEqual(dlg.size.text(), "Calculating…")
        self.assertEqual(model.head_calls, [])  # folders have no object to HEAD
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "1234 Bytes")
        self.assertEqual(model.size_calls, ["a/dir/"])

    def test_missing_object_falls_back_to_a_listing(self):
        model = _FakePropsModel(head_error=True, size=77)
        dlg = self._dlg(model, "a/implicit")
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "77 Bytes")

    def _settle_lock(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._lock_thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def test_object_lock_is_probed_off_the_main_thread(self):
        """Two more round trips per object, so it must not join the two the
        constructor already blocks on."""
        model = _FakePropsModel(
            head={"ContentLength": 1, "ETag": '"a"'},
            lock={"supported": True, "mode": "GOVERNANCE",
                  "retain_until": "2027-01-01", "legal_hold": "ON"})
        dlg = self._dlg(model, "a/f.txt")
        self.assertEqual(dlg.objectLock.text(), "…")
        self._settle_lock(dlg)
        self.assertEqual(model.lock_calls, ["a/f.txt"])
        self.assertIn("GOVERNANCE", dlg.objectLock.text())
        self.assertIn("legal hold ON", dlg.objectLock.text())
        self.assertIsNone(dlg._lock_thread)

    def test_a_folder_is_never_probed_for_a_lock(self):
        model = _FakePropsModel(size=10)
        dlg = self._dlg(model, "a/dir/")
        self._settle(dlg)
        self.assertEqual(model.lock_calls, [])
        self.assertEqual(dlg.objectLock.text(), "—")

    def test_a_bucket_without_object_lock_shows_a_dash(self):
        model = _FakePropsModel(head={"ContentLength": 1})
        dlg = self._dlg(model, "a/f.txt")
        self._settle_lock(dlg)
        self.assertEqual(dlg.objectLock.text(), "—")
        self.assertIsNone(dlg._lock_thread)

    def test_size_failure_reports_na(self):
        class Failing(_FakePropsModel):
            def get_size(self, key):
                raise RuntimeError("denied")

        dlg = self._dlg(Failing(head_error=True), "a/dir/")
        self._settle(dlg)
        self.assertEqual(dlg.size.text(), "N/A")
class DialogLoadFailureTests(unittest.TestCase):
    """REGRESSION: when the initial load fails, Save must be disabled —
    saving would replace the object's real tags/metadata with the empty
    dialog contents."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_tags_dialog_disables_save_on_load_failure(self):
        class FailingModel:
            def get_object_tags(self, key):
                raise RuntimeError("boom")

        with patch("main_window.QMessageBox.warning", return_value=None):
            dlg = TagsDialog(None, FailingModel(), "a/f.txt")
        save = dlg._btns.button(QDialogButtonBox.StandardButton.Save)
        self.assertFalse(save.isEnabled())

    def test_tags_dialog_save_enabled_on_success(self):
        class OkModel:
            def get_object_tags(self, key):
                return [{"Key": "k", "Value": "v"}]

        dlg = TagsDialog(None, OkModel(), "a/f.txt")
        save = dlg._btns.button(QDialogButtonBox.StandardButton.Save)
        self.assertTrue(save.isEnabled())
        self.assertEqual(dlg._table.rowCount(), 1)

    def test_metadata_dialog_disables_save_on_load_failure(self):
        class FailingModel:
            def get_object_metadata(self, key):
                raise RuntimeError("boom")

        with patch("main_window.QMessageBox.warning", return_value=None):
            dlg = MetadataDialog(None, FailingModel(), "a/f.txt")
        save = dlg.buttonBox.button(QDialogButtonBox.StandardButton.Save)
        self.assertFalse(save.isEnabled())
class TransferSettingsDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self, **kw):
        opts = dict(
            concurrency=4, max_concurrency=16,
            storage_classes=("",) + tuple(Model.STORAGE_CLASSES),
            sse_modes=Model.SSE_MODES,
        )
        opts.update(kw)
        dlg = TransferSettingsDialog(None, **opts)
        self.addCleanup(dlg.close)
        return dlg

    def test_round_trips_existing_values(self):
        dlg = self._dlg(concurrency=7, storage_class="GLACIER", sse="AES256")
        self.assertEqual(dlg.concurrency(), 7)
        self.assertEqual(dlg.storage_class(), "GLACIER")
        self.assertEqual(dlg.sse(), "AES256")

    def test_kms_field_only_enabled_for_kms(self):
        dlg = self._dlg(sse="AES256", kms_key_id="key-1")
        self.assertFalse(dlg._kms.isEnabled())
        self.assertEqual(dlg.kms_key_id(), "")   # dropped for non-kms mode
        dlg._sse.setCurrentIndex(dlg._sse.findData("aws:kms"))
        self.assertTrue(dlg._kms.isEnabled())
        self.assertEqual(dlg.kms_key_id(), "key-1")

    def test_defaults_are_empty(self):
        dlg = self._dlg()
        self.assertEqual(dlg.storage_class(), "")
        self.assertEqual(dlg.sse(), "")

    def test_parallel_files_and_verify_round_trip(self):
        dlg = self._dlg(parallel_files=9, verify_downloads=True)
        self.assertEqual(dlg.parallel_files(), 9)
        self.assertTrue(dlg.verify_downloads())
        dlg._verify.setChecked(False)
        self.assertFalse(dlg.verify_downloads())
class DialogDismissTests(unittest.TestCase):
    """FINDING (crash + data loss): every dialog here joined its worker
    threads in closeEvent, and Escape/reject()/accept() never raise a close
    event — so one keystroke skipped the join and left a running QThread
    parented to a dialog, and walked straight past PreviewDialog's
    unsaved-changes prompt."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class _Dlg(utils.DialogDismissMixin, QDialog):
        def __init__(self, veto=False):
            super().__init__()
            self.veto = veto
            self.stops = 0
            self.asked = 0
            self.finished_count = 0
            self.finished.connect(self._count)

        def _count(self, _result):
            self.finished_count += 1

        def confirm_dismiss(self):
            self.asked += 1
            return not self.veto

        def stop_threads(self):
            self.stops += 1

    @staticmethod
    def _escape(dlg):
        QApplication.sendEvent(dlg, QKeyEvent(
            QEvent.Type.KeyPress, Qt.Key.Key_Escape,
            Qt.KeyboardModifier.NoModifier))

    def _routes(self):
        return (("close", lambda d: d.close()),
                ("reject", lambda d: d.reject()),
                ("accept", lambda d: d.accept()),
                ("escape", self._escape))

    def test_every_way_out_runs_the_cleanup_exactly_once(self):
        for name, dismiss in self._routes():
            with self.subTest(route=name):
                dlg = self._Dlg()
                self.addCleanup(dlg.close)
                dlg.show()
                dismiss(dlg)
                self._app.processEvents()
                self.assertEqual(dlg.stops, 1)
                self.assertFalse(dlg.isVisible())

    def test_no_way_out_emits_finished_twice(self):
        """Cleanup must not be bought by firing the dialog's own signal
        again — callers connect to finished."""
        for name, dismiss in self._routes():
            with self.subTest(route=name):
                dlg = self._Dlg()
                self.addCleanup(dlg.close)
                dlg.show()
                dismiss(dlg)
                self._app.processEvents()
                self.assertEqual(dlg.finished_count, 1)

    def test_refusing_blocks_every_way_out(self):
        """The unsaved-work prompt was escapable; now it is not."""
        for name, dismiss in self._routes():
            with self.subTest(route=name):
                dlg = self._Dlg(veto=True)
                dlg.show()
                dismiss(dlg)
                self._app.processEvents()
                self.assertEqual(dlg.stops, 0)
                self.assertTrue(dlg.isVisible())
                dlg.veto = False
                dlg.close()

    def test_the_question_is_asked_once_per_dismissal(self):
        """Qt's own QDialog.closeEvent calls reject(), which re-enters done()
        while the first pass is still on the stack — an unguarded hook would
        prompt twice."""
        for name, dismiss in self._routes():
            with self.subTest(route=name):
                dlg = self._Dlg()
                self.addCleanup(dlg.close)
                dlg.show()
                dismiss(dlg)
                self._app.processEvents()
                self.assertEqual(dlg.asked, 1)

    def test_accepting_still_reports_its_result(self):
        dlg = self._Dlg()
        self.addCleanup(dlg.close)
        dlg.show()
        dlg.accept()
        self.assertEqual(dlg.result(), QDialog.DialogCode.Accepted)
class DialogThreadCleanupContractTests(unittest.TestCase):
    """A new dialog must not be able to reintroduce the Escape hole."""

    ROOT = PROJECT_ROOT

    def _dialogs_that_join(self):
        found = []
        for name, text in read_app_sources(self.ROOT):
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                bases = [ast.unparse(base) for base in node.bases]
                source = ast.unparse(node)
                if "QDialog" in bases and "join_qthread" in source:
                    found.append((name, node, bases))
        return found

    def test_the_audit_finds_the_dialogs_it_is_meant_to_guard(self):
        """A scan that silently matches nothing proves nothing."""
        names = {node.name for _f, node, _b in self._dialogs_that_join()}
        self.assertIn("PropertiesWindow", names)
        self.assertIn("PreviewDialog", names)
        self.assertGreaterEqual(len(names), 10)

    def test_every_thread_joining_dialog_cleans_up_on_every_exit(self):
        for filename, node, bases in self._dialogs_that_join():
            with self.subTest(dialog=node.name):
                methods = {m.name for m in node.body
                           if isinstance(m, ast.FunctionDef)}
                self.assertIn(
                    "DialogDismissMixin", bases,
                    f"{filename}:{node.name} joins worker threads but does "
                    "not mix in DialogDismissMixin, so Escape skips the join")
                self.assertIn(
                    "stop_threads", methods,
                    f"{filename}:{node.name} mixes the guard in but never "
                    "overrides stop_threads, so it cleans up nothing")

    def test_none_of_them_hide_the_cleanup_in_close_event(self):
        """closeEvent is the door Escape does not use."""
        for filename, node, _bases in self._dialogs_that_join():
            for member in node.body:
                if (isinstance(member, ast.FunctionDef)
                        and member.name == "closeEvent"):
                    with self.subTest(dialog=node.name):
                        self.assertNotIn(
                            "join_qthread", ast.unparse(member),
                            f"{filename}:{node.name} joins in closeEvent, "
                            "which Escape never reaches")
class OverwriteDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_choices(self):
        dlg = OverwriteDialog(None, ["a", "b"], total=5, what="file")
        self.addCleanup(dlg.close)
        self.assertIsNone(dlg.choice())
        dlg._pick(OverwriteDialog.OVERWRITE)
        self.assertEqual(dlg.choice(), OverwriteDialog.OVERWRITE)
class BulkRenameDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self, items):
        dlg = BulkRenameDialog(None, items)
        self.addCleanup(dlg.close)
        return dlg

    def _ok(self, dlg):
        return dlg._btns.button(QDialogButtonBox.StandardButton.Ok)

    def test_ok_disabled_until_something_changes(self):
        dlg = self._dlg([("a.txt", False)])
        self.assertFalse(self._ok(dlg).isEnabled())
        dlg._find.setText("a")
        dlg._replace.setText("b")
        self.assertTrue(self._ok(dlg).isEnabled())
        self.assertEqual(dlg.plan(), [("a.txt", "b.txt")])

    def test_preview_table_mirrors_the_plan(self):
        dlg = self._dlg([("x1.log", False), ("x2.log", False)])
        dlg._find.setText("x")
        dlg._replace.setText("y")
        self.assertEqual(dlg._table.rowCount(), 2)
        self.assertEqual(dlg._table.item(0, 1).text(), "y1.log")

    def test_ok_blocked_on_duplicate_targets(self):
        dlg = self._dlg([("a.txt", False), ("b.txt", False)])
        dlg._mode_template.setChecked(True)
        dlg._template.setText("same.txt")
        self.assertFalse(self._ok(dlg).isEnabled())
        self.assertIn("Cannot apply", dlg._info.text())

    def test_invalid_regex_blocks_ok(self):
        dlg = self._dlg([("a.txt", False)])
        dlg._regex.setChecked(True)
        dlg._find.setText("([")
        self.assertFalse(self._ok(dlg).isEnabled())
        self.assertIn("Invalid regular expression", dlg._info.text())

    def test_mode_switch_toggles_the_forms(self):
        dlg = self._dlg([("a.txt", False)])
        self.assertTrue(dlg._find_box.isVisibleTo(dlg))
        self.assertFalse(dlg._tpl_box.isVisibleTo(dlg))
        dlg._mode_template.setChecked(True)
        self.assertFalse(dlg._find_box.isVisibleTo(dlg))
        self.assertTrue(dlg._tpl_box.isVisibleTo(dlg))
class SyncDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, remote=None, prefix="pre/"):
        mw = _FakeMainWindow()
        mw.started = []
        mw.start_sync = lambda *a, **kw: mw.started.append(a + (kw,))
        dlg = SyncDialog(None, mw, _FakeSyncModel(remote), prefix)
        self.addCleanup(dlg.close)
        return dlg, mw

    def test_preview_needs_an_existing_folder(self):
        dlg, _mw = self._dlg()
        dlg._local.setText("/nonexistent/xyz")
        dlg._preview()
        self.assertIn("existing local folder", dlg._info.text())
        self.assertFalse(dlg._btn_run.isEnabled())

    def test_dry_run_lists_actions_without_transferring(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "new.txt"), "w") as handle:
                handle.write("12345")
            dlg, mw = self._dlg(remote={"old.txt": (3, 10.0)})
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            actions = {a["action"] for a in dlg._actions}
            self.assertIn("upload", actions)      # new.txt is missing remotely
            self.assertIn("skip", actions)        # old.txt kept (no delete_extra)
            self.assertTrue(dlg._btn_run.isEnabled())
            self.assertEqual(mw.started, [])      # dry run transfers nothing

    def test_delete_extra_turns_skips_into_deletes(self):
        with tempfile.TemporaryDirectory() as tmp:
            dlg, _mw = self._dlg(remote={"gone.txt": (3, 10.0)})
            dlg._local.setText(tmp)
            dlg._delete_extra.setChecked(True)
            dlg._preview()
            self._settle(dlg)
            self.assertEqual(
                [(a["action"], a["rel"]) for a in dlg.actionable()],
                [("delete_remote", "gone.txt")],
            )

    def test_direction_switch(self):
        dlg, _mw = self._dlg()
        self.assertEqual(dlg.direction(), "upload")
        dlg._down.setChecked(True)
        self.assertEqual(dlg.direction(), "download")

    def test_exclude_patterns_are_parsed_and_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("keep.txt", "drop.tmp"):
                with open(os.path.join(tmp, name), "w") as handle:
                    handle.write("x")
            dlg, _mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._exclude.setText("*.tmp, *.bak")
            self.assertEqual(dlg.exclude_patterns(), ["*.tmp", "*.bak"])
            dlg._preview()
            self._settle(dlg)
            self.assertEqual([a["rel"] for a in dlg._actions], ["keep.txt"])

    def test_run_hands_the_plan_to_the_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(len(mw.started), 1)
            actions, local_dir, prefix, direction = mw.started[0][:4]
            self.assertEqual(local_dir, tmp)
            self.assertEqual(prefix, "pre/")
            self.assertEqual(direction, "upload")
            self.assertEqual([a["rel"] for a in actions], ["f.txt"])

    def test_the_plan_carries_the_bucket_it_was_compared_against(self):
        """The job is bound to a model when it starts running, not when it is
        queued, so the bucket has to travel with it."""
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(mw.started[0][-1].get("bucket"), "b")

    def test_declining_the_confirmation_does_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.No):
                dlg._run()
            self.assertEqual(mw.started, [])

    def _reviewed(self):
        """A dialog showing a plan the user has read: one remote deletion."""
        dlg, mw = self._dlg()
        dlg._local.setText("/original")
        dlg._scan_inputs = dlg.inputs()
        dlg._on_plan([{"action": "delete_remote", "rel": "file",
                       "size": 4, "reason": "extra"}], None)
        return dlg, mw

    def test_changing_an_input_invalidates_the_reviewed_plan(self):
        """The plan on screen is what the user agreed to run. Edit a field
        after previewing and it describes a comparison that was never made —
        for a different folder, or with deletions the user never saw."""
        changes = {
            "folder": lambda d: d._local.setText("/different"),
            "direction": lambda d: d._down.setChecked(True),
            "exclude": lambda d: d._exclude.setText("*.txt"),
            "delete_extra": lambda d: d._delete_extra.setChecked(True),
        }
        for name, change in changes.items():
            with self.subTest(change=name):
                dlg, mw = self._reviewed()
                self.assertTrue(dlg._btn_run.isEnabled())
                change(dlg)
                self.assertFalse(dlg._btn_run.isEnabled())
                dlg._run()
                self.assertEqual(mw.started, [])

    def test_a_plan_for_inputs_that_changed_during_the_scan_is_discarded(self):
        dlg, _mw = self._reviewed()
        dlg._local.setText("/different")
        dlg._on_plan([{"action": "delete_remote", "rel": "file",
                       "size": 4, "reason": "extra"}], None)
        self.assertEqual(dlg.actionable(), [])
class CrossProfileSyncDialogTests(unittest.TestCase):
    """The cross-profile sync preview summarises its plan the same way the
    in-profile one does."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_summary_is_rendered_as_text(self):
        """summarize_sync_plan returns counts; handing the dict straight to
        setText is a TypeError, so the preview could not be shown at all."""
        fake = types.SimpleNamespace(
            _table=QTableWidget(0, 4), _summary=QLabel(), _ok_enabled=Mock())
        CrossProfileSyncDialog._render(
            fake, [{"action": "upload", "rel": "file", "size": 4,
                    "reason": "missing"}])
        self.assertIn("upload: 1", fake._summary.text())
        fake._ok_enabled.assert_called_once_with(True)
class UsageDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_result_renders_counts_classes_and_largest(self):
        dlg = main_window.BucketUsageDialog("bkt", "pre/")
        self.addCleanup(dlg.close)
        dlg.set_result(
            "bkt", "pre/", 3072, {"Documents": 1024, "Media": 2048, "Other": 0},
            {"docs": 1024}, count=7,
            by_class={"STANDARD": 1024, "GLACIER": 2048},
            largest=[(2048, "big.mp4"), (1024, "doc.pdf")],
        )
        self.assertIn("7", dlg.total_lbl.text())
        self.assertIn("GLACIER", dlg.by_class.text())
        self.assertIn("big.mp4", dlg.largest.text())

    def test_empty_result_says_so(self):
        dlg = main_window.BucketUsageDialog("bkt")
        self.addCleanup(dlg.close)
        dlg.set_result("bkt", "", 0, {}, {}, count=0, by_class={}, largest=[])
        self.assertIn("(empty)", dlg.by_class.text())
        self.assertIn("(empty)", dlg.largest.text())
class BulkTagsDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self):
        dlg = main_window.BulkTagsDialog(None, 3)
        self.addCleanup(dlg.close)
        return dlg

    def test_empty_dialog_is_a_noop(self):
        self.assertTrue(self._dlg().is_noop())

    def test_collects_tags_and_removals(self):
        dlg = self._dlg()
        dlg._table.item(0, 0).setText("env")
        dlg._table.item(0, 1).setText("prod")
        dlg._remove_keys.setText("old, stale")
        self.assertEqual(dlg.tags_to_add(), {"env": "prod"})
        self.assertEqual(dlg.tags_to_remove(), ["old", "stale"])
        self.assertFalse(dlg.is_noop())

    def test_blank_keys_are_dropped(self):
        dlg = self._dlg()
        dlg._table.item(0, 1).setText("value with no key")
        self.assertEqual(dlg.tags_to_add(), {})

    def test_replace_alone_is_meaningful(self):
        dlg = self._dlg()
        dlg._replace.setChecked(True)
        self.assertTrue(dlg.replace_all())
        self.assertFalse(dlg.is_noop())   # replace with no tags clears them
class PreviewRenderTests(unittest.TestCase):
    """PreviewDialog picks a page from the fetched bytes; drive _on_loaded
    directly so no network or worker thread is involved."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        bucket = "b"

        def clone_for_worker(self):
            return self

        def get_object_preview(self, key, max_bytes):
            return {"data": b"", "content_type": "", "size": 0,
                    "truncated": False}

    def _dlg(self, key):
        dlg = main_window.PreviewDialog(None, self.FakeModel(), key)
        self.addCleanup(dlg.close)
        deadline = time.monotonic() + 5
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        return dlg

    def test_binary_falls_back_to_a_hex_dump(self):
        dlg = self._dlg("a.bin")
        dlg._on_loaded({"data": b"\x00\x01ABC", "content_type": "",
                        "size": 5, "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 3)   # hex page
        self.assertIn("|..ABC|", dlg._hex.toPlainText())

    def test_escape_cannot_walk_past_the_unsaved_changes_prompt(self):
        """FINDING (data loss): the prompt lived in closeEvent, so Escape
        threw the edits away without asking."""
        dlg = self._dlg("notes.txt")
        # Registered after the helper's dlg.close(), so it runs BEFORE it:
        # a cleanup that trips the prompt would block on a modal box forever.
        self.addCleanup(setattr, dlg, "_editing", False)
        dlg.show()
        dlg._on_loaded({"data": b"hello", "content_type": "text/plain",
                        "size": 5, "truncated": False}, None)
        dlg._editing = True
        dlg._text.setPlainText("edited")
        # setPlainText clears the flag; typing is what would set it.
        dlg._text.document().setModified(True)

        asked = []
        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **k: asked.append(1)
                          or QMessageBox.StandardButton.No):
            QApplication.sendEvent(dlg, QKeyEvent(
                QEvent.Type.KeyPress, Qt.Key.Key_Escape,
                Qt.KeyboardModifier.NoModifier))
            self._app.processEvents()
        self.assertTrue(asked, "Escape discarded the edits without asking")
        self.assertTrue(dlg.isVisible())

        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **k: QMessageBox.StandardButton.Yes):
            dlg.reject()
            self._app.processEvents()
        self.assertFalse(dlg.isVisible())

    def test_code_gets_a_highlighter(self):
        dlg = self._dlg("script.py")
        dlg._on_loaded({"data": b"def f():\n    return 1\n",
                        "content_type": "text/plain", "size": 20,
                        "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 2)   # text page
        self.assertIsNotNone(dlg._highlighter)

    def test_plain_text_has_no_highlighter(self):
        dlg = self._dlg("notes.txt")
        dlg._on_loaded({"data": b"hello", "content_type": "text/plain",
                        "size": 5, "truncated": False}, None)
        self.assertEqual(dlg._stack.currentIndex(), 2)
        self.assertIsNone(dlg._highlighter)

    def test_truncated_pdf_explains_itself(self):
        dlg = self._dlg("doc.pdf")
        dlg._on_loaded({"data": b"%PDF-1.4 truncated", "content_type":
                        "application/pdf", "size": 10 ** 9, "truncated": True},
                       None)
        self.assertEqual(dlg._stack.currentIndex(), 0)   # status page
        self.assertIn("larger than the preview limit", dlg._status.text())

    def test_real_pdf_renders_when_qtpdf_is_available(self):
        if main_window.QPdfView is None:
            self.skipTest("QtPdf not available in this build")
        pdf = (b"%PDF-1.1\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
               b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
               b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 99 99]>>endobj\n"
               b"trailer<</Root 1 0 R>>\n")
        dlg = self._dlg("doc.pdf")
        dlg._on_loaded({"data": pdf, "content_type": "application/pdf",
                        "size": len(pdf), "truncated": False}, None)
        # either it rendered (page 4) or it reported why (page 0)
        self.assertIn(dlg._stack.currentIndex(), (0, 4))
class DuplicateFinderDialogTests(unittest.TestCase):
    MD5_A = "a" * 32
    MD5_B = "b" * 32

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settle(self, dlg, timeout=5.0):
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _dlg(self, rows=None, prefix="pre/", fail=False, read_only=False):
        mw = _FakeMainWindow()
        mw.is_read_only = lambda: read_only
        mw.deleted = []
        mw.delete_duplicate_keys = lambda keys: mw.deleted.append(list(keys))
        model = _FakeDuplicateModel(rows, fail=fail)
        dlg = main_window.DuplicateFinderDialog(None, mw, model, prefix)
        self.addCleanup(dlg.close)
        return dlg, mw, model

    def _rows(self):
        return [
            ("pre/old.bin", 2 * 1024 * 1024, self.MD5_A, _dt(1)),
            ("pre/new.bin", 2 * 1024 * 1024, self.MD5_A, _dt(3)),
            ("pre/unique.bin", 5 * 1024 * 1024, self.MD5_B, _dt(2)),
        ]

    def test_scan_groups_duplicates_and_reports_reclaimable_space(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_size.setText("1")
        dlg._min_unit.setCurrentIndex(0)      # bytes
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(len(dlg._groups), 1)
        self.assertEqual(dlg._tree.topLevelItemCount(), 1)
        self.assertEqual(dlg._tree.topLevelItem(0).childCount(), 2)
        self.assertIn("2.0 MB", dlg._info.text())

    def test_nothing_is_preselected(self):
        """Deletion is irreversible; the user must choose explicitly."""
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(dlg.selected_keys(), [])
        self.assertFalse(dlg._btn_delete.isEnabled())

    def test_keep_newest_and_oldest_pick_opposite_survivors(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        self.assertEqual(dlg.selected_keys(), ["pre/old.bin"])
        dlg._auto_select("oldest")
        self.assertEqual(dlg.selected_keys(), ["pre/new.bin"])
        dlg._clear_selection()
        self.assertEqual(dlg.selected_keys(), [])

    def test_min_size_filter_is_applied(self):
        dlg, _mw, _model = self._dlg(self._rows())
        dlg._min_size.setText("3")
        dlg._min_unit.setCurrentIndex(2)      # MB — above the duplicate pair
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(dlg._groups, [])
        self.assertIn("No duplicates", dlg._info.text())

    def test_scope_checkbox_widens_the_scan_to_the_bucket(self):
        dlg, _mw, model = self._dlg(self._rows(), prefix="pre/")
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(model.prefixes[-1], "pre/")
        dlg._scope.setChecked(True)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(model.prefixes[-1], "")

    def test_scan_passes_a_cancel_event(self):
        dlg, _mw, model = self._dlg(self._rows())
        dlg._scan()
        self._settle(dlg)
        self.assertIsNotNone(model.cancel_events[0])

    def test_scan_failure_is_reported(self):
        dlg, _mw, _model = self._dlg(fail=True)
        dlg._scan()
        self._settle(dlg)
        self.assertIn("Scan failed", dlg._info.text())
        self.assertTrue(dlg._btn_scan.isEnabled())

    def test_delete_hands_the_selection_to_the_window(self):
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            dlg._delete_selected()
        self.assertEqual(mw.deleted, [["pre/old.bin"]])

    def test_declining_the_confirmation_deletes_nothing(self):
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            dlg._delete_selected()
        self.assertEqual(mw.deleted, [])

    def test_warns_when_a_whole_group_is_selected(self):
        """Checking every copy destroys the content instead of de-duplicating."""
        dlg, mw, _model = self._dlg(self._rows())
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        for item in dlg._iter_key_items():
            item.setCheckState(0, Qt.CheckState.Checked)
        self.assertTrue(dlg._selection_would_empty_a_group())
        seen = {}
        with patch.object(main_window.QMessageBox, "question",
                          side_effect=lambda *a, **k: seen.update(text=a[2])
                          or QMessageBox.StandardButton.No):
            dlg._delete_selected()
        self.assertIn("WARNING", seen["text"])

    def test_read_only_profile_cannot_delete(self):
        dlg, _mw, _model = self._dlg(self._rows(), read_only=True)
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        dlg._auto_select("newest")
        self.assertFalse(dlg._btn_delete.isEnabled())

    def test_unconfirmed_group_is_labelled_and_not_auto_selected(self):
        rows = [
            ("pre/a.bin", 4096, self.MD5_A, _dt(1)),
            ("pre/b.bin", 4096, self.MD5_B + "-2", _dt(2)),
        ]
        dlg, _mw, _model = self._dlg(rows)
        dlg._min_unit.setCurrentIndex(0)
        dlg._scan()
        self._settle(dlg)
        self.assertEqual(len(dlg._groups), 1)
        self.assertFalse(dlg._groups[0]["confirmed"])
        self.assertIn("cannot confirm", dlg._tree.topLevelItem(0).text(0))
        dlg._auto_select("newest")
        self.assertEqual(dlg.selected_keys(), [])

    def test_min_size_parsing_is_forgiving(self):
        dlg, _mw, _model = self._dlg()
        dlg._min_unit.setCurrentIndex(0)
        dlg._min_size.setText("not a number")
        self.assertEqual(dlg.min_size_bytes(), 1)
        dlg._min_size.setText("0")
        self.assertEqual(dlg.min_size_bytes(), 1)   # zero-byte objects excluded
        dlg._min_size.setText("2")
        dlg._min_unit.setCurrentIndex(1)            # KB
        self.assertEqual(dlg.min_size_bytes(), 2048)
class SearchResultActionTests(unittest.TestCase):
    """Search used to be a dead end: one row at a time, go-to or copy-key."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False):
        settings = QSettings("s3duck-tests-search", "s3duck-tests-search")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(
                self.ROOT,
                settings,
                profile_name='prod',
                read_only=read_only)
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        return win

    def _dialog(self, win, rows=None):
        dlg = main_window.SearchDialog(win, win, win.data_model, "logs/")
        self.addCleanup(dlg.close)
        rows = rows if rows is not None else [
            ("logs/a/one.txt", 10, None),
            ("logs/b/two.txt", 20, None),
            ("logs/c/three.txt", 30, None),
        ]
        dlg._on_results(rows, None)
        return dlg

    def test_results_allow_a_multi_row_selection(self):
        win = self._window()
        dlg = self._dialog(win)
        self.assertEqual(
            dlg._table.selectionMode(),
            QAbstractItemView.SelectionMode.ExtendedSelection)
        dlg._table.selectAll()
        self.assertEqual(dlg.selected_keys(),
                         ["logs/a/one.txt", "logs/b/two.txt",
                          "logs/c/three.txt"])

    def test_buttons_track_what_is_selected(self):
        win = self._window()
        dlg = self._dialog(win)
        self.assertFalse(dlg._btn_actions.isEnabled())
        self.assertFalse(dlg._btn_copy.isEnabled())
        dlg._table.selectRow(0)
        self.assertTrue(dlg._btn_actions.isEnabled())
        self.assertTrue(dlg._btn_goto.isEnabled())
        dlg._table.selectAll()
        # go-to needs exactly one row; the bulk actions do not
        self.assertFalse(dlg._btn_goto.isEnabled())
        self.assertTrue(dlg._btn_actions.isEnabled())

    def test_action_menu_offers_the_queue_operations(self):
        win = self._window()
        dlg = self._dialog(win)
        menu = QMenu()
        dlg._build_actions_menu(menu, ["logs/a/one.txt", "logs/b/two.txt"])
        labels = [act.text() for act in menu.actions() if act.text()]
        self.assertIn("Download 2 object(s)…", labels)
        self.assertIn("Delete 2 object(s)…", labels)
        self.assertIn("Change storage class…", labels)
        self.assertIn("Fix Content-Type from extension", labels)

    def test_read_only_profile_gets_only_the_harmless_actions(self):
        win = self._window(read_only=True)
        dlg = self._dialog(win)
        menu = QMenu()
        dlg._build_actions_menu(menu, ["logs/a/one.txt"])
        labels = [act.text() for act in menu.actions() if act.text()]
        self.assertIn("Copy s3:// URIs", labels)
        self.assertIn("(read-only profile)", labels)
        for forbidden in ("Delete 1 object(s)…", "Change storage class…"):
            self.assertNotIn(forbidden, labels)

    def test_copy_uris_writes_one_line_per_key(self):
        win = self._window()
        dlg = self._dialog(win)
        dlg._copy_uris(["logs/a/one.txt", "logs/b/two.txt"])
        self.assertEqual(
            QApplication.clipboard().text(),
            "s3://bkt/logs/a/one.txt\ns3://bkt/logs/b/two.txt")

    def test_copy_keys_copies_the_whole_selection(self):
        win = self._window()
        dlg = self._dialog(win)
        dlg._table.selectAll()
        dlg._copy_selected()
        self.assertEqual(
            QApplication.clipboard().text().splitlines(),
            ["logs/a/one.txt", "logs/b/two.txt", "logs/c/three.txt"])
class PropertiesLockSummaryTests(unittest.TestCase):
    """Most buckets have no Object Lock, which is not an error."""

    def test_no_lock_reads_as_nothing_to_say(self):
        self.assertEqual(
            PropertiesWindow.lock_summary({"supported": False}), "")
        self.assertEqual(PropertiesWindow.lock_summary(None), "")

    def test_retention_and_hold_are_summarised(self):
        summary = PropertiesWindow.lock_summary({
            "supported": True, "mode": "GOVERNANCE",
            "retain_until": "2027-01-01", "legal_hold": "ON"})
        self.assertIn("GOVERNANCE until 2027-01-01", summary)
        self.assertIn("legal hold ON", summary)

    def test_an_off_hold_is_not_mentioned(self):
        self.assertEqual(
            PropertiesWindow.lock_summary(
                {"supported": True, "legal_hold": "OFF"}),
            "")
class DuplicateConfirmationTests(unittest.TestCase):
    """The duplicate finder can now settle its own candidates."""

    class _Model:
        bucket = "bkt"

        def __init__(self, fingerprints):
            self._fingerprints = fingerprints
            self.asked = []

        def object_fingerprint(self, key, version_id=""):
            self.asked.append(key)
            value = self._fingerprints[key]
            if isinstance(value, Exception):
                raise value
            return value

    def _group(self, *keys):
        return {"size": 10, "etag": "", "confirmed": False,
                "members": [(key, None) for key in keys],
                "count": len(keys), "wasted": 10}

    def test_matching_digests_confirm_a_candidate(self):
        model = self._Model({"a": ("full", "MD5:aa"), "b": ("full", "MD5:aa")})
        self.assertEqual(
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")]),
            ["same"])
        self.assertEqual(model.asked, ["a", "b"])

    def test_differing_whole_object_digests_rule_it_out(self):
        model = self._Model({"a": ("full", "MD5:aa"), "b": ("full", "MD5:bb")})
        self.assertEqual(
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")]),
            ["different"])

    def test_a_silent_backend_leaves_it_undecided(self):
        model = self._Model({"a": ("", ""), "b": ("full", "MD5:aa")})
        self.assertEqual(
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")]),
            ["unknown"])

    def test_a_failing_member_does_not_abort_the_pass(self):
        model = self._Model({"a": RuntimeError("denied"), "b": ("full", "MD5:aa")})
        self.assertEqual(
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")]),
            ["unknown"])

    def test_cancellation_stops_between_groups(self):
        model = self._Model({"a": ("full", "MD5:aa"), "b": ("full", "MD5:aa")})
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")], cancel_event=cancel)
        self.assertEqual(model.asked, [])

    def test_two_checksum_algorithms_do_not_rule_a_candidate_out(self):
        """Same size, same-looking pair, but one object was uploaded with a
        CRC32 and the other only has an ETag. Reading that as "different"
        threw away a real duplicate."""
        model = self._Model({"a": ("full", "CRC32:aa"), "b": ("full", "MD5:bb")})
        self.assertEqual(
            main_window.DuplicateFinderDialog.confirm_groups(
                model, [self._group("a", "b")]),
            ["unknown"])
class VersionDiffTests(unittest.TestCase):
    """The versions list could download any version but never say what
    changed — which is the question people open it with."""

    build = staticmethod(main_window.VersionDiffDialog.build_diff)

    def test_a_change_produces_a_unified_diff(self):
        text, problem = self.build(b"one\ntwo\n", b"one\nTWO\n", "v1", "v2")
        self.assertEqual(problem, "")
        self.assertIn("--- v1", text)
        self.assertIn("+++ v2", text)
        self.assertIn("-two", text)
        self.assertIn("+TWO", text)

    def test_identical_versions_say_so_instead_of_showing_nothing(self):
        text, problem = self.build(b"same\n", b"same\n", "v1", "v2")
        self.assertEqual(text, "")
        self.assertIn("identical", problem)

    def test_identical_but_truncated_admits_the_caveat(self):
        _text, problem = self.build(b"a", b"a", "v1", "v2", truncated=True)
        self.assertIn("part that was read", problem)

    def test_binary_content_is_refused(self):
        _text, problem = self.build(b"\x00\x01", b"\x00\x02", "v1", "v2")
        self.assertIn("binary", problem)

    def test_undecodable_text_is_refused(self):
        """A diff of replacement characters looks like a change on every
        line."""
        _text, problem = self.build(
            "héllo".encode("latin-1"), b"hello", "v1", "v2")
        self.assertIn("UTF-8", problem)

    def test_an_empty_first_version_still_diffs(self):
        text, problem = self.build(b"", b"new\n", "v1", "v2")
        self.assertEqual(problem, "")
        self.assertIn("+new", text)
class VersionSelectionTests(unittest.TestCase):
    """Two rows mean a diff; anything else does not."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class _Model:
        bucket = "bkt"

        def list_object_versions(self, key):
            return []

        def get_bucket_versioning_status(self):
            return "Enabled"

        def clone_for_worker(self):
            return self

    def _dialog(self, versions):
        dlg = main_window.VersionsDialog(
            None, None, self._Model(), "a/obj.txt")
        self.addCleanup(dlg.close)
        dlg._versions = versions
        dlg._render(versions, "Enabled")
        return dlg

    @staticmethod
    def _version(vid, latest=False, marker=False):
        return {"version_id": vid, "size": 10, "storage_class": "STANDARD",
                "last_modified": None, "is_latest": latest,
                "is_delete_marker": marker}

    def test_no_selection_offers_no_diff(self):
        dlg = self._dialog([self._version("v2", latest=True),
                            self._version("v1")])
        self.assertIsNone(dlg.diffable_pair())
        self.assertFalse(dlg._btn_diff.isEnabled())

    def test_two_versions_are_paired_oldest_first(self):
        dlg = self._dialog([self._version("v2", latest=True),
                            self._version("v1")])
        dlg._table.selectAll()
        # the table lists newest first, so the pair reads chronologically
        self.assertEqual(dlg.diffable_pair(), ("v1", "v2"))
        self.assertTrue(dlg._btn_diff.isEnabled())

    def test_three_rows_are_not_a_pair(self):
        dlg = self._dialog([self._version("v3", latest=True),
                            self._version("v2"), self._version("v1")])
        dlg._table.selectAll()
        self.assertIsNone(dlg.diffable_pair())

    def test_a_delete_marker_is_not_diffable(self):
        dlg = self._dialog([self._version("v2", latest=True, marker=True),
                            self._version("v1")])
        dlg._table.selectAll()
        self.assertIsNone(dlg.diffable_pair())

    def test_the_single_row_actions_ignore_a_two_row_selection(self):
        """Download/make-current/delete act on exactly one version."""
        dlg = self._dialog([self._version("v2", latest=True),
                            self._version("v1")])
        dlg._table.selectAll()
        self.assertIsNone(dlg._selected())
        self.assertFalse(dlg._btn_download.isEnabled())
        self.assertFalse(dlg._btn_delete.isEnabled())
class VersionPreviewTests(unittest.TestCase):
    """A preview can now be asked for one specific version."""

    def test_the_version_id_reaches_get_object(self):
        client = FakeS3Client(get_object_resp={
            "Body": io.BytesIO(b"hello"), "ContentLength": 5,
            "ContentType": "text/plain", "ETag": '"abc"'})
        m = make_model(bucket="bkt")
        m._client = client
        m.get_object_preview("a.txt", 1024, version_id="v7")
        self.assertEqual(client.calls_of("get_object")[0]["VersionId"], "v7")

    def test_without_one_nothing_extra_is_sent(self):
        client = FakeS3Client(get_object_resp={
            "Body": io.BytesIO(b"hello"), "ContentLength": 5})
        m = make_model(bucket="bkt")
        m._client = client
        m.get_object_preview("a.txt", 1024)
        self.assertNotIn("VersionId", client.calls_of("get_object")[0])
class SavedSearchTests(unittest.TestCase):
    """With results driving the queue, a recurring cleanup is worth one click
    rather than a re-typed form."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, profile="prod"):
        settings = QSettings("s3duck-tests-saved", "s3duck-tests-saved")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name=profile)
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        return win

    def _dialog(self, win):
        dlg = main_window.SearchDialog(win, win, win.data_model, "logs/")
        self.addCleanup(dlg.close)
        return dlg

    def test_nothing_is_saved_to_begin_with(self):
        win = self._window()
        self.assertEqual(win.load_saved_searches(), [])

    def test_a_corrupt_store_costs_the_list_not_the_dialog(self):
        win = self._window()
        win.settings.beginGroup("common")
        win.settings.setValue("searches/prod", "{nope")
        win.settings.endGroup()
        self.assertEqual(win.load_saved_searches(), [])

    def test_the_form_round_trips_through_a_config(self):
        win = self._window()
        dlg = self._dialog(win)
        dlg._query.setText("*.tmp")
        dlg._regex.setChecked(True)
        dlg._case.setChecked(True)
        dlg._exts.setText("log, tmp")
        dlg._min_size.setText("5")
        dlg._size_unit.setCurrentIndex(2)
        dlg._use_after.setChecked(True)
        config = dlg.search_config()

        other = self._dialog(win)
        other.apply_search_config(config)
        self.assertEqual(other.search_config(), config)

    def test_saving_then_choosing_restores_the_filters(self):
        win = self._window()
        dlg = self._dialog(win)
        dlg._query.setText("old")
        dlg._exts.setText("log")
        with patch.object(main_window.QInputDialog, "getText",
                          lambda *a, **kw: ("nightly", True)):
            dlg._save_search()
        self.assertEqual(
            [e["name"] for e in win.load_saved_searches()], ["nightly"])

        fresh = self._dialog(win)
        self.assertEqual(fresh._saved.count(), 2)   # "(none)" plus the saved
        with patch.object(main_window.SearchDialog, "_run_search",
                          lambda _s: None):
            fresh._saved.setCurrentIndex(1)
        self.assertEqual(fresh._query.text(), "old")
        self.assertEqual(fresh._exts.text(), "log")

    def test_saving_the_same_name_twice_replaces_it(self):
        win = self._window()
        dlg = self._dialog(win)
        with patch.object(main_window.QInputDialog, "getText",
                          lambda *a, **kw: ("nightly", True)):
            dlg._query.setText("first")
            dlg._save_search()
            dlg._query.setText("second")
            dlg._save_search()
        entries = win.load_saved_searches()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["filters"]["query"], "second")

    def test_cancelling_the_name_prompt_saves_nothing(self):
        win = self._window()
        dlg = self._dialog(win)
        with patch.object(main_window.QInputDialog, "getText",
                          lambda *a, **kw: ("", False)):
            dlg._save_search()
        self.assertEqual(win.load_saved_searches(), [])

    def test_deleting_removes_only_that_one(self):
        win = self._window()
        win.store_saved_searches([{"name": "a", "filters": {}},
                                  {"name": "b", "filters": {}}])
        dlg = self._dialog(win)
        with patch.object(main_window.SearchDialog, "_run_search",
                          lambda _s: None):
            dlg._saved.setCurrentIndex(1)   # "a"
            dlg._delete_search()
        self.assertEqual([e["name"] for e in win.load_saved_searches()], ["b"])

    def test_searches_are_per_profile(self):
        win = self._window("prod")
        win.store_saved_searches([{"name": "prod-only", "filters": {}}])
        win.profile_name = "dev"
        self.assertEqual(win.load_saved_searches(), [])
