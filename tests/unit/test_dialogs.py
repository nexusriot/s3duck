"""ui/dialogs/."""

import ast
import botocore.exceptions
import core.sync_plan as sync_plan_module
import hashlib
import io
import main_window
import os
import tempfile
import threading
import time
import types
import ui.dialogs.bucket_settings as bucket_settings_module
import ui.dialogs.compare as compare_module
import ui.dialogs.copy_move as copy_move_module
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
    FakeS3Client, PROJECT_ROOT, _FakeCompareModel, _FakeDuplicateModel,
    _FakeMainWindow,
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

    def test_a_rename_chain_blocks_ok_and_hands_out_no_plan(self):
        """FINDING (data loss): shifting a numbering (img1->img2->img3) was
        accepted; executed as copy-all-then-delete it left one file of
        three. The dialog must refuse it, and plan() must not hand it out
        even if the dialog is accepted some other way."""
        dlg = self._dlg([("img1.jpg", False), ("img2.jpg", False)])
        dlg._mode_template.setChecked(True)
        dlg._start.setValue(2)
        dlg._template.setText("img{n}{ext}")
        self.assertFalse(self._ok(dlg).isEnabled())
        self.assertIn("being renamed", dlg._info.text())
        self.assertEqual(dlg._table.rowCount(), 2)      # still previewed
        self.assertEqual(dlg.plan(), [])

    def test_a_field_access_template_does_not_abort(self):
        """REGRESSION (crash): typing "{name.ext}" raised AttributeError out
        of the textChanged slot, which PyQt6 turns into a process abort."""
        dlg = self._dlg([("a.txt", False)])
        dlg._mode_template.setChecked(True)
        dlg._template.setText("{name.ext}")
        self.assertIn("Invalid template", dlg._info.text())
        self.assertFalse(self._ok(dlg).isEnabled())

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

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "root reads a chmod 000 folder anyway")
    def test_an_unreadable_subfolder_is_an_error_not_remote_deletes(self):
        with tempfile.TemporaryDirectory() as tmp:
            locked = os.path.join(tmp, "locked")
            os.mkdir(locked)
            os.chmod(locked, 0)
            try:
                dlg, _mw = self._dlg(remote={"locked/a.txt": (3, 10.0)})
                dlg._local.setText(tmp)
                dlg._delete_extra.setChecked(True)
                dlg._preview()
                self._settle(dlg)
            finally:
                os.chmod(locked, 0o700)
            self.assertEqual(dlg.actionable(), [])
            self.assertFalse(dlg._btn_run.isEnabled())

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

    def test_a_plan_belongs_to_the_bucket_and_prefix_it_was_built_from(self):
        """The form cannot show it and no signal announces it, but the window
        can navigate while the dialog is open. The plan's rows are bare
        relative paths: under another bucket or prefix they name different
        objects, and "delete_remote old.html" would delete one of them."""
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            for moved in ("bucket", "prefix"):
                with self.subTest(moved=moved):
                    dlg, mw = self._dlg()
                    dlg._local.setText(tmp)
                    dlg._preview()
                    self._settle(dlg)
                    self.assertTrue(dlg._btn_run.isEnabled())

                    if moved == "bucket":
                        dlg._model.bucket = "somewhere-else"
                    else:
                        dlg._prefix = "other/"
                    with patch.object(main_window.QMessageBox, "question",
                                      return_value=QMessageBox.StandardButton.Yes):
                        dlg._run()
                    self.assertEqual(mw.started, [])
                    self.assertEqual(dlg._actions, [])

    def test_run_sends_the_bucket_and_prefix_that_were_reviewed(self):
        """Not the ones the window shows by the time Run is pressed."""
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg(prefix="reviewed/")
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(mw.started[0][2], "reviewed/")
            self.assertEqual(mw.started[0][-1]["bucket"], dlg._model.bucket)

    def test_a_navigation_during_the_confirmation_still_stops_the_run(self):
        """The confirmation runs a nested event loop, so the window is live
        while it is up: the bucket can change between the check before the
        prompt and the start after it. Checked again on the way out."""
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            self._settle(dlg)
            self.assertTrue(dlg._btn_run.isEnabled())

            def _navigate_then_confirm(*_args, **_kwargs):
                dlg._model.bucket = "somewhere-else"
                return QMessageBox.StandardButton.Yes

            with patch.object(main_window.QMessageBox, "question",
                              _navigate_then_confirm):
                dlg._run()
            self.assertEqual(mw.started, [])
            self.assertEqual(dlg._actions, [])

    def test_changing_the_folder_after_preview_invalidates_the_plan(self):
        """FINDING (data loss): Run read the folder box afresh, so a plan
        previewed against folder A ("delete_local old.html") ran against a
        folder typed afterwards — deleting a file nobody had reviewed."""
        with tempfile.TemporaryDirectory() as first, \
                tempfile.TemporaryDirectory() as second:
            for folder in (first, second):
                with open(os.path.join(folder, "old.html"), "w") as handle:
                    handle.write("x")
            dlg, mw = self._dlg()
            dlg._down.setChecked(True)
            dlg._delete_extra.setChecked(True)
            dlg._local.setText(first)
            dlg._preview()
            self._settle(dlg)
            self.assertEqual([(a["action"], a["rel"]) for a in dlg.actionable()],
                             [("delete_local", "old.html")])
            self.assertTrue(dlg._btn_run.isEnabled())

            dlg._local.setText(second)
            self.assertFalse(dlg._btn_run.isEnabled())
            self.assertEqual(dlg.actionable(), [])
            self.assertIn("preview again", dlg._info.text())
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(mw.started, [])

    def test_every_plan_input_invalidates_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            for change in ("direction", "delete_extra", "exclude"):
                with self.subTest(change=change):
                    dlg, _mw = self._dlg()
                    dlg._local.setText(tmp)
                    dlg._preview()
                    self._settle(dlg)
                    self.assertTrue(dlg._btn_run.isEnabled())
                    if change == "direction":
                        dlg._down.setChecked(True)
                    elif change == "delete_extra":
                        dlg._delete_extra.setChecked(True)
                    else:
                        dlg._exclude.setText("*.tmp")
                    self.assertFalse(dlg._btn_run.isEnabled(), change)
                    self.assertEqual(dlg._actions, [], change)

    def test_run_uses_the_inputs_the_plan_was_built_from(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, mw = self._dlg()
            dlg._local.setText(tmp + os.sep + " ")   # stripped, same folder
            dlg._preview()
            self._settle(dlg)
            with patch.object(main_window.QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._run()
            self.assertEqual(mw.started[0][1], tmp + os.sep)
            self.assertEqual(mw.started[0][3], "upload")

    def test_an_edit_during_the_scan_discards_its_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "f.txt"), "w") as handle:
                handle.write("x")
            dlg, _mw = self._dlg()
            dlg._local.setText(tmp)
            dlg._preview()
            dlg._exclude.setText("*.txt")        # before the result lands
            self._settle(dlg)
            self.assertEqual(dlg._actions, [])
            self.assertFalse(dlg._btn_run.isEnabled())
            self.assertIn("preview again", dlg._info.text())


class CrossProfileSyncDialogTests(unittest.TestCase):
    """The cross-profile sync preview summarises its plan the same way
    the in-profile one does.

    REGRESSION (crash): that summary was a dict handed to QLabel.setText,
    so every successful Compare raised TypeError inside a Qt slot — a
    process abort under PyQt6.
    """

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

    def _dlg(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        mw = _FakeMainWindow()
        dlg = main_window.CrossProfileSyncDialog(
            None, mw, model, "pre/", settings, current_profile="prof")
        self.addCleanup(dlg.close)
        return dlg, mw

    def test_render_spells_the_summary_out(self):
        dlg, _mw = self._dlg()
        dlg._render([
            {"action": "upload", "rel": "a.txt", "size": 2048,
             "reason": "missing at destination"},
            {"action": "skip", "rel": "b.txt", "size": 1, "reason": "up to date"},
        ])
        text = dlg._summary.text()
        self.assertIn("upload: 1", text)
        self.assertIn("skip: 1", text)
        self.assertIn("2.0 KB to transfer", text)
        self.assertTrue(dlg._buttons.button(
            QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_an_empty_plan_renders_too(self):
        dlg, _mw = self._dlg()
        dlg._render([])
        self.assertIn("to transfer", dlg._summary.text())
        self.assertFalse(dlg._buttons.button(
            QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_compare_reaches_the_summary_without_raising(self):
        dlg, mw = self._dlg()
        actions = [{"action": "upload", "rel": "a.txt", "size": 1,
                    "reason": "missing at destination"}]
        mw._run_with_progress = lambda _title, _fn: (actions, None)
        with patch.object(dlg.picker, "destination_bucket",
                          return_value="other"), \
             patch.object(dlg.picker, "selected_profile",
                          return_value=object()), \
             patch.object(copy_move_module, "build_profile_model",
                          return_value=object()):
            dlg.compare()
        self.assertIn("upload: 1", dlg._summary.text())
class PaneCompareContentTests(unittest.TestCase):
    """FINDING: the pane comparison called a same-size pair "same" only when
    the mtimes agreed. S3's LastModified is the time of the write and a
    download never sets the local mtime, so a folder compared with its own
    uploaded copy -- even right after "Copy selected" -- and a prefix with its
    server-side copy showed nearly every file as "differs". "Hide identical"
    hid nothing, and copying again moved unchanged data."""

    DATA = b"the same bytes on both sides"
    LOCAL_MTIME = 1_000.0
    REMOTE_MTIME = 2_000_000.0     # written long after the local file

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name

    def _write(self, rel, data):
        path = os.path.join(self.root, rel)
        with open(path, "wb") as handle:
            handle.write(data)
        os.utime(path, (self.LOCAL_MTIME, self.LOCAL_MTIME))
        return path

    @staticmethod
    def _md5(data):
        return hashlib.md5(data).hexdigest()

    def _dialog(self, model, left, right):
        mw = _FakeMainWindow()
        mw.data_model = model
        mw.is_read_only = lambda: False
        dlg = main_window.PaneCompareDialog(None, mw, left, right)
        self.addCleanup(dlg.close)
        return dlg

    def _compare(self, dlg, timeout=5.0):
        dlg._compare()
        deadline = time.monotonic() + timeout
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        return {row["rel"]: row["status"] for row in dlg._rows}

    def _local_vs_remote(self, etag, heads=None, part_size=8 * 1024 * 1024,
                         data=None):
        self._write("a.txt", self.DATA if data is None else data)
        model = _FakeCompareModel(
            trees={"b": {"pre/a.txt": (len(self.DATA), self.REMOTE_MTIME,
                                       etag)}},
            heads=heads, part_size=part_size)
        dlg = self._dialog(model, ("remote", "b", "pre/"),
                           ("local", self.root))
        return dlg, model

    def test_a_copied_file_is_identical_whatever_its_mtime(self):
        dlg, model = self._local_vs_remote(self._md5(self.DATA))
        self.assertEqual(self._compare(dlg), {"a.txt": "same"})
        self.assertEqual(dlg.visible_rows(), [])
        self.assertIn("1 identical", dlg._summary.text())
        # A match needs no confirmation.
        self.assertEqual(model.head_calls, [])

    def test_a_multipart_upload_is_rebuilt_at_the_configured_part_size(self):
        """What this app's own multipart uploads produce."""
        part = 8
        pieces = [self.DATA[i:i + part]
                  for i in range(0, len(self.DATA), part)]
        etag = Model.composite_etag(
            [hashlib.md5(piece).digest() for piece in pieces])
        dlg, _model = self._local_vs_remote(etag, part_size=part)
        self.assertEqual(self._compare(dlg), {"a.txt": "same"})

    def test_a_multipart_etag_no_guess_reproduces_is_unverified(self):
        dlg, model = self._local_vs_remote("f" * 32 + "-3")
        self.assertEqual(self._compare(dlg), {"a.txt": "unverified"})
        self.assertEqual(model.head_calls, [])

    def test_different_bytes_of_the_same_size_differ(self):
        changed = self.DATA.upper()
        dlg, model = self._local_vs_remote(self._md5(changed))
        self.assertEqual(self._compare(dlg), {"a.txt": "differs"})
        # Confirmed with a HEAD that the ETag really is an MD5.
        self.assertEqual(model.head_calls, [("b", "pre/a.txt")])

    def test_an_encrypted_objects_etag_is_not_taken_as_a_difference(self):
        """Under SSE-KMS or SSE-C the listed ETag is 32 hex characters and
        no digest of the bytes; the listing cannot tell, the HEAD can."""
        refused = botocore.exceptions.ClientError(
            {"Error": {"Code": "400", "Message": "Bad Request"}},
            "HeadObject")
        for label, head in (
                ("kms", {"ServerSideEncryption": "aws:kms"}),
                ("sse-c", {"SSECustomerAlgorithm": "AES256"}),
                ("sse-c, no key", refused)):
            with self.subTest(label):
                dlg, _model = self._local_vs_remote(
                    "c" * 32, heads={("b", "pre/a.txt"): head})
                self.assertEqual(self._compare(dlg), {"a.txt": "unverified"})

    def test_an_unverified_pair_stays_listed_and_says_why(self):
        dlg, _model = self._local_vs_remote(
            "c" * 32, heads={("b", "pre/a.txt"):
                             {"ServerSideEncryption": "aws:kms"}})
        self._compare(dlg)
        self.assertEqual([row["rel"] for row in dlg.visible_rows()],
                         ["a.txt"])
        self.assertEqual(dlg._table.item(0, 3).text(),
                         "same size, different time")
        self.assertTrue(dlg._table.item(0, 3).toolTip())
        self.assertIn("1 same size but different time", dlg._summary.text())

    def test_a_file_over_the_hash_limit_is_not_read(self):
        dlg, _model = self._local_vs_remote(self._md5(self.DATA))
        with patch.object(sync_plan_module, "COMPARE_HASH_LIMIT",
                          len(self.DATA) - 1):
            self.assertEqual(self._compare(dlg), {"a.txt": "unverified"})

    def test_close_clocks_still_skip_the_hash(self):
        """The old size-and-mtime rule holds where it can: an untouched file
        is not read just to be told it is the same."""
        self._write("a.txt", self.DATA)
        model = _FakeCompareModel(trees={"b": {"pre/a.txt": (
            len(self.DATA), self.LOCAL_MTIME + 1, "f" * 32 + "-2")}})
        dlg = self._dialog(model, ("remote", "b", "pre/"),
                           ("local", self.root))
        with patch.object(sync_plan_module, "file_matches_etag",
                          side_effect=AssertionError("hashed")):
            self.assertEqual(self._compare(dlg), {"a.txt": "same"})

    def _remote_vs_remote(self, left_etag, right_etag, heads=None):
        size = len(self.DATA)
        model = _FakeCompareModel(
            trees={"src": {"a/f.bin": (size, 100.0, left_etag)},
                   "dst": {"b/f.bin": (size, self.REMOTE_MTIME, right_etag)}},
            heads=heads)
        dlg = self._dialog(model, ("remote", "src", "a/"),
                           ("remote", "dst", "b/"))
        return dlg, model

    def test_a_server_side_copy_is_identical_whatever_its_mtime(self):
        etag = self._md5(self.DATA)
        dlg, model = self._remote_vs_remote(etag, etag)
        self.assertEqual(self._compare(dlg), {"f.bin": "same"})
        self.assertEqual(dlg.visible_rows(), [])
        self.assertEqual(model.head_calls, [])

    def test_two_plain_md5s_that_disagree_differ(self):
        dlg, model = self._remote_vs_remote("a" * 32, "b" * 32)
        self.assertEqual(self._compare(dlg), {"f.bin": "differs"})
        self.assertEqual(sorted(model.head_calls),
                         [("dst", "b/f.bin"), ("src", "a/f.bin")])

    def test_an_encrypted_copy_is_unverified_not_different(self):
        dlg, _model = self._remote_vs_remote(
            "a" * 32, "b" * 32,
            heads={("dst", "b/f.bin"): {"ServerSideEncryption": "aws:kms"}})
        self.assertEqual(self._compare(dlg), {"f.bin": "unverified"})

    def test_multipart_etags_that_disagree_are_unverified(self):
        dlg, model = self._remote_vs_remote("a" * 32 + "-2", "b" * 32)
        self.assertEqual(self._compare(dlg), {"f.bin": "unverified"})
        self.assertEqual(model.head_calls, [])

    def test_unhiding_shows_the_identical_rows_again(self):
        etag = self._md5(self.DATA)
        dlg, _model = self._remote_vs_remote(etag, etag)
        self._compare(dlg)
        dlg._hide_same.setChecked(False)
        self.assertEqual([row["status"] for row in dlg.visible_rows()],
                         ["same"])
        self.assertEqual(dlg._table.rowCount(), 1)

    def test_closing_mid_compare_cancels_the_hashing(self):
        """The event the check stops on between files is the one closing
        the dialog sets."""
        dlg, _model = self._local_vs_remote(self._md5(self.DATA))
        hashing = threading.Event()
        seen = []

        def capture(left, right, models, part_sizes=(), cancel=None):
            def check(*_args):
                seen.append(cancel)
                hashing.set()
                cancel.wait(5)
                raise TransferCancelled("cancelled")
            return check

        with patch.object(compare_module, "make_content_check", capture):
            dlg._compare()
            self.assertTrue(hashing.wait(5))
            dlg.stop_threads()
        self.assertTrue(seen[0].is_set())


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

    def test_a_read_cut_through_a_character_still_diffs(self):
        """FINDING: the 512 KiB ranged read can end inside a multi-byte
        character; its lone replacement character made a large Cyrillic or
        CJK file "not UTF-8 text" about half the time."""
        # each ends one byte into a two-byte "ж"
        older = ("строка журнала\n" * 3 + "ж").encode("utf-8")[:-1]
        newer = ("строка ЖУРНАЛА\n" * 3 + "ж").encode("utf-8")[:-1]
        self.assertTrue(older.decode("utf-8", "replace").endswith("�"))
        text, problem = self.build(older, newer, "v1", "v2", truncated=True)
        self.assertEqual(problem, "")
        self.assertIn("+строка ЖУРНАЛА", text)

    def test_a_bad_byte_before_the_cut_is_still_refused(self):
        _text, problem = self.build(
            "héllo\nworld".encode("latin-1"), b"hello\nworld", "v1", "v2",
            truncated=True)
        self.assertIn("UTF-8", problem)

    def test_an_untruncated_broken_tail_is_still_refused(self):
        _text, problem = self.build(
            "журнал".encode("utf-8")[:-1], b"x", "v1", "v2")
        self.assertIn("UTF-8", problem)
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
class BucketSettingsSaveTests(unittest.TestCase):
    """The bucket settings dialog may only write what it read and what the
    user changed. Each save used to rewrite whole documents from whatever
    the form held — an empty editor after a failed read, a "(none)" for an
    encryption mode it could not show, tag keys run through a file-extension
    parser — and every one of those removed or renamed real configuration."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class _Model:
        bucket = "bkt"

        def __init__(self, docs):
            self.docs = dict(docs)
            self.calls = []

        def clone_for_worker(self):
            return self

        def __getattr__(self, name):
            if name.startswith("get_"):
                def _get(_doc=name[4:]):
                    value = self.docs.get(_doc)
                    if isinstance(value, Exception):
                        raise value
                    return value
                return _get
            if name.startswith(("put_", "set_")):
                return lambda *a, **kw: self.calls.append((name, a, kw))
            raise AttributeError(name)

        def written(self):
            return [name for name, _a, _kw in self.calls]

    class _Window(_FakeMainWindow):
        def is_read_only(self):
            return False

    DOCS = {
        "bucket_lifecycle": [], "bucket_cors": [], "bucket_policy": "",
        "object_lock_configuration": {}, "bucket_policy_status": "",
        "bucket_encryption": {"sse": "aws:kms", "kms_key": "arn:k",
                              "bucket_key": True},
        "bucket_tags": {"CostCenter": "1234", "Owner": "", "*tier": "gold"},
        "bucket_website": {}, "bucket_notifications": [],
        "bucket_acceleration": "",
    }

    @staticmethod
    def _sync(_parent, _title, fn, modality=None):
        try:
            return fn(None), None
        except Exception as exc:
            return None, exc

    def _dlg(self, runner=None, **docs):
        model = self._Model(dict(self.DOCS, **docs))
        with patch.object(bucket_settings_module, "run_with_progress",
                          runner or self._sync):
            dlg = bucket_settings_module.BucketSettingsDialog(
                None, self._Window(), model)
        self.addCleanup(dlg.close)
        # reload() after a save runs through the same stand-in.
        patcher = patch.object(bucket_settings_module, "run_with_progress",
                               runner or self._sync)
        patcher.start()
        self.addCleanup(patcher.stop)
        return dlg, model

    def test_an_acceleration_toggle_writes_only_acceleration(self):
        """FINDING: ticking acceleration also rewrote the tags with their
        keys lowercased and empty-valued ones dropped."""
        dlg, model = self._dlg()
        dlg._acceleration.setChecked(True)
        dlg._save_bucket()
        self.assertEqual(model.written(), ["set_bucket_acceleration"])

    def test_a_tag_edit_keeps_case_and_empty_values(self):
        dlg, model = self._dlg()
        dlg._bucket_tags.setPlainText(
            dlg._bucket_tags.toPlainText() + "\nTeam = Infra")
        dlg._save_bucket()
        self.assertEqual(model.written(), ["put_bucket_tags"])
        self.assertEqual(model.calls[0][1][0], {
            "CostCenter": "1234", "Owner": "", "*tier": "gold",
            "Team": "Infra"})

    def test_saving_nothing_changed_writes_nothing(self):
        dlg, model = self._dlg()
        dlg._save_bucket()
        self.assertEqual(model.calls, [])
        self.assertIn("Nothing changed", dlg._bucket_status.text())

    def test_an_encryption_change_writes_only_encryption(self):
        dlg, model = self._dlg()
        dlg._sse.setCurrentIndex(dlg._sse.findData("AES256"))
        dlg._save_bucket()
        self.assertEqual(model.written(), ["put_bucket_encryption"])
        self.assertEqual(model.calls[0][2]["sse"], "AES256")

    def test_an_encryption_mode_the_combo_lacks_is_never_written(self):
        """FINDING: DSSE-KMS showed as "(none)", and a tags-only save then
        called put_bucket_encryption(sse="") — removing it."""
        dlg, model = self._dlg(bucket_encryption={
            "sse": "aws:kms:dsse", "kms_key": "arn:k", "bucket_key": False})
        self.assertFalse(dlg._sse.isEnabled())
        self.assertIn("aws:kms:dsse", dlg._bucket_status.text())
        dlg._bucket_tags.setPlainText("a = 1")
        dlg._save_bucket()
        self.assertEqual(model.written(), ["put_bucket_tags"])

    def test_a_failed_encryption_or_tags_read_is_never_written(self):
        dlg, model = self._dlg(bucket_encryption=RuntimeError("denied"),
                               bucket_tags=RuntimeError("denied"))
        self.assertFalse(dlg._sse.isEnabled())
        self.assertFalse(dlg._bucket_tags.isEnabled())
        dlg._acceleration.setChecked(True)
        dlg._save_bucket()
        self.assertEqual(model.written(), ["set_bucket_acceleration"])

    def test_a_failed_lifecycle_read_cannot_replace_the_rules(self):
        """FINDING: after a failed read the rule list was empty and
        writable, so adding one rule and saving replaced every real one."""
        dlg, model = self._dlg(bucket_lifecycle=RuntimeError("timeout"))
        for button in (dlg._btn_add, dlg._btn_edit, dlg._btn_remove,
                       dlg._btn_save):
            self.assertFalse(button.isEnabled())
        with patch.object(bucket_settings_module, "LifecycleRuleDialog") \
                as rule_dlg, \
             patch.object(bucket_settings_module.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            dlg._add_rule()
            dlg._save_lifecycle()
        rule_dlg.assert_not_called()
        self.assertEqual(model.calls, [])

    def test_a_cancelled_read_leaves_nothing_writable(self):
        """Cancelling the first read returned early and left every tab empty
        and writable — Save on Lifecycle then removed the configuration."""
        dlg, model = self._dlg(runner=lambda *_a, **_kw: (None, None))
        for button in (dlg._btn_add, dlg._btn_save, dlg._btn_cors_save,
                       dlg._btn_policy_save, dlg._btn_bucket_save,
                       dlg._btn_site_save):
            self.assertFalse(button.isEnabled())
        self.assertFalse(dlg._policy_edit.isEnabled())
        self.assertTrue(dlg._cors_text.isReadOnly())
        self.assertIn("cancelled", dlg._lifecycle_status.text())
        with patch.object(bucket_settings_module.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            dlg._save_lifecycle()
            dlg._save_cors()
            dlg._save_policy()
            dlg._save_website()
        self.assertEqual(model.calls, [])

    def test_a_failed_cors_or_policy_read_is_read_only(self):
        dlg, _model = self._dlg(bucket_cors=RuntimeError("denied"),
                                bucket_policy=RuntimeError("denied"))
        self.assertTrue(dlg._cors_text.isReadOnly())
        self.assertFalse(dlg._btn_cors_save.isEnabled())
        self.assertFalse(dlg._policy_edit.isEnabled())
        self.assertTrue(dlg._lifecycle_editable())

    def _edit_first_rule(self, dlg):
        dlg._rules_list.setCurrentRow(0)

        def _exec(rule_dialog):
            rule_dialog._accept()
            return QDialog.DialogCode.Accepted

        with patch.object(bucket_settings_module.LifecycleRuleDialog, "exec",
                          _exec):
            dlg._edit_rule()

    def test_editing_a_rule_keeps_what_the_form_has_no_box_for(self):
        """FINDING: Edit -> OK rebuilt the rule from the form alone, dropping
        NoncurrentVersionTransitions and NewerNoncurrentVersions — the latter
        turns "keep the newest 3 old versions" into "delete them all"."""
        rule = {
            "ID": "versions", "Status": "Enabled",
            "Filter": {"Prefix": "logs/"},
            "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}],
            "NoncurrentVersionTransitions": [
                {"NoncurrentDays": 30, "StorageClass": "GLACIER",
                 "NewerNoncurrentVersions": 2}],
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 90, "NewerNoncurrentVersions": 3},
            "Expiration": {"Days": 365},
        }
        dlg, _model = self._dlg(bucket_lifecycle=[rule])
        self._edit_first_rule(dlg)
        self.assertEqual(dlg._rules[0], rule)

    def test_a_rule_the_form_cannot_show_is_not_opened_for_editing(self):
        rules = [
            {"ID": "two", "Status": "Enabled", "Filter": {"Prefix": ""},
             "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"},
                             {"Days": 90, "StorageClass": "GLACIER"}]},
            {"ID": "zero", "Status": "Enabled", "Filter": {"Prefix": ""},
             "Transitions": [{"Days": 0,
                              "StorageClass": "INTELLIGENT_TIERING"}],
             "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}},
        ]
        for index in range(len(rules)):
            with self.subTest(rule=rules[index]["ID"]):
                dlg, _model = self._dlg(bucket_lifecycle=list(rules))
                dlg._rules_list.setCurrentRow(index)
                with patch.object(bucket_settings_module.QMessageBox,
                                  "information") as told, \
                     patch.object(bucket_settings_module,
                                  "LifecycleRuleDialog") as rule_dlg:
                    dlg._edit_rule()
                told.assert_called_once()
                rule_dlg.assert_not_called()
                self.assertEqual(dlg._rules, rules)

    def test_both_expirations_ticked_is_refused_not_trimmed(self):
        rule_dlg = bucket_settings_module.LifecycleRuleDialog(None)
        self.addCleanup(rule_dlg.close)
        rule_dlg._expire.setChecked(True)
        rule_dlg._markers.setChecked(True)
        with patch.object(bucket_settings_module.QMessageBox,
                          "warning") as warned:
            rule_dlg._accept()
        warned.assert_called_once()
        self.assertIsNone(rule_dlg.result_rule())
class PreviewEditFidelityTests(unittest.TestCase):
    """FINDING (data loss): Save from the in-place editor rewrote the whole
    object — line endings, BOM and NBSPs normalised by the text widget, and a
    bare PUT that reset its headers, metadata, class, encryption and tags."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class _Body:
        def __init__(self, data):
            self._data = data

        def read(self):
            return self._data

    class _Client(FakeS3Client):
        HEAD = {
            "ETag": '"abc"', "ContentType": "text/csv; charset=utf-8",
            "CacheControl": "max-age=60", "ContentLanguage": "de",
            "Metadata": {"owner": "finance"}, "StorageClass": "STANDARD_IA",
            "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "arn:k",
            "BucketKeyEnabled": True,
        }

        def __init__(self, body=b"id,name\n1,alpha\n", tags=None,
                     tag_error=None, head=None):
            super().__init__(head_object_resp=dict(head or self.HEAD))
            self.body = body
            self.tags = [{"Key": "Team", "Value": "Data"}] \
                if tags is None else tags
            self.tag_error = tag_error

        def get_object(self, **kw):
            self.calls.append(("get_object", kw))
            return {"Body": PreviewEditFidelityTests._Body(self.body),
                    "ContentType": "text/csv; charset=utf-8",
                    "ContentLength": len(self.body), "ETag": '"abc"'}

        def get_object_tagging(self, **kw):
            self.calls.append(("get_object_tagging", kw))
            if self.tag_error is not None:
                raise self.tag_error
            return {"TagSet": list(self.tags)}

        def put_object(self, **kw):
            self.calls.append(("put_object", kw))
            return {"ETag": '"new"'}

    def _model(self, client):
        model = make_model(bucket="bkt")
        model._client = client
        return model

    def _dlg(self, client, key="reports/q3.csv"):
        model = self._model(client)
        model.clone_for_worker = lambda: model
        dlg = main_window.PreviewDialog(None, model, key)
        self.addCleanup(dlg.close)
        self._wait(dlg, "_thread")
        return dlg

    def _wait(self, dlg, attr):
        deadline = time.monotonic() + 5
        while getattr(dlg, attr) is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def test_text_the_editor_would_rewrite_is_not_editable(self):
        for body, why in ((b"a\r\nb\r\n", "line endings"),
                          (b"\xef\xbb\xbfid,name\n", "byte-order mark"),
                          ("a b".encode(), "non-breaking"),
                          ("a b".encode(), "separators")):
            with self.subTest(why=why):
                dlg = self._dlg(self._Client(body=body))
                self.assertFalse(dlg._editable)
                self.assertTrue(dlg._edit_btn.isVisibleTo(dlg))
                self.assertFalse(dlg._edit_btn.isEnabled())
                self.assertIn(why, dlg._edit_btn.toolTip())
                dlg._toggle_edit()
                self.assertFalse(dlg._editing)

    def test_plain_lf_text_stays_editable(self):
        dlg = self._dlg(self._Client())
        self.assertTrue(dlg._editable)
        self.assertTrue(dlg._edit_btn.isEnabled())

    def test_save_sends_the_objects_own_properties_back(self):
        client = self._Client()
        dlg = self._dlg(client)
        dlg._toggle_edit()
        dlg._text.setPlainText(dlg._text.toPlainText().replace(
            "alpha", "ALPHA"))
        dlg._save()
        self._wait(dlg, "_save_thread")
        put = client.calls_of("put_object")[0]
        self.assertEqual(put["Body"], b"id,name\n1,ALPHA\n")
        self.assertEqual(put["IfMatch"], "abc")
        for field in ("ContentType", "CacheControl", "ContentLanguage",
                      "StorageClass", "ServerSideEncryption", "SSEKMSKeyId",
                      "BucketKeyEnabled", "Metadata"):
            self.assertEqual(put[field], self._Client.HEAD[field], field)
        self.assertEqual(put["Tagging"], "Team=Data")
        dlg._editing = False

    def test_unreadable_tags_refuse_the_save(self):
        client = self._Client(tag_error=botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}},
            "GetObjectTagging"))
        with self.assertRaises(RuntimeError):
            self._model(client).put_object_body(
                "a.csv", b"x", if_match='"abc"', preserve_properties=True)
        self.assertEqual(client.calls_of("put_object"), [])

    def test_a_backend_without_tagging_still_saves(self):
        client = self._Client(tag_error=botocore.exceptions.ClientError(
            {"Error": {"Code": "NotImplemented", "Message": "no"}},
            "GetObjectTagging"))
        self._model(client).put_object_body(
            "a.csv", b"x", if_match='"abc"', preserve_properties=True)
        self.assertNotIn("Tagging", client.calls_of("put_object")[0])

    def test_a_changed_object_is_refused_before_its_properties_are_copied(self):
        client = self._Client(head=dict(self._Client.HEAD, ETag='"other"'))
        with self.assertRaises(Model.PreconditionFailed):
            self._model(client).put_object_body(
                "a.csv", b"x", if_match='"abc"', preserve_properties=True)
        self.assertEqual(client.calls_of("put_object"), [])

    def test_the_profile_upload_defaults_do_not_retier_the_object(self):
        """A STANDARD object (HEAD names no class) stays STANDARD even when
        new uploads default to GLACIER."""
        client = self._Client(head={"ETag": '"abc"', "ContentType": "text/plain"})
        model = self._model(client)
        model.set_upload_options(storage_class="GLACIER", sse="AES256")
        model.put_object_body("a.txt", b"x", if_match='"abc"',
                              preserve_properties=True)
        put = client.calls_of("put_object")[0]
        self.assertNotIn("StorageClass", put)
        self.assertNotIn("ServerSideEncryption", put)
        self.assertEqual(put["ContentType"], "text/plain")
