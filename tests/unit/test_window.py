"""MainWindow and its mixins."""

import botocore.session
import contextlib
import inspect
import io
import main_window
import os
import s3duck
import shutil
import tempfile
import threading
import time
import types
import ui.context_menu as context_menu
import unittest
import utils
import workers.transfer as transfer_worker
from PyQt6.QtCore import QEvent, QPoint, QSettings, QThread, QUrl, Qt
from PyQt6.QtGui import QAction, QKeyEvent, QKeySequence, QShortcut
from PyQt6.QtWidgets import QApplication, QDialog, QMenu, QMessageBox
from main_window import (
    LIST_COLUMNS, LIST_OPTIONAL_COLUMNS, OverwriteDialog,
    build_profile_sync_job, collect_shortcuts, location_entries,
    parse_location, serialize_location,
)
from model import FSObjectType, Model, ReadOnlyError, TransferCancelled
from s3duck import SettingsItem, preselect_row
from settings_store import SettingsStore
from tests.support import (
    MIN_APP_SOURCES, PROJECT_ROOT, _FakeRunningThread, _RecordingDest,
    _StubModel, _ensure_qapp, make_main_window, patch_app_wide,
    read_app_sources,
)
from unittest.mock import patch


class SettingsAccessTests(unittest.TestCase):
    """QSettings groups are opened by a context manager, never by hand.

    A beginGroup whose endGroup is skipped -- by an early return, or by an
    exception between the two -- leaves the group open, and every later read
    and write in that session silently uses the wrong key. One site had two
    endGroup calls, one per exit path, and neither covered a raise.
    """

    def test_nothing_calls_beginGroup_or_endGroup_by_hand(self):
        offenders = []
        sources = read_app_sources()
        self.assertGreaterEqual(len(sources), MIN_APP_SOURCES,
                                "source discovery collapsed")
        for name, text in sources:
            if name == "settings_store.py":
                continue        # where the pairing is implemented
            for number, line in enumerate(text.splitlines(), 1):
                if ".beginGroup(" in line or ".endGroup(" in line:
                    offenders.append(f"{name}:{number}")
        self.assertEqual(offenders, [])

    def test_the_store_closes_its_group_when_the_body_raises(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        store = SettingsStore(settings, "prof")
        with self.assertRaises(RuntimeError):
            with store.group():
                raise RuntimeError("boom")
        # If the group were still open this would land under "common/".
        settings.setValue("at_top_level", "1")
        self.assertEqual(str(settings.value("at_top_level")), "1")
        self.assertIsNone(settings.value("common/at_top_level"))

    def test_per_profile_keys_all_share_one_spelling(self):
        """Three of the six used to omit the "default" fallback."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = make_main_window(PROJECT_ROOT, settings, profile_name="")
        self.addCleanup(win.close)
        keys = [win._binding_cache_settings_key(),
                win._bookmarks_settings_key(),
                win._last_location_key(),
                win._tabs_settings_key(),
                win._watch_settings_key(),
                win._saved_searches_key()]
        for key in keys:
            with self.subTest(key=key):
                self.assertTrue(key.endswith("/default"), key)

    def test_the_store_follows_a_profile_rename(self):
        """Switching profiles renames the window in place."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = make_main_window(PROJECT_ROOT, settings, profile_name="prod")
        self.addCleanup(win.close)
        self.assertEqual(win._saved_searches_key(), "searches/prod")
        win.profile_name = "dev"
        self.assertEqual(win._saved_searches_key(), "searches/dev")
class ContextMenuTests(unittest.TestCase):
    """REGRESSION (found while splitting eventFilter): the listing's context
    menu kept three hand-maintained lists -- the actions it added, the
    comparisons that dispatched them, and a tuple naming the ones to strip on
    a read-only profile. ``hold_action`` was in the last two and created by
    none, so the name was unbound. On a read-only profile that raised
    NameError while building the strip list, before the menu was shown at
    all; on a writable one it raised after the chosen entry had run but
    before Properties was dispatched, so right click -> Properties did
    nothing and every other entry printed a traceback.

    An entry now carries its own handler and its own read/write answer, so
    there is no second list to fall out of step with."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings,
                                   read_only=read_only)
        self.addCleanup(win.close)
        return win

    def _entries(self, win, *, read_only=False, bucket_mode=False,
                 kind=FSObjectType.FILE):
        """Build the menu without showing it, and report its labels."""
        menu = QMenu(win)
        builder = context_menu.MenuBuilder(win, menu)
        entry = types.SimpleNamespace(t=kind)
        shown = []
        with patch.object(context_menu.MenuBuilder, "run",
                          lambda self_, pos: shown.append(
                              [a.text() for a in menu.actions()]) or True):
            if bucket_mode:
                context_menu._bucket_list_menu(
                    win, builder, [object()], entry, not read_only, QPoint())
            else:
                context_menu._listing_menu(
                    win, builder, [object()], entry, "f.txt", "p/", "p/f.txt",
                    False, not read_only, QPoint())
        return shown[0]

    def test_every_entry_has_a_handler(self):
        """The failure mode was an entry no handler answered, and a handler
        no entry created."""
        win = self._window()
        menu = QMenu(win)
        builder = context_menu.MenuBuilder(win, menu)
        with patch.object(context_menu.MenuBuilder, "run",
                          lambda self_, pos: True):
            context_menu._listing_menu(
                win, builder, [object()],
                types.SimpleNamespace(t=FSObjectType.FILE),
                "f.txt", "p/", "p/f.txt", False, True, QPoint())
        added = [a for a in menu.actions() if not a.isSeparator()]
        self.assertTrue(added)
        for action in added:
            with self.subTest(action=action.text()):
                self.assertIn(action, builder._handlers)

    def test_the_legal_hold_entry_exists(self):
        """Bucket settings tells the user it is here."""
        labels = self._entries(self._window())
        self.assertTrue([l for l in labels if "legal hold" in l.lower()],
                        f"no legal hold entry in {labels}")

    def test_a_read_only_profile_still_gets_a_menu(self):
        """This raised NameError before the menu was ever shown."""
        labels = self._entries(self._window(read_only=True), read_only=True)
        self.assertIn("(read-only profile)", labels)

    def test_a_read_only_profile_keeps_the_entries_that_only_read(self):
        labels = self._entries(self._window(read_only=True), read_only=True)
        for kept in ("Download", "Properties", "Search here…"):
            with self.subTest(kept=kept):
                self.assertIn(kept, labels)

    def test_a_read_only_profile_loses_the_entries_that_write(self):
        labels = self._entries(self._window(read_only=True), read_only=True)
        for gone in ("Delete", "Create folder", "Rename…", "Legal hold…"):
            with self.subTest(gone=gone):
                self.assertNotIn(gone, labels)

    def test_copying_to_another_profile_survives_read_only(self):
        """It reads here and writes there, and the far side has its own
        read-only check."""
        labels = self._entries(self._window(read_only=True), read_only=True)
        self.assertIn("Copy to another profile…", labels)

    def test_the_bucket_list_menu_offers_bucket_operations(self):
        labels = self._entries(self._window(), bucket_mode=True,
                               kind=FSObjectType.BUCKET)
        for expected in ("Create bucket…", "Empty bucket…", "Delete bucket…"):
            with self.subTest(expected=expected):
                self.assertIn(expected, labels)
class MainWindowGuardTests(unittest.TestCase):
    """Window-level regressions: the shared-model race during bucket entry and
    QAction's 'checked' argument leaking into an optional slot parameter."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        # A private QSettings scope so tests never touch the real config.
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(self.win.close)
        self._settle()

    def _settle(self, timeout=5.0):
        # navigate() disables the list view and _on_navigation_finished
        # re-enables it, so that flag (not _nav_thread, which is only cleared
        # by the *next* navigate) marks the end of a navigation.
        deadline = time.monotonic() + timeout
        while (not self.win.listview.isEnabled()
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._app.processEvents()

    def test_entering_a_bucket_actually_loads_its_listing(self):
        """REGRESSION: the guard that blocks navigation during bucket entry
        also blocked the navigation the success handler itself starts — the
        worker emits success() before the thread stops running — so opening a
        bucket left the view on the bucket list."""
        self.assertTrue(self.win.in_bucket_list_mode())
        seq_before = self.win._nav_seq

        self.win.enter_bucket_async("bkt")
        deadline = time.monotonic() + 5
        while (self.win._nav_seq == seq_before
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._settle()

        self.assertEqual(self.win.data_model.bucket, "bkt")
        self.assertFalse(self.win.in_bucket_list_mode())
        self.assertGreater(self.win._nav_seq, seq_before)
        # the listing rendered: a bucket view always carries the [..] row
        self.assertIsNotNone(self.win.ix_by_name(main_window.UP_ENTRY_LABEL))
        self.assertTrue(self.win.listview.isEnabled())

    def test_double_clicking_a_bucket_row_opens_it(self):
        """The end-to-end path a user actually takes."""
        row = self.win.proxy.index(0, 0)
        self.assertEqual(row.data(), "bkt")
        seq_before = self.win._nav_seq

        self.win.list_doubleClicked(row)
        deadline = time.monotonic() + 5
        while (self.win._nav_seq == seq_before
               and time.monotonic() < deadline):
            self._app.processEvents()
        self._settle()

        self.assertEqual(self.win.data_model.bucket, "bkt")
        self.assertFalse(self.win.in_bucket_list_mode())

    def test_navigation_is_blocked_while_entering_a_bucket(self):
        """BucketEnterWorker mutates the shared model, so cloning it mid-entry
        would snapshot half-updated connection state."""
        self.win._bucket_enter_thread = _FakeRunningThread()
        self.assertTrue(self.win.bucket_enter_active())
        seq_before = self.win._nav_seq
        self.win.navigate()
        self.assertEqual(self.win._nav_seq, seq_before)  # no navigation started

        self.win._bucket_enter_thread = None
        self.assertFalse(self.win.bucket_enter_active())
        self.win.navigate()
        self.assertEqual(self.win._nav_seq, seq_before + 1)
        self._settle()

    def test_a_superseded_navigation_thread_is_not_forgotten(self):
        """FINDING (crash): navigate() dropped the previous nav QThread on the
        floor whether or not it had finished. The thread is a child of this
        window, so closing while a listing was still in flight had Qt destroy
        a running QThread, which aborts the process — an intermittent CI
        core dump with no failing test to show for it."""
        stale = _FakeRunningThread()
        self.win._nav_thread = stale

        self.win.navigate()
        self.addCleanup(self._settle)

        self.assertIn(stale, self.win._nav_orphan_threads)
        self.assertIsNot(self.win._nav_thread, stale)

    def test_closing_joins_a_superseded_navigation_thread(self):
        """Parking it is only half the fix: shutdown has to drain the list,
        otherwise the window still outlives a thread it owns."""
        stale = _FakeRunningThread()
        self.win._nav_orphan_threads = [stale]

        self.win._shutdown_threads()

        self.assertEqual(stale.quit_calls, 1)
        self.assertEqual(stale.wait_calls, [3000])

    def test_a_finished_navigation_thread_is_pruned(self):
        """The parking list must not grow for the lifetime of the window."""
        class _Finished(_FakeRunningThread):
            def isRunning(self):
                return False

        done = _Finished()
        self.win._nav_orphan_threads = [done]
        self.win._park_nav_thread()
        self.assertNotIn(done, self.win._nav_orphan_threads)

    def test_navigation_pins_its_worker_until_swept(self):
        """navigate() must route its NavigationWorker through the pin
        registry — the deleteLater it replaced is the deadlock pattern."""
        keys_before = set(utils._LIVE_WORKERS)
        self.win.navigate()
        self._settle()

        new = {k: v for k, v in utils._LIVE_WORKERS.items()
               if k not in keys_before}
        self.assertTrue(
            any(type(worker).__name__ == "NavigationWorker"
                for worker, _th in new.values()),
            f"no NavigationWorker pinned; new entries: {list(new)}")

        # Once its thread reports finished, a sweep lets it go.
        deadline = time.monotonic() + 5.0
        while any(k in utils._LIVE_WORKERS for k in new) \
                and time.monotonic() < deadline:
            self._app.processEvents()
            utils.reap_finished_workers()
        for k in new:
            self.assertNotIn(k, utils._LIVE_WORKERS)

    def test_shutdown_sweeps_pinned_workers(self):
        """_shutdown_threads is the last sweep point: a window must not exit
        leaving finished threads' workers pinned in the registry."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self.assertTrue(th.wait(5000))
        self.assertIn(id(wk), utils._LIVE_WORKERS)

        self.win._shutdown_threads()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_refresh_action_does_not_pass_checked_as_restore_name(self):
        """REGRESSION: triggered=self.navigate handed QAction's 'checked' bool
        to navigate(restore_name=...)."""
        self.win.btnRefresh.trigger()
        self.assertIsNone(self.win._nav_pending_restore_name)
        self._settle()

    def test_upload_actions_do_not_pass_checked_as_folder(self):
        """Same trap for upload(folder=...) / upload_folder(folder=...): the
        bool must never reach the destination-prefix parameter."""
        seen = []
        # Both actions are disabled in bucket-list mode; this test is about
        # argument passing, not enablement.
        self.win.btnUpload.setEnabled(True)
        self.win.btnUploadFolder.setEnabled(True)
        with patch.object(main_window.MainWindow, "upload",
                          lambda _s, folder=None: seen.append(("upload", folder))):
            self.win.btnUpload.trigger()
        with patch.object(main_window.MainWindow, "upload_folder",
                          lambda _s, folder=None: seen.append(("folder", folder))):
            self.win.btnUploadFolder.trigger()
        self.assertEqual(seen, [("upload", None), ("folder", None)])
class QuickOpenTests(unittest.TestCase):
    """Ctrl+P: reach a bucket or saved place by name instead of clicking back
    to the bucket list."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    BOOKMARKS = [
        {"name": "logs 2026", "bucket": "logs", "prefix": "2026/"},
        {"name": "", "bucket": "media", "prefix": ""},
    ]

    def test_bookmarks_come_before_buckets(self):
        """A saved place is almost always what someone is reaching for."""
        rows = location_entries(["assets", "logs"], self.BOOKMARKS)
        self.assertEqual([label for label, _h, _t in rows][:2],
                         ["logs 2026", "media"])

    def test_a_nameless_bookmark_falls_back_to_its_location(self):
        rows = location_entries([], [{"bucket": "media", "prefix": "raw/"}])
        self.assertEqual(rows, [("media/raw/", "media/raw/", ("media", "raw/"))])

    def test_the_open_bucket_is_not_offered(self):
        """Jumping to where you already are is not a useful result."""
        rows = location_entries(["assets", "logs"], [], current_bucket="logs")
        self.assertEqual([label for label, _h, _t in rows], ["assets"])

    def test_a_bookmark_for_the_open_bucket_is_still_offered(self):
        """Its prefix is a different place, even in the same bucket."""
        rows = location_entries([], self.BOOKMARKS, current_bucket="logs")
        self.assertIn("logs 2026", [label for label, _h, _t in rows])

    def test_duplicates_collapse(self):
        rows = location_entries(
            ["media", "media"], [{"bucket": "media", "prefix": ""}])
        self.assertEqual(len(rows), 1)

    def test_blank_entries_are_ignored(self):
        rows = location_entries(["", None], [{"bucket": ""}, None])
        self.assertEqual(rows, [])

    def test_targets_carry_the_bucket_and_prefix(self):
        rows = location_entries([], self.BOOKMARKS)
        self.assertEqual(rows[0][2], ("logs", "2026/"))

    def test_the_dialog_filters_with_the_palette_matcher(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], self.BOOKMARKS))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.count(), 4)
        dlg._query.setText("med")
        self.assertEqual(dlg._list.count(), 1)
        self.assertEqual(dlg.chosen_location(), ("media", ""))

    def test_arrow_keys_move_the_selection(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], []))
        self.addCleanup(dlg.close)
        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Down,
                          Qt.KeyboardModifier.NoModifier)
        self.assertTrue(dlg.eventFilter(dlg._query, event))
        self.assertEqual(dlg._list.currentRow(), 1)

    def test_the_highlighted_row_is_what_opens(self):
        """Not simply the first match — the arrows move the selection."""
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets", "logs"], []))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg.chosen_location(), ("assets", ""))
        dlg._list.setCurrentRow(1)
        self.assertEqual(dlg.chosen_location(), ("logs", ""))

    def test_nothing_is_chosen_when_nothing_matches(self):
        dlg = main_window.QuickOpenDialog(
            None, location_entries(["assets"], []))
        self.addCleanup(dlg.close)
        dlg._query.setText("zzzz")
        self.assertIsNone(dlg.chosen_location())

    def test_the_window_binds_ctrl_p(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        self.assertEqual(win._quick_open_shortcut.key(), QKeySequence("Ctrl+P"))

    def test_choosing_a_location_navigates_there(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        win._bookmarks = list(self.BOOKMARKS)
        jumped = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (["logs", "media"], None)), \
             patch.object(main_window.QuickOpenDialog, "exec",
                          lambda _s: QDialog.DialogCode.Accepted), \
             patch.object(main_window.QuickOpenDialog, "chosen_location",
                          lambda _s: ("logs", "2026/")), \
             patch.object(main_window.MainWindow, "go_to_bookmark",
                          lambda _s, entry: jumped.append(entry)):
            win.show_quick_open()
        self.assertEqual(jumped, [{"bucket": "logs", "prefix": "2026/"}])

    def test_a_failed_bucket_listing_still_offers_bookmarks(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        win._bookmarks = list(self.BOOKMARKS)
        shown = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, RuntimeError("no route"))), \
             patch_app_wide("QuickOpenDialog") as dlg_cls:
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Rejected
            win.show_quick_open()
            shown = list(dlg_cls.call_args.args[1])
        self.assertEqual([label for label, _h, _t in shown],
                         ["logs 2026", "media"])

    def test_cancelling_the_bucket_listing_opens_nothing(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        # Bookmarks exist, so "nothing to show" cannot stand in for the
        # cancellation check the way an empty list would.
        win._bookmarks = list(self.BOOKMARKS)
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, None)), \
             patch_app_wide("QuickOpenDialog") as dlg_cls:
            win.show_quick_open()
        dlg_cls.assert_not_called()
class CrossProfileSyncTests(unittest.TestCase):
    """Cross-profile copy landed first; syncing reuses the same planner by
    feeding it the two profiles' trees as source and destination."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_plan_becomes_copy_and_delete_rows(self):
        actions = [
            {"action": "upload", "rel": "a.txt", "size": 1, "reason": "missing"},
            {"action": "skip", "rel": "b.txt", "size": 1, "reason": "same"},
            {"action": "delete_remote", "rel": "old.txt", "size": 0,
             "reason": "not at source"},
        ]
        self.assertEqual(
            build_profile_sync_job(actions, "src/", "dst/"),
            [("copy", "src/a.txt", "dst/a.txt"),
             ("delete", "", "dst/old.txt")])

    def test_skips_never_become_work(self):
        actions = [{"action": "skip", "rel": "a", "size": 1, "reason": "same"}]
        self.assertEqual(build_profile_sync_job(actions, "", ""), [])

    def test_a_root_to_root_sync_keeps_bare_keys(self):
        actions = [{"action": "upload", "rel": "a/b.txt", "size": 1,
                    "reason": "missing"}]
        self.assertEqual(build_profile_sync_job(actions, "", ""),
                         [("copy", "a/b.txt", "a/b.txt")])

    def test_rows_without_a_path_are_ignored(self):
        self.assertEqual(
            build_profile_sync_job([{"action": "upload", "rel": ""}], "s/", "d/"),
            [])
        self.assertEqual(build_profile_sync_job(None, "s/", "d/"), [])

    def test_the_worker_copies_and_deletes_against_the_right_models(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        dest = _RecordingDest(bucket="dst")
        dest.deleted = []
        dest.delete = lambda key, log_fn=None, cancel_event=None: \
            dest.deleted.append(key)
        copied = []
        worker = main_window.Worker(
            source,
            [("copy", "src/a.txt", "dst/a.txt"), ("delete", "", "dst/old.txt")],
            dest_model=dest)
        with patch.object(Model, "copy_to_model",
                          lambda _s, src, model, dst, **kw:
                          copied.append((src, dst, model))):
            worker.sync_to_profile()
        self.assertEqual(copied, [("src/a.txt", "dst/a.txt", dest)])
        self.assertEqual(dest.deleted, ["dst/old.txt"])

    def test_the_worker_refuses_without_a_destination(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        worker = main_window.Worker(source, [("copy", "a", "b")])
        errors = []
        worker.error.connect(errors.append)
        worker.sync_to_profile()
        self.assertIn("destination", errors[0])

    def test_cancelling_stops_before_the_next_item(self):
        source = Model("https://a", "us-east-1", "AK", "SK", "src", False, False)
        dest = _RecordingDest(bucket="dst")
        worker = main_window.Worker(
            source, [("copy", "a", "a"), ("copy", "b", "b")], dest_model=dest)
        worker._cancel_event.set()
        done = []
        worker.finished.connect(lambda cancelled: done.append(cancelled))
        with patch.object(Model, "copy_to_model",
                          lambda *a, **kw: self.fail("copied after cancel")):
            worker.sync_to_profile()
        self.assertEqual(done, [True])

    def test_a_pull_reads_from_the_other_profile(self):
        """The queue entry carries its own source model, because a pull does
        not read from the window's model at all."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        other = Model("https://b", "us-east-1", "AK", "SK", "other", False, False)
        entry = main_window._QEntry(
            entry_id=1, method="sync_to_profile", job=[], source_model=other)
        self.assertIs(win._worker_model_for(entry), other)

    def test_an_ordinary_entry_still_gets_a_clone(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(root, settings)
        self.addCleanup(win.close)
        entry = main_window._QEntry(entry_id=1, method="upload", job=[])
        model = win._worker_model_for(entry)
        self.assertIsNot(model, win.data_model)
        self.assertIsInstance(model, Model)
class LocationRestoreTests(unittest.TestCase):
    """Bookmarks landed earlier; this is the other half — come back to where
    you were, in the profile you were using."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = PROJECT_ROOT

    def test_a_location_round_trips(self):
        for bucket, prefix in (("logs", "2026/08/"), ("logs", ""),
                               ("logs", "deep/nested/path/")):
            with self.subTest(bucket=bucket, prefix=prefix):
                text = serialize_location(bucket, prefix)
                self.assertEqual(parse_location(text), (bucket, prefix))

    def test_bucket_list_mode_stores_nothing(self):
        self.assertEqual(serialize_location("", ""), "")
        self.assertEqual(serialize_location(None, "x/"), "")
        self.assertEqual(parse_location(""), ("", ""))
        self.assertEqual(parse_location(None), ("", ""))

    def test_stray_slashes_do_not_change_the_meaning(self):
        self.assertEqual(parse_location("/logs/a/b/"), ("logs", "a/b/"))
        self.assertEqual(serialize_location("/logs/", "/a/"), "logs/a/")

    def test_a_prefix_without_a_trailing_slash_is_normalised(self):
        """Navigation treats a prefix as a folder, so the slash is not
        cosmetic — a hand-edited setting must still work."""
        self.assertEqual(parse_location("logs/a/b"), ("logs", "a/b/"))
        self.assertEqual(parse_location("logs"), ("logs", ""))

    def _win(self, profile="prof", settings=None):
        settings = settings or QSettings("s3duck-tests", "s3duck-tests")
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name=profile)
        self.addCleanup(win.close)
        return win

    def test_the_location_is_saved_per_profile(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = self._win("prod", settings)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/"
        win._save_last_location()
        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/2026/")
        self.assertIsNone(settings.value("last_location/dev"))
        settings.endGroup()

    def test_closing_the_window_persists_the_location(self):
        """Saved through writeSettings, so the hook itself is covered rather
        than only the helper it calls."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = self._win("prod", settings)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "deep/"
        win.close()
        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/deep/")
        settings.endGroup()

    def test_the_stored_location_is_reopened(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        settings.beginGroup("common")
        settings.setValue("last_location/prod", "logs/2026/")
        settings.endGroup()
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                          entered.append((name, target_prefix))):
            win = self._win("prod", settings)
            entered.clear()          # ignore whatever startup already did
            self.assertEqual(win.restore_last_location(), "logs")
        self.assertEqual(entered, [("logs", "2026/")])

    def test_no_stored_location_stays_on_the_bucket_list(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                          entered.append(name)):
            win = self._win("prod", settings)
            entered.clear()
            self.assertEqual(win.restore_last_location(), "")
        self.assertEqual(entered, [])

    def test_preselect_falls_back_when_the_profile_is_gone(self):
        """A deleted or renamed profile must not leave the list unselected."""
        items = [SettingsItem("dev", "u", "r", "", b"", b"", "false", "false",
                              b"", "false", ""),
                 SettingsItem("prod", "u", "r", "", b"", b"", "false", "false",
                              b"", "false", "")]
        self.assertEqual(preselect_row(items, "prod"), 1)
        self.assertEqual(preselect_row(items, "dev"), 0)
        self.assertEqual(preselect_row(items, "vanished"), 0)
        # An unnamed profile must not be matched by an empty remembered name.
        # It sits at index 1 on purpose: at index 0 the fallback and the bug
        # would both answer 0, and the test would prove nothing.
        unnamed = SettingsItem("", "u", "r", "", b"", b"", "false", "false",
                               b"", "false", "")
        mixed = [items[0], unnamed, items[1]]
        self.assertEqual(preselect_row(mixed, ""), 0)
        self.assertEqual(preselect_row(mixed, "prod"), 2)
        self.assertEqual(preselect_row(items, ""), 0)
        self.assertEqual(preselect_row(items, None), 0)
        self.assertEqual(preselect_row([], "prod"), -1)

    def test_the_launcher_preselects_the_remembered_profile(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        win = s3duck.Profiles()
        self.addCleanup(win.close)
        win.settings = settings
        win.items = [
            SettingsItem("dev", "u", "r", "", b"", b"", "false", "false",
                         b"", "false", ""),
            SettingsItem("prod", "u", "r", "", b"", b"", "false", "false",
                         b"", "false", ""),
        ]
        win.populate_list()
        win.listWidget.setCurrentIndex(
            win.listWidget.model().index(preselect_row(win.items, "prod"), 0))
        self.assertEqual(win.listWidget.currentRow(), 1)
class CrossProfileCopyTests(unittest.TestCase):
    """Server-side copy cannot use two sets of credentials, so moving objects
    between accounts or providers had no route at all."""

    def setUp(self):
        self.src = Model("https://s3.amazonaws.com", "us-east-1",
                         "AK", "SK", "srcbkt", False, False)
        self.dest = _RecordingDest()

    def _source_object(self, body=b"payload", **extra):
        resp = {"Body": io.BytesIO(body), "ContentLength": len(body)}
        resp.update(extra)
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: resp)
        return resp

    def test_an_object_is_read_here_and_written_there(self):
        self._source_object(b"hello")
        self.src.copy_to_model("a/b.txt", self.dest, "into/b.txt")
        self.assertEqual(self.dest.uploads,
                         [("dst", "into/b.txt", b"hello", None)])

    def test_content_type_and_metadata_survive_the_stream(self):
        """Streaming loses everything CopyObject would have preserved."""
        self._source_object(ContentType="text/csv",
                            Metadata={"owner": "vlad"},
                            ContentEncoding="gzip")
        self.src.copy_to_model("a.csv", self.dest, "a.csv")
        extra = self.dest.uploads[0][3]
        self.assertEqual(extra["ContentType"], "text/csv")
        self.assertEqual(extra["Metadata"], {"owner": "vlad"})
        self.assertEqual(extra["ContentEncoding"], "gzip")

    def test_the_destinations_upload_policy_wins(self):
        """Storage class and encryption belong to where the object lands."""
        self._source_object(ContentType="text/plain")
        self.dest.upload_extra_args = {"StorageClass": "GLACIER"}
        self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(self.dest.uploads[0][3]["StorageClass"], "GLACIER")

    def test_a_read_only_destination_is_refused(self):
        """The source profile's flag says nothing about the target's."""
        self._source_object()
        self.dest.read_only = True
        with self.assertRaises(ReadOnlyError):
            self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(self.dest.uploads, [])

    def test_a_read_only_source_can_still_copy_out(self):
        """Read-only protects the source's data; copying it elsewhere does not
        modify it, so it must not be blocked."""
        self.src.read_only = True
        self._source_object(b"x")
        self.src.copy_to_model("a.txt", self.dest, "a.txt")
        self.assertEqual(len(self.dest.uploads), 1)

    def test_cancelling_before_the_read_writes_nothing(self):
        self._source_object()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self.src.copy_to_model("a.txt", self.dest, "a.txt",
                                   cancel_event=cancel)
        self.assertEqual(self.dest.uploads, [])

    def test_a_prefix_keeps_its_shape(self):
        listed = [("src/a.txt", 1), ("src/sub/", 0), ("src/sub/b.txt", 2)]
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": io.BytesIO(b"z"),
                                     "ContentLength": 1})
        with patch.object(Model, "get_keys", lambda _s, p, **kw: listed):
            self.src.copy_prefix_to_model("src/", self.dest, "into/")
        self.assertEqual([key for _b, key, _d, _e in self.dest.uploads],
                         ["into/a.txt", "into/sub/b.txt"])
        self.assertEqual(self.dest.folders, ["into/sub/"])

    def test_a_prefix_can_land_at_the_bucket_root(self):
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": io.BytesIO(b"z"),
                                     "ContentLength": 1})
        with patch.object(Model, "get_keys",
                          lambda _s, p, **kw: [("src/a.txt", 1)]):
            self.src.copy_prefix_to_model("src/", self.dest, "")
        self.assertEqual(self.dest.uploads[0][1], "a.txt")

    def test_a_whole_bucket_can_be_copied_from_the_root(self):
        """prefix_of("") used to answer "/", which matches nothing on S3, so
        a root-to-root copy listed zero keys and reported success."""
        seen = []
        self.src._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": io.BytesIO(b"z"),
                                     "ContentLength": 1})
        with patch.object(Model, "get_keys",
                          lambda _s, p, **kw: seen.append(p) or [("a.txt", 1)]):
            self.src.copy_prefix_to_model("", self.dest, "")
        self.assertEqual(seen, [""], "the bucket root is the empty prefix")
        self.assertEqual([key for _b, key, _d, _e in self.dest.uploads],
                         ["a.txt"])

    def test_a_read_only_destination_refuses_a_whole_prefix(self):
        """And refuses it BEFORE listing: the per-file guard would also stop
        the copy, but only after a full recursive listing has been paid for."""
        self.dest.read_only = True
        listed = []
        with patch.object(Model, "get_keys",
                          lambda _s, p, **kw: listed.append(p) or [("a", 1)]):
            with self.assertRaises(ReadOnlyError):
                self.src.copy_prefix_to_model("src/", self.dest, "into/")
        self.assertEqual(self.dest.uploads, [])
        self.assertEqual(listed, [], "listed the prefix before refusing")

    def test_the_worker_refuses_a_job_with_no_destination(self):
        """A queued entry that lost its destination model must fail loudly
        rather than silently doing nothing."""
        worker = main_window.Worker(self.src, [("a.txt", "a.txt", False)])
        errors = []
        worker.error.connect(errors.append)
        worker.copy_to_profile()
        self.assertTrue(errors)
        self.assertIn("destination", errors[0])

    def test_the_worker_dispatches_files_and_folders(self):
        seen = []
        worker = main_window.Worker(
            self.src,
            [("f.txt", "to/f.txt", False), ("d/", "to/d/", True)],
            dest_model=self.dest)
        with patch.object(Model, "copy_to_model",
                          lambda _s, k, m, t, **kw: seen.append(("file", k, t))), \
             patch.object(Model, "copy_prefix_to_model",
                          lambda _s, k, m, t, **kw: seen.append(("dir", k, t))):
            worker.copy_to_profile()
        self.assertEqual(seen, [("file", "f.txt", "to/f.txt"),
                                ("dir", "d/", "to/d/")])
class UploadOverwriteTests(unittest.TestCase):
    """REGRESSION (silent data loss): download, copy/move, paste and rename all
    checked their destination, but every upload path replaced remote objects
    with no prompt."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(self.win.close)
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = ""

    @staticmethod
    @contextlib.contextmanager
    def answering(choice, accepted=True):
        """
        Drive the REAL OverwriteDialog's exec/choice.

        Patching the whole class instead would make `OverwriteDialog.SKIP` in
        the code under test a mock attribute, so a Skip assertion would pass
        even when the code took the Overwrite branch.
        """
        verdict = (QDialog.DialogCode.Accepted if accepted
                   else QDialog.DialogCode.Rejected)
        with patch.object(main_window.OverwriteDialog, "exec",
                          lambda _s: verdict), \
             patch.object(main_window.OverwriteDialog, "choice",
                          lambda _s: choice):
            yield

    def _run_upload(self, files, conflicts, choice=None, cancel_prompt=False):
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set(conflicts)), \
             self.answering(choice, accepted=not cancel_prompt), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (files, "All (*)")
            self.win.upload()
        return started

    def test_upload_skips_existing_objects_when_asked(self):
        started = self._run_upload(
            ["/tmp/a.txt", "/tmp/b.txt"], conflicts={"a.txt"},
            choice=OverwriteDialog.SKIP)
        self.assertEqual(len(started), 1)
        self.assertEqual([k for k, _local in started[0][1]], ["b.txt"])

    def test_upload_overwrite_keeps_everything(self):
        started = self._run_upload(
            ["/tmp/a.txt", "/tmp/b.txt"], conflicts={"a.txt"},
            choice=OverwriteDialog.OVERWRITE)
        self.assertEqual([k for k, _local in started[0][1]], ["a.txt", "b.txt"])

    def test_cancelling_the_prompt_uploads_nothing(self):
        self.assertEqual(
            self._run_upload(["/tmp/a.txt"], conflicts={"a.txt"},
                             choice=OverwriteDialog.OVERWRITE,
                             cancel_prompt=True),
            [])

    def test_no_conflicts_means_no_prompt(self):
        started = []
        shown = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.OverwriteDialog, "exec",
                          lambda _s: shown.append(1)), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (
                ["/tmp/a.txt"], "All (*)")
            self.win.upload()
        self.assertEqual(shown, [])
        self.assertEqual(len(started), 1)

    def test_a_failed_destination_scan_aborts(self):
        """_destination_conflicts returns None when the lookup failed or was
        cancelled — uploading anyway would be the silent overwrite again."""
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: None), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.return_value.getOpenFileNames.return_value = (
                ["/tmp/a.txt"], "All (*)")
            self.win.upload()
        self.assertEqual(started, [])

    def test_a_clean_job_still_asks_the_service_not_to_overwrite(self):
        """THE POINT: the destination check is a snapshot. Nothing conflicted
        when it was taken, which is not the same as nothing conflicting by
        the time the bytes arrive."""
        job = [("a.txt", "/tmp/a.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()):
            self.assertEqual(self.win._guard_upload(job), (job, True))

    def test_choosing_skip_keeps_the_precondition(self):
        job = [("a.txt", "/tmp/a.txt"), ("b.txt", "/tmp/b.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.SKIP):
            remaining, if_none_match = self.win._guard_upload(job)
        self.assertEqual(remaining, [("b.txt", "/tmp/b.txt")])
        self.assertTrue(if_none_match)

    def test_choosing_overwrite_drops_the_precondition(self):
        """Otherwise the service would refuse exactly the writes the user
        just asked for."""
        job = [("a.txt", "/tmp/a.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.OVERWRITE):
            self.assertEqual(self.win._guard_upload(job), (job, False))

    def test_the_choice_reaches_the_queued_job(self):
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append(kw)):
            fd.return_value.getOpenFileNames.return_value = (
                ["/tmp/a.txt"], "All (*)")
            self.win.upload()
        self.assertEqual([kw.get("if_none_match") for kw in started], [True])

    def test_folder_markers_are_not_treated_as_conflicts(self):
        """A directory placeholder only creates a prefix, so it must never be
        offered as an object about to be overwritten."""
        asked = []

        def _conflicts(_s, keys, bucket=None):
            asked.append(list(keys))
            return set()

        job = [("dir/", None), ("dir/f.txt", "/tmp/f.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          _conflicts):
            self.win._guard_upload(job)
        self.assertEqual(asked, [["dir/f.txt"]])

    def test_skipping_every_file_uploads_nothing(self):
        """Only folder markers left is not a job worth queueing."""
        job = [("dir/", None), ("dir/f.txt", "/tmp/f.txt")]
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"dir/f.txt"}), \
             self.answering(OverwriteDialog.SKIP):
            self.assertEqual(self.win._guard_upload(job), (None, False))

    def test_upload_folder_is_guarded(self):
        started = []
        with patch.object(main_window, "QFileDialog") as fd, \
             patch.object(main_window, "_build_upload_job_for_path",
                          lambda path, dest: [("a.txt", "/tmp/a.txt"),
                                              ("b.txt", "/tmp/b.txt")]), \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.SKIP), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            fd.getExistingDirectory.return_value = "/tmp/src"
            self.win.upload_folder()
        self.assertEqual([k for k, _l in started[0][1]], ["b.txt"])

    def test_drag_and_drop_is_guarded(self):
        """The third upload entry point — dropping files onto the list."""
        started = []
        event = types.SimpleNamespace(
            source=lambda: None,
            setDropAction=lambda _a: None,
            accept=lambda: None,
            ignore=lambda: None,
            mimeData=lambda: types.SimpleNamespace(
                hasUrls=lambda: True,
                urls=lambda: [QUrl.fromLocalFile("/tmp/a.txt")]),
        )
        with patch.object(main_window, "_build_upload_job_for_path",
                          lambda path, dest: [("a.txt", "/tmp/a.txt")]), \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"a.txt"}), \
             self.answering(OverwriteDialog.SKIP), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.listview.dropEvent(event)
        self.assertEqual(started, [])
class FolderDownloadOverwriteTests(unittest.TestCase):
    """REGRESSION: the download prompt covered file rows only, so downloading a
    folder overwrote a whole local tree with no warning."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(self.win.close)

    def test_download_target_resolves_folders_to_their_local_root(self):
        self.assertEqual(
            self.win._download_target(("k/f.txt", "/out/f.txt", 1, "/out")),
            "/out/f.txt")
        self.assertEqual(
            self.win._download_target(("pics/2024/", None, None, "/out")),
            os.path.join("/out", "2024"))

    def test_a_folder_with_existing_local_files_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "pics"))
            with open(os.path.join(tmp, "pics", "a.jpg"), "w") as fh:
                fh.write("x")
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: [("pics/a.jpg", 1)]):
                hits = self.win._folder_download_conflicts(
                    [("pics/", None, None, tmp)])
        self.assertEqual(hits, {os.path.join(tmp, "pics")})

    def test_a_folder_whose_files_are_absent_is_not_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "pics"))
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: [("pics/a.jpg", 1)]):
                hits = self.win._folder_download_conflicts(
                    [("pics/", None, None, tmp)])
        self.assertEqual(hits, set())

    def test_no_folders_means_no_scan(self):
        called = []
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: called.append(t) or (set(), None)):
            self.assertEqual(self.win._folder_download_conflicts([]), set())
        self.assertEqual(called, [])

    def test_a_cancelled_scan_aborts_the_download(self):
        with patch.object(main_window.MainWindow, "_run_with_progress",
                          lambda _s, t, fn: (None, None)):
            self.assertIsNone(self.win._folder_download_conflicts(
                [("pics/", None, None, "/out")]))

    def test_the_scan_lists_a_trailing_slashed_prefix(self):
        """Listing 'pics' instead of 'pics/' also matches the sibling prefix
        'pics-old/', so the check would scan the wrong objects."""
        asked = []
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(_StubModel, "get_keys",
                              lambda _s, p, **kw: asked.append(p) or []):
                self.win._folder_download_conflicts(
                    [("pics", None, None, tmp)])
        self.assertEqual(asked, ["pics/"])

    def _download_folder(self, choice, folder_hits):
        """Drive download() with a single folder row selected."""
        started = []
        index = types.SimpleNamespace(column=lambda: 0)
        item = types.SimpleNamespace(size=0)
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(main_window, "QFileDialog") as fd, \
                 patch.object(self.win.listview, "selectionModel",
                              lambda: types.SimpleNamespace(
                                  selectedIndexes=lambda: [index])), \
                 patch.object(main_window.MainWindow, "get_row_primary_item",
                              lambda _s, _ix: (item, "pics",
                                               FSObjectType.FOLDER)), \
                 patch.object(main_window.MainWindow,
                              "_folder_download_conflicts",
                              lambda _s, entries: {
                                  os.path.join(tmp, "pics")} if folder_hits
                              else set()), \
                 patch.object(main_window.OverwriteDialog, "exec",
                              lambda _s: QDialog.DialogCode.Accepted), \
                 patch.object(main_window.OverwriteDialog, "choice",
                              lambda _s: choice), \
                 patch.object(main_window.MainWindow,
                              "assign_thread_operation",
                              lambda _s, m, j, **kw: started.append((m, j))):
                fd.getExistingDirectory.return_value = tmp
                self.win.data_model.bucket = "bkt"   # else download() no-ops
                self.win.data_model.current_folder = ""
                self.win.download()
        return started

    def test_downloading_a_folder_onto_existing_files_can_be_skipped(self):
        """END-TO-END: the whole point — a folder download used to overwrite a
        local tree with no prompt at all."""
        self.assertEqual(
            self._download_folder(OverwriteDialog.SKIP, folder_hits=True), [])

    def test_downloading_a_folder_proceeds_when_overwrite_is_chosen(self):
        started = self._download_folder(
            OverwriteDialog.OVERWRITE, folder_hits=True)
        self.assertEqual(len(started), 1)
        self.assertEqual(started[0][1][0][0], "pics/")

    def test_a_clean_destination_downloads_without_a_prompt(self):
        started = self._download_folder(OverwriteDialog.SKIP, folder_hits=False)
        self.assertEqual(len(started), 1)
class QueueRowRetryButtonTests(unittest.TestCase):
    """REGRESSION (the test, not the code): this class was called
    QueueRetryTests, and so was another one further down the file. The second
    definition replaced the first at import, so this test never ran once --
    unittest only ever saw the surviving class of that name."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_retry_button_only_for_unfinished_jobs(self):
        entry = main_window._QEntry(1, "upload", [("k", "/tmp/f")], label="Upload")
        row = main_window._QueueRow(entry)
        self.addCleanup(row.deleteLater)
        self.assertFalse(row._retry_btn.isVisible())
        row.set_status("done")
        self.assertFalse(row._retry_btn.isVisible())
        row.set_status("error")
        self.assertTrue(row._retry_btn.isVisibleTo(row))
        row.set_status("cancelled")
        self.assertTrue(row._retry_btn.isVisibleTo(row))
class MainWindowBatchTests(unittest.TestCase):
    """Window-level behaviour of the new batch: selection summary, view-state
    persistence, binding-cache round-trip and queue retry."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _make_window(self, settings=None):
        settings = settings or QSettings("s3duck-tests", "s3duck-tests")
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(
                PROJECT_ROOT,
                settings,
                session_token='TOKEN')
        self.addCleanup(win.close)
        deadline = time.monotonic() + 5
        while not win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        return win

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        self.win = self._make_window(self.settings)

    def test_session_token_reaches_the_model(self):
        self.assertEqual(self.win.data_model.session_token, "TOKEN")

    def test_selection_summary_counts_multiple_rows(self):
        # bucket-list mode has a single bucket row -> no summary for one row
        self.assertEqual(self.win._selection_summary(), "")
        self.win.listview.selectAll()
        self.assertEqual(self.win._selection_summary(), "")  # still one bucket

    def test_binding_cache_survives_a_restart(self):
        self.win.data_model.binding_cache["bkt"] = (
            "https://s3.eu-north-1.amazonaws.com", "eu-north-1", True)
        self.win.close()          # writeSettings persists the cache
        again = self._make_window(self.settings)
        self.assertEqual(
            again.data_model.binding_cache.get("bkt"),
            ("https://s3.eu-north-1.amazonaws.com", "eu-north-1", True),
        )

    def test_view_state_survives_a_restart(self):
        self.win.listview.header().resizeSection(0, 411)
        self.win.close()
        again = self._make_window(self.settings)
        self.assertEqual(again.listview.header().sectionSize(0), 411)

    def test_retry_requeues_a_failed_entry(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation("upload", [("k", "/tmp/f")])
            self.assertEqual(len(started), 1)
            entry = started[0]
            entry.status = "error"
            self.win._on_queue_retry_requested(entry.entry_id)
        self.assertEqual(len(started), 2)
        self.assertEqual(started[1].job, entry.job)
        self.assertNotEqual(started[1].entry_id, entry.entry_id)

    def test_failed_transfer_is_marked_error_not_done(self):
        """REGRESSION: a transfer that raised still finished as "done", which
        also meant the queue offered no retry for it."""
        class FailingUpload:
            def upload_file(self, *_a, **_kw):
                raise RuntimeError("boom")

        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: FailingUpload()), \
             patch.object(main_window.QMessageBox, "critical",
                          lambda *_a, **_kw: None):
            self.win.assign_thread_operation(
                "upload", [("k", "/nonexistent/f")], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()

        entries = list(self.win._queue_entries.values())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].status, "error")
        self.assertIn("boom", entries[0].error or "")

    def test_retry_ignores_a_successful_entry(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation("upload", [("k", "/tmp/f")])
            started[0].status = "done"
            self.win._on_queue_retry_requested(started[0].entry_id)
        self.assertEqual(len(started), 1)

    def test_download_skips_existing_files_when_asked(self):
        job = [("a", "/tmp/exists", 1, "/tmp"), ("b", "/tmp/fresh", 1, "/tmp")]
        conflicts = {"/tmp/exists"}
        with patch_app_wide("OverwriteDialog") as dlg_cls:
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.choice.return_value = main_window.OverwriteDialog.SKIP
            remaining = self.win._resolve_overwrites(
                job, conflicts, what="file", index_of=lambda e: e[1])
        self.assertEqual([e[1] for e in remaining], ["/tmp/fresh"])

    def test_overwrite_choice_keeps_everything(self):
        job = [("a", "/tmp/exists", 1, "/tmp")]
        with patch_app_wide("OverwriteDialog") as dlg_cls:
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.choice.return_value = main_window.OverwriteDialog.OVERWRITE
            remaining = self.win._resolve_overwrites(
                job, {"/tmp/exists"}, what="file", index_of=lambda e: e[1])
        self.assertEqual(remaining, job)

    def test_cancelling_the_prompt_aborts(self):
        job = [("a", "/tmp/exists", 1, "/tmp")]
        with patch_app_wide("OverwriteDialog") as dlg_cls:
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Rejected
            self.assertIsNone(self.win._resolve_overwrites(
                job, {"/tmp/exists"}, what="file", index_of=lambda e: e[1]))

    def test_bulk_rename_builds_a_move_job(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = "pre/"
        targets = [("a.txt", "pre/a.txt", False), ("dir", "pre/dir/", True)]
        started = []
        with patch.object(main_window.MainWindow, "_collect_selected_targets",
                          lambda _s: targets), \
             patch.object(main_window, "BulkRenameDialog") as dlg_cls, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            instance = dlg_cls.return_value
            instance.exec.return_value = QDialog.DialogCode.Accepted
            instance.plan.return_value = [("a.txt", "b.txt"), ("dir", "folder")]
            self.win.bulk_rename()

        self.assertEqual(len(started), 1)
        method, job = started[0]
        self.assertEqual(method, "move")
        # folders keep their trailing slash on both sides; files do not
        self.assertEqual(job, [
            ("pre/a.txt", "pre/b.txt", False, None),
            ("pre/dir/", "pre/folder/", True, None),
        ])

    def test_bulk_rename_respects_overwrite_skip(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = ""
        targets = [("a.txt", "a.txt", False), ("b.txt", "b.txt", False)]
        started = []
        with patch.object(main_window.MainWindow, "_collect_selected_targets",
                          lambda _s: targets), \
             patch.object(main_window, "BulkRenameDialog") as dlg_cls, \
             patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: {"x.txt"}), \
             patch_app_wide("OverwriteDialog") as ow_cls, \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            dlg_cls.return_value.exec.return_value = QDialog.DialogCode.Accepted
            dlg_cls.return_value.plan.return_value = [
                ("a.txt", "x.txt"), ("b.txt", "y.txt")]
            ow_cls.return_value.exec.return_value = QDialog.DialogCode.Accepted
            ow_cls.return_value.choice.return_value = OverwriteDialog.SKIP
            self.win.bulk_rename()

        self.assertEqual([e[1] for e in started[0][1]], ["y.txt"])

    def test_start_sync_maps_actions_to_local_paths(self):
        started = []
        actions = [
            {"action": "upload", "rel": "a/b.txt", "size": 5},
            {"action": "delete_remote", "rel": "old.txt", "size": 1},
        ]
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.start_sync(actions, "/tmp/local", "pre/", "upload")
        method, job = started[0]
        self.assertEqual(method, "sync")
        self.assertEqual(job[0][0], "upload")
        self.assertEqual(job[0][3], "pre/a/b.txt")
        self.assertEqual(job[0][2], os.path.join("/tmp/local", "a", "b.txt"))
        self.assertEqual(job[1][3], "pre/old.txt")

    def test_queue_drain_reports_the_batch_once(self):
        self.win._batch_stats = {"done": 2, "cancelled": 0, "error": 1}
        shown = []
        with patch.object(main_window.MainWindow, "_ensure_tray_icon",
                          lambda _s: None):
            self.win.statusBar().showMessage = lambda msg, *a: shown.append(msg)
            self.win._queue_start_next()
        self.assertTrue(shown)
        self.assertIn("failed", shown[-1])
        # stats reset, so a second drain stays quiet
        shown.clear()
        self.win._queue_start_next()
        self.assertEqual(shown, [])

    def test_read_only_disables_write_actions_and_marks_the_title(self):
        self.win.data_model.read_only = True
        self.win.enable_action_buttons()
        self.win.update_window_title()
        self.assertFalse(self.win.btnRemove.isEnabled())
        self.assertFalse(self.win.btnCreateFolder.isEnabled())
        self.assertFalse(self.win.btnUpload.isEnabled())
        self.assertFalse(self.win.btnUploadFolder.isEnabled())
        self.assertIn("read-only", self.win.windowTitle())
        # An armed undo is itself a write, so read-only must take it away
        # again — check it is genuinely enabled first, or this proves nothing.
        self.win.data_model.bucket = "bkt"
        self.win._undo_delete = {"bucket": "bkt", "keys": ["a.txt"]}
        self.win.data_model.read_only = False
        self.win.enable_action_buttons()
        self.assertTrue(self.win.btnUndoDelete.isEnabled())
        self.win.data_model.read_only = True
        self.win.enable_action_buttons()
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        # reading stays available
        self.assertTrue(self.win.btnDownload.isEnabled())

    def test_writable_profile_keeps_write_actions(self):
        self.win.data_model.read_only = False
        self.win.data_model.bucket = "bkt"
        self.win.enable_action_buttons()
        self.win.update_window_title()
        self.assertTrue(self.win.btnRemove.isEnabled())
        self.assertTrue(self.win.btnUpload.isEnabled())
        self.assertNotIn("read-only", self.win.windowTitle())

    def test_a_real_delete_arms_undo(self):
        """The arming happens in the transfer-finished handler, so drive an
        actual delete job rather than calling the recorder directly."""
        class FakeDeleter:
            def delete(self, key, log_fn=None, cancel_event=None):
                return True

        self.win.data_model.bucket = "bkt"
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: FakeDeleter()):
            self.win.assign_thread_operation(
                "delete", ["a.txt", "dir/"], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()

        self.assertIsNotNone(self.win._undo_delete)
        self.assertEqual(self.win._undo_delete["keys"], ["a.txt", "dir/"])
        self.assertTrue(self.win.btnUndoDelete.isEnabled())

    def test_delete_arms_undo_and_undo_queues_a_job(self):
        self.win.data_model.bucket = "bkt"
        self.assertFalse(self.win.btnUndoDelete.isEnabled())
        self.win._record_undoable_delete(["a.txt", "dir/"])
        self.assertTrue(self.win.btnUndoDelete.isEnabled())

        started = []
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.undo_delete()
        self.assertEqual(started, [("undelete", [("a.txt",), ("dir/",)])])
        # a one-shot action: it disarms once used
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_undo_is_dropped_when_the_bucket_changes(self):
        self.win.data_model.bucket = "bkt"
        self.win._record_undoable_delete(["a.txt"])
        self.win._return_to_bucket_list_mode()
        self.assertIsNone(self.win._undo_delete)
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_undo_refuses_when_the_bucket_no_longer_matches(self):
        self.win.data_model.bucket = "bkt"
        self.win._record_undoable_delete(["a.txt"])
        self.win.data_model.bucket = "other"
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.undo_delete()
        self.assertEqual(started, [])

    def test_undo_stays_disabled_on_a_read_only_profile(self):
        self.win.data_model.bucket = "bkt"
        self.win.data_model.read_only = True
        self.win._record_undoable_delete(["a.txt"])
        self.assertFalse(self.win.btnUndoDelete.isEnabled())

    def test_progress_helper_returns_when_the_work_finishes_instantly(self):
        """REGRESSION (hang): the helper used QProgressDialog.exec(), but a
        worker that finished before exec() was entered had already called
        reset() — leaving the app frozen behind a dialog nothing would close.
        A small bucket made destination checks fast enough to hit it."""
        result, exc = self.win._run_with_progress("t", lambda _w: 42)
        self.assertEqual(result, 42)
        self.assertIsNone(exc)

    def test_progress_helper_surfaces_worker_errors(self):
        def _boom(_w):
            raise RuntimeError("nope")

        result, exc = self.win._run_with_progress("t", _boom)
        self.assertIsNone(result)
        self.assertIsInstance(exc, RuntimeError)

    def test_progress_helper_handles_slow_work(self):
        def _slow(_w):
            time.sleep(0.2)
            return "done"

        result, exc = self.win._run_with_progress("t", _slow)
        self.assertEqual(result, "done")
        self.assertIsNone(exc)

    def test_no_conflicts_runs_untouched(self):
        job = [("a", "/tmp/fresh", 1, "/tmp")]
        self.assertEqual(
            self.win._resolve_overwrites(
                job, set(), what="file", index_of=lambda e: e[1]),
            job,
        )
class TransferHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(self.win.close)
        deadline = time.monotonic() + 5
        while not self.win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def _entry(self, method="upload", job=None, status="done"):
        entry = main_window._QEntry(1, method, job or [("k", "/tmp/f")],
                                    label="Upload 1 item(s)")
        entry.status = status
        return entry

    def test_completed_job_is_recorded_newest_first(self):
        self.win._record_history(self._entry(), done_bytes=2048)
        self.win._record_history(
            self._entry(method="download", status="error"), done_bytes=10)
        history = self.win.load_transfer_history()
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["method"], "download")
        self.assertEqual(history[0]["status"], "error")
        self.assertEqual(history[1]["bytes"], 2048)

    def test_non_transfer_ops_do_not_inherit_stale_byte_counts(self):
        """REGRESSION: a delete recorded right after a big download showed the
        download's byte total, because _smooth_done was read unconditionally."""
        class NoopDeleter:
            def delete(self, key, log_fn=None, cancel_event=None):
                return True

        self.win._smooth_done = 12345          # residue of an earlier transfer
        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: NoopDeleter()):
            self.win.assign_thread_operation(
                "delete", ["a.txt"], need_refresh=False)
            deadline = time.monotonic() + 5
            while (self.win._active_entry is not None
                   and time.monotonic() < deadline):
                self._app.processEvents()
            self._app.processEvents()
        record = self.win.load_transfer_history()[0]
        self.assertEqual(record["method"], "delete")
        self.assertEqual(record["bytes"], 0)

    def test_small_jobs_are_rerunnable(self):
        self.win._record_history(self._entry())
        self.assertIn("job", self.win.load_transfer_history()[0])

    def test_huge_jobs_are_not_stored_for_rerun(self):
        big = [("k%d" % i, "/tmp/f%d" % i)
               for i in range(main_window.MainWindow.HISTORY_JOB_LIMIT + 1)]
        self.win._record_history(self._entry(job=big))
        record = self.win.load_transfer_history()[0]
        self.assertNotIn("job", record)
        self.assertEqual(record["items"], len(big))

    def test_history_is_capped(self):
        limit = main_window.MainWindow.HISTORY_LIMIT
        entries = [{"when": str(i), "method": "upload", "label": "x",
                    "items": 1, "bytes": 0, "status": "done"}
                   for i in range(limit + 25)]
        self.win._save_transfer_history(entries)
        self.assertEqual(len(self.win.load_transfer_history()), limit)

    def test_rerun_requeues_the_stored_job(self):
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.rerun_history_entry(
                {"method": "upload", "job": [["k", "/tmp/f"]]})
        # JSON turned the tuples into lists; they come back as tuples
        self.assertEqual(started, [("upload", [("k", "/tmp/f")])])

    def test_rerun_ignores_entries_without_a_job(self):
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.rerun_history_entry({"method": "upload"})
        self.assertEqual(started, [])

    def test_clear_empties_the_log(self):
        self.win._record_history(self._entry())
        self.win.clear_transfer_history()
        self.assertEqual(self.win.load_transfer_history(), [])

    def test_corrupt_history_is_ignored(self):
        self.settings.beginGroup("common")
        self.settings.setValue("transfer_history", "{not json")
        self.settings.endGroup()
        self.assertEqual(self.win.load_transfer_history(), [])

    def test_optional_columns_hidden_by_default_and_toggleable(self):
        for index in LIST_OPTIONAL_COLUMNS:
            self.assertTrue(self.win.listview.isColumnHidden(index))
        self.assertEqual(self.win.model.columnCount(), len(LIST_COLUMNS))
        self.win.listview.setColumnHidden(3, False)
        self.win.close()

        again_settings = self.settings
        with patch.object(main_window, "DataModel", _StubModel):
            again = make_main_window(PROJECT_ROOT, again_settings)
        self.addCleanup(again.close)
        # visibility survives a restart
        self.assertFalse(again.listview.isColumnHidden(3))
        self.assertTrue(again.listview.isColumnHidden(4))

    def test_shortcut_help_includes_actions_and_listview_keys(self):
        """REGRESSION: most QActions are parentless, so collecting only
        findChildren(QAction) left the toolbar out of the cheat sheet."""
        actions = list(self.win.tBar.actions()) + self.win.findChildren(QAction)
        rows = collect_shortcuts(actions, main_window.LISTVIEW_KEY_HELP)
        labels = [label for _k, label in rows]
        all_keys = " | ".join(k for k, _l in rows)
        for expected in ("Ctrl+D", "Ctrl+U", "Ctrl+Z", "Ctrl+L"):
            self.assertIn(expected, all_keys)  # real toolbar actions
        # Refresh binds two keys; both must be listed, not just the first
        self.assertIn("F5", all_keys)
        self.assertIn("Ctrl+R", all_keys)
        self.assertIn("Backspace", all_keys)   # an event-filter key
        self.assertIn("Ctrl+E", all_keys)      # a bare QShortcut
        self.assertTrue(any("Download" in l for l in labels))
        # labels lose their "(Ctrl+D)" hint
        self.assertNotIn("Download (Ctrl+D)", labels)
class ClipboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests", "s3duck-tests")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(self.win.close)
        deadline = time.monotonic() + 5
        while not self.win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = "from/"

    def _select(self, items):
        return patch.object(main_window.MainWindow, "_collect_selected_targets",
                            lambda _s: items)

    def test_copy_stores_the_selection_and_the_uris(self):
        items = [("a.txt", "from/a.txt", False)]
        with self._select(items):
            self.win.copy_to_clipboard()
        self.assertEqual(self.win._clipboard["mode"], "copy")
        self.assertEqual(self.win._clipboard["bucket"], "bkt")
        self.assertEqual(
            QApplication.clipboard().text(), "s3://bkt/from/a.txt")

    def test_cut_is_blocked_on_a_read_only_profile(self):
        self.win.data_model.read_only = True
        with self._select([("a.txt", "from/a.txt", False)]):
            self.win.copy_to_clipboard(cut=True)
        self.assertIsNone(self.win._clipboard)

    def test_paste_copies_into_the_current_folder(self):
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][0], "copy")
        self.assertEqual(started[0][1], [("from/a.txt", "to/a.txt", False, None)])
        # a copy stays on the clipboard for another paste
        self.assertIsNotNone(self.win._clipboard)

    def test_paste_after_cut_moves_and_clears_the_clipboard(self):
        started = []
        self.win._clipboard = {"mode": "cut", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][0], "move")
        self.assertIsNone(self.win._clipboard)

    def test_paste_across_buckets_sets_the_destination_bucket(self):
        """REGRESSION (critical): the copy/move workers resolve CopySource —
        and move's delete pass — against their model's bucket. A cross-bucket
        paste must therefore hand the SOURCE bucket to the transfer, or the
        copy reads from the destination bucket and a cut deletes an unrelated
        destination object that happens to share the key."""
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "other",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = ""
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j, kw))):
            self.win.paste_from_clipboard()
        method, job, kwargs = started[0]
        self.assertEqual(job[0][3], "bkt")                     # destination
        self.assertEqual(kwargs.get("source_bucket"), "other")  # source

    def test_same_bucket_paste_has_no_source_override(self):
        started = []
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        self.win.data_model.current_folder = "to/"
        with patch.object(main_window.MainWindow, "_destination_conflicts",
                          lambda _s, keys, bucket=None: set()), \
             patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j, kw))):
            self.win.paste_from_clipboard()
        self.assertEqual(started[0][2].get("source_bucket", ""), "")

    def test_worker_model_is_bound_to_the_source_bucket(self):
        """The entry's source_bucket must rebind the worker's model clone —
        bucket, navigation AND endpoint, since the clone inherits the
        destination bucket's (possibly virtual-host) endpoint binding."""
        self.win.data_model.current_folder = "deep/"
        entry = main_window._QEntry(
            1, "copy", [("from/a.txt", "a.txt", False, "bkt")],
            label="Copy", source_bucket="other")
        model = self.win._worker_model_for(entry)
        self.assertEqual(model.bucket, "other")
        self.assertEqual(model.current_folder, "")
        self.assertEqual(model.endpoint_url, model.profile_endpoint_url)
        self.assertIsNone(model._client)

        plain = main_window._QEntry(2, "copy", [("a", "b", False, None)],
                                    label="Copy")
        self.assertEqual(self.win._worker_model_for(plain).bucket, "bkt")

    def test_retry_keeps_the_source_bucket(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, entry: started.append(entry)):
            self.win.assign_thread_operation(
                "copy", [("from/a.txt", "a.txt", False, "bkt")],
                source_bucket="other")
            started[0].status = "error"
            self.win._on_queue_retry_requested(started[0].entry_id)
        self.assertEqual(len(started), 2)
        self.assertEqual(started[1].source_bucket, "other")

    def test_profile_switch_clears_the_clipboard(self):
        """A clipboard from another profile points at another account's
        objects; pasting it through new credentials must be impossible."""
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        prof = types.SimpleNamespace(
            name="p2", url="https://s3.amazonaws.com", region="us-east-1",
            access_key="AK2", secret_key="SK2", no_ssl_check=False,
            use_path=False, session_token="", read_only=False)
        self.win.apply_profile(prof)
        deadline = time.monotonic() + 5
        while not self.win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        self.assertIsNone(self.win._clipboard)

    def test_paste_is_blocked_when_read_only(self):
        started = []
        self.win.data_model.read_only = True
        self.win._clipboard = {"mode": "copy", "bucket": "bkt",
                               "items": [("a.txt", "from/a.txt", False)]}
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started, [])

    def test_bookmark_is_added_and_survives_a_restart(self):
        self.win.data_model.current_folder = "logs/2026/"
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("prod logs", True)):
            self.win.add_bookmark()
        self.assertEqual(len(self.win._bookmarks), 1)
        self.assertEqual(self.win._bookmarks[0]["prefix"], "logs/2026/")
        self.win.close()

        with patch.object(main_window, "DataModel", _StubModel):
            again = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(again.close)
        self.assertEqual(
            [b["name"] for b in again._bookmarks], ["prod logs"])

    def test_duplicate_location_is_not_bookmarked_twice(self):
        self.win.data_model.current_folder = "logs/"
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("one", True)):
            self.win.add_bookmark()
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("two", True)):
            self.win.add_bookmark()
        self.assertEqual(len(self.win._bookmarks), 1)

    def test_cancelling_the_name_prompt_adds_nothing(self):
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("", False)):
            self.win.add_bookmark()
        self.assertEqual(self.win._bookmarks, [])

    def test_bookmarks_need_an_open_bucket(self):
        self.win.data_model.bucket = ""
        with patch.object(main_window.QInputDialog, "getText",
                          return_value=("x", True)):
            self.win.add_bookmark()
        self.assertEqual(self.win._bookmarks, [])

    def test_same_bucket_bookmark_navigates_without_reopening(self):
        entered = []
        navigated = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                              entered.append((name, target_prefix))), \
             patch.object(main_window.MainWindow, "navigate",
                          lambda _s, **kw: navigated.append(kw)):
            self.win.go_to_bookmark(
                {"bucket": "bkt", "prefix": "logs/"})
        self.assertEqual(entered, [])
        self.assertEqual(self.win.data_model.current_folder, "logs/")
        self.assertEqual(len(navigated), 1)

    def test_other_bucket_bookmark_reopens_that_bucket(self):
        entered = []
        with patch.object(main_window.MainWindow, "enter_bucket_async",
                          lambda _s, name, target_prefix=None:
                              entered.append((name, target_prefix))):
            self.win.go_to_bookmark({"bucket": "other", "prefix": "deep/"})
        self.assertEqual(entered, [("other", "deep/")])

    def test_bookmark_menu_lists_saved_locations(self):
        self.win._bookmarks = [
            {"name": "one", "bucket": "bkt", "prefix": "a/"},
            {"name": "two", "bucket": "bkt2", "prefix": ""},
        ]
        self.win._rebuild_bookmark_menu()
        labels = [a.text() for a in self.win.bookmarkButton.menu().actions()
                  if not a.isSeparator()]
        self.assertIn("one", labels)
        self.assertIn("two", labels)
        self.assertTrue(any("Bookmark this location" in l for l in labels))

    def test_manage_dialog_edits_are_kept(self):
        self.win._bookmarks = [
            {"name": "one", "bucket": "bkt", "prefix": "a/"},
            {"name": "two", "bucket": "bkt2", "prefix": ""},
        ]
        dlg = main_window.BookmarksDialog(None, self.win._bookmarks)
        self.addCleanup(dlg.close)
        dlg._table.item(0, 0).setText("renamed")
        dlg._table.setCurrentCell(1, 0)
        dlg._remove()
        result = dlg.bookmarks()
        self.assertEqual([b["name"] for b in result], ["renamed"])
        self.assertEqual(result[0]["bucket"], "bkt")

    def test_paste_with_empty_clipboard_does_nothing(self):
        started = []
        self.win._clipboard = None
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: started.append((m, j))):
            self.win.paste_from_clipboard()
        self.assertEqual(started, [])
class KeyListActionTests(unittest.TestCase):
    """Main-window actions driven by an explicit key list rather than the
    listing selection."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False):
        settings = QSettings("s3duck-tests-keys", "s3duck-tests-keys")
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

    def test_delete_keys_queues_after_confirmation(self):
        win = self._window()
        queued = []
        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **kw: QMessageBox.StandardButton.Yes), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.delete_keys(["logs/a.txt", "logs/b.txt"])
        self.assertEqual(queued, [("delete", ["logs/a.txt", "logs/b.txt"])])

    def test_delete_keys_does_nothing_when_declined(self):
        win = self._window()
        queued = []
        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **kw: QMessageBox.StandardButton.No), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.delete_keys(["logs/a.txt"])
        self.assertEqual(queued, [])

    def test_delete_keys_refuses_on_a_read_only_profile(self):
        win = self._window(read_only=True)
        queued = []
        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **kw: QMessageBox.StandardButton.Yes), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.delete_keys(["logs/a.txt"])
        self.assertEqual(queued, [])

    def test_download_keys_keeps_the_prefix_tree(self):
        """Two results can share a basename; flattening would overwrite one
        with the other without a word."""
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        queued = []
        with patch.object(main_window.QFileDialog, "getExistingDirectory",
                          lambda *a, **kw: directory), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.download_keys(["logs/a/same.txt", "logs/b/same.txt"])
        method, job = queued[0]
        self.assertEqual(method, "download")
        self.assertEqual(
            [entry[1] for entry in job],
            [os.path.join(directory, "logs", "a", "same.txt"),
             os.path.join(directory, "logs", "b", "same.txt")])
        for entry in job:
            self.assertTrue(os.path.isdir(os.path.dirname(entry[1])))

    def test_download_keys_refuses_a_key_that_escapes_the_folder(self):
        """Search spans the whole bucket, so a key with '..' segments is
        exactly what turns up here."""
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        queued = []
        warned = []
        with patch.object(main_window.QFileDialog, "getExistingDirectory",
                          lambda *a, **kw: directory), \
                patch.object(main_window.QMessageBox, "warning",
                             lambda *a, **kw: warned.append(a)), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.download_keys(["../../escape.txt", "logs/ok.txt"])
        self.assertTrue(warned, "the skip was not reported")
        _method, job = queued[0]
        self.assertEqual([entry[0] for entry in job], ["logs/ok.txt"])
        for entry in job:
            self.assertTrue(
                os.path.abspath(entry[1]).startswith(
                    os.path.abspath(directory) + os.sep))

    def test_download_keys_ignores_folder_markers(self):
        win = self._window()
        queued = []
        with patch.object(main_window.QFileDialog, "getExistingDirectory",
                          lambda *a, **kw: ""), \
                patch.object(main_window.MainWindow, "assign_thread_operation",
                             lambda _s, m, j, **kw: queued.append((m, j))):
            win.download_keys(["logs/a/"])
        self.assertEqual(queued, [])
class LocationTabTests(unittest.TestCase):
    """Tabs are remembered locations for the one listing view — the model,
    the queue and the log stay shared."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, profile="prod"):
        settings = QSettings("s3duck-tests-tabs", "s3duck-tests-tabs")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name=profile)
        self.addCleanup(win.close)
        return win, settings

    def test_a_fresh_window_has_exactly_one_hidden_tab(self):
        win, _ = self._window()
        self.assertEqual(len(win._tabs), 1)
        # isHidden, not isVisible: the window itself is never shown in tests.
        self.assertTrue(win.tabbar.isHidden())

    def test_labels_name_the_deepest_segment(self):
        label = main_window.MainWindow.tab_label
        self.assertEqual(label("", ""), "buckets")
        self.assertEqual(label("logs", ""), "logs")
        self.assertEqual(label("logs", "2026/"), "2026")
        self.assertEqual(label("logs", "2026/09/"), "09")

    def test_a_new_tab_appears_after_the_current_one(self):
        win, _ = self._window()
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/"
        win._remember_current_tab()
        win.new_tab()
        self.assertEqual(len(win._tabs), 2)
        self.assertEqual(win.tabbar.currentIndex(), 1)
        self.assertFalse(win.tabbar.isHidden())

    def test_open_in_new_tab_switches_to_it(self):
        win, _ = self._window()
        opened = []
        with patch.object(main_window.MainWindow, "open_location",
                          lambda _s, b, p: opened.append((b, p))):
            win.open_in_new_tab("logs", "2026/")
        self.assertEqual(win.tab_locations()[-1], ("logs", "2026/"))
        self.assertEqual(opened, [("logs", "2026/")])

    def test_switching_tabs_navigates_to_what_it_remembers(self):
        win, _ = self._window()
        opened = []
        with patch.object(main_window.MainWindow, "open_location",
                          lambda _s, b, p: opened.append((b, p))):
            win.open_in_new_tab("logs", "2026/")
            win.open_in_new_tab("backups", "")
            opened.clear()
            win.tabbar.setCurrentIndex(1)
        self.assertEqual(opened, [("logs", "2026/")])

    def test_the_last_tab_never_closes(self):
        win, _ = self._window()
        win.close_tab()
        self.assertEqual(len(win._tabs), 1)

    def test_closing_a_tab_leaves_the_rest_in_order(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("a", "")
            win.open_in_new_tab("b", "")
            win.close_tab(1)
        self.assertEqual([b for b, _p in win.tab_locations()], ["", "b"])
        self.assertEqual(win.tabbar.count(), 2)

    def test_close_others_keeps_the_named_one(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("a", "")
            win.open_in_new_tab("b", "")
            win.close_other_tabs(2)
        self.assertEqual(win.tab_locations(), [("b", "")])
        self.assertEqual(win.tabbar.count(), 1)

    def test_next_tab_wraps_around(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("a", "")
            win.tabbar.setCurrentIndex(0)
            win.next_tab(1)
            self.assertEqual(win.tabbar.currentIndex(), 1)
            win.next_tab(1)
            self.assertEqual(win.tabbar.currentIndex(), 0)
            win.next_tab(-1)
            self.assertEqual(win.tabbar.currentIndex(), 1)

    def test_dragging_a_tab_reorders_the_locations(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("a", "")
            win.open_in_new_tab("b", "")
        win._on_tab_moved(0, 2)
        self.assertEqual([b for b, _p in win.tab_locations()],
                         ["a", "b", ""])

    def test_tabs_are_persisted_per_profile(self):
        win, settings = self._window()
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("logs", "2026/")
        settings.beginGroup("common")
        stored = settings.value("tabs/prod")
        settings.endGroup()
        self.assertIn("logs", str(stored))

        win._tabs = []
        win._load_tabs()
        self.assertEqual(win.tab_locations()[-1], ("logs", "2026/"))

    def test_a_corrupt_stored_set_falls_back_to_one_tab(self):
        win, settings = self._window()
        settings.beginGroup("common")
        settings.setValue("tabs/prod", "{not json")
        settings.endGroup()
        win._load_tabs()
        self.assertEqual(len(win._tabs), 1)

    def test_navigation_updates_the_active_tab(self):
        win, _ = self._window()
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/09/"
        win._remember_current_tab()
        self.assertEqual(win.tab_locations()[win.tabbar.currentIndex()],
                         ("logs", "2026/09/"))
        self.assertEqual(win.tabbar.tabText(win.tabbar.currentIndex()), "09")
class WatchModeTests(unittest.TestCase):
    """A folder mirrored up on an interval, built on the sync planner."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-watch", "s3duck-tests-watch")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        return win, settings

    def _config(self, local="/tmp/x"):
        return {"local": local, "bucket": "bkt", "prefix": "backup/",
                "interval": 300, "exclude": "", "delete_extra": False}

    def test_a_key_escaping_the_watched_folder_is_dropped_from_the_plan(self):
        """The one containment site the download_file backstop cannot cover:
        Worker.sync passes the file's OWN parent as folder_path, so if
        start_sync builds an escaping path nothing downstream refuses it."""
        win, _ = self._window()
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: queued.append(job)), \
             patch.object(main_window.QMessageBox, "warning",
                          lambda *a, **kw: None):
            win.start_sync(
                [{"action": "download", "rel": "../../escape.txt", "size": 1},
                 {"action": "download", "rel": "ok.txt", "size": 1}],
                "/tmp/target", "pre/", "download", bucket="bkt")
        self.assertEqual(len(queued), 1)
        rels = [entry[1] for entry in queued[0]]
        self.assertEqual(rels, ["ok.txt"])
        for _action, _rel, local_path, _key, _size in queued[0]:
            self.assertTrue(
                os.path.abspath(local_path).startswith(
                    os.path.abspath("/tmp/target") + os.sep),
                f"{local_path} escaped the target folder")

    def test_a_plan_that_is_entirely_unsafe_queues_nothing(self):
        win, _ = self._window()
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: queued.append(job)), \
             patch.object(main_window.QMessageBox, "warning",
                          lambda *a, **kw: None):
            win.start_sync(
                [{"action": "download", "rel": "../../escape.txt", "size": 1}],
                "/tmp/target", "pre/", "download", bucket="bkt")
        self.assertEqual(queued, [])

    def test_a_window_starts_with_no_watch(self):
        win, _ = self._window()
        self.assertFalse(win.watch_active())
        self.assertTrue(win.watch_status.isHidden())

    def test_starting_shows_the_indicator_and_arms_the_timer(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        self.assertTrue(win.watch_active())
        self.assertTrue(win._watch_timer.isActive())
        self.assertFalse(win.watch_status.isHidden())
        win.stop_watch()
        self.assertFalse(win._watch_timer.isActive())
        self.assertTrue(win.watch_status.isHidden())

    def test_the_settings_are_remembered_but_never_auto_started(self):
        """A background uploader that resumes unasked is how a folder gets
        mirrored somewhere the user forgot about."""
        win, settings = self._window()
        win._watch_config = self._config()
        win._save_watch_config()
        win._watch_config = {}
        win._load_watch_config()
        self.assertEqual(win._watch_config["prefix"], "backup/")
        self.assertFalse(win.watch_active())

    def test_a_corrupt_stored_config_is_ignored(self):
        win, settings = self._window()
        settings.beginGroup("common")
        settings.setValue("watch/prod", "{nope")
        settings.endGroup()
        win._load_watch_config()
        self.assertEqual(win._watch_config, {})

    def test_only_real_changes_are_queued(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        started = []
        with patch.object(main_window.MainWindow, "start_sync",
                          lambda _s, a, l, p, d, bucket="": started.append(
                              (a, l, p, d, bucket))):
            win._on_watch_planned([
                {"action": "skip", "rel": "a.txt", "size": 1},
                {"action": "upload", "rel": "b.txt", "size": 2},
            ], None)
        self.assertEqual(len(started), 1)
        self.assertEqual([e["rel"] for e in started[0][0]], ["b.txt"])
        self.assertEqual(started[0][3], "upload")

    def test_the_pass_is_queued_against_the_watched_bucket(self):
        """A queued job binds to a model when it STARTS. The watch fires on a
        timer, long after the dialog closed, so without carrying the bucket
        the mirror uploaded into whatever bucket was open by then — and with
        "delete extras" on, deleted from it."""
        win, _ = self._window()
        config = self._config()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(config)
        # The user has navigated somewhere else since the watch started.
        win.data_model.bucket = "some-other-bucket"
        started = []
        with patch.object(main_window.MainWindow, "start_sync",
                          lambda _s, a, l, p, d, bucket="": started.append(
                              bucket)):
            win._on_watch_planned(
                [{"action": "upload", "rel": "b.txt", "size": 2}], None)
        self.assertEqual(started, [config["bucket"]])

    def test_an_all_skip_pass_queues_nothing(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        started = []
        with patch.object(main_window.MainWindow, "start_sync",
                          lambda *a: started.append(a)):
            win._on_watch_planned([{"action": "skip", "rel": "a", "size": 1}],
                                  None)
        self.assertEqual(started, [])

    def test_a_failed_comparison_is_logged_not_raised(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        win._on_watch_planned(None, RuntimeError("no network"))
        self.assertFalse(win.transfers_active())

    def test_a_stopped_watch_ignores_a_late_result(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        win.stop_watch()
        started = []
        with patch.object(main_window.MainWindow, "start_sync",
                          lambda *a: started.append(a)):
            win._on_watch_planned(
                [{"action": "upload", "rel": "b.txt", "size": 2}], None)
        self.assertEqual(started, [])

    def test_a_tick_is_skipped_while_the_queue_is_busy(self):
        win, _ = self._window()
        with patch.object(main_window.MainWindow, "_watch_tick",
                          lambda _s: None):
            win.start_watch(self._config())
        with patch.object(main_window.MainWindow, "transfers_active",
                          lambda _s: True):
            win._watch_tick()
        self.assertIsNone(win._watch_thread)
class DualPaneTests(unittest.TestCase):
    """Two listings side by side, with F5/F6 running through the same queue."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False):
        settings = QSettings("s3duck-tests-panes", "s3duck-tests-panes")
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
        win.data_model.current_folder = "here/"
        return win

    def _queued(self):
        calls = []
        return calls, patch.object(
            main_window.MainWindow, "assign_thread_operation",
            lambda _s, method, job, **kw: calls.append((method, job, kw)))

    def test_the_pane_starts_hidden_and_toggles(self):
        win = self._window()
        self.assertFalse(win.dual_pane_active())
        win.toggle_dual_pane()
        self.assertTrue(win.dual_pane_active())
        win.toggle_dual_pane()
        self.assertFalse(win.dual_pane_active())

    def test_a_local_pane_lists_a_real_directory(self):
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        os.mkdir(os.path.join(directory, "sub"))
        with open(os.path.join(directory, "a.txt"), "w") as handle:
            handle.write("x")
        win.second_pane.set_local(directory)
        names = [name for name, _d, _s in win.second_pane.entries()]
        self.assertEqual(names, ["sub", "a.txt"])   # folders first

    def test_going_up_leaves_the_directory(self):
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        child = os.path.join(directory, "sub")
        os.mkdir(child)
        win.second_pane.set_local(child)
        win.second_pane.go_up()
        self.assertEqual(win.second_pane.local_target(),
                         os.path.abspath(directory))

    def test_remote_up_trims_one_prefix_segment(self):
        win = self._window()
        pane = win.second_pane
        with patch.object(main_window.SecondPane, "refresh", lambda _s: None):
            pane.set_remote("bkt", "a/b/")
            pane.go_up()
            self.assertEqual(pane.remote_target(), ("bkt", "a/"))
            pane.go_up()
            self.assertEqual(pane.remote_target(), ("bkt", ""))
            pane.go_up()
            self.assertEqual(pane.remote_target(), ("bkt", ""))

    def test_f5_without_the_pane_does_nothing(self):
        win = self._window()
        calls, patcher = self._queued()
        with patcher:
            win.pane_transfer()
        self.assertEqual(calls, [])

    def test_copy_to_a_local_pane_queues_a_download(self):
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        win.toggle_dual_pane()
        win.second_pane.set_local(directory)
        calls, patcher = self._queued()
        with patcher, patch.object(
                main_window.MainWindow, "_collect_selected_targets",
                lambda _s: [("a.txt", "here/a.txt", False)]):
            win.pane_transfer(move=False)
        method, job, _kw = calls[0]
        self.assertEqual(method, "download")
        self.assertEqual(job[0][0], "here/a.txt")
        self.assertEqual(job[0][1], os.path.join(directory, "a.txt"))

    def test_moving_to_a_local_pane_is_refused(self):
        win = self._window()
        win.toggle_dual_pane()
        win.second_pane.set_local(tempfile.gettempdir())
        calls, patcher = self._queued()
        with patcher, patch.object(
                main_window.MainWindow, "_collect_selected_targets",
                lambda _s: [("a.txt", "here/a.txt", False)]):
            win.pane_transfer(move=True)
        self.assertEqual(calls, [])

    def test_copy_to_a_remote_pane_crosses_buckets(self):
        win = self._window()
        win.toggle_dual_pane()
        with patch.object(main_window.SecondPane, "refresh", lambda _s: None):
            win.second_pane.set_remote("other", "dst/")
        calls, patcher = self._queued()
        with patcher, patch.object(
                main_window.MainWindow, "_collect_selected_targets",
                lambda _s: [("a.txt", "here/a.txt", False)]):
            win.pane_transfer(move=False)
        method, job, _kw = calls[0]
        self.assertEqual(method, "copy")
        self.assertEqual(job[0], ("here/a.txt", "dst/a.txt", False, "other"))

    def test_bringing_a_remote_selection_here_binds_the_source_bucket(self):
        """copy/move resolve CopySource against their model's bucket, so the
        worker has to run on the SOURCE side."""
        win = self._window()
        win.toggle_dual_pane()
        with patch.object(main_window.SecondPane, "refresh", lambda _s: None):
            win.second_pane.set_remote("other", "src/")
        win.second_pane._entries = [("a.txt", False, 10)]
        calls, patcher = self._queued()
        with patcher, patch.object(
                main_window.SecondPane, "selected",
                lambda _s: [("a.txt", False, 10)]), \
                patch.object(win.second_pane.view, "hasFocus",
                             lambda: True):
            win.pane_transfer(move=False)
        method, job, kw = calls[0]
        self.assertEqual(method, "copy")
        self.assertEqual(job[0], ("src/a.txt", "here/a.txt", False, "bkt"))
        self.assertEqual(kw.get("source_bucket"), "other")

    def test_uploading_a_local_selection_walks_folders(self):
        win = self._window()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        os.makedirs(os.path.join(directory, "sub", "deep"))
        for path in (("a.txt",), ("sub", "b.txt"), ("sub", "deep", "c.txt")):
            with open(os.path.join(directory, *path), "w") as handle:
                handle.write("x")
        win.toggle_dual_pane()
        win.second_pane.set_local(directory)
        calls, patcher = self._queued()
        with patcher, patch.object(
                main_window.SecondPane, "selected",
                lambda _s: [("a.txt", False, 1), ("sub", True, 0)]), \
                patch.object(win.second_pane.view, "hasFocus",
                             lambda: True):
            win.pane_transfer(move=False)
        method, job, _kw = calls[0]
        self.assertEqual(method, "upload")
        self.assertEqual(
            sorted(key for key, _path in job),
            ["here/a.txt", "here/sub/b.txt", "here/sub/deep/c.txt"])

    def test_a_read_only_profile_transfers_nothing(self):
        win = self._window(read_only=True)
        win.toggle_dual_pane()
        calls, patcher = self._queued()
        with patcher:
            win.pane_transfer()
        self.assertEqual(calls, [])
class TabRestoreOrderTests(unittest.TestCase):
    """The window opens wherever the remembered last location says, which is
    not necessarily tab 0."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-tabrestore", "s3duck-tests-tabrestore")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def test_the_first_navigation_selects_the_matching_tab(self):
        win = self._window()
        win._tabs = [{"bucket": "a", "prefix": ""},
                     {"bucket": "b", "prefix": "x/"},
                     {"bucket": "c", "prefix": ""}]
        win._sync_tab_bar()
        win._tab_restore_pending = True
        win.data_model.bucket = "b"
        win.data_model.current_folder = "x/"

        win._remember_current_tab()

        self.assertEqual(win.tabbar.currentIndex(), 1)
        # and nothing was overwritten
        self.assertEqual([b for b, _p in win.tab_locations()], ["a", "b", "c"])

    def test_an_unmatched_location_falls_back_to_recording_it(self):
        win = self._window()
        win._tabs = [{"bucket": "a", "prefix": ""}]
        win._sync_tab_bar()
        win._tab_restore_pending = True
        win.data_model.bucket = "z"

        win._remember_current_tab()

        self.assertEqual(win.tab_locations(), [("z", "")])

    def test_the_restore_only_happens_once(self):
        win = self._window()
        win._tabs = [{"bucket": "a", "prefix": ""},
                     {"bucket": "b", "prefix": ""}]
        win._sync_tab_bar()
        win._tab_restore_pending = True
        win.data_model.bucket = "b"
        win._remember_current_tab()
        self.assertEqual(win.tabbar.currentIndex(), 1)

        # a later navigation inside that tab records, rather than jumping
        win.data_model.current_folder = "deep/"
        win._remember_current_tab()
        self.assertEqual(win.tab_locations()[1], ("b", "deep/"))
class PaneShortcutConflictTests(unittest.TestCase):
    """F5 was already Refresh. Two live bindings for one key make Qt call it
    ambiguous and fire neither, so the commander keys are armed only while
    the second pane is on screen."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-keys2", "s3duck-tests-keys2")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    @staticmethod
    def _keys(action):
        return [sequence.toString() for sequence in action.shortcuts()]

    def test_single_pane_keeps_f5_on_refresh(self):
        win = self._window()
        self.assertIn("F5", self._keys(win.btnRefresh))
        self.assertFalse(win._pane_copy_shortcut.isEnabled())
        self.assertFalse(win._pane_move_shortcut.isEnabled())

    def test_dual_pane_hands_f5_to_the_panes(self):
        win = self._window()
        win.toggle_dual_pane()
        self.assertNotIn("F5", self._keys(win.btnRefresh))
        self.assertIn("Ctrl+R", self._keys(win.btnRefresh))
        self.assertTrue(win._pane_copy_shortcut.isEnabled())
        self.assertTrue(win._pane_move_shortcut.isEnabled())
        self.assertIn("Ctrl+R", win.btnRefresh.text())

    def test_closing_the_pane_gives_f5_back(self):
        win = self._window()
        win.toggle_dual_pane()
        win.toggle_dual_pane()
        self.assertIn("F5", self._keys(win.btnRefresh))
        self.assertFalse(win._pane_copy_shortcut.isEnabled())
        self.assertIn("F5", win.btnRefresh.text())

    def test_no_two_window_shortcuts_claim_the_same_key(self):
        """A regression net for the whole keymap, not just F5."""
        win = self._window()
        seen = {}
        for shortcut in win.findChildren(QShortcut):
            if not shortcut.isEnabled():
                continue
            seen.setdefault(shortcut.key().toString(), []).append("shortcut")
        for action in win.findChildren(QAction):
            for sequence in action.shortcuts():
                seen.setdefault(sequence.toString(), []).append(action.text())
        clashes = {key: owners for key, owners in seen.items()
                   if key and len(owners) > 1}
        self.assertEqual(clashes, {})
class NavigationHistoryTests(unittest.TestCase):
    """Back used to mean "the folder I was in a moment ago", one level, same
    bucket. Each tab now keeps a real trail."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-hist", "s3duck-tests-hist")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def _visit(self, win, bucket, prefix):
        win.data_model.bucket = bucket
        win.data_model.current_folder = prefix
        win._remember_current_tab()

    def test_a_fresh_tab_can_go_nowhere(self):
        win = self._window()
        self.assertFalse(win.can_go_back())
        self.assertFalse(win.can_go_forward())
        self.assertFalse(win.btnBack.isEnabled())
        self.assertFalse(win.btnForward.isEnabled())

    def test_each_move_is_recorded(self):
        win = self._window()
        self._visit(win, "bkt", "")
        self._visit(win, "bkt", "a/")
        self._visit(win, "bkt", "a/b/")
        entry = win.current_tab_entry()
        self.assertEqual(entry["history"][-3:],
                         [("bkt", ""), ("bkt", "a/"), ("bkt", "a/b/")])
        self.assertTrue(win.can_go_back())
        self.assertFalse(win.can_go_forward())

    def test_the_same_location_twice_is_recorded_once(self):
        win = self._window()
        self._visit(win, "bkt", "a/")
        before = len(win.current_tab_entry()["history"])
        self._visit(win, "bkt", "a/")
        self.assertEqual(len(win.current_tab_entry()["history"]), before)

    def test_back_then_forward_returns_to_the_same_place(self):
        win = self._window()
        self._visit(win, "bkt", "")
        self._visit(win, "bkt", "a/")
        visited = []
        with patch.object(main_window.MainWindow, "open_location",
                          lambda _s, b, p: visited.append((b, p))):
            win.goBack()
            self.assertEqual(visited[-1], ("bkt", ""))
            self.assertTrue(win.can_go_forward())
            win.goForward()
            self.assertEqual(visited[-1], ("bkt", "a/"))
            self.assertFalse(win.can_go_forward())

    def test_a_history_move_is_not_itself_recorded(self):
        """Otherwise Back would bounce between two entries forever."""
        win = self._window()
        self._visit(win, "bkt", "")
        self._visit(win, "bkt", "a/")
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.goBack()
        # the navigation the move causes lands here
        self._visit(win, "bkt", "")
        entry = win.current_tab_entry()
        # ("", "") is the bucket list the window opened on, and Back from the
        # first bucket legitimately returns to it.
        self.assertEqual(entry["history"],
                         [("", ""), ("bkt", ""), ("bkt", "a/")])
        self.assertEqual(entry["pos"], 1)
        self.assertTrue(win.can_go_forward())

    def test_a_new_move_after_going_back_drops_the_forward_branch(self):
        win = self._window()
        self._visit(win, "bkt", "")
        self._visit(win, "bkt", "a/")
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.goBack()
        self._visit(win, "bkt", "")      # the back landing
        self._visit(win, "bkt", "c/")    # a fresh move
        entry = win.current_tab_entry()
        self.assertEqual(entry["history"],
                         [("", ""), ("bkt", ""), ("bkt", "c/")])
        self.assertFalse(win.can_go_forward())

    def test_back_at_the_start_does_nothing(self):
        win = self._window()
        self._visit(win, "bkt", "a/")
        visited = []
        with patch.object(main_window.MainWindow, "open_location",
                          lambda _s, b, p: visited.append((b, p))):
            win.goBack()
            win.goBack()
            win.goBack()
        self.assertEqual(len(visited), 1)

    def test_history_crosses_buckets(self):
        win = self._window()
        self._visit(win, "one", "")
        self._visit(win, "two", "x/")
        visited = []
        with patch.object(main_window.MainWindow, "open_location",
                          lambda _s, b, p: visited.append((b, p))):
            win.goBack()
        self.assertEqual(visited[-1], ("one", ""))

    def test_each_tab_keeps_its_own_trail(self):
        win = self._window()
        self._visit(win, "bkt", "a/")
        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.open_in_new_tab("other", "")
        self._visit(win, "other", "z/")
        first, second = win._tabs[0], win._tabs[1]
        self.assertEqual(first["history"][-1], ("bkt", "a/"))
        self.assertEqual(second["history"][-1], ("other", "z/"))

    def test_the_trail_is_capped(self):
        entry = main_window.MainWindow._new_tab_entry("b", "")
        for index in range(main_window.MainWindow.HISTORY_DEPTH + 25):
            main_window.MainWindow.push_history(entry, ("b", f"{index}/"))
        self.assertEqual(len(entry["history"]),
                         main_window.MainWindow.HISTORY_DEPTH)
        self.assertEqual(entry["pos"], len(entry["history"]) - 1)

    def test_a_restored_tab_starts_with_a_clean_trail(self):
        win = self._window()
        self._visit(win, "bkt", "a/")
        win._save_tabs()
        win._load_tabs()
        entry = win._tabs[0]
        self.assertEqual(entry["history"], [("bkt", "a/")])
        self.assertFalse(win.can_go_back())
class FailureDetailTests(unittest.TestCase):
    """str(exc) throws away the request id, which is the first thing any
    provider's support desk asks for."""

    def _client_error(self):
        return botocore.exceptions.ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "no soup"},
                "ResponseMetadata": {
                    "HTTPStatusCode": 403,
                    "RequestId": "REQ123",
                    "HostId": "HOST456",
                    "RetryAttempts": 2,
                    "HTTPHeaders": {"x-amz-request-id": "REQ123",
                                    "x-amz-id-2": "HOST456"},
                },
            },
            "GetObject")

    def test_the_report_carries_what_support_asks_for(self):
        text = utils.describe_client_error(self._client_error())
        for fragment in ("AccessDenied", "no soup", "403", "REQ123",
                         "HOST456", "GetObject"):
            self.assertIn(fragment, text)

    def test_retry_count_is_included_when_there_was_one(self):
        self.assertIn("Retries: 2",
                      utils.describe_client_error(self._client_error()))

    def test_a_plain_exception_still_produces_something_useful(self):
        text = utils.describe_client_error(RuntimeError("disk full"))
        self.assertIn("disk full", text)
        self.assertIn("RuntimeError", text)

    def test_empty_metadata_is_not_rendered_as_blank_rows(self):
        exc = botocore.exceptions.ClientError(
            {"Error": {"Code": "Slow", "Message": ""},
             "ResponseMetadata": {}}, "PutObject")
        text = utils.describe_client_error(exc)
        self.assertNotIn("Request id:", text)
        self.assertNotIn("Message:", text)
        self.assertIn("Code: Slow", text)
class QueueFailureDetailTests(unittest.TestCase):
    """A failed row keeps its details so the job can still be reported
    accurately hours later."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-fail", "s3duck-tests-fail")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def test_a_fresh_entry_has_no_details(self):
        entry = main_window._QEntry(1, "upload", [("k", "/tmp/f")])
        self.assertEqual(entry.error_details, "")

    def test_a_failed_row_offers_the_details_button(self):
        entry = main_window._QEntry(1, "upload", [("k", "/tmp/f")])
        row = main_window._QueueRow(entry)
        self.addCleanup(row.deleteLater)
        self.assertTrue(row._details_btn.isHidden())
        row.set_status("error")
        self.assertFalse(row._details_btn.isHidden())
        row.set_status("done")
        self.assertTrue(row._details_btn.isHidden())

    def test_the_dialog_shows_and_copies_what_was_recorded(self):
        win = self._window()
        entry = main_window._QEntry(7, "download", [], label="logs/a.txt")
        entry.error = "AccessDenied"
        entry.error_details = "Code: AccessDenied\nRequest id: REQ123"
        win._queue_entries[7] = entry

        shown = {}

        class _Box:
            # The real class is also the namespace for Icon/ButtonRole, so a
            # stand-in has to carry them too.
            Icon = QMessageBox.Icon
            ButtonRole = QMessageBox.ButtonRole
            StandardButton = QMessageBox.StandardButton

            def __init__(self, _parent):
                pass

            def setWindowTitle(self, text):
                shown["title"] = text

            def setIcon(self, _icon):
                pass

            def setText(self, text):
                shown["text"] = text

            def setDetailedText(self, text):
                shown["details"] = text

            def addButton(self, *args):
                return object()

            def exec(self):
                return 0

            def clickedButton(self):
                return None

        with patch_app_wide("QMessageBox", _Box):
            win.show_failure_details(7)
        self.assertIn("logs/a.txt", shown["text"])
        self.assertIn("REQ123", shown["details"])

    def test_an_unknown_entry_is_ignored(self):
        win = self._window()
        win.show_failure_details(999)   # must not raise

    OPERATIONS = ("download", "upload", "delete", "copy", "move", "sync",
                  "sync_to_profile", "copy_to_profile", "delete_buckets",
                  "empty_buckets", "set_tags", "zip_download", "undelete",
                  "set_storage_class", "set_content_type", "restore")

    def test_every_operation_goes_through_the_one_failure_path(self):
        """Was: count the except-blocks and check they all emit details.

        There is one except-block now, in job_runner, so the thing to check
        is that every operation is wrapped by it -- which also means an
        operation cannot be added without the contract.
        """
        for name in self.OPERATIONS:
            with self.subTest(operation=name):
                method = getattr(main_window.Worker, name)
                self.assertTrue(
                    hasattr(method, "__wrapped__"),
                    f"{name} is not wrapped by job_runner")

    def test_the_failure_path_reports_all_three_ways(self):
        source = inspect.getsource(transfer_worker.job_runner)
        for expected in ("self.progress.emit", "self.details.emit",
                         "self.error.emit", "self.finished.emit"):
            with self.subTest(signal=expected):
                self.assertIn(expected, source)

    def test_a_failing_operation_reports_and_still_finishes(self):
        seen = {}
        worker = main_window.Worker(object(), [])
        worker.progress.connect(lambda m: seen.setdefault("progress", m))
        worker.details.connect(
            lambda text, transient: seen.setdefault("details", text))
        worker.error.connect(lambda m: seen.setdefault("error", m))
        worker.finished.connect(lambda c: seen.__setitem__("finished", c))

        @transfer_worker.job_runner("widget polish")
        def boom(_self):
            raise RuntimeError("the service said no")

        boom(worker)
        self.assertIn("widget polish failed", seen["progress"])
        self.assertEqual(seen["error"], "the service said no")
        self.assertIn("details", seen)
        self.assertIs(seen["finished"], False)

    def test_a_cancelled_operation_reports_nothing_and_finishes_cancelled(self):
        """The user asked for it, so it is not a failure."""
        seen = {}
        worker = main_window.Worker(object(), [])
        worker.error.connect(lambda m: seen.setdefault("error", m))
        worker.details.connect(
            lambda text, transient: seen.setdefault("details", text))
        worker.finished.connect(lambda c: seen.__setitem__("finished", c))

        @transfer_worker.job_runner("widget polish")
        def stop(_self):
            raise TransferCancelled("cancelled")

        stop(worker)
        self.assertNotIn("error", seen)
        self.assertNotIn("details", seen)
        self.assertIs(seen["finished"], True)

    def test_finished_fires_even_when_the_body_returns_normally(self):
        """finished is what releases the thread and re-enables the toolbar."""
        seen = {}
        worker = main_window.Worker(object(), [])
        worker.finished.connect(lambda c: seen.__setitem__("finished", c))

        @transfer_worker.job_runner("widget polish")
        def fine(_self):
            return None

        fine(worker)
        self.assertIs(seen["finished"], False)
class HistoryButtonStateTests(unittest.TestCase):
    """The toolbar has to say where the trail can go, not just carry the
    actions."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-histbtn", "s3duck-tests-histbtn")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def test_the_buttons_follow_the_trail(self):
        win = self._window()
        self.assertFalse(win.btnBack.isEnabled())
        self.assertFalse(win.btnForward.isEnabled())

        win.data_model.bucket = "bkt"
        win._remember_current_tab()
        win.data_model.current_folder = "a/"
        win._remember_current_tab()
        self.assertTrue(win.btnBack.isEnabled())
        self.assertFalse(win.btnForward.isEnabled())

        with patch.object(main_window.MainWindow, "open_location",
                          lambda *_a: None):
            win.goBack()
        self.assertTrue(win.btnForward.isEnabled())

    def test_both_actions_carry_their_shortcuts(self):
        win = self._window()
        self.assertEqual(
            [k.toString() for k in win.btnBack.shortcuts()], ["Alt+Left"])
        self.assertEqual(
            [k.toString() for k in win.btnForward.shortcuts()], ["Alt+Right"])
class QueueRetryTests(unittest.TestCase):
    """Retry-all, and one automatic retry for a failure the service asked us
    to repeat."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-retry", "s3duck-tests-retry")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def _entry(self, win, entry_id, status, transient=False):
        entry = main_window._QEntry(entry_id, "upload", [("k", "/tmp/f")],
                                    label=f"job {entry_id}")
        entry.status = status
        entry.error_transient = transient
        win._queue_entries[entry_id] = entry
        return entry

    def test_only_failed_jobs_are_collected(self):
        win = self._window()
        self._entry(win, 1, "done")
        self._entry(win, 2, "error")
        self._entry(win, 3, "cancelled")
        self._entry(win, 4, "error")
        self.assertEqual([e.entry_id for e in win.failed_entries()], [2, 4])

    def test_retry_all_requeues_each_one(self):
        win = self._window()
        self._entry(win, 1, "error")
        self._entry(win, 2, "error")
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: queued.append(m)):
            win.retry_failed_transfers()
        self.assertEqual(queued, ["upload", "upload"])

    def test_retry_all_with_nothing_failed_does_nothing(self):
        win = self._window()
        self._entry(win, 1, "done")
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: queued.append(m)):
            win.retry_failed_transfers()
        self.assertEqual(queued, [])

    def test_a_transient_failure_is_retried_once(self):
        win = self._window()
        entry = self._entry(win, 1, "error", transient=True)
        self.assertTrue(win._maybe_auto_retry(entry))
        self.assertEqual(entry.auto_retries, 1)
        # and never again
        self.assertFalse(win._maybe_auto_retry(entry))

    def test_a_refusal_is_not_retried(self):
        win = self._window()
        entry = self._entry(win, 1, "error", transient=False)
        self.assertFalse(win._maybe_auto_retry(entry))
        self.assertEqual(entry.auto_retries, 0)

    def test_the_setting_switches_it_off(self):
        win = self._window()
        win.settings.beginGroup("common")
        win.settings.setValue("auto_retry", "false")
        win.settings.endGroup()
        entry = self._entry(win, 1, "error", transient=True)
        self.assertFalse(win._maybe_auto_retry(entry))

    def test_it_is_on_by_default(self):
        self.assertTrue(self._window().auto_retry_enabled())

    def test_the_retry_carries_the_cross_profile_models(self):
        """Dropping them re-queued a job the worker could only refuse."""
        win = self._window()
        entry = self._entry(win, 1, "error")
        entry.source_bucket = "other"
        entry.dest_model = object()
        entry.source_model = object()
        seen = {}
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: seen.update(kw)):
            win._requeue(entry)
        self.assertEqual(seen["source_bucket"], "other")
        self.assertIs(seen["dest_model"], entry.dest_model)
        self.assertIs(seen["source_model"], entry.source_model)
class ProblemReportTests(unittest.TestCase):
    """Nobody reconstructs a Qt plugin list and the last failure's request id
    from memory."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-report", "s3duck-tests-report")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        # Never touch the real log file from a test.
        win.log_file = utils.LogFile("")
        return win

    def test_the_report_carries_diagnostics_and_the_log(self):
        win = self._window()
        win.log("something happened")
        text = win.build_problem_report()
        self.assertIn("SVG icons render", text)
        self.assertIn("something happened", text)
        self.assertIn("Log (last", text)

    def test_the_last_failure_is_included(self):
        win = self._window()
        entry = main_window._QEntry(3, "upload", [], label="logs/a.txt")
        entry.status = "error"
        entry.error_details = "Code: AccessDenied\nRequest id: REQ9"
        win._queue_entries[3] = entry
        text = win.build_problem_report()
        self.assertIn("Last failure", text)
        self.assertIn("REQ9", text)

    def test_a_clean_session_has_no_failure_section(self):
        win = self._window()
        self.assertNotIn("Last failure", win.build_problem_report())

    def test_the_report_never_carries_the_secret_key(self):
        win = self._window()
        win.data_model.secret_key = "SUPERSECRET"
        self.assertNotIn("SUPERSECRET", win.build_problem_report())

    def test_the_log_file_receives_what_the_view_shows(self):
        win = self._window()
        path = os.path.join(tempfile.mkdtemp(), "s.log")
        self.addCleanup(shutil.rmtree, os.path.dirname(path),
                        ignore_errors=True)
        win.log_file = utils.LogFile(path)
        win.log("written to both")
        self.assertIn("written to both", win.logview.toPlainText())
        self.assertIn("written to both", win.log_file.tail())
        # and it says which profile the line came from
        self.assertIn("[prod]", win.log_file.tail())
