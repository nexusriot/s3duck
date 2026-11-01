"""Delete-to-trash, and the download direction of the folder watch."""

import main_window
import os
import tempfile
import unittest
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox
from core.sync_plan import local_delete_guard
from core.trash import TRASH_PREFIX, in_trash, restore_rows, split_for_trash
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from ui.dialogs.compare import WatchFolderDialog
from unittest.mock import Mock, patch


class TrashCoreTests(unittest.TestCase):

    def test_split(self):
        moves, deletes = split_for_trash(
            ["a.txt", "dir/", ".trash/old.txt", ""], {"a.txt": "STANDARD_IA"})
        self.assertEqual(moves, [
            ("a.txt", ".trash/a.txt", False, None, "STANDARD_IA"),
            ("dir/", ".trash/dir/", True, None, None)])
        self.assertEqual(deletes, [".trash/old.txt"])

    def test_restore(self):
        self.assertEqual(
            restore_rows([".trash/x/y.txt", ".trash/d/", "plain", TRASH_PREFIX]),
            [(".trash/x/y.txt", "x/y.txt", False, None, None),
             (".trash/d/", "d/", True, None, None)])
        self.assertTrue(in_trash(".trash/a"))
        self.assertFalse(in_trash("a/.trash/b"))


class LocalDeleteGuardTests(unittest.TestCase):

    def _actions(self, deletes, uploads=0):
        return ([{"action": "delete_local"}] * deletes
                + [{"action": "download"}] * uploads)

    def test_small_passes_are_allowed(self):
        self.assertEqual(local_delete_guard(self._actions(5), 10), (True, 5))
        self.assertEqual(local_delete_guard(self._actions(0, 99), 10), (True, 0))

    def test_a_large_share_is_refused(self):
        self.assertEqual(local_delete_guard(self._actions(6), 10), (False, 6))
        self.assertEqual(local_delete_guard(self._actions(100), 1000), (True, 100))
        self.assertEqual(local_delete_guard(self._actions(101), 1000), (False, 101))


class _Window(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-trash", "s3duck-tests-trash")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings, bucket="bkt")
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        win.assign_thread_operation = Mock()
        return win


class TrashWindowTests(_Window):

    def test_off_by_default_deletes_for_real(self):
        win = self._window()
        self.assertFalse(win.trash_enabled())
        self.assertEqual(win.delete_verb(["a"]), "Delete")
        self.assertEqual(win.queue_delete(["a"]), (0, 1))
        win.assign_thread_operation.assert_called_once_with("delete", ["a"])

    def test_on_moves_into_the_trash(self):
        win = self._window()
        win.set_trash_enabled(True)
        self.assertTrue(win.actUseTrash.isChecked())
        self.assertEqual(win.delete_verb(["a"]), f"Move to {TRASH_PREFIX}")
        self.assertEqual(win.delete_verb([".trash/a"]), "Delete")
        self.assertEqual(win.queue_delete(["a", ".trash/b"]), (1, 1))
        calls = [c.args for c in win.assign_thread_operation.call_args_list]
        self.assertEqual(calls[0][0], "move")
        self.assertEqual(calls[0][1][0][1], ".trash/a")
        self.assertEqual(calls[1], ("delete", [".trash/b"]))

    def test_the_setting_is_per_profile(self):
        win = self._window()
        win.set_trash_enabled(True)
        win.profile_name = "other"
        self.assertFalse(win.trash_enabled())

    def test_restore_checks_the_destination(self):
        win = self._window()
        win._destination_conflicts = Mock(return_value=set())
        self.assertTrue(win.restore_from_trash([".trash/a.txt"]))
        win._destination_conflicts.assert_called_once_with(["a.txt"])
        args = win.assign_thread_operation.call_args.args
        self.assertEqual(args[0], "move")
        self.assertEqual(args[1][0][:2], (".trash/a.txt", "a.txt"))
        self.assertFalse(win.restore_from_trash(["not-trash.txt"]))

    def test_empty_trash_asks_with_the_size(self):
        win = self._window()
        win._run_with_progress = Mock(return_value=((3, 3000), None))
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No) as ask:
            self.assertFalse(win.empty_trash())
        self.assertIn("3 object(s)", ask.call_args.args[2])
        win.assign_thread_operation.assert_not_called()
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.assertTrue(win.empty_trash())
        win.assign_thread_operation.assert_called_once_with("delete", [TRASH_PREFIX])

    def test_an_empty_trash_has_nothing_to_ask(self):
        win = self._window()
        win._run_with_progress = Mock(return_value=((0, 0), None))
        self.assertFalse(win.empty_trash())


class DownloadWatchTests(_Window):

    def _config(self, folder, **kw):
        config = {"local": folder, "interval": 60, "exclude": "",
                  "delete_extra": True, "direction": "download",
                  "bucket": "bkt", "prefix": "p/"}
        config.update(kw)
        return config

    def test_a_small_pass_is_fetched_down(self):
        win = self._window()
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(main_window.MainWindow, "_watch_tick", lambda _s: None):
                win.start_watch(self._config(folder))
            started = []
            with patch.object(main_window.MainWindow, "start_sync",
                              lambda _s, a, l, p, d, bucket="": started.append(d)):
                win._on_watch_planned({"plan": [
                    {"action": "download", "rel": "a", "size": 1},
                    {"action": "delete_local", "rel": "b", "size": 1}],
                    "local_count": 20}, None)
        self.assertEqual(started, ["download"])

    def test_a_pass_that_would_empty_the_folder_stops_the_watch(self):
        win = self._window()
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(main_window.MainWindow, "_watch_tick", lambda _s: None):
                win.start_watch(self._config(folder))
            with patch.object(main_window.MainWindow, "start_sync") as start:
                win._on_watch_planned({"plan": [
                    {"action": "delete_local", "rel": str(i), "size": 1}
                    for i in range(50)], "local_count": 50}, None)
        start.assert_not_called()
        self.assertFalse(win.watch_active())

    def test_a_read_only_profile_may_mirror_down(self):
        win = self._window()
        win.is_read_only = lambda: True
        with tempfile.TemporaryDirectory() as folder:
            dlg = Mock()
            dlg.exec.return_value = 1
            dlg.config.return_value = self._config(folder)
            with patch("ui.window.watch.WatchFolderDialog", return_value=dlg), \
                 patch.object(main_window.MainWindow, "start_watch") as start:
                win.open_watch()
            start.assert_called_once()
            dlg.config.return_value = self._config(folder, direction="upload")
            with patch("ui.window.watch.WatchFolderDialog", return_value=dlg), \
                 patch.object(main_window.MainWindow, "start_watch") as start:
                win.open_watch()
            start.assert_not_called()


class WatchDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_direction_and_confirmation(self):
        with tempfile.TemporaryDirectory() as folder:
            dlg = WatchFolderDialog(None, "bkt", "p/", {
                "local": folder, "direction": "download", "delete_extra": True})
            self.assertEqual(dlg.direction(), "download")
            self.assertIn("Delete local files", dlg._delete_extra.text())
            self.assertIn("refused", dlg._guard_note.text())
            with patch.object(QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.No):
                dlg._accept()
            self.assertEqual(dlg.result(), 0, "accepted without the confirmation")
            with patch.object(QMessageBox, "question",
                              return_value=QMessageBox.StandardButton.Yes):
                dlg._accept()
            self.assertEqual(dlg.config()["direction"], "download")
            self.assertTrue(os.path.isdir(dlg.config()["local"]))
            dlg._direction.setCurrentIndex(0)
            self.assertEqual(dlg._guard_note.text(), "")


if __name__ == "__main__":
    unittest.main()
