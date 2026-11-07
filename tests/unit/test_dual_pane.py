"""The second pane: a local file manager half, and another profile's account."""

import main_window
import os
import tempfile
import types
import unittest
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox
from core.local_files import MODIFIED, NAME, SIZE, child_path, sort_rows, validate_new_name
from core.locations import CROSS_PROFILE_METHODS, build_profile_copy_job
from ui.dialogs.copy_move import CrossProfileCopyDialog
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from ui.widgets.second_pane import SecondPane
from unittest.mock import Mock, patch
from workers.transfer import Worker


class LocalFilesTests(unittest.TestCase):

    ROWS = [("b.txt", False, 10), ("A", True, 0), ("c.bin", False, 5),
            ("z", True, 0)]

    def test_folders_stay_first_in_either_direction(self):
        self.assertEqual([r[0] for r in sort_rows(self.ROWS, NAME)],
                         ["A", "z", "b.txt", "c.bin"])
        self.assertEqual([r[0] for r in sort_rows(self.ROWS, NAME, True)],
                         ["z", "A", "c.bin", "b.txt"])

    def test_size_and_modified(self):
        self.assertEqual([r[0] for r in sort_rows(self.ROWS, SIZE)][2:],
                         ["c.bin", "b.txt"])
        stamps = {"b.txt": 1, "c.bin": 2, "A": 5, "z": 3}
        self.assertEqual(
            [r[0] for r in sort_rows(self.ROWS, MODIFIED, True, stamps)],
            ["A", "z", "c.bin", "b.txt"])

    def test_names(self):
        self.assertEqual(validate_new_name("ok.txt"), "")
        for bad in ("", "  ", ".", "..", "a/b", "a\\b", "x:y", "trail.",
                    "trail ", "CON", "nul.txt", "q?"):
            self.assertTrue(validate_new_name(bad), bad)
        self.assertIn("already exists", validate_new_name("a", {"a"}))

    def test_child_path_refuses_an_escape(self):
        self.assertEqual(child_path("/tmp/x", "y"), os.path.join("/tmp/x", "y"))
        with self.assertRaises(ValueError):
            child_path("/tmp/x", "../y")

    def test_profile_copy_job(self):
        self.assertEqual(
            build_profile_copy_job(
                [("f", "src/f", False), ("d/", "src/d/", True), ("", "x", False)],
                "dst/"),
            [("src/f", "dst/f", False), ("src/d/", "dst/d/", True)])
        self.assertIn("move_to_profile", CROSS_PROFILE_METHODS)


class _FakeOther:
    """Another profile's model: enough for listing and conflict checks."""

    def __init__(self, read_only=False):
        self.bucket = "other-bkt"
        self.current_folder = ""
        self.read_only = read_only
        self.listed = []

    def clone_for_worker(self):
        return self

    def list(self, prefix):
        self.listed.append(prefix)
        return []


class SecondPaneLocalTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name, body in (("big.bin", b"x" * 100), ("small.txt", b"x")):
            with open(os.path.join(self.tmp.name, name), "wb") as handle:
                handle.write(body)
        os.mkdir(os.path.join(self.tmp.name, "folder"))
        self.mw = types.SimpleNamespace(
            log=Mock(), settings=None, profile_name="me",
            pane_transfer_from_second=Mock())
        model = types.SimpleNamespace(bucket="", current_folder="")
        self.pane = SecondPane(None, self.mw, model)
        self.pane.set_local(self.tmp.name)

    def names(self):
        return [n for n, _d, _s in self.pane.entries()]

    def test_sorting_by_size_keeps_folders_first(self):
        self.pane.sort_by(SIZE, descending=True)
        self.assertEqual(self.names(), ["folder", "big.bin", "small.txt"])
        self.pane._on_header_clicked(SIZE)
        self.assertEqual(self.pane.sort_state(), (SIZE, False))
        self.assertEqual(self.names(), ["folder", "small.txt", "big.bin"])

    def test_modified_column_is_filled(self):
        row = self.names().index("big.bin")
        self.assertTrue(self.pane.model.item(row, 2).text())

    def test_rename(self):
        self.assertTrue(self.pane.rename("small.txt", "renamed.txt"))
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "renamed.txt")))
        self.assertIn("renamed.txt", self.names())
        self.assertEqual([n for n, _d, _s in self.pane.selected()], ["renamed.txt"])

    def test_rename_refuses_a_taken_or_illegal_name(self):
        with patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(self.pane.rename("small.txt", "big.bin"))
            self.assertFalse(self.pane.rename("small.txt", "../escape"))
        self.assertEqual(warn.call_count, 2)
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "small.txt")))

    def test_rename_through_the_prompt(self):
        self.pane.select_names(["small.txt"])
        with patch("ui.widgets.second_pane.QInputDialog.getText",
                   return_value=("prompted.txt", True)):
            self.assertTrue(self.pane.rename_selected())
        self.assertIn("prompted.txt", self.names())

    def test_new_folder(self):
        with patch("ui.widgets.second_pane.QInputDialog.getText",
                   return_value=("made", True)):
            self.assertTrue(self.pane.create_folder())
        self.assertTrue(os.path.isdir(os.path.join(self.tmp.name, "made")))
        with patch.object(QMessageBox, "warning"):
            self.assertFalse(self.pane.make_folder("made"))

    def test_delete_goes_to_the_trash(self):
        self.pane.select_names(["small.txt"])
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes), \
             patch("ui.widgets.second_pane.QFile.moveToTrash",
                   return_value=(True, "")) as trash:
            self.assertTrue(self.pane.delete_selected())
        trash.assert_called_once_with(os.path.join(self.tmp.name, "small.txt"))

    def test_without_a_trash_deleting_needs_a_second_yes(self):
        answers = [QMessageBox.StandardButton.Yes, QMessageBox.StandardButton.No]
        self.pane.select_names(["small.txt"])
        with patch.object(QMessageBox, "question", side_effect=answers), \
             patch("ui.widgets.second_pane.QFile.moveToTrash",
                   return_value=(False, "")):
            self.pane.delete_selected()
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "small.txt")))
        answers = [QMessageBox.StandardButton.Yes, QMessageBox.StandardButton.Yes]
        with patch.object(QMessageBox, "question", side_effect=answers), \
             patch("ui.widgets.second_pane.QFile.moveToTrash",
                   return_value=(False, "")):
            self.pane.select_names(["folder"])
            self.pane.delete_selected()
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name, "folder")))

    def test_declining_the_delete_keeps_the_file(self):
        self.pane.select_names(["small.txt"])
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No), \
             patch("ui.widgets.second_pane.QFile.moveToTrash") as trash:
            self.assertFalse(self.pane.delete_selected())
        trash.assert_not_called()

    def test_context_menu_entries(self):
        self.pane.select_names(["small.txt"])
        labels = [a.text() for a in self.pane.build_context_menu().actions()
                  if a.text()]
        for wanted in ("Open", "Upload to the main pane (F5)", "Rename… (F2)",
                       "New folder… (F7)", "Move to trash (Del)", "Copy path",
                       "Show in file manager", "Refresh"):
            self.assertIn(wanted, labels)

    def test_other_profile_mode(self):
        other = _FakeOther()
        self.pane.set_other_profile("theirs", other, "other-bkt", "p/")
        self.assertEqual(self.pane.current_mode(), SecondPane.OTHER)
        self.assertEqual(self.pane.other_profile(), "theirs")
        self.assertEqual(self.pane.remote_target(), ("other-bkt", "p/"))
        self.assertIn("[theirs]", self.pane.location())
        self.assertEqual(self.pane.other_model().bucket, "other-bkt")
        self.pane.shutdown()
        self.pane.set_local(self.tmp.name)
        self.assertEqual(self.pane.other_profile(), "")
        self.assertIsNone(self.pane.other_model())

    def test_cancelling_the_profile_prompt_keeps_the_mode(self):
        with patch.object(SecondPane, "choose_other_profile", return_value=False):
            self.pane.mode.setCurrentIndex(self.pane.mode.findData(SecondPane.OTHER))
        self.assertEqual(self.pane.current_mode(), SecondPane.LOCAL)


class CrossProfilePaneTransferTests(unittest.TestCase):
    """F5/F6 with the second pane on another profile."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        settings = QSettings("s3duck-tests-pane", "s3duck-tests-pane")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, settings, bucket="bkt")
        self.addCleanup(self.win.close)
        self.win.data_model.bucket = "bkt"
        self.win.data_model.current_folder = "here/"
        self.win.second_pane.show()
        self.other = _FakeOther()
        self.win.second_pane.set_other_profile("theirs", self.other,
                                               "other-bkt", "there/")
        self.win.second_pane.shutdown()
        self.win.assign_thread_operation = Mock()
        self.win._destination_conflicts = Mock(return_value=set())
        self.win.in_bucket_list_mode = lambda: False
        self.win._collect_selected_targets = lambda: [("f.txt", "here/f.txt", False)]

    def test_main_to_other_is_a_copy_to_profile(self):
        self.win._pane_profile_transfer(False, False)
        call = self.win.assign_thread_operation.call_args
        self.assertEqual(call.args[0], "copy_to_profile")
        self.assertEqual(call.args[1], [("here/f.txt", "there/f.txt", False)])
        self.assertEqual(call.kwargs["dest_model"].bucket, "other-bkt")
        self.assertIsNone(call.kwargs["source_model"])

    def test_other_to_main_pulls_through_the_other_model(self):
        self.win.second_pane._show([("g.txt", False, 1), ("d", True, 0)])
        self.win.second_pane.select_names(["g.txt", "d"])
        self.win._pane_profile_transfer(True, False)
        call = self.win.assign_thread_operation.call_args
        self.assertEqual(call.args[0], "copy_to_profile")
        self.assertEqual(sorted(call.args[1]),
                         [("there/d/", "here/d/", True),
                          ("there/g.txt", "here/g.txt", False)])
        self.assertIs(call.kwargs["source_model"], self.other)
        self.assertTrue(call.kwargs["need_refresh"])

    def test_a_move_asks_first(self):
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            self.win._pane_profile_transfer(False, True)
        self.win.assign_thread_operation.assert_not_called()
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.win._pane_profile_transfer(False, True)
        self.assertEqual(self.win.assign_thread_operation.call_args.args[0],
                         "move_to_profile")

    def test_read_only_is_checked_per_side(self):
        self.other.read_only = True
        self.win._pane_profile_transfer(False, False)
        self.win.assign_thread_operation.assert_not_called()
        # Pulling from a read-only profile is fine; moving out of it is not.
        self.win.second_pane._show([("g.txt", False, 1)])
        self.win.second_pane.select_names(["g.txt"])
        self.win._pane_profile_transfer(True, False)
        self.assertEqual(self.win.assign_thread_operation.call_count, 1)
        self.win._pane_profile_transfer(True, True)
        self.assertEqual(self.win.assign_thread_operation.call_count, 1)

    def test_a_read_only_window_may_still_push_a_copy(self):
        self.win.is_read_only = lambda: True
        self.win._pane_profile_transfer(False, False)
        self.assertEqual(self.win.assign_thread_operation.call_count, 1)
        self.win._pane_profile_transfer(False, True)
        self.assertEqual(self.win.assign_thread_operation.call_count, 1)

    def test_pane_transfer_routes_other_mode(self):
        with patch.object(main_window.MainWindow, "_pane_profile_transfer") as route:
            self.win.pane_transfer(move=True)
        route.assert_called_once_with(False, True)

    def test_compare_is_refused_with_a_pointer(self):
        self.assertIsNone(self.win.pane_sides())
        with patch.object(main_window, "PaneCompareDialog") as dialog:
            self.win.compare_panes()
        dialog.assert_not_called()


class MoveToProfileWorkerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _source(self, fail_on=None):
        calls = []

        class Source:
            parallel_files = 1

            def copy_to_model(self, src, dest, dst, **_kw):
                if src == fail_on:
                    raise RuntimeError("copy refused")
                calls.append(("copy", src, dst))

            def copy_prefix_to_model(self, src, dest, dst, **_kw):
                calls.append(("copy_prefix", src, dst))
                return [src + "copied.txt"]

            def delete_exact(self, keys, **_kw):
                for key in keys:
                    self.delete(key)

            def delete(self, key, **_kw):
                calls.append(("delete", key))
        return Source(), calls

    def test_deletes_follow_every_copy(self):
        source, calls = self._source()
        dest = types.SimpleNamespace(bucket="d")
        worker = Worker(source, [("a", "x/a", False), ("f/", "x/f/", True)],
                        dest_model=dest)
        worker.move_to_profile()
        self.assertEqual(calls, [("copy", "a", "x/a"),
                                 ("copy_prefix", "f/", "x/f/"),
                                 ("delete", "a"), ("delete", "f/copied.txt")])

    def test_a_failed_copy_deletes_nothing(self):
        source, calls = self._source(fail_on="b")
        dest = types.SimpleNamespace(bucket="d")
        worker = Worker(source, [("a", "x/a", False), ("b", "x/b", False)],
                        dest_model=dest)
        errors = []
        worker.error.connect(errors.append)
        worker.move_to_profile()
        self.assertTrue(errors)
        self.assertNotIn("delete", [c[0] for c in calls])


class CopyDialogMoveTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_dialog_offers_a_move(self):
        settings = QSettings("s3duck-tests-pane", "s3duck-tests-pane")
        dlg = CrossProfileCopyDialog(None, settings, 2)
        self.assertFalse(dlg.move_requested())
        dlg.move.setChecked(True)
        self.assertTrue(dlg.move_requested())
        dlg.picker.stop_loader()


if __name__ == "__main__":
    unittest.main()
