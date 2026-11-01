"""Second pane navigation, keys, remote listings and the profile prompt."""

import os
import tempfile
import time
import types
import unittest
import ui.widgets.second_pane as second_pane
from datetime import datetime, timezone
from PyQt6.QtCore import QEvent, Qt
from PyQt6.QtGui import QKeyEvent
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox
from core.entries import Item
from model import FSObjectType
from tests.support import _ensure_qapp
from ui.widgets.second_pane import SecondPane
from unittest.mock import Mock, patch


class _Remote:
    """A model whose listing is a dict of prefix -> Items."""

    def __init__(self, listings=None, fail=None):
        self.bucket = ""
        self.current_folder = ""
        self.listings = listings or {}
        self.fail = fail
        self.listed = []

    def clone_for_worker(self):
        return self

    def list(self, prefix):
        self.listed.append((self.bucket, prefix))
        if self.fail:
            raise self.fail
        return self.listings.get(prefix, [])


def _key(key, modifiers=Qt.KeyboardModifier.NoModifier):
    return QKeyEvent(QEvent.Type.KeyPress, key, modifiers)


class SecondPanePathTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        os.mkdir(os.path.join(self.tmp.name, "inner"))
        with open(os.path.join(self.tmp.name, "f.txt"), "wb") as handle:
            handle.write(b"x")
        self.mw = types.SimpleNamespace(
            log=Mock(), settings=None, profile_name="me",
            pane_transfer_from_second=Mock())
        self.remote = _Remote({
            "": [Item("dir", FSObjectType.FOLDER, "", 0),
                 Item("top.txt", FSObjectType.FILE,
                      datetime(2026, 1, 1, tzinfo=timezone.utc), 4)],
            "dir/": [Item("deep.txt", FSObjectType.FILE, None, 1)]})
        self.pane = SecondPane(None, self.mw, self.remote)
        self.addCleanup(self.pane.shutdown)

    def _wait(self):
        deadline = time.monotonic() + 5
        while self.pane._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def names(self):
        return [n for n, _d, _s in self.pane.entries()]

    def test_a_remote_listing_runs_off_thread_and_navigates(self):
        self.pane.set_remote("bkt", "")
        self._wait()
        self.assertEqual(self.names(), ["dir", "top.txt"])
        self.assertTrue(self.pane.is_remote())
        self.assertTrue(self.pane.model.item(1, 2).text().startswith("2026-01-01"))
        self.pane._on_activated(self.pane.model.index(0, 0))
        self._wait()
        self.assertEqual(self.pane.remote_target(), ("bkt", "dir/"))
        self.assertEqual(self.names(), ["deep.txt"])
        self.pane.go_up()
        self._wait()
        self.assertEqual(self.pane.remote_target(), ("bkt", ""))
        self.pane.go_up()
        self.assertEqual(self.pane.remote_target(), ("bkt", ""))

    def test_a_failed_listing_is_shown(self):
        pane = SecondPane(None, self.mw, _Remote(fail=RuntimeError("denied")))
        self.addCleanup(pane.shutdown)
        pane.set_remote("bkt", "")
        deadline = time.monotonic() + 5
        while pane._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()
        self.assertIn("denied", pane.status.text())
        self.assertEqual(pane.entries(), [])

    def test_an_unreadable_local_folder_is_shown(self):
        with patch.object(second_pane.os, "listdir",
                          side_effect=PermissionError("Permission denied")):
            self.pane.set_local(self.tmp.name)
        self.assertIn("Permission denied", self.pane.status.text())
        self.assertEqual(self.pane.entries(), [])

    def test_no_bucket_and_no_profile(self):
        self.pane.set_remote("", "")
        self.assertEqual(self.pane.status.text(), "No bucket selected")
        self.pane._other_bucket = "b"
        self.pane._set_mode(SecondPane.OTHER)
        self.assertEqual(self.pane.status.text(), "No profile selected")

    def test_typed_locations(self):
        self.pane.set_local(self.tmp.name)
        self.pane.path.setText(os.path.join(self.tmp.name, "inner"))
        self.pane._go_typed()
        self.assertEqual(self.pane.local_target(),
                         os.path.join(self.tmp.name, "inner"))
        self.pane.path.setText("/definitely/not/here")
        self.pane._go_typed()
        self.assertEqual(self.pane.local_target(),
                         os.path.join(self.tmp.name, "inner"))
        self.pane.set_other_profile("theirs", _Remote(), "ob", "")
        self._wait()
        self.pane.path.setText("[theirs] s3://ob/sub/")
        self.pane._go_typed()
        self._wait()
        self.assertEqual(self.pane.remote_target(), ("ob", "sub/"))
        self.assertEqual(self.pane._other_model.bucket, "ob")

    def test_browse_in_each_mode(self):
        self.pane.set_local(self.tmp.name)
        with patch.object(second_pane.QFileDialog, "getExistingDirectory",
                          return_value=os.path.join(self.tmp.name, "inner")):
            self.pane._browse()
        self.assertTrue(self.pane.local_target().endswith("inner"))
        self.pane.set_remote("bkt", "")
        self._wait()
        with patch.object(second_pane.QInputDialog, "getText",
                          return_value=("s3://other/p/", True)):
            self.pane._browse()
        self._wait()
        self.assertEqual(self.pane.remote_target(), ("other", "p/"))
        with patch.object(second_pane.QInputDialog, "getText",
                          return_value=("", False)):
            self.pane._browse()
        self.assertEqual(self.pane.remote_target(), ("other", "p/"))
        self.pane.set_other_profile("theirs", _Remote(), "ob", "")
        self._wait()
        with patch.object(SecondPane, "choose_other_profile") as choose:
            self.pane._browse()
        choose.assert_called_once()

    def test_local_activation_and_keys(self):
        self.pane.set_local(self.tmp.name)
        with patch.object(second_pane.QDesktopServices, "openUrl") as opened:
            self.pane._on_activated(self.pane.model.index(1, 0))
        opened.assert_called_once()
        self.pane.view.setCurrentIndex(self.pane.model.index(0, 0))
        self.assertTrue(self.pane._handle_key(_key(Qt.Key.Key_Return)))
        self.assertTrue(self.pane.local_target().endswith("inner"))
        self.assertTrue(self.pane.eventFilter(self.pane.view, _key(Qt.Key.Key_Backspace)))
        self.assertEqual(self.pane.local_target(), self.tmp.name)
        for key, method in ((Qt.Key.Key_F2, "rename_selected"),
                            (Qt.Key.Key_Delete, "delete_selected"),
                            (Qt.Key.Key_F7, "create_folder")):
            with patch.object(SecondPane, method) as handler:
                self.assertTrue(self.pane._handle_key(_key(key)))
            handler.assert_called_once()
        self.assertFalse(self.pane._handle_key(_key(Qt.Key.Key_A)))

    def test_remote_keys_do_not_edit(self):
        self.pane.set_remote("bkt", "")
        self._wait()
        self.pane.view.setCurrentIndex(self.pane.model.index(-1, -1))
        self.assertFalse(self.pane._handle_key(_key(Qt.Key.Key_Return)))
        with patch.object(SecondPane, "delete_selected") as delete:
            self.assertFalse(self.pane._handle_key(_key(Qt.Key.Key_Delete)))
        delete.assert_not_called()
        self.assertIsNone(self.pane.rename_selected() or None)
        self.assertFalse(self.pane.create_folder())
        self.assertFalse(self.pane.delete_selected())

    def test_remote_context_menu_and_copy(self):
        self.pane.set_other_profile("theirs", self.remote, "bkt", "")
        self._wait()
        self.pane.select_names(["dir", "top.txt"])
        labels = [a.text() for a in self.pane.build_context_menu().actions() if a.text()]
        self.assertEqual(labels, ["Copy to the main pane (F5)", "Copy s3:// URI",
                                  "Choose profile…", "Refresh"])
        self.pane.copy_paths()
        self.assertEqual(QApplication.clipboard().text(),
                         "s3://bkt/dir/\ns3://bkt/top.txt")
        self.pane.set_local(self.tmp.name)
        self.pane.select_names(["f.txt"])
        self.pane.copy_paths()
        self.assertEqual(QApplication.clipboard().text(),
                         os.path.join(self.tmp.name, "f.txt"))
        with patch.object(second_pane.QDesktopServices, "openUrl") as opened:
            self.pane.reveal()
        opened.assert_called_once()

    def test_prompts_that_are_declined_change_nothing(self):
        self.pane.set_local(self.tmp.name)
        self.pane.select_names(["f.txt"])
        with patch.object(second_pane.QInputDialog, "getText",
                          return_value=("f.txt", True)):
            self.assertFalse(self.pane.rename_selected())
        with patch.object(second_pane.QInputDialog, "getText",
                          return_value=("", False)):
            self.assertFalse(self.pane.create_folder())

    def test_filesystem_refusals_are_reported(self):
        self.pane.set_local(self.tmp.name)
        with patch.object(second_pane.os, "rename", side_effect=OSError("busy")), \
                patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(self.pane.rename("f.txt", "g.txt"))
        with patch.object(second_pane.os, "mkdir", side_effect=OSError("full")), \
                patch.object(QMessageBox, "warning"):
            self.assertFalse(self.pane.make_folder("new"))
        warn.assert_called_once()
        with patch.object(second_pane.QFile, "moveToTrash", return_value=(False, "")), \
                patch.object(QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes), \
                patch.object(second_pane.os, "remove", side_effect=OSError("ro")), \
                patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(self.pane.trash(["f.txt", "../escape"]))
        text = warn.call_args.args[2]
        self.assertIn("f.txt: ro", text)
        self.assertIn("../escape", text)

    def test_choose_other_profile(self):
        profile = types.SimpleNamespace(name="theirs")
        picker = Mock()
        picker.destination_bucket.return_value = "ob"
        picker.destination_prefix.return_value = "p/"
        picker.selected_profile.return_value = profile
        dlg = types.SimpleNamespace(picker=picker, exec=lambda: QDialog.DialogCode.Accepted)
        with patch.object(second_pane, "PaneProfileDialog", return_value=dlg), \
                patch.object(second_pane, "build_profile_model",
                             return_value=_Remote()) as build:
            self.assertTrue(self.pane.choose_other_profile())
        self._wait()
        build.assert_called_once_with(profile, "ob")
        self.assertEqual(self.pane.other_profile(), "theirs")
        self.assertEqual(self.pane.remote_target(), ("ob", "p/"))

    def test_choose_other_profile_refusals(self):
        picker = Mock()
        dlg = types.SimpleNamespace(picker=picker,
                                    exec=lambda: QDialog.DialogCode.Rejected)
        with patch.object(second_pane, "PaneProfileDialog", return_value=dlg):
            self.assertFalse(self.pane.choose_other_profile())
        dlg.exec = lambda: QDialog.DialogCode.Accepted
        picker.destination_bucket.return_value = ""
        with patch.object(second_pane, "PaneProfileDialog", return_value=dlg), \
                patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(self.pane.choose_other_profile())
        warn.assert_called_once()
        picker.destination_bucket.return_value = "ob"
        picker.selected_profile.side_effect = RuntimeError("bad key")
        with patch.object(second_pane, "PaneProfileDialog", return_value=dlg), \
                patch.object(QMessageBox, "critical") as critical:
            self.assertFalse(self.pane.choose_other_profile())
        critical.assert_called_once()
        picker.selected_profile.side_effect = None
        picker.selected_profile.return_value = None
        with patch.object(second_pane, "PaneProfileDialog", return_value=dlg):
            self.assertFalse(self.pane.choose_other_profile())
        self.assertEqual(self.pane.current_mode(), SecondPane.LOCAL)

    def test_the_profile_prompt_dialog(self):
        with patch("ui.widgets.profile_picker.load_profiles", return_value=[]):
            dlg = second_pane.PaneProfileDialog(None, None, "me")
        self.assertIn("No other profile", dlg.picker.note.text())
        dlg.close()

    def test_choosing_other_in_the_combo_prompts(self):
        with patch.object(SecondPane, "choose_other_profile",
                          return_value=True) as choose:
            self.pane.mode.setCurrentIndex(self.pane.mode.findData(SecondPane.OTHER))
        choose.assert_called_once()


if __name__ == "__main__":
    unittest.main()
