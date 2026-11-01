"""The profile list: filter, reorder, last used, duplicate names, last probe."""

import s3duck
import types
import unittest
from PyQt6.QtCore import QEvent, QSettings, Qt
from PyQt6.QtGui import QKeyEvent
from PyQt6.QtWidgets import QDialogButtonBox
from s3duck import (
    SettingsItem, UNREACHABLE_BADGE, describe_probe, filter_profile_rows,
    move_item, name_taken, order_by_last_used, profile_badges,
)
from settings import SettingsWindow
from tests.support import _ensure_qapp
from unittest.mock import patch


def _item(name, url="https://s3.amazonaws.com", region="us-east-1", bucket=""):
    return SettingsItem(name, url, region, bucket, b"", b"", "false", "false",
                        b"", "false")


class ProfileListHelperTests(unittest.TestCase):

    ITEMS = [_item("prod", region="eu-west-1"), _item("dev"),
             _item("minio", url="http://minio.lan:9000", bucket="backups")]

    def test_filter_matches_every_word_anywhere(self):
        self.assertEqual(filter_profile_rows(self.ITEMS, ""), [0, 1, 2])
        self.assertEqual(filter_profile_rows(self.ITEMS, "PROD eu"), [0])
        self.assertEqual(filter_profile_rows(self.ITEMS, "backups"), [2])
        self.assertEqual(filter_profile_rows(self.ITEMS, "lan 9000"), [2])
        self.assertEqual(filter_profile_rows(self.ITEMS, "nothing"), [])

    def test_name_taken(self):
        self.assertTrue(name_taken(self.ITEMS, " dev "))
        self.assertFalse(name_taken(self.ITEMS, "dev", exclude_index=1))
        self.assertFalse(name_taken(self.ITEMS, "staging"))

    def test_order_by_last_used(self):
        ordered = order_by_last_used(self.ITEMS, {"minio": 20, "dev": 10})
        self.assertEqual([i.name for i in ordered], ["minio", "dev", "prod"])
        self.assertEqual([i.name for i in order_by_last_used(self.ITEMS, {})],
                         ["prod", "dev", "minio"])

    def test_move_item(self):
        items = list("abc")
        self.assertEqual(move_item(items, 0, 1), 1)
        self.assertEqual(items, ["b", "a", "c"])
        self.assertEqual(move_item(items, 0, -1), 0)
        self.assertEqual(move_item(items, 2, 1), 2)
        self.assertEqual(items, ["b", "a", "c"])

    def test_probe_badge_and_description(self):
        item = _item("x")
        self.assertNotIn(UNREACHABLE_BADGE, profile_badges(item))
        item.probe = {"state": "refused", "when": "t", "reason": "timeout"}
        self.assertIn(UNREACHABLE_BADGE, profile_badges(item))
        self.assertIn("FAILED", describe_probe(item.probe))
        self.assertIn("timeout", describe_probe(item.probe))
        self.assertIn("OK", describe_probe({"state": "ok", "when": "t"}))
        self.assertEqual(describe_probe(None), "Never connected from here")


class ProfileListWindowTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, names):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests-list", "s3duck-tests-list")
        win.settings.clear()
        self.addCleanup(win.settings.clear)
        win.items = [_item(n) for n in names]
        win.populate_list()
        if win.items:
            win.listWidget.setCurrentRow(0)
        self.addCleanup(win.close)
        return win

    def _shown(self, win):
        return [win.listWidget.item(r).text().split("\n")[0]
                for r in range(win.listWidget.count())]

    def test_the_filter_appears_with_a_long_list_and_maps_rows(self):
        short = self._launcher(["a", "b"])
        self.assertTrue(short.filterEdit.isHidden())
        win = self._launcher([f"p{i}" for i in range(7)] + ["special"])
        self.assertFalse(win.filterEdit.isHidden())
        win.filterEdit.setText("spec")
        self.assertEqual(self._shown(win), ["special"])
        self.assertEqual(win._current_item_index(), 7,
                         "a filtered row acted on the wrong profile")

    def test_select_item_clears_a_filter_that_hides_it(self):
        win = self._launcher([f"p{i}" for i in range(7)])
        win.filterEdit.setText("p6")
        win.select_item(2)
        self.assertEqual(win.filterEdit.text(), "")
        self.assertEqual(win._current_item_index(), 2)

    def test_reorder_saves_the_order(self):
        win = self._launcher(["a", "b", "c"])
        with patch.object(win, "save_settings") as save:
            self.assertTrue(win.move_selected(1))
        save.assert_called_once()
        self.assertEqual([i.name for i in win.items], ["b", "a", "c"])
        self.assertEqual(win._current_item_index(), 1)

    def test_ctrl_down_moves_and_a_filter_blocks_it(self):
        win = self._launcher(["a", "b", "c"])
        with patch.object(win, "save_settings"):
            event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Down,
                              Qt.KeyboardModifier.ControlModifier)
            self.assertTrue(win.eventFilter(win.listWidget, event))
            self.assertEqual(win.items[1].name, "a")
            win.filterEdit.setText("a")
            self.assertFalse(win.move_selected(1))
        self.assertFalse(win.btnUp.isEnabled())

    def test_probe_is_recorded_and_shown(self):
        win = self._launcher(["a", "b"])
        win.record_probe(win.items[1], False, "connection refused")
        self.assertIn(UNREACHABLE_BADGE,
                      win.listWidget.item(1).data(Qt.ItemDataRole.UserRole))
        self.assertIn("connection refused", win.listWidget.item(1).toolTip())
        win.record_probe(win.items[1], True)
        self.assertNotIn(UNREACHABLE_BADGE,
                         win.listWidget.item(1).data(Qt.ItemDataRole.UserRole))
        self.assertIn("b", win._load_json("profile_last_used"))

    def test_sort_by_last_used(self):
        win = self._launcher(["a", "b", "c"])
        win._save_json("profile_last_used", {"c": 5, "b": 1})
        win.listWidget.setCurrentRow(0)
        with patch.object(win, "save_settings"):
            win.sort_by_last_used()
        self.assertEqual([i.name for i in win.items], ["c", "b", "a"])
        self.assertEqual(win.items[win._current_item_index()].name, "a")

    def test_rename_and_delete_carry_the_state(self):
        win = self._launcher(["a"])
        win._save_json("profile_probes", {"a": {"state": "ok"}})
        win._rename_profile_state("a", "z")
        self.assertEqual(list(win._load_json("profile_probes")), ["z"])
        win._forget_profile_state("z")
        self.assertEqual(win._load_json("profile_probes"), {})

    def test_a_copy_gets_a_unique_name(self):
        win = self._launcher(["a", "a-copy"])
        with patch.object(win, "save_settings"):
            win.copy_profile()
        self.assertEqual(win.items[-1].name, "a-copy-2")


class DuplicateNameDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_taken_name_disables_ok(self):
        dlg = SettingsWindow(None, taken_names=["prod"])
        ok = dlg.buttonBox.button(QDialogButtonBox.StandardButton.Ok)
        dlg.urlLineEdit.setText("https://s3.amazonaws.com")
        dlg.accessKeyEdit.setText("AK")
        dlg.secretKeyEdit.setText("SK")
        dlg.nameLineEdit.setText("prod")
        self.assertFalse(ok.isEnabled())
        self.assertIn("already", dlg.nameLineEdit.toolTip())
        dlg.nameLineEdit.setText("prod2")
        self.assertTrue(ok.isEnabled())


class ProfileStateLifecycleTests(unittest.TestCase):
    """Probe and last-used state follow a profile through add, edit, rename
    and delete, and a damaged settings value cannot break the launcher."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, names):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests-life", "s3duck-tests-life")
        win.settings.clear()
        self.addCleanup(win.settings.clear)
        win.items = [_item(n) for n in names]
        win.populate_list()
        if win.items:
            win.listWidget.setCurrentRow(0)
        self.addCleanup(win.close)
        return win

    def _value(self, name):
        fields = [name, "https://s3.amazonaws.com", "us-east-1", "", "AK", "SK",
                  False, False, "", False, "", "", "", "", "", False, "", ""]
        return tuple(fields)

    def test_add_passes_the_names_in_use(self):
        win = self._launcher(["a", "b"])
        dialog = types.SimpleNamespace(exec=lambda: None)
        with patch.object(s3duck, "SettingsWindow", return_value=dialog) as cls:
            win.onAdd()
        self.assertEqual(cls.call_args.kwargs["taken_names"], ["a", "b"])

    def test_edit_excludes_its_own_name_and_a_rename_carries_the_state(self):
        win = self._launcher(["a", "b"])
        win._save_json("profile_probes", {"a": {"state": "refused", "when": "t"}})
        win._save_json("profile_last_used", {"a": 5})
        crypto = types.SimpleNamespace(encrypt=lambda v: v)
        dialog = types.SimpleNamespace(exec=lambda: self._value("renamed"))
        with patch.object(win, "_secrets_or_warn", return_value=("AK", "SK", "")), \
                patch.object(win, "_crypto_or_warn", return_value=crypto), \
                patch.object(win, "save_settings"), \
                patch.object(s3duck, "SettingsWindow", return_value=dialog) as cls:
            win.onEdit()
        self.assertEqual(cls.call_args.kwargs["taken_names"], ["b"])
        self.assertEqual(win.items[0].name, "renamed")
        self.assertEqual(win.items[0].probe["state"], "refused")
        self.assertIn("renamed", win._load_json("profile_last_used"))
        self.assertIn(UNREACHABLE_BADGE,
                      win.listWidget.item(0).data(Qt.ItemDataRole.UserRole))

    def test_delete_forgets_the_state(self):
        win = self._launcher(["a"])
        win._save_json("profile_probes", {"a": {"state": "ok"}})
        with patch.object(s3duck.QMessageBox, "question",
                          return_value=s3duck.QMessageBox.StandardButton.Yes), \
                patch.object(win, "save_settings"):
            win.onDelete()
        self.assertEqual(win._load_json("profile_probes"), {})

    def test_a_damaged_value_reads_as_empty(self):
        win = self._launcher(["a"])
        with s3duck.SettingsStore(win.settings).group():
            win.settings.setValue("profile_probes", "{not json")
            win.settings.setValue("profile_last_used", "[1, 2]")
        self.assertEqual(win._load_json("profile_probes"), {})
        self.assertEqual(win._load_json("profile_last_used"), {})

    def test_load_attaches_the_stored_probe(self):
        win = self._launcher([])
        win._save_json("profile_probes", {"x": {"state": "refused"}})
        win.settings.beginGroup("profiles")
        win.settings.beginWriteArray("profiles", 1)
        win.settings.setArrayIndex(0)
        win.settings.setValue("name", "x")
        win.settings.endArray()
        win.settings.endGroup()
        win.items = []
        win.load()
        self.assertEqual(win.items[0].probe, {"state": "refused"})

    def test_moving_needs_a_selection(self):
        win = self._launcher(["a", "b"])
        win.listWidget.setCurrentRow(-1)
        win.listWidget.clearSelection()
        self.assertFalse(win.move_selected(1))
        win.listWidget.setCurrentRow(1)
        self.assertFalse(win.move_selected(1), "moved past the end")

    def test_the_context_menu_offers_sort(self):
        win = self._launcher(["a"])
        chosen = []

        def pick(menu, *_a):
            actions = [a for a in menu.actions() if a.text() == "Sort by last used"]
            chosen.append(bool(actions))
            return actions[0] if actions else None

        event = types.SimpleNamespace(type=lambda: QEvent.Type.ContextMenu,
                                      globalPos=lambda: None)
        with patch.object(s3duck.QMenu, "exec", pick), \
                patch.object(win, "sort_by_last_used") as sort:
            self.assertTrue(win.eventFilter(win.listWidget, event))
        self.assertEqual(chosen, [True])
        sort.assert_called_once()


if __name__ == "__main__":
    unittest.main()
