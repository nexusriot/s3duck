"""The window side of this round's features: ACL, retention, SSE-C and report
handlers, the new context-menu entries, delete-to-trash wording, the
cross-profile move and the profile switch's clean-ups."""

import main_window
import time
import types
import ui.context_menu as context_menu
import unittest
import ui.window.access as window_access
import ui.window.reports as window_reports
from PyQt6.QtCore import QPoint, QSettings
from PyQt6.QtWidgets import QDialog, QMenu, QMessageBox
from core.trash import TRASH_PREFIX
from model import FSObjectType
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from unittest.mock import Mock, patch

KEY = bytes(range(32))
OWNER = {"id": "a" * 64, "display": "me"}


def _inline(_parent, _title, fn, *a, **kw):
    """run_with_progress, but on this thread and with no dialog."""
    try:
        return fn(None), None
    except Exception as exc:
        return None, exc


class _WindowCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False, bucket="bkt"):
        settings = QSettings("s3duck-tests-waccess", "s3duck-tests-waccess")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings, read_only=read_only)
        self.addCleanup(win.close)
        win.data_model.bucket = bucket
        win.data_model.current_folder = ""
        win.data_model.clone_for_worker = lambda: win.data_model
        return win


class AclHandlerTests(_WindowCase):

    def test_an_object_acl_is_read_and_saved_through_the_model(self):
        win = self._window()
        acl = {"owner": OWNER, "grants": []}
        win.data_model.get_object_acl = Mock(return_value=acl)
        win.data_model.put_object_acl_grants = Mock()
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "AclDialog") as dialog:
            win.edit_object_acl("p/f.txt")
        win.data_model.get_object_acl.assert_called_once_with("p/f.txt")
        args, kwargs = dialog.call_args
        self.assertEqual(args[1:3], ("p/f.txt", acl))
        self.assertTrue(kwargs["writable"])
        kwargs["save"](OWNER, [{"type": "Group"}])
        win.data_model.put_object_acl_grants.assert_called_once()

    def test_a_refusal_because_acls_are_off_is_explained(self):
        win = self._window()
        win.data_model.get_object_acl = Mock(return_value={"owner": OWNER,
                                                           "grants": []})
        win.data_model.put_object_acl_grants = Mock(
            side_effect=Exception("AccessControlListNotSupported"))
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "AclDialog") as dialog:
            win.edit_object_acl("k")
        with self.assertRaises(RuntimeError) as caught:
            dialog.call_args.kwargs["save"](OWNER, [])
        self.assertIn("BucketOwnerEnforced", str(caught.exception))

    def test_a_read_failure_is_shown_and_nothing_opens(self):
        win = self._window()
        win.data_model.get_object_acl = Mock(side_effect=RuntimeError("denied"))
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "AclDialog") as dialog, \
                patch.object(window_access.QMessageBox, "warning") as warn:
            self.assertIsNone(win.edit_object_acl("k"))
        dialog.assert_not_called()
        warn.assert_called_once()

    def test_cancelled_and_up_entry(self):
        win = self._window()
        with patch.object(window_access, "run_with_progress",
                          return_value=(None, None)), \
                patch.object(window_access, "AclDialog") as dialog:
            self.assertIsNone(win.edit_object_acl("k"))
            self.assertIsNone(win.edit_object_acl("[..]"))
        dialog.assert_not_called()

    def test_a_read_only_window_opens_the_acl_read_only(self):
        win = self._window(read_only=True)
        win.data_model.get_bucket_acl = Mock(return_value={"owner": OWNER,
                                                           "grants": []})
        win.data_model.put_bucket_acl = Mock()
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "AclDialog") as dialog:
            win.edit_bucket_acl()
        self.assertFalse(dialog.call_args.kwargs["writable"])
        dialog.call_args.kwargs["save"](OWNER, [])
        win.data_model.put_bucket_acl.assert_called_once()

    def test_the_bucket_acl_needs_a_bucket(self):
        win = self._window(bucket="")
        with patch.object(window_access, "AclDialog") as dialog:
            self.assertIsNone(win.edit_bucket_acl())
        dialog.assert_not_called()

    def test_a_bucket_acl_refusal_is_explained(self):
        win = self._window()
        win.data_model.get_bucket_acl = Mock(return_value={"owner": OWNER,
                                                           "grants": []})
        win.data_model.put_bucket_acl = Mock(
            side_effect=Exception("AccessControlListNotSupported"))
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "AclDialog") as dialog:
            win.edit_bucket_acl()
        with self.assertRaises(RuntimeError):
            dialog.call_args.kwargs["save"](OWNER, [])


class RetentionHandlerTests(_WindowCase):

    def _dialog(self, accepted=True, ok=True):
        dlg = Mock()
        dlg.exec.return_value = (QDialog.DialogCode.Accepted if accepted
                                 else QDialog.DialogCode.Rejected)
        dlg.assess.return_value = ok
        until = main_window.datetime(2030, 1, 1, tzinfo=main_window.timezone.utc)
        dlg.chosen.return_value = ("GOVERNANCE", until, True)
        return dlg

    def _state(self, win, **state):
        base = {"supported": True, "mode": "GOVERNANCE",
                "retain_until": "2031-01-01 00:00:00+00:00", "legal_hold": ""}
        base.update(state)
        win.data_model.get_object_lock_state = Mock(return_value=base)
        win.data_model.set_object_retention = Mock()

    def test_the_chosen_retention_is_applied_with_its_bypass(self):
        win = self._window()
        self._state(win)
        dlg = self._dialog()
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "RetentionDialog",
                             return_value=dlg) as cls:
            self.assertTrue(win.set_object_retention_ui("k"))
        state = cls.call_args.args[2]
        self.assertEqual(state["retain_until_dt"].year, 2031,
                         "the current date was not parsed for the dialog")
        call = win.data_model.set_object_retention.call_args
        self.assertEqual(call.args[:2], ("k", "GOVERNANCE"))
        self.assertTrue(call.kwargs["bypass_governance"])

    def test_nothing_is_sent_when_declined_or_refused(self):
        win = self._window()
        self._state(win)
        for dlg in (self._dialog(accepted=False), self._dialog(ok=False)):
            with patch.object(window_access, "run_with_progress", _inline), \
                    patch.object(window_access, "RetentionDialog",
                                 return_value=dlg):
                self.assertFalse(win.set_object_retention_ui("k"))
        win.data_model.set_object_retention.assert_not_called()

    def test_a_bucket_without_object_lock_says_so(self):
        win = self._window()
        self._state(win, supported=False)
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access.QMessageBox, "information") as info, \
                patch.object(window_access, "RetentionDialog") as cls:
            self.assertFalse(win.set_object_retention_ui("k"))
        cls.assert_not_called()
        self.assertIn("not enabled", info.call_args.args[2])

    def test_a_service_refusal_is_reported(self):
        win = self._window()
        self._state(win)
        win.data_model.set_object_retention.side_effect = RuntimeError("AccessDenied")
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access, "RetentionDialog",
                             return_value=self._dialog()), \
                patch.object(window_access.QMessageBox, "critical") as critical:
            self.assertFalse(win.set_object_retention_ui("k"))
        critical.assert_called_once()

    def test_folders_read_only_and_a_failed_read(self):
        win = self._window()
        self.assertFalse(win.set_object_retention_ui("dir/"))
        win.data_model.get_object_lock_state = Mock(side_effect=RuntimeError("x"))
        with patch.object(window_access, "run_with_progress", _inline), \
                patch.object(window_access.QMessageBox, "warning"):
            self.assertFalse(win.set_object_retention_ui("k"))
        ro = self._window(read_only=True)
        self.assertFalse(ro.set_object_retention_ui("k"))


class SseCHandlerTests(_WindowCase):

    def test_the_dialog_sets_and_clears_the_key(self):
        win = self._window()
        dlg = Mock()
        dlg.exec.return_value = QDialog.DialogCode.Accepted
        dlg.key_bytes.return_value = KEY
        with patch.object(window_access, "SseCKeyDialog", return_value=dlg):
            self.assertTrue(win.sse_c_key_ui())
        self.assertEqual(win.data_model.sse_c_key, KEY)
        dlg.key_bytes.side_effect = ValueError("bad key")
        with patch.object(window_access, "SseCKeyDialog", return_value=dlg), \
                patch.object(window_access.QMessageBox, "warning"):
            self.assertFalse(win.sse_c_key_ui())
        self.assertEqual(win.data_model.sse_c_key, KEY, "a bad entry cleared the key")
        dlg.exec.return_value = QDialog.DialogCode.Rejected
        with patch.object(window_access, "SseCKeyDialog", return_value=dlg):
            self.assertFalse(win.sse_c_key_ui())

    def test_a_profile_switch_drops_the_key(self):
        win = self._window()
        win.apply_sse_c_key(KEY)
        prof = types.SimpleNamespace(
            name="p2", url="https://s3.amazonaws.com", region="us-east-1",
            access_key="AK2", secret_key="SK2", no_ssl_check=False,
            use_path=False, session_token="", read_only=False)
        win.apply_profile(prof)
        deadline = time.monotonic() + 5
        while not win.listview.isEnabled() and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertEqual(win.data_model.sse_c_key, b"")
        self.assertNotIn("[SSE-C]", win.windowTitle())


class ReportHandlerTests(_WindowCase):

    def test_open_inventory_lists_configurations_first(self):
        win = self._window()
        win.data_model.list_inventory_configurations = Mock(return_value=[{"id": "d"}])
        with patch.object(window_reports, "run_with_progress", _inline), \
                patch.object(window_reports, "InventoryDialog") as dialog:
            win.open_inventory()
        self.assertEqual(dialog.call_args.args[2], [{"id": "d"}])

    def test_a_configuration_read_failure_still_opens_the_dialog(self):
        win = self._window()
        win.data_model.list_inventory_configurations = Mock(
            side_effect=RuntimeError("NotImplemented"))
        with patch.object(window_reports, "run_with_progress", _inline), \
                patch.object(window_reports, "InventoryDialog") as dialog, \
                patch.object(window_reports.QMessageBox, "warning"):
            win.open_inventory()
        self.assertEqual(dialog.call_args.args[2], [],
                         "a manifest can still be opened by key")

    def test_inventory_needs_a_bucket_and_can_be_cancelled(self):
        win = self._window(bucket="")
        with patch.object(window_reports, "InventoryDialog") as dialog:
            self.assertIsNone(win.open_inventory())
        win = self._window()
        with patch.object(window_reports, "run_with_progress",
                          return_value=(None, None)), \
                patch.object(window_reports, "InventoryDialog") as dialog:
            self.assertIsNone(win.open_inventory())
        dialog.assert_not_called()

    def test_select_only_for_queryable_formats(self):
        win = self._window()
        with patch.object(window_reports, "SelectDialog") as dialog:
            self.assertIsNone(win.query_with_select("photo.png"))
            dialog.assert_not_called()
            win.query_with_select("rows.csv")
        dialog.assert_called_once_with(win, win.data_model, "rows.csv")


class ContextMenuEntryTests(_WindowCase):

    def _labels(self, win, key="p/f.txt", writable=True):
        menu = QMenu(win)
        builder = context_menu.MenuBuilder(win, menu)
        shown = []
        with patch.object(context_menu.MenuBuilder, "run",
                          lambda self_, pos: shown.append(
                              [a.text() for a in menu.actions()]) or True):
            context_menu._listing_menu(
                win, builder, [object()],
                types.SimpleNamespace(t=FSObjectType.FILE),
                key.rsplit("/", 1)[-1], "p/", key, False, writable, QPoint())
        return shown[0]

    def test_new_file_entries(self):
        labels = self._labels(self._window(), "p/rows.csv")
        for wanted in ("Retention…", "Permissions (ACL)…", "Query with S3 Select…"):
            self.assertIn(wanted, labels)
        self.assertNotIn("Query with S3 Select…",
                         self._labels(self._window(), "p/photo.png"))

    def test_read_only_keeps_reading_entries(self):
        labels = self._labels(self._window(), "p/rows.csv", writable=False)
        self.assertIn("Permissions (ACL)…", labels)
        self.assertIn("Query with S3 Select…", labels)
        self.assertNotIn("Retention…", labels)

    def test_restore_appears_only_inside_the_trash(self):
        win = self._window()
        self.assertNotIn("Restore from trash", self._labels(win))
        win.data_model.current_folder = TRASH_PREFIX + "p/"
        self.assertIn("Restore from trash", self._labels(win))


class DeleteWordingTests(_WindowCase):

    def _select(self, win, names):
        indexes = [types.SimpleNamespace(column=lambda: 0, name=n) for n in names]
        win.listview.selectionModel = lambda: types.SimpleNamespace(
            selectedIndexes=lambda: indexes)
        win.get_row_primary_item = lambda ix: (object(), ix.name, FSObjectType.FILE)

    def test_a_delete_with_the_trash_on_says_move(self):
        win = self._window()
        win.set_trash_enabled(True)
        win.in_bucket_list_mode = lambda: False
        self._select(win, ["a.txt"])
        win.queue_delete = Mock()
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes) as ask:
            win.delete()
        self.assertEqual(ask.call_args.args[1], f"Move to {TRASH_PREFIX}")
        self.assertIn("restored", ask.call_args.args[2])
        win.queue_delete.assert_called_once_with(["a.txt"])

    def test_a_plain_delete_keeps_its_wording(self):
        win = self._window()
        win.in_bucket_list_mode = lambda: False
        self._select(win, ["a.txt"])
        win.queue_delete = Mock()
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No) as ask:
            win.delete()
        self.assertEqual(ask.call_args.args[1], "Delete")
        win.queue_delete.assert_not_called()

    def test_delete_keys_uses_the_trash(self):
        win = self._window()
        win.set_trash_enabled(True)
        win.assign_thread_operation = Mock()
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes) as ask:
            win.delete_keys(["x.txt"])
        self.assertIn("Move to", ask.call_args.args[2])
        self.assertEqual(win.assign_thread_operation.call_args.args[0], "move")


class TrashEdgeTests(_WindowCase):

    def test_read_only_blocks_restore_and_empty(self):
        win = self._window(read_only=True)
        self.assertFalse(win.restore_from_trash([".trash/a"]))
        self.assertFalse(win.empty_trash())

    def test_restore_from_the_selection_and_skip_all(self):
        win = self._window()
        win._collect_selected_targets = lambda: [("a", ".trash/a", False)]
        win._destination_conflicts = Mock(return_value={"a"})
        win._resolve_overwrites = Mock(return_value=None)
        win.assign_thread_operation = Mock()
        self.assertFalse(win.restore_from_trash())
        win._destination_conflicts.return_value = None
        self.assertFalse(win.restore_from_trash())
        win.assign_thread_operation.assert_not_called()

    def test_empty_trash_needs_a_bucket_and_reports_a_failed_scan(self):
        win = self._window(bucket="")
        self.assertFalse(win.empty_trash())
        win = self._window()
        win._run_with_progress = Mock(return_value=(None, RuntimeError("denied")))
        with patch("ui.window.trash.QMessageBox.warning") as warn:
            self.assertFalse(win.empty_trash())
        warn.assert_called_once()
        win._run_with_progress = Mock(return_value=(None, None))
        self.assertFalse(win.empty_trash())

    def test_the_measure_counts_files_only(self):
        win = self._window()
        win.data_model.get_keys = lambda prefix: [(".trash/d/", 0),
                                                  (".trash/d/a", 5),
                                                  (".trash/b", 7)]
        win._run_with_progress = lambda title, fn: (fn(None), None)
        with patch.object(main_window.QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No) as ask:
            win.empty_trash()
        self.assertIn("2 object(s), 12.0 B", ask.call_args.args[2])

    def test_the_toggle_follows_a_profile_switch(self):
        win = self._window()
        win.set_trash_enabled(True)
        prof = types.SimpleNamespace(
            name="other", url="https://s3.amazonaws.com", region="us-east-1",
            access_key="AK2", secret_key="SK2", no_ssl_check=False,
            use_path=False, session_token="", read_only=False)
        win.apply_profile(prof)
        self.assertFalse(win.actUseTrash.isChecked())


class CrossProfileMoveTests(_WindowCase):

    def _dialog(self, move):
        dlg = Mock()
        dlg.exec.return_value = 1
        dlg.destination_bucket.return_value = "dest"
        dlg.destination_prefix.return_value = "in/"
        dlg.selected_profile.return_value = types.SimpleNamespace(
            read_only=False, name="theirs")
        dlg.move_requested.return_value = move
        return dlg

    def _window_for_copy(self, read_only=False):
        win = self._window(read_only=read_only)
        win.in_bucket_list_mode = lambda: False
        win._collect_selected_targets = lambda: [("f", "f", False)]
        win._destination_conflicts = Mock(return_value=set())
        win.assign_thread_operation = Mock()
        return win

    def test_a_confirmed_move_queues_move_to_profile(self):
        win = self._window_for_copy()
        with patch("main_window.CrossProfileCopyDialog",
                   return_value=self._dialog(True)), \
                patch("main_window.build_profile_model", return_value=object()), \
                patch.object(main_window.QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes):
            win.copy_to_profile()
        call = win.assign_thread_operation.call_args
        self.assertEqual(call.args[:2], ("move_to_profile", [("f", "in/f", False)]))
        self.assertTrue(call.kwargs["need_refresh"])

    def test_a_declined_move_queues_nothing(self):
        win = self._window_for_copy()
        with patch("main_window.CrossProfileCopyDialog",
                   return_value=self._dialog(True)), \
                patch("main_window.build_profile_model", return_value=object()), \
                patch.object(main_window.QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.No):
            win.copy_to_profile()
        win.assign_thread_operation.assert_not_called()

    def test_a_read_only_source_cannot_move(self):
        win = self._window_for_copy(read_only=True)
        with patch("main_window.CrossProfileCopyDialog",
                   return_value=self._dialog(True)), \
                patch.object(main_window.QMessageBox, "warning") as warn:
            win.copy_to_profile()
        warn.assert_called_once()
        win.assign_thread_operation.assert_not_called()


class PaneRoutingTests(_WindowCase):

    def test_the_pane_menu_routes_by_mode(self):
        win = self._window()
        win._pane_profile_transfer = Mock()
        win._transfer_from_second_pane = Mock()
        win.pane_transfer_from_second()
        win._transfer_from_second_pane.assert_not_called()
        win.second_pane.show()
        win.pane_transfer_from_second()
        win._transfer_from_second_pane.assert_called_once_with(False)
        win.second_pane.current_mode = lambda: main_window.SecondPane.OTHER
        win.pane_transfer_from_second()
        win._pane_profile_transfer.assert_called_once_with(True, False)

    def test_a_read_only_window_refuses_a_local_upload(self):
        win = self._window(read_only=True)
        win.second_pane.show()
        win._transfer_from_second_pane = Mock()
        win.pane_transfer_from_second()
        win._transfer_from_second_pane.assert_not_called()

    def test_profile_transfer_guards(self):
        win = self._window()
        win.assign_thread_operation = Mock()
        win._pane_profile_transfer(False, False)
        win.assign_thread_operation.assert_not_called()

        class _Other:
            bucket = "ob"
            read_only = False

            def clone_for_worker(self):
                return self

        win.second_pane.set_other_profile("o", _Other(), "ob", "")
        win.second_pane.shutdown()
        win.in_bucket_list_mode = lambda: True
        win._pane_profile_transfer(True, False)
        win._pane_profile_transfer(False, False)
        win.in_bucket_list_mode = lambda: False
        win._collect_selected_targets = lambda: []
        win._pane_profile_transfer(False, False)
        win._collect_selected_targets = lambda: [("f", "f", False)]
        win._destination_conflicts = Mock(return_value=None)
        win._pane_profile_transfer(False, False)
        win._destination_conflicts = Mock(return_value={"f"})
        win._resolve_overwrites = Mock(return_value=None)
        win._pane_profile_transfer(False, False)
        win.assign_thread_operation.assert_not_called()
        ro = self._window(read_only=True)
        ro.assign_thread_operation = Mock()
        ro.second_pane.set_other_profile("o", _Other(), "ob", "")
        ro.second_pane.shutdown()
        ro.in_bucket_list_mode = lambda: False
        ro._pane_profile_transfer(True, False)
        ro.assign_thread_operation.assert_not_called()


if __name__ == "__main__":
    unittest.main()
