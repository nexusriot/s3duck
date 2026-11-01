"""Dialog paths of this round that the feature tests did not reach: Bucket
settings' Access/Logging/Replication tabs and template insert, the grant
editor, the QR dialog, the inventory and Select dialogs, and the media
player's controls."""

import json
import unittest
import ui.dialogs.bucket_settings as bucket_settings
import ui.dialogs.preview as preview
import ui.dialogs.reports as reports
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox
from core.acl import ALL_USERS
from core.policy_templates import build_template
from s3.access import PUBLIC_ACCESS_FLAGS
from tests.support import _ensure_qapp
from ui.dialogs.access import AclDialog, GrantDialog, PolicyTemplateDialog
from ui.dialogs.presigned import PresignedLinkDialog, QrCodeDialog
from unittest.mock import Mock, patch

OWNER = {"id": "a" * 64, "display": "me"}


class _DocModel:
    """Every bucket document, answered from memory."""

    bucket = "bkt"
    current_folder = "site/"

    def __init__(self, **overrides):
        self.calls = []
        self.answers = {
            "get_bucket_lifecycle": [], "get_bucket_cors": [],
            "get_bucket_policy": "", "get_object_lock_configuration": {},
            "get_bucket_encryption": {}, "get_bucket_tags": {},
            "get_bucket_website": {}, "get_bucket_notifications": [],
            "get_bucket_acceleration": "",
            "get_public_access_block": {f: True for f in PUBLIC_ACCESS_FLAGS},
            "get_bucket_logging": {}, "get_bucket_replication": {"rules": []},
            "get_bucket_policy_status": "",
        }
        self.answers.update(overrides)

    def clone_for_worker(self):
        return self

    def __getattr__(self, name):
        if name in self.answers:
            value = self.answers[name]
            if isinstance(value, Exception):
                def fail():
                    raise value
                return fail
            return lambda: value

        def write(*args, **kwargs):
            self.calls.append((name, args, kwargs))
        return write


class _Window:
    def __init__(self, read_only=False):
        self.read_only = read_only
        self.logged = []
        self.edit_bucket_acl = Mock()

    def is_read_only(self):
        return self.read_only

    def log(self, text):
        self.logged.append(text)


def _inline(_parent, _title, fn, *a, **kw):
    try:
        return fn(None), None
    except Exception as exc:
        return None, exc


class BucketSettingsTabTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self, model=None, read_only=False):
        model = model or _DocModel()
        window = _Window(read_only)
        with patch.object(bucket_settings, "run_with_progress", _inline):
            dlg = bucket_settings.BucketSettingsDialog(None, window, model)
        self.addCleanup(dlg.close)
        return dlg, model, window

    def test_the_access_tab_renders_and_saves_the_flags(self):
        dlg, model, _ = self._dialog()
        self.assertTrue(all(box.isChecked() for box in dlg._pab.values()))
        self.assertEqual(dlg._pab_status.text(), "")
        self.assertTrue(dlg.save_public_access_block(), "all-on needs no question")
        self.assertEqual(model.calls[-1][0], "put_public_access_block")

    def test_turning_a_flag_off_asks_first(self):
        dlg, model, _ = self._dialog()
        dlg._pab["BlockPublicAcls"].setChecked(False)
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            self.assertFalse(dlg.save_public_access_block())
        self.assertEqual(model.calls, [])
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.assertTrue(dlg.save_public_access_block())
        self.assertFalse(model.calls[-1][1][0]["BlockPublicAcls"])

    def test_an_absent_setting_points_at_the_account_level(self):
        dlg, _, _ = self._dialog(_DocModel(get_public_access_block=None))
        self.assertIn("account-level", dlg._pab_status.text())
        self.assertFalse(any(box.isChecked() for box in dlg._pab.values()))

    def test_removing_the_setting(self):
        dlg, model, _ = self._dialog()
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            self.assertFalse(dlg.remove_public_access_block())
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.assertTrue(dlg.remove_public_access_block())
        self.assertEqual(model.calls[-1][0], "delete_public_access_block")
        self.assertFalse(any(box.isChecked() for box in dlg._pab.values()))

    def test_a_refused_access_write_is_reported(self):
        class _Refusing(_DocModel):
            def put_public_access_block(self, *a, **kw):
                raise RuntimeError("NotImplemented")

            def delete_public_access_block(self, *a, **kw):
                raise RuntimeError("NotImplemented")

        dlg, _, _ = self._dialog(_Refusing())
        with patch.object(QMessageBox, "critical") as critical, \
                patch.object(QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes):
            self.assertFalse(dlg.save_public_access_block())
            self.assertFalse(dlg.remove_public_access_block())
        self.assertEqual(critical.call_count, 2)

    def test_read_errors_stay_in_their_own_tab(self):
        dlg, _, _ = self._dialog(_DocModel(
            get_public_access_block=RuntimeError("denied-pab"),
            get_bucket_logging=RuntimeError("denied-log"),
            get_bucket_replication=RuntimeError("denied-rep")))
        self.assertIn("denied-pab", dlg._pab_status.text())
        self.assertIn("denied-log", dlg._log_status.text())
        self.assertIn("denied-rep", dlg._replication.text())

    def test_logging_round_trip_and_the_self_target_refusal(self):
        dlg, model, _ = self._dialog(_DocModel(get_bucket_logging={
            "target_bucket": "logs", "target_prefix": "bkt/"}))
        self.assertEqual(dlg._log_bucket.text(), "logs")
        self.assertEqual(dlg._log_status.text(), "")
        dlg._log_bucket.setText("bkt")
        with patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(dlg.save_logging())
        warn.assert_called_once()
        self.assertEqual(model.calls, [])
        dlg._log_bucket.setText("")
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No) as ask:
            self.assertFalse(dlg.save_logging(), "logging went off unasked")
        self.assertIn("currently goes to logs", ask.call_args.args[2])
        self.assertEqual(model.calls, [])
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.assertTrue(dlg.save_logging())
        self.assertEqual(model.calls[-1][1][0], "")
        self.assertEqual(dlg._log_status.text(), "Access logging is off.")

    def test_a_failed_read_cannot_be_saved_over(self):
        """REGRESSION (security): a failed read left four unticked boxes
        and an enabled Save, which wrote every protection off over a
        configuration nobody had seen. Logging the same, as "off"."""
        dlg, model, _ = self._dialog(_DocModel(
            get_public_access_block=RuntimeError("AccessDenied"),
            get_bucket_logging=RuntimeError("AccessDenied")))
        self.assertFalse(dlg._btn_pab_save.isEnabled())
        self.assertFalse(dlg._btn_pab_remove.isEnabled())
        self.assertFalse(dlg._btn_log_save.isEnabled())
        self.assertFalse(dlg.save_public_access_block())
        self.assertFalse(dlg.remove_public_access_block())
        self.assertFalse(dlg.save_logging())
        self.assertEqual(model.calls, [])
        self.assertIn("cannot be saved", dlg._pab_status.text())

    def test_a_refused_logging_write_is_reported(self):
        class _Refusing(_DocModel):
            def put_bucket_logging(self, *a, **kw):
                raise RuntimeError("NotImplemented")

        dlg, _, _ = self._dialog(_Refusing())
        dlg._log_bucket.setText("logs")
        with patch.object(QMessageBox, "critical") as critical:
            self.assertFalse(dlg.save_logging())
        critical.assert_called_once()

    def test_replication_rules_are_listed(self):
        dlg, _, _ = self._dialog(_DocModel(get_bucket_replication={
            "role": "arn:role", "rules": [{
                "id": "r1", "status": "Enabled", "prefix": "a/", "tags": [],
                "destination": "arn:aws:s3:::dst", "storage_class": "",
                "delete_markers": ""}]}))
        self.assertIn("Role: arn:role", dlg._replication.text())
        self.assertIn("r1: prefix 'a/'", dlg._replication.text())

    def test_the_template_button_follows_allow_editing(self):
        dlg, _, _ = self._dialog()
        self.assertFalse(dlg._btn_policy_template.isEnabled())
        dlg._policy_edit.setChecked(True)
        self.assertTrue(dlg._btn_policy_template.isEnabled())

    def test_inserting_a_template_keeps_the_existing_statements(self):
        existing = json.dumps({"Version": "2012-10-17", "Statement": [
            {"Sid": "Mine", "Effect": "Deny", "Principal": "*",
             "Action": "s3:DeleteObject", "Resource": "x"}]})
        dlg, _, _ = self._dialog(_DocModel(get_bucket_policy=existing))
        self.assertTrue(dlg.insert_policy_template(
            build_template("public_read", "bkt", "site/")))
        sids = [s["Sid"] for s in json.loads(dlg._policy_text.toPlainText())["Statement"]]
        self.assertEqual(sids[0], "Mine")
        self.assertIn("public access", dlg._policy_status.text())

    def test_a_broken_policy_is_not_overwritten_by_a_template(self):
        dlg, _, _ = self._dialog()
        dlg._policy_text.setPlainText("{broken")
        with patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(dlg.insert_policy_template(
                build_template("deny_http", "bkt")))
        warn.assert_called_once()
        self.assertEqual(dlg._policy_text.toPlainText(), "{broken")

    def test_the_template_picker_path(self):
        dlg, _, _ = self._dialog()
        picker = Mock()
        picker.exec.return_value = QDialog.DialogCode.Rejected
        with patch.object(bucket_settings, "PolicyTemplateDialog", return_value=picker):
            self.assertFalse(dlg.insert_policy_template())
        picker.exec.return_value = QDialog.DialogCode.Accepted
        picker.statements.return_value = build_template("deny_http", "bkt")
        with patch.object(bucket_settings, "PolicyTemplateDialog",
                          return_value=picker) as cls:
            self.assertTrue(dlg.insert_policy_template())
        self.assertEqual(cls.call_args.args[1:], ("bkt", "site/"))

    def test_a_read_only_window_disables_the_access_writes(self):
        dlg, _, _ = self._dialog(read_only=True)
        self.assertFalse(any(box.isEnabled() for box in dlg._pab.values()))
        self.assertTrue(dlg._log_bucket.isReadOnly())


class GrantDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_each_grantee_kind(self):
        dlg = GrantDialog(None)
        dlg.who.setText("not-an-id")
        dlg._accept()
        self.assertIn("64 hexadecimal", dlg.problem.text())
        self.assertEqual(dlg.result(), 0)
        dlg.who.setText("b" * 64)
        dlg._accept()
        self.assertEqual(dlg.result(), QDialog.DialogCode.Accepted)
        self.assertEqual(dlg.grant()["id"], "b" * 64)

        dlg = GrantDialog(None)
        dlg.kind.setCurrentIndex(dlg.kind.findData("Group"))
        self.assertFalse(dlg.who.isEnabled())
        self.assertTrue(dlg.group.isEnabled())
        self.assertEqual(dlg.grant()["uri"], ALL_USERS)

        dlg.kind.setCurrentIndex(dlg.kind.findData("AmazonCustomerByEmail"))
        dlg.who.setText("x@example.com")
        dlg.permission.setCurrentText("WRITE")
        self.assertEqual((dlg.grant()["email"], dlg.grant()["permission"]),
                         ("x@example.com", "WRITE"))

    def test_the_acl_editor_adds_through_the_grant_dialog(self):
        acl = AclDialog(None, "k", {"owner": OWNER, "grants": []},
                        save=lambda *a: None)
        picker = Mock()
        picker.exec.return_value = QDialog.DialogCode.Rejected
        with patch("ui.dialogs.access.GrantDialog", return_value=picker):
            self.assertFalse(acl.add_grant())
        picker.exec.return_value = QDialog.DialogCode.Accepted
        picker.grant.return_value = {"type": "Group", "uri": ALL_USERS, "id": "",
                                     "display": "", "email": "",
                                     "permission": "READ"}
        with patch("ui.dialogs.access.GrantDialog", return_value=picker):
            self.assertTrue(acl.add_grant())
        self.assertEqual(len(acl.grants()), 1)
        self.assertIn("outside your account", acl.note.text())

    def test_the_template_dialog_accepts_a_valid_choice(self):
        dlg = PolicyTemplateDialog(None, "bkt")
        dlg._accept()
        self.assertEqual(dlg.result(), QDialog.DialogCode.Accepted)


class QrAndLinkDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_qr_dialog_shows_the_image(self):
        dlg = QrCodeDialog(None, "https://example.com/a")
        self.assertFalse(dlg.image.pixmap().isNull())

    def test_the_qr_button_is_for_download_links_only(self):
        class _Model:
            def presigned_get_url(self, key, secs):
                return "https://get/" + key

            def presigned_put_url(self, key, secs):
                return "https://put/" + key

            def presigned_post(self, key, secs, **kw):
                raise RuntimeError("no POST here")

        dlg = PresignedLinkDialog(None, _Model(), "a.txt")
        self.assertTrue(dlg._qr_btn.isEnabled())
        dlg._type.setCurrentIndex(PresignedLinkDialog.PUT)
        self.assertFalse(dlg._qr_btn.isEnabled())
        dlg._type.setCurrentIndex(PresignedLinkDialog.POST)
        self.assertIn("no POST here", dlg._note.text())
        self.assertIsNone(dlg.post_form())

    def test_copy_gives_curl_for_a_form(self):
        class _Model:
            def presigned_get_url(self, key, secs):
                return "https://get/" + key

            def presigned_post(self, key, secs, **kw):
                return {"url": "https://post/", "fields": {"key": key}}

        dlg = PresignedLinkDialog(None, _Model(), "a.txt")
        dlg._copy()
        self.assertEqual(QApplication.clipboard().text(), "https://get/a.txt")
        dlg._type.setCurrentIndex(PresignedLinkDialog.POST)
        dlg._copy()
        self.assertTrue(QApplication.clipboard().text().startswith("curl "))


class InventoryDialogPathTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class _Model:
        bucket = "src"
        current_folder = ""

        def clone_for_worker(self):
            return self

    CONFIG = {"id": "d", "enabled": False, "frequency": "Daily", "format": "CSV",
              "destination_bucket": "reports"}

    def test_the_latest_report_route(self):
        dlg = reports.InventoryDialog(None, self._Model(), [self.CONFIG])
        self.assertIn("[disabled]", dlg.config.itemText(0))
        with patch.object(reports, "run_with_progress", return_value=("", None)):
            self.assertFalse(dlg.load_latest())
        self.assertIn("48 hours", dlg.status.text())
        with patch.object(reports, "run_with_progress",
                          return_value=(None, RuntimeError("denied"))), \
                patch.object(QMessageBox, "warning") as warn:
            self.assertFalse(dlg.load_latest())
        warn.assert_called_once()
        with patch.object(reports, "run_with_progress",
                          return_value=("m.json", None)), \
                patch.object(reports.InventoryDialog, "load",
                             return_value=True) as load:
            self.assertTrue(dlg.load_latest())
        load.assert_called_once_with("reports", "m.json")

    def test_without_configurations_and_without_etags(self):
        dlg = reports.InventoryDialog(None, self._Model(), [])
        self.assertFalse(dlg.load_latest())
        dlg.render()
        with patch.object(reports, "run_with_progress", return_value=(None, None)):
            self.assertFalse(dlg.load("b", "k"))
        report = {"rows": [{"Key": "a", "Size": "1"}],
                  "manifest": {"created": "", "source_bucket": "",
                               "schema": ["Bucket", "Key", "Size"]}}
        with patch.object(reports, "run_with_progress", return_value=(report, None)):
            self.assertTrue(dlg.load("b", "k"))
        self.assertIn("ETag field", dlg.duplicates.text())
        self.assertIn("unknown date", dlg.status.text())

    def test_select_with_nothing_to_run_or_cancelled(self):
        dlg = reports.SelectDialog(None, self._Model(), "rows.json")
        self.assertTrue(dlg.header.isHidden(), "a header option on JSON input")
        dlg.sql.setPlainText("  ")
        self.assertFalse(dlg.run())
        dlg.sql.setPlainText("SELECT 1")
        with patch.object(reports, "run_with_progress", return_value=(None, None)):
            self.assertFalse(dlg.run())
        with patch.object(reports, "run_with_progress",
                          return_value=(None, RuntimeError("AccessDenied"))):
            self.assertFalse(dlg.run())
        self.assertNotIn("does not implement", dlg.status.text())


class _Model:
    bucket = "b"
    no_ssl_check = False
    requester_pays = False

    def clone_for_worker(self):
        return self

    def presigned_get_url(self, key, expires_sec=3600):
        return "http://127.0.0.1:9/" + key

    def get_object_preview(self, key, max_bytes):
        return {"data": b"a,b\n", "content_type": "text/csv", "size": 4,
                "truncated": False}


@unittest.skipIf(preview.QMediaPlayer is None, "QtMultimedia not installed")
class MediaControlTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self):
        dlg = preview.PreviewDialog(None, _Model(), "clip.mp4")
        self.addCleanup(dlg.close)
        return dlg

    def test_play_pause_and_the_label(self):
        dlg = self._dlg()
        dlg._player = Mock()
        dlg._player.playbackState.return_value = preview.QMediaPlayer.PlaybackState.StoppedState
        dlg._toggle_play()
        dlg._player.play.assert_called_once()
        dlg._player.playbackState.return_value = preview.QMediaPlayer.PlaybackState.PlayingState
        dlg._toggle_play()
        dlg._player.pause.assert_called_once()
        dlg._on_playback_state(preview.QMediaPlayer.PlaybackState.PlayingState)
        self.assertEqual(dlg._play_btn.text(), "Pause")
        dlg._on_playback_state(preview.QMediaPlayer.PlaybackState.PausedState)
        self.assertEqual(dlg._play_btn.text(), "Play")

    def test_seek_and_position(self):
        dlg = self._dlg()
        dlg._player = Mock()
        dlg._player.position.return_value = 30_000
        dlg._on_duration(60_000)
        dlg._on_position(30_000)
        self.assertEqual(dlg._seek.value(), 30_000)
        self.assertEqual(dlg._time.text(), "0:30 / 1:00")
        dlg._on_seek(45_000)
        dlg._player.setPosition.assert_called_once_with(45_000)

    def test_a_playback_error_is_shown(self):
        dlg = self._dlg()
        dlg._on_media_error(None, "codec missing")
        self.assertIn("codec missing", dlg._media_note.text())
        self.assertFalse(dlg._media_note.isHidden())

    def test_no_player_is_harmless(self):
        dlg = preview.PreviewDialog(None, _Model(), "rows.csv")
        self.addCleanup(dlg.close)
        dlg._player = None
        dlg._toggle_play()
        dlg._on_seek(1)
        dlg._update_time()
        dlg._stop_media()

    def test_a_csv_preview_offers_select(self):
        dlg = preview.PreviewDialog(None, _Model(), "rows.csv")
        self.addCleanup(dlg.close)
        self.assertFalse(dlg._select_btn.isHidden())
        other = preview.PreviewDialog(None, _Model(), "notes.txt")
        self.addCleanup(other.close)
        self.assertTrue(other._select_btn.isHidden())


if __name__ == "__main__":
    unittest.main()
