"""Bucket settings' access tabs, rendered from a real server's answers.

The tabs are filled from one background pass of nine-plus reads; which of
them a backend implements decides what each tab says. A policy template is
also only worth anything once the service has accepted the document it
produced.
"""

import json

from PyQt6.QtWidgets import QDialog

from core.policy_templates import build_template
from tests.e2e_gui.harness import GuiServerCase
from ui.dialogs.bucket_settings import BucketSettingsDialog


class BucketAccessTabsTests(GuiServerCase):

    def setUp(self):
        super().setUp()
        self.navigate("")

    def _dialog(self):
        dlg = BucketSettingsDialog(self.window, self.window,
                                   self.window.data_model)
        self.addCleanup(QDialog.close, dlg)
        return dlg

    def test_the_new_tabs_render_what_the_server_said(self):
        dlg = self._dialog()
        titles = [dlg.tabs.tabText(i) for i in range(dlg.tabs.count())]
        for wanted in ("Access", "Logging", "Replication"):
            self.assertIn(wanted, titles)
        self.assertEqual(dlg._replication.text(),
                         "No replication rules are configured.")
        self.assertIn(dlg._log_status.text(),
                      ("Access logging is off.",),
                      dlg._log_status.text())
        # Either a real setting or the honest "account-level applies".
        self.assertTrue(dlg._pab_status.text() == ""
                        or "account-level" in dlg._pab_status.text(),
                        dlg._pab_status.text())

    def test_a_template_is_accepted_by_the_service(self):
        dlg = self._dialog()
        dlg._policy_edit.setChecked(True)
        statements = build_template("public_read", self.bucket, prefix="site/")
        self.assertTrue(dlg.insert_policy_template(statements))
        text = dlg._policy_text.toPlainText()
        self.assertIn("site/*", text)
        with self.recording_messages() as shown, self.confirming():
            dlg._save_policy()
        self.assertEqual([m for m in shown if m[0] == "critical"], [], shown)
        stored = json.loads(self.s3.get_bucket_policy(Bucket=self.bucket)["Policy"])
        resources = [s.get("Resource") for s in stored["Statement"]]
        self.assertTrue(any("site/" in str(r) for r in resources), stored)
        self.s3.delete_bucket_policy(Bucket=self.bucket)
