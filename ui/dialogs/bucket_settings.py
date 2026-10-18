"""Lifecycle, CORS, policy, encryption and Object Lock for one bucket.
"""

import json
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFormLayout,
    QHBoxLayout, QLabel, QLineEdit, QListWidget, QListWidgetItem,
    QMessageBox, QPlainTextEdit, QPushButton, QSpinBox, QTabWidget,
    QVBoxLayout, QWidget,
)
from model import Model as DataModel
from utils import run_with_progress


class LifecycleRuleDialog(QDialog):
    """One lifecycle rule: what it covers and what it does."""

    def __init__(self, parent, rule=None):
        super().__init__(parent)
        self.setWindowTitle("Lifecycle rule")
        self.setMinimumWidth(460)
        rule = dict(rule or {})

        self._id = QLineEdit(str(rule.get("ID") or ""))
        self._id.setPlaceholderText("expire-logs")
        self._prefix = QLineEdit(DataModel.lifecycle_rule_scope(rule))
        self._prefix.setPlaceholderText("logs/  (blank = the whole bucket)")
        self._enabled = QCheckBox("Enabled")
        self._enabled.setChecked(rule.get("Status", "Enabled") == "Enabled")

        transitions = (rule.get("Transitions") or [{}])[0]
        self._transition = QCheckBox("Move to another storage class after")
        self._transition_days = QSpinBox()
        self._transition_days.setRange(1, 36500)
        self._transition_days.setSuffix(" days")
        self._transition_days.setValue(int(transitions.get("Days") or 30))
        self._transition_class = QComboBox()
        for name in ("STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING",
                     "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE"):
            self._transition_class.addItem(name, name)
        index = self._transition_class.findData(
            transitions.get("StorageClass") or "STANDARD_IA")
        self._transition_class.setCurrentIndex(max(index, 0))
        self._transition.setChecked(bool(transitions.get("Days")))

        expiration = rule.get("Expiration") or {}
        self._expire = QCheckBox("Delete objects after")
        self._expire_days = QSpinBox()
        self._expire_days.setRange(1, 36500)
        self._expire_days.setSuffix(" days")
        self._expire_days.setValue(int(expiration.get("Days") or 365))
        self._expire.setChecked(expiration.get("Days") is not None)

        noncurrent = rule.get("NoncurrentVersionExpiration") or {}
        self._noncurrent = QCheckBox("Delete noncurrent versions after")
        self._noncurrent_days = QSpinBox()
        self._noncurrent_days.setRange(1, 36500)
        self._noncurrent_days.setSuffix(" days")
        self._noncurrent_days.setValue(
            int(noncurrent.get("NoncurrentDays") or 30))
        self._noncurrent.setChecked(
            noncurrent.get("NoncurrentDays") is not None)

        abort = rule.get("AbortIncompleteMultipartUpload") or {}
        self._abort = QCheckBox("Abort incomplete multipart uploads after")
        self._abort_days = QSpinBox()
        self._abort_days.setRange(1, 3650)
        self._abort_days.setSuffix(" days")
        self._abort_days.setValue(
            int(abort.get("DaysAfterInitiation") or 7))
        self._abort.setChecked(abort.get("DaysAfterInitiation") is not None)

        self._markers = QCheckBox("Clean up expired delete markers")
        self._markers.setChecked(
            bool(expiration.get("ExpiredObjectDeleteMarker")))

        form = QFormLayout()
        form.addRow(QLabel("Rule name"), self._id)
        form.addRow(QLabel("Applies to prefix"), self._prefix)
        form.addRow(self._enabled)
        for check, spin in ((self._transition, self._transition_days),
                            (self._expire, self._expire_days),
                            (self._noncurrent, self._noncurrent_days),
                            (self._abort, self._abort_days)):
            row = QHBoxLayout()
            row.addWidget(check)
            row.addWidget(spin)
            row.addStretch(1)
            check.toggled.connect(spin.setEnabled)
            spin.setEnabled(check.isChecked())
            form.addRow(row)
        transition_row = QHBoxLayout()
        transition_row.addWidget(QLabel("Storage class"))
        transition_row.addWidget(self._transition_class)
        transition_row.addStretch(1)
        form.addRow(transition_row)
        form.addRow(self._markers)

        note = QLabel(
            "Lifecycle actions are executed by the storage service, not by "
            "this app, and deletions they make are permanent.")
        note.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(note)
        layout.addWidget(buttons)
        self._rule = None

    def _accept(self):
        try:
            self._rule = self.rule()
        except ValueError as exc:
            QMessageBox.warning(self, "Lifecycle rule", str(exc))
            return
        self.accept()

    def rule(self) -> dict:
        return DataModel.build_lifecycle_rule(
            self._id.text(),
            prefix=self._prefix.text().strip(),
            enabled=self._enabled.isChecked(),
            transition_days=(self._transition_days.value()
                             if self._transition.isChecked() else None),
            transition_class=(self._transition_class.currentData()
                              if self._transition.isChecked() else ""),
            expire_days=(self._expire_days.value()
                         if self._expire.isChecked() else None),
            noncurrent_days=(self._noncurrent_days.value()
                             if self._noncurrent.isChecked() else None),
            abort_days=(self._abort_days.value()
                        if self._abort.isChecked() else None),
            expire_delete_markers=self._markers.isChecked(),
        )

    def result_rule(self):
        return self._rule


class BucketSettingsDialog(QDialog):
    """
    Lifecycle, CORS, policy and Object Lock for one bucket.

    Four documents that all live on the bucket rather than on an object, kept
    in one place so the toolbar does not grow four more entries.
    """

    def __init__(self, parent, main_window, model):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self.setWindowTitle(f"Bucket settings — {model.bucket}")
        self.resize(720, 520)
        self._writable = not main_window.is_read_only()
        self._acceleration_was = False

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_lifecycle(), "Lifecycle")
        self.tabs.addTab(self._build_cors(), "CORS")
        self.tabs.addTab(self._build_policy(), "Policy")
        self.tabs.addTab(self._build_bucket(), "Encryption && tags")
        self.tabs.addTab(self._build_website(), "Website")
        self.tabs.addTab(self._build_events(), "Events")
        self.tabs.addTab(self._build_lock(), "Object Lock")

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self.tabs, 1)
        layout.addLayout(row)
        self.reload()

    def _build_lifecycle(self):
        page = QWidget()
        self._rules = []
        self._rules_list = QListWidget()
        self._rules_list.itemDoubleClicked.connect(
            lambda _i: self._edit_rule())
        self._lifecycle_status = QLabel("")
        self._lifecycle_status.setWordWrap(True)

        self._btn_add = QPushButton("Add…")
        self._btn_edit = QPushButton("Edit…")
        self._btn_remove = QPushButton("Remove")
        self._btn_save = QPushButton("Save to bucket")
        self._btn_add.clicked.connect(self._add_rule)
        self._btn_edit.clicked.connect(self._edit_rule)
        self._btn_remove.clicked.connect(self._remove_rule)
        self._btn_save.clicked.connect(self._save_lifecycle)
        for button in (self._btn_add, self._btn_edit, self._btn_remove,
                       self._btn_save):
            button.setEnabled(self._writable)

        buttons = QHBoxLayout()
        for button in (self._btn_add, self._btn_edit, self._btn_remove):
            buttons.addWidget(button)
        buttons.addStretch(1)
        buttons.addWidget(self._btn_save)

        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Rules the storage service applies on its own schedule. "
            "Transitions and expirations happen without this app running."))
        layout.addWidget(self._rules_list, 1)
        layout.addWidget(self._lifecycle_status)
        layout.addLayout(buttons)
        return page

    def _build_cors(self):
        page = QWidget()
        self._cors_text = QPlainTextEdit()
        self._cors_text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._cors_text.setReadOnly(not self._writable)
        self._cors_status = QLabel("")
        self._cors_status.setWordWrap(True)
        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_cors)
        save.setEnabled(self._writable)
        self._btn_cors_save = save
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "The CORS rules as JSON — a list of rule objects. An empty "
            "document removes the configuration."))
        layout.addWidget(self._cors_text, 1)
        layout.addWidget(self._cors_status)
        layout.addLayout(row)
        return page

    def _build_policy(self):
        page = QWidget()
        self._policy_text = QPlainTextEdit()
        self._policy_text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._policy_text.setReadOnly(True)
        self._policy_status = QLabel("")
        self._policy_status.setWordWrap(True)
        self._policy_edit = QCheckBox("Allow editing")
        self._policy_edit.setEnabled(self._writable)
        self._policy_edit.toggled.connect(self._toggle_policy_edit)
        self._btn_policy_save = QPushButton("Save to bucket")
        self._btn_policy_save.clicked.connect(self._save_policy)
        self._btn_policy_save.setEnabled(False)
        row = QHBoxLayout()
        row.addWidget(self._policy_edit)
        row.addStretch(1)
        row.addWidget(self._btn_policy_save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "The bucket policy decides who can reach this bucket. It opens "
            "read-only on purpose: a wrong policy can expose every object in "
            "it, or lock you out of your own bucket."))
        layout.addWidget(self._policy_text, 1)
        layout.addWidget(self._policy_status)
        layout.addLayout(row)
        return page

    def _build_bucket(self):
        """Default encryption, the bucket's own tags, and acceleration."""
        page = QWidget()
        self._sse = QComboBox()
        self._sse.addItem("(none)", "")
        for mode in DataModel.SSE_MODES:
            if mode:
                self._sse.addItem(mode, mode)
        self._sse_key = QLineEdit()
        self._sse_key.setPlaceholderText("KMS key id or ARN (aws:kms only)")
        self._bucket_key = QCheckBox(
            "Use an S3 Bucket Key (fewer KMS requests)")
        self._sse.currentIndexChanged.connect(self._sync_sse_fields)

        self._bucket_tags = QPlainTextEdit()
        self._bucket_tags.setPlaceholderText("one per line:  key = value")
        self._bucket_tags.setMaximumHeight(90)

        self._acceleration = QCheckBox("Transfer acceleration")
        self._bucket_status = QLabel("")
        self._bucket_status.setWordWrap(True)

        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_bucket)
        save.setEnabled(self._writable)
        self._btn_bucket_save = save
        for widget in (self._sse, self._sse_key, self._bucket_key,
                       self._bucket_tags, self._acceleration):
            widget.setEnabled(self._writable)

        form = QFormLayout()
        form.addRow(QLabel("Default encryption"), self._sse)
        form.addRow(QLabel("KMS key"), self._sse_key)
        form.addRow(self._bucket_key)
        form.addRow(QLabel("Bucket tags"), self._bucket_tags)
        form.addRow(self._acceleration)

        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Default encryption applies to objects uploaded from anywhere, "
            "not just this app. Bucket tags are the bucket's own, separate "
            "from the tags on its objects."))
        layout.addLayout(form)
        layout.addWidget(self._bucket_status)
        layout.addStretch(1)
        layout.addLayout(row)
        return page

    def _build_website(self):
        page = QWidget()
        self._site_index = QLineEdit()
        self._site_index.setPlaceholderText("index.html")
        self._site_error = QLineEdit()
        self._site_error.setPlaceholderText("error.html (optional)")
        self._site_redirect = QLineEdit()
        self._site_redirect.setPlaceholderText(
            "example.com — redirects every request, instead of serving")
        self._site_status = QLabel("")
        self._site_status.setWordWrap(True)
        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_website)
        save.setEnabled(self._writable)
        self._btn_site_save = save
        for widget in (self._site_index, self._site_error,
                       self._site_redirect):
            widget.setReadOnly(not self._writable)

        form = QFormLayout()
        form.addRow(QLabel("Index document"), self._site_index)
        form.addRow(QLabel("Error document"), self._site_error)
        form.addRow(QLabel("Redirect all requests to"), self._site_redirect)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Static-website hosting serves this bucket over its website "
            "endpoint. It does not make the objects public on its own — the "
            "policy does that. Leaving everything blank removes the "
            "configuration."))
        layout.addLayout(form)
        layout.addWidget(self._site_status)
        layout.addStretch(1)
        layout.addLayout(row)
        return page

    def _build_events(self):
        page = QWidget()
        self._events = QLabel("")
        self._events.setWordWrap(True)
        self._events.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Event notifications, read-only: a destination ARN cannot be "
            "validated from here, and a wrong one silently stops every "
            "event."))
        layout.addWidget(self._events)
        layout.addStretch(1)
        return page

    def _sync_sse_fields(self):
        mode = self._sse.currentData() or ""
        self._sse_key.setEnabled(self._writable and mode == "aws:kms")
        self._bucket_key.setEnabled(self._writable and mode == "aws:kms")

    def _save_bucket(self):
        tags = DataModel.parse_content_type_overrides(
            self._bucket_tags.toPlainText())
        try:
            self._model.put_bucket_encryption(
                sse=self._sse.currentData() or "",
                kms_key=self._sse_key.text().strip(),
                bucket_key=self._bucket_key.isChecked(),
                log_fn=self._mw.log)
            self._model.put_bucket_tags(tags, log_fn=self._mw.log)
            if self._acceleration.isChecked() != self._acceleration_was:
                self._model.set_bucket_acceleration(
                    self._acceleration.isChecked(), log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Bucket settings", str(exc))
            return
        self._bucket_status.setText("Saved.")
        self.reload()

    def _save_website(self):
        try:
            self._model.put_bucket_website(
                index=self._site_index.text(),
                error=self._site_error.text(),
                redirect=self._site_redirect.text(),
                log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Website hosting", str(exc))
            return
        self._site_status.setText("Saved.")
        self.reload()

    def _build_lock(self):
        page = QWidget()
        self._lock_status = QLabel("")
        self._lock_status.setWordWrap(True)
        self._lock_status.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        layout = QVBoxLayout(page)
        layout.addWidget(self._lock_status)
        layout.addStretch(1)
        return page

    @staticmethod
    def read_documents(model) -> dict:
        """
        Fetch all four bucket documents, each failure kept beside its own key.

        One dict rather than four calls from the UI thread: this is five round
        trips, and a dialog that blocks for them is the thing every other
        network path in this app already avoids.
        """
        out = {}
        for name, fetch in (
            ("lifecycle", model.get_bucket_lifecycle),
            ("cors", model.get_bucket_cors),
            ("policy", model.get_bucket_policy),
            ("lock", model.get_object_lock_configuration),
            ("encryption", model.get_bucket_encryption),
            ("tags", model.get_bucket_tags),
            ("website", model.get_bucket_website),
            ("events", model.get_bucket_notifications),
            ("acceleration", model.get_bucket_acceleration),
        ):
            try:
                out[name] = fetch()
            except Exception as exc:
                out[name] = None
                out[name + "_error"] = str(exc)
        try:
            out["policy_status"] = model.get_bucket_policy_status()
        except Exception:
            out["policy_status"] = ""
        return out

    def reload(self):
        """Read all four documents off the UI thread and render them."""
        clone = self._model.clone_for_worker()

        def _read(_worker):
            return self.read_documents(clone)

        payload, exc = run_with_progress(
            self, f"Reading settings for {self._model.bucket}…", _read)
        if payload is None and exc is None:
            return  # cancelled
        if exc is not None:
            payload = {"lifecycle_error": str(exc), "cors_error": str(exc),
                       "policy_error": str(exc), "lock_error": str(exc)}
        self.apply_documents(payload)

    def apply_documents(self, payload):
        """Render what read_documents returned."""
        payload = dict(payload or {})

        if payload.get("lifecycle_error"):
            self._rules = []
            self._lifecycle_status.setText(
                f"Could not read the rules: {payload['lifecycle_error']}")
        else:
            self._rules = list(payload.get("lifecycle") or [])
            self._lifecycle_status.setText(
                "" if self._rules else "No lifecycle rules on this bucket.")
        self._refresh_rules()

        if payload.get("cors_error"):
            self._cors_status.setText(
                f"Could not read CORS: {payload['cors_error']}")
        else:
            rules = list(payload.get("cors") or [])
            self._cors_text.setPlainText(
                json.dumps(rules, indent=2) if rules else "")
            self._cors_status.setText(
                "" if rules else "No CORS configuration on this bucket.")

        if payload.get("policy_error"):
            self._policy_status.setText(
                f"Could not read the policy: {payload['policy_error']}")
        else:
            policy = str(payload.get("policy") or "")
            if policy:
                try:
                    policy = json.dumps(json.loads(policy), indent=2)
                except ValueError:
                    pass
            self._policy_text.setPlainText(policy)
            status = payload.get("policy_status") or ""
            self._policy_status.setText(
                (f"This bucket is {status}." if status else "")
                + ("" if policy else " No bucket policy is set."))

        self._apply_bucket_documents(payload)

        if payload.get("lock_error"):
            self._lock_status.setText(
                f"Could not read Object Lock: {payload['lock_error']}")
            return
        config = dict(payload.get("lock") or {})
        if not config.get("enabled"):
            self._lock_status.setText(
                "Object Lock is not enabled on this bucket.\n\n"
                "It can only be turned on when a bucket is created, and it "
                "makes objects undeletable for the retention period — "
                "including by you.")
            return
        retention = config.get("mode") or "none"
        window = ""
        if config.get("days"):
            window = f"{config['days']} day(s)"
        elif config.get("years"):
            window = f"{config['years']} year(s)"
        self._lock_status.setText(
            "Object Lock is <b>enabled</b> on this bucket.<br><br>"
            f"Default retention mode: {retention}<br>"
            f"Default retention period: {window or 'not set'}<br><br>"
            "Per-object retention and legal holds are shown in Properties, "
            "and a legal hold can be placed from the object's context menu.")

    def _apply_bucket_documents(self, payload):
        """Render the encryption / tags / website / events tabs."""
        encryption = dict(payload.get("encryption") or {})
        index = self._sse.findData(encryption.get("sse", ""))
        self._sse.setCurrentIndex(max(index, 0))
        self._sse_key.setText(encryption.get("kms_key", ""))
        self._bucket_key.setChecked(bool(encryption.get("bucket_key")))
        self._sync_sse_fields()

        tags = dict(payload.get("tags") or {})
        self._bucket_tags.setPlainText(
            "\n".join(f"{key} = {value}" for key, value in sorted(tags.items())))

        status = str(payload.get("acceleration") or "")
        self._acceleration_was = status == "Enabled"
        self._acceleration.setChecked(self._acceleration_was)
        self._acceleration.setEnabled(self._writable and bool(status is not None))

        problems = [payload[name] for name in
                    ("encryption_error", "tags_error", "acceleration_error")
                    if payload.get(name)]
        self._bucket_status.setText(
            "  ".join(problems) if problems else "")

        website = dict(payload.get("website") or {})
        self._site_index.setText(website.get("index", ""))
        self._site_error.setText(website.get("error", ""))
        self._site_redirect.setText(website.get("redirect", ""))
        if payload.get("website_error"):
            self._site_status.setText(
                f"Could not read it: {payload['website_error']}")
        else:
            self._site_status.setText(
                "" if website else "Website hosting is not configured.")

        if payload.get("events_error"):
            self._events.setText(
                f"Could not read them: {payload['events_error']}")
            return
        events = list(payload.get("events") or [])
        if not events:
            self._events.setText("No event notifications are configured.")
            return
        lines = []
        for kind, target, names in events:
            lines.append(
                f"<b>{kind}</b> → {target}"
                + (f"<br>&nbsp;&nbsp;{', '.join(names)}" if names else ""))
        self._events.setText("<br>".join(lines))

    def _refresh_rules(self):
        self._rules_list.clear()
        for rule in self._rules:
            name = str(rule.get("ID") or "(unnamed)")
            state = "" if rule.get("Status") == "Enabled" else "  [disabled]"
            item = QListWidgetItem(
                f"{name}{state}\n    "
                f"{DataModel.summarize_lifecycle_rule(rule)}")
            self._rules_list.addItem(item)

    def _selected_rule_index(self) -> int:
        return self._rules_list.currentRow()

    def _add_rule(self):
        dlg = LifecycleRuleDialog(self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._rules.append(dlg.result_rule())
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _edit_rule(self):
        index = self._selected_rule_index()
        if not (0 <= index < len(self._rules)):
            return
        if not DataModel.lifecycle_filter_is_simple(self._rules[index]):
            # Rebuilding it from the prefix form would drop the tag or size
            # filter, quietly widening the rule to every object under the
            # prefix — for an expiration rule, that deletes more than before.
            QMessageBox.information(
                self, "Lifecycle rule",
                "This rule is filtered by tag or object size, which this "
                "editor cannot represent. It can be removed here, but editing "
                "it would silently widen it to the whole prefix — change it "
                "in the provider's own console instead.")
            return
        dlg = LifecycleRuleDialog(self, self._rules[index])
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._rules[index] = dlg.result_rule()
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _remove_rule(self):
        index = self._selected_rule_index()
        if not (0 <= index < len(self._rules)):
            return
        del self._rules[index]
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _save_lifecycle(self):
        summary = "\n".join(
            f"• {rule.get('ID')}: {DataModel.summarize_lifecycle_rule(rule)}"
            for rule in self._rules
        ) or "(no rules — the configuration will be removed)"
        answer = QMessageBox.question(
            self, "Lifecycle rules",
            "Write these rules to "
            f"{self._model.bucket}?\n\n{summary}\n\n"
            "The service will apply them on its own schedule, and "
            "expirations delete objects permanently.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.put_bucket_lifecycle(
                self._rules, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Lifecycle rules", str(exc))
            return
        self._lifecycle_status.setText("Saved.")
        self.reload()

    def _save_cors(self):
        text = self._cors_text.toPlainText().strip()
        rules = []
        if text:
            try:
                rules = json.loads(text)
            except ValueError as exc:
                QMessageBox.warning(self, "CORS", f"Not valid JSON: {exc}")
                return
            if not isinstance(rules, list):
                QMessageBox.warning(
                    self, "CORS", "The document must be a list of rules.")
                return
        try:
            self._model.put_bucket_cors(rules, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "CORS", str(exc))
            return
        self._cors_status.setText("Saved.")
        self.reload()

    def _toggle_policy_edit(self, enabled):
        self._policy_text.setReadOnly(not enabled)
        self._btn_policy_save.setEnabled(bool(enabled))

    def _save_policy(self):
        text = self._policy_text.toPlainText().strip()
        answer = QMessageBox.question(
            self, "Bucket policy",
            ("Remove the bucket policy?" if not text else
             "Replace the policy on "
             f"{self._model.bucket}?\n\nA policy decides who can reach "
             "every object in this bucket."),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.put_bucket_policy(text, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Bucket policy", str(exc))
            return
        self._policy_status.setText("Saved.")
        self.reload()
