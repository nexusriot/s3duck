"""Permissions and protection: ACL grants, Object Lock retention, bucket-policy
templates and the session's customer-provided encryption key.
"""

import base64
import hashlib
import os
from datetime import datetime, timedelta, timezone
from PyQt6.QtCore import QDateTime, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDateTimeEdit, QDialog,
    QDialogButtonBox, QFormLayout, QHBoxLayout, QLabel, QLineEdit,
    QMessageBox, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.acl import (
    GROUPS, PERMISSIONS, acl_is_editable, describe_grantee,
    owner_keeps_control, public_grants, validate_grant,
)
from core.policy_templates import TEMPLATES, build_template
from core.retention import MODES, assess_retention_change


class GrantDialog(QDialog):
    """One grant: who, and what they may do."""

    KINDS = (("Canonical user ID", "CanonicalUser"),
             ("Group", "Group"),
             ("Email address", "AmazonCustomerByEmail"))

    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle("Add grant")
        self.kind = QComboBox()
        for label, value in self.KINDS:
            self.kind.addItem(label, value)
        self.who = QLineEdit()
        self.group = QComboBox()
        for label, uri in GROUPS.items():
            self.group.addItem(label, uri)
        self.permission = QComboBox()
        self.permission.addItems(PERMISSIONS)
        self.permission.setCurrentText("READ")
        self.problem = QLabel("")
        self.problem.setWordWrap(True)
        form = QFormLayout(self)
        form.addRow("Grantee", self.kind)
        form.addRow("ID / email", self.who)
        form.addRow("Group", self.group)
        form.addRow("Permission", self.permission)
        form.addRow(self.problem)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                                   | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)
        self.kind.currentIndexChanged.connect(self._sync)
        self._sync()

    def _sync(self):
        is_group = self.kind.currentData() == "Group"
        self.group.setEnabled(is_group)
        self.who.setEnabled(not is_group)

    def grant(self) -> dict:
        kind = self.kind.currentData()
        grant = {"type": kind, "id": "", "display": "", "uri": "", "email": "",
                 "permission": self.permission.currentText()}
        if kind == "Group":
            grant["uri"] = self.group.currentData()
        elif kind == "AmazonCustomerByEmail":
            grant["email"] = self.who.text().strip()
        else:
            grant["id"] = self.who.text().strip()
        return grant

    def _accept(self):
        problem = validate_grant(self.grant())
        if problem:
            self.problem.setText(problem)
            return
        self.accept()


class AclDialog(QDialog):
    """
    View and edit one ACL -- an object's or the bucket's.

    The save asks twice as hard when the result is public or when the owner
    would lose FULL_CONTROL, because either is a click that is easy to make
    and slow to notice.
    """

    def __init__(self, parent, target, acl, writable=True, save=None):
        super().__init__(parent)
        self.setWindowTitle(f"Permissions (ACL) — {target}")
        self.resize(620, 360)
        self._owner = dict((acl or {}).get("owner") or {})
        self._grants = [dict(g) for g in (acl or {}).get("grants") or []]
        self._save = save
        self._writable = writable and save is not None and acl_is_editable(acl)
        self._unsupported = not acl_is_editable(acl)

        owner = self._owner.get("display") or self._owner.get("id") or "unknown"
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Grantee", "Type", "Permission"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        self.note = QLabel("")
        self.note.setWordWrap(True)

        add_btn = QPushButton("Add grant…")
        add_btn.clicked.connect(self.add_grant)
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(self.remove_selected)
        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self.save)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        for button in (add_btn, remove_btn, self.save_btn):
            button.setEnabled(self._writable)

        row = QHBoxLayout()
        row.addWidget(add_btn)
        row.addWidget(remove_btn)
        row.addStretch(1)
        row.addWidget(self.save_btn)
        row.addWidget(close_btn)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(f"Owner: {owner}"))
        lay.addWidget(self.table, 1)
        lay.addWidget(self.note)
        lay.addLayout(row)
        self._render()

    def grants(self) -> list:
        return [dict(g) for g in self._grants]

    def _render(self):
        self.table.setRowCount(0)
        for grant in self._grants:
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, text in enumerate((describe_grantee(grant), grant["type"],
                                        grant["permission"])):
                self.table.setItem(row, col, QTableWidgetItem(text))
        if self._unsupported:
            self.note.setText(
                "This server reports an ACL without an owner ID, which means "
                "it implements ACLs only as the owner's full control. There "
                "is nothing to edit here; use the bucket policy instead.")
            return
        public = public_grants(self._grants)
        self.note.setText(
            f"⚠ {len(public)} grant(s) make this readable or writable by "
            "people outside your account." if public else "")

    def add_grant(self, grant=None):
        if grant is None:
            dlg = GrantDialog(self)
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return False
            grant = dlg.grant()
        self._grants.append(dict(grant))
        self._render()
        return True

    def remove_selected(self):
        rows = sorted({i.row() for i in self.table.selectedIndexes()}, reverse=True)
        for row in rows:
            if 0 <= row < len(self._grants):
                self._grants.pop(row)
        self._render()

    def save(self):
        if not self._writable:
            return False
        warnings = []
        if public_grants(self._grants):
            warnings.append("Anyone outside your account will get access.")
        if not owner_keeps_control(self._owner, self._grants):
            warnings.append("The owner loses FULL_CONTROL and may not be able "
                            "to change this ACL again.")
        if warnings and QMessageBox.question(
                self, "Permissions (ACL)",
                "Save this ACL?\n\n" + "\n".join(warnings)) \
                != QMessageBox.StandardButton.Yes:
            return False
        try:
            self._save(self._owner, self._grants)
        except Exception as exc:
            QMessageBox.critical(self, "Permissions (ACL)", str(exc))
            return False
        self.accept()
        return True


class RetentionDialog(QDialog):
    """Set one object's Object Lock retention mode and date."""

    def __init__(self, parent, key, state, now=None):
        super().__init__(parent)
        self.setWindowTitle(f"Retention — {key}")
        self._state = dict(state or {})
        self._now = now
        self.mode = QComboBox()
        self.mode.addItems(MODES)
        self.until = QDateTimeEdit()
        self.until.setCalendarPopup(True)
        self.until.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.bypass = QCheckBox(
            "Bypass governance retention (needs s3:BypassGovernanceRetention)")
        self.understood = QCheckBox(
            "I understand COMPLIANCE cannot be shortened or removed by anyone, "
            "including the account root, until the date passes")
        self.verdict = QLabel("")
        self.verdict.setWordWrap(True)

        current = self._state.get("mode") or "none"
        until = self._state.get("retain_until_dt")
        start = until or (datetime.now(timezone.utc) + timedelta(days=30))
        self.until.setDateTime(QDateTime.fromSecsSinceEpoch(int(start.timestamp())))
        if self._state.get("mode") in MODES:
            self.mode.setCurrentText(self._state["mode"])

        form = QFormLayout()
        form.addRow("Current", QLabel(
            f"{current}" + (
                f" until {until.astimezone():%Y-%m-%d %H:%M} (local time)"
                if until else "")))
        form.addRow("Mode", self.mode)
        form.addRow("Retain until", self.until)
        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                                        | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(self.bypass)
        lay.addWidget(self.understood)
        lay.addWidget(self.verdict)
        lay.addWidget(self.buttons)
        for signal in (self.mode.currentIndexChanged, self.until.dateTimeChanged,
                       self.bypass.toggled, self.understood.toggled):
            signal.connect(lambda *_a: self.assess())
        self.assess()

    def chosen(self):
        """``(mode, aware datetime, bypass)``."""
        stamp = self.until.dateTime().toSecsSinceEpoch()
        # The bypass counts only while it is offered: ticked for a shortening
        # the user then undid, it would still be sent with an extension.
        return (self.mode.currentText(),
                datetime.fromtimestamp(stamp, tz=timezone.utc),
                self.bypass.isChecked() and not self.bypass.isHidden())

    def assess(self) -> bool:
        mode, until, bypass = self.chosen()
        problem, needs_bypass, irreversible = assess_retention_change(
            self._state.get("mode"), self._state.get("retain_until_dt"),
            mode, until, now=self._now)
        self.bypass.setVisible(needs_bypass)
        self.understood.setVisible(irreversible and not problem)
        ok = not problem
        if needs_bypass and not bypass:
            ok = False
            problem = ("Shortening GOVERNANCE retention needs the bypass, and "
                       "the permission behind it.")
        if irreversible and not self.understood.isChecked():
            ok = False
        self.verdict.setText(problem)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(ok)
        return ok


class PolicyTemplateDialog(QDialog):
    """Pick a reviewed statement to merge into the bucket policy."""

    def __init__(self, parent, bucket, prefix=""):
        super().__init__(parent)
        self.setWindowTitle("Insert policy template")
        self._bucket = bucket
        self.template = QComboBox()
        for name, label in TEMPLATES.items():
            self.template.addItem(label, name)
        self.prefix = QLineEdit(prefix)
        self.prefix.setPlaceholderText("prefix (blank = whole bucket)")
        self.account = QLineEdit()
        self.account.setPlaceholderText("12-digit AWS account ID")
        self.write = QCheckBox("Also allow writes and deletes")
        self.problem = QLabel("")
        self.problem.setWordWrap(True)
        form = QFormLayout(self)
        form.addRow("Template", self.template)
        form.addRow("Prefix", self.prefix)
        form.addRow("Account", self.account)
        form.addRow("", self.write)
        form.addRow(self.problem)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                                   | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)
        self.template.currentIndexChanged.connect(self._sync)
        self._sync()

    def _sync(self):
        name = self.template.currentData()
        self.prefix.setEnabled(name in ("public_read", "allow_account"))
        self.account.setEnabled(name == "allow_account")
        self.write.setEnabled(name == "allow_account")

    def statements(self) -> list:
        return build_template(
            self.template.currentData(), self._bucket,
            prefix=self.prefix.text().strip(), account_id=self.account.text(),
            write=self.write.isChecked())

    def _accept(self):
        try:
            self.statements()
        except ValueError as exc:
            self.problem.setText(str(exc))
            return
        self.accept()


class SseCKeyDialog(QDialog):
    """
    Set or clear the customer-provided key (SSE-C) for this session.

    The key is shown once, in base64, when generated here: it is never
    written anywhere, and an object encrypted with a key nobody kept is an
    object nobody can read again -- the service does not store it either.
    """

    def __init__(self, parent, current=b""):
        super().__init__(parent)
        self.setWindowTitle("Customer encryption key (SSE-C)")
        self.setMinimumWidth(560)
        self.key = QLineEdit(base64.b64encode(current).decode() if current else "")
        self.key.setPlaceholderText("base64 of 32 random bytes")
        self.key.setEchoMode(QLineEdit.EchoMode.Password)
        show = QCheckBox("Show")
        show.toggled.connect(lambda on: self.key.setEchoMode(
            QLineEdit.EchoMode.Normal if on else QLineEdit.EchoMode.Password))
        generate = QPushButton("Generate")
        generate.clicked.connect(self.generate)
        self.fingerprint = QLabel("")
        self.key.textChanged.connect(self._sync)
        row = QHBoxLayout()
        row.addWidget(self.key, 1)
        row.addWidget(show)
        row.addWidget(generate)
        note = QLabel(
            "While a key is set, every object read and written in this window "
            "uses it: uploads are encrypted with it, and objects that were "
            "not cannot be read until it is cleared. The key is kept in "
            "memory only. <b>Save it somewhere safe</b> — S3 does not keep "
            "it, and without it the objects are unrecoverable.")
        note.setWordWrap(True)
        note.setTextFormat(Qt.TextFormat.RichText)
        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                                        | QDialogButtonBox.StandardButton.Cancel)
        clear = self.buttons.addButton("Clear key",
                                       QDialogButtonBox.ButtonRole.ResetRole)
        clear.clicked.connect(lambda: self.key.setText(""))
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        lay = QVBoxLayout(self)
        lay.addWidget(note)
        lay.addLayout(row)
        lay.addWidget(self.fingerprint)
        lay.addWidget(self.buttons)
        self._sync()

    def generate(self):
        self.key.setText(base64.b64encode(os.urandom(32)).decode())
        self.key.setEchoMode(QLineEdit.EchoMode.Normal)

    def key_bytes(self):
        """The decoded key, b"" for none; raises ValueError when malformed."""
        return parse_sse_c_key(self.key.text())

    def _sync(self):
        try:
            key = self.key_bytes()
        except ValueError as exc:
            self.fingerprint.setText(str(exc))
            self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(False)
            return
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(True)
        self.fingerprint.setText(
            f"Key MD5: {sse_c_fingerprint(key)}" if key else "No key: SSE-C off.")


def parse_sse_c_key(text) -> bytes:
    text = str(text or "").strip()
    if not text:
        return b""
    try:
        key = base64.b64decode(text, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("The key is not valid base64.") from exc
    if len(key) != 32:
        raise ValueError(f"An SSE-C key is 32 bytes; this one is {len(key)}.")
    return key


def sse_c_fingerprint(key) -> str:
    """The MD5 S3 itself reports, so a key can be matched without showing it."""
    return base64.b64encode(hashlib.md5(key).digest()).decode() if key else ""

