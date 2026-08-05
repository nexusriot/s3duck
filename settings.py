from PyQt6 import QtCore
from PyQt6.QtCore import *
from PyQt6.QtWidgets import *
from PyQt6.QtGui import *

from utils import str_to_bool, center_on_screen, load_aws_profiles

EMPTY_SETTINGS = ("", "", "", "", "", "", "false", "true", "")


class SettingsWindow(QDialog):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings", EMPTY_SETTINGS)
        super().__init__(*args, **kwargs)
        # Profiles saved before session-token support carry 8 fields.
        settings = tuple(settings) + EMPTY_SETTINGS[len(settings):]
        (
            name,
            url,
            region,
            bucket,
            access_key,
            secret_key,
            no_ssl_check,
            use_path,
            session_token,
        ) = settings
        self.setWindowTitle("Profile settings")
        self.resize(600, 250)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        self.formGroupBox = QGroupBox("Connection settings")
        self.nameLineEdit = QLineEdit()
        self.urlLineEdit = QLineEdit()
        self.regionEdit = QLineEdit()
        self.bucketName = QLineEdit()
        self.accessKeyEdit = QLineEdit()
        self.secretKeyEdit = QLineEdit()
        self.sessionTokenEdit = QLineEdit()
        self.noSslCheck = QCheckBox()
        self.usePath = QCheckBox()

        self.createForm()
        self.importButton = QPushButton("Import from ~/.aws…")
        self.importButton.setToolTip(
            "Fill these fields from a profile in ~/.aws/credentials"
        )
        self.importButton.clicked.connect(self.import_from_aws)
        self.buttonBox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttonBox.accepted.connect(self.setRetVal)
        self.buttonBox.rejected.connect(self.reject)
        importRow = QHBoxLayout()
        importRow.addWidget(self.importButton)
        importRow.addStretch(1)
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(self.formGroupBox)
        mainLayout.addLayout(importRow)
        mainLayout.addWidget(self.buttonBox)
        self.setLayout(mainLayout)
        self.nameLineEdit.textChanged.connect(self.on_text_changed)
        self.urlLineEdit.textChanged.connect(self.on_text_changed)
        self.regionEdit.textChanged.connect(self.on_text_changed)
        self.accessKeyEdit.textChanged.connect(self.on_text_changed)
        self.secretKeyEdit.textChanged.connect(self.on_text_changed)
        btn_apply = self.buttonBox.button(QDialogButtonBox.StandardButton.Ok)
        btn_apply.setEnabled(False)
        self.retrunVal = None
        self.nameLineEdit.setText(name)
        self.urlLineEdit.setText(url)
        self.regionEdit.setText(region)
        self.bucketName.setText(bucket)
        self.accessKeyEdit.setText(access_key)
        self.secretKeyEdit.setText(secret_key)
        self.secretKeyEdit.setEchoMode(QLineEdit.EchoMode.Password)
        self.sessionTokenEdit.setText(session_token)
        self.sessionTokenEdit.setEchoMode(QLineEdit.EchoMode.Password)
        self.noSslCheck.setChecked(str_to_bool(no_ssl_check))
        self.usePath.setChecked(str_to_bool(use_path))

    def import_from_aws(self):
        profiles = load_aws_profiles()
        if not profiles:
            QMessageBox.information(
                self, "Import from ~/.aws",
                "No usable profiles found in ~/.aws/credentials.",
            )
            return
        names = sorted(profiles)
        default = names.index("default") if "default" in names else 0
        chosen, ok = QInputDialog.getItem(
            self, "Import from ~/.aws", "AWS profile:", names, default, False
        )
        if not ok or not chosen:
            return
        entry = profiles[chosen]
        self.accessKeyEdit.setText(entry.get("access_key", ""))
        self.secretKeyEdit.setText(entry.get("secret_key", ""))
        self.sessionTokenEdit.setText(entry.get("session_token", ""))
        if entry.get("region"):
            self.regionEdit.setText(entry["region"])
        if entry.get("endpoint_url"):
            self.urlLineEdit.setText(entry["endpoint_url"])
        elif not self.urlLineEdit.text():
            region = entry.get("region") or "us-east-1"
            self.urlLineEdit.setText(f"https://s3.{region}.amazonaws.com")
        if not self.nameLineEdit.text():
            self.nameLineEdit.setText(chosen)
        self.on_text_changed()

    def showEvent(self, event):
        super().showEvent(event)
        center_on_screen(self)

    @QtCore.pyqtSlot()
    def on_text_changed(self):
        btn_apply = self.buttonBox.button(QDialogButtonBox.StandardButton.Ok)
        btn_apply.setEnabled(
            bool(self.nameLineEdit.text())
            and bool(self.urlLineEdit.text())
            and bool(self.accessKeyEdit.text())
            and bool(self.secretKeyEdit.text())
        )

    def setRetVal(self):
        self.retrunVal = (
            self.nameLineEdit.text(),
            self.urlLineEdit.text(),
            self.regionEdit.text(),
            self.bucketName.text(),
            self.accessKeyEdit.text(),
            self.secretKeyEdit.text(),
            self.noSslCheck.isChecked(),
            self.usePath.isChecked(),
            self.sessionTokenEdit.text(),
        )
        self.close()

    def exec(self):
        super().exec()
        return self.retrunVal

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Name"), self.nameLineEdit)
        layout.addRow(QLabel("Url"), self.urlLineEdit)
        layout.addRow(QLabel("Region"), self.regionEdit)
        layout.addRow(QLabel("Bucket name"), self.bucketName)
        layout.addRow(QLabel("Access key"), self.accessKeyEdit)
        layout.addRow(QLabel("Secret key"), self.secretKeyEdit)
        layout.addRow(
            QLabel("Session token (temporary credentials)"), self.sessionTokenEdit
        )
        layout.addRow(
            QLabel("No SSL check (self-signed certificate support)"), self.noSslCheck
        )
        layout.addRow(QLabel("Use path in config (minio support)"), self.usePath)
        self.formGroupBox.setLayout(layout)
