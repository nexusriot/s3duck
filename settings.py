from PyQt5 import QtCore
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *

from utils import str_to_bool


class SettingsWindow(QDialog):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings", ("", "", "", "", "", "", "false", "true"))
        super().__init__(*args, **kwargs)
        (
            name,
            url,
            region,
            bucket,
            access_key,
            secret_key,
            no_ssl_check,
            use_path,
        ) = settings
        self.setWindowTitle("Profile settings")
        self.setGeometry(140, 140, 600, 250)
        qtRectangle = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        qtRectangle.moveCenter(centerPoint)
        self.move(qtRectangle.topLeft())
        self.setWindowModality(Qt.ApplicationModal)

        self.formGroupBox = QGroupBox("Connection settings")
        self.nameLineEdit = QLineEdit()
        self.urlLineEdit = QLineEdit()
        self.regionEdit = QLineEdit()
        self.bucketName = QLineEdit()
        self.accessKeyEdit = QLineEdit()
        self.secretKeyEdit = QLineEdit()
        self.noSslCheck = QCheckBox()
        self.usePath = QCheckBox()

        self.createForm()
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttonBox.accepted.connect(self.setRetVal)
        self.buttonBox.rejected.connect(self.reject)
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(self.formGroupBox)
        mainLayout.addWidget(self.buttonBox)
        self.setLayout(mainLayout)
        self.nameLineEdit.textChanged.connect(self.on_text_changed)
        self.urlLineEdit.textChanged.connect(self.on_text_changed)
        self.regionEdit.textChanged.connect(self.on_text_changed)
        self.accessKeyEdit.textChanged.connect(self.on_text_changed)
        self.secretKeyEdit.textChanged.connect(self.on_text_changed)
        btn_apply = self.buttonBox.button(QDialogButtonBox.Ok)
        btn_apply.clicked.connect(self.setRetVal)
        btn_apply.setEnabled(False)
        self.retrunVal = None
        self.nameLineEdit.setText(name)
        self.urlLineEdit.setText(url)
        self.regionEdit.setText(region)
        self.bucketName.setText(bucket)
        self.accessKeyEdit.setText(access_key)
        self.secretKeyEdit.setText(secret_key)
        self.secretKeyEdit.setEchoMode(QLineEdit.Password)
        self.noSslCheck.setChecked(str_to_bool(no_ssl_check))
        self.usePath.setChecked(str_to_bool(use_path))

    @QtCore.pyqtSlot()
    def on_text_changed(self):
        btn_apply = self.buttonBox.button(QDialogButtonBox.Ok)
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
        )
        self.close()

    def exec_(self):
        super().exec_()
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
            QLabel("No SSL check (self-signed certificate support)"), self.noSslCheck
        )
        layout.addRow(QLabel("Use path in config (minio support)"), self.usePath)
        self.formGroupBox.setLayout(layout)
