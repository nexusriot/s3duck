from PyQt5 import QtCore
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *


class SettingsWindow(QDialog):

    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        url, region, bucket, access_key, secret_key = settings
        self.setWindowTitle("Application settings")
        self.setGeometry(100, 100, 460, 200)
        qtRectangle = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        qtRectangle.moveCenter(centerPoint)
        self.move(qtRectangle.topLeft())
        self.setWindowModality(Qt.ApplicationModal)

        self.formGroupBox = QGroupBox("Connection settings")
        self.urlLineEdit = QLineEdit()
        self.regionEdit = QLineEdit()
        self.bucketName = QLineEdit()
        self.accessKeyEdit = QLineEdit()
        self.secretKeyEdit = QLineEdit()

        self.createForm()
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttonBox.accepted.connect(self.setRetVal)
        self.buttonBox.rejected.connect(self.reject)
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(self.formGroupBox)
        mainLayout.addWidget(self.buttonBox)
        self.setLayout(mainLayout)
        self.urlLineEdit.textChanged.connect(self.on_text_changed)
        self.regionEdit.textChanged.connect(self.on_text_changed)
        self.accessKeyEdit.textChanged.connect(self.on_text_changed)
        self.secretKeyEdit.textChanged.connect(self.on_text_changed)
        btn_apply = self.buttonBox.button(QDialogButtonBox.Ok)
        btn_apply.clicked.connect(self.setRetVal)
        btn_apply.setEnabled(False)
        self.retrunVal = None
        self.urlLineEdit.setText(url)
        self.regionEdit.setText(region)
        self.bucketName.setText(bucket)
        self.accessKeyEdit.setText(access_key)
        self.secretKeyEdit.setText(secret_key)

    @QtCore.pyqtSlot()
    def on_text_changed(self):
        btn_apply = self.buttonBox.button(QDialogButtonBox.Ok)
        btn_apply.setEnabled(
            bool(self.urlLineEdit.text()) and
            bool(self.regionEdit.text()) and
            bool(self.bucketName.text()) and
            bool(self.accessKeyEdit.text()) and
            bool(self.secretKeyEdit.text())
        )

    def setRetVal(self):
        self.retrunVal = (
            self.urlLineEdit.text(),
            self.regionEdit.text(),
            self.bucketName.text(),
            self.accessKeyEdit.text(),
            self.secretKeyEdit.text()
        )
        self.close()

    def exec_(self):
        super().exec_()
        return self.retrunVal

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Url"), self.urlLineEdit)
        layout.addRow(QLabel("Region"), self.regionEdit)
        layout.addRow(QLabel("Bucket name"), self.bucketName)
        layout.addRow(QLabel("Access key"), self.accessKeyEdit)
        layout.addRow(QLabel("Secret key"), self.secretKeyEdit)
        self.formGroupBox.setLayout(layout)
