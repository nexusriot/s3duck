import botocore.exceptions
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *


class PropertiesWindow(QDialog):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        model, key = settings
        self.setWindowTitle("Object properties")
        self.setGeometry(140, 140, 600, 200)
        qtRectangle = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        qtRectangle.moveCenter(centerPoint)
        self.move(qtRectangle.topLeft())
        self.setWindowModality(Qt.ApplicationModal)
        self.key = key
        self.model = model

        self.formGroupBox = QGroupBox("Properties")
        self.keyName = QLabel()
        self.size = QLabel()
        self.eTag = QLabel()

        self.createForm()
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok)
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(self.formGroupBox)
        mainLayout.addWidget(self.buttonBox)
        self.setLayout(mainLayout)
        btn_apply = self.buttonBox.button(QDialogButtonBox.Ok)
        btn_apply.clicked.connect(self.exit)
        self.keyName.setText(key)
        self.e_tag = ""
        try:
            resp = self.model.object_properties(key)
            self.e_tag = resp.get("ETag", "").replace('"', "")
        except botocore.exceptions.ClientError:
            pass
        self.size.setText(str(self.model.get_size(key)) + " Bytes")
        self.eTag.setText(self.e_tag)

    def exit(self):
        self.close()

    def exec_(self):
        super().exec_()

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Key"), self.keyName)
        layout.addRow(QLabel("Size"), self.size)
        layout.addRow(QLabel("ETag"), self.eTag)
        self.formGroupBox.setLayout(layout)
