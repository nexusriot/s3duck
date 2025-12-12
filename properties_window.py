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

        # defaults
        display_key = key if key else "<bucket root>"
        display_size = ""
        display_etag = ""

        is_bucket_root = False

        try:
            resp = self.model.object_properties(key)
            # resp is either:
            #   - dict from bucket_properties()
            #   - real get_object() response (boto3 dict-like)
            if isinstance(resp, dict) and resp.get("IsBucketRoot"):
                # bucket root props
                is_bucket_root = True
                display_key = f"s3://{resp.get('Bucket','')}/"
                display_size = "N/A"
                display_etag = ""
            else:
                # normal object
                # get ETag safely
                et = ""
                try:
                    et = resp.get("ETag", "").replace('"', "")
                except Exception:
                    pass
                display_etag = et

                # compute size using get_size (handles folders/prefix too)
                display_size = str(self.model.get_size(key)) + " Bytes"

        except botocore.exceptions.ClientError:
            # fallback on access errors
            display_size = "N/A"
            display_etag = ""
        except Exception:
            display_size = "N/A"
            display_etag = ""

        # fill widgets
        self.keyName.setText(display_key)
        self.size.setText(display_size)
        self.eTag.setText(display_etag)

    def exit(self):
        self.close()

    def exec_(self):
        super().exec_()

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Key / Bucket"), self.keyName)
        layout.addRow(QLabel("Size"), self.size)
        layout.addRow(QLabel("ETag"), self.eTag)
        self.formGroupBox.setLayout(layout)
