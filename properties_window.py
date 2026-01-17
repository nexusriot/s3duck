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
        self.publicUrl = QLineEdit()
        self.publicUrl.setReadOnly(True)

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

        display_public_url = ""


        try:
            resp = self.model.object_properties(key)
            # resp is either:
            #   - dict from bucket_properties()
            #   - real get_object() response (boto3 dict-like)
            is_bucket_root =  isinstance(resp, dict) and resp.get("IsBucketRoot")
            if is_bucket_root:
                # bucket root props
                display_key = f"s3://{resp.get('Bucket','')}/"
                display_size = "N/A"
                display_etag = ""
            else:
                et = ""
                try:
                    et = resp.get("ETag", "").replace('"', "")
                except Exception:
                    pass
                display_etag = et

                # compute size using get_size (handles folders/prefix too)
                display_size = str(self.model.get_size(key)) + " Bytes"
            try:
                if is_bucket_root or not key:
                    # bucket root URL
                    ep = self.model.endpoint.rstrip("/")
                    b = (self.model.bucket or "").strip("/")
                    display_public_url = f"{ep}/{b}/" if b else ep
                else:
                    # object URL (preferred: use helper if you added it)
                    if hasattr(self.model, "direct_object_url"):
                        display_public_url = self.model.direct_object_url(key)
                    else:
                        # fallback: path-style
                        ep = self.model.endpoint.rstrip("/")
                        display_public_url = f"{ep}/{self.model.bucket}/{key}"
            except Exception:
                # TODO warning
                pass

        except botocore.exceptions.ClientError:
            # fallback on access errors
            display_size = "N/A"
            display_etag = ""
        except Exception:
            display_size = "N/A"
            display_etag = ""


        self.keyName.setText(display_key)
        self.size.setText(display_size)
        self.eTag.setText(display_etag)
        self.publicUrl.setText(display_public_url)

    def exit(self):
        self.close()

    def exec_(self):
        super().exec_()

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Key / Bucket"), self.keyName)
        layout.addRow(QLabel("Size"), self.size)
        layout.addRow(QLabel("ETag"), self.eTag)
        layout.addRow(QLabel("Public URL"), self.publicUrl)
        self.formGroupBox.setLayout(layout)
