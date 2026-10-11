"""Minting a time-limited link to one object.
"""

from PyQt6 import QtWidgets
from PyQt6.QtWidgets import (
    QComboBox, QDialog, QFormLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QSpinBox, QVBoxLayout, QWidget,
)


class PresignedLinkDialog(QDialog):
    """Generate a temporary download (GET) or upload (PUT) link for an object,
    with a configurable expiry."""

    UNITS = (("Minutes", 60), ("Hours", 3600), ("Days", 86400))
    MAX_EXPIRES = 7 * 24 * 3600  # SigV4 presigned-URL maximum (7 days)

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Share link — {base}")
        self.resize(640, 220)

        self._type = QComboBox()
        self._type.addItems(["Download (GET)", "Upload (PUT)"])
        self._amount = QSpinBox()
        self._amount.setRange(1, 100000)
        self._amount.setValue(1)
        self._unit = QComboBox()
        self._unit.addItems([u for u, _ in self.UNITS])
        self._unit.setCurrentIndex(1)  # Hours

        exp_row = QHBoxLayout()
        exp_row.addWidget(self._amount)
        exp_row.addWidget(self._unit)
        exp_row.addStretch(1)
        exp_w = QWidget()
        exp_w.setLayout(exp_row)

        form = QFormLayout()
        form.addRow(QLabel("Link type"), self._type)
        form.addRow(QLabel("Expires in"), exp_w)

        self._url = QLineEdit()
        self._url.setReadOnly(True)
        self._note = QLabel("")
        self._note.setWordWrap(True)

        copy_btn = QPushButton("Copy")
        close_btn = QPushButton("Close")
        copy_btn.clicked.connect(self._copy)
        close_btn.clicked.connect(self.reject)
        btns = QHBoxLayout()
        btns.addWidget(copy_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(QLabel("URL:"))
        lay.addWidget(self._url)
        lay.addWidget(self._note)
        lay.addLayout(btns)

        self._type.currentIndexChanged.connect(self._regenerate)
        self._amount.valueChanged.connect(self._regenerate)
        self._unit.currentIndexChanged.connect(self._regenerate)
        self._regenerate()

    def _expires_sec(self):
        return self._amount.value() * self.UNITS[self._unit.currentIndex()][1]

    def _regenerate(self):
        secs = self._expires_sec()
        note = ""
        if secs > self.MAX_EXPIRES:
            secs = self.MAX_EXPIRES
            note = "Clamped to the S3 maximum of 7 days. "
        is_upload = self._type.currentIndex() == 1
        try:
            if is_upload:
                url = self._model.presigned_put_url(self._key, secs)
            else:
                url = self._model.presigned_get_url(self._key, secs)
        except Exception as exc:
            self._url.setText("")
            self._note.setText(f"Could not generate link: {exc}")
            return
        self._url.setText(url)
        if is_upload:
            note += 'Upload with:  curl --upload-file <file> "<URL>"'
        self._note.setText(note)

    def _copy(self):
        if self._url.text():
            QtWidgets.QApplication.clipboard().setText(self._url.text())
            self._note.setText("Link copied to clipboard.")
