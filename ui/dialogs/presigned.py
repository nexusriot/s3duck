"""Minting a time-limited link to one object, or a browser upload form.
"""

import html
import shlex
from PyQt6 import QtWidgets
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QImage, QPixmap
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QFormLayout, QHBoxLayout, QLabel,
    QLineEdit, QPlainTextEdit, QPushButton, QSpinBox, QVBoxLayout, QWidget,
)
from core.qr import LOW, MEDIUM, byte_capacity, encode as qr_encode

# The quiet zone the standard requires around a symbol, in modules.
QR_BORDER = 4


def qr_level_for(text) -> int:
    """MEDIUM while the symbol stays small enough to scan off a screen."""
    size = len(str(text).encode("utf-8"))
    return MEDIUM if size <= byte_capacity(25, MEDIUM) else LOW


def qr_image(text, scale=4) -> QImage:
    """*text* as a black-on-white QR image with its quiet zone."""
    matrix = qr_encode(text, qr_level_for(text))
    modules = len(matrix) + 2 * QR_BORDER
    image = QImage(modules, modules, QImage.Format.Format_RGB32)
    image.fill(QColor("white"))
    black = QColor("black").rgb()
    for y, row in enumerate(matrix):
        for x, dark in enumerate(row):
            if dark:
                image.setPixel(x + QR_BORDER, y + QR_BORDER, black)
    return image.scaled(modules * scale, modules * scale,
                        Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.FastTransformation)


class QrCodeDialog(QDialog):
    """A link as a QR code, to hand to a phone."""

    def __init__(self, parent, text):
        super().__init__(parent)
        self.setWindowTitle("QR code")
        lay = QVBoxLayout(self)
        self.image = QLabel()
        self.image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        note = QLabel("")
        note.setWordWrap(True)
        try:
            image = qr_image(text, scale=max(2, 600 // (len(qr_encode(
                text, qr_level_for(text))) + 2 * QR_BORDER)))
        except ValueError as exc:
            note.setText(str(exc))
        else:
            self.image.setPixmap(QPixmap.fromImage(image))
            note.setText("Anyone who scans this can use the link until it "
                         "expires.")
        close = QPushButton("Close")
        close.clicked.connect(self.accept)
        lay.addWidget(self.image)
        lay.addWidget(note)
        lay.addWidget(close)


def post_form_curl(post) -> str:
    """A curl command that submits *post* (``{"url", "fields"}``) with a file."""
    parts = ["curl"]
    # --form-string, not -F: -F reads a value starting with @ or < as a file
    # and parses ;type= out of it, which a key or a Content-Type can contain.
    for name, value in (post.get("fields") or {}).items():
        parts += ["--form-string", shlex.quote(f"{name}={value}")]
    # The file must be the last field: S3 ignores anything after it.
    parts += ["-F", shlex.quote("file=@/path/to/file"),
              shlex.quote(post.get("url") or "")]
    return " ".join(parts)


def post_form_html(post) -> str:
    """The same form as a minimal HTML page, ready to hand to a browser."""
    rows = "\n".join(
        f'  <input type="hidden" name="{html.escape(str(name))}" '
        f'value="{html.escape(str(value))}">'
        for name, value in (post.get("fields") or {}).items())
    return (f'<form action="{html.escape(post.get("url") or "")}" method="post" '
            'enctype="multipart/form-data">\n'
            f"{rows}\n"
            '  <input type="file" name="file">\n'
            '  <input type="submit" value="Upload">\n'
            "</form>")


class PresignedLinkDialog(QDialog):
    """Generate a temporary download (GET) or upload (PUT) link for an object,
    or an upload form (POST) the service enforces limits on, with a
    configurable expiry."""

    GET, PUT, POST = 0, 1, 2

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
        self._type.addItems(["Download (GET)", "Upload (PUT)",
                             "Upload form (POST)"])
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

        # POST-only: limits the service checks on every upload through the
        # form, which a presigned PUT cannot express.
        self._max_mb = QSpinBox()
        self._max_mb.setRange(0, 5 * 1024)
        self._max_mb.setValue(100)
        self._max_mb.setSuffix(" MiB")
        self._max_mb.setSpecialValueText("no limit")
        self._ctype = QLineEdit()
        self._ctype.setPlaceholderText("e.g. image/png (blank = any)")
        self._any_name = QCheckBox(
            "Let the uploader choose the file name (in this object's folder)")
        self._post_widgets = (self._max_mb, self._ctype, self._any_name)

        form = QFormLayout()
        form.addRow(QLabel("Link type"), self._type)
        form.addRow(QLabel("Expires in"), exp_w)
        form.addRow(QLabel("Largest upload"), self._max_mb)
        form.addRow(QLabel("Content-Type"), self._ctype)
        form.addRow(QLabel(""), self._any_name)

        self._url = QLineEdit()
        self._url.setReadOnly(True)
        self._note = QLabel("")
        self._note.setWordWrap(True)
        self._fields = QPlainTextEdit()
        self._fields.setReadOnly(True)
        self._fields.setMaximumHeight(140)
        self._fields.hide()
        self._post = None

        copy_btn = QPushButton("Copy")
        self._qr_btn = QPushButton("QR code…")
        close_btn = QPushButton("Close")
        copy_btn.clicked.connect(self._copy)
        self._qr_btn.clicked.connect(
            lambda: QrCodeDialog(self, self._url.text()).exec())
        close_btn.clicked.connect(self.reject)
        btns = QHBoxLayout()
        btns.addWidget(copy_btn)
        btns.addWidget(self._qr_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(QLabel("URL:"))
        lay.addWidget(self._url)
        lay.addWidget(self._fields)
        lay.addWidget(self._note)
        lay.addLayout(btns)

        self._type.currentIndexChanged.connect(self._regenerate)
        self._amount.valueChanged.connect(self._regenerate)
        self._unit.currentIndexChanged.connect(self._regenerate)
        self._max_mb.valueChanged.connect(self._regenerate)
        self._ctype.textChanged.connect(self._regenerate)
        self._any_name.toggled.connect(self._regenerate)
        self._regenerate()

    def _expires_sec(self):
        return self._amount.value() * self.UNITS[self._unit.currentIndex()][1]

    def _regenerate(self):
        secs = self._expires_sec()
        note = ""
        if secs > self.MAX_EXPIRES:
            secs = self.MAX_EXPIRES
            note = "Clamped to the S3 maximum of 7 days. "
        mode = self._type.currentIndex()
        for widget in self._post_widgets:
            widget.setEnabled(mode == self.POST)
        self._fields.setVisible(mode == self.POST)
        # A form is not a link: a phone cannot submit one from a scan.
        self._qr_btn.setEnabled(mode == self.GET)
        self._post = None
        if mode == self.POST:
            self._regenerate_post(secs, note)
            return
        is_upload = mode == self.PUT
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

    def _regenerate_post(self, secs, note):
        key = self._key
        any_name = self._any_name.isChecked()
        if any_name:
            key = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
        try:
            post = self._model.presigned_post(
                key, secs, max_bytes=self._max_mb.value() * 1024 * 1024,
                content_type=self._ctype.text().strip(),
                any_key_under_prefix=any_name)
        except Exception as exc:
            self._url.setText("")
            self._fields.setPlainText("")
            self._note.setText(f"Could not generate the form: {exc}")
            return
        self._post = post
        self._url.setText(post["url"])
        self._fields.setPlainText(
            post_form_curl(post) + "\n\n" + post_form_html(post))
        self._note.setText(
            note + "The size and Content-Type limits are enforced by the "
            "service. Copy gives the curl command; the HTML form below it "
            "works in any browser.")

    def post_form(self):
        """The current upload form, or None when another link type is shown."""
        return self._post

    def _copy(self):
        if self._post is not None:
            QtWidgets.QApplication.clipboard().setText(post_form_curl(self._post))
            self._note.setText("curl command copied to clipboard.")
            return
        if self._url.text():
            QtWidgets.QApplication.clipboard().setText(self._url.text())
            self._note.setText("Link copied to clipboard.")
