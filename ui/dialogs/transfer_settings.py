"""Concurrency, part sizes, bandwidth, checksums and upload rules.
"""

from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFormLayout, QLabel,
    QLineEdit, QPlainTextEdit, QSpinBox, QVBoxLayout,
)
from model import CHECKSUM_ALGORITHMS, Model as DataModel
from ui.constants import DEFAULT_LISTING_LIMIT


class TransferSettingsDialog(QDialog):
    """Parallelism plus the storage class / encryption applied to uploads."""

    def __init__(self, parent, *, concurrency, max_concurrency, storage_classes,
                 sse_modes, storage_class="", sse="", kms_key_id="",
                 notify=True, parallel_files=4, max_parallel_files=32,
                 verify_downloads=False, rate_limit_kbps=0,
                 checksum_algorithm="", multipart_threshold_mb=16,
                 multipart_chunksize_mb=8, resumable_uploads=True,
                 detect_content_type=True, content_type_overrides=None,
                 listing_limit=DEFAULT_LISTING_LIMIT, auto_retry=True,
                 upload_rules=None, log_to_file=True):
        super().__init__(parent)
        self.setWindowTitle("Transfer settings")
        self.setMinimumWidth(460)

        self._concurrency = QSpinBox()
        self._concurrency.setRange(1, max_concurrency)
        self._concurrency.setValue(int(concurrency))

        self._parallel_files = QSpinBox()
        self._parallel_files.setRange(1, max_parallel_files)
        self._parallel_files.setValue(int(parallel_files))

        self._storage = QComboBox()
        for name in storage_classes:
            self._storage.addItem(name or "(bucket default)", name)
        self._select_data(self._storage, storage_class)

        self._sse = QComboBox()
        for mode in sse_modes:
            self._sse.addItem(mode or "(none)", mode)
        self._select_data(self._sse, sse)

        self._kms = QLineEdit(kms_key_id)
        self._kms.setPlaceholderText("KMS key id or ARN (aws:kms only)")
        self._sse.currentIndexChanged.connect(self._sync_kms)

        self._notify = QCheckBox(
            "Notify when transfers finish while the window is in the background")
        self._notify.setChecked(bool(notify))

        self._rate_limit = QSpinBox()
        self._rate_limit.setRange(0, 10 * 1024 * 1024)
        self._rate_limit.setSuffix(" KB/s")
        self._rate_limit.setSpecialValueText("unlimited")
        self._rate_limit.setValue(int(rate_limit_kbps or 0))

        self._verify = QCheckBox(
            "Verify downloads against the object's stored digest "
            "(re-reads each file)")
        self._verify.setChecked(bool(verify_downloads))

        self._resumable = QCheckBox(
            "Resume interrupted uploads (multipart, leaves parts on the "
            "server until the transfer finishes)")
        self._resumable.setChecked(bool(resumable_uploads))

        self._chunk = QSpinBox()
        self._chunk.setRange(DataModel.MIN_MULTIPART_CHUNKSIZE_MB,
                             DataModel.MAX_MULTIPART_CHUNKSIZE_MB)
        self._chunk.setSuffix(" MiB")
        self._chunk.setValue(int(multipart_chunksize_mb))

        self._threshold = QSpinBox()
        self._threshold.setRange(DataModel.MIN_MULTIPART_CHUNKSIZE_MB, 4096)
        self._threshold.setSuffix(" MiB")
        self._threshold.setValue(int(multipart_threshold_mb))

        self._checksum = QComboBox()
        self._checksum.addItem("None (ETag only)", "")
        for name in CHECKSUM_ALGORITHMS:
            self._checksum.addItem(name, name)
        index = self._checksum.findData(str(checksum_algorithm or "").upper())
        self._checksum.setCurrentIndex(max(index, 0))

        self._listing_limit = QSpinBox()
        self._listing_limit.setRange(0, 5_000_000)
        self._listing_limit.setSingleStep(1000)
        self._listing_limit.setSpecialValueText("no limit")
        self._listing_limit.setValue(int(listing_limit or 0))

        self._log_to_file = QCheckBox(
            "Write the session log to ~/.config/s3duck/s3duck.log")
        self._log_to_file.setChecked(bool(log_to_file))

        self._auto_retry = QCheckBox(
            "Retry a job once when the service asks for it "
            "(throttling, 5xx, dropped connection)")
        self._auto_retry.setChecked(bool(auto_retry))

        self._detect_type = QCheckBox(
            "Set Content-Type from the file extension when uploading")
        self._detect_type.setChecked(bool(detect_content_type))

        self._type_overrides = QPlainTextEdit(
            DataModel.format_content_type_overrides(content_type_overrides))
        self._type_overrides.setPlaceholderText(
            "one per line:  ext = type\nmd = text/markdown")
        self._type_overrides.setMaximumHeight(72)

        self._upload_rules = QPlainTextEdit(
            DataModel.format_upload_rules(upload_rules))
        self._upload_rules.setPlaceholderText(
            "archive/* -> class=GLACIER\n"
            "logs/ -> class=STANDARD_IA, tag:team=infra")
        self._upload_rules.setMaximumHeight(72)
        self._detect_type.toggled.connect(self._type_overrides.setEnabled)
        self._type_overrides.setEnabled(self._detect_type.isChecked())

        form = QFormLayout()
        form.addRow(QLabel("Files transferred at once"), self._parallel_files)
        form.addRow(QLabel("Connections per file (multipart)"), self._concurrency)
        form.addRow(QLabel("Bandwidth limit (all transfers)"), self._rate_limit)
        form.addRow(QLabel("Multipart part size"), self._chunk)
        form.addRow(QLabel("Use multipart above"), self._threshold)
        form.addRow(QLabel("Upload storage class"), self._storage)
        form.addRow(QLabel("Upload encryption"), self._sse)
        form.addRow(QLabel("KMS key"), self._kms)
        form.addRow(QLabel("Upload checksum"), self._checksum)
        form.addRow(QLabel("Entries loaded per listing"), self._listing_limit)
        form.addRow(self._detect_type)
        form.addRow(QLabel("Content-Type overrides"), self._type_overrides)
        form.addRow(QLabel("Upload rules by destination"), self._upload_rules)
        form.addRow(self._resumable)
        form.addRow(self._auto_retry)
        form.addRow(self._log_to_file)
        form.addRow(self._verify)
        form.addRow(self._notify)

        note = QLabel(
            "Total connections is roughly files × connections-per-file. "
            "Storage class and encryption apply to new uploads; existing "
            "objects are unaffected, use \"Change storage class…\" for those. "
            "Without a detected Content-Type an object is stored as "
            "binary/octet-stream, which a browser downloads instead of showing."
        )
        note.setWordWrap(True)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(note)
        lay.addWidget(btns)
        self._sync_kms()

    @staticmethod
    def _select_data(combo, value):
        idx = combo.findData(value or "")
        combo.setCurrentIndex(idx if idx >= 0 else 0)

    def _sync_kms(self):
        self._kms.setEnabled(self.sse() == "aws:kms")

    def concurrency(self) -> int:
        return self._concurrency.value()

    def parallel_files(self) -> int:
        return self._parallel_files.value()

    def storage_class(self) -> str:
        return self._storage.currentData() or ""

    def sse(self) -> str:
        return self._sse.currentData() or ""

    def kms_key_id(self) -> str:
        return self._kms.text().strip() if self.sse() == "aws:kms" else ""

    def notify(self) -> bool:
        return self._notify.isChecked()

    def verify_downloads(self) -> bool:
        return self._verify.isChecked()

    def checksum_algorithm(self) -> str:
        return self._checksum.currentData() or ""

    def resumable_uploads(self) -> bool:
        return self._resumable.isChecked()

    def multipart_chunksize_mb(self) -> int:
        return self._chunk.value()

    def multipart_threshold_mb(self) -> int:
        return self._threshold.value()

    def rate_limit_kbps(self) -> int:
        return self._rate_limit.value()

    def listing_limit(self) -> int:
        return self._listing_limit.value()

    def auto_retry(self) -> bool:
        return self._auto_retry.isChecked()

    def upload_rules(self) -> list:
        return DataModel.parse_upload_rules(self._upload_rules.toPlainText())

    def log_to_file(self) -> bool:
        return self._log_to_file.isChecked()

    def detect_content_type(self) -> bool:
        return self._detect_type.isChecked()

    def content_type_overrides(self) -> dict:
        return DataModel.parse_content_type_overrides(
            self._type_overrides.toPlainText())
