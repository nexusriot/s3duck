"""Inventory reports and S3 Select queries.
"""

from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QFormLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QPlainTextEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QVBoxLayout,
)
from core.inventory import inventory_duplicates, summarize_inventory
from core.naming import _human_bytes
from s3.reports import SELECT_FORMATS
from utils import run_with_progress


class InventoryDialog(QDialog):
    """
    Usage and duplicates from the bucket's latest S3 Inventory report.

    A report is at most a day or a week old, and says so; it is offered next
    to the listing-based tools, not instead of them, because a listing is
    always current.
    """

    def __init__(self, parent, model, configs=None):
        super().__init__(parent)
        self._model = model
        self._configs = list(configs or [])
        self.result = None
        self.setWindowTitle(f"Inventory report — {model.bucket}")
        self.resize(640, 460)

        self.config = QComboBox()
        for config in self._configs:
            state = "" if config["enabled"] else " [disabled]"
            self.config.addItem(
                f"{config['id']} — {config['frequency']} {config['format']} → "
                f"{config['destination_bucket']}{state}")
        load_latest = QPushButton("Load latest report")
        load_latest.clicked.connect(lambda: self.load_latest())
        load_latest.setEnabled(bool(self._configs))

        self.manifest_bucket = QLineEdit()
        self.manifest_bucket.setPlaceholderText("bucket holding the report")
        self.manifest_key = QLineEdit()
        self.manifest_key.setPlaceholderText(".../manifest.json")
        load_manual = QPushButton("Load this manifest")
        load_manual.clicked.connect(lambda: self.load(
            self.manifest_bucket.text().strip(), self.manifest_key.text().strip()))

        self.prefix = QLineEdit(model.current_folder or "")
        self.prefix.setPlaceholderText("only count keys under… (blank = all)")
        self.prefix.editingFinished.connect(self.render)

        self.status = QLabel(
            "" if self._configs else
            "No inventory configuration on this bucket; a manifest can still "
            "be opened by key.")
        self.status.setWordWrap(True)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Storage class", "Objects", "Size"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        self.duplicates = QLabel("")
        self.duplicates.setWordWrap(True)

        form = QFormLayout()
        row = QHBoxLayout()
        row.addWidget(self.config, 1)
        row.addWidget(load_latest)
        form.addRow("Configuration", row)
        manual = QHBoxLayout()
        manual.addWidget(self.manifest_bucket)
        manual.addWidget(self.manifest_key, 2)
        manual.addWidget(load_manual)
        form.addRow("Or a manifest", manual)
        form.addRow("Prefix", self.prefix)
        close = QPushButton("Close")
        close.clicked.connect(self.reject)
        bottom = QHBoxLayout()
        bottom.addStretch(1)
        bottom.addWidget(close)
        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(self.status)
        lay.addWidget(self.table, 1)
        lay.addWidget(self.duplicates)
        lay.addLayout(bottom)

    def load_latest(self):
        index = self.config.currentIndex()
        if not (0 <= index < len(self._configs)):
            return False
        config = self._configs[index]
        clone = self._model.clone_for_worker()
        key, exc = run_with_progress(
            self, "Finding the latest report…",
            lambda _w: clone.find_latest_inventory(config))
        if exc is not None:
            QMessageBox.warning(self, "Inventory", str(exc))
            return False
        if not key:
            self.status.setText(
                "No report has been delivered for this configuration yet "
                "(the first one can take up to 48 hours).")
            return False
        return self.load(config["destination_bucket"], key)

    def load(self, bucket, key):
        if not bucket or not key:
            self.status.setText("Name the bucket and the manifest key.")
            return False
        clone = self._model.clone_for_worker()
        result, exc = run_with_progress(
            self, "Reading the inventory report…",
            lambda _w: clone.load_inventory(bucket, key))
        if result is None and exc is None:
            return False
        if exc is not None:
            QMessageBox.warning(self, "Inventory", str(exc))
            return False
        self.result = result
        self.render()
        return True

    def render(self):
        if not self.result:
            return
        rows = self.result["rows"]
        manifest = self.result["manifest"]
        prefix = self.prefix.text().strip()
        summary = summarize_inventory(rows, prefix)
        self.status.setText(
            f"Report of {manifest['created'] or 'unknown date'} for "
            f"{manifest['source_bucket'] or self._model.bucket}: "
            f"{summary['objects']} object(s), {_human_bytes(summary['bytes'])}"
            " — as of the report, not now.")
        self.table.setRowCount(0)
        for cls, totals in summary["by_class"].items():
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, text in enumerate((cls, str(totals["objects"]),
                                        _human_bytes(totals["bytes"]))):
                self.table.setItem(row, col, QTableWidgetItem(text))
        if "ETag" not in manifest["schema"]:
            self.duplicates.setText(
                "Duplicates need the ETag field in the report; add it to the "
                "inventory configuration's optional fields.")
            return
        dup = inventory_duplicates(rows, prefix)["summary"]
        self.duplicates.setText(
            f"Duplicates: {dup['groups']} group(s), {dup['redundant']} "
            f"redundant object(s), {_human_bytes(dup['wasted'])} reclaimable "
            "(confirmed by ETag). Use Find duplicates to act on them.")


def select_format(key) -> str:
    """The S3 Select input format for *key*'s extension, or ""."""
    dot = str(key or "").rfind(".")
    return SELECT_FORMATS.get(str(key)[dot:].lower(), "") if dot >= 0 else ""


class SelectDialog(QDialog):
    """Run SQL over one CSV or JSON object where it is stored."""

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._format = select_format(key) or "csv"
        self.setWindowTitle(f"S3 Select — {key.rsplit('/', 1)[-1]}")
        self.resize(720, 520)
        self.sql = QPlainTextEdit("SELECT * FROM s3object s LIMIT 100")
        self.sql.setMaximumHeight(90)
        self.header = QCheckBox("First line is a header (refer to columns by name)")
        self.header.setChecked(True)
        self.header.setVisible(self._format in ("csv", "tsv"))
        self.output = QPlainTextEdit()
        self.output.setReadOnly(True)
        self.status = QLabel(
            "The query runs on the service: only the matching rows are "
            "transferred, and AWS bills the bytes it scans.")
        self.status.setWordWrap(True)
        run = QPushButton("Run")
        run.clicked.connect(lambda: self.run())
        close = QPushButton("Close")
        close.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addWidget(self.header)
        row.addStretch(1)
        row.addWidget(run)
        row.addWidget(close)
        lay = QVBoxLayout(self)
        lay.addWidget(self.sql)
        lay.addLayout(row)
        lay.addWidget(self.output, 1)
        lay.addWidget(self.status)

    def run(self):
        sql = self.sql.toPlainText().strip()
        if not sql:
            return False
        clone = self._model.clone_for_worker()
        result, exc = run_with_progress(
            self, "Running the query…",
            lambda _w: clone.select_object(
                self._key, sql, self._format, self.header.isChecked()))
        if result is None and exc is None:
            return False
        if exc is not None:
            text = str(exc)
            if "NotImplemented" in text or "MethodNotAllowed" in text:
                text += ("\n\nThis server does not implement S3 Select. AWS "
                         "also closed it to new customers in 2024.")
            self.status.setText(f"Query failed: {text}")
            return False
        self.output.setPlainText(result["text"])
        scanned = _human_bytes(result["scanned"]) if result["scanned"] else "?"
        self.status.setText(
            f"Scanned {scanned}."
            + (" Output cut off at 1 MiB." if result["truncated"] else ""))
        return True
