"""The Qt plugin / icon / library report.
"""

from PyQt6.QtGui import QFontDatabase
from PyQt6.QtWidgets import (
    QApplication, QDialog, QHBoxLayout, QPlainTextEdit, QPushButton,
    QVBoxLayout,
)


class DiagnosticsDialog(QDialog):
    """
    Read-only environment report with a one-click copy.

    Copying matters more than reading it here: the whole point is to get these
    facts into a bug report, where "the venv has QtSvg but the .deb does not"
    is the kind of detail nobody thinks to mention.
    """

    def __init__(self, parent, report):
        super().__init__(parent)
        self.setWindowTitle("Diagnostics")
        self.resize(640, 520)
        self._report = report

        self._text = QPlainTextEdit()
        self._text.setPlainText(report)
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._text.setFont(QFontDatabase.systemFont(
            QFontDatabase.SystemFont.FixedFont))

        self._copy = QPushButton("Copy to clipboard")
        self._copy.clicked.connect(self.copy_report)
        close = QPushButton("Close")
        close.clicked.connect(self.accept)

        row = QHBoxLayout()
        row.addWidget(self._copy)
        row.addStretch(1)
        row.addWidget(close)

        lay = QVBoxLayout(self)
        lay.addWidget(self._text)
        lay.addLayout(row)

    def copy_report(self):
        QApplication.clipboard().setText(self._report)
        self._copy.setText("Copied")
