"""What has been transferred, and what to do about the ones that failed.
"""

from PyQt6.QtWidgets import (
    QAbstractItemView, QDialog, QHBoxLayout, QHeaderView, QLabel,
    QPlainTextEdit, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.naming import _human_bytes


class TransferHistoryDialog(QDialog):
    """Past transfers, with the option to run one again."""

    def __init__(self, parent, main_window, entries):
        super().__init__(parent)
        self._mw = main_window
        self._entries = list(entries or [])
        self.setWindowTitle("Transfer history")
        self.resize(820, 460)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(
            ["When", "Operation", "Items", "Bytes", "Result"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self._table.itemSelectionChanged.connect(self._update_buttons)
        self._render()

        self._btn_rerun = QPushButton("Run again")
        self._btn_rerun.clicked.connect(self._rerun)
        self._btn_rerun.setEnabled(False)
        clear_btn = QPushButton("Clear history")
        clear_btn.clicked.connect(self._clear)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_rerun)
        row.addWidget(clear_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def _render(self):
        self._table.setRowCount(0)
        for entry in self._entries:
            r = self._table.rowCount()
            self._table.insertRow(r)
            cells = [
                entry.get("when", ""),
                entry.get("label", entry.get("method", "")),
                str(entry.get("items", "")),
                _human_bytes(entry.get("bytes") or 0),
                entry.get("status", ""),
            ]
            for c, text in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(text))
        self._table.resizeColumnToContents(0)

    def _selected(self):
        r = self._table.currentRow()
        if 0 <= r < len(self._entries):
            return self._entries[r]
        return None

    def _update_buttons(self):
        entry = self._selected()
        self._btn_rerun.setEnabled(bool(entry and entry.get("job")))

    def _rerun(self):
        entry = self._selected()
        if not entry or not entry.get("job"):
            return
        self.accept()
        self._mw.rerun_history_entry(entry)

    def _clear(self):
        self._entries = []
        self._mw.clear_transfer_history()
        self._render()
        self._update_buttons()


class OverwriteDialog(QDialog):
    """Ask what to do about destinations that already exist."""

    SKIP = "skip"
    OVERWRITE = "overwrite"

    def __init__(self, parent, conflicts, *, total, what="object"):
        super().__init__(parent)
        self.setWindowTitle("Destination already exists")
        self.setMinimumWidth(520)
        self._choice = None

        head = QLabel(
            f"<b>{len(conflicts)} of {total} {what}(s)</b> already exist at the "
            "destination."
        )
        head.setWordWrap(True)

        listing = QPlainTextEdit()
        listing.setReadOnly(True)
        shown = conflicts[:200]
        listing.setPlainText("\n".join(shown) + (
            f"\n… and {len(conflicts) - len(shown)} more"
            if len(conflicts) > len(shown) else ""
        ))
        listing.setMaximumHeight(200)

        skip_btn = QPushButton("Skip existing")
        over_btn = QPushButton("Overwrite")
        cancel_btn = QPushButton("Cancel")
        skip_btn.clicked.connect(lambda: self._pick(self.SKIP))
        over_btn.clicked.connect(lambda: self._pick(self.OVERWRITE))
        cancel_btn.clicked.connect(self.reject)
        skip_btn.setDefault(True)

        row = QHBoxLayout()
        row.addWidget(skip_btn)
        row.addWidget(over_btn)
        row.addStretch(1)
        row.addWidget(cancel_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(head)
        lay.addWidget(listing, 1)
        lay.addLayout(row)

    def _pick(self, choice):
        self._choice = choice
        self.accept()

    def choice(self):
        return self._choice
