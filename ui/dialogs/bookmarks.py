"""Managing saved places.
"""

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QDialog, QHBoxLayout, QHeaderView, QLabel,
    QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout,
)


class BookmarksDialog(QDialog):
    """Rename or delete saved locations."""

    def __init__(self, parent, bookmarks):
        super().__init__(parent)
        self.setWindowTitle("Bookmarks")
        self.resize(620, 420)
        self._bookmarks = [dict(b) for b in bookmarks or []]

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Name", "Location"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self._render()

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(self._remove)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)

        row = QHBoxLayout()
        row.addWidget(remove_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Edit a name in place, or remove a bookmark."))
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def _render(self):
        self._table.setRowCount(0)
        for entry in self._bookmarks:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(entry.get("name", "")))
            location = QTableWidgetItem(
                f"s3://{entry.get('bucket', '')}/{entry.get('prefix', '')}")
            location.setFlags(location.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(r, 1, location)

    def _sync_names(self):
        """Pull in-place edits back into the backing list.

        _render rebuilds the table from that list, so without this a rename
        followed by a removal would silently lose the rename."""
        for index, entry in enumerate(self._bookmarks):
            item = self._table.item(index, 0)
            if item is not None and item.text():
                entry["name"] = item.text()

    def _remove(self):
        self._sync_names()
        r = self._table.currentRow()
        if 0 <= r < len(self._bookmarks):
            del self._bookmarks[r]
            self._render()

    def bookmarks(self) -> list:
        """The edited list — names are read back out of the table."""
        self._sync_names()
        return [
            {
                "name": entry.get("name", ""),
                "bucket": entry.get("bucket", ""),
                "prefix": entry.get("prefix", ""),
            }
            for entry in self._bookmarks
        ]
