"""The keyboard reference, built from the window's real actions plus the
hand-kept list of keys that have no action (core.commands.LISTVIEW_KEY_HELP).
"""

from PyQt6.QtWidgets import (
    QAbstractItemView, QDialog, QHBoxLayout, QHeaderView, QPushButton,
    QTableWidget, QTableWidgetItem, QVBoxLayout,
)


class ShortcutsDialog(QDialog):
    """Keyboard reference — plain letters are reserved for type-to-search, so
    the shortcuts are modifier, function or navigation keys, none of which
    are obvious."""

    def __init__(self, parent, rows):
        super().__init__(parent)
        self.setWindowTitle("Keyboard shortcuts")
        self.resize(560, 560)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Shortcut", "Action"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        for keys, label in rows:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(keys))
            self._table.setItem(r, 1, QTableWidgetItem(label))
        self._table.resizeColumnToContents(0)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)
