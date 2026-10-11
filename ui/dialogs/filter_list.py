"""The two type-to-filter pickers: quick open and the command palette.
"""

from PyQt6.QtCore import QEvent, Qt
from PyQt6.QtWidgets import QDialog, QLineEdit, QListWidget, QVBoxLayout
from core.commands import filter_commands


class FilterListDialog(QDialog):
    """
    A query box over a filtered list: type to narrow, arrow to move, Enter to
    pick.

    Shared by the command palette and the quick-open switcher, which were
    identical down to the row-selection accessor. Entries are
    ``(label, extra, payload)``; only the payload's meaning differs.
    """

    def __init__(self, parent, entries, title, placeholder):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(520, 380)
        self._entries = list(entries or [])
        self._visible = []

        self._query = QLineEdit()
        self._query.setPlaceholderText(placeholder)
        self._query.textChanged.connect(self._refresh)
        self._query.installEventFilter(self)

        self._list = QListWidget()
        self._list.itemActivated.connect(lambda _i: self.accept())
        self._list.itemDoubleClicked.connect(lambda _i: self.accept())

        lay = QVBoxLayout(self)
        lay.addWidget(self._query)
        lay.addWidget(self._list)
        self._refresh("")

    def _refresh(self, text):
        self._visible = filter_commands(self._entries, text)
        self._list.clear()
        for label, extra, _payload in self._visible:
            self._list.addItem(f"{label}\t{extra}" if extra else label)
        if self._visible:
            self._list.setCurrentRow(0)

    def eventFilter(self, source, event):
        """Arrows and Enter belong to the list while the cursor is in the box."""
        if source is self._query and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            if key in (Qt.Key.Key_Down, Qt.Key.Key_Up):
                row = self._list.currentRow()
                step = 1 if key == Qt.Key.Key_Down else -1
                self._list.setCurrentRow(
                    max(0, min(self._list.count() - 1, row + step)))
                return True
            if key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                if self._list.count():
                    self.accept()
                return True
        return super().eventFilter(source, event)

    def selected_payload(self):
        """The highlighted row's payload, or None."""
        row = self._list.currentRow()
        if row < 0 or row >= len(self._visible):
            return None
        return self._visible[row][2]


class QuickOpenDialog(FilterListDialog):
    """
    Type-to-jump across bookmarks and buckets.

    Uses the command palette's matcher, so ranking behaves the same in both
    places rather than drifting into two notions of "best match".
    """

    def __init__(self, parent, entries):
        super().__init__(parent, entries, "Go to",
                         "Type a bucket or bookmark…")

    def chosen_location(self):
        """``(bucket, prefix)`` for the highlighted row, or None."""
        return self.selected_payload()


class CommandPaletteDialog(FilterListDialog):
    """
    Type-to-run access to every action.

    The action surface outgrew the toolbar — toolbar, Tools menu, several
    context menus and ~20 shortcuts — so a keyboard-first index is the only
    way to reach a rarely-used command without hunting.
    """

    def __init__(self, parent, entries):
        super().__init__(parent, entries, "Commands", "Type a command…")

    def chosen_action(self):
        """The QAction to trigger, or None."""
        return self.selected_payload()
