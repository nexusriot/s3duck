"""The clickable bucket/prefix path above the listing.
"""

from PyQt6.QtCore import QTimer, Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QFrame, QHBoxLayout, QLabel, QScrollArea, QToolButton, QWidget,
)


class Breadcrumb(QScrollArea):
    """A clickable path bar. Each segment navigates to that prefix; the leading
    'Buckets' segment returns to the bucket list."""

    home = pyqtSignal()
    go = pyqtSignal(str)   # target prefix ('' == bucket root)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setFixedHeight(28)
        self._inner = QWidget()
        self._lay = QHBoxLayout(self._inner)
        self._lay.setContentsMargins(4, 0, 4, 0)
        self._lay.setSpacing(1)
        self._lay.addStretch(1)
        self.setWidget(self._inner)
        self._last_key = None

    def _clear(self):
        # Remove everything except the trailing stretch.
        while self._lay.count() > 1:
            item = self._lay.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()

    def _add_segment(self, text, target, *, is_last):
        btn = QToolButton()
        btn.setText(text)
        btn.setAutoRaise(True)
        if is_last:
            btn.setEnabled(False)
        else:
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            if target is None:
                btn.clicked.connect(lambda: self.home.emit())
            else:
                btn.clicked.connect(lambda _c=False, t=target: self.go.emit(t))
        self._lay.insertWidget(self._lay.count() - 1, btn)

    def _add_sep(self):
        sep = QLabel("›")
        sep.setEnabled(False)
        self._lay.insertWidget(self._lay.count() - 1, sep)

    def set_location(self, bucket, current_folder, in_bucket_list):
        # Skip a rebuild when nothing changed (navigation + resize both call us).
        state_key = (bool(in_bucket_list), bucket or "", current_folder or "")
        if state_key == self._last_key:
            return
        self._last_key = state_key

        self._clear()
        self._add_segment("Buckets", None, is_last=in_bucket_list)
        if not in_bucket_list and bucket:
            folder = current_folder or ""
            self._add_sep()
            self._add_segment(bucket, "", is_last=(folder == ""))
            parts = [p for p in folder.split("/") if p]
            acc = ""
            for i, p in enumerate(parts):
                acc += p + "/"
                self._add_sep()
                self._add_segment(p, acc, is_last=(i == len(parts) - 1))
        # Keep the newest (deepest) segment in view.
        QTimer.singleShot(0, lambda: self.horizontalScrollBar().setValue(
            self.horizontalScrollBar().maximum()))
