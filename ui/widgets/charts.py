"""The usage charts, painted with QPainter rather than pulled in as a dependency.
"""

from PyQt6.QtCore import QRectF, Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFontMetrics, QPainter, QPen
from PyQt6.QtWidgets import QWidget
from core.naming import _human_bytes
from core.treemap import squarify


class PieWidget(QWidget):
    def __init__(self, by_cat: dict, parent=None):
        super().__init__(parent)
        self.by_cat = dict(by_cat or {})
        self.setMinimumSize(220, 220)

    def set_data(self, by_cat: dict):
        self.by_cat = dict(by_cat or {})
        self.update()

    def paintEvent(self, e):
        total = sum(max(0, int(v)) for v in self.by_cat.values()) or 1

        # simple, fixed colors
        colors = {
            "Documents": QColor(80, 160, 255),
            "Media": QColor(120, 220, 120),
            "Other": QColor(220, 220, 120),
        }

        r = min(self.width(), self.height()) - 20
        rect = QRectF((self.width() - r) / 2, (self.height() - r) / 2, r, r)

        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        p.setPen(QPen(QColor(40, 40, 40), 1))

        start = 0.0
        for name, val in self.by_cat.items():
            v = max(0, int(val))
            if v <= 0:
                continue
            span = 360.0 * (v / total)
            p.setBrush(colors.get(name, QColor(180, 180, 180)))
            p.drawPie(rect, int(start * 16), int(span * 16))
            start += span


class TreemapWidget(QWidget):
    """
    One level of a prefix tree as proportional rectangles.

    A pie by file category cannot answer "which folder is the bill"; area is
    the only encoding that makes a 400 GB prefix look like one.
    """

    drilled = pyqtSignal(str)

    PALETTE = ("#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#76b7b2",
               "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac")

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows = []
        self._rects = []
        self.setMinimumHeight(260)
        self.setMouseTracking(True)

    def set_rows(self, rows):
        self._rows = list(rows or [])
        self._layout()
        self.update()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._layout()

    def _layout(self):
        self._rects = squarify(
            [row[1] for row in self._rows], 0, 0,
            max(1, self.width()), max(1, self.height()))

    def _row_at(self, point):
        for row, (x, y, width, height) in zip(self._rows, self._rects):
            if (x <= point.x() <= x + width) and (y <= point.y() <= y + height):
                return row
        return None

    def paintEvent(self, _event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        metrics = QFontMetrics(self.font())
        for index, (row, rect) in enumerate(zip(self._rows, self._rects)):
            name, size, is_group = row
            x, y, width, height = (int(v) for v in rect)
            if width <= 0 or height <= 0:
                continue
            colour = QColor(self.PALETTE[index % len(self.PALETTE)])
            if is_group:
                colour = QColor("#9e9e9e")
            painter.fillRect(x, y, width, height, colour)
            painter.setPen(QPen(QColor(0, 0, 0, 60)))
            painter.drawRect(x, y, width, height)
            # A label only where it fits; a clipped one is worse than none.
            if width < 48 or height < metrics.height() + 4:
                continue
            painter.setPen(QPen(QColor("#101010")))
            label = metrics.elidedText(
                name, Qt.TextElideMode.ElideMiddle, width - 8)
            painter.drawText(x + 4, y + metrics.ascent() + 3, label)
            if height >= metrics.height() * 2 + 6:
                painter.drawText(
                    x + 4, y + metrics.height() + metrics.ascent() + 3,
                    _human_bytes(size))
        painter.end()

    def mouseMoveEvent(self, event):
        row = self._row_at(event.position().toPoint())
        self.setToolTip(
            f"{row[0]} — {_human_bytes(row[1])}" if row else "")
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        row = self._row_at(event.position().toPoint())
        if row and not row[2]:
            self.drilled.emit(row[0])
        super().mouseReleaseEvent(event)
