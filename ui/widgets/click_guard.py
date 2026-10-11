"""Swallowing the stray click a just-closed menu would otherwise deliver.
"""

from PyQt6.QtCore import QEvent, QObject, QTimer
from PyQt6.QtWidgets import QWidget


class _OneShotClickGuard(QObject):
    """
    Swallows exactly one mouse press+release pair on a target widget.
    Auto-disarms after the pair or a short timeout.
    """
    def __init__(self, target: QWidget, timeout_ms: int = 350):
        super().__init__(target)
        self._target = target
        self._armed = False
        self._need_press = False
        self._need_release = False
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self.disarm)

    def arm(self):
        if self._armed:
            return
        self._armed = True
        self._need_press = True
        self._need_release = True
        self._target.installEventFilter(self)
        self._timer.start(350)

    def disarm(self):
        if not self._armed:
            return
        try:
            self._target.removeEventFilter(self)
        finally:
            self._armed = False
            self._need_press = False
            self._need_release = False

    def eventFilter(self, obj, event):
        if not self._armed:
            return False
        et = event.type()
        if et == QEvent.Type.MouseButtonPress and self._need_press:
            self._need_press = False
            return True
        if et == QEvent.Type.MouseButtonRelease and self._need_release:
            self._need_release = False
            QTimer.singleShot(0, self.disarm)
            return True
        return False
