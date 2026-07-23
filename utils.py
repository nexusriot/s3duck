from PyQt6.QtGui import QCursor
from PyQt6.QtWidgets import QApplication


def str_to_bool(s):
    if str(s).lower() == "true":
        return True
    elif str(s).lower() == "false":
        return False
    else:
        return False


def center_on_screen(widget):
    """Center a top-level widget on the *active* screen.

    Picks the screen under the mouse cursor (so it lands on the monitor the
    user is actually working on in a multi-monitor setup), falling back to the
    widget's current screen and then the primary screen. Uses availableGeometry
    so the window respects taskbars/docks, and frameGeometry so the window
    decorations are accounted for.
    """
    screen = QApplication.screenAt(QCursor.pos())
    if screen is None:
        screen = widget.screen() or QApplication.primaryScreen()
    if screen is None:
        return
    available = screen.availableGeometry()
    frame = widget.frameGeometry()
    frame.moveCenter(available.center())
    widget.move(frame.topLeft())
