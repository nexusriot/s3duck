#!/usr/bin/env python3
"""
Print the environment a CI leg is actually testing.

The versions matter more than usual here: the same code has behaved
differently on a PyQt6 wheel, a pinned older wheel and Debian's
python3-pyqt6, so a failing run should say up front which one it was.
"""
import _bootstrap    # noqa: F401  (project root on sys.path, before diagnostics)
import os
import sys

from PyQt6.QtCore import PYQT_VERSION_STR, QT_VERSION_STR
from PyQt6.QtWidgets import QApplication

import diagnostics

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    app = QApplication(sys.argv)                                 # noqa: F841
    print(f"python        : {sys.version.split()[0]}")
    print(f"PyQt6         : {PYQT_VERSION_STR}")
    print(f"Qt runtime    : {QT_VERSION_STR}")
    print(f"SVG readable  : {diagnostics.svg_supported()}")
    print(f"QtSvg module  : {diagnostics.QtSvg is not None}")
    print(f"QtPdf module  : {diagnostics.QtPdf is not None}")
    print(f"image formats : {', '.join(diagnostics.image_formats())}")
    icons = diagnostics.icon_status(ROOT)
    print(f"icons         : {icons}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
