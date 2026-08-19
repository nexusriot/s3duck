#!/usr/bin/env python3
"""
Confirm this Qt really cannot draw a bundled SVG icon.

Run in its own process after ``ci_strip_svg.py``: Qt libraries loaded before
the deletion would keep working and the check would pass for the wrong reason.

The test is a rendered pixmap, not the image-format list — QIcon reaches SVG
through the icon-engine plugin, so an icon can draw while "svg" is absent from
``supportedImageFormats()``.
"""
import _bootstrap    # noqa: F401  (project root on sys.path, before diagnostics)
import os
import sys

from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QApplication

import diagnostics

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    app = QApplication(sys.argv)                                  # noqa: F841
    print(f"image formats: {', '.join(diagnostics.image_formats())}")

    sample = os.path.join(ROOT, "icons", "pie_24px.svg")
    if not os.path.exists(sample):
        print(f"::error::{sample} is missing, cannot verify")
        return 1
    if not QIcon(sample).pixmap(24, 24).toImage().isNull():
        print("::error::a bundled SVG still renders, so this leg proves "
              "nothing about the packaged Qt")
        return 1
    if diagnostics.svg_supported(ROOT):
        print("::error::diagnostics still reports SVG support")
        return 1
    print("SVG icons unavailable, as on Debian/Mint's python3-pyqt6")
    return 0


if __name__ == "__main__":
    sys.exit(main())
