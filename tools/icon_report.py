#!/usr/bin/env python3
"""
Report how this desktop's icon theme resolves every icon S3 Duck asks for.

Run it on the machine where icons look wrong:

    ./.venv/bin/python tools/icon_report.py

For each name it shows whether the theme claims it, whether the themed icon
actually draws, and which source S3 Duck ends up using. A row marked
"theme claims it but draws nothing" is the case that used to leave a blank
toolbar button.
"""
import _bootstrap    # noqa: F401  (project root on sys.path, before utils)
import os
import sys

from PyQt6.QtGui import QIcon, QImageReader
from PyQt6.QtWidgets import QApplication

from diagnostics import icon_calls
from utils import (
    ICON_PROBE_SIZE, bundled_icon, icon_is_visible, themed_icon,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES = ("main_window.py", "s3duck.py", "settings.py",
           "properties_window.py", "profile_switcher.py")


def main() -> int:
    app = QApplication(sys.argv)                                 # noqa: F841
    print(f"Qt icon theme : {QIcon.themeName() or '(none)'}")
    print(f"search paths  : {QIcon.themeSearchPaths()}")
    formats = sorted(bytes(f).decode()
                     for f in QImageReader.supportedImageFormats())
    svg_ok = "svg" in formats
    print(f"probe size    : {ICON_PROBE_SIZE}px")
    print(f"SVG readable  : {svg_ok}"
          f"{'' if svg_ok else '   <- no SVG plugin; PNG twins are used'}\n")

    # Distinct (name, fallback) pairs: the same theme name is used at more
    # than one site with different bundled art, and collapsing by name alone
    # would report whichever site happened to be parsed last.
    seen = set()
    for source in SOURCES:
        path = os.path.join(ROOT, source)
        if not os.path.exists(path):
            continue
        with open(path) as handle:
            text = handle.read()
        seen.update(icon_calls(text))

    print(f"{'theme name':32} {'claimed':8} {'visible':8} used")
    blanks = hollow = 0
    for name, bundled in sorted(seen):
        claimed = QIcon.hasThemeIcon(name)
        themed = QIcon.fromTheme(name)
        draws = icon_is_visible(themed, ICON_PROBE_SIZE)
        resolved = themed_icon(
            name, os.path.join(ROOT, "icons", bundled) if bundled else "")
        if draws:
            used = "theme"
        elif bundled and icon_is_visible(resolved, ICON_PROBE_SIZE):
            # Say which FORMAT actually rendered, not just which file was
            # declared: an .svg silently falls through to its .png twin.
            svg_path = os.path.join(ROOT, "icons", bundled)
            direct = QIcon(svg_path)
            fmt = "svg" if icon_is_visible(direct, ICON_PROBE_SIZE) else "png twin"
            used = f"bundled {fmt} ({bundled})"
        else:
            used = "NOTHING — blank"
            blanks += 1
        if claimed and not draws:
            hollow += 1
            used += "   <- theme claims it but paints nothing visible"
        print(f"{name:32} {str(claimed):8} {str(draws):8} {used}")

    print(f"\n{len(seen)} names, {hollow} hollow theme entries "
          f"(these were the blank buttons), {blanks} would still render blank")
    return 1 if blanks else 0


if __name__ == "__main__":
    sys.exit(main())
