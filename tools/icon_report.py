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
import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt6.QtGui import QIcon                                    # noqa: E402
from PyQt6.QtWidgets import QApplication                         # noqa: E402

from utils import (                                              # noqa: E402
    ICON_PROBE_SIZE, icon_is_visible, themed_icon,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES = ("main_window.py", "s3duck.py", "settings.py",
           "properties_window.py", "profile_switcher.py")


def icon_calls(source_text):
    """(theme_name, bundled_filename) for every themed_icon(...) call.

    Parsed rather than pattern-matched: the calls wrap across lines, and a
    line-oriented regex silently reports a present fallback as missing.
    """
    found = []
    for node in ast.walk(ast.parse(source_text)):
        if not (isinstance(node, ast.Call)
                and getattr(node.func, "id", "") == "themed_icon"
                and node.args):
            continue
        name = getattr(node.args[0], "value", None)
        if not isinstance(name, str):
            continue
        bundled = ""
        if len(node.args) > 1:
            strings = [n.value for n in ast.walk(node.args[1])
                       if isinstance(n, ast.Constant) and isinstance(n.value, str)]
            for text in strings:
                if text.endswith((".svg", ".png", ".ico")):
                    bundled = text
        found.append((name, bundled))
    return found


def main() -> int:
    app = QApplication(sys.argv)                                 # noqa: F841
    print(f"Qt icon theme : {QIcon.themeName() or '(none)'}")
    print(f"search paths  : {QIcon.themeSearchPaths()}")
    print(f"probe size    : {ICON_PROBE_SIZE}px\n")

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
            used = f"bundled ({bundled})"
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
