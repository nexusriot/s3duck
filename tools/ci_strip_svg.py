#!/usr/bin/env python3
"""
Remove PyQt6's SVG support, reproducing Debian/Mint's python3-pyqt6.

That package ships no QtSvg and its Qt has no SVG support, so
``QIcon("x.svg")`` is not null — it simply paints nothing, which is how a
toolbar full of blank buttons happened.

Deleting only; ``ci_check_no_svg.py`` verifies the result in a fresh process,
because Qt libraries already loaded into this one would make the check lie.
"""
import pathlib
import sys

import PyQt6

PATTERNS = (
    # The icon engine is what QIcon("x.svg") actually goes through; removing
    # the image-format plugin alone left every SVG icon rendering, which made
    # an earlier version of this script prove nothing.
    "Qt6/plugins/iconengines/libqsvgicon*.so",
    "Qt6/plugins/imageformats/libqsvg*.so",
    "Qt6/lib/libQt6Svg*.so*",
    "QtSvg.abi3.so",
    "QtSvgWidgets.abi3.so",
)


def main() -> int:
    root = pathlib.Path(PyQt6.__file__).resolve().parent
    removed = []
    for pattern in PATTERNS:
        for path in root.glob(pattern):
            try:
                path.unlink()
            except OSError as exc:
                print(f"could not remove {path}: {exc}")
                continue
            removed.append(path.name)
    print(f"removed from {root}: {removed or 'nothing'}")
    if not removed:
        print("::error::nothing was removed, so this leg proves nothing")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
