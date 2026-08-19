"""
Environment facts worth having when something looks wrong.

Written after a run of blank toolbar icons that took three rounds to explain:
the venv's PyQt6 wheel bundles QtSvg, the distribution package does not, and
nothing in the app said so. Everything here is collected as plain data so it
can be asserted in tests and pasted into a bug report.
"""
import ast
import os
import platform
import sys

import boto3
import botocore
import cryptography
from PyQt6.QtCore import PYQT_VERSION_STR, QT_VERSION_STR
from PyQt6.QtGui import QIcon, QImageReader

from utils import ICON_PROBE_SIZE, bundled_icon, icon_is_visible

try:
    from PyQt6 import QtSvg
except ImportError:
    QtSvg = None

try:
    from PyQt6 import QtPdf
except ImportError:
    QtPdf = None

ICON_SOURCES = ("main_window.py", "s3duck.py", "settings.py",
                "properties_window.py", "profile_switcher.py")


def image_formats() -> list:
    """Image formats this Qt build can actually read."""
    return sorted(bytes(fmt).decode()
                  for fmt in QImageReader.supportedImageFormats())


def svg_supported(root=None) -> bool:
    """
    Whether a bundled SVG icon actually renders here.

    Measured by drawing one, not by looking for "svg" in the image formats:
    QIcon reaches SVG through the icon-engine plugin, so a build can render
    SVG icons perfectly while "svg" is absent from the format list. Reporting
    the format list as "SVG readable" said "no" on a machine where every SVG
    icon drew fine — the opposite of what this line is for.
    """
    base = root or os.path.dirname(os.path.abspath(__file__))
    sample = os.path.join(base, "icons", "pie_24px.svg")
    if os.path.exists(sample):
        return icon_is_visible(QIcon(sample), ICON_PROBE_SIZE)
    return "svg" in image_formats()


def icon_calls(source_text) -> list:
    """
    ``(theme_name, bundled_filename)`` for every ``themed_icon(...)`` call.

    Parsed rather than pattern-matched: the calls wrap across lines, and a
    line-oriented regex reports a present fallback as missing.
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
            for sub in ast.walk(node.args[1]):
                if (isinstance(sub, ast.Constant)
                        and isinstance(sub.value, str)
                        and sub.value.endswith((".svg", ".png", ".ico"))):
                    bundled = sub.value
        found.append((name, bundled))
    return found


def icon_status(root) -> dict:
    """
    How every icon the app asks for resolves here.

    ``blank`` is the number that would render as an empty button — the symptom
    that started all this.
    """
    pairs = set()
    for source in ICON_SOURCES:
        path = os.path.join(root, source)
        if not os.path.exists(path):
            continue
        with open(path) as handle:
            pairs.update(icon_calls(handle.read()))

    themed = bundled = blank = hollow = 0
    for name, filename in sorted(pairs):
        from_theme = icon_is_visible(QIcon.fromTheme(name), ICON_PROBE_SIZE)
        if QIcon.hasThemeIcon(name) and not from_theme:
            hollow += 1
        if from_theme:
            themed += 1
            continue
        path = os.path.join(root, "icons", filename) if filename else ""
        if path and icon_is_visible(bundled_icon(path), ICON_PROBE_SIZE):
            bundled += 1
        else:
            blank += 1
    return {"total": len(pairs), "themed": themed, "bundled": bundled,
            "blank": blank, "hollow": hollow}


def collect(root, *, version="", model=None, profile_name="") -> list:
    """The whole report as ``[(section, [(label, value), ...]), ...]``."""
    icons = icon_status(root)
    sections = [
        ("Application", [
            ("Version", version or "unknown"),
            ("Python", sys.version.split()[0]),
            ("Platform", f"{platform.system()} {platform.release()}"),
            ("Install path", root),
        ]),
        ("Qt", [
            ("PyQt6", PYQT_VERSION_STR),
            ("Qt runtime", QT_VERSION_STR),
            ("SVG icons render", _yes_no(svg_supported(root))),
            ("QtSvg module", _yes_no(QtSvg is not None)),
            ("QtPdf module (PDF preview)", _yes_no(QtPdf is not None)),
            ("Icon theme", QIcon.themeName() or "(none)"),
            ("Image formats", ", ".join(image_formats())),
        ]),
        ("Icons", [
            ("Requested", str(icons["total"])),
            ("From the desktop theme", str(icons["themed"])),
            ("From bundled art", str(icons["bundled"])),
            ("Theme entries that draw nothing", str(icons["hollow"])),
            ("Would render blank", str(icons["blank"])),
        ]),
        ("Libraries", [
            ("boto3", getattr(boto3, "__version__", "?")),
            ("botocore", getattr(botocore, "__version__", "?")),
            ("cryptography", getattr(cryptography, "__version__", "?")),
        ]),
    ]
    if model is not None:
        sections.append(("Profile", [
            ("Name", profile_name or "(unnamed)"),
            ("Endpoint", str(getattr(model, "profile_endpoint_url", "") or
                             getattr(model, "endpoint_url", "") or "(default)")),
            ("Region", str(getattr(model, "region_name", "") or "(unset)")),
            ("Bucket", str(getattr(model, "bucket", "") or "(bucket list)")),
            ("Read-only", _yes_no(getattr(model, "read_only", False))),
            ("TLS verification", _yes_no(
                not getattr(model, "no_ssl_check", False))),
            ("Path-style addressing", _yes_no(getattr(model, "use_path", False))),
        ]))
        sections.append(("Transfers", [
            ("Files at once", str(getattr(model, "parallel_files", "?"))),
            ("Connections per file",
             str(getattr(model, "transfer_concurrency", "?"))),
            ("Multipart part size",
             f"{getattr(model, 'multipart_chunksize_mb', '?')} MiB"),
            ("Multipart above",
             f"{getattr(model, 'multipart_threshold_mb', '?')} MiB"),
            ("Resumable uploads",
             _yes_no(getattr(model, "resumable_uploads", False))),
            ("Verify downloads",
             _yes_no(getattr(model, "verify_downloads", False))),
            ("Upload checksum",
             str(getattr(model, "checksum_algorithm", "") or "none")),
            ("Bandwidth limit",
             "unlimited" if getattr(model, "rate_limiter", None) is None
             else f"{int(model.rate_limiter.rate_bps) // 1024} KB/s"),
            ("Resume records",
             str(getattr(model, "upload_state_dir", "?"))),
        ]))
    return sections


def _yes_no(value) -> str:
    return "yes" if value else "no"


def format_report(sections) -> str:
    """The report as plain text, for pasting into a bug report."""
    lines = []
    for title, rows in sections:
        lines.append(f"== {title} ==")
        width = max((len(label) for label, _v in rows), default=0)
        for label, value in rows:
            lines.append(f"{label.ljust(width)} : {value}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"
