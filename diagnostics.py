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

from model import CHECKSUM_ALGORITHMS, installed_sdk_accepts
from utils import ICON_PROBE_SIZE, bundled_icon, icon_is_visible

try:
    from PyQt6 import QtSvg
except ImportError:
    QtSvg = None

try:
    import awscrt
    from awscrt import checksums as _crt_checksums
except ImportError:
    awscrt = None
    _crt_checksums = None

try:
    from PyQt6 import QtPdf
except ImportError:
    QtPdf = None

# Directories that are not the application, for the source walk below.
NON_APP_DIRS = {".venv", "__pycache__", ".git", "build", "dist", "tests",
                "tools", ".idea", ".github", "icons", "resources", "DEBIAN"}


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
    ``(theme_name, bundled_filename)`` for every icon the app asks for.

    Parsed rather than pattern-matched: the calls wrap across lines, and a
    line-oriented regex reports a present fallback as missing.

    Two shapes count. ``themed_icon(name, path)`` is the direct one. The
    context menu instead passes the pair to its builder, which makes the
    ``themed_icon`` call itself once with variables -- so matching only the
    direct shape would silently drop around thirty menu icons from a report
    whose entire job is to say how every icon resolves.
    """
    found = []
    for node in ast.walk(ast.parse(source_text)):
        if not isinstance(node, ast.Call):
            continue
        direct = getattr(node.func, "id", "") == "themed_icon"
        viabuilder = (isinstance(node.func, ast.Attribute)
                      and node.func.attr == "add")
        if not (direct or viabuilder) or not node.args:
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
        # A bare `.add("something", ...)` that names no icon file is some
        # other add() entirely.
        if viabuilder and not bundled:
            continue
        found.append((name, bundled))
    return found


def icon_status(root) -> dict:
    """
    How every icon the app asks for resolves here.

    ``blank`` is the number that would render as an empty button — the symptom
    that started all this.
    """
    pairs = set()
    # Walked rather than listed: the sources moved into packages, and a
    # hardcoded tuple of filenames would quietly report on a handful of them
    # while saying nothing about the rest.
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames
                       if d not in NON_APP_DIRS and not d.startswith(".")]
        for name in filenames:
            if not name.endswith(".py"):
                continue
            with open(os.path.join(dirpath, name), encoding="utf-8") as handle:
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
            ("awscrt", awscrt_version()),
            # Which upload checksums this install can both send and verify
            # locally; the list is built from that, not from what S3 accepts.
            ("Checksums available", ", ".join(CHECKSUM_ALGORITHMS)),
            # A distribution's python3-botocore often predates these, and the
            # app then quietly drops the parameter rather than failing the
            # upload — which is exactly the question someone reads this
            # report to answer.
            ("Conditional writes (If-None-Match)",
             _yes_no(installed_sdk_accepts("PutObject", "IfNoneMatch"))),
            ("Whole-object checksums (ChecksumType)",
             _yes_no(installed_sdk_accepts("CreateMultipartUpload",
                                           "ChecksumType"))),
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
            ("Requester pays", _yes_no(
                getattr(model, "requester_pays", False))),
            ("Public base URL",
             str(getattr(model, "public_base_url", "") or "(endpoint)")),
            ("TLS verification", _yes_no(
                not getattr(model, "no_ssl_check", False))),
            ("CA bundle",
             str(getattr(model, "ca_bundle", "") or "(system trust store)")),
            ("HTTP proxy",
             redact_url_credentials(getattr(model, "proxy_url", "")) or
             "(direct)"),
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
            ("Detect Content-Type",
             _yes_no(getattr(model, "detect_content_type", False))),
            ("Content-Type overrides",
             str(len(getattr(model, "content_type_overrides", {}) or {}))),
            ("Bandwidth limit",
             "unlimited" if getattr(model, "rate_limiter", None) is None
             else f"{int(model.rate_limiter.rate_bps) // 1024} KB/s"),
            ("Resume records",
             str(getattr(model, "upload_state_dir", "?"))),
        ]))
    return sections


def awscrt_version() -> str:
    """The awscrt build in use, or why its checksums are unavailable.

    Without it CRC32C and CRC64NVME are simply not offered: botocore needs
    it to send them and this app needs it to verify them, so the two halves
    are missing together rather than one outliving the other.
    """
    if _crt_checksums is None:
        return "not installed (no CRC32C / CRC64NVME)"
    return getattr(awscrt, "__version__", "installed")


def _yes_no(value) -> str:
    return "yes" if value else "no"


def redact_url_credentials(value) -> str:
    """
    A URL with any inline ``user:password@`` replaced by ``***``.

    This report exists to be pasted into a bug report, and a corporate proxy
    is routinely configured as http://user:password@proxy.corp:3128 — which
    would put the password in every issue the user ever files.

    The whole userinfo goes, not just the password: a login name is no more
    pastable than the secret next to it, and dropping both keeps the malformed
    cases (no host, no scheme, an "@" and nothing else) from leaking a half.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    scheme, sep, rest = text.partition("://")
    if not sep:
        scheme, rest = "", text
    authority, tail = _split_authority(rest)
    if "@" not in authority and "@" in tail and not _plausible_host(authority):
        # An unencoded "/", "?" or "#" in the password ended the authority
        # early: "alice:p/ss@proxy" reads as host "alice", port "p". No real
        # host:port looks like that, so the userinfo runs to the last "@".
        userinfo, _at, after = rest.rpartition("@")
        host, tail = _split_authority(after)
        authority = f"{userinfo}@{host}"
    if "@" not in authority:
        return text
    # Last "@" wins: a password may contain one, a host may not. Whatever is
    # left is the host — brackets and port included, so IPv6 survives intact.
    host = authority.rpartition("@")[2]
    return f"{scheme}{sep}***@{host}{tail}"


def _split_authority(rest):
    """(authority, tail): credentials can only live in the authority, which
    ends at the first "/", "?" or "#". A path or query may hold an "@"."""
    end = len(rest)
    for mark in "/?#":
        found = rest.find(mark)
        if found != -1:
            end = min(end, found)
    return rest[:end], rest[end:]


def _plausible_host(authority) -> bool:
    """False for a host:port whose port is empty or not a number."""
    if authority.startswith("["):
        authority = authority.partition("]")[2]
    if ":" not in authority:
        return True
    return authority.rpartition(":")[2].isdigit()


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
