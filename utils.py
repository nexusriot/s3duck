import configparser
import os

from PyQt6.QtGui import QCursor
from PyQt6.QtWidgets import QApplication


def str_to_bool(s):
    return str(s).lower() == "true"


def scan_local_tree(root) -> dict:
    """
    Map every file under *root* to ``{relative_posix_path: (size, mtime)}``
    for sync comparison. Unreadable entries and symlinked directories are
    skipped rather than aborting the scan.
    """
    out = {}
    root = os.path.abspath(root or "")
    if not os.path.isdir(root):
        return out
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames
                       if not os.path.islink(os.path.join(dirpath, d))]
        for filename in filenames:
            full = os.path.join(dirpath, filename)
            try:
                stat = os.stat(full)
            except OSError:
                continue
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            out[rel] = (int(stat.st_size), float(stat.st_mtime))
    return out


def load_aws_profiles(credentials_path=None, config_path=None) -> dict:
    """
    Parse the AWS shared credentials/config files into
    ``{profile_name: {access_key, secret_key, session_token, region, endpoint_url}}``.

    Both files are optional and parse errors are swallowed — this only ever
    pre-fills a form. ``~/.aws/config`` names profiles ``[profile foo]`` (except
    ``[default]``), and supplies region/endpoint for a credentials-file profile
    of the same name.
    """
    home = os.path.expanduser("~")
    credentials_path = credentials_path or os.path.join(home, ".aws", "credentials")
    config_path = config_path or os.path.join(home, ".aws", "config")

    def _read(path):
        parser = configparser.RawConfigParser()
        try:
            parser.read(path)
        except Exception:
            return configparser.RawConfigParser()
        return parser

    profiles = {}

    creds = _read(credentials_path)
    for section in creds.sections():
        access = creds.get(section, "aws_access_key_id", fallback="") or ""
        secret = creds.get(section, "aws_secret_access_key", fallback="") or ""
        if not access and not secret:
            continue
        profiles[section] = {
            "access_key": access.strip(),
            "secret_key": secret.strip(),
            "session_token": (
                creds.get(section, "aws_session_token", fallback="") or ""
            ).strip(),
            "region": (creds.get(section, "region", fallback="") or "").strip(),
            "endpoint_url": (
                creds.get(section, "endpoint_url", fallback="") or ""
            ).strip(),
        }

    cfg = _read(config_path)
    for section in cfg.sections():
        name = section[len("profile "):] if section.startswith("profile ") else section
        entry = profiles.setdefault(name, {
            "access_key": "", "secret_key": "", "session_token": "",
            "region": "", "endpoint_url": "",
        })
        for key, field in (("region", "region"), ("endpoint_url", "endpoint_url")):
            value = (cfg.get(section, key, fallback="") or "").strip()
            if value and not entry.get(field):
                entry[field] = value

    # Drop config-only profiles that carry no usable credentials.
    return {n: v for n, v in profiles.items() if v.get("access_key")}


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
