import base64
import configparser
import json
import os
import re
import secrets
import shutil
import tempfile

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from PyQt6.QtCore import (
    QEventLoop, QObject, QThread, Qt, pyqtSignal, pyqtSlot,
)
from PyQt6.QtGui import QCursor, QIcon
from PyQt6.QtWidgets import QApplication, QProgressDialog

PROFILE_BUNDLE_VERSION = 1
PROFILE_BUNDLE_ITERATIONS = 480000


class BundleError(Exception):
    """Raised when a profile bundle cannot be read or decrypted."""


class CredentialError(Exception):
    """Stored credentials could not be read with the configured key."""


class Crypto:
    """
    Fernet wrapper for the credentials held in QSettings.

    Shared by the launcher and the runtime profile switcher, which used to
    carry separate copies. Failures surface as CredentialError because every
    caller runs inside a Qt slot, and an exception escaping a slot aborts the
    process in PyQt6 rather than showing a dialog.
    """

    def __init__(self, key):
        self.key = key
        self._fernet = None

    @property
    def fernet(self):
        if self._fernet is None:
            if not self.key:
                raise CredentialError(
                    "The encryption key is missing from settings "
                    "(common/key), so stored credentials cannot be read."
                )
            raw = self.key.encode() if isinstance(self.key, str) else self.key
            try:
                self._fernet = Fernet(raw)
            except Exception as exc:
                raise CredentialError(
                    f"The stored encryption key is not usable: {exc}"
                ) from exc
        return self._fernet

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode()

    def encrypt(self, value) -> bytes:
        return self.fernet.encrypt(str(value or "").encode())

    def decrypt(self, value) -> str:
        """
        Decrypt a required field. Raises CredentialError when it is absent or
        cannot be read — use decrypt_optional for fields that may be unset.
        """
        fernet = self.fernet
        if not value:
            raise CredentialError(
                "The stored value is empty, so there is nothing to decrypt."
            )
        # QSettings returns bytes or a QByteArray depending on the backend;
        # str() on a QByteArray yields its repr, not the token.
        if isinstance(value, (bytes, bytearray)):
            token = bytes(value)
        elif isinstance(value, str):
            token = value.encode()
        else:
            try:
                token = bytes(value)          # QByteArray and friends
            except Exception:
                token = str(value).encode()
        try:
            return fernet.decrypt(token).decode()
        except Exception as exc:
            raise CredentialError(
                "Could not decrypt the stored credentials with the current "
                "key — they were most likely saved with a different one."
            ) from exc


def require_crypto(key) -> Crypto:
    """A Crypto validated up front, so write paths fail before storing."""
    crypto = Crypto(key)
    crypto.fernet
    return crypto


def decrypt_optional(crypto, value) -> str:
    """Decrypt a possibly absent/legacy field without failing the whole load."""
    if not value:
        return ""
    try:
        return crypto.decrypt(value)
    except Exception:
        return ""


def _bundle_key(passphrase: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=PROFILE_BUNDLE_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive((passphrase or "").encode()))


def export_profile_bundle(profiles, passphrase: str) -> bytes:
    """
    Serialise profiles (with plaintext credentials) into an encrypted bundle.

    The credentials are protected with a passphrase-derived key rather than
    written in the clear, so the file can be moved between machines without
    also carrying this installation's Fernet key.
    """
    if not passphrase:
        raise BundleError("A passphrase is required to export profiles")
    salt = secrets.token_bytes(16)
    payload = json.dumps(list(profiles)).encode()
    token = Fernet(_bundle_key(passphrase, salt)).encrypt(payload)
    document = {
        "version": PROFILE_BUNDLE_VERSION,
        "salt": base64.b64encode(salt).decode(),
        "data": token.decode(),
    }
    return json.dumps(document, indent=2).encode()


def import_profile_bundle(blob, passphrase: str) -> list:
    """Reverse export_profile_bundle. Raises BundleError on any problem."""
    try:
        document = json.loads(blob.decode() if isinstance(blob, bytes) else blob)
    except Exception as exc:
        raise BundleError(f"Not a valid profile bundle: {exc}") from exc
    if not isinstance(document, dict) or "data" not in document or "salt" not in document:
        raise BundleError("Not a valid profile bundle")
    version = document.get("version")
    if version != PROFILE_BUNDLE_VERSION:
        raise BundleError(f"Unsupported bundle version: {version}")
    try:
        salt = base64.b64decode(document["salt"])
    except Exception as exc:
        raise BundleError("Bundle salt is corrupt") from exc
    try:
        payload = Fernet(_bundle_key(passphrase, salt)).decrypt(
            document["data"].encode())
    except InvalidToken as exc:
        raise BundleError("Wrong passphrase, or the bundle was modified") from exc
    except Exception as exc:
        raise BundleError(f"Could not decrypt bundle: {exc}") from exc
    try:
        profiles = json.loads(payload.decode())
    except Exception as exc:
        raise BundleError(f"Bundle contents are corrupt: {exc}") from exc
    if not isinstance(profiles, list):
        raise BundleError("Bundle does not contain a list of profiles")
    return profiles


# Preset accents offered per profile. Named rather than free-form so the
# colours stay distinguishable and consistent between the launcher and the
# main window.
PROFILE_ACCENTS = (
    ("None", ""),
    ("Red", "#c62828"),
    ("Amber", "#b26a00"),
    ("Green", "#2e7d32"),
    ("Blue", "#1565c0"),
    ("Purple", "#6a1b9a"),
    ("Grey", "#546e7a"),
)

_ACCENT_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")


def normalize_accent(value) -> str:
    """
    A profile accent as a lowercase hex string, or "" when unset/invalid.

    Settings are user-editable text, so anything that is not a colour must
    degrade to "no accent" rather than reaching QColor and painting nothing
    visible (or worse, an unstyled band that looks like a rendering bug).
    """
    text = str(value or "").strip()
    return text.lower() if _ACCENT_RE.match(text) else ""


ICON_PROBE_SIZE = 24
ICON_ALPHA_FLOOR = 8

_icon_cache = {}


def icon_is_visible(icon, probe_size: int = ICON_PROBE_SIZE) -> bool:
    """
    Whether an icon paints anything a user could see at *probe_size*.

    Neither ``isNull()`` nor a non-null pixmap is enough: a theme entry can
    resolve to a fully transparent placeholder, which passes both checks and
    still leaves an empty button. The only dependable test is to rasterise it
    and look for a pixel with real alpha. Scanning stops at the first visible
    pixel, so a working icon costs almost nothing.
    """
    if icon is None:
        return False
    # A null icon yields a null pixmap yields a null 0x0 image, so the scan
    # below already covers every empty case — one check instead of three.
    image = icon.pixmap(probe_size, probe_size).toImage()
    for y in range(image.height()):
        for x in range(image.width()):
            if image.pixelColor(x, y).alpha() > ICON_ALPHA_FLOOR:
                return True
    return False


def themed_icon(name, fallback_path="", probe_size: int = ICON_PROBE_SIZE):
    """
    A toolbar/menu icon that is never blank.

    ``QIcon.fromTheme(name, fallback)`` uses the fallback only when the theme
    has NO entry for *name*. A theme that registers the name but ships nothing
    visible at the size we paint — several names under Linux Mint's default
    theme — therefore yields an empty button instead of the bundled icon, so
    the themed icon is rendered and checked before it is trusted.

    Results are cached because context menus rebuild their icons on every
    right-click. A desktop icon-theme change therefore needs a restart to be
    picked up, which is what Qt effectively requires anyway.
    """
    key = (name, fallback_path, probe_size)
    if key in _icon_cache:
        return _icon_cache[key]
    icon = QIcon.fromTheme(name) if name else QIcon()
    if not icon_is_visible(icon, probe_size):
        icon = QIcon(fallback_path) if fallback_path else QIcon()
    _icon_cache[key] = icon
    return icon


TEMP_PREFIX = "s3duck_"


def pid_is_alive(pid) -> bool:
    """
    Whether *pid* is still running.

    Errs towards True: a temp directory is only ever deleted when its owner is
    known to be gone, so an unknown answer must never authorise a delete.
    """
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (ValueError, TypeError):
        return True
    except OSError:
        return True     # exists but not ours, or the platform won't say
    return True


class TempWorkspace:
    """
    Session-scoped scratch space for downloaded object payloads.

    Previews, "open with default app" and drag-out staging all write object
    contents to disk, and those files must outlive the operation — an external
    viewer or the drop target still holds them — so they cannot be deleted
    inline. They were never deleted at all, leaving decrypted payloads in the
    system temp directory indefinitely.

    Every directory is created under one per-process root named
    ``s3duck_<pid>_<random>``. ``cleanup()`` removes the whole root on exit,
    and ``sweep()`` reclaims roots left behind by a crashed run — identified
    by a PID that is no longer alive, so a second instance running right now
    is never touched.
    """

    def __init__(self, root=None, pid=None):
        self.root = root or tempfile.gettempdir()
        self.pid = os.getpid() if pid is None else int(pid)
        self._session = None

    @property
    def session_dir(self) -> str:
        """The per-process root, created on first use."""
        if self._session is None:
            os.makedirs(self.root, exist_ok=True)
            self._session = tempfile.mkdtemp(
                prefix=f"{TEMP_PREFIX}{self.pid}_", dir=self.root)
        return self._session

    def make(self, prefix="") -> str:
        """A fresh directory inside this session's root."""
        return tempfile.mkdtemp(prefix=prefix or "", dir=self.session_dir)

    def cleanup(self):
        """Remove everything this session created."""
        session, self._session = self._session, None
        if session:
            shutil.rmtree(session, ignore_errors=True)

    def owner_pid(self, name):
        """The PID encoded in a session directory name, or None."""
        if not name.startswith(TEMP_PREFIX):
            return None
        tail = name[len(TEMP_PREFIX):].split("_")[0]
        try:
            return int(tail)
        except ValueError:
            return None

    def sweep(self, is_alive=pid_is_alive) -> list:
        """Remove roots owned by processes that are gone. Returns their paths."""
        removed = []
        try:
            names = os.listdir(self.root)
        except OSError:
            return removed
        for name in names:
            pid = self.owner_pid(name)
            if pid is None or pid == self.pid or is_alive(pid):
                continue
            path = os.path.join(self.root, name)
            if not os.path.isdir(path):
                continue
            shutil.rmtree(path, ignore_errors=True)
            if not os.path.exists(path):
                removed.append(path)
        return removed


def join_qthread(th, timeout_ms: int = 2000):
    """Quit and join a worker QThread.

    Dialog worker threads are parented to the dialog, so one still running when
    the dialog is destroyed aborts the process ("QThread: Destroyed while
    thread is still running"). Callers join in the done handler (the work is
    over by then, so this returns immediately) and again on close, which covers
    a dialog dismissed mid-load.
    """
    if th is None:
        return
    try:
        if th.isRunning():
            th.quit()
            th.wait(timeout_ms)
    except RuntimeError:
        pass  # already deleted by Qt


class FuncWorker(QObject):
    """Run one function on a QThread and report its result or exception.

    The function receives this worker as its only argument so it can emit
    byte progress via ``worker.progress`` while it runs.
    """
    done = pyqtSignal(object, object)   # (result, exception)
    progress = pyqtSignal(int, int)     # (current_bytes, total_bytes)

    def __init__(self, fn):
        super().__init__()
        self._fn = fn

    @pyqtSlot()
    def run(self):
        try:
            res = self._fn(self)
            self.done.emit(res, None)
        except Exception as exc:
            self.done.emit(None, exc)


def run_with_progress(parent, title, fn, modality=None):
    """
    Run fn(worker) on a QThread while showing a modal busy dialog.
    Returns (result, exception); result is None if the user cancelled.
    """
    prog = QProgressDialog(title, "Cancel", 0, 0, parent)
    prog.setWindowTitle(title)
    prog.setWindowModality(modality or Qt.WindowModality.ApplicationModal)
    prog.setMinimumDuration(0)
    prog.setAutoClose(False)
    prog.setAutoReset(False)

    state = {"result": None, "exc": None, "done": False}
    thread = QThread(parent)
    worker = FuncWorker(fn)
    worker.moveToThread(thread)
    thread.started.connect(worker.run)

    def _on_done(result, exc):
        state.update(result=result, exc=exc, done=True)
        prog.reset()

    worker.done.connect(_on_done)
    worker.done.connect(thread.quit)
    worker.done.connect(worker.deleteLater)
    thread.finished.connect(thread.deleteLater)
    thread.start()

    # Deliberately not prog.exec(): a fast worker can finish (and call
    # reset()) before exec() is entered, and exec() would then block
    # forever with nothing left to close it. Pumping events with a bounded
    # wait cannot deadlock however the race falls out.
    prog.show()
    while not state["done"] and not prog.wasCanceled():
        QApplication.processEvents(
            QEventLoop.ProcessEventsFlag.WaitForMoreEvents, 50)
    prog.close()

    if not state["done"]:
        state["exc"] = None
        state["result"] = None
    join_qthread(thread)
    return state["result"], state["exc"]


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
