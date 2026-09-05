import base64
import configparser
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from PyQt6.QtCore import (
    QEventLoop, QMetaObject, QObject, QThread, Qt, pyqtSignal, pyqtSlot,
)
try:
    from PyQt6 import sip
except ImportError:
    sip = None
try:
    from PyQt6.QtNetwork import QLocalServer, QLocalSocket
except ImportError:
    # Debian splits some Qt modules out; without this one the app still runs,
    # it just cannot hand a location to an already-running window.
    QLocalServer = None
    QLocalSocket = None
try:
    import keyring
except Exception:
    # Any failure here — not installed, or installed but unable to find a
    # backend — means the same thing to us: the OS store is not usable.
    keyring = None
from PyQt6.QtGui import QCursor, QIcon, QTransform
from PyQt6.QtWidgets import QApplication, QProgressDialog

PROFILE_BUNDLE_VERSION = 1
PROFILE_BUNDLE_ITERATIONS = 480000

# Where an expiry hides in ~/.aws/credentials. There is no standard key: the
# CLI's own SSO cache uses none of these, while aws-vault, awsume, saml2aws and
# gimme-aws-creds each picked a different one.
AWS_EXPIRY_KEYS = (
    "aws_session_expiration",
    "x_security_token_expires",
    "aws_credential_expiration",
    "aws_expiration",
    "expiration",
)

# Credentials with less than this left are reported as expiring soon, which is
# roughly the window in which starting a long transfer is a bad idea.
EXPIRY_WARN_SECONDS = 15 * 60


class CredentialProcessError(Exception):
    """Raised when a profile's credential_process cannot produce keys."""


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


# Where the credential key lives. "local" is the historical behaviour and
# stays the default: the key sits in the same QSettings file as the ciphertext,
# which protects against a casual reader and nothing else.
CREDENTIAL_STORE_LOCAL = "local"
CREDENTIAL_STORE_KEYRING = "keyring"
CREDENTIAL_STORE_PASSPHRASE = "passphrase"
CREDENTIAL_STORES = (
    CREDENTIAL_STORE_LOCAL,
    CREDENTIAL_STORE_KEYRING,
    CREDENTIAL_STORE_PASSPHRASE,
)

KEYRING_SERVICE = "s3duck"
KEYRING_ENTRY = "credential-key"

# The unlocked key for this process. The launcher unlocks once; the profile
# switcher and the main window read it from here rather than prompting again.
_SESSION_KEY = {"value": ""}


def session_key() -> str:
    return _SESSION_KEY["value"]


def set_session_key(value):
    _SESSION_KEY["value"] = str(value or "")


def keyring_available() -> bool:
    """Whether an OS secret store is present AND actually usable."""
    if keyring is None:
        return False
    try:
        backend = keyring.get_keyring()
    except Exception:
        return False
    # keyring always returns *a* backend; the fail/null ones raise on write,
    # which would lose the key rather than store it.
    name = type(backend).__name__.lower()
    return "fail" not in name and "null" not in name


def wrap_key(key: str, passphrase: str) -> str:
    """Seal the credential key under a passphrase, as a storable string."""
    if not passphrase:
        raise CredentialError("A passphrase is required.")
    salt = secrets.token_bytes(16)
    token = Fernet(_bundle_key(passphrase, salt)).encrypt(
        str(key or "").encode())
    return json.dumps({
        "salt": base64.b64encode(salt).decode(),
        "data": token.decode(),
    })


def unwrap_key(blob, passphrase: str) -> str:
    """Reverse wrap_key. Raises CredentialError on a wrong passphrase."""
    try:
        document = json.loads(
            blob.decode() if isinstance(blob, (bytes, bytearray)) else str(blob))
        salt = base64.b64decode(document["salt"])
        data = document["data"].encode()
    except Exception as exc:
        raise CredentialError(
            "The passphrase-protected key is corrupt.") from exc
    try:
        return Fernet(_bundle_key(passphrase, salt)).decrypt(data).decode()
    except InvalidToken as exc:
        raise CredentialError("Wrong passphrase.") from exc
    except Exception as exc:
        raise CredentialError(f"Could not unlock the key: {exc}") from exc


class CredentialStore:
    """
    Where the Fernet key protecting stored credentials is kept.

    The default keeps it beside the ciphertext, which is obfuscation rather
    than encryption; the other two modes hand it to the OS secret store or
    seal it under a passphrase the user types once per launch. Switching modes
    re-homes the same key, so the stored profiles stay readable.
    """

    def __init__(self, settings, ask_passphrase=None):
        self.settings = settings
        # Called as ask_passphrase(prompt) -> str or None. Supplied by the UI
        # so this class never depends on a particular dialog.
        self.ask_passphrase = ask_passphrase

    def _get(self, name, default=""):
        self.settings.beginGroup("common")
        value = self.settings.value(name, default)
        self.settings.endGroup()
        return value

    def _set(self, name, value):
        self.settings.beginGroup("common")
        if value is None:
            self.settings.remove(name)
        else:
            self.settings.setValue(name, value)
        self.settings.endGroup()

    def mode(self) -> str:
        stored = str(self._get("credential_store", CREDENTIAL_STORE_LOCAL) or "")
        return stored if stored in CREDENTIAL_STORES else CREDENTIAL_STORE_LOCAL

    def describe(self) -> str:
        return {
            CREDENTIAL_STORE_LOCAL:
                "In the settings file, next to the credentials",
            CREDENTIAL_STORE_KEYRING:
                "In the operating system's secret store",
            CREDENTIAL_STORE_PASSPHRASE:
                "Sealed under a passphrase, entered once per launch",
        }[self.mode()]

    def load_key(self, passphrase=None) -> str:
        """
        The credential key, unlocking it if the mode requires that.

        Returns "" when there is nothing stored yet (a first run), and raises
        CredentialError when there is something stored that cannot be read.
        """
        cached = session_key()
        if cached:
            return cached
        mode = self.mode()
        if mode == CREDENTIAL_STORE_KEYRING:
            if keyring is None:
                raise CredentialError(
                    "This installation is set to keep the credential key in "
                    "the OS secret store, but the 'keyring' package is not "
                    "installed.")
            try:
                key = keyring.get_password(KEYRING_SERVICE, KEYRING_ENTRY) or ""
            except Exception as exc:
                raise CredentialError(
                    f"The OS secret store could not be read: {exc}") from exc
        elif mode == CREDENTIAL_STORE_PASSPHRASE:
            blob = self._get("key_wrapped", "")
            if not blob:
                return ""
            if passphrase is None and self.ask_passphrase is not None:
                passphrase = self.ask_passphrase(
                    "Passphrase for the stored credentials:")
            if not passphrase:
                raise CredentialError("No passphrase was entered.")
            key = unwrap_key(blob, passphrase)
        else:
            key = str(self._get("key", "") or "")
        # Only the modes that cannot be re-read for free are cached: the local
        # key is in the settings file, and caching it process-wide would let
        # one settings scope answer for another.
        if key and mode != CREDENTIAL_STORE_LOCAL:
            set_session_key(key)
        return key

    def ensure_key(self) -> str:
        """The credential key, creating one on a first run."""
        key = self.load_key()
        if key:
            return key
        key = Crypto.generate_key()
        mode = self.mode()
        self._store_key(key, mode)
        if mode != CREDENTIAL_STORE_LOCAL:
            set_session_key(key)
        return key

    def _store_key(self, key, mode, passphrase=None):
        if mode == CREDENTIAL_STORE_KEYRING:
            if not keyring_available():
                raise CredentialError(
                    "No usable OS secret store was found.")
            keyring.set_password(KEYRING_SERVICE, KEYRING_ENTRY, key)
        elif mode == CREDENTIAL_STORE_PASSPHRASE:
            if not passphrase:
                raise CredentialError("A passphrase is required.")
            self._set("key_wrapped", wrap_key(key, passphrase))
        else:
            self._set("key", key)

    def _clear_key(self, mode):
        if mode == CREDENTIAL_STORE_KEYRING:
            if keyring is not None:
                try:
                    keyring.delete_password(KEYRING_SERVICE, KEYRING_ENTRY)
                except Exception:
                    # Nothing stored, or a store that refuses deletes. The key
                    # is already somewhere else by now, so this is cosmetic.
                    pass
        elif mode == CREDENTIAL_STORE_PASSPHRASE:
            self._set("key_wrapped", None)
        else:
            self._set("key", None)

    def set_mode(self, mode, passphrase=None):
        """
        Move the existing key to another home.

        The new home is written before the old one is cleared: a failure
        halfway through must never leave the key nowhere.
        """
        if mode not in CREDENTIAL_STORES:
            raise CredentialError(f"Unknown credential store: {mode}")
        previous = self.mode()
        key = self.load_key() or Crypto.generate_key()
        self._store_key(key, mode, passphrase=passphrase)
        self._set("credential_store", mode)
        if previous != mode:
            self._clear_key(previous)
        set_session_key("" if mode == CREDENTIAL_STORE_LOCAL else key)
        return mode


def resolve_credential_key(settings) -> str:
    """
    The credential key for this process.

    The settings file wins when it holds one, so a component reading a
    different settings scope is never handed another one's key; the unlocked
    session key is the fallback, which is what the keyring and passphrase
    modes leave behind (they store nothing in the settings file at all).
    """
    settings.beginGroup("common")
    key = settings.value("key", "")
    settings.endGroup()
    return str(key or "") or session_key()


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


def bundled_icon(path, probe_size: int = ICON_PROBE_SIZE):
    """
    A bundled icon in a format this Qt build can actually read.

    Debian/Mint's ``python3-pyqt6`` ships no QtSvg and its Qt has no SVG image
    plugin, so ``QIcon("x.svg")`` there is NOT null — it just paints nothing.
    Every .svg therefore has a same-stem .png twin, and the first candidate
    that renders visibly wins: SVG when it works (crisper at any size), PNG
    otherwise.
    """
    candidates = [path] if path else []
    stem, ext = os.path.splitext(path or "")
    if ext.lower() == ".svg":
        candidates.append(stem + ".png")
    for candidate in candidates:
        icon = QIcon(candidate)
        if icon_is_visible(icon, probe_size):
            return icon
    return QIcon()


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
        icon = bundled_icon(fallback_path, probe_size)
    _icon_cache[key] = icon
    return icon


# Failures that are the service asking for patience rather than saying no.
TRANSIENT_ERROR_CODES = frozenset({
    "InternalError", "InternalServerError", "ServiceUnavailable", "SlowDown",
    "RequestTimeout", "RequestTimeoutException", "RequestTimeTooSkewed",
    "ThrottlingException", "Throttling", "TooManyRequests",
    "OperationAborted", "PriorRequestNotComplete", "BandwidthLimitExceeded",
    "503 SlowDown",
})
TRANSIENT_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})


def is_transient_error(exc) -> bool:
    """
    Whether a failure is worth retrying on its own.

    botocore already retries inside one request; this is about the job above
    it, which it gave up on. Only the service's own "try again" answers count
    — an AccessDenied retried on a timer is just a slower AccessDenied.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "")
        if code in TRANSIENT_ERROR_CODES:
            return True
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        try:
            if int(status) in TRANSIENT_HTTP_STATUS:
                return True
        except (TypeError, ValueError):
            pass
        return False
    # Connection-level failures never reached the service at all.
    name = type(exc).__name__
    return name in ("EndpointConnectionError", "ConnectionClosedError",
                    "ConnectTimeoutError", "ReadTimeoutError",
                    "IncompleteReadError", "ConnectionError")


def describe_client_error(exc) -> str:
    """
    Everything a provider's support desk asks for about one failure.

    botocore carries the request id, the extended (host) id and the HTTP
    status in the exception's response, and none of it survives ``str(exc)``
    — which is all the log used to keep, so a report of "it failed" could
    never be traced to a request.
    """
    lines = [f"Error: {exc}", f"Type: {type(exc).__name__}"]
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error") or {}
        metadata = response.get("ResponseMetadata") or {}
        for label, value in (
            ("Code", error.get("Code")),
            ("Message", error.get("Message")),
            ("HTTP status", metadata.get("HTTPStatusCode")),
            ("Request id", metadata.get("RequestId")),
            ("Host id", metadata.get("HostId")),
            ("Retries", (metadata.get("RetryAttempts")
                         if metadata.get("RetryAttempts") else None)),
        ):
            if value not in (None, ""):
                lines.append(f"{label}: {value}")
        headers = metadata.get("HTTPHeaders") or {}
        for header in ("x-amz-request-id", "x-amz-id-2", "x-amz-bucket-region"):
            if headers.get(header):
                lines.append(f"{header}: {headers[header]}")
    operation = getattr(exc, "operation_name", "")
    if operation:
        lines.insert(1, f"Operation: {operation}")
    return "\n".join(lines)


def mirrored_icon(icon, probe_size: int = ICON_PROBE_SIZE):
    """
    A horizontally flipped copy of an icon.

    Forward is Back pointing the other way, and mirroring the bundled arrow
    keeps the pair consistent without a second asset — which also means no
    second PNG twin to keep in step for the no-SVG builds.
    """
    if icon is None:
        return QIcon()
    pixmap = icon.pixmap(probe_size, probe_size)
    if pixmap.isNull():
        return icon
    return QIcon(pixmap.transformed(QTransform().scale(-1, 1)))


def forward_icon(back, probe_size: int = ICON_PROBE_SIZE):
    """The theme's "go-next" when it paints something, else a mirrored Back."""
    themed = QIcon.fromTheme("go-next")
    if icon_is_visible(themed, probe_size):
        return themed
    return mirrored_icon(back, probe_size)


# One socket per user, so two logins on one machine do not talk to each other.
INSTANCE_SOCKET = "s3duck-ipc"


def instance_socket_name(base=INSTANCE_SOCKET) -> str:
    try:
        return f"{base}-{os.getuid()}"
    except AttributeError:      # Windows
        return f"{base}-{os.environ.get('USERNAME', 'user')}"


def send_to_running_instance(payload, name="", timeout_ms=400) -> bool:
    """
    Hand a location to an already-running window; True when it took it.

    Only used when the command line names a location — a bare launch still
    opens its own window, because two profiles side by side is a thing people
    legitimately want.
    """
    if QLocalSocket is None:
        return False
    socket = QLocalSocket()
    socket.connectToServer(name or instance_socket_name())
    if not socket.waitForConnected(timeout_ms):
        return False
    socket.write(str(payload or "").encode("utf-8"))
    socket.flush()
    socket.waitForBytesWritten(timeout_ms)
    socket.disconnectFromServer()
    return True


class InstanceServer(QObject):
    """Listens for locations handed over by a second launch."""

    received = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._server = None

    def start(self, name="") -> bool:
        if QLocalServer is None:
            return False
        name = name or instance_socket_name()
        self._server = QLocalServer(self)
        # A crash leaves the socket file behind; without this the second run
        # can never listen again.
        QLocalServer.removeServer(name)
        if not self._server.listen(name):
            self._server = None
            return False
        self._server.newConnection.connect(self._on_connection)
        return True

    def _on_connection(self):
        socket = self._server.nextPendingConnection()
        if socket is None:
            return
        if socket.waitForReadyRead(400):
            payload = bytes(socket.readAll()).decode("utf-8", "replace")
            if payload.strip():
                self.received.emit(payload.strip())
        socket.disconnectFromServer()

    def stop(self):
        if self._server is not None:
            self._server.close()
            self._server = None


class LogFile:
    """
    A single rotating log file for the session log.

    The log view is capped at a few thousand lines and dies with the window,
    which is fine until someone needs to say what happened an hour ago. Every
    write is best-effort: a full disk must not take the app down with it.
    """

    MAX_BYTES = 2 * 1024 * 1024

    def __init__(self, path="", max_bytes=MAX_BYTES):
        self.path = str(path or "")
        self.max_bytes = int(max_bytes)
        self.enabled = bool(self.path)
        self._lock = threading.Lock()

    @staticmethod
    def default_path() -> str:
        return os.path.join(
            os.path.expanduser("~"), ".config", "s3duck", "s3duck.log")

    def write(self, line):
        if not self.enabled or not self.path:
            return False
        try:
            with self._lock:
                self._rotate_if_needed()
                os.makedirs(os.path.dirname(self.path), exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as handle:
                    handle.write(str(line).rstrip("\n") + "\n")
            return True
        except OSError:
            # One failed write disables the file rather than repeating the
            # error for every subsequent line.
            self.enabled = False
            return False

    def _rotate_if_needed(self):
        try:
            if os.path.getsize(self.path) < self.max_bytes:
                return
        except OSError:
            return
        backup = self.path + ".1"
        try:
            if os.path.exists(backup):
                os.remove(backup)
            os.replace(self.path, backup)
        except OSError:
            pass

    def tail(self, lines=200) -> str:
        """The last *lines* of the log, or "" when there is no file."""
        try:
            with open(self.path, encoding="utf-8", errors="replace") as handle:
                return "".join(handle.readlines()[-int(lines):])
        except OSError:
            return ""


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
    reap_finished_workers()


_LIVE_WORKERS = {}


def _thread_finished(th):
    """True once ``th`` can no longer run its worker (finished or deleted)."""
    if sip is not None:
        try:
            if sip.isdeleted(th):
                return True
        except Exception:
            pass
    try:
        return th.isFinished()
    except RuntimeError:
        return True


class _WorkerReaper(QObject):
    """Releases dead workers one full trip through the GUI event queue later.

    The release must not be an immediate DECREF: destroying the worker severs
    its connections and frees the Python callables they hold, but a delivery
    for one of those callables may already sit in the GUI queue (the worker
    emitted just before finishing) — invoking it after the free is a segfault.
    ``bury`` therefore parks the last reference and posts a queued ``flush``;
    the queue is FIFO, so by the time ``flush`` drops the reference every
    delivery that was queued ahead of it has run, and a finished thread can
    queue nothing new. The old in-worker deleteLater got this ordering for
    free from the queue itself — this preserves it while keeping the actual
    destructor on the GUI thread.
    """

    def __init__(self):
        super().__init__()
        self._graveyard = []

    def bury(self, entry):
        self._graveyard.append(entry)
        QMetaObject.invokeMethod(self, "flush",
                                 Qt.ConnectionType.QueuedConnection)

    @pyqtSlot()
    def flush(self):
        self._graveyard.clear()


_REAPER = None


def _reaper():
    global _REAPER
    if _REAPER is None:
        _REAPER = _WorkerReaper()
    return _REAPER


def reap_finished_workers():
    """Queue the release of every pinned worker whose thread is done."""
    for key, entry in list(_LIVE_WORKERS.items()):
        if _thread_finished(entry[1]):
            del _LIVE_WORKERS[key]
            _reaper().bury(entry)


def release_worker_on_finish(thread, worker):
    """Hold ``worker`` at least until ``thread`` has finished, then release it
    from the GUI thread.

    Replaces the old teardown — a worker signal wired to the worker's own
    ``deleteLater`` — on objects moved to a QThread. That deleteLater runs the
    C++ destructor on the worker thread; tearing down connections takes one of
    Qt's pooled signal-slot mutexes and then the GIL (sip checks each
    disconnectNotify for a Python override). The GUI thread always holds the
    GIL, and any connect/disconnect/destroy it performs takes mutexes from that
    same pool, so unrelated objects can collide on one mutex — the threads then
    each hold what the other needs and the process deadlocks. Dropping the
    last reference here instead runs the destructor on the GUI thread, where
    the GIL and the mutex are taken by the same thread and no cycle can form.

    Release is a sweep, not a signal: the next ``release_worker_on_finish``
    or ``join_qthread`` call hands workers with a finished thread to the
    reaper, which frees them one event-queue trip later (see _WorkerReaper for
    why the delay is load-bearing). Connecting a Python callable to
    ``thread.finished`` was tried first and crashed — a dialog destroyed with
    that delivery still queued frees the callable while the call is in
    flight. ``isFinished`` (not ``isRunning``) keeps a freshly created,
    not-yet-started thread from having its worker swept out from under it.
    """
    reap_finished_workers()
    _LIVE_WORKERS[id(worker)] = (worker, thread)


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
    release_worker_on_finish(thread, worker)
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


def parse_expiry(value) -> float:
    """
    Read a credential expiry into epoch seconds; 0 when there is not one.

    Accepts what the various credential helpers actually write: RFC 3339 with
    a ``Z`` or an offset, a naive local timestamp, or a bare epoch. Anything
    unparseable is treated as absent — a profile must not become unusable
    because a helper invented a new format.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if value > 0 else 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        number = float(text)
    except ValueError:
        pass
    else:
        return number if number > 0 else 0.0
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return float(parsed.timestamp())


def format_duration(seconds) -> str:
    """A short human span: 2d, 3h 20m, 14m, 45s."""
    total = int(max(0, seconds))
    if total >= 86400:
        days, rest = divmod(total, 86400)
        hours = rest // 3600
        return f"{days}d {hours}h" if hours else f"{days}d"
    if total >= 3600:
        hours, rest = divmod(total, 3600)
        minutes = rest // 60
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    if total >= 60:
        return f"{total // 60}m"
    return f"{total}s"


def expiry_state(expires_at, now=None, warn_within=EXPIRY_WARN_SECONDS) -> tuple:
    """
    Classify a credential expiry as ``(state, label)``.

    States are "none" (nothing to say), "ok", "soon" and "expired". The label
    is what a badge or a status line shows; it is empty for "none" so a caller
    can render it unconditionally.
    """
    moment = parse_expiry(expires_at)
    if moment <= 0:
        return "none", ""
    remaining = moment - (time.time() if now is None else float(now))
    if remaining <= 0:
        return "expired", "expired"
    if remaining <= float(warn_within):
        return "soon", f"expires in {format_duration(remaining)}"
    return "ok", f"expires in {format_duration(remaining)}"


def run_credential_process(command, timeout=60) -> dict:
    """
    Mint credentials by running the profile's ``credential_process``.

    This is the AWS SDKs' own refresh mechanism, which is the only practical
    way to keep an SSO or assume-role session alive: the command prints a JSON
    document with AccessKeyId / SecretAccessKey / SessionToken / Expiration.

    The command is split with shlex and run WITHOUT a shell, so a stored
    profile cannot smuggle in a pipeline or a redirect.
    """
    text = str(command or "").strip()
    if not text:
        raise CredentialProcessError("No credential process is configured.")
    try:
        argv = shlex.split(text)
    except ValueError as exc:
        raise CredentialProcessError(f"Unparseable command: {exc}") from exc
    if not argv:
        raise CredentialProcessError("No credential process is configured.")
    try:
        completed = subprocess.run(
            argv, capture_output=True, timeout=timeout, check=False)
    except FileNotFoundError as exc:
        raise CredentialProcessError(f"{argv[0]}: not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise CredentialProcessError(
            f"{argv[0]}: timed out after {timeout}s") from exc
    except OSError as exc:
        raise CredentialProcessError(f"{argv[0]}: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or b"").decode(
            "utf-8", "replace").strip().splitlines()
        raise CredentialProcessError(
            f"{argv[0]} exited {completed.returncode}"
            + (f": {detail[-1]}" if detail else ""))
    try:
        payload = json.loads((completed.stdout or b"").decode("utf-8", "replace"))
    except ValueError as exc:
        raise CredentialProcessError(
            f"{argv[0]} did not print JSON credentials") from exc
    if not isinstance(payload, dict):
        raise CredentialProcessError(
            f"{argv[0]} did not print a JSON object")
    access = str(payload.get("AccessKeyId") or "").strip()
    secret = str(payload.get("SecretAccessKey") or "").strip()
    if not access or not secret:
        raise CredentialProcessError(
            f"{argv[0]} printed no AccessKeyId/SecretAccessKey")
    return {
        "access_key": access,
        "secret_key": secret,
        "session_token": str(payload.get("SessionToken") or "").strip(),
        "expires": str(payload.get("Expiration") or "").strip(),
    }


def load_aws_profiles(credentials_path=None, config_path=None) -> dict:
    """
    Parse the AWS shared credentials/config files into
    ``{profile_name: {access_key, secret_key, session_token, region,
    endpoint_url, expires, credential_process}}``.

    Both files are optional and parse errors are swallowed — this only ever
    pre-fills a form. ``~/.aws/config`` names profiles ``[profile foo]`` (except
    ``[default]``), and supplies region/endpoint for a credentials-file profile
    of the same name.

    A profile whose credentials come from a ``credential_process`` has no keys
    in the file at all, so it is kept even with an empty access key: the
    command is what makes it usable.
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

    def _expiry_of(parser, section):
        for key in AWS_EXPIRY_KEYS:
            value = (parser.get(section, key, fallback="") or "").strip()
            if value:
                return value
        return ""

    profiles = {}

    creds = _read(credentials_path)
    for section in creds.sections():
        access = creds.get(section, "aws_access_key_id", fallback="") or ""
        secret = creds.get(section, "aws_secret_access_key", fallback="") or ""
        process = (
            creds.get(section, "credential_process", fallback="") or "").strip()
        if not access and not secret and not process:
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
            "expires": _expiry_of(creds, section),
            "credential_process": process,
        }

    cfg = _read(config_path)
    for section in cfg.sections():
        name = section[len("profile "):] if section.startswith("profile ") else section
        entry = profiles.setdefault(name, {
            "access_key": "", "secret_key": "", "session_token": "",
            "region": "", "endpoint_url": "", "expires": "",
            "credential_process": "",
        })
        for key, field in (("region", "region"),
                           ("endpoint_url", "endpoint_url"),
                           ("credential_process", "credential_process")):
            value = (cfg.get(section, key, fallback="") or "").strip()
            if value and not entry.get(field):
                entry[field] = value
        if not entry.get("expires"):
            entry["expires"] = _expiry_of(cfg, section)

    # Drop config-only profiles that can produce neither keys nor a command.
    return {n: v for n, v in profiles.items()
            if v.get("access_key") or v.get("credential_process")}


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
