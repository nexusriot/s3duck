#!/usr/bin/python

import sys
import os
import pathlib
import urllib3
from copy import deepcopy
from PyQt6.QtGui import QIcon, QColor, QFont, QFontMetrics, QAction
from PyQt6 import QtCore
from PyQt6.QtCore import *
from PyQt6.QtWidgets import (
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QHBoxLayout,
    QApplication,
    QVBoxLayout,
    QMessageBox,
    QDialog,
    QFileDialog,
    QInputDialog,
    QLineEdit,
    QMenu,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)

from model import Model as DataModel
from settings import SettingsWindow
from main_window import MainWindow
from utils import (
    str_to_bool, center_on_screen, export_profile_bundle,
    import_profile_bundle, BundleError, Crypto, CredentialError,
    require_crypto, decrypt_optional, run_with_progress,
    normalize_accent, themed_icon, expiry_state, load_aws_profiles,
    run_credential_process, CredentialStore, CREDENTIAL_STORE_LOCAL,
    CREDENTIAL_STORE_KEYRING, CREDENTIAL_STORE_PASSPHRASE, keyring_available,
    InstanceServer, send_to_running_instance,
)
from theme import apply_theme

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_profile_secrets(key, item) -> tuple:
    """
    Decrypt a profile's stored secrets as (access_key, secret_key, token).

    Raises CredentialError — never the underlying AttributeError/ValueError/
    InvalidToken, which would abort the process from a Qt slot.
    """
    crypto = Crypto(key)
    return (
        crypto.decrypt(item.enc_access_key),
        crypto.decrypt(item.enc_secret_key),
        decrypt_optional(crypto, item.enc_session_token),
    )


def selected_row_index(row, item_count) -> int:
    """
    Validate a Qt row against the backing list.

    QModelIndex.row() is -1 when nothing is selected, and a bare items[row]
    would then silently act on the LAST profile. Returns -1 when there is no
    usable selection.
    """
    if row is None or row < 0 or row >= int(item_count or 0):
        return -1
    return int(row)


READ_ONLY_BADGE = "read-only"
INSECURE_BADGE = "TLS unverified"
EXPIRED_BADGE = "expired"

# Badges are colour-coded by what they mean for the user: amber for a mode
# worth knowing about, red for an actual exposure. Fixed colours rather than
# palette roles, because both must stay legible under every theme.
BADGE_COLORS = {
    READ_ONLY_BADGE: QColor("#b26a00"),
    INSECURE_BADGE: QColor("#c62828"),
    EXPIRED_BADGE: QColor("#c62828"),
}


def expiry_badge(item, now=None) -> str:
    """
    The credential-lifetime badge for a profile row, or "" when there is none.

    Temporary keys used to look exactly like permanent ones, so a lapsed
    session showed up as an unexplained authentication failure on connect.
    """
    state, label = expiry_state(getattr(item, "session_expires", ""), now=now)
    if state == "expired":
        return EXPIRED_BADGE
    if state == "soon":
        return label
    return ""


def preselect_row(items, last_name) -> int:
    """
    Which profile row to select on startup.

    Falls back to the first row when the remembered name is gone (deleted or
    renamed), so a stale setting can never leave the list with no selection.
    """
    if not items:
        return -1
    wanted = str(last_name or "").strip()
    for index, item in enumerate(items):
        if str(getattr(item, "name", "") or "").strip() == wanted and wanted:
            return index
    return 0


def profile_summary(item) -> str:
    """
    Second line of a profile row: where this profile points.

    The list used to show only the name, so two profiles differing solely by
    endpoint or region were indistinguishable.
    """
    parts = [str(item.url or "").strip() or "(no endpoint)"]
    for value in (item.region, item.bucket_name):
        text = str(value or "").strip()
        if text:
            parts.append(text)
    return " · ".join(parts)


def profile_badges(item) -> list:
    """
    Safety flags, shown beside the name rather than inside the summary.

    read-only decides whether this window can destroy data and TLS-unverified
    means the connection is interceptable. Appended to the dim endpoint line
    they read as more metadata; the point of a badge is that it does not.
    """
    badges = []
    if str_to_bool(item.read_only):
        badges.append(READ_ONLY_BADGE)
    if str_to_bool(item.no_ssl_check):
        badges.append(INSECURE_BADGE)
    expiry = expiry_badge(item)
    if expiry:
        badges.append(expiry)
    return badges


def badge_color(name, palette, selected: bool) -> QColor:
    """A badge keeps its own colour except on a selected row, where it would
    sit on the highlight brush and lose contrast."""
    if selected:
        return QColor(palette.highlightedText().color())
    if name in BADGE_COLORS:
        return QColor(BADGE_COLORS[name])
    # "expires in 12m" is generated, not one of the fixed badges, and means
    # the same thing as the amber ones: worth knowing, not yet broken.
    if name.startswith("expires in "):
        return QColor("#b26a00")
    return QColor(palette.text().color())


def _summary_font(base: QFont) -> QFont:
    """The subtitle font: a notch smaller than the row's, with a floor so it
    stays legible when the user runs a small UI font."""
    font = QFont(base)
    size = base.pointSizeF()
    if size > 0:
        font.setPointSizeF(max(size - 1.0, 7.0))
    return font


def row_text_color(palette, selected: bool) -> QColor:
    """Pen for a profile row. A selected row is painted on the highlight
    brush, where the ordinary text colour can be unreadable."""
    brush = palette.highlightedText() if selected else palette.text()
    return QColor(brush.color())


class ProfileRowDelegate(QStyledItemDelegate):
    """
    Draws a profile row as a name plus safety badges above a dimmed summary.

    A plain two-line item renders both lines identically, which reads as two
    profiles rather than one with a subtitle. Endpoints are elided in the
    middle so the scheme and the distinguishing host tail both survive; the
    name is elided only after the badges have been given their room, because
    a truncated badge would be worse than a truncated name.
    """

    BADGE_GAP = 6
    SWATCH_WIDTH = 4

    def _parts(self, index):
        text = index.data(Qt.ItemDataRole.DisplayRole) or ""
        name, _, summary = str(text).partition("\n")
        return name, summary

    def _badges(self, index):
        return list(index.data(Qt.ItemDataRole.UserRole) or [])

    def paint(self, painter, option, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        name, summary = self._parts(index)
        badges = self._badges(index)
        opt.text = ""
        style = opt.widget.style() if opt.widget else QApplication.style()
        style.drawControl(
            QStyle.ControlElement.CE_ItemViewItem, opt, painter, opt.widget)

        selected = bool(opt.state & QStyle.StateFlag.State_Selected)
        colour = row_text_color(opt.palette, selected)
        rect = opt.rect.adjusted(6, 3, -6, -3)
        small = _summary_font(opt.font)

        # The profile's accent, same colour the open window is banded with.
        accent = normalize_accent(index.data(Qt.ItemDataRole.UserRole + 1))
        if accent:
            swatch = QRect(rect.left(), rect.top(),
                           self.SWATCH_WIDTH, rect.height())
            painter.fillRect(swatch, QColor(accent))
            rect = rect.adjusted(self.SWATCH_WIDTH + 5, 0, 0, 0)

        painter.save()
        painter.setPen(colour)
        painter.setFont(opt.font)
        top = QFontMetrics(opt.font)

        # Reserve the badges' width before eliding the name, so a long name
        # cannot push a safety flag off the row.
        badge_metrics = QFontMetrics(small)
        labels = [f"[{text}]" for text in badges]
        reserved = sum(
            badge_metrics.horizontalAdvance(label) + self.BADGE_GAP
            for label in labels)
        name_width = max(rect.width() - reserved, 0)
        shown = top.elidedText(name, Qt.TextElideMode.ElideRight, name_width)
        painter.drawText(rect.left(), rect.top() + top.ascent(), shown)

        x = rect.left() + top.horizontalAdvance(shown) + self.BADGE_GAP
        painter.setFont(small)
        for text, label in zip(badges, labels):
            painter.setPen(badge_color(text, opt.palette, selected))
            painter.drawText(x, rect.top() + top.ascent(), label)
            x += badge_metrics.horizontalAdvance(label) + self.BADGE_GAP

        if summary:
            faded = QColor(colour)
            faded.setAlphaF(0.7)
            painter.setPen(faded)
            painter.drawText(
                rect.left(), rect.top() + top.height() + badge_metrics.ascent(),
                badge_metrics.elidedText(
                    summary, Qt.TextElideMode.ElideMiddle, rect.width()))
        painter.restore()

    def sizeHint(self, option, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        _, summary = self._parts(index)
        height = QFontMetrics(opt.font).height() + 6
        if summary:
            height += QFontMetrics(_summary_font(opt.font)).height()
        return QSize(opt.rect.width(), height)


class SettingsItem:
    def __init__(
        self,
        name,
        url,
        region,
        bucket_name,
        enc_access_key,
        enc_secret_key,
        no_ssl_check,
        use_path,
        enc_session_token="",
        read_only="false",
        color="",
        session_expires="",
        aws_profile="",
        credential_process="",
        public_base_url="",
        requester_pays="false",
        proxy_url="",
        ca_bundle="",
    ):
        self.name = name
        self.url = url
        self.region = region
        self.bucket_name = bucket_name
        self.enc_access_key = enc_access_key
        self.enc_secret_key = enc_secret_key
        self.no_ssl_check = no_ssl_check
        self.use_path = use_path
        self.enc_session_token = enc_session_token
        self.read_only = read_only
        self.color = color
        # Temporary credentials only: when they lapse, and how to mint new
        # ones without going back to the AWS console.
        self.session_expires = session_expires
        self.aws_profile = aws_profile
        self.credential_process = credential_process
        self.public_base_url = public_base_url
        self.requester_pays = requester_pays
        # Network reachability, not identity: an outbound proxy and the CA
        # bundle that makes a private certificate trustworthy.
        self.proxy_url = proxy_url
        self.ca_bundle = ca_bundle


def get_current_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        current_dir = pathlib.Path(sys._MEIPASS)
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    return current_dir


class Profiles(QDialog):
    # Keys handled on the profile list. Enter opens the selected profile the
    # same way a double-click does; the rest mirror the context menu.
    KEY_ACTIONS = {
        Qt.Key.Key_Return: "onStart",
        Qt.Key.Key_Enter: "onStart",
        Qt.Key.Key_Delete: "onDelete",
        Qt.Key.Key_F2: "onEdit",
    }

    def __init__(self, startup=None):
        super().__init__()
        self.startup = dict(startup or {})
        # Set when a location is waiting for the window that is about to open.
        self.pending_location = ""
        self.current_dir = get_current_dir()
        # store settings in ~/.config/s3duck
        self.settings = QSettings("s3duck", "s3duck")
        self.credential_store = CredentialStore(
            self.settings,
            ask_passphrase=lambda prompt: self._ask_passphrase(
                "Unlock credentials", prompt))
        self.items = []
        vbox = QVBoxLayout(self)
        hbox = QHBoxLayout()

        self.listWidget = QListWidget(self)
        self.listWidget.setItemDelegate(ProfileRowDelegate(self.listWidget))

        self.btnRun = QPushButton("Run", self)
        self.btnAdd = QPushButton("Add", self)
        self.btnEdit = QPushButton("Edit", self)
        self.btnDelete = QPushButton("Delete", self)

        self.btnRun.clicked.connect(self.onStart)
        self.btnAdd.clicked.connect(self.onAdd)
        self.btnEdit.clicked.connect(self.onEdit)
        self.btnDelete.clicked.connect(self.onDelete)
        # Buttons in a QDialog are autoDefault, so Enter anywhere fired the
        # first one (Add) instead of opening the selected profile.
        for button in (self.btnRun, self.btnAdd, self.btnEdit, self.btnDelete):
            button.setAutoDefault(False)
            button.setDefault(False)
        self.main_window = None

        vbox.addWidget(self.listWidget)
        hbox.addWidget(self.btnAdd)
        hbox.addWidget(self.btnRun)
        hbox.addWidget(self.btnEdit)
        hbox.addWidget(self.btnDelete)
        self.btnEdit.setEnabled(False)
        self.btnDelete.setEnabled(False)
        self.btnRun.setEnabled(False)
        self.main_settings = None
        vbox.addLayout(hbox)
        self.setLayout(vbox)
        self.resize(350, 250)
        self.setWindowTitle("Profiles")
        self.listWidget.currentItemChanged.connect(self.on_elements_changed)
        self.listWidget.itemSelectionChanged.connect(self.on_elements_changed)
        self.listWidget.installEventFilter(self)
        self.unlock_credentials()
        self.load()
        self.populate_list()
        self.settings.beginGroup("common")
        last_profile = self.settings.value("last_profile", "") or ""
        self.settings.endGroup()
        row = preselect_row(self.items, last_profile)
        if row >= 0:
            self.listWidget.setCurrentIndex(
                self.listWidget.model().index(row, 0))
        self.listWidget.doubleClicked.connect(self.onStart)

        # Listening is best-effort: when another window already holds the
        # socket, this one simply cannot receive handovers.
        self.instance_server = InstanceServer(self)
        self.instance_server.received.connect(self.open_location_request)
        self.instance_server.start()

        self.show()
        if self.startup.get("location"):
            QtCore.QTimer.singleShot(0, self._open_startup_location)

    def showEvent(self, event):
        # Center on the active screen once the frame geometry is known
        # (multi-monitor aware). Done here rather than in __init__ so window
        # decorations are accounted for.
        super().showEvent(event)
        center_on_screen(self)

    def _open_startup_location(self):
        """Open the location the command line named, once the UI is up."""
        location = str(self.startup.get("location") or "")
        if location:
            self.open_location_request(location)

    def profile_row_for(self, name) -> int:
        """The row of a profile by name, or -1."""
        wanted = str(name or "").strip()
        for index, item in enumerate(self.items):
            if str(item.name or "").strip() == wanted and wanted:
                return index
        return -1

    def open_location_request(self, location):
        """
        Act on an `s3://bucket/prefix` handed in by a launch or a second run.

        An open window takes it as a new tab; otherwise a profile is started
        and asked to land there.
        """
        location = str(location or "").strip()
        if not location:
            return
        window = getattr(self, "main_window", None)
        if window is not None and window.isVisible():
            bucket, prefix = window._parse_s3_location(location, "")
            if not bucket:
                return
            window.open_in_new_tab(bucket, prefix)
            window.raise_()
            window.activateWindow()
            return
        wanted = str(self.startup.get("profile") or "")
        row = self.profile_row_for(wanted)
        if row < 0 and self.listWidget.count():
            row = max(0, self.listWidget.currentRow())
        if row < 0:
            QMessageBox.information(
                self, "Open location",
                f"No profile is configured to open {location}.")
            return
        self.listWidget.setCurrentIndex(
            self.listWidget.model().index(row, 0))
        self.pending_location = location
        self.onStart()

    def unlock_credentials(self):
        """
        Make the credential key available for this run.

        Only the passphrase mode can actually fail here, and a wrong one is
        worth retrying rather than starting a session where every profile
        reports the same decryption error.
        """
        for _attempt in range(3):
            try:
                self.credential_store.load_key()
                return True
            except CredentialError as exc:
                answer = QMessageBox.question(
                    self, "Unlock credentials",
                    f"{exc}\n\nTry again?",
                    QMessageBox.StandardButton.Yes
                    | QMessageBox.StandardButton.No)
                if answer != QMessageBox.StandardButton.Yes:
                    return False
        return False

    def _stored_key(self):
        try:
            return self.credential_store.load_key()
        except CredentialError:
            return ""

    def _secrets_or_warn(self, item):
        """(access, secret, token) for a profile, or None after reporting."""
        try:
            return load_profile_secrets(self._stored_key(), item)
        except CredentialError as exc:
            QMessageBox.critical(
                self, "Credentials",
                f"Profile '{item.name}':\n\n{exc}")
            return None

    def _crypto_or_warn(self):
        """A validated Crypto for write paths, or None after reporting."""
        try:
            return require_crypto(self.credential_store.ensure_key())
        except CredentialError as exc:
            QMessageBox.critical(self, "Credentials", str(exc))
            return None

    def credential_storage(self):
        """Choose where the key protecting stored credentials lives."""
        store = self.credential_store
        current = store.mode()
        options = [
            (CREDENTIAL_STORE_LOCAL,
             "Settings file (default) — readable by anything that can read "
             "your config"),
            (CREDENTIAL_STORE_KEYRING,
             "OS secret store" + ("" if keyring_available()
                                  else " — unavailable: install 'keyring'")),
            (CREDENTIAL_STORE_PASSPHRASE,
             "Passphrase — asked once each time s3duck starts"),
        ]
        labels = [label for _mode, label in options]
        index = [mode for mode, _label in options].index(current)
        chosen, ok = QInputDialog.getItem(
            self, "Credential storage",
            "Where to keep the key that protects saved credentials:",
            labels, index, False)
        if not ok:
            return
        mode = options[labels.index(chosen)][0]
        if mode == current:
            return
        passphrase = None
        if mode == CREDENTIAL_STORE_PASSPHRASE:
            passphrase = self._ask_passphrase(
                "Credential storage", "New passphrase:")
            if passphrase is None:
                return
            again = self._ask_passphrase(
                "Credential storage", "Repeat the passphrase:")
            if again != passphrase:
                QMessageBox.warning(
                    self, "Credential storage", "The passphrases differ.")
                return
        try:
            store.set_mode(mode, passphrase=passphrase)
        except CredentialError as exc:
            QMessageBox.critical(self, "Credential storage", str(exc))
            return
        QMessageBox.information(
            self, "Credential storage",
            f"The credential key now lives here:\n\n{store.describe()}")

    def _current_item_index(self) -> int:
        model = self.listWidget.selectionModel()
        row = model.currentIndex().row() if model is not None else -1
        return selected_row_index(row, len(self.items))

    def select_last(self):
        index = self.listWidget.model().index(
            self.listWidget.count() - 1, 0
        )
        self.listWidget.setCurrentIndex(index)

    def copy_profile(self):
        elem = self._current_item_index()
        if elem < 0:
            return
        item = deepcopy(self.items[elem])
        item.name = "%s-copy" % item.name
        self.items.append(item)
        self.save_settings()
        self.populate_list()
        self.select_last()

    def _store_refreshed(self, item, minted) -> tuple:
        """Encrypt freshly minted credentials back onto a profile row."""
        crypto = self._crypto_or_warn()
        if crypto is None:
            return None
        item.enc_access_key = crypto.encrypt(minted["access_key"])
        item.enc_secret_key = crypto.encrypt(minted["secret_key"])
        item.enc_session_token = crypto.encrypt(minted.get("session_token", ""))
        item.session_expires = minted.get("expires", "")
        self.save_settings()
        self.populate_list()
        return (minted["access_key"], minted["secret_key"],
                minted.get("session_token", ""))

    def _refresh_from_process(self, item):
        """
        Mint new credentials by running the profile's credential_process.

        This is how an SSO or assumed-role session stays usable: the stored
        keys are a snapshot that lapses, the command is the thing that can
        produce another one.
        """
        def _mint(_worker):
            return run_credential_process(item.credential_process)

        minted, exc = run_with_progress(
            self, "Refreshing credentials for %s…" % item.name, _mint)
        if minted is None and exc is None:
            return None  # cancelled
        if exc is not None:
            QMessageBox.critical(self, "Refresh credentials", str(exc))
            return None
        return self._store_refreshed(item, minted)

    def _refresh_from_aws_file(self, item):
        """Re-read the ~/.aws profile this one was imported from."""
        entry = load_aws_profiles().get(item.aws_profile)
        if not entry:
            QMessageBox.warning(
                self, "Refresh credentials",
                "Profile '%s' is no longer in ~/.aws/credentials."
                % item.aws_profile)
            return None
        if not entry.get("access_key"):
            QMessageBox.warning(
                self, "Refresh credentials",
                "Profile '%s' in ~/.aws/credentials has no access key."
                % item.aws_profile)
            return None
        return self._store_refreshed(item, {
            "access_key": entry.get("access_key", ""),
            "secret_key": entry.get("secret_key", ""),
            "session_token": entry.get("session_token", ""),
            "expires": entry.get("expires", ""),
        })

    def refresh_credentials(self, item=None, quiet=False):
        """
        Replace a profile's temporary credentials with fresh ones.

        Prefers the credential process, because it can mint a session without
        anything else having run first; falls back to re-reading the ~/.aws
        profile the keys were imported from.
        """
        if item is None:
            elem = self._current_item_index()
            if elem < 0:
                return None
            item = self.items[elem]
        if item.credential_process:
            return self._refresh_from_process(item)
        if item.aws_profile:
            return self._refresh_from_aws_file(item)
        if not quiet:
            QMessageBox.information(
                self, "Refresh credentials",
                "This profile has no credential process and was not imported "
                "from ~/.aws, so there is nothing to refresh from. Edit it and "
                "either import the profile again or set a credential process.")
        return None

    def check_profile(self):
        """
        Keep old behavior:
        still checks a specific bucket configured on this profile.
        """
        elem = self._current_item_index()
        if elem < 0:
            return
        item = self.items[elem]
        secrets = self._secrets_or_warn(item)
        if secrets is None:
            return
        acc_key, secret_key, session_token = secrets
        dm = DataModel(
            item.url,
            item.region,
            acc_key,
            secret_key,
            item.bucket_name,
            str_to_bool(item.no_ssl_check),
            str_to_bool(item.use_path),
            session_token=session_token,
            read_only=str_to_bool(item.read_only),
            requester_pays=str_to_bool(item.requester_pays),
            proxy_url=item.proxy_url,
            ca_bundle=item.ca_bundle,
        )
        # Same reasoning as onStart: this reaches the network (and may create
        # and delete a probe key), so it cannot run on the GUI thread.
        result, exc = run_with_progress(
            self, "Checking %s…" % item.name, lambda worker: dm.check_profile())
        if result is None and exc is None:
            return  # cancelled
        ok, reason = (False, str(exc)) if exc is not None else result

        msgBox = QMessageBox()
        msgBox.setWindowTitle("Profile check")
        msgBox.setStandardButtons(QMessageBox.StandardButton.Ok)
        if ok:
            msgBox.setIcon(QMessageBox.Icon.Information)
            msgBox.setText("Check result OK")
        else:
            msgBox.setIcon(QMessageBox.Icon.Critical)
            msgBox.setText("Check failed: %s" % reason)
        msgBox.exec()

    def eventFilter(self, source, event):
        if (
            event.type() == QtCore.QEvent.Type.KeyPress
            and source is self.listWidget
        ):
            key = event.key()
            handler = self.KEY_ACTIONS.get(key)
            if handler is not None:
                getattr(self, handler)()
                return True
        if (
            event.type() == QtCore.QEvent.Type.ContextMenu
            and source is self.listWidget
        ):
            copy_profile_action = None
            delete_action = None
            edit_profile_action = None
            check_action = None
            refresh_action = None
            menu = QMenu()
            ixs = self.listWidget.selectedIndexes()
            add_profile_action = QAction(
                themed_icon("list-add", os.path.join(
                            self.current_dir, "icons", "plus_24px.svg"
                        )),
                "Add profile",
            )
            menu.addAction(add_profile_action)
            storage_action = QAction(
                themed_icon("dialog-password", os.path.join(
                    self.current_dir, "icons", "settings_24px.svg")),
                "Credential storage…")
            menu.addAction(storage_action)
            import_action = QAction(
                themed_icon("document-open", os.path.join(self.current_dir, "icons", "folder_24px.svg")), "Import profiles…")
            export_action = QAction(
                themed_icon("document-save", os.path.join(self.current_dir, "icons", "download_24px.svg")), "Export profiles…")
            menu.addAction(import_action)
            if self.items:
                menu.addAction(export_action)
            if ixs:
                copy_profile_action = QAction(
                    themed_icon("edit-copy", os.path.join(
                                self.current_dir, "icons", "copy_24px.svg"
                            )),
                    "Copy profile",
                )
                edit_profile_action = QAction(
                    themed_icon("edit-clear", os.path.join(
                                self.current_dir, "icons", "edit_24px.svg"
                            )),
                    "Edit profile",
                )
                check_action = QAction(
                    themed_icon("applications-utilities", os.path.join(
                                self.current_dir, "icons", "ok_24px.svg"
                            )),
                    "Check profile",
                )
                refresh_action = QAction(
                    themed_icon("view-refresh", os.path.join(
                                self.current_dir, "icons", "refresh_24px.svg"
                            )),
                    "Refresh credentials",
                )
                delete_action = QAction(
                    themed_icon("edit-delete", os.path.join(
                                self.current_dir,
                                "icons",
                                "delete_24px.svg",
                            )),
                    "Delete profile",
                )
                menu.addAction(copy_profile_action)
                menu.addAction(edit_profile_action)
                menu.addAction(check_action)
                menu.addAction(refresh_action)
                menu.addAction(delete_action)

            clk = menu.exec(event.globalPos())
            if clk is None:
                return super().eventFilter(source, event)
            if clk == copy_profile_action:
                self.copy_profile()
            if clk == edit_profile_action:
                self.onEdit()
            if clk == delete_action:
                self.onDelete()
            if clk == check_action:
                self.check_profile()
            if refresh_action is not None and clk == refresh_action:
                self.refresh_credentials()
            if clk == add_profile_action:
                self.onAdd()
                return True
            if clk == storage_action:
                self.credential_storage()
                return True
            if clk == import_action:
                self.onImport()
                return True
            if clk == export_action:
                self.onExport()
                return True
        return super().eventFilter(source, event)

    def load(self):
        self.settings.beginGroup("profiles")
        for index in range(self.settings.beginReadArray("profiles")):
            self.settings.setArrayIndex(index)
            self.items.append(
                SettingsItem(
                    self.settings.value("name"),
                    self.settings.value("url"),
                    self.settings.value("region"),
                    self.settings.value("bucket_name"),
                    self.settings.value("access_key"),
                    self.settings.value("secret_key"),
                    self.settings.value("no_ssl_check", "false"),
                    self.settings.value("use_path", "false"),
                    self.settings.value("session_token", ""),
                    self.settings.value("read_only", "false"),
                    self.settings.value("color", ""),
                    self.settings.value("session_expires", ""),
                    self.settings.value("aws_profile", ""),
                    self.settings.value("credential_process", ""),
                    self.settings.value("public_base_url", ""),
                    self.settings.value("requester_pays", "false"),
                    self.settings.value("proxy_url", ""),
                    self.settings.value("ca_bundle", ""),
                )
            )
        self.settings.endArray()
        self.settings.endGroup()

    def onStart(self):
        elem = self._current_item_index()
        if elem < 0:
            return
        item = self.items[elem]
        secrets = self._secrets_or_warn(item)
        if secrets is None:
            return
        acc_key, secret_key, session_token = secrets
        # Temporary credentials that have lapsed (or are about to) would fail
        # the connect probe with an opaque auth error, so mint a session first
        # when the profile knows how.
        state, _label = expiry_state(item.session_expires)
        if state in ("expired", "soon") or not acc_key:
            if item.credential_process or item.aws_profile:
                refreshed = self.refresh_credentials(item, quiet=True)
                if refreshed is not None:
                    acc_key, secret_key, session_token = refreshed
                elif state == "expired":
                    return
            elif state == "expired":
                if QMessageBox.question(
                    self, "Expired credentials",
                    "This profile's temporary credentials expired. Connect "
                    "anyway?",
                    QMessageBox.StandardButton.Yes
                    | QMessageBox.StandardButton.No,
                ) != QMessageBox.StandardButton.Yes:
                    return
        no_ssl_check = str_to_bool(item.no_ssl_check)
        use_path = str_to_bool(item.use_path)
        read_only = str_to_bool(item.read_only)

        # Build DataModel with NO bucket initially.
        dm = DataModel(
            item.url,
            item.region,
            acc_key,
            secret_key,
            "",  # start with no active bucket -> we'll show bucket list
            no_ssl_check,
            use_path,
            session_token=session_token,
            read_only=read_only,
            requester_pays=str_to_bool(item.requester_pays),
            proxy_url=item.proxy_url,
            ca_bundle=item.ca_bundle,
        )

        # A CA bundle that is not there surfaces as an SSLError deep inside
        # the first request, which reads like the server is broken.
        tls_problem = dm.check_tls_settings()
        if tls_problem:
            QMessageBox.warning(self, "Profile", tls_problem)

        # Sanity check creds: try to list buckets. Off the GUI thread, because
        # an unreachable endpoint blocks for the full botocore connect timeout
        # and the launcher would sit there frozen with no way to back out.
        def _probe(worker):
            dm.list_buckets()
            return True

        probed, exc = run_with_progress(
            self, "Connecting to %s…" % item.name, _probe)
        if probed is None and exc is None:
            return  # cancelled
        ok = probed is True
        reason = str(exc) if exc is not None else None

        if ok:
            # Pass empty bucket so MainWindow starts in bucket-list mode
            settings = (
                self.current_dir,
                self.settings,
                item.name,
                item.url,
                item.region,
                "",  # no bucket selected at start
                acc_key,
                secret_key,
                no_ssl_check,
                use_path,
                session_token,
                read_only,
                item.color,
                item.session_expires,
                item.aws_profile,
                item.credential_process,
                item.public_base_url,
                str_to_bool(item.requester_pays),
                item.proxy_url,
                item.ca_bundle,
            )
            self.settings.beginGroup("common")
            self.settings.setValue("last_profile", item.name)
            self.settings.endGroup()
            self.main_settings = settings
            self.main_window = MainWindow(settings=self.main_settings)
            self.main_window.show()
            self.hide()
            pending = getattr(self, "pending_location", "")
            if pending:
                self.pending_location = ""
                bucket, prefix = self.main_window._parse_s3_location(
                    pending, "")
                if bucket:
                    self.main_window.open_location(bucket, prefix)
        else:
            msgBox = QMessageBox()
            msgBox.setWindowTitle("Profile check")
            msgBox.setStandardButtons(QMessageBox.StandardButton.Ok)
            msgBox.setIcon(QMessageBox.Icon.Critical)
            if reason:
                msgBox.setText("Cannot list buckets: %s" % reason)
            else:
                msgBox.setText("Cannot list buckets")
            msgBox.exec()

    def _ask_passphrase(self, title, prompt):
        text, ok = QInputDialog.getText(
            self, title, prompt, QLineEdit.EchoMode.Password)
        if not ok or not text:
            return None
        return text

    def onExport(self):
        """Write all profiles to a passphrase-encrypted bundle."""
        if not self.items:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export profiles", "s3duck-profiles.json",
            "Profile bundles (*.json);;All files (*)")
        if not path:
            return
        passphrase = self._ask_passphrase(
            "Export profiles",
            "Passphrase to protect the exported credentials:")
        if passphrase is None:
            return
        key = self._stored_key()
        payload = []
        for item in self.items:
            try:
                acc, sec, tok = load_profile_secrets(key, item)
            except CredentialError as exc:
                QMessageBox.critical(
                    self, "Export profiles",
                    f"Profile '{item.name}' could not be decrypted, so nothing "
                    f"was exported:\n\n{exc}")
                return
            payload.append({
                "name": item.name,
                "url": item.url,
                "region": item.region,
                "bucket_name": item.bucket_name,
                "access_key": acc,
                "secret_key": sec,
                "session_token": tok,
                "no_ssl_check": str(item.no_ssl_check),
                "use_path": str(item.use_path),
                "read_only": str(item.read_only),
                "color": str(item.color or ""),
                "session_expires": str(item.session_expires or ""),
                "aws_profile": str(item.aws_profile or ""),
                "credential_process": str(item.credential_process or ""),
                "public_base_url": str(item.public_base_url or ""),
                "requester_pays": str(item.requester_pays or "false"),
                "proxy_url": str(item.proxy_url or ""),
                "ca_bundle": str(item.ca_bundle or ""),
            })
        try:
            blob = export_profile_bundle(payload, passphrase)
            with open(path, "wb") as handle:
                handle.write(blob)
        except (BundleError, OSError) as exc:
            QMessageBox.critical(self, "Export profiles", str(exc))
            return
        QMessageBox.information(
            self, "Export profiles",
            f"Exported {len(payload)} profile(s) to:\n{path}")

    def onImport(self):
        """Add profiles from a bundle produced by Export."""
        path, _ = QFileDialog.getOpenFileName(
            self, "Import profiles", "",
            "Profile bundles (*.json);;All files (*)")
        if not path:
            return
        passphrase = self._ask_passphrase(
            "Import profiles", "Passphrase used when the bundle was exported:")
        if passphrase is None:
            return
        try:
            with open(path, "rb") as handle:
                profiles = import_profile_bundle(handle.read(), passphrase)
        except (BundleError, OSError) as exc:
            QMessageBox.critical(self, "Import profiles", str(exc))
            return

        crypto = self._crypto_or_warn()
        if crypto is None:
            return
        existing = {item.name for item in self.items}
        added = 0
        for entry in profiles:
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            # Never clobber a profile that is already configured here.
            while name in existing:
                name = f"{name}-imported"
            existing.add(name)
            self.items.append(
                SettingsItem(
                    name,
                    str(entry.get("url") or ""),
                    str(entry.get("region") or ""),
                    str(entry.get("bucket_name") or ""),
                    crypto.encrypt(str(entry.get("access_key") or "")),
                    crypto.encrypt(str(entry.get("secret_key") or "")),
                    str(entry.get("no_ssl_check", "false")).lower(),
                    str(entry.get("use_path", "false")).lower(),
                    crypto.encrypt(str(entry.get("session_token") or "")),
                    str(entry.get("read_only", "false")).lower(),
                    normalize_accent(entry.get("color", "")),
                    str(entry.get("session_expires") or ""),
                    str(entry.get("aws_profile") or ""),
                    str(entry.get("credential_process") or ""),
                    str(entry.get("public_base_url") or ""),
                    str(entry.get("requester_pays", "false")).lower(),
                    str(entry.get("proxy_url") or ""),
                    str(entry.get("ca_bundle") or ""),
                )
            )
            added += 1
        if not added:
            QMessageBox.warning(
                self, "Import profiles", "The bundle contained no profiles.")
            return
        self.save_settings()
        self.populate_list()
        self.select_last()
        QMessageBox.information(
            self, "Import profiles", f"Imported {added} profile(s).")

    def save_settings(self):
        self.settings.beginGroup("profiles")
        self.settings.beginWriteArray("profiles")
        for index, item in enumerate(self.items):
            self.settings.setArrayIndex(index)
            self.settings.setValue("name", item.name)
            self.settings.setValue("url", item.url)
            self.settings.setValue("region", item.region)
            self.settings.setValue("bucket_name", item.bucket_name)
            self.settings.setValue("access_key", item.enc_access_key)
            self.settings.setValue("secret_key", item.enc_secret_key)
            self.settings.setValue("no_ssl_check", item.no_ssl_check)
            self.settings.setValue("use_path", item.use_path)
            self.settings.setValue("session_token", item.enc_session_token)
            self.settings.setValue("read_only", item.read_only)
            self.settings.setValue("color", item.color)
            self.settings.setValue("session_expires", item.session_expires)
            self.settings.setValue("aws_profile", item.aws_profile)
            self.settings.setValue(
                "credential_process", item.credential_process)
            self.settings.setValue("public_base_url", item.public_base_url)
            self.settings.setValue("requester_pays", item.requester_pays)
            self.settings.setValue("proxy_url", item.proxy_url)
            self.settings.setValue("ca_bundle", item.ca_bundle)
        self.settings.endArray()
        self.settings.endGroup()

    def populate_list(self):
        self.listWidget.clear()
        for item in self.items:
            summary = profile_summary(item)
            badges = profile_badges(item)
            row = QListWidgetItem(f"{item.name}\n{summary}")
            row.setData(Qt.ItemDataRole.UserRole, badges)
            row.setData(Qt.ItemDataRole.UserRole + 1, item.color)
            # The row elides; the tooltip is where the whole truth stays.
            row.setToolTip(" · ".join([summary] + badges))
            self.listWidget.addItem(row)

    def onAdd(self):
        settings = SettingsWindow(self)
        value = settings.exec()
        if value:
            crypto = self._crypto_or_warn()
            if crypto is None:
                return
            (
                name,
                url,
                region,
                bucket,
                access_key,
                secret_key,
                no_ssl_check,
                use_path,
                session_token,
                read_only,
                color,
                session_expires,
                aws_profile,
                credential_process,
                public_base_url,
                requester_pays,
                proxy_url,
                ca_bundle,
            ) = value
            enc_access_key = crypto.encrypt(access_key)
            enc_secret_key = crypto.encrypt(secret_key)
            enc_session_token = crypto.encrypt(session_token or "")
            self.items.append(
                SettingsItem(
                    name,
                    url,
                    region,
                    bucket,
                    enc_access_key,
                    enc_secret_key,
                    no_ssl_check,
                    use_path,
                    enc_session_token,
                    str(bool(read_only)).lower(),
                    normalize_accent(color),
                    session_expires,
                    aws_profile,
                    credential_process,
                    public_base_url,
                    str(bool(requester_pays)).lower(),
                    proxy_url,
                    ca_bundle,
                )
            )
            self.save_settings()
            self.populate_list()
            self.select_last()

    def onEdit(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = self._current_item_index()
        if elem < 0:
            return
        item = self.items[elem]
        secrets = self._secrets_or_warn(item)
        if secrets is None:
            return
        crypto = self._crypto_or_warn()
        if crypto is None:
            return
        acc_key, secret_key, session_token = secrets
        settings = (
            item.name,
            item.url,
            item.region,
            item.bucket_name,
            acc_key,
            secret_key,
            item.no_ssl_check,
            item.use_path,
            session_token,
            item.read_only,
            item.color,
            item.session_expires,
            item.aws_profile,
            item.credential_process,
            item.public_base_url,
            item.requester_pays,
            item.proxy_url,
            item.ca_bundle,
        )
        settings = SettingsWindow(self, settings=settings)
        value = settings.exec()
        if value:
            (
                name,
                url,
                region,
                bucket,
                access_key,
                secret_key,
                no_ssl_check,
                use_path,
                session_token,
                read_only,
                color,
                session_expires,
                aws_profile,
                credential_process,
                public_base_url,
                requester_pays,
                proxy_url,
                ca_bundle,
            ) = value
            enc_access_key = crypto.encrypt(access_key)
            enc_secret_key = crypto.encrypt(secret_key)
            enc_session_token = crypto.encrypt(session_token or "")
            self.items[elem] = SettingsItem(
                name,
                url,
                region,
                bucket,
                enc_access_key,
                enc_secret_key,
                no_ssl_check,
                use_path,
                enc_session_token,
                str(bool(read_only)).lower(),
                normalize_accent(color),
                session_expires,
                aws_profile,
                credential_process,
                public_base_url,
                str(bool(requester_pays)).lower(),
                proxy_url,
                ca_bundle,
            )
            self.save_settings()
            self.populate_list()
            self.listWidget.setCurrentIndex(index)

    def onDelete(self):
        elem = self._current_item_index()
        if elem < 0:
            return
        qm = QMessageBox
        ret = qm.question(
            self,
            "",
            "Are you sure to delete objects : %s ?" % self.items[elem].name,
            qm.StandardButton.Yes | qm.StandardButton.No,
        )
        if ret == qm.StandardButton.Yes:
            del self.items[elem]
            self.save_settings()
            self.populate_list()

    @QtCore.pyqtSlot()
    def on_elements_changed(self):
        self.btnRun.setEnabled(
            self.listWidget.count() > 0
            and bool(self.listWidget.selectedIndexes())
        )
        self.btnEdit.setEnabled(
            self.listWidget.count() > 0
            and bool(self.listWidget.selectedIndexes())
        )
        self.btnDelete.setEnabled(
            self.listWidget.count() > 0
            and bool(self.listWidget.selectedIndexes())
        )


def parse_cli(argv) -> dict:
    """
    Read the command line: an optional profile and a location to open.

    Kept tiny and pure — the point is `s3duck s3://bucket/prefix` from a
    terminal, a chat link or a file manager, not a second CLI to learn.
    """
    out = {"profile": "", "location": "", "help": False, "unknown": []}
    items = list(argv or [])
    index = 0
    while index < len(items):
        item = str(items[index])
        if item in ("-h", "--help"):
            out["help"] = True
        elif item in ("-p", "--profile"):
            index += 1
            if index < len(items):
                out["profile"] = str(items[index])
        elif item.startswith("--profile="):
            out["profile"] = item.split("=", 1)[1]
        elif item.startswith("-"):
            out["unknown"].append(item)
        elif not out["location"]:
            out["location"] = item
        else:
            out["unknown"].append(item)
        index += 1
    return out


CLI_USAGE = """s3duck — GUI client for S3-compatible storage

  s3duck                          open the profile launcher
  s3duck s3://bucket/prefix/      open that location
  s3duck -p NAME s3://bucket/     open it with a named profile

A location handed to an already-running window opens there in a new tab."""


def main():
    args = parse_cli(sys.argv[1:])
    if args["help"]:
        print(CLI_USAGE)
        return
    # Only a location is handed over: a bare launch still opens its own
    # window, because two profiles side by side is a thing people want.
    if args["location"] and send_to_running_instance(args["location"]):
        return

    app = QApplication(sys.argv)
    # Cross-platform font with emoji fallback
    font = QFont()
    # prefer system UI font; then add family fallbacks
    font.setFamilies([
        "Segoe UI", "Noto Sans", "Helvetica Neue", "Cantarell", "Ubuntu", "San Francisco",
        "Apple Color Emoji", "Noto Color Emoji", "Segoe UI Emoji"
    ])
    font.setPointSize(10)
    app.setFont(font)
    icon = QIcon(os.path.join(get_current_dir(), "resources", "ducky.ico"))
    app.setWindowIcon(icon)

    # Apply the saved theme before any window is shown.
    _settings = QSettings("s3duck", "s3duck")
    _settings.beginGroup("common")
    _saved_theme = _settings.value("theme", "system") or "system"
    _settings.endGroup()
    apply_theme(app, _saved_theme)

    profiles = Profiles(startup=args)
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
