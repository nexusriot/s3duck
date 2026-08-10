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
    normalize_accent, themed_icon,
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

# Badges are colour-coded by what they mean for the user: amber for a mode
# worth knowing about, red for an actual exposure. Fixed colours rather than
# palette roles, because both must stay legible under every theme.
BADGE_COLORS = {
    READ_ONLY_BADGE: QColor("#b26a00"),
    INSECURE_BADGE: QColor("#c62828"),
}


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
    return badges


def badge_color(name, palette, selected: bool) -> QColor:
    """A badge keeps its own colour except on a selected row, where it would
    sit on the highlight brush and lose contrast."""
    if selected:
        return QColor(palette.highlightedText().color())
    return QColor(BADGE_COLORS.get(name, palette.text().color()))


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

    def __init__(self):
        super().__init__()
        self.current_dir = get_current_dir()
        # store settings in ~/.config/s3duck
        self.settings = QSettings("s3duck", "s3duck")
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
        self.load()
        self.populate_list()
        if self.listWidget.count() > 0:
            index = self.listWidget.model().index(0, 0)
            self.listWidget.setCurrentIndex(index)
        self.listWidget.doubleClicked.connect(self.onStart)
        self.show()

    def showEvent(self, event):
        # Center on the active screen once the frame geometry is known
        # (multi-monitor aware). Done here rather than in __init__ so window
        # decorations are accounted for.
        super().showEvent(event)
        center_on_screen(self)

    def _stored_key(self):
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        return key

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
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        if not key:
            key = Crypto.generate_key()
            self.settings.setValue("key", key)
        self.settings.endGroup()
        try:
            return require_crypto(key)
        except CredentialError as exc:
            QMessageBox.critical(self, "Credentials", str(exc))
            return None

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
            menu = QMenu()
            ixs = self.listWidget.selectedIndexes()
            add_profile_action = QAction(
                themed_icon("list-add", os.path.join(
                            self.current_dir, "icons", "plus_24px.svg"
                        )),
                "Add profile",
            )
            menu.addAction(add_profile_action)
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
            if clk == add_profile_action:
                self.onAdd()
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
        )

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
            )
            self.main_settings = settings
            self.main_window = MainWindow(settings=self.main_settings)
            self.main_window.show()
            self.hide()
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


def main():
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

    profiles = Profiles()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
