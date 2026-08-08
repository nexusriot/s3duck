#!/usr/bin/python

import sys
import os
import pathlib
import urllib3
from copy import deepcopy
from PyQt6.QtGui import QIcon, QFont, QAction
from cryptography.fernet import Fernet
from PyQt6 import QtCore
from PyQt6.QtCore import *
from PyQt6.QtWidgets import (
    QListWidget,
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
)

from model import Model as DataModel
from settings import SettingsWindow
from main_window import MainWindow
from utils import (
    str_to_bool, center_on_screen, export_profile_bundle,
    import_profile_bundle, BundleError,
)
from theme import apply_theme

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Crypto:
    def __init__(self, key):
        self.key = key
        self._fernet = None

    @property
    def fernet(self):
        if self._fernet is None:
            self._fernet = Fernet(self.key.encode())
        return self._fernet

    def encrypt(self, value):
        return self.fernet.encrypt(value.encode())

    @staticmethod
    def generate_key():
        return Fernet.generate_key().decode()

    def decrypt_cred(self, val):
        return self.fernet.decrypt(val).decode()


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


def decrypt_optional(crypto, value) -> str:
    """Decrypt a possibly absent/legacy field without failing the whole load."""
    if not value:
        return ""
    try:
        return crypto.decrypt_cred(value)
    except Exception:
        return ""


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


def get_current_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        current_dir = pathlib.Path(sys._MEIPASS)
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    return current_dir


class Profiles(QDialog):
    def __init__(self):
        super().__init__()
        self.current_dir = get_current_dir()
        # store settings in ~/.config/s3duck
        self.settings = QSettings("s3duck", "s3duck")
        self.items = []
        vbox = QVBoxLayout(self)
        hbox = QHBoxLayout()

        self.listWidget = QListWidget(self)

        self.btnRun = QPushButton("Run", self)
        self.btnAdd = QPushButton("Add", self)
        self.btnEdit = QPushButton("Edit", self)
        self.btnDelete = QPushButton("Delete", self)

        self.btnRun.clicked.connect(self.onStart)
        self.btnAdd.clicked.connect(self.onAdd)
        self.btnEdit.clicked.connect(self.onEdit)
        self.btnDelete.clicked.connect(self.onDelete)
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
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        crypto = Crypto(key)
        dm = DataModel(
            item.url,
            item.region,
            crypto.decrypt_cred(item.enc_access_key),
            crypto.decrypt_cred(item.enc_secret_key),
            item.bucket_name,
            str_to_bool(item.no_ssl_check),
            str_to_bool(item.use_path),
            session_token=decrypt_optional(crypto, item.enc_session_token),
            read_only=str_to_bool(item.read_only),
        )
        ok, reason = dm.check_profile()
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
                QIcon.fromTheme(
                    "list-add",
                    QIcon(
                        os.path.join(
                            self.current_dir, "icons", "plus_24px.svg"
                        )
                    ),
                ),
                "Add profile",
            )
            menu.addAction(add_profile_action)
            import_action = QAction(
                QIcon.fromTheme("document-open"), "Import profiles…")
            export_action = QAction(
                QIcon.fromTheme("document-save"), "Export profiles…")
            menu.addAction(import_action)
            if self.items:
                menu.addAction(export_action)
            if ixs:
                copy_profile_action = QAction(
                    QIcon.fromTheme(
                        "edit-copy",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "copy_24px.svg"
                            )
                        ),
                    ),
                    "Copy profile",
                )
                edit_profile_action = QAction(
                    QIcon.fromTheme(
                        "edit-clear",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "edit_24px.svg"
                            )
                        ),
                    ),
                    "Edit profile",
                )
                check_action = QAction(
                    QIcon.fromTheme(
                        "applications-utilities",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "ok_24px.svg"
                            )
                        ),
                    ),
                    "Check profile",
                )
                delete_action = QAction(
                    QIcon.fromTheme(
                        "edit-delete",
                        QIcon(
                            os.path.join(
                                self.current_dir,
                                "icons",
                                "delete_24px.svg",
                            )
                        ),
                    ),
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
                )
            )
        self.settings.endArray()
        self.settings.endGroup()

    def onStart(self):
        elem = self._current_item_index()
        if elem < 0:
            return
        item = self.items[elem]
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        crypto = Crypto(key)

        acc_key = crypto.decrypt_cred(item.enc_access_key)
        secret_key = crypto.decrypt_cred(item.enc_secret_key)
        session_token = decrypt_optional(crypto, item.enc_session_token)
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

        # Sanity check creds: try to list buckets
        ok = False
        reason = None
        try:
            dm.list_buckets()
            ok = True
        except Exception as exc:
            reason = str(exc)

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

    def _crypto(self):
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        if not key:
            key = Crypto.generate_key()
            self.settings.beginGroup("common")
            self.settings.setValue("key", key)
            self.settings.endGroup()
        return Crypto(key)

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
        crypto = self._crypto()
        payload = []
        for item in self.items:
            payload.append({
                "name": item.name,
                "url": item.url,
                "region": item.region,
                "bucket_name": item.bucket_name,
                "access_key": crypto.decrypt_cred(item.enc_access_key),
                "secret_key": crypto.decrypt_cred(item.enc_secret_key),
                "session_token": decrypt_optional(crypto, item.enc_session_token),
                "no_ssl_check": str(item.no_ssl_check),
                "use_path": str(item.use_path),
                "read_only": str(item.read_only),
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

        crypto = self._crypto()
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
        self.settings.endArray()
        self.settings.endGroup()

    def populate_list(self):
        self.listWidget.clear()
        elems = [x.name for x in self.items]
        self.listWidget.addItems(elems)

    def onAdd(self):
        settings = SettingsWindow(self)
        value = settings.exec()
        if value:
            self.settings.beginGroup("common")
            key = self.settings.value("key")
            self.settings.endGroup()
            if not key:
                key = Crypto.generate_key()
                self.settings.beginGroup("common")
                self.settings.setValue("key", key)
                self.settings.endGroup()
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
            ) = value
            crypto = Crypto(key)
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
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        crypto = Crypto(key)
        settings = (
            item.name,
            item.url,
            item.region,
            item.bucket_name,
            crypto.decrypt_cred(item.enc_access_key),
            crypto.decrypt_cred(item.enc_secret_key),
            item.no_ssl_check,
            item.use_path,
            decrypt_optional(crypto, item.enc_session_token),
            item.read_only,
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
