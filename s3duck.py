#!/usr/bin/python

import sys
import os
import pathlib
import urllib3
from copy import deepcopy
from PyQt5.QtGui import QIcon
from cryptography.fernet import Fernet
from PyQt5 import QtCore
from PyQt5.QtCore import *
from PyQt5.QtWidgets import (
    QListWidget,
    QPushButton,
    QHBoxLayout,
    QApplication,
    QVBoxLayout,
    QSplitter,
    QMessageBox,
    QDialog, QMenu, QAction,
)

from model import Model as DataModel
from settings import SettingsWindow
from main_window import MainWindow
from utils import str_to_bool

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
            use_path
    ):
        self.name = name
        self.url = url
        self.region = region
        self.bucket_name = bucket_name
        self.enc_access_key = enc_access_key
        self.enc_secret_key = enc_secret_key
        self.no_ssl_check = no_ssl_check
        self.use_path = use_path


def get_current_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, '_MEIPASS'):
        current_dir = pathlib.Path(sys._MEIPASS)
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    return current_dir


class Profiles(QDialog):

    def __init__(self):
        super().__init__()
        self.current_dir = get_current_dir()
        # store settings in ~/config/s3duck
        self.settings = QSettings("s3duck", "s3duck")
        self.items = []
        vbox = QVBoxLayout(self)
        hbox = QHBoxLayout()

        self.listWidget = QListWidget(self)

        self.splitter = QSplitter()
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
        self.setGeometry(800, 400, 350, 250)
        self.setWindowTitle('Profiles')
        self.listWidget.currentItemChanged.connect(self.on_elements_changed)
        self. listWidget.itemSelectionChanged.connect(self.on_elements_changed)
        self.listWidget.installEventFilter(self)
        self.load()
        self.populate_list()
        if self.listWidget.count() > 0:
            index = self.listWidget.model().index(0, 0)
            self.listWidget.setCurrentIndex(index)
        self.listWidget.doubleClicked.connect(self.onStart)
        self.show()

    def select_last(self):
        index = self.listWidget.model().index(self.listWidget.count() - 1, 0)
        self.listWidget.setCurrentIndex(index)

    def copy_profile(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = index.row()
        item = deepcopy(self.items[elem])
        item.name = "%s-copy" % item.name
        self.items.append(item)
        self.save_settings()
        self.populate_list()
        self.select_last()

    def check_profile(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = index.row()
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
            str_to_bool(item.use_path)
        )
        ok, reason = dm.check_profile()
        msgBox = QMessageBox()
        msgBox.setWindowTitle("Profile check")
        msgBox.setStandardButtons(QMessageBox.Ok)
        if ok:
            msgBox.setIcon(QMessageBox.Information)
            msgBox.setText("Check result OK")
        else:
            msgBox.setIcon(QMessageBox.Critical)
            msgBox.setText("Check failed: %s" % reason)
        msgBox.exec()

    def eventFilter(self, source, event):
        if (event.type() == QtCore.QEvent.ContextMenu and
                source is self.listWidget):
            copy_profile_action = delete_action = edit_profile_action = check_action = QObject()
            menu = QMenu()
            ixs = self.listWidget.selectedIndexes()
            add_profile_action = QAction(QIcon.fromTheme("list-add", QIcon(os.path.join(
                        self.current_dir, "icons", "plus_24px.svg"))), "Add profile")
            menu.addAction(add_profile_action)
            if ixs:
                copy_profile_action = QAction(QIcon.fromTheme("edit-copy", QIcon(os.path.join(
                        self.current_dir, "icons", "copy_24px.svg"))), "Copy profile")
                edit_profile_action = QAction(QIcon.fromTheme("edit-clear", QIcon(os.path.join(
                        self.current_dir, "icons", "edit_24px.svg"))), "Edit profile")
                check_action = QAction(QIcon.fromTheme("applications-utilities", QIcon(os.path.join(
                        self.current_dir, "icons", "ok_24px.svg"))), "Check profile")
                delete_action = QAction(QIcon.fromTheme("edit-delete", QIcon(os.path.join(
                    self.current_dir, "icons", "delete_24px.svg"))), "Delete profile")
                menu.addAction(copy_profile_action)
                menu.addAction(edit_profile_action)
                menu.addAction(check_action)
                menu.addAction(delete_action)

            clk = menu.exec_(event.globalPos())
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
        return super().eventFilter(source, event)

    def load(self):
        self.settings.beginGroup("common")
        self.settings.endGroup()
        self.settings.beginGroup("profiles")
        for index in range(self.settings.beginReadArray('profiles')):
            self.settings.setArrayIndex(index)
            self.items.append(SettingsItem(
                self.settings.value("name"),
                self.settings.value("url"),
                self.settings.value("region"),
                self.settings.value("bucket_name"),
                self.settings.value("access_key"),
                self.settings.value("secret_key"),
                self.settings.value("no_ssl_check", "false"),
                self.settings.value("use_path", "false")
            ))
        self.settings.endArray()
        self.settings.endGroup()

    def onStart(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = index.row()
        if elem < 0:
            return
        item = self.items[elem]
        self.settings.beginGroup("common")
        key = self.settings.value("key")
        self.settings.endGroup()
        crypto = Crypto(key)

        acc_key = crypto.decrypt_cred(item.enc_access_key)
        secret_key = crypto.decrypt_cred(item.enc_secret_key)
        no_ssl_check = str_to_bool(item.no_ssl_check)
        use_path = str_to_bool(item.use_path)

        # try to get bucket
        dm = DataModel(
            item.url,
            item.region,
            acc_key,
            secret_key,
            item.bucket_name,
            no_ssl_check,
            use_path
        )
        res, reason = dm.check_bucket()
        if res:
            settings = (
                self.current_dir,
                self.settings,
                item.name,
                item.url,
                item.region,
                item.bucket_name,
                acc_key,
                secret_key,
                no_ssl_check,
                use_path
            )
            self.main_settings = settings
            self.main_window = MainWindow(settings=self.main_settings)
            self.main_window.show()
            self.hide()
        else:
            msgBox = QMessageBox()
            msgBox.setWindowTitle("Profile check")
            msgBox.setStandardButtons(QMessageBox.Ok)
            msgBox.setIcon(QMessageBox.Critical)
            msgBox.setText("Check failed: %s" % reason)
            msgBox.exec()

    def save_settings(self):
        self.settings.beginGroup("profiles")
        self.settings.beginWriteArray('profiles')
        for index, item in enumerate(self.items):
            self.settings.setArrayIndex(index)
            self.settings.setValue("name", item.name)
            self.settings.setValue('url', item.url)
            self.settings.setValue('region', item.region)
            self.settings.setValue('bucket_name', item.bucket_name)
            self.settings.setValue('access_key', item.enc_access_key)
            self.settings.setValue('secret_key', item.enc_secret_key)
            self.settings.setValue("no_ssl_check", item.no_ssl_check)
            self.settings.setValue("use_path", item.use_path)
        self.settings.endArray()
        self.settings.endGroup()

    def populate_list(self):
        self.listWidget.clear()
        elems = [x.name for x in self.items]
        self.listWidget.addItems(elems)

    def onAdd(self):
        settings = SettingsWindow(self)
        value = settings.exec_()
        if value:
            self.settings.beginGroup("common")
            key = self.settings.value("key")
            self.settings.endGroup()
            if not key:
                key = Crypto.generate_key()
                self.settings.beginGroup("common")
                self.settings.setValue("key", key)
                self.settings.endGroup()
            name, url, region, bucket, access_key, secret_key, no_ssl_check, use_path = value
            # encrypt access & secret key
            crypto = Crypto(key)
            enc_access_key = crypto.encrypt(access_key)
            enc_secret_key = crypto.encrypt(secret_key)
            self.items.append(
                SettingsItem(
                    name,
                    url,
                    region,
                    bucket,
                    enc_access_key,
                    enc_secret_key,
                    no_ssl_check,
                    use_path
                    )
                )
            self.save_settings()
            self.populate_list()
            self.select_last()

    def onEdit(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = index.row()
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
            item.use_path
        )
        settings = SettingsWindow(self, settings=settings)
        value = settings.exec_()
        if value:
            name, url, region, bucket, access_key, secret_key, no_ssl_check, use_path = value
            enc_access_key = crypto.encrypt(access_key)
            enc_secret_key = crypto.encrypt(secret_key)
            self.items[elem] = SettingsItem(
                    name,
                    url,
                    region,
                    bucket,
                    enc_access_key,
                    enc_secret_key,
                    no_ssl_check,
                    use_path
                )
            self.save_settings()
            self.populate_list()
            self.listWidget.setCurrentIndex(index)

    def onDelete(self):
        index = self.listWidget.selectionModel().currentIndex()
        elem = index.row()
        if elem < 0:
            return
        qm = QMessageBox
        ret = qm.question(self, '', "Are you sure to delete objects : %s ?" % self.items[elem].name, qm.Yes | qm.No)
        if ret == qm.Yes:
            del self.items[elem]
            self.save_settings()
            self.populate_list()

    @QtCore.pyqtSlot()
    def on_elements_changed(self):
        self.btnRun.setEnabled(
            self.listWidget.count() > 0 and bool(self.listWidget.selectedIndexes())
        )
        self.btnEdit.setEnabled(
            self.listWidget.count() > 0 and bool(self.listWidget.selectedIndexes())
        )
        self.btnDelete .setEnabled(
            self.listWidget.count() > 0 and bool(self.listWidget.selectedIndexes())
        )


def main():
    app = QApplication(sys.argv)
    icon = QIcon(
        os.path.join(get_current_dir(), "resources", "ducky.ico"))
    app.setWindowIcon(icon)
    profiles = Profiles()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
