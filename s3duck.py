#!/usr/bin/python

import sys

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
    QDialog,
)


from settings import SettingsWindow
from main_window import MainWindow


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

    def __init__(self, name, url, region, bucket_name, enc_access_key, enc_secret_key):
        self.name = name
        self.url = url
        self.region = region
        self.bucket_name = bucket_name
        self.enc_access_key = enc_access_key
        self.enc_secret_key = enc_secret_key


class Profiles(QDialog):

    def __init__(self):
        super().__init__()
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
        self.listWidget.currentItemChanged.connect(self.on_element_count_changed)
        self.load()
        self.populate_list()
        self.show()

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
                self.settings.value("secret_key")
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
        settings = (
            self.settings,
            item.name,
            item.url,
            item.region,
            item.bucket_name,
            crypto.decrypt_cred(item.enc_access_key),
            crypto.decrypt_cred(item.enc_secret_key)
        )
        self.main_settings = settings
        self.main_window = MainWindow(settings=self.main_settings)
        self.main_window.show()
        self.hide()

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
            name, url, region, bucket, access_key, secret_key = value
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
                    enc_secret_key
                    )
                )
            self.save_settings()
            self.populate_list()

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
            crypto.decrypt_cred(item.enc_secret_key)
        )
        settings = SettingsWindow(self, settings=settings)
        value = settings.exec_()
        if value:
            name, url, region, bucket, access_key, secret_key = value
            enc_access_key = crypto.encrypt(access_key)
            enc_secret_key = crypto.encrypt(secret_key)
            self.items[elem] = SettingsItem(
                    name,
                    url,
                    region,
                    bucket,
                    enc_access_key,
                    enc_secret_key
                )
            self.save_settings()
            self.populate_list()

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
    def on_element_count_changed(self):
        self.btnRun.setEnabled(
            self.listWidget.count() > 0
        )
        self.btnEdit.setEnabled(
            self.listWidget.count() > 0
        )
        self.btnDelete .setEnabled(
            self.listWidget.count() > 0
        )
        if self.listWidget.count() > 0:
            self.listWidget.selectionModel().selectedIndexes()


def main():
    app = QApplication(sys.argv)
    profiles = Profiles()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
