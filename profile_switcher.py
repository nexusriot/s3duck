# profile_switcher.py
from dataclasses import dataclass
from typing import List, Optional, Tuple

from PyQt5.QtCore import QSettings
from PyQt5.QtWidgets import (
    QDialog, QListWidget, QPushButton, QHBoxLayout, QVBoxLayout,
    QMessageBox, QLabel
)
from cryptography.fernet import Fernet

from utils import str_to_bool


@dataclass
class Profile:
    name: str
    url: str
    region: str
    bucket: str
    access_key: str
    secret_key: str
    no_ssl_check: bool
    use_path: bool


class Crypto:
    def __init__(self, key: str):
        self.fernet = Fernet(key.encode() if isinstance(key, str) else key)

    def decrypt(self, value: str) -> str:
        # QSettings sometimes returns QByteArray/str; handle both
        if value is None:
            return ""
        if isinstance(value, bytes):
            enc = value
        else:
            enc = str(value).encode()
        return self.fernet.decrypt(enc).decode()


def load_profiles(settings: QSettings) -> List[dict]:
    items = []
    settings.beginGroup("profiles")
    n = settings.beginReadArray("profiles")
    for i in range(n):
        settings.setArrayIndex(i)
        items.append({
            "name": settings.value("name", ""),
            "url": settings.value("url", ""),
            "region": settings.value("region", ""),
            "bucket_name": settings.value("bucket_name", ""),
            "access_key": settings.value("access_key", ""),
            "secret_key": settings.value("secret_key", ""),
            "no_ssl_check": settings.value("no_ssl_check", "false"),
            "use_path": settings.value("use_path", "false"),
        })
    settings.endArray()
    settings.endGroup()
    return items


def decrypt_profile(settings: QSettings, raw: dict) -> Profile:
    settings.beginGroup("common")
    key = settings.value("key", "")
    settings.endGroup()

    if not key:
        raise RuntimeError("Encryption key is missing in settings (group: common/key)")

    crypto = Crypto(key)
    return Profile(
        name=str(raw.get("name") or ""),
        url=str(raw.get("url") or ""),
        region=str(raw.get("region") or ""),
        bucket=str(raw.get("bucket_name") or ""),
        access_key=crypto.decrypt(raw.get("access_key")),
        secret_key=crypto.decrypt(raw.get("secret_key")),
        no_ssl_check=str_to_bool(raw.get("no_ssl_check", "false")),
        use_path=str_to_bool(raw.get("use_path", "false")),
    )


class ProfileSwitchWindow(QDialog):
    """
    Separate window to switch profiles at runtime.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Switch profile")
        self.resize(420, 320)

        self.settings = QSettings("s3duck", "s3duck")
        self.raw_profiles = load_profiles(self.settings)

        self.listw = QListWidget(self)
        for p in self.raw_profiles:
            self.listw.addItem(p.get("name") or "<unnamed>")

        self.btn_apply = QPushButton("Apply", self)
        self.btn_cancel = QPushButton("Cancel", self)
        self.btn_apply.setEnabled(False)

        self.listw.currentRowChanged.connect(self._on_row_changed)
        self.btn_apply.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

        info = QLabel("Select a profile and click Apply.\n(Current transfers won’t be stopped automatically.)")
        info.setWordWrap(True)

        h = QHBoxLayout()
        h.addStretch(1)
        h.addWidget(self.btn_apply)
        h.addWidget(self.btn_cancel)

        v = QVBoxLayout(self)
        v.addWidget(info)
        v.addWidget(self.listw)
        v.addLayout(h)

    def _on_row_changed(self, row: int):
        self.btn_apply.setEnabled(row >= 0)

    def get_selected_profile(self) -> Optional[Profile]:
        row = self.listw.currentRow()
        if row < 0 or row >= len(self.raw_profiles):
            return None
        try:
            return decrypt_profile(self.settings, self.raw_profiles[row])
        except Exception as exc:
            QMessageBox.warning(self, "Profile", str(exc))
            return None
