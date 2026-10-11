"""Choosing another profile, and building a Model for it.
"""

from PyQt6.QtCore import QThread
from PyQt6.QtWidgets import (
    QComboBox, QDialog, QFormLayout, QLabel, QLineEdit, QWidget,
)
from model import Model as DataModel, prefix_of
from profile_switcher import decrypt_profile, load_profiles
from utils import FuncWorker, join_qthread, release_worker_on_finish


def build_profile_model(profile, bucket=""):
    """A DataModel for another profile's credentials and endpoint."""
    return DataModel(
        profile.url,
        profile.region,
        profile.access_key,
        profile.secret_key,
        bucket,
        profile.no_ssl_check,
        profile.use_path,
        session_token=profile.session_token,
        read_only=profile.read_only,
        requester_pays=getattr(profile, "requester_pays", False),
        public_base_url=getattr(profile, "public_base_url", ""),
        proxy_url=getattr(profile, "proxy_url", ""),
        ca_bundle=getattr(profile, "ca_bundle", ""),
    )


class ProfilePicker(QWidget):
    """
    Pick another profile, one of its buckets and a prefix.

    Shared by the cross-profile copy and sync dialogs. Extracted rather than
    copied because the bucket list is fetched off the GUI thread — a different
    endpoint may be slow or unreachable — and a second copy of that thread
    plumbing is a second chance to leak a QThread.
    """

    def __init__(self, parent, settings, current_profile=""):
        super().__init__(parent)
        self._settings = settings
        self._thread = None
        self._worker = None
        self.note = QLabel("")
        self.note.setWordWrap(True)

        self._raw = [
            raw for raw in load_profiles(settings)
            if str(raw.get("name") or "") != current_profile
        ]

        self.profile = QComboBox()
        for raw in self._raw:
            self.profile.addItem(str(raw.get("name") or "<unnamed>"))
        self.profile.currentIndexChanged.connect(self._on_profile_changed)

        self.bucket = QComboBox()
        self.bucket.setEditable(True)
        self.prefix = QLineEdit()
        self.prefix.setPlaceholderText("prefix (blank = bucket root)")

        form = QFormLayout(self)
        form.setContentsMargins(0, 0, 0, 0)
        form.addRow(QLabel("Profile"), self.profile)
        form.addRow(QLabel("Bucket"), self.bucket)
        form.addRow(QLabel("Prefix"), self.prefix)

        if isinstance(parent, QDialog):
            # accept() does not fire closeEvent, so a dialog dismissed while a
            # bucket listing is in flight would be destroyed with a live
            # QThread — which aborts the process in PyQt6. finished covers
            # accept, reject and close alike.
            parent.finished.connect(lambda _result: self.stop_loader())

        if self._raw:
            self._on_profile_changed(0)

    def has_profiles(self) -> bool:
        return bool(self._raw)

    def selected_profile(self):
        """The decrypted target Profile, or None."""
        index = self.profile.currentIndex()
        if index < 0 or index >= len(self._raw):
            return None
        return decrypt_profile(self._settings, self._raw[index])

    def destination_bucket(self) -> str:
        return self.bucket.currentText().strip()

    def destination_prefix(self) -> str:
        text = self.prefix.text().strip().lstrip("/")
        return prefix_of(text) if text else ""

    def _on_profile_changed(self, _index):
        self.bucket.clear()
        try:
            profile = self.selected_profile()
        except Exception as exc:
            self.note.setText(f"Could not read that profile: {exc}")
            return
        if profile is None:
            return
        if profile.bucket:
            self.bucket.setCurrentText(profile.bucket)
        self._load_buckets(profile)

    def _load_buckets(self, profile):
        self.stop_loader()
        model = build_profile_model(profile)

        def _fetch(_w):
            return [b.name for b in model.list_buckets()]

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_buckets)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_buckets(self, result, exc):
        self.stop_loader()
        if exc is not None:
            self.note.setText(f"Could not list that profile's buckets: {exc}")
            return
        typed = self.bucket.currentText()
        self.bucket.blockSignals(True)
        self.bucket.clear()
        self.bucket.addItems(result or [])
        self.bucket.setCurrentText(typed)
        self.bucket.blockSignals(False)

    def stop_loader(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)
