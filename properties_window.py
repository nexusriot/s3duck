from PyQt6.QtCore import *
from PyQt6.QtWidgets import *
from PyQt6.QtGui import *

from utils import center_on_screen


class _SizeWorker(QObject):
    """Sums a prefix's size off the main thread (a recursive listing)."""

    done = pyqtSignal(object, object)  # (size_or_None, exception_or_None)

    def __init__(self, model, key):
        super().__init__()
        self._model = model
        self._key = key

    @pyqtSlot()
    def run(self):
        try:
            self.done.emit(self._model.get_size(self._key), None)
        except Exception as exc:
            self.done.emit(None, exc)


class PropertiesWindow(QDialog):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        model, key = settings
        self.setWindowTitle("Object properties")
        self.resize(600, 200)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.key = key
        self.model = model
        self._size_thread = None
        self._size_worker = None

        self.formGroupBox = QGroupBox("Properties")
        self.keyName = QLabel()
        self.size = QLabel()
        self.eTag = QLabel()
        self.storageClass = QLabel()
        self.restoreStatus = QLabel()
        self.publicUrl = QLineEdit()
        self.publicUrl.setReadOnly(True)

        self.createForm()
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(self.formGroupBox)
        mainLayout.addWidget(self.buttonBox)
        self.setLayout(mainLayout)
        btn_apply = self.buttonBox.button(QDialogButtonBox.StandardButton.Ok)
        btn_apply.clicked.connect(self.exit)

        # defaults
        display_key = key if key else "<bucket root>"
        display_size = "N/A"
        display_etag = ""
        display_storage_class = ""
        display_restore = ""
        display_public_url = ""

        is_folder = bool(key) and key.endswith("/")

        # ETag/StorageClass/Restore come from head_object, which only exists
        # for real objects. Folders (prefixes) and implicit folders have no
        # object to HEAD, so don't let a 404 there abort the rest.
        if not is_folder:
            try:
                resp = self.model.object_properties(key)
                display_etag = (resp.get("ETag") or "").replace('"', "")
                # StorageClass is omitted by S3 for the default STANDARD tier.
                display_storage_class = resp.get("StorageClass") or "STANDARD"
                display_restore = self.model.parse_restore_status(
                    resp.get("Restore"))
                # head_object already carries the exact byte count, so a single
                # object needs no extra listing call at all.
                content_length = resp.get("ContentLength")
                if content_length is not None:
                    display_size = f"{int(content_length)} Bytes"
            except Exception:
                pass

        try:
            if not key:
                # bucket root URL — respect virtual-host endpoints
                ep = self.model.endpoint_url.rstrip("/")
                b = (self.model.bucket or "").strip("/")
                if b and self.model._endpoint_has_bucket(ep, b):
                    display_public_url = f"{ep}/"
                else:
                    display_public_url = f"{ep}/{b}/" if b else ep
            else:
                display_public_url = self.model.direct_object_url(key)
        except Exception:
            pass

        self.keyName.setText(display_key)
        self.size.setText(display_size)
        self.eTag.setText(display_etag)
        self.storageClass.setText(display_storage_class)
        self.restoreStatus.setText(display_restore or "—")
        self.publicUrl.setText(display_public_url)

        # A prefix has no single size to HEAD, so it needs a recursive listing.
        # That can span many pages, so never run it on the main thread.
        if display_size == "N/A":
            self._start_size_calc()

    def _start_size_calc(self):
        self.size.setText("Calculating…")
        try:
            worker_model = self.model.clone_for_worker()
        except Exception:
            worker_model = self.model
        self._size_thread = QThread(self)
        self._size_worker = _SizeWorker(worker_model, self.key)
        self._size_worker.moveToThread(self._size_thread)
        self._size_thread.started.connect(self._size_worker.run)
        self._size_worker.done.connect(self._on_size)
        self._size_worker.done.connect(self._size_worker.deleteLater)
        self._size_thread.finished.connect(self._size_thread.deleteLater)
        self._size_thread.start()

    def _on_size(self, size, exc):
        self._stop_size_thread()
        if exc is not None or size is None:
            self.size.setText("N/A")
            return
        self.size.setText(f"{int(size)} Bytes")

    def _stop_size_thread(self):
        th, self._size_thread, self._size_worker = self._size_thread, None, None
        if th is None:
            return
        try:
            if th.isRunning():
                th.quit()
                th.wait(2000)
        except RuntimeError:
            pass

    def closeEvent(self, event):
        # The worker thread is parented to this dialog; closing while it runs
        # would destroy a running QThread and abort the process.
        self._stop_size_thread()
        super().closeEvent(event)

    def showEvent(self, event):
        super().showEvent(event)
        center_on_screen(self)

    def exit(self):
        self.close()

    def exec(self):
        super().exec()

    def createForm(self):
        layout = QFormLayout()
        layout.addRow(QLabel("Key / Bucket"), self.keyName)
        layout.addRow(QLabel("Size"), self.size)
        layout.addRow(QLabel("ETag"), self.eTag)
        layout.addRow(QLabel("Storage class"), self.storageClass)
        layout.addRow(QLabel("Restore"), self.restoreStatus)
        layout.addRow(QLabel("Public URL"), self.publicUrl)
        self.formGroupBox.setLayout(layout)
