"""In-app preview and in-place editing of an object's body.
"""

import os
import threading
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, QThread, QUrl, Qt
from PyQt6.QtGui import QDesktopServices, QFontDatabase, QPixmap
from PyQt6.QtWidgets import (
    QDialog, QHBoxLayout, QLabel, QMessageBox, QPlainTextEdit,
    QProgressDialog, QPushButton, QScrollArea, QStackedWidget, QVBoxLayout,
)
from core.formatting import hex_dump
from model import Model as DataModel
from ui.widgets.highlighters import CodeHighlighter
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)

try:
    from PyQt6.QtPdf import QPdfDocument
    from PyQt6.QtPdfWidgets import QPdfView
except ImportError:
    QPdfDocument = None
    QPdfView = None


class PreviewDialog(DialogDismissMixin, QDialog):
    """Inline preview for a single object: images and text render in-app;
    anything else can be opened with the OS default application."""

    IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".ico"}
    TEXT_EXTS = {
        ".txt", ".md", ".log", ".json", ".xml", ".yaml", ".yml", ".csv",
        ".tsv", ".ini", ".cfg", ".conf", ".py", ".js", ".ts", ".html",
        ".htm", ".css", ".sh", ".c", ".h", ".cpp", ".go", ".rs", ".java",
        ".sql", ".toml", ".svg",
    }
    CODE_EXTS = {
        ".py", ".js", ".ts", ".c", ".h", ".cpp", ".go", ".rs", ".java",
        ".sh", ".sql", ".css", ".html", ".htm", ".xml", ".json", ".yaml",
        ".yml", ".toml", ".ini", ".cfg", ".conf",
    }
    PDF_EXTS = {".pdf"}
    IMG_LIMIT = 25 * 1024 * 1024
    TEXT_LIMIT = 1 * 1024 * 1024
    PDF_LIMIT = 50 * 1024 * 1024
    # Characters the text editor does not hand back as it was given them:
    # QPlainTextEdit turns CR and CRLF into LF, NBSP into a space, the two
    # Unicode separators (and Qt's own frame markers) into LF, and drops a
    # leading byte-order mark. Saving would rewrite every one of them across
    # the whole file, whatever the user actually changed.
    UNFAITHFUL_CHARS = (
        ("\r", "Windows or classic-Mac line endings"),
        ("\u00a0", "non-breaking spaces"),
        ("\u2028", "Unicode line separators"),
        ("\u2029", "Unicode paragraph separators"),
        ("\ufdd0", "U+FDD0"),
        ("\ufdd1", "U+FDD1"),
    )

    def __init__(self, parent, model, key):
        super().__init__(parent)
        # _open_external stages the download in the window's temp workspace;
        # without this it raised AttributeError from inside a Qt slot.
        self._mw = parent
        self._model = model
        self._key = key
        self._thread = None
        self._worker = None
        self._dl_thread = None
        self._dl_worker = None
        self._save_thread = None
        self._save_worker = None
        self._etag = ""
        self._editable = False
        self._editing = False

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Preview — {base}")
        self.resize(760, 620)

        self._stack = QStackedWidget()

        self._status = QLabel("Loading preview…")
        self._status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._status.setWordWrap(True)

        self._img_label = QLabel()
        self._img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._img_scroll = QScrollArea()
        self._img_scroll.setWidget(self._img_label)
        self._img_scroll.setWidgetResizable(True)

        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._text.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont)
            )
        except Exception:
            pass

        self._hex = QPlainTextEdit()
        self._hex.setReadOnly(True)
        self._hex.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._hex.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        except Exception:
            pass

        self._highlighter = None
        self._pdf_doc = None
        self._pdf_view = None

        self._stack.addWidget(self._status)      # index 0
        self._stack.addWidget(self._img_scroll)  # index 1
        self._stack.addWidget(self._text)        # index 2
        self._stack.addWidget(self._hex)         # index 3
        if QPdfView is not None:
            self._pdf_doc = QPdfDocument(self)
            self._pdf_view = QPdfView(self)
            self._pdf_view.setDocument(self._pdf_doc)
            self._pdf_view.setPageMode(QPdfView.PageMode.MultiPage)
            self._stack.addWidget(self._pdf_view)   # index 4

        self._open_btn = QPushButton("Open with default app")
        self._open_btn.clicked.connect(self._open_external)
        self._edit_btn = QPushButton("Edit")
        self._edit_btn.clicked.connect(self._toggle_edit)
        self._edit_btn.hide()
        self._save_btn = QPushButton("Save")
        self._save_btn.clicked.connect(self._save)
        self._save_btn.hide()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)

        btns = QHBoxLayout()
        btns.addWidget(self._open_btn)
        btns.addWidget(self._edit_btn)
        btns.addWidget(self._save_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._stack, 1)
        lay.addLayout(btns)

        self._start_load()

    def _ext(self):
        return os.path.splitext(self._key)[1].lower()

    @classmethod
    def unfaithful_characters(cls, text) -> list:
        """What in *text* the editor would silently change on save."""
        found = [label for char, label in cls.UNFAITHFUL_CHARS
                 if char in (text or "")]
        if (text or "").startswith("\ufeff"):
            found.append("a byte-order mark")
        return found

    def _start_load(self):
        ext = self._ext()
        if ext in self.IMAGE_EXTS:
            max_bytes = self.IMG_LIMIT
        elif ext in self.PDF_EXTS and QPdfView is not None:
            # A PDF cannot be rendered from a partial file.
            max_bytes = self.PDF_LIMIT
        else:
            max_bytes = self.TEXT_LIMIT
        key = self._key
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return clone.get_object_preview(key, max_bytes)

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def confirm_dismiss(self) -> bool:
        # Escape reaches this now too. It used to walk straight past the
        # prompt and throw the edits away without asking.
        if self._editing and self._text.document().isModified():
            answer = QMessageBox.question(
                self, "Preview",
                "Discard the unsaved changes to this object?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if answer != QMessageBox.StandardButton.Yes:
                return False
        return True

    def stop_threads(self):
        self._stop_load_thread()
        join_qthread(self._save_thread)
        self._save_thread = None
        self._save_worker = None

    def _stop_load_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_loaded(self, result, exc):
        self._stop_load_thread()
        if exc is not None:
            self._status.setText(f"Could not load preview:\n{exc}")
            self._stack.setCurrentIndex(0)
            return

        data = result.get("data") or b""
        ctype = (result.get("content_type") or "").lower()
        truncated = result.get("truncated")
        self._etag = str(result.get("etag") or "")
        ext = self._ext()

        # Raster images render as a pixmap (SVG is XML, shown as text below).
        is_raster = (ext in self.IMAGE_EXTS) or (
            ctype.startswith("image/") and "svg" not in ctype
        )
        if is_raster and ext != ".svg":
            pix = QPixmap()
            if pix.loadFromData(data):
                self._img_label.setPixmap(pix)
                self._stack.setCurrentIndex(1)
                return
            # not a decodable raster -> fall through to text/binary handling

        if ext in self.PDF_EXTS or ctype == "application/pdf":
            if self._pdf_view is not None and not truncated:
                self._pdf_bytes = QByteArray(data)
                self._pdf_buffer = QBuffer(self._pdf_bytes, self)
                self._pdf_buffer.open(QIODevice.OpenModeFlag.ReadOnly)
                self._pdf_doc.load(self._pdf_buffer)
                if self._pdf_doc.status() == QPdfDocument.Status.Ready:
                    self._stack.setCurrentIndex(4)
                    return
            self._status.setText(
                "Could not render this PDF inline"
                + (" (file is larger than the preview limit)." if truncated
                   else ".")
                + '\n\nUse "Open with default app".'
            )
            self._stack.setCurrentIndex(0)
            return

        if b"\x00" in data[:8192]:
            # Binary: show a hex dump rather than refusing outright.
            self._hex.setPlainText(hex_dump(data))
            self._stack.setCurrentIndex(3)
            return

        text = data.decode("utf-8", errors="replace")
        if truncated:
            text += "\n\n… (preview truncated)"
        self._text.setPlainText(text)
        if ext in self.CODE_EXTS and self._highlighter is None:
            self._highlighter = CodeHighlighter(self._text.document())
        self._stack.setCurrentIndex(2)
        # Editing is offered only for text that arrived whole and decoded
        # cleanly: saving a truncated body would silently delete the tail, and
        # saving a replacement-charactered decode would corrupt the file.
        # Nor for text the editor cannot hand back unchanged — a CRLF file
        # came back LF throughout, its BOM gone, from a one-word edit.
        read_only = getattr(self._model, "read_only", False)
        unfaithful = self.unfaithful_characters(text)
        self._editable = (
            not truncated
            and not read_only
            and "\ufffd" not in text
            and not unfaithful)
        self._edit_btn.setVisible(
            self._editable or bool(unfaithful and not truncated and not read_only))
        self._edit_btn.setEnabled(self._editable)
        if truncated:
            self._edit_btn.setToolTip(
                "Too large to edit in-app; the preview is truncated")
        elif read_only:
            self._edit_btn.setToolTip("Profile is read-only")
        elif unfaithful:
            self._edit_btn.setToolTip(
                "Not editable here: this file contains "
                + ", ".join(unfaithful)
                + ", which saving from this editor would rewrite throughout "
                "the file. Use \"Open with default app\" instead.")
        else:
            self._edit_btn.setToolTip("")

    def _toggle_edit(self):
        if not self._editable:
            return
        self._editing = not self._editing
        self._text.setReadOnly(not self._editing)
        self._edit_btn.setText("Cancel edit" if self._editing else "Edit")
        self._save_btn.setVisible(self._editing)
        if self._editing:
            self._text.setFocus(Qt.FocusReason.OtherFocusReason)
        else:
            # Cancelling reloads rather than keeping the edited buffer around
            # looking authoritative.
            self._start_load()

    def _save(self):
        if self._save_thread is not None:
            return
        payload = self._text.toPlainText().encode("utf-8")
        key = self._key
        etag = self._etag
        clone = self._model.clone_for_worker()

        def _write(_w):
            # The object's own headers, metadata, class, encryption and tags
            # go back with the new body; a bare PUT would have reset them all.
            return clone.put_object_body(key, payload, if_match=etag,
                                         preserve_properties=True)

        self._save_btn.setEnabled(False)
        self._save_thread = QThread(self)
        self._save_worker = FuncWorker(_write)
        self._save_worker.moveToThread(self._save_thread)
        self._save_thread.started.connect(self._save_worker.run)
        self._save_worker.done.connect(self._on_saved)
        self._save_worker.done.connect(self._save_thread.quit)
        release_worker_on_finish(self._save_thread, self._save_worker)
        self._save_thread.finished.connect(self._save_thread.deleteLater)
        self._save_thread.start()

    def _on_saved(self, result, exc):
        th, self._save_thread, self._save_worker = self._save_thread, None, None
        join_qthread(th)
        self._save_btn.setEnabled(True)
        if exc is not None:
            if isinstance(exc, DataModel.PreconditionFailed):
                QMessageBox.warning(
                    self, "Save",
                    f"{exc}\n\nNothing was written. Close and reopen the "
                    "preview to see the current contents.")
            else:
                QMessageBox.warning(self, "Save", f"Could not save:\n{exc}")
            return
        self._etag = str(result or "")
        self._editing = False
        self._text.setReadOnly(True)
        self._edit_btn.setText("Edit")
        self._save_btn.hide()
        if getattr(self._mw, "statusBar", None) is not None:
            self._mw.statusBar().showMessage("Saved", 3000)

    def _open_external(self):
        if self._dl_thread is not None:
            return
        base = os.path.basename(self._key.rstrip("/")) or "object"
        tmp_dir = self._mw.temp_workspace.make(prefix="preview_")
        out_path = os.path.join(tmp_dir, base)
        key = self._key
        clone = self._model.clone_for_worker()
        cancel = threading.Event()

        prog = QProgressDialog("Downloading…", "Cancel", 0, 100, self)
        prog.setWindowTitle("Open with default app")
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.setAutoClose(False)
        prog.setAutoReset(False)
        prog.canceled.connect(cancel.set)

        def _dl(w):
            def _cb(total, cur, _k):
                w.progress.emit(int(cur), int(total or 0))
            clone.download_file(
                key, out_path, tmp_dir, progress_cb=_cb, cancel_event=cancel
            )
            return out_path

        self._dl_thread = QThread(self)
        self._dl_worker = FuncWorker(_dl)
        self._dl_worker.moveToThread(self._dl_thread)
        self._dl_thread.started.connect(self._dl_worker.run)

        def _on_prog(cur, total):
            if total > 0:
                prog.setMaximum(100)
                prog.setValue(min(100, int(cur * 100 / total)))
            else:
                prog.setMaximum(0)  # indeterminate

        def _on_done(result, exc):
            prog.close()
            self._dl_thread.quit()
            self._dl_thread = None
            self._dl_worker = None
            self._open_btn.setEnabled(True)
            if exc is not None:
                if not cancel.is_set():
                    QMessageBox.warning(
                        self, "Open", f"Could not download file:\n{exc}"
                    )
                return
            QDesktopServices.openUrl(QUrl.fromLocalFile(result))

        self._dl_worker.progress.connect(_on_prog)
        self._dl_worker.done.connect(_on_done)
        release_worker_on_finish(self._dl_thread, self._dl_worker)
        self._dl_thread.finished.connect(self._dl_thread.deleteLater)
        self._open_btn.setEnabled(False)
        self._dl_thread.start()
        prog.show()
