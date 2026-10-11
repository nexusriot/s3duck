"""Object tags, and the metadata beside them.
"""

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QDialog, QDialogButtonBox, QFormLayout,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMessageBox, QPushButton,
    QTableWidget, QTableWidgetItem, QVBoxLayout,
)


class TagsDialog(QDialog):
    def __init__(self, parent, model, key: str):
        super().__init__(parent)
        short_name = key.split("/")[-1] or key
        self.setWindowTitle(f"Tags — {short_name}")
        self.setMinimumSize(500, 320)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self._model = model
        self._key = key

        key_lbl = QLabel(f"Object:  {key}")
        key_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Tag key", "Tag value"])
        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().hide()
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        add_btn = QPushButton("Add tag")
        add_btn.clicked.connect(self._add_row)
        rem_btn = QPushButton("Remove selected")
        rem_btn.clicked.connect(self._remove_row)

        btn_row = QHBoxLayout()
        btn_row.addWidget(add_btn)
        btn_row.addWidget(rem_btn)
        btn_row.addStretch()

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self._save)
        btns.rejected.connect(self.reject)
        self._btns = btns

        lay = QVBoxLayout()
        lay.addWidget(key_lbl)
        lay.addWidget(self._table)
        lay.addLayout(btn_row)
        lay.addWidget(btns)
        self.setLayout(lay)

        self._load_tags()

    def _load_tags(self):
        try:
            tags = self._model.get_object_tags(self._key)
        except Exception as exc:
            # Saving now would replace the object's real tags with the empty
            # table, so block it.
            save_btn = self._btns.button(QDialogButtonBox.StandardButton.Save)
            if save_btn is not None:
                save_btn.setEnabled(False)
            QMessageBox.warning(self, "Tags", f"Could not load tags:\n{exc}")
            return
        for tag in tags:
            self._insert_row(tag.get("Key", ""), tag.get("Value", ""))

    def _insert_row(self, k: str = "", v: str = ""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(k))
        self._table.setItem(r, 1, QTableWidgetItem(v))

    def _add_row(self):
        self._insert_row()
        r = self._table.rowCount() - 1
        self._table.scrollToItem(self._table.item(r, 0))
        self._table.editItem(self._table.item(r, 0))

    def _remove_row(self):
        rows = sorted({idx.row() for idx in self._table.selectedIndexes()}, reverse=True)
        for r in rows:
            self._table.removeRow(r)

    def _save(self):
        tags = []
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            k = (k_item.text() if k_item else "").strip()
            v = (v_item.text() if v_item else "").strip()
            if k:
                tags.append({"Key": k, "Value": v})
        try:
            self._model.put_object_tags(self._key, tags)
            self.accept()
        except Exception as exc:
            QMessageBox.critical(self, "Tags", f"Could not save tags:\n{exc}")


class MetadataDialog(QDialog):
    """Edit an object's system headers (Content-Type, Cache-Control, …) and
    custom x-amz-meta-* user metadata."""

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._storage_class = "STANDARD"

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Metadata — {base}")
        self.resize(560, 480)

        self._content_type = QLineEdit()
        self._cache_control = QLineEdit()
        self._content_disposition = QLineEdit()
        self._content_encoding = QLineEdit()

        form = QFormLayout()
        form.addRow(QLabel("Content-Type"), self._content_type)
        form.addRow(QLabel("Cache-Control"), self._cache_control)
        form.addRow(QLabel("Content-Disposition"), self._content_disposition)
        form.addRow(QLabel("Content-Encoding"), self._content_encoding)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Metadata key", "Value"])
        self._table.horizontalHeader().setStretchLastSection(True)

        add_btn = QPushButton("Add")
        del_btn = QPushButton("Remove")
        add_btn.clicked.connect(lambda: self._insert_row("", ""))
        del_btn.clicked.connect(self._remove_row)
        row_btns = QHBoxLayout()
        row_btns.addWidget(add_btn)
        row_btns.addWidget(del_btn)
        row_btns.addStretch(1)

        self.buttonBox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttonBox.accepted.connect(self._save)
        self.buttonBox.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(QLabel("Custom metadata (x-amz-meta-*):"))
        lay.addWidget(self._table, 1)
        lay.addLayout(row_btns)
        lay.addWidget(self.buttonBox)

        self._load()

    def _insert_row(self, k="", v=""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(k))
        self._table.setItem(r, 1, QTableWidgetItem(v))

    def _remove_row(self):
        r = self._table.currentRow()
        if r >= 0:
            self._table.removeRow(r)

    def _load(self):
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            meta = self._model.get_object_metadata(self._key)
        except Exception as exc:
            QApplication.restoreOverrideCursor()
            # Saving now would wipe the object's real metadata with the empty
            # form, so block it.
            save_btn = self.buttonBox.button(QDialogButtonBox.StandardButton.Save)
            if save_btn is not None:
                save_btn.setEnabled(False)
            QMessageBox.warning(self, "Metadata", f"Could not load metadata:\n{exc}")
            return
        QApplication.restoreOverrideCursor()
        self._storage_class = meta.get("storage_class") or "STANDARD"
        self._content_type.setText(meta.get("content_type", ""))
        self._cache_control.setText(meta.get("cache_control", ""))
        self._content_disposition.setText(meta.get("content_disposition", ""))
        self._content_encoding.setText(meta.get("content_encoding", ""))
        for k, v in (meta.get("metadata") or {}).items():
            self._insert_row(str(k), str(v))

    def _save(self):
        metadata = {}
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            k = (k_item.text() if k_item else "").strip()
            v = (v_item.text() if v_item else "").strip()
            if k:
                metadata[k] = v
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            self._model.set_object_metadata(
                self._key,
                content_type=self._content_type.text().strip(),
                cache_control=self._cache_control.text().strip(),
                content_disposition=self._content_disposition.text().strip(),
                content_encoding=self._content_encoding.text().strip(),
                metadata=metadata,
                storage_class=self._storage_class,
            )
        except Exception as exc:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Metadata", f"Could not save metadata:\n{exc}")
            return
        QApplication.restoreOverrideCursor()
        self.accept()
