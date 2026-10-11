"""Operations over a whole selection: rename and retag.
"""

import re
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QDialog, QDialogButtonBox, QFormLayout,
    QGroupBox, QHBoxLayout, QHeaderView, QLabel, QLineEdit, QPushButton,
    QRadioButton, QSpinBox, QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.bulk_rename import (
    BULK_RENAME_FIND, BULK_RENAME_TEMPLATE, bulk_rename_plan,
)


class BulkRenameDialog(QDialog):
    """Rename a whole selection by find-and-replace or a numbering template,
    with a live preview of the resulting names."""

    def __init__(self, parent, items):
        super().__init__(parent)
        self.setWindowTitle("Rename multiple")
        self.resize(660, 520)
        self._items = list(items)   # [(name, is_folder)]
        self._plan = []

        self._mode_find = QRadioButton("Find and replace")
        self._mode_template = QRadioButton("Numbering template")
        self._mode_find.setChecked(True)
        self._mode_find.toggled.connect(self._refresh)

        self._find = QLineEdit()
        self._find.setPlaceholderText("text to find")
        self._replace = QLineEdit()
        self._replace.setPlaceholderText("replacement (may be empty)")
        self._regex = QCheckBox("Regular expression (\\1 backreferences)")
        self._case = QCheckBox("Case sensitive")
        self._case.setChecked(True)

        self._template = QLineEdit("{name}{ext}")
        self._start = QSpinBox()
        self._start.setRange(0, 1000000)
        self._start.setValue(1)
        self._padding = QSpinBox()
        self._padding.setRange(1, 9)
        self._padding.setValue(1)

        for widget in (self._find, self._replace, self._template):
            widget.textChanged.connect(self._refresh)
        for widget in (self._regex, self._case):
            widget.toggled.connect(self._refresh)
        for widget in (self._start, self._padding):
            widget.valueChanged.connect(self._refresh)

        find_form = QFormLayout()
        find_form.addRow(QLabel("Find"), self._find)
        find_form.addRow(QLabel("Replace with"), self._replace)
        find_form.addRow(self._regex)
        find_form.addRow(self._case)
        self._find_box = QGroupBox()
        self._find_box.setLayout(find_form)

        tpl_form = QFormLayout()
        tpl_form.addRow(QLabel("Template"), self._template)
        tpl_form.addRow(QLabel("Start at"), self._start)
        tpl_form.addRow(QLabel("Digits"), self._padding)
        tpl_form.addRow(QLabel(
            "Placeholders: {name} {ext} {n} {orig}"))
        self._tpl_box = QGroupBox()
        self._tpl_box.setLayout(tpl_form)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Current name", "New name"])
        self._table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        self._btns.accepted.connect(self.accept)
        self._btns.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        mode_row = QHBoxLayout()
        mode_row.addWidget(self._mode_find)
        mode_row.addWidget(self._mode_template)
        mode_row.addStretch(1)
        lay.addLayout(mode_row)
        lay.addWidget(self._find_box)
        lay.addWidget(self._tpl_box)
        lay.addWidget(QLabel("Preview:"))
        lay.addWidget(self._table, 1)
        lay.addWidget(self._info)
        lay.addWidget(self._btns)

        self._refresh()

    def _mode(self):
        return BULK_RENAME_FIND if self._mode_find.isChecked() else BULK_RENAME_TEMPLATE

    def _refresh(self):
        is_find = self._mode() == BULK_RENAME_FIND
        self._find_box.setVisible(is_find)
        self._tpl_box.setVisible(not is_find)

        plan, problems = bulk_rename_plan(
            self._items,
            mode=self._mode(),
            find=self._find.text(),
            replace=self._replace.text(),
            regex=self._regex.isChecked(),
            case_sensitive=self._case.isChecked(),
            template=self._template.text(),
            start=self._start.value(),
            padding=self._padding.value(),
        )
        self._plan = plan

        self._table.setRowCount(0)
        for old, new in plan:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(old))
            self._table.setItem(r, 1, QTableWidgetItem(new))

        if problems:
            self._info.setText(
                "<b>Cannot apply:</b><br>" + "<br>".join(problems[:10]))
        elif plan:
            self._info.setText(f"{len(plan)} of {len(self._items)} will be renamed.")
        else:
            self._info.setText("No names would change.")

        ok = self._btns.button(QDialogButtonBox.StandardButton.Ok)
        if ok is not None:
            ok.setEnabled(bool(plan) and not problems)

    def plan(self):
        return list(self._plan)


class BulkTagsDialog(QDialog):
    """Add, overwrite or strip tags across a whole selection."""

    def __init__(self, parent, target_count):
        super().__init__(parent)
        self.setWindowTitle("Edit tags on selection")
        self.setMinimumWidth(520)

        head = QLabel(
            f"Applying to <b>{target_count} selected item(s)</b>. Folders are "
            "expanded to every object beneath them."
        )
        head.setWordWrap(True)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Tag key", "Tag value"])
        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().hide()

        add_btn = QPushButton("Add row")
        del_btn = QPushButton("Remove row")
        add_btn.clicked.connect(lambda: self._insert_row())
        del_btn.clicked.connect(self._remove_row)
        row_btns = QHBoxLayout()
        row_btns.addWidget(add_btn)
        row_btns.addWidget(del_btn)
        row_btns.addStretch(1)

        self._remove_keys = QLineEdit()
        self._remove_keys.setPlaceholderText(
            "Tag keys to delete, comma separated")

        self._replace = QCheckBox(
            "Replace all existing tags (anything not listed above is removed)")

        self.buttonBox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addWidget(head)
        lay.addWidget(QLabel("Tags to set:"))
        lay.addWidget(self._table, 1)
        lay.addLayout(row_btns)
        lay.addWidget(QLabel("Tags to remove:"))
        lay.addWidget(self._remove_keys)
        lay.addWidget(self._replace)
        lay.addWidget(self.buttonBox)

        self._insert_row()

    def _insert_row(self, key="", value=""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(key))
        self._table.setItem(r, 1, QTableWidgetItem(value))

    def _remove_row(self):
        r = self._table.currentRow()
        if r >= 0:
            self._table.removeRow(r)

    def tags_to_add(self) -> dict:
        out = {}
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            key = (k_item.text() if k_item else "").strip()
            if key:
                out[key] = (v_item.text() if v_item else "").strip()
        return out

    def tags_to_remove(self) -> list:
        return [k for k in re.split(r"[,\s]+", self._remove_keys.text() or "")
                if k]

    def replace_all(self) -> bool:
        return self._replace.isChecked()

    def is_noop(self) -> bool:
        return not self.tags_to_add() and not self.tags_to_remove() \
            and not self.replace_all()
