"""The other half of the dual pane -- a local folder, a remote prefix in this
account, or a prefix in another profile's account.
"""

import os
import shutil
import time
from PyQt6.QtCore import QFile, QThread, QUrl, Qt
from PyQt6.QtGui import QDesktopServices, QKeySequence, QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QComboBox, QDialog, QDialogButtonBox,
    QFileDialog, QHBoxLayout, QInputDialog, QLabel, QLineEdit, QMenu,
    QMessageBox, QPushButton, QTreeView, QVBoxLayout, QWidget,
)
from core.local_files import NAME, child_path, sort_rows, validate_new_name
from core.locations import parse_s3_location
from core.naming import _human_bytes, as_epoch
from model import FSObjectType
from ui.widgets.profile_picker import ProfilePicker, build_profile_model
from utils import FuncWorker, join_qthread, release_worker_on_finish


class PaneProfileDialog(QDialog):
    """Pick the profile, bucket and prefix the second pane shows."""

    def __init__(self, parent, settings, current_profile=""):
        super().__init__(parent)
        self.setWindowTitle("Second pane: another profile")
        self.setMinimumWidth(460)
        self.picker = ProfilePicker(self, settings, current_profile)
        self.picker.note.setText(
            "Copies between the panes stream through this machine, because "
            "no server-side copy can use two sets of credentials.")
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        lay = QVBoxLayout(self)
        lay.addWidget(self.picker)
        lay.addWidget(self.picker.note)
        lay.addWidget(buttons)
        if not self.picker.has_profiles():
            self.picker.note.setText("No other profile is configured.")
            buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(False)

    def closeEvent(self, event):
        self.picker.stop_loader()
        super().closeEvent(event)


class SecondPane(QWidget):
    """
    The other half of a two-pane view.

    Deliberately its own small model rather than a second MainWindow: every
    transfer the pane takes part in is executed by the window's existing
    queue. The local side is a file manager in its own right -- sorting,
    rename, new folder, delete to the trash -- because a folder you cannot
    tidy is a folder you leave the app to manage.
    """

    LOCAL = "local"
    REMOTE = "remote"
    OTHER = "other"
    COLUMNS = ("Name", "Size", "Modified")

    def __init__(self, parent, main_window, model):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._thread = None
        self._worker = None
        self._entries = []
        self._modified = {}
        self._sort = (NAME, False)
        self._bucket = model.bucket or ""
        self._prefix = model.current_folder or ""
        self._local_dir = os.path.expanduser("~")
        # Another profile's connection, when the pane shows that account.
        self._other_model = None
        self._other_profile = ""
        self._other_bucket = ""
        self._other_prefix = ""
        self._last_mode_index = 0
        self._scan_error = ""
        # What the listing in flight was asked for, and whether another
        # refresh arrived while it ran.
        self._listing_for = None
        self._refresh_again = False

        self.mode = QComboBox()
        self.mode.addItem("Local folder", self.LOCAL)
        self.mode.addItem("Remote prefix", self.REMOTE)
        self.mode.addItem("Other profile…", self.OTHER)
        self.mode.currentIndexChanged.connect(self._on_mode_changed)

        self.path = QLineEdit(self._local_dir)
        self.path.returnPressed.connect(self._go_typed)
        self.up_btn = QPushButton("↑")
        self.up_btn.setFixedWidth(28)
        self.up_btn.setToolTip("Up one level")
        self.up_btn.clicked.connect(self.go_up)
        self.browse_btn = QPushButton("…")
        self.browse_btn.setFixedWidth(28)
        self.browse_btn.clicked.connect(self._browse)

        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.addWidget(self.mode)
        top.addWidget(self.up_btn)
        top.addWidget(self.path, 1)
        top.addWidget(self.browse_btn)

        self.view = QTreeView()
        self.view.setRootIsDecorated(False)
        self.view.setUniformRowHeights(True)
        # Sorting is done here, not by the view: folders stay on top in
        # either direction, and selected() maps rows straight to entries.
        self.view.setSortingEnabled(False)
        self.view.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self.view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.view.doubleClicked.connect(self._on_activated)
        self.view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.view.customContextMenuRequested.connect(self._context_menu)
        self.view.installEventFilter(self)
        header = self.view.header()
        header.setSectionsClickable(True)
        header.setSortIndicatorShown(True)
        header.setSortIndicator(NAME, Qt.SortOrder.AscendingOrder)
        header.sectionClicked.connect(self._on_header_clicked)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(list(self.COLUMNS))
        self.view.setModel(self.model)

        self.status = QLabel("")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        layout.addLayout(top)
        layout.addWidget(self.view, 1)
        layout.addWidget(self.status)

    def current_mode(self) -> str:
        return self.mode.currentData() or self.LOCAL

    def is_remote(self) -> bool:
        return self.current_mode() in (self.REMOTE, self.OTHER)

    def location(self) -> str:
        if self.current_mode() == self.LOCAL:
            return self._local_dir
        bucket, prefix = self.remote_target()
        if self.current_mode() == self.OTHER:
            return f"[{self._other_profile}] s3://{bucket}/{prefix}"
        return f"s3://{bucket}/{prefix}"

    def _set_mode(self, mode):
        """Switch mode without the profile prompt; refreshes either way."""
        index = self.mode.findData(mode)
        if self.mode.currentIndex() != index:
            self.mode.blockSignals(True)
            self.mode.setCurrentIndex(index)
            self.mode.blockSignals(False)
        self._last_mode_index = index
        self.refresh()

    def set_remote(self, bucket, prefix):
        """Point the pane at a remote prefix in this account and show it."""
        self._bucket = bucket or ""
        self._prefix = prefix or ""
        self._set_mode(self.REMOTE)

    def set_local(self, path):
        self._local_dir = os.path.abspath(path or os.path.expanduser("~"))
        self._set_mode(self.LOCAL)

    def set_other_profile(self, profile_name, model, bucket, prefix=""):
        """Point the pane at a prefix in another profile's account."""
        self._other_profile = str(profile_name or "")
        self._other_model = model
        self._other_bucket = bucket or ""
        self._other_prefix = prefix or ""
        if model is not None:
            model.bucket = self._other_bucket
        self._set_mode(self.OTHER)

    def other_profile(self) -> str:
        """The other profile's name while the pane shows it, else ""."""
        return self._other_profile if self.current_mode() == self.OTHER else ""

    def other_model(self):
        """A model bound to the other profile's bucket, or None."""
        if self.current_mode() != self.OTHER or self._other_model is None:
            return None
        clone = self._other_model.clone_for_worker()
        clone.bucket = self._other_bucket
        clone.current_folder = self._other_prefix
        return clone

    def remote_target(self) -> tuple:
        if self.current_mode() == self.OTHER:
            return self._other_bucket, self._other_prefix
        return self._bucket, self._prefix

    def _set_remote_target(self, bucket, prefix):
        if self.current_mode() == self.OTHER:
            self._other_bucket, self._other_prefix = bucket, prefix
            if self._other_model is not None:
                self._other_model.bucket = bucket
        else:
            self._bucket, self._prefix = bucket, prefix
        self.refresh()

    def local_target(self) -> str:
        return self._local_dir

    def entries(self) -> list:
        """``[(name, is_dir, size)]`` for what is currently listed."""
        return list(self._entries)

    def sort_state(self) -> tuple:
        return self._sort

    def selected(self) -> list:
        """The selected rows as ``(name, is_dir, size)``."""
        rows = sorted({index.row()
                       for index in self.view.selectionModel().selectedIndexes()})
        return [self._entries[row] for row in rows
                if 0 <= row < len(self._entries)]

    def select_names(self, names):
        """Select the listed rows called ``names`` (for tests and after a rename)."""
        wanted = set(names)
        selection = self.view.selectionModel()
        selection.clearSelection()
        for row, (name, _d, _s) in enumerate(self._entries):
            if name in wanted:
                selection.select(
                    self.model.index(row, 0),
                    selection.SelectionFlag.Select
                    | selection.SelectionFlag.Rows)

    def _on_mode_changed(self, index):
        if self.mode.itemData(index) == self.OTHER and self._other_model is None:
            if not self.choose_other_profile():
                # Cancelled: go back to whatever the pane was showing.
                self.mode.blockSignals(True)
                self.mode.setCurrentIndex(self._last_mode_index)
                self.mode.blockSignals(False)
                return
            return
        self._last_mode_index = index
        self.refresh()

    def choose_other_profile(self) -> bool:
        """Ask which profile, bucket and prefix to show; True if one was chosen."""
        dlg = PaneProfileDialog(self, self._mw.settings,
                                current_profile=self._mw.profile_name)
        accepted = dlg.exec() == QDialog.DialogCode.Accepted
        dlg.picker.stop_loader()
        if not accepted:
            return False
        bucket = dlg.picker.destination_bucket()
        if not bucket:
            QMessageBox.warning(self, "Second pane", "Choose a bucket.")
            return False
        try:
            profile = dlg.picker.selected_profile()
        except Exception as exc:
            QMessageBox.critical(
                self, "Second pane",
                f"Could not read that profile's credentials:\n{exc}")
            return False
        if profile is None:
            return False
        self.set_other_profile(profile.name, build_profile_model(profile, bucket),
                               bucket, dlg.picker.destination_prefix())
        return True

    def _browse(self):
        if self.current_mode() == self.LOCAL:
            path = QFileDialog.getExistingDirectory(
                self, "Folder", self._local_dir)
            if path:
                self.set_local(path)
            return
        if self.current_mode() == self.OTHER:
            self.choose_other_profile()
            return
        text, ok = QInputDialog.getText(
            self, "Remote prefix", "s3://bucket/prefix:",
            text=f"s3://{self._bucket}/{self._prefix}")
        if not ok:
            return
        bucket, prefix = parse_s3_location(text, "")
        if bucket:
            self.set_remote(bucket, prefix)

    def _go_typed(self):
        text = self.path.text().strip()
        if self.current_mode() == self.LOCAL:
            if os.path.isdir(text):
                self.set_local(text)
            return
        current_bucket, _ = self.remote_target()
        if text.startswith("["):
            text = text.split("]", 1)[-1].strip()
        bucket, prefix = parse_s3_location(text, current_bucket)
        if bucket:
            self._set_remote_target(bucket, prefix)

    def go_up(self):
        if self.current_mode() == self.LOCAL:
            parent = os.path.dirname(self._local_dir.rstrip(os.sep))
            if parent and parent != self._local_dir:
                self.set_local(parent)
            return
        bucket, prefix = self.remote_target()
        if not prefix:
            return
        trimmed = prefix.rstrip("/")
        parent = trimmed.rsplit("/", 1)[0] + "/" if "/" in trimmed else ""
        self._set_remote_target(bucket, parent)

    def _on_activated(self, index):
        row = index.row()
        if not (0 <= row < len(self._entries)):
            return
        name, is_dir, _size = self._entries[row]
        if self.current_mode() == self.LOCAL:
            path = os.path.join(self._local_dir, name)
            if is_dir:
                self.set_local(path)
            else:
                QDesktopServices.openUrl(QUrl.fromLocalFile(path))
            return
        if is_dir:
            bucket, prefix = self.remote_target()
            self._set_remote_target(bucket, (prefix or "") + name + "/")

    def _on_header_clicked(self, column):
        current, descending = self._sort
        descending = (not descending) if column == current else False
        self.sort_by(column, descending)

    def sort_by(self, column, descending=False):
        self._sort = (int(column), bool(descending))
        self.view.header().setSortIndicator(
            int(column),
            Qt.SortOrder.DescendingOrder if descending
            else Qt.SortOrder.AscendingOrder)
        self._show(self._entries)

    def eventFilter(self, obj, event):
        if obj is self.view and event.type() == event.Type.KeyPress:
            if self._handle_key(event):
                return True
        return super().eventFilter(obj, event)

    def _handle_key(self, event) -> bool:
        key = event.key()
        if key == Qt.Key.Key_Backspace:
            self.go_up()
            return True
        if key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            index = self.view.currentIndex()
            if index.isValid():
                self._on_activated(index)
                return True
            return False
        if self.current_mode() != self.LOCAL:
            return False
        if key == Qt.Key.Key_F2:
            self.rename_selected()
            return True
        if key == Qt.Key.Key_Delete:
            self.delete_selected()
            return True
        if key == Qt.Key.Key_F7 or event.matches(QKeySequence.StandardKey.New):
            self.create_folder()
            return True
        return False

    def _context_menu(self, pos):
        menu = self.build_context_menu()
        menu.exec(self.view.viewport().mapToGlobal(pos))

    def build_context_menu(self) -> QMenu:
        """The pane's right-click menu, built for the current selection."""
        menu = QMenu(self)
        selection = self.selected()
        if self.current_mode() == self.LOCAL:
            if len(selection) == 1:
                menu.addAction("Open", lambda: self._on_activated(
                    self.view.currentIndex()))
            if selection:
                menu.addAction("Upload to the main pane (F5)",
                               lambda: self._mw.pane_transfer_from_second())
                menu.addSeparator()
            if len(selection) == 1:
                menu.addAction("Rename… (F2)", self.rename_selected)
            menu.addAction("New folder… (F7)", self.create_folder)
            if selection:
                menu.addAction("Move to trash (Del)", self.delete_selected)
                menu.addSeparator()
                menu.addAction("Copy path", self.copy_paths)
            menu.addAction("Show in file manager", self.reveal)
        else:
            if selection:
                menu.addAction("Copy to the main pane (F5)",
                               lambda: self._mw.pane_transfer_from_second())
                menu.addAction("Copy s3:// URI", self.copy_paths)
                menu.addSeparator()
            if self.current_mode() == self.OTHER:
                menu.addAction("Choose profile…", self.choose_other_profile)
        menu.addAction("Refresh", self.refresh)
        return menu

    def copy_paths(self):
        if self.current_mode() == self.LOCAL:
            paths = [os.path.join(self._local_dir, n) for n, _d, _s in self.selected()]
        else:
            bucket, prefix = self.remote_target()
            paths = [f"s3://{bucket}/{prefix}{n}{'/' if d else ''}"
                     for n, d, _s in self.selected()]
        QApplication.clipboard().setText("\n".join(paths))
        self.status.setText(f"Copied {len(paths)} path(s)")

    def reveal(self):
        QDesktopServices.openUrl(QUrl.fromLocalFile(self._local_dir))

    def _local_names(self):
        try:
            return set(os.listdir(self._local_dir))
        except OSError:
            return set()

    def rename_selected(self):
        selection = self.selected()
        if len(selection) != 1 or self.current_mode() != self.LOCAL:
            return False
        old = selection[0][0]
        new, ok = QInputDialog.getText(self, "Rename", "New name:", text=old)
        if not ok or new == old:
            return False
        return self.rename(old, new)

    def rename(self, old, new) -> bool:
        """Rename a local entry; refuses a name that is taken or illegal."""
        problem = validate_new_name(new, self._local_names() - {old})
        if problem:
            QMessageBox.warning(self, "Rename", problem)
            return False
        try:
            os.rename(child_path(self._local_dir, old),
                      child_path(self._local_dir, new))
        except (OSError, ValueError) as exc:
            QMessageBox.warning(self, "Rename", str(exc))
            return False
        self._mw.log(f"renamed {os.path.join(self._local_dir, old)} -> {new}")
        self.refresh()
        self.select_names([new])
        return True

    def create_folder(self):
        if self.current_mode() != self.LOCAL:
            return False
        name, ok = QInputDialog.getText(self, "New folder", "Folder name:")
        if not ok:
            return False
        return self.make_folder(name)

    def make_folder(self, name) -> bool:
        problem = validate_new_name(name, self._local_names())
        if problem:
            QMessageBox.warning(self, "New folder", problem)
            return False
        try:
            os.mkdir(child_path(self._local_dir, name))
        except (OSError, ValueError) as exc:
            QMessageBox.warning(self, "New folder", str(exc))
            return False
        self._mw.log(f"created folder {os.path.join(self._local_dir, name)}")
        self.refresh()
        self.select_names([name])
        return True

    def delete_selected(self):
        selection = self.selected()
        if not selection or self.current_mode() != self.LOCAL:
            return False
        names = [name for name, _d, _s in selection]
        shown = "\n".join(names[:10]) + (
            f"\n… and {len(names) - 10} more" if len(names) > 10 else "")
        if QMessageBox.question(
                self, "Move to trash",
                f"Move {len(names)} item(s) to the trash?\n\n{shown}") \
                != QMessageBox.StandardButton.Yes:
            return False
        return self.trash(names)

    def trash(self, names) -> bool:
        """
        Move local entries to the desktop trash.

        Only when the platform has no trash is anything deleted outright, and
        then only after a second, explicit question: a file manager whose
        Delete cannot be undone is one keystroke from losing a folder.
        """
        failed, untrashable = [], []
        for name in names:
            try:
                path = child_path(self._local_dir, name)
            except ValueError as exc:
                failed.append(f"{name}: {exc}")
                continue
            if not QFile.moveToTrash(path)[0]:
                untrashable.append(path)
        if untrashable:
            answer = QMessageBox.question(
                self, "No trash available",
                f"{len(untrashable)} item(s) could not be moved to a trash on "
                "this system. Delete them permanently? This cannot be undone.")
            if answer == QMessageBox.StandardButton.Yes:
                for path in untrashable:
                    try:
                        if os.path.isdir(path) and not os.path.islink(path):
                            shutil.rmtree(path)
                        else:
                            os.remove(path)
                    except OSError as exc:
                        failed.append(f"{os.path.basename(path)}: {exc}")
        if failed:
            QMessageBox.warning(self, "Move to trash", "\n".join(failed[:10]))
        self._mw.log(f"moved {len(names) - len(failed)} local item(s) to the trash")
        self.refresh()
        return not failed

    def view_target(self) -> tuple:
        """Everything that decides what the rows on screen belong to."""
        mode = self.current_mode()
        if mode == self.LOCAL:
            return (mode, self._local_dir)
        bucket, prefix = self.remote_target()
        return (mode, id(self._other_model) if mode == self.OTHER else None,
                bucket, prefix)

    def refresh(self):
        """
        Show the current target's contents.

        The rows are what selected() maps onto the current target, and they
        feed F6 and Del. Rows from a previous location must therefore never
        stay on screen once the target moved: they are cleared before a new
        remote listing starts, and a listing that lands for a target the
        pane has left since is thrown away rather than shown.
        """
        self.path.setText(self.location())
        if self.current_mode() == self.LOCAL:
            self._scan_error = ""
            self._show(self._scan_local())
            if self._scan_error:
                self.status.setText(self._scan_error)
            return
        bucket, prefix = self.remote_target()
        if not bucket:
            self._show([])
            self.status.setText("No bucket selected")
            return
        if self._thread is not None:
            if self._listing_for != self.view_target():
                self._show([])
                self.status.setText("Loading…")
            self._refresh_again = True
            return
        if self.current_mode() == self.OTHER:
            if self._other_model is None:
                self._show([])
                self.status.setText("No profile selected")
                return
            clone = self._other_model.clone_for_worker()
        else:
            clone = self._model.clone_for_worker()
        clone.bucket = bucket

        def _list(_w):
            return clone.list(prefix)

        if self._listing_for != self.view_target():
            self._show([])
        self._listing_for = self.view_target()
        self._refresh_again = False
        self.status.setText("Loading…")
        self._thread = QThread(self)
        self._worker = FuncWorker(_list)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_listed)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _scan_local(self):
        rows = []
        self._modified = {}
        try:
            names = os.listdir(self._local_dir)
        except OSError as exc:
            # Shown by refresh after _show, which would overwrite it here.
            self._scan_error = str(exc)
            return []
        for name in names:
            full = os.path.join(self._local_dir, name)
            try:
                is_dir = os.path.isdir(full)
                stat = os.stat(full)
            except OSError:
                continue
            rows.append((name, is_dir, 0 if is_dir else int(stat.st_size)))
            self._modified[name] = stat.st_mtime
        return rows

    def _on_listed(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        if self._listing_for != self.view_target() or self._refresh_again:
            # Stale, or superseded by a refresh asked for while it ran.
            self._listing_for = None
            self._refresh_again = False
            self.refresh()
            return
        if exc is not None:
            # _show writes the folder/file count into the status line, so
            # the error goes in after it or it is never seen.
            self._show([])
            self.status.setText(f"Could not list: {exc}")
            return
        rows = []
        self._modified = {}
        for item in result or []:
            rows.append((item.name, item.type_ == FSObjectType.FOLDER,
                         int(item.size or 0)))
            try:
                self._modified[item.name] = as_epoch(item.modified)
            except Exception:
                pass
        self._show(rows)

    def _show(self, rows):
        column, descending = self._sort
        self._entries = sort_rows(rows, column, descending, self._modified)
        self.model.removeRows(0, self.model.rowCount())
        for name, is_dir, size in self._entries:
            stamp = self._modified.get(name)
            cells = [
                QStandardItem(name),
                QStandardItem("" if is_dir else _human_bytes(size)),
                QStandardItem(
                    time.strftime("%Y-%m-%d %H:%M", time.localtime(stamp))
                    if stamp else ""),
            ]
            for cell in cells:
                cell.setEditable(False)
            self.model.appendRow(cells)
        folders = sum(1 for _n, is_dir, _s in self._entries if is_dir)
        self.status.setText(
            f"{folders} folder(s), {len(self._entries) - folders} file(s)")

    def shutdown(self):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
