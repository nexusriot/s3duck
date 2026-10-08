"""Saved places and the menu that offers them."""

import os
from PyQt6.QtCore import QSize, Qt
from PyQt6.QtWidgets import QDialog, QInputDialog, QMenu, QToolButton
from core.bookmarks import (
    add_bookmark_to, bookmark_label, parse_bookmarks, serialize_bookmarks,
)
from core.locations import location_entries
from ui.dialogs.bookmarks import BookmarksDialog
from ui.dialogs.filter_list import QuickOpenDialog
from utils import themed_icon


class BookmarksMixin:
    """Saved places and the menu that offers them."""

    def _bookmarks_settings_key(self) -> str:
        return self.store.profile_key("bookmarks")

    def _load_bookmarks(self):
        with self.store.group():
            raw = self.settings.value(self._bookmarks_settings_key(), "") or ""
        self._bookmarks = parse_bookmarks(raw)

    def _save_bookmarks(self):
        with self.store.group():
            self.settings.setValue(
                self._bookmarks_settings_key(), serialize_bookmarks(self._bookmarks))

    def add_bookmark(self):
        """Save the current bucket/prefix for one-click return."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to bookmark it", 2000)
            return
        bucket = self.data_model.bucket
        prefix = self.data_model.current_folder or ""
        suggested = bookmark_label(bucket, prefix)
        name, ok = QInputDialog.getText(
            self, "Add bookmark", "Name:", text=suggested)
        if not ok:
            return
        self._bookmarks, added = add_bookmark_to(
            self._bookmarks, (name or "").strip(), bucket, prefix)
        if not added:
            self.statusBar().showMessage("This location is already bookmarked", 3000)
            return
        self._save_bookmarks()
        self._rebuild_bookmark_menu()
        self.log(f"bookmarked s3://{bucket}/{prefix}")
        self.statusBar().showMessage("Bookmark added", 3000)

    def manage_bookmarks(self):
        dlg = BookmarksDialog(self, self._bookmarks)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._bookmarks = dlg.bookmarks()
        self._save_bookmarks()
        self._rebuild_bookmark_menu()

    def go_to_bookmark(self, entry):
        """Jump to a saved location, crossing buckets when needed."""
        bucket = entry.get("bucket") or ""
        prefix = entry.get("prefix") or ""
        if not bucket:
            return
        if bucket == self.data_model.bucket:
            self.change_current_folder(prefix)
            self.navigate(select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix)

    def _rebuild_bookmark_menu(self):
        menu = self.bookmarkButton.menu()
        menu.clear()
        add_act = menu.addAction("Bookmark this location (Ctrl+B)")
        add_act.triggered.connect(lambda: self.add_bookmark())
        manage_act = menu.addAction("Manage bookmarks…")
        manage_act.triggered.connect(lambda: self.manage_bookmarks())
        manage_act.setEnabled(bool(self._bookmarks))
        if self._bookmarks:
            menu.addSeparator()
            for entry in self._bookmarks:
                act = menu.addAction(entry.get("name") or bookmark_label(
                    entry.get("bucket"), entry.get("prefix")))
                act.setToolTip(
                    f"s3://{entry.get('bucket', '')}/{entry.get('prefix', '')}")
                act.triggered.connect(
                    lambda _checked=False, e=dict(entry): self.go_to_bookmark(e))

    def _build_bookmark_button(self):
        self.bookmarkButton = QToolButton()
        self.bookmarkButton.setIcon(
            themed_icon("bookmarks", os.path.join(self.current_dir, "icons", "ok_24px.svg"))
        )
        self.bookmarkButton.setIconSize(QSize(26, 26))
        self.bookmarkButton.setToolButtonStyle(
            Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.bookmarkButton.setToolTip("Bookmarks")
        self.bookmarkButton.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        self.bookmarkButton.setMenu(QMenu(self.bookmarkButton))
        self.tBar.addWidget(self.bookmarkButton)
        self._rebuild_bookmark_menu()

    def show_quick_open(self):
        """Jump to a bookmark or bucket by name."""
        clone = self.data_model.clone_for_worker()

        def _fetch(_worker):
            # Off the GUI thread: reaching the endpoint from a slot is the
            # freeze this app has been stamping out everywhere else.
            return [item.name for item in clone.list_buckets()]

        buckets, exc = self._run_with_progress("Listing buckets…", _fetch)
        if buckets is None and exc is None:
            return  # cancelled
        if exc is not None:
            self.log(f"quick open: could not list buckets ({exc})")
            buckets = []

        entries = location_entries(
            buckets or [], self._bookmarks, self.data_model.bucket)
        if not entries:
            self.statusBar().showMessage("Nowhere to go yet", 2000)
            return
        dlg = QuickOpenDialog(self, entries)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        target = dlg.chosen_location()
        if target is None:
            return
        bucket, prefix = target
        # Same navigation the bookmark menu uses, including the same-bucket
        # shortcut that avoids a pointless re-entry.
        self.go_to_bookmark({"bucket": bucket, "prefix": prefix})
