"""Tabs: one bucket/prefix per tab, remembered per profile."""

import json
from PyQt6.QtWidgets import QMenu
from model import FSObjectType
from ui.constants import UP_ENTRY_LABEL


class TabsMixin:
    """Tabs: one bucket/prefix per tab, remembered per profile."""

    def tab_locations(self) -> list:
        """The (bucket, prefix) each tab points at, in tab order."""
        return [(entry["bucket"], entry["prefix"]) for entry in self._tabs]

    @staticmethod
    def tab_label(bucket: str, prefix: str) -> str:
        """
        A tab's caption: the deepest name that identifies the location.

        The whole prefix would not fit and the bucket alone cannot tell two
        folders apart, so the last path segment wins and the bucket is the
        fallback.
        """
        if not bucket:
            return "buckets"
        trimmed = (prefix or "").strip("/")
        if not trimmed:
            return bucket
        return trimmed.rsplit("/", 1)[-1]

    def _sync_tab_bar(self):
        """Rebuild the captions and show the bar only when it earns its row."""
        self._tab_switching = True
        try:
            while self.tabbar.count() > len(self._tabs):
                self.tabbar.removeTab(self.tabbar.count() - 1)
            for index, entry in enumerate(self._tabs):
                label = self.tab_label(entry["bucket"], entry["prefix"])
                location = ("s3://%s/%s" % (entry["bucket"], entry["prefix"])
                            if entry["bucket"] else "All buckets")
                if index < self.tabbar.count():
                    self.tabbar.setTabText(index, label)
                else:
                    self.tabbar.addTab(label)
                self.tabbar.setTabToolTip(index, location)
        finally:
            self._tab_switching = False
        self.tabbar.setVisible(len(self._tabs) > 1)

    @staticmethod
    def _new_tab_entry(bucket="", prefix="") -> dict:
        """A tab: where it points, plus the trail it took to get there."""
        return {"bucket": bucket, "prefix": prefix,
                "history": [(bucket, prefix)], "pos": 0}

    def current_tab_entry(self):
        index = self.tabbar.currentIndex()
        if 0 <= index < len(self._tabs):
            return self._tabs[index]
        return None

    def _remember_current_tab(self):
        """Store where the view is now on the tab that is showing it."""
        if self._tab_switching or not self._tabs:
            return
        here = (self.data_model.bucket or "",
                self.data_model.current_folder or "")
        if self._tab_restore_pending:
            self._tab_restore_pending = False
            for position, entry in enumerate(self._tabs):
                if self._entry_location(entry) == here:
                    self._tab_switching = True
                    try:
                        self.tabbar.setCurrentIndex(position)
                    finally:
                        self._tab_switching = False
                    self._update_history_buttons()
                    return
        index = self.tabbar.currentIndex()
        if 0 <= index < len(self._tabs):
            entry = self._tabs[index]
            entry["bucket"], entry["prefix"] = here
            if self._history_navigating:
                self._history_navigating = False
            else:
                self.push_history(entry, here)
            self._sync_tab_bar()
            self._save_tabs()
        self._update_history_buttons()

    def new_tab(self):
        """Open another tab on the current location."""
        if not self._tabs:
            self._tabs = [self._current_location()]
        self._tabs.insert(self.tabbar.currentIndex() + 1,
                          self._current_location())
        self._sync_tab_bar()
        self._tab_switching = True
        try:
            self.tabbar.setCurrentIndex(self.tabbar.currentIndex() + 1)
        finally:
            self._tab_switching = False
        self._save_tabs()
        self.statusBar().showMessage("New tab", 2000)

    def close_tab(self, index=None):
        """Close a tab; the last one stays, since the view needs a location."""
        if len(self._tabs) <= 1:
            self.statusBar().showMessage("The last tab stays open", 2000)
            return
        index = self.tabbar.currentIndex() if index is None else int(index)
        if not (0 <= index < len(self._tabs)):
            return
        del self._tabs[index]
        self._tab_switching = True
        try:
            self.tabbar.removeTab(index)
        finally:
            self._tab_switching = False
        self._sync_tab_bar()
        self._save_tabs()
        self._goto_tab(self.tabbar.currentIndex())

    def close_other_tabs(self, index=None):
        index = self.tabbar.currentIndex() if index is None else int(index)
        if not (0 <= index < len(self._tabs)) or len(self._tabs) <= 1:
            return
        self._tabs = [self._tabs[index]]
        self._tab_switching = True
        try:
            while self.tabbar.count() > 1:
                self.tabbar.removeTab(self.tabbar.count() - 1)
            self.tabbar.setCurrentIndex(0)
        finally:
            self._tab_switching = False
        self._sync_tab_bar()
        self._save_tabs()

    def open_in_new_tab(self, bucket, prefix):
        """Add a tab for another location and switch to it."""
        if not self._tabs:
            self._tabs = [self._current_location()]
        self._tabs.insert(self.tabbar.currentIndex() + 1,
                          self._new_tab_entry(bucket or "", prefix or ""))
        self._sync_tab_bar()
        self._save_tabs()
        self.tabbar.setCurrentIndex(self.tabbar.currentIndex() + 1)

    def open_selection_in_new_tab(self):
        """Open the selected bucket or folder in another tab."""
        _item, name, kind = self.get_row_primary_item(
            self.listview.currentIndex())
        if not name or name == UP_ENTRY_LABEL:
            return
        if self.in_bucket_list_mode():
            self.open_in_new_tab(name, "")
            return
        if kind != FSObjectType.FOLDER:
            self.statusBar().showMessage("Only folders open in a tab", 2000)
            return
        self.open_in_new_tab(
            self.data_model.bucket,
            (self.data_model.current_folder or "") + name + "/")

    def _tab_menu(self, pos):
        index = self.tabbar.tabAt(pos)
        menu = QMenu(self)
        act_new = menu.addAction("New tab")
        act_dup = menu.addAction("Duplicate tab")
        menu.addSeparator()
        act_close = menu.addAction("Close tab")
        act_others = menu.addAction("Close other tabs")
        act_close.setEnabled(len(self._tabs) > 1)
        act_others.setEnabled(len(self._tabs) > 1)
        chosen = menu.exec(self.tabbar.mapToGlobal(pos))
        if chosen is None:
            return
        if chosen is act_new or chosen is act_dup:
            self.new_tab()
        elif chosen is act_close:
            self.close_tab(index if index >= 0 else None)
        elif chosen is act_others:
            self.close_other_tabs(index if index >= 0 else None)

    def next_tab(self, step=1):
        if len(self._tabs) <= 1:
            return
        count = len(self._tabs)
        self.tabbar.setCurrentIndex(
            (self.tabbar.currentIndex() + int(step)) % count)

    def _on_tab_moved(self, source, target):
        if not (0 <= source < len(self._tabs)) or not (
                0 <= target < len(self._tabs)):
            return
        self._tabs.insert(target, self._tabs.pop(source))
        self._save_tabs()

    def _on_tab_changed(self, index):
        if self._tab_switching:
            return
        self._goto_tab(index)

    def _goto_tab(self, index):
        """Navigate the shared view to what a tab remembers."""
        if not (0 <= index < len(self._tabs)):
            return
        self._update_history_buttons()
        entry = self._tabs[index]
        if (entry["bucket"] == (self.data_model.bucket or "")
                and entry["prefix"] == (self.data_model.current_folder or "")):
            return
        self.open_location(entry["bucket"], entry["prefix"])

    def _tabs_settings_key(self) -> str:
        # tabs, watch and searches used to drop the "or default" fallback that
        # bindings, bookmarks and last_location carried, so a profile with an
        # empty name -- which the profile dialog does not prevent -- stored
        # these three under "tabs/", "watch/" and "searches/" while its other
        # state lived under ".../default". All six agree now.
        return self.store.profile_key("tabs")

    def _save_tabs(self):
        with self.store.group():
            self.settings.setValue(
                self._tabs_settings_key(),
                json.dumps([[e["bucket"], e["prefix"]] for e in self._tabs]))

    def _load_tabs(self):
        """
        Restore this profile's tabs, falling back to a single one.

        A stored tab is only a location, so a bucket that has since been
        deleted costs one failed navigation, not a broken window.
        """
        raw = self.store.get(self._tabs_settings_key(), "")
        entries = []
        try:
            for row in json.loads(raw or "[]"):
                if isinstance(row, (list, tuple)) and len(row) == 2:
                    # A trail is session state; only the location is restored.
                    entries.append(self._new_tab_entry(
                        str(row[0] or ""), str(row[1] or "")))
        except (ValueError, TypeError):
            entries = []
        self._tabs = entries or [self._current_location()]
        # The window opens wherever the remembered last location says, which
        # is not necessarily tab 0. The first navigation selects the matching
        # tab instead of overwriting whichever one happens to be current.
        self._tab_restore_pending = True
        self._sync_tab_bar()
