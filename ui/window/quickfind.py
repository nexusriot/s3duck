"""The type-to-filter row over the listing, and saved searches."""

import json
from PyQt6.QtCore import Qt
from ui.constants import UP_ENTRY_LABEL
from ui.dialogs.search import SearchDialog


class QuickFindMixin:
    """The type-to-filter row over the listing, and saved searches."""

    def _toggle_search(self):
        if self._filter_row.isVisible() and self.search_edit.hasFocus():
            self._hide_search()
        else:
            self._filter_row.show()
            self.search_edit.setFocus(Qt.FocusReason.ShortcutFocusReason)
            self.search_edit.selectAll()

    def _open_search_with_text(self, text: str):
        """Open the quick-find bar and append typed text (type-to-search)."""
        self._filter_row.show()
        self.search_edit.setFocus(Qt.FocusReason.ShortcutFocusReason)
        self.search_edit.setText(self.search_edit.text() + text)

    def _hide_search(self):
        self.search_edit.blockSignals(True)
        self.search_edit.clear()
        self.search_edit.blockSignals(False)
        self._filter_row.hide()
        self.proxy.set_filter_text("")
        self._update_filter_status()
        self.listview.setFocus(Qt.FocusReason.OtherFocusReason)

    def filter_match_counts(self) -> tuple:
        """``(shown, total)`` rows for the current filter, ignoring [..]."""
        total = 0
        shown = 0
        for row in range(self.model.rowCount()):
            name = str(self.model.item(row, 0).text()
                       if self.model.item(row, 0) is not None else "")
            if name == UP_ENTRY_LABEL:
                continue
            total += 1
            if self.proxy.matches(name):
                shown += 1
        return shown, total

    def _update_filter_status(self):
        if not self.search_edit.text().strip():
            self.filter_status.hide()
            return
        shown, total = self.filter_match_counts()
        self.filter_status.setText(f"{shown} of {total}")
        self.filter_status.setStyleSheet(
            "color: #c62828;" if shown == 0 else "")
        self.filter_status.show()

    def _on_search_text_changed(self, text):
        self.proxy.set_filter_text(text)
        self._update_filter_status()
        # Keep a valid selection among the visible (filtered) rows.
        if self.proxy.rowCount() > 0 and not self.listview.currentIndex().isValid():
            self.listview.setCurrentIndex(self.proxy.index(0, 0))

    def _reset_search_on_navigate(self):
        # The filter is per-listing; clear it whenever the listing changes.
        if self.search_edit.text() or self._filter_row.isVisible():
            self.search_edit.blockSignals(True)
            self.search_edit.clear()
            self.search_edit.blockSignals(False)
            self._filter_row.hide()
            self.proxy.set_filter_text("")
            self._update_filter_status()

    def _filter_box_key(self, event) -> bool:
        """Keys typed in the quick-find box. True when handled."""
        k = event.key()
        if k == Qt.Key.Key_Escape:
            self._hide_search()
            return True
        if k in (Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_Down):
            # Move focus into the (filtered) list to navigate/activate.
            self.listview.setFocus(Qt.FocusReason.OtherFocusReason)
            if self.proxy.rowCount() > 0 and not self.listview.currentIndex().isValid():
                self.listview.setCurrentIndex(self.proxy.index(0, 0))
            return True
        return False

    def _saved_searches_key(self) -> str:
        return self.store.profile_key("searches")

    def load_saved_searches(self) -> list:
        """
        This profile's saved searches, newest format only.

        A corrupt value costs the list, not the dialog — the search form still
        opens with nothing saved.
        """
        raw = self.store.get(self._saved_searches_key(), "")
        try:
            entries = json.loads(raw or "[]")
        except (ValueError, TypeError):
            return []
        if not isinstance(entries, list):
            return []
        return [entry for entry in entries
                if isinstance(entry, dict) and entry.get("name")]

    def store_saved_searches(self, entries):
        with self.store.group():
            self.settings.setValue(
                self._saved_searches_key(), json.dumps(list(entries or [])))

    def open_search(self):
        """Recursively search the current bucket/prefix by key substring."""
        if self.in_bucket_list_mode():
            self.statusBar().showMessage("Open a bucket to search", 2000)
            return
        SearchDialog(self, self, self.data_model,
                     self.data_model.current_folder or "").exec()
