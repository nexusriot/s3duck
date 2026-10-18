"""The listing's right-click menu.

This was 467 lines inside MainWindow.eventFilter, in two halves that had to
be kept in step by hand: a run of ``addAction`` calls assigning two dozen
local QAction variables, then a run of ``if clicked == that_variable``
comparisons, plus a third hand-written tuple naming the ones to strip on a
read-only profile.

Keeping three lists in agreement is the kind of job nothing checks, and it
had already failed: ``hold_action`` appeared in the strip tuple and in the
dispatch but was never created anywhere, so the name was simply unbound. On
a read-only profile that raised NameError while building the strip list --
before the menu was shown at all -- and on a writable one it raised after
the chosen action had run but before "Properties" was dispatched, so right
click -> Properties did nothing and every other entry printed a traceback.

Here an entry is added and answered in one call, and says there whether it
writes. There is no second list to fall out of step with, and the read-only
strip is derived rather than retyped.
"""

import os

from PyQt6.QtGui import QAction

from model import FSObjectType
from ui.constants import UP_ENTRY_LABEL
from utils import themed_icon


class MenuBuilder:
    """A QMenu plus what each of its entries does."""

    def __init__(self, window, menu):
        self._window = window
        self._menu = menu
        self._handlers = {}
        self._mutating = []

    def add(self, theme_name, icon_file, label, handler, *,
            mutates=True, tooltip=""):
        """One entry, with the thing it does and whether it writes.

        ``mutates`` is what the read-only strip reads, so an entry cannot be
        added without someone deciding that question for it.
        """
        icon = themed_icon(theme_name, os.path.join(
            self._window.current_dir, "icons", icon_file))
        action = QAction(icon, label)
        if tooltip:
            action.setToolTip(tooltip)
        self._menu.addAction(action)
        self._handlers[action] = handler
        if mutates:
            self._mutating.append(action)
        return action

    def separator(self):
        self._menu.addSeparator()

    def strip_mutating(self):
        """Leave a read-only profile only the entries that read.

        The model refuses writes as well; this is so the menu does not offer
        what it cannot do.
        """
        for action in self._mutating:
            self._menu.removeAction(action)
            self._handlers.pop(action, None)
        hint = QAction("(read-only profile)", self._window)
        hint.setEnabled(False)
        self._menu.addAction(hint)

    def run(self, position) -> bool:
        """Show the menu and run whatever was chosen."""
        self._window._menu_click_guard.arm()
        clicked = self._menu.exec(position)
        if not clicked:
            return False
        handler = self._handlers.get(clicked)
        if handler is not None:
            handler()
        return True


def _bucket_list_menu(window, builder, ixs, entry, writable, position) -> bool:
    """The menu over the list of buckets."""
    if not writable:
        builder.strip_mutating()        # nothing was added, so this is the hint
        builder.run(position)
        return True

    builder.add("folder-new", "create_new_folder_24px.svg", "Create bucket…",
                window.new_bucket)
    # Delete and empty only make sense on an actual bucket row.
    if ixs and entry and getattr(entry, "t", None) == FSObjectType.BUCKET:
        builder.add("tab-new", "folder_24px.svg", "Open in new tab (Ctrl+T)",
                    window.open_selection_in_new_tab, mutates=False)
        builder.add("edit-clear", "delete_24px.svg", "Empty bucket…",
                    window.empty_bucket_ui)
        builder.add("edit-delete", "delete_24px.svg", "Delete bucket…",
                    window.delete_bucket_ui)
    builder.run(position)
    return True


def _listing_menu(window, builder, ixs, entry, raw_name, upload_path, key,
                  up_selected, writable, position) -> bool:
    """The menu over the contents of a bucket."""
    model = window.data_model
    here = model.current_folder or "/"

    if entry and getattr(entry, "t", None) == FSObjectType.FOLDER \
            and not up_selected:
        builder.add("network-server", "file_upload_24px.svg",
                    "Upload -> %s" % upload_path,
                    lambda: window.upload(upload_path))
    builder.add("network-server", "file_upload_24px.svg",
                "Upload -> %s" % here, window.upload)
    builder.add("folder", "folder_24px.svg",
                "Upload folder -> %s" % here, window.upload_folder)
    builder.add("folder-new", "create_new_folder_24px.svg", "Create folder",
                window.new_folder)
    builder.add("edit-find", "document_24px.svg", "Search here…",
                window.open_search, mutates=False)

    clipboard = window._clipboard
    if clipboard and clipboard.get("items"):
        builder.add("edit-paste", "copy_24px.svg",
                    "Paste %d item(s) here (Ctrl+V)" % len(clipboard["items"]),
                    window.paste_from_clipboard)

    builder.add("document-open-recent", "document_24px.svg",
                "Bucket versioning…", window.bucket_versioning_ui)
    builder.add("edit-clear-history", "delete_24px.svg",
                "Incomplete uploads…", window.show_incomplete_uploads)
    builder.add("folder-sync", "refresh_24px.svg",
                "Sync with local folder… (Ctrl+E)", window.open_sync)

    if ixs and not up_selected:
        is_folder = entry and getattr(entry, "t", None) == FSObjectType.FOLDER
        is_file = entry and getattr(entry, "t", None) == FSObjectType.FILE
        if is_folder:
            builder.add("tab-new", "folder_24px.svg",
                        "Open in new tab (Ctrl+T)",
                        window.open_selection_in_new_tab, mutates=False)
        builder.add("emblem-downloads", "download_24px.svg", "Download",
                    window.download, mutates=False)
        if is_file:
            builder.add("document-open", "folder_24px.svg", "Open / preview",
                        lambda: window.open_or_preview(key), mutates=False)
            builder.add("document-open-recent", "document_24px.svg",
                        "Versions…", lambda: window.show_versions(key),
                        mutates=False)
            builder.add("insert-link", "copy_24px.svg", "Share link…",
                        lambda: window.share_link(key), mutates=False)
            builder.add("insert-link", "copy_24px.svg",
                        "Make public + copy URL…",
                        lambda: window.make_public_and_copy(key))
        builder.add("edit-delete", "delete_24px.svg", "Delete", window.delete)

        builder.separator()
        builder.add("edit-copy", "copy_24px.svg", "Copy (Ctrl+C)",
                    window.copy_to_clipboard, mutates=False)
        builder.add("edit-cut", "copy_24px.svg", "Cut (Ctrl+X)",
                    lambda: window.copy_to_clipboard(cut=True))
        builder.add("package-x-generic", "document_24px.svg",
                    "Download as ZIP…", window.download_as_zip, mutates=False)
        builder.add("document-properties", "settings_24px.svg",
                    "Edit tags on selection…", window.bulk_tags)
        builder.add("edit-copy", "copy_24px.svg", "Copy / Move to…",
                    window.copy_move)
        # Not stripped on a read-only profile: copying data OUT reads the
        # source and writes elsewhere, and the destination model enforces
        # its own read-only.
        builder.add("document-send", "file_upload_24px.svg",
                    "Copy to another profile…", window.copy_to_profile,
                    mutates=False)
        builder.add("edit-rename", "edit_24px.svg", "Rename…",
                    window.rename_selected)
        builder.add("edit-rename", "edit_24px.svg",
                    "Rename multiple… (Shift+F2)", window.bulk_rename)
        builder.add("drive-harddisk", "bucket_24px.svg",
                    "Change storage class…", window.change_storage_class_ui)
        builder.add("text-x-generic", "document_24px.svg",
                    "Fix Content-Type from extension",
                    window.fix_content_type_ui,
                    tooltip="Re-stamp Content-Type on the selection from "
                            "each key's extension")
        builder.add("emblem-downloads", "download_24px.svg",
                    "Restore from Glacier…", window.restore_from_glacier)
        if is_file:
            builder.add("document-properties", "settings_24px.svg",
                        "Edit tags…", lambda: window.edit_tags(key))
            builder.add("document-properties", "settings_24px.svg",
                        "Edit metadata…", lambda: window.edit_metadata(key))
            # The entry whose absence left hold_action unbound. Bucket
            # settings tells the user a legal hold is placed "from the
            # object's context menu", so this is where it belongs.
            builder.add("channel-secure", "ok_24px.svg", "Legal hold…",
                        lambda: window.toggle_legal_hold(key))

    if raw_name and entry and raw_name.rstrip("/") != UP_ENTRY_LABEL:
        builder.add("document-properties", "puzzle_24px.svg", "Properties",
                    lambda: window.properties(window.data_model, key),
                    mutates=False)

    if not writable:
        builder.strip_mutating()
    return builder.run(position)


def show_listing_menu(window, event) -> bool:
    """Build, show and dispatch the listing's context menu."""
    ixs = window.listview.selectedIndexes()
    # Row-primary rather than the raw clicked column.
    entry, raw_name, raw_key = window.name_by_first_ix(ixs)
    up_selected = bool(
        entry is not None and raw_name
        and raw_name.rstrip("/") == UP_ENTRY_LABEL)
    writable = not window.is_read_only()

    upload_path = raw_key
    if upload_path is None or up_selected:
        upload_path = window.data_model.current_folder
    # Deliberately not upload_path: on the [..] row that one falls back to
    # the current folder, while the key still names the row that was clicked.
    key = raw_key or window.data_model.current_folder

    window.menu.clear()
    builder = MenuBuilder(window, window.menu)
    position = event.globalPos()

    if window.in_bucket_list_mode():
        return _bucket_list_menu(window, builder, ixs, entry, writable,
                                 position)
    return _listing_menu(window, builder, ixs, entry, raw_name, upload_path,
                         key, up_selected, writable, position)
