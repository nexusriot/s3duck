"""The thumbnail grid: switching to it, and filling its tiles."""

from PyQt6.QtGui import QIcon, QPixmap
from core.entries import FSObjectType
from ui.widgets.thumbnails import (
    THUMBS_PER_LISTING, ThumbnailCache, ThumbnailGrid, ThumbnailLoader,
    wants_thumbnail,
)


class ThumbnailMixin:
    """The thumbnail grid: switching to it, and filling its tiles."""

    def _attach_thumbnail_grid(self, layout):
        """Create the grid on the listing's model and selection."""
        self.thumb_grid = ThumbnailGrid(self)
        self.thumb_grid.setModel(self.proxy)
        self.thumb_grid.setSelectionModel(self.listview.selectionModel())
        self.thumb_grid.doubleClicked.connect(self.list_doubleClicked)
        self.thumb_grid.installEventFilter(self)
        self.thumb_grid.hide()
        layout.addWidget(self.thumb_grid)
        self._thumb_cache = ThumbnailCache()
        self._thumb_loader = ThumbnailLoader(self)
        self._thumb_loader.loaded.connect(self._on_thumbnail_loaded)
        self._thumb_targets = {}

    def thumbnail_view_active(self) -> bool:
        grid = getattr(self, "thumb_grid", None)
        return grid is not None and not grid.isHidden()

    def set_thumbnail_view(self, on):
        """Show the listing as a grid of thumbnails, or as the list again."""
        on = bool(on)
        self.thumb_grid.setVisible(on)
        self.listview.setVisible(not on)
        (self.thumb_grid if on else self.listview).setFocus()
        self.store.set("thumbnail_view", "true" if on else "false")
        action = getattr(self, "actThumbnails", None)
        if action is not None and action.isChecked() != on:
            action.blockSignals(True)
            action.setChecked(on)
            action.blockSignals(False)
        if on:
            self.request_thumbnails()
        else:
            self._thumb_loader.new_generation()

    def request_thumbnails(self):
        """Queue a thumbnail for every image row of the current listing."""
        loader = self._thumb_loader
        loader.new_generation()
        self._thumb_targets = {}
        if not self.thumbnail_view_active() or self.in_bucket_list_mode():
            return 0
        if not self.listing_settled():
            # The rows are the folder being left, named under the one being
            # entered; the landing listing asks again.
            return 0
        bucket = self.data_model.bucket
        folder = self.data_model.current_folder or ""
        clone = None
        queued = 0
        for row in range(self.model.rowCount()):
            item = self.model.item(row, 0)
            if item is None or getattr(item, "t", None) != FSObjectType.FILE:
                continue
            name = item.text()
            size = int(getattr(item, "size", 0) or 0)
            etag_item = self.model.item(row, 4)
            etag = etag_item.text() if etag_item is not None else ""
            key = folder + name
            ident = (bucket, key, etag)
            cached = self._thumb_cache.get(ident)
            if cached is not None:
                item.setIcon(QIcon(QPixmap.fromImage(cached)))
                continue
            if not wants_thumbnail(name, size) or queued >= THUMBS_PER_LISTING:
                continue
            if clone is None:
                clone = self.data_model.clone_for_worker()
            self._thumb_targets[key] = ident
            loader.request(clone, key)
            queued += 1
        return queued

    def _on_thumbnail_loaded(self, generation, key, image):
        if generation != self._thumb_loader.generation:
            return
        ident = self._thumb_targets.pop(key, None)
        if ident is None or image is None:
            return
        self._thumb_cache.put(ident, image)
        name = key[len(self.data_model.current_folder or ""):]
        for row in range(self.model.rowCount()):
            item = self.model.item(row, 0)
            if item is not None and item.text() == name:
                item.setIcon(QIcon(QPixmap.fromImage(image)))
                break

    def shutdown_thumbnails(self):
        loader = getattr(self, "_thumb_loader", None)
        if loader is not None:
            loader.new_generation()
            loader.shutdown()
