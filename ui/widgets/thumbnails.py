"""The listing as a grid of thumbnails, and the loader that fills it.

The grid is a second view on the same proxy model and the same selection
model as the list, so every action that reads "the selection" -- the
context menu, Delete, Copy, F5 -- works unchanged in either view.
"""

import collections
import os
import queue
import threading
from PyQt6.QtCore import QObject, QSize, Qt, pyqtSignal
from PyQt6.QtGui import QImage
from PyQt6.QtWidgets import QAbstractItemView, QListView

THUMB_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
THUMB_SIZE = 128
# Bigger originals are not fetched: a thumbnail is a whole GET, and a 40 MB
# photo per tile would turn browsing a folder into downloading it.
THUMB_MAX_BYTES = 8 * 1024 * 1024
THUMBS_PER_LISTING = 300


def wants_thumbnail(name, size) -> bool:
    return (os.path.splitext(str(name or ""))[1].lower() in THUMB_EXTS
            and 0 < int(size or 0) <= THUMB_MAX_BYTES)


def thumbnail_image(data, side=THUMB_SIZE):
    """*data* decoded and scaled to fit a square, or None if undecodable."""
    image = QImage.fromData(data)
    if image.isNull():
        return None
    return image.scaled(side, side, Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation)


class ThumbnailCache:
    """Least-recently-used images keyed by (bucket, key, etag)."""

    def __init__(self, limit=600):
        self._items = collections.OrderedDict()
        self._limit = int(limit)

    def get(self, ident):
        image = self._items.get(ident)
        if image is not None:
            self._items.move_to_end(ident)
        return image

    def put(self, ident, image):
        self._items[ident] = image
        self._items.move_to_end(ident)
        while len(self._items) > self._limit:
            self._items.popitem(last=False)

    def __len__(self):
        return len(self._items)


class ThumbnailLoader(QObject):
    """
    Fetches thumbnails on one background thread, newest listing first.

    Every request carries the listing generation it was made for; moving to
    another folder bumps it, which drops the queue and makes any result
    still in flight arrive marked stale rather than painted onto the wrong
    tile.
    """

    loaded = pyqtSignal(int, str, object)   # generation, key, QImage or None

    def __init__(self, parent=None):
        super().__init__(parent)
        self._queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = None
        self.generation = 0

    def new_generation(self):
        self.generation += 1
        try:
            while True:
                self._queue.get_nowait()
        except queue.Empty:
            pass
        return self.generation

    def request(self, model, key):
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="s3duck-thumbnails", daemon=True)
            self._thread.start()
        self._queue.put((self.generation, model, key))

    def _run(self):
        while not self._stop.is_set():
            try:
                generation, model, key = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            if generation != self.generation:
                continue
            try:
                body = model.get_object_preview(key, THUMB_MAX_BYTES)
                image = None if body.get("truncated") else \
                    thumbnail_image(body.get("data") or b"")
            except Exception:
                image = None
            self.loaded.emit(generation, key, image)

    def shutdown(self, timeout=3.0):
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout)


class ThumbnailGrid(QListView):
    """Icon-mode view over the listing's model and selection."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setViewMode(QListView.ViewMode.IconMode)
        self.setResizeMode(QListView.ResizeMode.Adjust)
        self.setMovement(QListView.Movement.Static)
        self.setUniformItemSizes(True)
        self.setWordWrap(True)
        self.setIconSize(QSize(THUMB_SIZE, THUMB_SIZE))
        self.setGridSize(QSize(THUMB_SIZE + 32, THUMB_SIZE + 44))
        self.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
