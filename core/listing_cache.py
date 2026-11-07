"""Bounded, in-memory listings for one window's navigation session."""

from collections import OrderedDict


class ListingCache:
    def __init__(self, max_folders=32, max_items=50000):
        self.max_folders = max_folders
        self.max_items = max_items
        self._entries = OrderedDict()
        self._item_count = 0

    def clear(self):
        self._entries.clear()
        self._item_count = 0

    def get(self, key):
        payload = self._entries.get(key)
        if payload is not None:
            self._entries.move_to_end(key)
        return payload

    def put(self, key, payload):
        previous = self._entries.pop(key, None)
        if previous is not None:
            self._item_count -= len(previous["items"])
        items = tuple(payload.get("items") or ())
        if len(items) > self.max_items:
            return
        # Connection promotions belong to the live worker, not a cached view.
        self._entries[key] = dict(
            mode="bucket_items", bucket=payload["bucket"],
            prefix=payload["prefix"], items=items,
            truncated=bool(payload.get("truncated")), cached=True)
        self._item_count += len(items)
        while (len(self._entries) > self.max_folders
               or self._item_count > self.max_items):
            _, oldest = self._entries.popitem(last=False)
            self._item_count -= len(oldest["items"])
