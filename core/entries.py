"""What one row of a listing is.

A bucket, a prefix or an object, plus the download plan built from a
tree of them. No client and no widget: both sides of the app pass these
around, so they belong to neither.
"""

from enum import Enum
from typing import NamedTuple


class PrefixDownloadPlan(NamedTuple):
    base_dir: str
    dirs: list
    files: list      # [(key, out_path, size)]
    unsafe: list     # keys that cannot be written under base_dir
    reasons: dict    # unsafe key -> why, in words the user can act on
class FSObjectType(Enum):
    FILE = 1
    FOLDER = 2
    BUCKET = 3  # top-level S3 bucket
class Item:
    def __init__(self, name, type_, modified, size, storage_class="", etag=""):
        self.name = name
        self.type_ = type_
        self.modified = modified
        self.size = size
        # Both come free with ListObjectsV2, so the optional listing columns
        # cost no extra request.
        self.storage_class = storage_class
        self.etag = etag

    def __repr__(self):
        # .value, not the member: FSObjectType is a plain Enum, so "%d" on
        # the member itself raises TypeError -- which made repr(Item(...))
        # fail, and with it any log line or assertion message holding one.
        return "name: %s; type_: %d(%s), modified: %s size: %d" % (
            self.name,
            self.type_.value,
            "file"
            if self.type_ == FSObjectType.FILE
            else ("dir" if self.type_ == FSObjectType.FOLDER else "bucket"),
            self.modified,
            self.size,
        )
