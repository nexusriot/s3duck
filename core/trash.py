"""The ``.trash/`` convention: delete as a move, restore as the move back.

Undo-delete needs versioning, and most buckets do not have it. With the
convention on, a delete moves each object under ``.trash/`` at the same
relative key, so restoring is the same move in reverse and nothing has to be
remembered outside the bucket itself. Deleting something already in the
trash is a real delete -- that is what emptying it means.
"""

TRASH_PREFIX = ".trash/"


def in_trash(key) -> bool:
    return str(key or "").startswith(TRASH_PREFIX)


def split_for_trash(keys, classes=None):
    """
    ``(move_rows, delete_keys)`` for a delete with the trash on.

    ``move_rows`` are the move worker's
    ``(src, dst, is_folder, dst_bucket, storage_class)`` rows; keys already in
    the trash are returned in ``delete_keys`` to be deleted for good.
    """
    classes = classes or {}
    moves, deletes = [], []
    for key in keys or []:
        key = str(key or "")
        if not key:
            continue
        if in_trash(key):
            deletes.append(key)
            continue
        moves.append((key, TRASH_PREFIX + key, key.endswith("/"), None,
                      classes.get(key) or None))
    return moves, deletes


def restore_rows(keys, classes=None) -> list:
    """Move rows taking trashed keys back to where they were deleted from."""
    classes = classes or {}
    rows = []
    for key in keys or []:
        key = str(key or "")
        if not in_trash(key) or key == TRASH_PREFIX:
            continue
        rows.append((key, key[len(TRASH_PREFIX):], key.endswith("/"), None,
                     classes.get(key) or None))
    return rows
