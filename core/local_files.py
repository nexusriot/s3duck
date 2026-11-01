"""The local half of the dual pane: ordering rows and vetting names.

Kept out of the widget so the rules a rename or a new folder must pass can be
tested without a QApplication, and so the remote half sorts the same way.
"""

import os

# Column indices of the pane's listing.
NAME, SIZE, MODIFIED = 0, 1, 2

# Names the local filesystem will not take on at least one supported OS.
# Windows is the strict one; refusing these everywhere keeps a folder that
# was created on Linux from becoming unsyncable on the next machine.
_FORBIDDEN_CHARS = set('<>:"|?*') | {"/", "\\", "\0"}
_RESERVED_WINDOWS = {"CON", "PRN", "AUX", "NUL",
                     *(f"COM{i}" for i in range(1, 10)),
                     *(f"LPT{i}" for i in range(1, 10))}


def sort_rows(rows, column=NAME, descending=False, modified=None):
    """
    ``rows`` of ``(name, is_dir, size)`` in display order.

    Folders always come first, whichever way the column runs: a file manager
    that interleaves them when sorting by size buries the folders among the
    zero-byte files. ``modified`` maps a name to its epoch seconds.
    """
    modified = modified or {}

    def key(row):
        name, _is_dir, size = row
        if column == SIZE:
            return (int(size or 0), name.lower())
        if column == MODIFIED:
            return (float(modified.get(name) or 0), name.lower())
        return (name.lower(), name)

    folders = sorted((r for r in rows if r[1]), key=key, reverse=descending)
    files = sorted((r for r in rows if not r[1]), key=key, reverse=descending)
    return folders + files


def validate_new_name(name, existing=()) -> str:
    """Why *name* cannot be created or renamed to, or "" if it can."""
    name = str(name or "")
    if not name.strip():
        return "A name is required."
    if name in (".", ".."):
        return f"'{name}' is not a usable name."
    bad = sorted(ch for ch in set(name) if ch in _FORBIDDEN_CHARS)
    if bad:
        shown = " ".join(repr(ch) for ch in bad)
        return f"A name cannot contain {shown}."
    if name != name.rstrip(" ."):
        return "A name cannot end with a space or a dot."
    if name.split(".")[0].upper() in _RESERVED_WINDOWS:
        return f"'{name}' is reserved on Windows."
    if name in set(existing or ()):
        return f"'{name}' already exists here."
    return ""


def child_path(folder, name) -> str:
    """``folder/name``, refusing a name that would land anywhere else."""
    problem = validate_new_name(name)
    if problem:
        raise ValueError(problem)
    path = os.path.join(folder, name)
    if os.path.dirname(os.path.abspath(path)) != os.path.abspath(folder):
        raise ValueError(f"'{name}' is not inside {folder}")
    return path
