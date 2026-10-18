"""Compiling a search form into one predicate over listing rows."""

import os
import re

from core.naming import as_epoch


def build_search_matcher(query: str = "", *, regex: bool = False,
                        case_sensitive: bool = False,
                        min_size=None, max_size=None,
                        modified_after=None, modified_before=None,
                        extensions=None):
    """
    Compile the search filters once into ``match(key, size, modified)``.

    Raises ValueError for an invalid regular expression so the UI can
    report it instead of failing mid-listing.
    """
    needle = query or ""
    compiled = None
    if regex and needle:
        try:
            compiled = re.compile(
                needle, 0 if case_sensitive else re.IGNORECASE)
        except re.error as exc:
            raise ValueError(f"Invalid regular expression: {exc}") from exc
    elif not case_sensitive:
        needle = needle.lower()

    wanted_exts = None
    if extensions:
        wanted_exts = set()
        for raw in extensions:
            ext = (raw or "").strip().lower()
            if not ext:
                continue
            wanted_exts.add(ext if ext.startswith(".") else "." + ext)
        if not wanted_exts:
            wanted_exts = None

    after = as_epoch(modified_after) if modified_after is not None else None
    before = as_epoch(modified_before) if modified_before is not None else None

    def _match(key, size, modified):
        if compiled is not None:
            if not compiled.search(key):
                return False
        elif needle:
            haystack = key if case_sensitive else key.lower()
            if needle not in haystack:
                return False
        if min_size is not None and int(size or 0) < int(min_size):
            return False
        if max_size is not None and int(size or 0) > int(max_size):
            return False
        if wanted_exts is not None:
            if os.path.splitext(key)[1].lower() not in wanted_exts:
                return False
        if after is not None or before is not None:
            stamp = as_epoch(modified)
            if after is not None and stamp < after:
                return False
            if before is not None and stamp > before:
                return False
        return True

    return _match
