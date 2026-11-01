"""Naming, sizing and timestamp helpers.

Nothing here knows about Qt or boto3: these are the small decisions -- what
kind of file a key names, how big a number reads, what "modified" means when
four layers spell it four ways -- that the UI and the S3 layer both make.
"""

import os
from datetime import datetime, timezone


DOC_EXT = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".txt", ".md", ".rtf", ".odt", ".ods", ".odp", ".csv",
}
MEDIA_EXT = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff",
    ".mp3", ".wav", ".flac", ".ogg", ".m4a",
    ".mp4", ".mkv", ".mov", ".avi", ".webm",
}
def _to_epoch(v) -> int:
    """
    Conversion of various 'modified' representations to epoch seconds.

    Supports:
      - int/float epoch
      - datetime
      - ISO-like strings: "2026-02-08 18:59:33", "2026-02-08T18:59:33", etc.
    """
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, datetime):
        try:
            return int(v.timestamp())
        except Exception:
            return 0
    s = str(v).strip()
    if not s:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except Exception:
            pass
    # If it has microseconds: 2026-02-08 18:59:33.123
    try:
        if "." in s:
            s2 = s.split(".", 1)[0]
            return int(datetime.strptime(s2, "%Y-%m-%d %H:%M:%S").timestamp())
    except Exception:
        pass
    # ISO 8601 with timezone offset / microseconds
    # (e.g. boto3 LastModified -> "2026-02-08 18:59:33+00:00")
    try:
        norm = s.replace("T", " ")
        if norm.endswith("Z"):
            norm = norm[:-1] + "+00:00"
        return int(datetime.fromisoformat(norm).timestamp())
    except Exception:
        pass
    return 0
AUDIO_PREVIEW_EXT = {".mp3", ".wav", ".ogg", ".oga", ".opus", ".flac",
                     ".m4a", ".aac", ".wma"}
VIDEO_PREVIEW_EXT = {".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi",
                     ".ogv", ".wmv", ".mpg", ".mpeg"}


def media_kind(key, content_type="") -> str:
    """
    "audio", "video" or "" for what the preview should stream.

    The extension decides first; the stored Content-Type only fills in for a
    key without a recognised one, because uploads by other tools are
    routinely stamped ``binary/octet-stream`` whatever they hold.
    """
    ext = os.path.splitext(str(key or "").lower())[1]
    if ext in AUDIO_PREVIEW_EXT:
        return "audio"
    if ext in VIDEO_PREVIEW_EXT:
        return "video"
    ctype = str(content_type or "").lower()
    if ctype.startswith("audio/"):
        return "audio"
    if ctype.startswith("video/"):
        return "video"
    return ""


def format_media_time(ms) -> str:
    """``m:ss``, or ``h:mm:ss`` past an hour, for a player position."""
    seconds = max(0, int(ms or 0) // 1000)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"
def categorize_key(key: str) -> str:
    k = (key or "").lower()
    _, ext = os.path.splitext(k)
    if ext in DOC_EXT:
        return "Documents"
    if ext in MEDIA_EXT:
        return "Media"
    return "Other"
def _human_bytes(n):
    n = float(n or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    return f"{n:.1f} {units[i]}"
def _dest_inside_source(src_key: str, dst_key: str, is_folder: bool) -> bool:
    """True when a copy/move target is the source itself or nests inside it.
    Moving 'photos/' into 'photos/2024/' copies the tree into itself and the
    follow-up delete of the source prefix then destroys the fresh copy."""
    if src_key == dst_key:
        return True
    return bool(is_folder) and dst_key.startswith(src_key)


def as_epoch(value) -> float:
    """
    Best-effort epoch seconds for an S3 LastModified / local mtime.
    Naive datetimes are read as UTC; anything unusable becomes 0.0.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return float(value.timestamp())
    except Exception:
        return 0.0
