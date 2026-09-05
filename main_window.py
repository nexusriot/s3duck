import csv
import difflib
import fnmatch
import io
import json
import os
import re
import sys
import pathlib
import time
import zipfile
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
import threading

try:
    from PyQt6 import sip
except ImportError:
    sip = None

# Optional: shipped with PyQt6 but absent from stripped-down builds.
try:
    from PyQt6.QtPdf import QPdfDocument
    from PyQt6.QtPdfWidgets import QPdfView
except ImportError:
    QPdfDocument = None
    QPdfView = None

from PyQt6 import QtCore
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import *
from PyQt6.QtCore import *
from PyQt6.QtGui import QIcon, QStandardItemModel, QStandardItem, QAction
from PyQt6.QtGui import QFontDatabase, QShortcut, QKeySequence, QPainter, QPen, QColor, QFont, QFontMetrics, QPalette
from PyQt6.QtGui import QDesktopServices, QPixmap, QActionGroup
from PyQt6.QtGui import QDrag, QSyntaxHighlighter, QTextCharFormat
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, QMimeData, QRectF, QUrl
from model import Model as DataModel
from model import FSObjectType
from model import TransferCancelled
from model import run_parallel
from model import CHECKSUM_ALGORITHMS, plan_prefix_download, prefix_of
from utils import (
    CredentialError, CredentialProcessError, FuncWorker, TempWorkspace,
    expiry_state, join_qthread, load_aws_profiles, normalize_accent,
    reap_finished_workers, release_worker_on_finish, require_crypto,
    describe_client_error, forward_icon, is_transient_error, LogFile,
    resolve_credential_key, run_credential_process, run_with_progress,
    scan_local_tree, themed_icon,
)
from properties_window import PropertiesWindow
import diagnostics
from profile_switcher import ProfileSwitchWindow, load_profiles, decrypt_profile
from theme import apply_theme, THEMES


OS_FAMILY_MAP = {"Linux": "🐧", "Windows": "⊞ Win", "Darwin": " MacOS"}
__VERSION__ = "0.20.1"

UP_ENTRY_LABEL = "[..]"  # special row to go one level up

# Listing columns. Storage class and ETag come free with ListObjectsV2 but are
# hidden by default; the header context menu toggles them.
LIST_COLUMNS = ("Name", "Size", "Modified", "Storage class", "ETag")
LIST_COLUMN_DEFAULT_WIDTHS = (320, 80, 140, 110, 240)
LIST_OPTIONAL_COLUMNS = (3, 4)

PROGRESS_EMIT_INTERVAL_SEC = 0.6   # ~1.6 updates/sec
PROGRESS_MIN_BYTE_DELTA = 1 * 1024 * 1024  # also emit if at least 1MB progressed
TICK_INTERVAL_MS = 600             # UI tick
# How often the credential countdown is refreshed. A minute is fine: the
# warning window is fifteen.
CREDENTIAL_CHECK_INTERVAL_MS = 60 * 1000
# How long to wait before re-queueing a job the service asked us to retry.
AUTO_RETRY_DELAY_MS = 5000
# How often a long listing reports its running count.
LISTING_COUNT_INTERVAL_SEC = 0.4
# Entries fetched for one listing before it stops and says so. A prefix that
# large is not browsable anyway; Search is the tool for it.
DEFAULT_LISTING_LIMIT = 50000
EMA_ALPHA = 0.15                   # smoother rate
RATE_WINDOW_SEC = 2.0              # window for instantaneous rate


class NavigationWorker(QObject):
    finished = pyqtSignal(int, object, str)  # seq, payload, err_str
    counted = pyqtSignal(int, int)           # seq, entries so far

    def __init__(self, data_model_clone, seq: int, bucket: str, prefix: str,
                 max_items: int = 0):
        super().__init__()
        self._max_items = int(max_items or 0)
        # Private model clone — the worker owns its own boto3 client so it
        # cannot race the main thread or other navigation workers on shared
        # client/region/endpoint state.
        self._dm = data_model_clone
        self._seq = seq
        self._bucket = bucket or ""
        self._prefix = prefix or ""

    def _capture_state(self):
        return {
            "endpoint_url": self._dm.endpoint_url,
            "region_name": self._dm.region_name,
            "use_path": self._dm.use_path,
        }

    @pyqtSlot()
    def run(self):
        try:
            if not self._bucket:
                buckets = self._dm.list_buckets()
                payload = {
                    "mode": "bucket_list",
                    "buckets": buckets,
                    "promoted": self._capture_state(),
                }
            else:
                # Emitted per page: a prefix with tens of thousands of direct
                # children used to spend a silent minute here.
                last = [0.0]

                def _page(count):
                    now = time.monotonic()
                    if count and (now - last[0]) >= LISTING_COUNT_INTERVAL_SEC:
                        last[0] = now
                        self.counted.emit(self._seq, int(count))

                items = self._dm.list(
                    self._prefix, page_cb=_page, max_items=self._max_items)
                payload = {
                    "mode": "bucket_items",
                    "items": items,
                    "bucket": self._bucket,
                    "prefix": self._prefix,
                    "truncated": bool(
                        getattr(self._dm, "last_listing_truncated", False)),
                    "promoted": self._capture_state(),
                }
            self.finished.emit(self._seq, payload, "")
        except Exception as exc:
            self.finished.emit(self._seq, None, str(exc))


class BucketEnterWorker(QObject):
    """Runs enter_bucket (+ endpoint retry) off the main thread."""
    success = pyqtSignal(str)         # bucket_name
    failure = pyqtSignal(str, str)    # bucket_name, error_message
    log_msg = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, data_model, name: str):
        super().__init__()
        self._dm = data_model
        self._name = name

    @pyqtSlot()
    def run(self):
        name = self._name
        first_exc = None
        try:
            self._dm.enter_bucket(name)
            self.success.emit(name)
            self.finished.emit()
            return
        except Exception as exc:
            first_exc = exc
            self.log_msg.emit(f"Open bucket failed for '{name}': {exc}")

        # Try to fetch hints (region/endpoint)
        region_hint, endpoint_hint = None, None
        try:
            region_hint, endpoint_hint = self._dm.get_bucket_hints(name)
        except Exception as hint_exc:
            self.log_msg.emit(f"While probing hints: {hint_exc}")

        if region_hint:
            self.log_msg.emit(f"Hint: bucket '{name}' region may be '{region_hint}'")
        else:
            self.log_msg.emit(f"Hint: bucket '{name}' region unknown (no header)")
        if endpoint_hint:
            self.log_msg.emit(f"Hint: suggested endpoint for '{name}': {endpoint_hint}")

        retried = False
        retry_err = None
        if region_hint:
            base_endpoint = self._dm.profile_endpoint_url or self._dm.endpoint_url
            swapped = self._dm.build_region_swapped_endpoint(base_endpoint, region_hint)
            candidate_endpoint = endpoint_hint or swapped
            if candidate_endpoint:
                old_endpoint = self._dm.endpoint_url
                old_region = self._dm.region_name
                old_use_path = self._dm.use_path
                old_client = self._dm._client
                try:
                    self.log_msg.emit(
                        f"Retry: temporarily switching endpoint to '{candidate_endpoint}' "
                        f"and region to '{region_hint}' for bucket '{name}'"
                    )
                    self._dm.endpoint_url = candidate_endpoint
                    self._dm.region_name = region_hint
                    self._dm._client = None
                    self._dm.enter_bucket(name)
                    retried = True
                except Exception as rexc:
                    retry_err = rexc
                    self.log_msg.emit(f"Retry failed for '{name}': {rexc}")
                    self._dm.endpoint_url = old_endpoint
                    self._dm.region_name = old_region
                    self._dm.use_path = old_use_path
                    self._dm._client = old_client

        if retried:
            self.success.emit(name)
        else:
            self.failure.emit(name, str(retry_err or first_exc))
        self.finished.emit()


STALL_DECAY_INTERVAL_SEC = 2.0     # when no progress, decay displayed rate

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


def _build_upload_job_for_path(path: str, dest_prefix: str) -> list:
    """
    Build upload job entries [(key, local_path_or_None), ...] for one local
    file or directory tree, rooted under dest_prefix. Directories contribute
    a placeholder entry (local None) per directory level plus one entry per
    file. os.walk stays inside 'path'; a glob on path + "**/**" also matched
    sibling dirs sharing the name prefix.
    """
    if not path:
        return []
    path = os.path.normpath(path)
    base_path, _tail = os.path.split(path)
    dest_prefix = dest_prefix or ""
    job = []
    if os.path.isdir(path):
        for dirpath, _dirnames, filenames in os.walk(path):
            dir_key = pathlib.Path(
                os.path.join(dest_prefix, os.path.relpath(dirpath, base_path))
            ).as_posix()
            job.append((dir_key, None))
            for filename in filenames:
                full = os.path.join(dirpath, filename)
                key = pathlib.Path(
                    os.path.join(dest_prefix, os.path.relpath(full, base_path))
                ).as_posix()
                job.append((key, full))
    else:
        key = pathlib.Path(
            os.path.join(dest_prefix, os.path.relpath(path, base_path))
        ).as_posix()
        job.append((key, path))
    return job


BULK_RENAME_FIND = "find"
BULK_RENAME_TEMPLATE = "template"

# Filesystem and S3 timestamps disagree by up to a couple of seconds
# (second resolution remotely, 2s granularity on some local filesystems), so
# only a larger difference counts as "newer".
SYNC_MTIME_TOLERANCE_SEC = 2.0


def bulk_rename_plan(items, *, mode=BULK_RENAME_FIND, find="", replace="",
                     regex=False, case_sensitive=True,
                     template="{name}{ext}", start=1, padding=1):
    """
    Work out new names for a multi-selection.

    ``items`` is [(name, is_folder), ...]. Returns ``(plan, problems)`` where
    plan is [(old_name, new_name), ...] for entries that actually change and
    problems is a list of human-readable reasons a rename was rejected.

    Template placeholders: {name} (stem, or whole name for a folder), {ext}
    (".txt" or ""), {n} (counter, zero-padded to *padding*), {orig}.
    """
    plan = []
    problems = []
    counter = int(start)

    for name, is_folder in items:
        if is_folder:
            stem, ext = name, ""
        else:
            stem, ext = os.path.splitext(name)

        if mode == BULK_RENAME_FIND:
            if not find:
                continue
            if regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    new = re.sub(find, replace, name, flags=flags)
                except re.error as exc:
                    return [], [f"Invalid regular expression: {exc}"]
            elif case_sensitive:
                new = name.replace(find, replace)
            else:
                # A lambda replacement keeps backslashes in *replace* literal.
                new = re.sub(re.escape(find), lambda _m: replace, name,
                             flags=re.IGNORECASE)
        else:
            try:
                new = template.format(
                    name=stem, ext=ext, orig=name,
                    n=str(counter).zfill(max(1, int(padding))),
                )
            except (KeyError, IndexError) as exc:
                return [], [f"Unknown placeholder in template: {exc}"]
            counter += 1

        new = (new or "").strip()
        if new == name:
            continue
        if not new:
            problems.append(f"{name}: new name would be empty")
            continue
        if "/" in new:
            problems.append(f"{name}: new name cannot contain '/'")
            continue
        plan.append((name, new))

    targets = {}
    for old, new in plan:
        targets.setdefault(new, []).append(old)
    for new, olds in sorted(targets.items()):
        if len(olds) > 1:
            problems.append(
                f"'{new}' would be produced by {len(olds)} items: "
                + ", ".join(sorted(olds))
            )
    return plan, problems


def _etag_is_comparable(etag) -> bool:
    """
    True when an ETag pins the content on its own.

    A single-part upload's ETag is the MD5 of the body. A multipart ETag is
    '<md5-of-part-md5s>-<n>': still deterministic, so two objects sharing one
    are identical — but two objects with *different* multipart ETags may still
    hold the same bytes if they were uploaded with different part sizes.
    """
    return bool(etag) and "-" not in etag


def find_duplicate_groups(entries, *, min_size=1) -> list:
    """
    Group objects that hold the same content.

    ``entries`` is an iterable of ``(key, size, etag, last_modified)``.
    Returns groups sorted by reclaimable bytes, each a dict of
    ``{size, etag, members, count, wasted, confirmed}`` where members are
    ``(key, last_modified)`` sorted by key.

    Objects sharing a size *and* an ETag are confirmed duplicates. Objects
    sharing only a size are reported as unconfirmed **only** when their ETags
    cannot settle it (a multipart or missing ETag is involved); two distinct
    plain-MD5 ETags prove the contents differ, so those are never grouped.
    Zero-byte objects are excluded by default — every empty file matches every
    other, which is noise rather than a finding.
    """
    by_size = {}
    for entry in entries or []:
        key, size, etag, modified = (list(entry) + [None] * 4)[:4]
        if not key or str(key).endswith("/"):
            continue
        size = int(size or 0)
        if size < int(min_size):
            continue
        by_size.setdefault(size, []).append(
            (str(key), (etag or "").replace('"', "").strip(), modified))

    groups = []
    for size, rows in by_size.items():
        by_etag = {}
        for key, etag, modified in rows:
            by_etag.setdefault(etag, []).append((key, modified))

        leftovers = []
        for etag, members in by_etag.items():
            if etag and len(members) >= 2:
                groups.append({
                    "size": size,
                    "etag": etag,
                    "members": sorted(members, key=lambda m: m[0]),
                    "count": len(members),
                    "wasted": size * (len(members) - 1),
                    "confirmed": True,
                })
            else:
                leftovers.extend((key, etag, modified)
                                 for key, modified in members)

        # Same size, unresolved ETags: only a real candidate when at least one
        # side cannot be compared by ETag at all.
        if len(leftovers) >= 2 and any(
                not _etag_is_comparable(etag) for _k, etag, _m in leftovers):
            members = sorted(((key, modified) for key, _e, modified in leftovers),
                             key=lambda m: m[0])
            groups.append({
                "size": size,
                "etag": "",
                "members": members,
                "count": len(members),
                "wasted": size * (len(members) - 1),
                "confirmed": False,
            })

    groups.sort(key=lambda g: (-g["wasted"], -g["size"], g["members"][0][0]))
    return groups


def summarize_duplicate_groups(groups) -> dict:
    """Totals for the duplicate report header."""
    groups = list(groups or [])
    confirmed = [g for g in groups if g.get("confirmed")]
    return {
        "groups": len(groups),
        "confirmed_groups": len(confirmed),
        "redundant": sum(g["count"] - 1 for g in groups),
        "wasted": sum(g["wasted"] for g in confirmed),
    }


def select_redundant_keys(groups, keep="newest") -> set:
    """
    Which keys to delete so one copy of each group survives.

    keep is "newest" or "oldest" by last-modified; entries without a timestamp
    sort oldest so a dated copy is preferred as the survivor. Unconfirmed
    groups are never auto-selected — their members are not proven identical.
    """
    if keep not in ("newest", "oldest"):
        raise ValueError("keep must be 'newest' or 'oldest'")
    chosen = set()
    for group in groups or []:
        if not group.get("confirmed"):
            continue
        members = list(group.get("members") or [])
        if len(members) < 2:
            continue
        ordered = sorted(
            members,
            key=lambda m: (m[1] is not None, _to_epoch(m[1]), m[0]),
        )
        survivor = ordered[-1] if keep == "newest" else ordered[0]
        for key, _modified in members:
            if key != survivor[0]:
                chosen.add(key)
    return chosen


def bookmark_label(bucket, prefix) -> str:
    """Default display name for a saved location."""
    bucket = bucket or ""
    prefix = prefix or ""
    return f"{bucket}/{prefix}" if prefix else (bucket or "/")


def parse_bookmarks(raw) -> list:
    """
    Read the stored bookmark list.

    One tab-separated `name<TAB>bucket<TAB>prefix` record per line; malformed
    lines are dropped rather than losing the whole list.
    """
    out = []
    for line in str(raw or "").splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        name, bucket, prefix = (p.strip() for p in parts)
        if not bucket:
            continue
        out.append({
            "name": name or bookmark_label(bucket, prefix),
            "bucket": bucket,
            "prefix": prefix,
        })
    return out


def serialize_bookmarks(bookmarks) -> str:
    """Inverse of parse_bookmarks. Tabs and newlines in a name are stripped so
    a record can never span lines."""
    lines = []
    for entry in bookmarks or []:
        bucket = (entry.get("bucket") or "").strip()
        if not bucket:
            continue
        name = re.sub(r"[\t\r\n]+", " ", entry.get("name") or "").strip()
        prefix = (entry.get("prefix") or "").strip()
        lines.append("\t".join(
            [name or bookmark_label(bucket, prefix), bucket, prefix]))
    return "\n".join(lines)


def add_bookmark_to(bookmarks, name, bucket, prefix) -> tuple:
    """
    Append a bookmark unless that exact location is already saved.

    Returns ``(new_list, added)`` so the caller can report a duplicate rather
    than silently growing the menu.
    """
    entries = list(bookmarks or [])
    for entry in entries:
        if entry.get("bucket") == bucket and (entry.get("prefix") or "") == (prefix or ""):
            return entries, False
    entries.append({
        "name": name or bookmark_label(bucket, prefix),
        "bucket": bucket,
        "prefix": prefix or "",
    })
    return entries, True


def build_paste_job(clip, dst_bucket, dst_prefix):
    """
    Turn a clipboard payload into copy/move job entries.

    clip is {"mode", "bucket", "items": [(name, key, is_folder)]}. Returns
    ``(job, skipped)`` where job rows are
    ``(src_key, dst_key, is_folder, dst_bucket_or_None)`` — dst_bucket is None
    when the paste stays inside the source bucket, matching what the copy/move
    workers expect.
    """
    job = []
    skipped = []
    if not clip:
        return job, skipped
    src_bucket = clip.get("bucket") or ""
    cross_bucket = bool(dst_bucket) and dst_bucket != src_bucket
    prefix = dst_prefix or ""

    for name, src_key, is_folder in clip.get("items") or []:
        dst_key = prefix + name + ("/" if is_folder else "")
        if not cross_bucket and _dest_inside_source(src_key, dst_key, is_folder):
            skipped.append(name)
            continue
        job.append((src_key, dst_key, is_folder,
                    dst_bucket if cross_bucket else None))
    return job, skipped


def build_exclude_matcher(patterns):
    """
    Compile sync exclude patterns into ``match(rel_path) -> bool``.

    A pattern matches if it globs the whole relative path or just the file
    name, so `*.tmp` works at any depth. A pattern ending in `/` (or naming a
    directory) excludes everything beneath it, which is what `node_modules/`
    is expected to do.

    A single string is split here rather than being iterated character by
    character — passing the raw text from an input box is the obvious mistake,
    and its symptom is the lone "*" excluding absolutely everything.
    """
    if isinstance(patterns, str):
        patterns = re.split(r"[,\s]+", patterns)
    cleaned = [p.strip() for p in (patterns or []) if p and p.strip()]
    if not cleaned:
        return lambda _rel: False

    dir_prefixes = []
    globs = []
    for pattern in cleaned:
        if pattern.endswith("/"):
            dir_prefixes.append(pattern.rstrip("/"))
        else:
            globs.append(pattern)

    def _match(rel):
        rel = (rel or "").strip("/")
        if not rel:
            return False
        parts = rel.split("/")
        for prefix in dir_prefixes:
            if rel == prefix or rel.startswith(prefix + "/"):
                return True
            if prefix in parts[:-1]:
                return True
        name = parts[-1]
        for pattern in globs:
            if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern):
                return True
            # a bare directory name also excludes its contents
            if pattern in parts[:-1]:
                return True
        return False

    return _match


def build_sync_plan(local_entries, remote_entries, *, direction,
                    delete_extra=False, tolerance=SYNC_MTIME_TOLERANCE_SEC,
                    exclude=None):
    """
    Compare two ``{rel_path: (size, mtime_epoch)}`` maps and return the list of
    actions needed to make the destination match the source.

    direction "upload" treats local as the source, "download" treats remote as
    the source. Each action is a dict with keys: action
    (upload/download/delete_remote/delete_local/skip), rel, size, reason.
    """
    if direction not in ("upload", "download"):
        raise ValueError("direction must be 'upload' or 'download'")

    is_excluded = exclude if callable(exclude) else build_exclude_matcher(exclude)

    if direction == "upload":
        source, dest = local_entries, remote_entries
        transfer, delete_action = "upload", "delete_remote"
    else:
        source, dest = remote_entries, local_entries
        transfer, delete_action = "download", "delete_local"

    actions = []
    for rel in sorted(source):
        if is_excluded(rel):
            continue
        src_size, src_mtime = source[rel]
        if rel not in dest:
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "missing at destination"})
            continue
        dst_size, dst_mtime = dest[rel]
        if int(src_size) != int(dst_size):
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "size differs"})
        elif float(src_mtime) - float(dst_mtime) > tolerance:
            actions.append({"action": transfer, "rel": rel,
                            "size": src_size, "reason": "source is newer"})
        else:
            actions.append({"action": "skip", "rel": rel,
                            "size": src_size, "reason": "up to date"})

    for rel in sorted(dest):
        if rel in source or is_excluded(rel):
            continue
        size = dest[rel][0]
        if delete_extra:
            actions.append({"action": delete_action, "rel": rel,
                            "size": size, "reason": "not at source"})
        else:
            actions.append({"action": "skip", "rel": rel, "size": size,
                            "reason": "extra at destination (kept)"})
    return actions


MANIFEST_HEADER = "# s3duck-manifest v1"
MANIFEST_COLUMNS = ("key", "size", "etag", "modified")


def build_manifest(entries, bucket="", prefix="", stamp="") -> str:
    """
    A CSV record of what a prefix held, from one listing.

    Size and ETag come back with ListObjectsV2, so a manifest of a million
    objects costs no more than browsing them — which is the point: an
    integrity record nobody can afford to take is not taken.
    """
    buffer = io.StringIO()
    buffer.write(f"{MANIFEST_HEADER}\n")
    buffer.write(f"# bucket: {bucket}\n")
    buffer.write(f"# prefix: {prefix}\n")
    buffer.write(f"# taken: {stamp}\n")
    writer = csv.writer(buffer)
    writer.writerow(MANIFEST_COLUMNS)
    for entry in sorted(entries or [], key=lambda row: str(row[0])):
        key, size, etag, modified = (list(entry) + [None] * 4)[:4]
        writer.writerow([str(key), int(size or 0), str(etag or ""),
                         "" if modified is None else str(modified)])
    return buffer.getvalue()


def parse_manifest(text) -> tuple:
    """
    Read a manifest back as ``(meta, {key: (size, etag)})``.

    Raises ValueError on anything that is not one of ours — verifying against
    an arbitrary CSV would report every object as changed.
    """
    lines = str(text or "").splitlines()
    if not lines or not lines[0].startswith(MANIFEST_HEADER):
        raise ValueError("This is not an s3duck manifest.")
    meta = {}
    body = []
    for line in lines[1:]:
        if line.startswith("#"):
            label, _, value = line[1:].partition(":")
            meta[label.strip()] = value.strip()
        else:
            body.append(line)
    if not body:
        raise ValueError("The manifest has no entries.")
    reader = csv.reader(body)
    header = next(reader, [])
    if [column.strip() for column in header] != list(MANIFEST_COLUMNS):
        raise ValueError("The manifest columns are not the expected ones.")
    records = {}
    for row in reader:
        if len(row) < 3 or not row[0]:
            continue
        try:
            size = int(row[1])
        except (TypeError, ValueError):
            continue
        records[row[0]] = (size, str(row[2] or ""))
    return meta, records


def compare_manifest(stored, current) -> dict:
    """
    Diff a manifest against a fresh listing.

    Returns ``{"missing", "added", "changed", "same"}`` — changed carries
    ``(key, reason)`` so "the size is the same but the content is not" reads
    differently from "it grew".
    """
    stored = dict(stored or {})
    current = dict(current or {})
    missing = sorted(set(stored) - set(current))
    added = sorted(set(current) - set(stored))
    changed = []
    same = 0
    for key in sorted(set(stored) & set(current)):
        was_size, was_etag = stored[key]
        now_size, now_etag = current[key]
        if int(was_size) != int(now_size):
            changed.append((key, f"size {was_size} → {now_size}"))
        elif was_etag and now_etag and was_etag != now_etag:
            changed.append((key, "same size, different content"))
        else:
            same += 1
    return {"missing": missing, "added": added, "changed": changed,
            "same": same}


def build_prefix_tree(entries, separator="/") -> dict:
    """
    Nest ``(key, size)`` pairs into ``{name: {size, count, children}}``.

    The usage pie answers "what kind of files"; this answers "which folder",
    which is the one a bill is actually made of.
    """
    root = {"size": 0, "count": 0, "children": {}}
    for key, size in entries or []:
        text = str(key or "")
        if not text or text.endswith(separator):
            continue
        size = int(size or 0)
        node = root
        node["size"] += size
        node["count"] += 1
        parts = [part for part in text.split(separator) if part]
        for part in parts[:-1]:
            node = node["children"].setdefault(
                part, {"size": 0, "count": 0, "children": {}})
            node["size"] += size
            node["count"] += 1
        if parts:
            leaf = node["children"].setdefault(
                parts[-1], {"size": 0, "count": 0, "children": {}})
            leaf["size"] += size
            leaf["count"] += 1
    return root


def node_at_path(root, path) -> dict:
    """Walk a prefix tree to ``path`` (a list of names), or {} if it is gone."""
    node = root or {}
    for part in path or []:
        node = (node.get("children") or {}).get(part)
        if node is None:
            return {}
    return node


def treemap_children(node, limit=80) -> list:
    """
    ``[(name, size, is_group)]`` for one level, biggest first.

    Everything past the limit is folded into one "other" tile: a thousand
    slivers is not a picture of anything.
    """
    rows = sorted(((name, child["size"])
                   for name, child in (node.get("children") or {}).items()),
                  key=lambda row: row[1], reverse=True)
    rows = [row for row in rows if row[1] > 0]
    if len(rows) <= limit:
        return [(name, size, False) for name, size in rows]
    head = [(name, size, False) for name, size in rows[:limit]]
    rest = sum(size for _name, size in rows[limit:])
    if rest:
        head.append((f"… {len(rows) - limit} more", rest, True))
    return head


def _treemap_row(sizes, x, y, dx, dy) -> list:
    """Lay one run of areas along the shorter side of the box."""
    covered = sum(sizes)
    rects = []
    if covered <= 0:
        return rects
    if dx >= dy:
        width = covered / dy if dy else 0
        for size in sizes:
            height = size / width if width else 0
            rects.append((x, y, width, height))
            y += height
    else:
        height = covered / dx if dx else 0
        for size in sizes:
            width = size / height if height else 0
            rects.append((x, y, width, height))
            x += width
    return rects


def _treemap_worst(sizes, dx, dy) -> float:
    """The worst aspect ratio a run would produce; lower is squarer."""
    rects = _treemap_row(sizes, 0, 0, dx, dy)
    worst = 0.0
    for _x, _y, width, height in rects:
        if width <= 0 or height <= 0:
            return float("inf")
        worst = max(worst, width / height, height / width)
    return worst


def squarify(values, x=0.0, y=0.0, width=1.0, height=1.0) -> list:
    """
    Squarified treemap rectangles for *values*, in the same order.

    Iterative rather than recursive: a bucket with thousands of prefixes would
    otherwise recurse once per row.
    """
    values = [float(value) for value in values or []]
    if not values or width <= 0 or height <= 0:
        return []
    total = sum(values)
    if total <= 0:
        return []
    scale = (width * height) / total
    areas = [value * scale for value in values]

    rects = []
    index = 0
    while index < len(areas):
        run = 1
        while (index + run < len(areas)
               and _treemap_worst(areas[index:index + run], width, height)
               >= _treemap_worst(areas[index:index + run + 1], width, height)):
            run += 1
        current = areas[index:index + run]
        rects.extend(_treemap_row(current, x, y, width, height))
        covered = sum(current)
        if width >= height:
            used = covered / height if height else 0
            x += used
            width -= used
        else:
            used = covered / width if width else 0
            y += used
            height -= used
        index += run
        if width <= 0 or height <= 0:
            break
    return rects


def build_compare_plan(left, right, *, tolerance=SYNC_MTIME_TOLERANCE_SEC,
                       exclude=None) -> list:
    """
    Diff two ``{rel_path: (size, mtime)}`` maps without picking a direction.

    Returns ``[{rel, status, left_size, right_size}]`` where status is
    "only_left", "only_right", "differs" or "same". Direction-free on purpose:
    the same comparison drives a copy either way, and calling one side "the
    source" before the user has said so is how a compare turns into a sync
    nobody asked for.
    """
    is_excluded = exclude if callable(exclude) else build_exclude_matcher(exclude)
    rows = []
    for rel in sorted(set(left or {}) | set(right or {})):
        if is_excluded(rel):
            continue
        here = (left or {}).get(rel)
        there = (right or {}).get(rel)
        if here is None:
            rows.append({"rel": rel, "status": "only_right",
                         "left_size": None, "right_size": int(there[0])})
            continue
        if there is None:
            rows.append({"rel": rel, "status": "only_left",
                         "left_size": int(here[0]), "right_size": None})
            continue
        same_size = int(here[0]) == int(there[0])
        close_enough = abs(float(here[1]) - float(there[1])) <= tolerance
        rows.append({
            "rel": rel,
            "status": "same" if (same_size and close_enough) else "differs",
            "left_size": int(here[0]),
            "right_size": int(there[0]),
        })
    return rows


def summarize_compare_plan(rows) -> dict:
    """Count a compare plan by status."""
    counts = {"only_left": 0, "only_right": 0, "differs": 0, "same": 0}
    for row in rows or []:
        status = row.get("status", "same")
        counts[status] = counts.get(status, 0) + 1
    return counts


def summarize_sync_plan(actions) -> dict:
    """Count actions by kind and total the bytes that would move."""
    counts = {}
    total_bytes = 0
    for entry in actions or []:
        kind = entry.get("action", "skip")
        counts[kind] = counts.get(kind, 0) + 1
        if kind in ("upload", "download"):
            total_bytes += int(entry.get("size") or 0)
    counts["bytes"] = total_bytes
    return counts


# Keys with no QAction behind them — handled either in the list view's event
# filter or by a bare QShortcut — so scanning actions cannot find them.
LISTVIEW_KEY_HELP = (
    ("Ctrl+K", "Run any command by name"),
    ("Ctrl+P", "Go to a bucket or bookmark by name"),
    ("Ctrl+F", "Quick filter the current listing"),
    ("Ctrl+Shift+F", "Recursive search under this prefix"),
    ("Ctrl+E", "Sync with a local folder"),
    ("Shift+F2", "Rename multiple items"),
    ("Ctrl+/", "Show this shortcut list"),
    ("Ctrl+C", "Copy selection to the clipboard"),
    ("Ctrl+X", "Cut selection to the clipboard"),
    ("Ctrl+V", "Paste clipboard into this folder"),
    ("Ctrl+B", "Bookmark the current location"),
    ("Ctrl+Shift+D", "Find duplicate objects"),
    ("Ctrl+T", "Open another tab on this location"),
    ("Ctrl+W", "Close the current tab"),
    ("Ctrl+Tab", "Next tab (Ctrl+Shift+Tab for the previous one)"),
    ("F3", "Show or hide the second pane"),
    ("F5", "Dual pane: copy to the other pane — otherwise Refresh"),
    ("F6", "Dual pane: move to the other pane"),
    ("Enter", "Open the selected bucket / folder / file"),
    ("Backspace", "Go up one level"),
    ("Del", "Delete selection"),
    ("Insert", "Create bucket or folder"),
    ("F2", "Rename selected item"),
    ("Home", "Back to the bucket list"),
    ("Esc", "Clear the quick filter, else cancel transfers"),
    ("Any letter", "Start the quick filter (type-to-search)"),
)


def clean_action_label(text) -> str:
    """
    An action's text as a human-facing command name.

    Drops the mnemonic ampersand and the "(Ctrl+C)" hint several labels bake
    in — both are noise once the keys have a column of their own. The ellipsis
    is stripped first, or it holds the hint off the end of the string where
    the pattern anchors.
    """
    label = str(text or "").replace("&", "").strip().rstrip(" …")
    return re.sub(r"\s*\([^)]*\)\s*$", "", label).strip(" …")


def collect_shortcuts(actions, extra=()) -> list:
    """
    Build the (keys, description) rows for the shortcut help.

    Derived from the real QActions so the list cannot drift from the app, with
    the event-filter keys supplied separately.
    """
    rows = []
    seen = set()
    for action in actions or []:
        try:
            # shortcuts() (plural) — an action can carry several, and
            # shortcut() would only ever report the first (F5 but not Ctrl+R).
            bindings = [k.toString() for k in action.shortcuts()]
            label = action.text()
        except Exception:
            continue
        keys = ", ".join(k for k in bindings if k)
        if not keys or not label:
            continue
        label = clean_action_label(label)
        if (keys, label) in seen:
            continue
        seen.add((keys, label))
        rows.append((keys, label))
    rows.sort(key=lambda row: row[1].lower())
    return rows + list(extra)


def location_entries(buckets, bookmarks, current_bucket="") -> list:
    """
    ``(label, hint, (bucket, prefix))`` rows for the quick-open switcher.

    Bookmarks come first because a saved place is almost always what someone
    is reaching for; the bucket that is already open is dropped, since jumping
    to where you already are is not a useful result.
    """
    rows, seen = [], set()
    for entry in bookmarks or []:
        bucket = str((entry or {}).get("bucket") or "").strip()
        if not bucket:
            continue
        prefix = str((entry or {}).get("prefix") or "").strip()
        label = str((entry or {}).get("name") or "").strip() or \
            serialize_location(bucket, prefix)
        key = (bucket, prefix)
        if key in seen:
            continue
        seen.add(key)
        rows.append((label, serialize_location(bucket, prefix), key))
    for name in buckets or []:
        bucket = str(name or "").strip()
        if not bucket or bucket == str(current_bucket or "").strip():
            continue
        key = (bucket, "")
        if key in seen:
            continue
        seen.add(key)
        rows.append((bucket, "bucket", key))
    return rows


def build_profile_sync_job(actions, source_prefix, dest_prefix) -> list:
    """
    Map a sync plan onto ``(operation, src_key, dst_key)`` rows.

    The planner is reused unchanged by feeding it the two profiles' trees as
    source and destination, so its "upload"/"delete_remote" verbs mean
    "copy to the other profile" and "delete over there" here.
    """
    rows = []
    for action in actions or []:
        rel = str(action.get("rel") or "")
        if not rel:
            continue
        kind = action.get("action")
        if kind == "upload":
            rows.append(("copy", source_prefix + rel, dest_prefix + rel))
        elif kind == "delete_remote":
            rows.append(("delete", "", dest_prefix + rel))
    return rows


# Jobs that hold a live connection to another profile, so they cannot be
# reconstructed from persisted history.
CROSS_PROFILE_METHODS = ("copy_to_profile", "sync_to_profile")


def serialize_location(bucket, prefix) -> str:
    """
    A browsing location as one settings string, or "" in bucket-list mode.

    Its own codec rather than reusing the s3:// parser, whose meaning depends
    on which bucket happens to be open when it runs.
    """
    name = str(bucket or "").strip().strip("/")
    if not name:
        return ""
    inner = str(prefix or "").strip().strip("/")
    return f"{name}/{inner}/" if inner else name


def parse_location(text) -> tuple:
    """Reverse serialize_location as (bucket, prefix); prefix ends with "/"."""
    raw = str(text or "").strip().lstrip("/")
    if not raw:
        return "", ""
    name, _, inner = raw.partition("/")
    inner = inner.strip("/")
    return name.strip(), (inner + "/" if inner else "")


def command_entries(actions) -> list:
    """
    ``(label, keys, action)`` for every action the palette can run.

    Built from the live QActions, like the shortcut sheet, so the palette
    cannot drift from the app. Disabled and hidden actions are dropped because
    running one would do nothing, and separators carry no label at all.
    """
    out, seen = [], set()
    for action in actions or []:
        try:
            # isEnabled() is False for a hidden action too, so this covers
            # both. Separators carry no label and cannot be run.
            if action.isSeparator() or not action.isEnabled():
                continue
            raw = action.text() or ""
            keys = ", ".join(
                k.toString() for k in action.shortcuts() if k.toString())
        except Exception:
            continue
        label = clean_action_label(raw)
        if not label or label.lower() in seen:
            continue
        seen.add(label.lower())
        out.append((label, keys, action))
    out.sort(key=lambda row: row[0].lower())
    return out


def palette_score(label, query):
    """
    Rank one command against the typed query; None when it does not match.

    Two tiers so the obvious answer wins: a contiguous substring beats a
    scattered subsequence — otherwise typing "co" would rank "Show incomplete
    uploads" alongside "Copy". Within a tier the earlier match wins, which is
    what puts a prefix match ("Copy") ahead of a later one ("Recopy").
    """
    text = str(label or "").lower()
    needle = str(query or "").strip().lower()
    if not needle:
        return (0, 0, text)
    position = text.find(needle)
    if position >= 0:
        return (0, position, text)
    # An iterator, not the string: the characters must appear IN ORDER, or
    # "yc" would match "Copy".
    remaining = iter(text)
    if all(char in remaining for char in needle):
        return (1, len(text), text)
    return None


def filter_commands(entries, query) -> list:
    """The palette's visible list: matches only, best first."""
    scored = []
    for entry in entries or []:
        score = palette_score(entry[0], query)
        if score is not None:
            scored.append((score, entry))
    scored.sort(key=lambda pair: pair[0])
    return [entry for _score, entry in scored]


class DiagnosticsDialog(QDialog):
    """
    Read-only environment report with a one-click copy.

    Copying matters more than reading it here: the whole point is to get these
    facts into a bug report, where "the venv has QtSvg but the .deb does not"
    is the kind of detail nobody thinks to mention.
    """

    def __init__(self, parent, report):
        super().__init__(parent)
        self.setWindowTitle("Diagnostics")
        self.resize(640, 520)
        self._report = report

        self._text = QPlainTextEdit()
        self._text.setPlainText(report)
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._text.setFont(QFontDatabase.systemFont(
            QFontDatabase.SystemFont.FixedFont))

        self._copy = QPushButton("Copy to clipboard")
        self._copy.clicked.connect(self.copy_report)
        close = QPushButton("Close")
        close.clicked.connect(self.accept)

        row = QHBoxLayout()
        row.addWidget(self._copy)
        row.addStretch(1)
        row.addWidget(close)

        lay = QVBoxLayout(self)
        lay.addWidget(self._text)
        lay.addLayout(row)

    def copy_report(self):
        QApplication.clipboard().setText(self._report)
        self._copy.setText("Copied")


class FilterListDialog(QDialog):
    """
    A query box over a filtered list: type to narrow, arrow to move, Enter to
    pick.

    Shared by the command palette and the quick-open switcher, which were
    identical down to the row-selection accessor. Entries are
    ``(label, extra, payload)``; only the payload's meaning differs.
    """

    def __init__(self, parent, entries, title, placeholder):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(520, 380)
        self._entries = list(entries or [])
        self._visible = []

        self._query = QLineEdit()
        self._query.setPlaceholderText(placeholder)
        self._query.textChanged.connect(self._refresh)
        self._query.installEventFilter(self)

        self._list = QListWidget()
        self._list.itemActivated.connect(lambda _i: self.accept())
        self._list.itemDoubleClicked.connect(lambda _i: self.accept())

        lay = QVBoxLayout(self)
        lay.addWidget(self._query)
        lay.addWidget(self._list)
        self._refresh("")

    def _refresh(self, text):
        self._visible = filter_commands(self._entries, text)
        self._list.clear()
        for label, extra, _payload in self._visible:
            self._list.addItem(f"{label}\t{extra}" if extra else label)
        if self._visible:
            self._list.setCurrentRow(0)

    def eventFilter(self, source, event):
        """Arrows and Enter belong to the list while the cursor is in the box."""
        if source is self._query and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            if key in (Qt.Key.Key_Down, Qt.Key.Key_Up):
                row = self._list.currentRow()
                step = 1 if key == Qt.Key.Key_Down else -1
                self._list.setCurrentRow(
                    max(0, min(self._list.count() - 1, row + step)))
                return True
            if key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                if self._list.count():
                    self.accept()
                return True
        return super().eventFilter(source, event)

    def selected_payload(self):
        """The highlighted row's payload, or None."""
        row = self._list.currentRow()
        if row < 0 or row >= len(self._visible):
            return None
        return self._visible[row][2]


class QuickOpenDialog(FilterListDialog):
    """
    Type-to-jump across bookmarks and buckets.

    Uses the command palette's matcher, so ranking behaves the same in both
    places rather than drifting into two notions of "best match".
    """

    def __init__(self, parent, entries):
        super().__init__(parent, entries, "Go to",
                         "Type a bucket or bookmark…")

    def chosen_location(self):
        """``(bucket, prefix)`` for the highlighted row, or None."""
        return self.selected_payload()


class CommandPaletteDialog(FilterListDialog):
    """
    Type-to-run access to every action.

    The action surface outgrew the toolbar — toolbar, Tools menu, several
    context menus and ~20 shortcuts — so a keyboard-first index is the only
    way to reach a rarely-used command without hunting.
    """

    def __init__(self, parent, entries):
        super().__init__(parent, entries, "Commands", "Type a command…")

    def chosen_action(self):
        """The QAction to trigger, or None."""
        return self.selected_payload()


def format_completion_notification(stats) -> tuple:
    """Build the (title, body) shown when the transfer queue drains."""
    done = int(stats.get("done", 0))
    errors = int(stats.get("error", 0))
    cancelled = int(stats.get("cancelled", 0))
    if errors:
        title = "S3 Duck — transfers failed"
    elif cancelled and not done:
        title = "S3 Duck — transfers cancelled"
    else:
        title = "S3 Duck — transfers finished"
    parts = []
    if done:
        parts.append(f"{done} completed")
    if errors:
        parts.append(f"{errors} failed")
    if cancelled:
        parts.append(f"{cancelled} cancelled")
    return title, ", ".join(parts) or "nothing to do"


def hex_dump(data, width: int = 16, max_bytes: int = 64 * 1024) -> str:
    """Classic offset / hex / ASCII rendering for binary previews."""
    data = data or b""
    truncated = len(data) > max_bytes
    view = data[:max_bytes]
    lines = []
    for offset in range(0, len(view), width):
        block = view[offset:offset + width]
        hex_part = " ".join(f"{b:02x}" for b in block)
        hex_part = hex_part.ljust(width * 3 - 1)
        text = "".join(chr(b) if 32 <= b < 127 else "." for b in block)
        lines.append(f"{offset:08x}  {hex_part}  |{text}|")
    if truncated:
        lines.append(f"… {len(data) - max_bytes} more byte(s) not shown")
    return "\n".join(lines)


class CodeHighlighter(QSyntaxHighlighter):
    """
    Deliberately language-agnostic: strings, comments, numbers and a common
    keyword set. Enough to make code readable without shipping a grammar per
    language.
    """

    KEYWORDS = (
        "and as assert async await break case class const continue def default"
        " del elif else except export extends finally for from func function"
        " global if impl import in interface is lambda let match mod new nil"
        " none not null or pass private protected public raise return select"
        " self static struct switch this throw try type var void while with"
        " yield true false"
    ).split()

    def __init__(self, document):
        super().__init__(document)
        self._rules = []

        keyword_fmt = QTextCharFormat()
        keyword_fmt.setForeground(QColor(86, 156, 214))
        keyword_fmt.setFontWeight(QFont.Weight.Bold)
        self._rules.append((
            re.compile(r"\b(" + "|".join(self.KEYWORDS) + r")\b"), keyword_fmt))

        number_fmt = QTextCharFormat()
        number_fmt.setForeground(QColor(181, 206, 168))
        self._rules.append((re.compile(r"\b\d[\d_.xXa-fA-F]*\b"), number_fmt))

        string_fmt = QTextCharFormat()
        string_fmt.setForeground(QColor(206, 145, 120))
        self._rules.append((
            re.compile(r"'[^'\\]*(?:\\.[^'\\]*)*'"
                       r'|"[^"\\]*(?:\\.[^"\\]*)*"'), string_fmt))

        comment_fmt = QTextCharFormat()
        comment_fmt.setForeground(QColor(106, 153, 85))
        comment_fmt.setFontItalic(True)
        # Applied last so a trailing comment wins over anything inside it.
        self._comment_rule = (re.compile(r"(#|//).*$"), comment_fmt)

    def highlightBlock(self, text):
        for pattern, fmt in self._rules:
            for match in pattern.finditer(text):
                self.setFormat(match.start(), match.end() - match.start(), fmt)
        pattern, fmt = self._comment_rule
        match = pattern.search(text)
        if match:
            self.setFormat(match.start(), len(text) - match.start(), fmt)


def _listing_summary(items) -> str:
    """Status-bar summary of the current listing: counts + total file size."""
    folders = files = 0
    total = 0
    for i in items or []:
        if i.type_ == FSObjectType.FOLDER:
            folders += 1
        elif i.type_ == FSObjectType.FILE:
            files += 1
            total += int(i.size or 0)
    return f"{folders} dir(s), {files} file(s), {_human_bytes(total)}"


class _OneShotClickGuard(QObject):
    """
    Swallows exactly one mouse press+release pair on a target widget.
    Auto-disarms after the pair or a short timeout.
    """
    def __init__(self, target: QWidget, timeout_ms: int = 350):
        super().__init__(target)
        self._target = target
        self._armed = False
        self._need_press = False
        self._need_release = False
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self.disarm)

    def arm(self):
        if self._armed:
            return
        self._armed = True
        self._need_press = True
        self._need_release = True
        self._target.installEventFilter(self)
        self._timer.start(350)

    def disarm(self):
        if not self._armed:
            return
        try:
            self._target.removeEventFilter(self)
        finally:
            self._armed = False
            self._need_press = False
            self._need_release = False

    def eventFilter(self, obj, event):
        if not self._armed:
            return False
        et = event.type()
        if et == QEvent.Type.MouseButtonPress and self._need_press:
            self._need_press = False
            return True
        if et == QEvent.Type.MouseButtonRelease and self._need_release:
            self._need_release = False
            QTimer.singleShot(0, self.disarm)
            return True
        return False


class Tree(QTreeView):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)

    def startDrag(self, supported_actions):
        """
        Drag objects out to a file manager.

        Qt hands the drop target real file URLs, so the selection has to be on
        disk before the drag begins; the parent downloads it to a temp folder
        behind a cancellable progress dialog.
        """
        if self.parent.in_bucket_list_mode():
            return
        paths = self.parent.prepare_drag_files()
        if not paths:
            return
        mime = QMimeData()
        mime.setUrls([QUrl.fromLocalFile(p) for p in paths if os.path.exists(p)])
        if not mime.urls():
            return
        drag = QDrag(self)
        drag.setMimeData(mime)
        drag.exec(Qt.DropAction.CopyAction)

    def dragEnterEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return
        if event.mimeData().hasUrls():
            if not self.parent.in_bucket_list_mode():
                event.accept()
            else:
                event.ignore()
        else:
            event.ignore()
        return

    def dragMoveEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return
        if event.mimeData().hasUrls():
            if not self.parent.in_bucket_list_mode():
                event.setDropAction(Qt.DropAction.MoveAction)
                event.accept()
            else:
                event.ignore()
        else:
            event.ignore()

    def dropEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return

        if self.parent.in_bucket_list_mode():
            event.ignore()
            return

        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction)
            event.accept()

            job = []
            for url in event.mimeData().urls():
                path = str(url.toLocalFile())
                job.extend(_build_upload_job_for_path(
                    path, self.parent.data_model.current_folder))
            job = self.parent._guard_upload(job)
            if not job:
                return
            self.parent.assign_thread_operation("upload", job)
        else:
            event.ignore()


class ListItem(QStandardItem):
    def __init__(self, size, t, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.size = size
        self.t = t  # FSObjectType


class UpTopProxyModel(QSortFilterProxyModel):
    """
    Proxy to:
      - always pin the UP_ENTRY_LABEL row to the very top
      - sort by type priority: BUCKET < FOLDER < FILE
      - within same type, apply existing column-based sort
    """
    def __init__(self, up_label, parent=None):
        super().__init__(parent)
        self.up_label = up_label
        self._order = Qt.SortOrder.AscendingOrder  # remember current sort order
        self._filter_text = ""
        self._glob = False

    def set_filter_text(self, text):
        self._filter_text = (text or "").strip().lower()
        # Only * and ? switch on glob matching. A bare "[" is far more often
        # part of a file name than the start of a character class, and
        # treating it as one silently anchors the pattern.
        self._glob = any(ch in self._filter_text for ch in "*?")
        self.invalidateFilter()

    def matches(self, name) -> bool:
        """Whether one row name passes the current filter."""
        if not self._filter_text:
            return True
        lowered = str(name or "").lower()
        if self._glob:
            return fnmatch.fnmatchcase(lowered, self._filter_text)
        return self._filter_text in lowered

    def filterAcceptsRow(self, source_row, source_parent):
        if not self._filter_text:
            return True
        model = self.sourceModel()
        idx = model.index(source_row, 0, source_parent)
        name = str(model.data(idx) or "")
        # Always keep the "[..]" up-entry visible while filtering.
        if name == self.up_label:
            return True
        return self.matches(name)

    def sort(self, column, order=Qt.SortOrder.AscendingOrder):
        self._order = order
        super().sort(column, order)

    def _is_up_row(self, src_idx: QModelIndex) -> bool:
        base = src_idx.sibling(src_idx.row(), 0)
        return str(base.data()) == self.up_label

    def _item_type(self, src_idx: QModelIndex):
        model = self.sourceModel()
        item = model.itemFromIndex(src_idx.sibling(src_idx.row(), 0))
        return getattr(item, "t", None)

    def lessThan(self, left: QModelIndex, right: QModelIndex) -> bool:
        left_is_up = self._is_up_row(left)
        right_is_up = self._is_up_row(right)
        if left_is_up != right_is_up:
            if self._order == Qt.SortOrder.AscendingOrder:
                return left_is_up and not right_is_up
            else:
                return (not left_is_up) and right_is_up

        lt = self._item_type(left)
        rt = self._item_type(right)

        def _rank(t):
            if t == FSObjectType.BUCKET:
                return 0
            if t == FSObjectType.FOLDER:
                return 1
            if t == FSObjectType.FILE:
                return 2
            return 99

        if lt is not None and rt is not None:
            rl = _rank(lt)
            rr = _rank(rt)
            if rl != rr:
                return rl < rr

        col = left.column()
        model = self.sourceModel()

        def _name(idx: QModelIndex) -> str:
            n = idx.sibling(idx.row(), 0).data()
            return str(n or "").lower()

        if col == 0:
            return _name(left) < _name(right)

        if col == 1:
            l_item = model.itemFromIndex(left)
            r_item = model.itemFromIndex(right)
            ln = getattr(l_item, "size", 0) or 0
            rn = getattr(r_item, "size", 0) or 0
            if ln != rn:
                return int(ln) < int(rn)
            return _name(left) < _name(right)

        if col == 2:
            ld = left.data()
            rd = right.data()
            le = _to_epoch(ld)
            re = _to_epoch(rd)
            if le != re:
                return le < re
            return _name(left) < _name(right)

        return str(left.data() or "").lower() < str(right.data() or "").lower()


class Worker(QObject):
    finished = pyqtSignal(bool)
    progress = pyqtSignal(str)
    batch_progress = pyqtSignal(object, object)

    error = pyqtSignal(str)
    # The same failure with its request id and HTTP status attached — what a
    # provider's support desk asks for, and what str(exc) throws away — plus
    # whether the service was asking for patience rather than saying no.
    details = pyqtSignal(str, bool)

    def __init__(self, data_model, job, dest_model=None):
        super().__init__()
        self.data_model = data_model
        self.job = job
        self.dest_model = dest_model
        self._cancel_event = threading.Event()

    @property
    def _file_workers(self) -> int:
        """How many files to move at once (1 keeps ordering deterministic)."""
        return int(getattr(self.data_model, "parallel_files", 1) or 1)

    @pyqtSlot()
    def cancel(self):
        self._cancel_event.set()
        self.progress.emit("cancel requested…")

    def download(self):
        cancelled = False
        try:
            # total bytes across everything in this batch, including dirs
            total_bytes_all = 0
            for key, local_name, size, folder_path in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                if local_name is not None:
                    total_bytes_all += int(size or 0)
                else:
                    for k, s in self.data_model.get_keys(key, log_fn=self.progress.emit):
                        if self._cancel_event.is_set():
                            raise TransferCancelled("cancelled")
                        if k and not k.endswith("/"):
                            total_bytes_all += int(s or 0)
            total_bytes_all = max(1, int(total_bytes_all))

            done_all = 0
            done_all_lock = threading.Lock()
            throttle_state = {"t": 0.0, "b": 0}

            def emit_throttled(current_total, file_cur, file_total, key):
                now = time.time()
                should_emit = False
                if (now - throttle_state["t"]) >= PROGRESS_EMIT_INTERVAL_SEC:
                    should_emit = True
                elif (current_total - throttle_state["b"]) >= PROGRESS_MIN_BYTE_DELTA:
                    should_emit = True
                elif current_total >= total_bytes_all:
                    should_emit = True

                if should_emit:
                    throttle_state["t"] = now
                    throttle_state["b"] = current_total
                    self.batch_progress.emit(int(current_total), int(total_bytes_all))

            def make_cb():
                last_sent_per_key = {}

                def _cb(total_file, cur_file, key):
                    nonlocal done_all
                    if self._cancel_event.is_set():
                        raise TransferCancelled("cancelled")

                    key = str(key or "")
                    with done_all_lock:
                        prev = int(last_sent_per_key.get(key, 0))
                        cur = int(cur_file)
                        if cur > prev:
                            delta = cur - prev
                            last_sent_per_key[key] = cur
                            done_all += delta
                        current_total = done_all

                    emit_throttled(current_total, int(cur_file), int(total_file or 1), key)

                return _cb

            def _download_one(item):
                key, local_name, size, folder_path = item
                if local_name:
                    msg = "downloading %s -> %s (%s)" % (key, local_name, size)
                else:
                    msg = "downloading directory: %s -> %s" % (key, folder_path)
                self.progress.emit(msg)
                self.data_model.download_file(
                    key, local_name, folder_path,
                    progress_cb=make_cb(),
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )

            run_parallel(self.job, _download_one, self._file_workers,
                         cancel_event=self._cancel_event)

            self.batch_progress.emit(int(done_all), int(total_bytes_all))

        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"download failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)

        finally:
            self.finished.emit(cancelled)

    def delete(self):
        cancelled = False
        try:
            for key in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                msg = "moving %s -> /dev/null" % key
                self.progress.emit(msg)
                self.data_model.delete(key, log_fn=self.progress.emit)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"delete failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def upload(self):
        cancelled = False
        try:
            total_bytes_all = 0
            for key, local_name in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if local_name:
                    try:
                        total_bytes_all += int(os.path.getsize(local_name))
                    except Exception:
                        pass
            total_bytes_all = max(1, int(total_bytes_all))

            done_all = 0
            done_all_lock = threading.Lock()
            throttle_state = {"t": 0.0, "b": 0}

            def emit_throttled(current_total, file_cur, file_total, key):
                now = time.time()
                should_emit = False
                if (now - throttle_state["t"]) >= PROGRESS_EMIT_INTERVAL_SEC:
                    should_emit = True
                elif (current_total - throttle_state["b"]) >= PROGRESS_MIN_BYTE_DELTA:
                    should_emit = True
                elif current_total >= total_bytes_all:
                    should_emit = True

                if should_emit:
                    throttle_state["t"] = now
                    throttle_state["b"] = current_total
                    self.batch_progress.emit(int(current_total), int(total_bytes_all))

            def make_cb():
                last_sent_per_key = {}

                def _cb(total_file, cur_file, key):
                    nonlocal done_all
                    if self._cancel_event.is_set():
                        raise TransferCancelled("cancelled")

                    key = str(key or "")
                    with done_all_lock:
                        prev = int(last_sent_per_key.get(key, 0))
                        cur = int(cur_file)
                        if cur > prev:
                            delta = cur - prev
                            last_sent_per_key[key] = cur
                            done_all += delta
                        current_total = done_all

                    emit_throttled(current_total, int(cur_file), int(total_file or 1), key)

                return _cb

            def _upload_one(item):
                key, local_name = item
                if local_name is not None:
                    msg = "uploading %s -> %s" % (local_name, key)
                else:
                    msg = "creating folder %s" % key
                self.progress.emit(msg)
                self.data_model.upload_file(
                    local_name, key,
                    progress_cb=make_cb() if local_name else None,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )

            run_parallel(self.job, _upload_one, self._file_workers,
                         cancel_event=self._cancel_event)

            self.batch_progress.emit(int(done_all), int(total_bytes_all))

        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"upload failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)

        finally:
            self.finished.emit(cancelled)

    def sync_to_profile(self):
        # job = [(operation, src_key, dst_key)] against self.dest_model
        cancelled = False
        try:
            if self.dest_model is None:
                raise Exception("no destination profile for this sync")
            for operation, src_key, dst_key in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if operation == "copy":
                    self.progress.emit(
                        f"copying {src_key} -> {self.dest_model.bucket}/{dst_key}")
                    self.data_model.copy_to_model(
                        src_key, self.dest_model, dst_key,
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit)
                elif operation == "delete":
                    self.progress.emit(
                        f"deleting {self.dest_model.bucket}/{dst_key}")
                    self.dest_model.delete(
                        dst_key, log_fn=self.progress.emit,
                        cancel_event=self._cancel_event)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"sync to profile failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def copy_to_profile(self):
        # job = [(src_key, dst_key, is_folder)] into self.dest_model
        cancelled = False
        try:
            if self.dest_model is None:
                raise Exception("no destination profile for this copy")
            where = f"{self.dest_model.bucket}"
            for src_key, dst_key, is_folder in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if is_folder:
                    self.progress.emit(
                        f"copying folder {src_key} -> {where}/{dst_key}")
                    self.data_model.copy_prefix_to_model(
                        src_key, self.dest_model, dst_key,
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit)
                else:
                    self.progress.emit(f"copying {src_key} -> {where}/{dst_key}")
                    self.data_model.copy_to_model(
                        src_key, self.dest_model, dst_key,
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"copy to profile failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def copy(self):
        # job = [(src_key, dst_key, is_folder, dst_bucket_or_None)]
        cancelled = False
        try:
            for src_key, dst_key, is_folder, dst_bucket in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                where = f" in {dst_bucket}" if dst_bucket else ""
                if is_folder:
                    self.progress.emit(
                        f"copying folder {src_key} -> {dst_key}{where}")
                    self.data_model.copy_prefix(
                        src_key, dst_key, dst_bucket=dst_bucket,
                        log_fn=self.progress.emit,
                        cancel_event=self._cancel_event,
                    )
                else:
                    self.progress.emit(f"copying {src_key} -> {dst_key}{where}")
                    self.data_model.copy_object(
                        src_key, dst_key, dst_bucket=dst_bucket,
                        log_fn=self.progress.emit)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"copy failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def move(self):
        # job = [(src_key, dst_key, is_folder, dst_bucket_or_None)]
        cancelled = False
        try:
            for src_key, dst_key, is_folder, dst_bucket in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                where = f" in {dst_bucket}" if dst_bucket else ""
                if is_folder:
                    self.progress.emit(
                        f"moving folder {src_key} -> {dst_key}{where}")
                    self.data_model.copy_prefix(
                        src_key, dst_key, dst_bucket=dst_bucket,
                        log_fn=self.progress.emit,
                        cancel_event=self._cancel_event,
                    )
                else:
                    self.progress.emit(f"moving {src_key} -> {dst_key}{where}")
                    self.data_model.copy_object(
                        src_key, dst_key, dst_bucket=dst_bucket,
                        log_fn=self.progress.emit)
            # Sources are removed only after every copy succeeded, so a failure
            # mid-way never leaves the data deleted-but-not-copied.
            for src_key, _dst_key, _is_folder, _dst_bucket in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                self.progress.emit(f"removing {src_key}")
                self.data_model.delete(src_key, log_fn=self.progress.emit)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"move failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def delete_buckets(self):
        # job = [(bucket_name, recursive)]
        cancelled = False
        failures = []
        try:
            for bucket_name, recursive in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                try:
                    if recursive:
                        self.progress.emit(f"deleting bucket {bucket_name} (recursive)")
                        self.data_model.delete_bucket_recursive(
                            bucket_name,
                            cancel_event=self._cancel_event,
                            log_fn=self.progress.emit,
                        )
                    else:
                        self.progress.emit(f"deleting bucket {bucket_name}")
                        self.data_model.delete_bucket(bucket_name)
                    self.progress.emit(f"deleted bucket {bucket_name}")
                except TransferCancelled:
                    raise
                except Exception as exc:
                    # One bad bucket must not abandon the rest of the batch.
                    failures.append(f"{bucket_name}: {exc}")
                    self.progress.emit(f"delete failed for {bucket_name}: {exc}")
            if failures:
                self.error.emit(
                    "Some buckets could not be deleted:\n\n" + "\n".join(failures)
                )
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"bucket delete failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def sync(self):
        """
        job = [(action, rel, local_path, key, size)] — already filtered to the
        actions the user approved in the dry-run plan.
        """
        cancelled = False
        try:
            total_bytes_all = max(1, sum(
                int(size or 0) for action, _rel, _lp, _k, size in self.job
                if action in ("upload", "download")
            ))
            done_all = 0
            done_lock = threading.Lock()
            throttle = {"t": 0.0, "b": 0}

            def _emit(current):
                now = time.time()
                if ((now - throttle["t"]) >= PROGRESS_EMIT_INTERVAL_SEC
                        or (current - throttle["b"]) >= PROGRESS_MIN_BYTE_DELTA
                        or current >= total_bytes_all):
                    throttle["t"] = now
                    throttle["b"] = current
                    self.batch_progress.emit(int(current), int(total_bytes_all))

            def _make_cb():
                seen = {}

                def _cb(_total_file, cur_file, key):
                    nonlocal done_all
                    if self._cancel_event.is_set():
                        raise TransferCancelled("cancelled")
                    key = str(key or "")
                    with done_lock:
                        prev = int(seen.get(key, 0))
                        cur = int(cur_file)
                        if cur > prev:
                            seen[key] = cur
                            done_all += cur - prev
                        current = done_all
                    _emit(current)

                return _cb

            def _sync_one(item):
                action, rel, local_path, key, _size = item
                if action == "upload":
                    self.progress.emit(f"sync upload {rel}")
                    self.data_model.upload_file(
                        local_path, key, progress_cb=_make_cb(),
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit,
                    )
                elif action == "download":
                    self.progress.emit(f"sync download {rel}")
                    parent = os.path.dirname(local_path)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    self.data_model.download_file(
                        key, local_path, parent, progress_cb=_make_cb(),
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit,
                    )
                elif action == "delete_remote":
                    self.progress.emit(f"sync delete remote {rel}")
                    self.data_model.delete(key, log_fn=self.progress.emit)
                elif action == "delete_local":
                    self.progress.emit(f"sync delete local {rel}")
                    try:
                        os.remove(local_path)
                    except OSError as exc:
                        self.progress.emit(f"could not delete {local_path}: {exc}")

            run_parallel(self.job, _sync_one, self._file_workers,
                         cancel_event=self._cancel_event)

            self.batch_progress.emit(int(done_all), int(total_bytes_all))
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"sync failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def set_tags(self):
        # job = [(key, is_folder, add_dict, remove_list, replace_bool)]
        cancelled = False
        changed = 0
        try:
            for key, is_folder, add, remove, replace in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for k in self._iter_object_keys(key, is_folder):
                    self.data_model.update_object_tags(
                        k, add=add, remove=remove, replace=replace)
                    changed += 1
                    self.progress.emit(f"tagged {k}")
            self.progress.emit(f"tagging done: {changed} object(s)")
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"tagging failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def zip_download(self):
        """
        job = [(zip_path, base_prefix, key, is_folder)] — every row carries the
        same archive path and base prefix. Objects stream straight into the zip
        so nothing is staged on disk twice.
        """
        cancelled = False
        archive = None
        try:
            zip_path = self.job[0][0]
            base_prefix = self.job[0][1]

            # Expand folders and total the bytes before opening the archive.
            entries = []
            total_bytes_all = 0
            for _zp, _bp, key, is_folder in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if is_folder:
                    for k, size in self.data_model.get_keys(
                            key, log_fn=self.progress.emit):
                        if not k or k.endswith("/"):
                            continue
                        entries.append((k, int(size or 0)))
                        total_bytes_all += int(size or 0)
                else:
                    size = self.data_model.get_size(key)
                    entries.append((key, int(size or 0)))
                    total_bytes_all += int(size or 0)
            total_bytes_all = max(1, total_bytes_all)

            done = 0
            throttle = {"t": 0.0, "b": 0}

            def _emit(current):
                now = time.time()
                if ((now - throttle["t"]) >= PROGRESS_EMIT_INTERVAL_SEC
                        or (current - throttle["b"]) >= PROGRESS_MIN_BYTE_DELTA
                        or current >= total_bytes_all):
                    throttle["t"] = now
                    throttle["b"] = current
                    self.batch_progress.emit(int(current), int(total_bytes_all))

            archive = zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
            for key, _size in entries:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                arcname = key[len(base_prefix):] if key.startswith(base_prefix) else key
                arcname = arcname.lstrip("/") or os.path.basename(key)
                self.progress.emit(f"archiving {key}")
                with archive.open(arcname, "w") as target:
                    for block in self.data_model.stream_object(
                            key, cancel_event=self._cancel_event):
                        target.write(block)
                        done += len(block)
                        _emit(done)
            self.batch_progress.emit(int(done), int(total_bytes_all))
            self.progress.emit(f"wrote {len(entries)} object(s) to {zip_path}")
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"zip download failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            if archive is not None:
                try:
                    archive.close()
                except Exception:
                    pass
            # A cancelled or failed archive is unusable; do not leave it behind.
            if cancelled:
                try:
                    os.remove(self.job[0][0])
                except OSError:
                    pass
            self.finished.emit(cancelled)

    def undelete(self):
        # job = [(key,)]
        cancelled = False
        restored = 0
        try:
            for (key,) in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                self.progress.emit(f"restoring {key}")
                restored += self.data_model.undelete(key, log_fn=self.progress.emit)
            if restored:
                self.progress.emit(f"restored {restored} object(s)")
            else:
                self.progress.emit(
                    "nothing to restore — the bucket has no delete markers "
                    "(versioning was not enabled, so the delete was permanent)"
                )
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"undo failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def empty_buckets(self):
        # job = [(bucket_name,)]
        cancelled = False
        failures = []
        try:
            for (bucket_name,) in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                try:
                    self.progress.emit(f"emptying bucket {bucket_name}")
                    self.data_model.empty_bucket(
                        bucket_name,
                        cancel_event=self._cancel_event,
                        log_fn=self.progress.emit,
                    )
                    self.progress.emit(f"emptied bucket {bucket_name}")
                except TransferCancelled:
                    raise
                except Exception as exc:
                    failures.append(f"{bucket_name}: {exc}")
                    self.progress.emit(f"emptying failed for {bucket_name}: {exc}")
            if failures:
                self.error.emit(
                    "Some buckets could not be emptied:\n\n" + "\n".join(failures)
                )
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"empty bucket failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def _iter_object_keys(self, key, is_folder):
        """Yield each concrete object key for a job target (recursing folders)."""
        if is_folder:
            for k, _ in self.data_model.get_keys(key, log_fn=self.progress.emit):
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                if k and not k.endswith("/"):
                    yield k
        else:
            yield key

    def set_storage_class(self):
        # job = [(key, is_folder, storage_class)]
        cancelled = False
        try:
            for key, is_folder, storage_class in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for k in self._iter_object_keys(key, is_folder):
                    self.progress.emit(f"storage-class {storage_class}: {k}")
                    self.data_model.change_storage_class(
                        k, storage_class, log_fn=self.progress.emit
                    )
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"storage-class change failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def set_content_type(self):
        # job = [(key, is_folder)]
        cancelled = False
        fixed = 0
        skipped = 0
        try:
            for key, is_folder in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for k in self._iter_object_keys(key, is_folder):
                    changed, content_type = self.data_model.retype_object(
                        k, log_fn=self.progress.emit)
                    if changed:
                        fixed += 1
                        self.progress.emit(f"content-type {content_type}: {k}")
                    else:
                        skipped += 1
            self.progress.emit(
                f"content-type done: {fixed} updated, {skipped} unchanged")
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"content-type fix failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)

    def restore(self):
        # job = [(key, is_folder, days, tier)]
        cancelled = False
        initiated = 0
        skipped = 0
        try:
            for key, is_folder, days, tier in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
                for k in self._iter_object_keys(key, is_folder):
                    ok, reason = self.data_model.restore_object(
                        k, days=days, tier=tier
                    )
                    if ok:
                        initiated += 1
                        self.progress.emit(f"restore initiated: {k}")
                    else:
                        skipped += 1
                        self.progress.emit(f"restore skipped ({reason}): {k}")
            self.progress.emit(
                f"restore done: {initiated} initiated, {skipped} skipped"
            )
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"restore failed: {msg}")
                self.details.emit(describe_client_error(exc), is_transient_error(exc))
                self.error.emit(msg)
        finally:
            self.finished.emit(cancelled)


class UsageWorker(QObject):
    finished = pyqtSignal(str, str, object)  # bucket, prefix, result_or_exc

    def __init__(self, data_model, bucket_name: str, prefix: str):
        super().__init__()
        self.data_model = data_model
        self.bucket_name = bucket_name
        self.prefix = prefix or ""

    @pyqtSlot()
    def run(self):
        try:
            total = 0
            count = 0
            by_cat = {"Documents": 0, "Media": 0, "Other": 0}
            by_top = {}
            by_class = {}
            largest = []

            pref = self.prefix or ""
            pref_len = len(pref)

            for k, s, storage in self.data_model.get_keys_for_bucket(
                    self.bucket_name, pref):
                if not k or str(k).endswith("/"):
                    continue
                key = str(k)
                sz = int(s or 0)
                total += sz
                count += 1

                cat = categorize_key(key)
                by_cat[cat] = by_cat.get(cat, 0) + sz

                cls = storage or "STANDARD"
                by_class[cls] = by_class.get(cls, 0) + sz

                rel = key[pref_len:] if key.startswith(pref) else key
                top = rel.split("/", 1)[0] if "/" in rel else "(files)"
                by_top[top] = by_top.get(top, 0) + sz

                largest.append((sz, rel))
                if len(largest) > 200:      # keep the scan memory-bounded
                    largest.sort(reverse=True)
                    del largest[50:]

            by_top = dict(sorted(by_top.items(), key=lambda kv: kv[1], reverse=True)[:12])
            largest.sort(reverse=True)

            self.finished.emit(self.bucket_name, self.prefix, {
                "total": total, "count": count, "by_cat": by_cat,
                "by_top": by_top, "by_class": by_class,
                "largest": largest[:10],
                "cost": (DataModel.estimate_storage_cost(by_class)
                         if self.data_model.is_aws_endpoint() else None),
                "colder": (DataModel.colder_class_options(by_class)
                           if self.data_model.is_aws_endpoint() else []),
            })
        except Exception as exc:
            self.finished.emit(self.bucket_name, self.prefix, exc)


class PieWidget(QWidget):
    def __init__(self, by_cat: dict, parent=None):
        super().__init__(parent)
        self.by_cat = dict(by_cat or {})
        self.setMinimumSize(220, 220)

    def set_data(self, by_cat: dict):
        self.by_cat = dict(by_cat or {})
        self.update()

    def paintEvent(self, e):
        total = sum(max(0, int(v)) for v in self.by_cat.values()) or 1

        # simple, fixed colors
        colors = {
            "Documents": QColor(80, 160, 255),
            "Media": QColor(120, 220, 120),
            "Other": QColor(220, 220, 120),
        }

        r = min(self.width(), self.height()) - 20
        rect = QRectF((self.width() - r) / 2, (self.height() - r) / 2, r, r)

        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        p.setPen(QPen(QColor(40, 40, 40), 1))

        start = 0.0
        for name, val in self.by_cat.items():
            v = max(0, int(val))
            if v <= 0:
                continue
            span = 360.0 * (v / total)
            p.setBrush(colors.get(name, QColor(180, 180, 180)))
            p.drawPie(rect, int(start * 16), int(span * 16))
            start += span


class TreemapWidget(QWidget):
    """
    One level of a prefix tree as proportional rectangles.

    A pie by file category cannot answer "which folder is the bill"; area is
    the only encoding that makes a 400 GB prefix look like one.
    """

    drilled = pyqtSignal(str)

    PALETTE = ("#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#76b7b2",
               "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac")

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows = []
        self._rects = []
        self.setMinimumHeight(260)
        self.setMouseTracking(True)

    def set_rows(self, rows):
        self._rows = list(rows or [])
        self._layout()
        self.update()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._layout()

    def _layout(self):
        self._rects = squarify(
            [row[1] for row in self._rows], 0, 0,
            max(1, self.width()), max(1, self.height()))

    def _row_at(self, point):
        for row, (x, y, width, height) in zip(self._rows, self._rects):
            if (x <= point.x() <= x + width) and (y <= point.y() <= y + height):
                return row
        return None

    def paintEvent(self, _event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        metrics = QFontMetrics(self.font())
        for index, (row, rect) in enumerate(zip(self._rows, self._rects)):
            name, size, is_group = row
            x, y, width, height = (int(v) for v in rect)
            if width <= 0 or height <= 0:
                continue
            colour = QColor(self.PALETTE[index % len(self.PALETTE)])
            if is_group:
                colour = QColor("#9e9e9e")
            painter.fillRect(x, y, width, height, colour)
            painter.setPen(QPen(QColor(0, 0, 0, 60)))
            painter.drawRect(x, y, width, height)
            # A label only where it fits; a clipped one is worse than none.
            if width < 48 or height < metrics.height() + 4:
                continue
            painter.setPen(QPen(QColor("#101010")))
            label = metrics.elidedText(
                name, Qt.TextElideMode.ElideMiddle, width - 8)
            painter.drawText(x + 4, y + metrics.ascent() + 3, label)
            if height >= metrics.height() * 2 + 6:
                painter.drawText(
                    x + 4, y + metrics.height() + metrics.ascent() + 3,
                    _human_bytes(size))
        painter.end()

    def mouseMoveEvent(self, event):
        row = self._row_at(event.position().toPoint())
        self.setToolTip(
            f"{row[0]} — {_human_bytes(row[1])}" if row else "")
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        row = self._row_at(event.position().toPoint())
        if row and not row[2]:
            self.drilled.emit(row[0])
        super().mouseReleaseEvent(event)


class SizeExplorerDialog(QDialog):
    """Drill into a bucket by prefix, sized by what it actually holds."""

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._root = {}
        self._path = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Size explorer — {model.bucket}/{self._prefix}")
        self.resize(880, 620)

        self._breadcrumb = QLabel("")
        self._breadcrumb.setWordWrap(True)
        self._up = QPushButton("Up")
        self._up.clicked.connect(self.go_up)
        self._rescan = QPushButton("Rescan")
        self._rescan.clicked.connect(self._scan)

        top = QHBoxLayout()
        top.addWidget(self._up)
        top.addWidget(self._breadcrumb, 1)
        top.addWidget(self._rescan)

        self._map = TreemapWidget()
        self._map.drilled.connect(self.drill)
        self._info = QLabel("Scanning…")
        self._info.setWordWrap(True)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addLayout(top)
        layout.addWidget(self._map, 1)
        layout.addWidget(self._info)
        layout.addLayout(row)
        self._scan()

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        super().closeEvent(event)

    def _scan(self):
        if self._thread is not None:
            return
        clone = self._model.clone_for_worker()
        bucket = self._model.bucket
        prefix = self._prefix
        cancel = threading.Event()
        self._cancel = cancel
        self._info.setText("Scanning…")
        self._rescan.setEnabled(False)

        def _run(_worker):
            entries = []
            for key, size, _storage in clone.get_keys_for_bucket(
                    bucket, prefix):
                if cancel.is_set():
                    raise TransferCancelled("cancelled")
                relative = key[len(prefix):] if key.startswith(prefix) else key
                entries.append((relative, size))
            return build_prefix_tree(entries)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_scanned)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_scanned(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(thread)
        self._rescan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Scan failed: {exc}")
            return
        self._root = result or {}
        self._path = []
        self._render()

    def current_node(self) -> dict:
        return node_at_path(self._root, self._path)

    def drill(self, name):
        node = node_at_path(self._root, self._path + [name])
        if not node or not node.get("children"):
            return   # a leaf object, not a prefix
        self._path.append(name)
        self._render()

    def go_up(self):
        if self._path:
            self._path.pop()
            self._render()

    def _render(self):
        node = self.current_node()
        rows = treemap_children(node)
        self._map.set_rows(rows)
        where = "/".join(self._path)
        self._breadcrumb.setText(
            f"{self._model.bucket}/{self._prefix}{where}"
            + ("/" if where else ""))
        self._up.setEnabled(bool(self._path))
        self._info.setText(
            f"{_human_bytes(node.get('size', 0))} in "
            f"{node.get('count', 0)} object(s) · {len(rows)} entries here"
            + ("  ·  click a tile to go deeper" if rows else ""))


class BucketUsageDialog(QDialog):

    def __init__(self, bucket_name: str, prefix: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Usage")
        self.setModal(False)

        self._bucket = bucket_name
        self._prefix = prefix or ""

        self.title = QLabel(self._title_html(bucket_name, self._prefix))
        self.total_lbl = QLabel("Total: <b>Calculating…</b>")

        self.pie = PieWidget({"Documents": 0, "Media": 0, "Other": 0}, self)

        self.legend_labels = {}
        legend = QVBoxLayout()
        for k in ["Documents", "Media", "Other"]:
            lbl = QLabel(f"{k}: Calculating…")
            self.legend_labels[k] = lbl
            legend.addWidget(lbl)
        legend.addStretch(1)

        self.top_groups = QLabel("<b>Top groups</b><br><pre>Calculating…</pre>")
        self.top_groups.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        self.by_class = QLabel("<b>Storage classes</b><br><pre>Calculating…</pre>")
        self.by_class.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        # Only ever filled for an AWS endpoint: the bundled prices are AWS
        # list prices, and quoting them at a MinIO endpoint invents a number.
        self.cost = QLabel("")
        self.cost.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        self.cost.hide()
        self.largest = QLabel("<b>Largest objects</b><br><pre>Calculating…</pre>")
        self.largest.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        top = QHBoxLayout()
        top.addWidget(self.pie, 0)
        top.addLayout(legend, 1)

        btn = QPushButton("Close")
        btn.clicked.connect(self.close)

        layout = QVBoxLayout()
        layout.addWidget(self.title)
        layout.addWidget(self.total_lbl)
        layout.addLayout(top)
        layout.addWidget(self.top_groups)
        layout.addWidget(self.by_class)
        layout.addWidget(self.cost)
        layout.addWidget(self.largest)
        layout.addWidget(btn)
        self.setLayout(layout)

    def _title_html(self, bucket: str, prefix: str) -> str:
        if prefix:
            return f"<b>{bucket}</b><br><span style='color:#666;'>/{prefix}</span>"
        return f"<b>{bucket}</b>"

    def set_calculating(self, bucket_name: str, prefix: str = ""):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText("Total: <b>Calculating…</b>")
        self.pie.set_data({"Documents": 0, "Media": 0, "Other": 0})
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: Calculating…")
        self.top_groups.setText("<b>Top groups</b><br><pre>Calculating…</pre>")
        self.by_class.setText("<b>Storage classes</b><br><pre>Calculating…</pre>")
        self.cost.hide()
        self.largest.setText("<b>Largest objects</b><br><pre>Calculating…</pre>")

    def set_error(self, bucket_name: str, prefix: str, err: Exception):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText("Total: <b>n/a</b>")
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: n/a")
        self.top_groups.setText(f"<b>Top groups</b><br><pre>n/a\n{err}</pre>")
        self.by_class.setText("<b>Storage classes</b><br><pre>n/a</pre>")
        self.cost.hide()
        self.largest.setText("<b>Largest objects</b><br><pre>n/a</pre>")

    @staticmethod
    def cost_html(cost, colder) -> str:
        """The estimated-cost panel, or "" when there is nothing to show."""
        if not cost:
            return ""
        lines = [f"{'Storage (estimated)':22s}  ${cost['total']:,.2f} / month"]
        for name, amount in sorted(cost.get("by_class", {}).items(),
                                   key=lambda kv: kv[1], reverse=True):
            if amount:
                lines.append(f"  {name:20s}  ${amount:,.2f}")
        if cost.get("unpriced"):
            lines.append("  not priced: " + ", ".join(cost["unpriced"]))
        if colder:
            lines.append("")
            lines.append("If the STANDARD bytes moved:")
            for name, amount, saving, caveat in colder:
                lines.append(
                    f"  {name:20s}  ${amount:,.2f}  "
                    f"(saves ${saving:,.2f})  — {caveat}")
        return ("<b>Estimated cost</b> "
                "<span style='color:#666;'>us-east-1 list price, storage "
                "only — requests, retrieval and transfer are not "
                "included</span><br><pre>" + "\n".join(lines) + "</pre>")

    def set_result(self, bucket_name: str, prefix: str, total: int, by_cat: dict,
                   by_top: dict, count: int = 0, by_class: dict = None,
                   largest=None, cost=None, colder=None):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText(
            f"Total: <b>{_human_bytes(int(total))}</b>"
            + (f" in <b>{int(count)}</b> object(s)" if count else ""))

        norm_cat = {"Documents": 0, "Media": 0, "Other": 0}
        for k, v in (by_cat or {}).items():
            if k in norm_cat:
                norm_cat[k] = int(v or 0)

        self.pie.set_data(norm_cat)
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: {_human_bytes(norm_cat[k])}")

        lines = []
        for k, v in (by_top or {}).items():
            lines.append(f"{k:24s}  {_human_bytes(int(v or 0))}")
        if not lines:
            lines = ["(empty)"]

        self.top_groups.setText("<b>Top groups</b><br><pre>" + "\n".join(lines) + "</pre>")

        class_lines = [
            f"{name:22s}  {_human_bytes(int(size or 0))}"
            for name, size in sorted((by_class or {}).items(),
                                     key=lambda kv: kv[1], reverse=True)
        ] or ["(empty)"]
        self.by_class.setText(
            "<b>Storage classes</b><br><pre>" + "\n".join(class_lines) + "</pre>")

        html = self.cost_html(cost, colder or [])
        self.cost.setText(html)
        self.cost.setVisible(bool(html))

        big_lines = [
            f"{_human_bytes(int(size or 0)):>10s}  {name}"
            for size, name in (largest or [])
        ] or ["(empty)"]
        self.largest.setText(
            "<b>Largest objects</b><br><pre>" + "\n".join(big_lines) + "</pre>")


class LifecycleRuleDialog(QDialog):
    """One lifecycle rule: what it covers and what it does."""

    def __init__(self, parent, rule=None):
        super().__init__(parent)
        self.setWindowTitle("Lifecycle rule")
        self.setMinimumWidth(460)
        rule = dict(rule or {})

        self._id = QLineEdit(str(rule.get("ID") or ""))
        self._id.setPlaceholderText("expire-logs")
        self._prefix = QLineEdit(DataModel.lifecycle_rule_scope(rule))
        self._prefix.setPlaceholderText("logs/  (blank = the whole bucket)")
        self._enabled = QCheckBox("Enabled")
        self._enabled.setChecked(rule.get("Status", "Enabled") == "Enabled")

        transitions = (rule.get("Transitions") or [{}])[0]
        self._transition = QCheckBox("Move to another storage class after")
        self._transition_days = QSpinBox()
        self._transition_days.setRange(1, 36500)
        self._transition_days.setSuffix(" days")
        self._transition_days.setValue(int(transitions.get("Days") or 30))
        self._transition_class = QComboBox()
        for name in ("STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING",
                     "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE"):
            self._transition_class.addItem(name, name)
        index = self._transition_class.findData(
            transitions.get("StorageClass") or "STANDARD_IA")
        self._transition_class.setCurrentIndex(max(index, 0))
        self._transition.setChecked(bool(transitions.get("Days")))

        expiration = rule.get("Expiration") or {}
        self._expire = QCheckBox("Delete objects after")
        self._expire_days = QSpinBox()
        self._expire_days.setRange(1, 36500)
        self._expire_days.setSuffix(" days")
        self._expire_days.setValue(int(expiration.get("Days") or 365))
        self._expire.setChecked(expiration.get("Days") is not None)

        noncurrent = rule.get("NoncurrentVersionExpiration") or {}
        self._noncurrent = QCheckBox("Delete noncurrent versions after")
        self._noncurrent_days = QSpinBox()
        self._noncurrent_days.setRange(1, 36500)
        self._noncurrent_days.setSuffix(" days")
        self._noncurrent_days.setValue(
            int(noncurrent.get("NoncurrentDays") or 30))
        self._noncurrent.setChecked(
            noncurrent.get("NoncurrentDays") is not None)

        abort = rule.get("AbortIncompleteMultipartUpload") or {}
        self._abort = QCheckBox("Abort incomplete multipart uploads after")
        self._abort_days = QSpinBox()
        self._abort_days.setRange(1, 3650)
        self._abort_days.setSuffix(" days")
        self._abort_days.setValue(
            int(abort.get("DaysAfterInitiation") or 7))
        self._abort.setChecked(abort.get("DaysAfterInitiation") is not None)

        self._markers = QCheckBox("Clean up expired delete markers")
        self._markers.setChecked(
            bool(expiration.get("ExpiredObjectDeleteMarker")))

        form = QFormLayout()
        form.addRow(QLabel("Rule name"), self._id)
        form.addRow(QLabel("Applies to prefix"), self._prefix)
        form.addRow(self._enabled)
        for check, spin in ((self._transition, self._transition_days),
                            (self._expire, self._expire_days),
                            (self._noncurrent, self._noncurrent_days),
                            (self._abort, self._abort_days)):
            row = QHBoxLayout()
            row.addWidget(check)
            row.addWidget(spin)
            row.addStretch(1)
            check.toggled.connect(spin.setEnabled)
            spin.setEnabled(check.isChecked())
            form.addRow(row)
        transition_row = QHBoxLayout()
        transition_row.addWidget(QLabel("Storage class"))
        transition_row.addWidget(self._transition_class)
        transition_row.addStretch(1)
        form.addRow(transition_row)
        form.addRow(self._markers)

        note = QLabel(
            "Lifecycle actions are executed by the storage service, not by "
            "this app, and deletions they make are permanent.")
        note.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(note)
        layout.addWidget(buttons)
        self._rule = None

    def _accept(self):
        try:
            self._rule = self.rule()
        except ValueError as exc:
            QMessageBox.warning(self, "Lifecycle rule", str(exc))
            return
        self.accept()

    def rule(self) -> dict:
        return DataModel.build_lifecycle_rule(
            self._id.text(),
            prefix=self._prefix.text().strip(),
            enabled=self._enabled.isChecked(),
            transition_days=(self._transition_days.value()
                             if self._transition.isChecked() else None),
            transition_class=(self._transition_class.currentData()
                              if self._transition.isChecked() else ""),
            expire_days=(self._expire_days.value()
                         if self._expire.isChecked() else None),
            noncurrent_days=(self._noncurrent_days.value()
                             if self._noncurrent.isChecked() else None),
            abort_days=(self._abort_days.value()
                        if self._abort.isChecked() else None),
            expire_delete_markers=self._markers.isChecked(),
        )

    def result_rule(self):
        return self._rule


class BucketSettingsDialog(QDialog):
    """
    Lifecycle, CORS, policy and Object Lock for one bucket.

    Four documents that all live on the bucket rather than on an object, kept
    in one place so the toolbar does not grow four more entries.
    """

    def __init__(self, parent, main_window, model):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self.setWindowTitle(f"Bucket settings — {model.bucket}")
        self.resize(720, 520)
        self._writable = not main_window.is_read_only()
        self._acceleration_was = False

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_lifecycle(), "Lifecycle")
        self.tabs.addTab(self._build_cors(), "CORS")
        self.tabs.addTab(self._build_policy(), "Policy")
        self.tabs.addTab(self._build_bucket(), "Encryption && tags")
        self.tabs.addTab(self._build_website(), "Website")
        self.tabs.addTab(self._build_events(), "Events")
        self.tabs.addTab(self._build_lock(), "Object Lock")

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self.tabs, 1)
        layout.addLayout(row)
        self.reload()

    def _build_lifecycle(self):
        page = QWidget()
        self._rules = []
        self._rules_list = QListWidget()
        self._rules_list.itemDoubleClicked.connect(
            lambda _i: self._edit_rule())
        self._lifecycle_status = QLabel("")
        self._lifecycle_status.setWordWrap(True)

        self._btn_add = QPushButton("Add…")
        self._btn_edit = QPushButton("Edit…")
        self._btn_remove = QPushButton("Remove")
        self._btn_save = QPushButton("Save to bucket")
        self._btn_add.clicked.connect(self._add_rule)
        self._btn_edit.clicked.connect(self._edit_rule)
        self._btn_remove.clicked.connect(self._remove_rule)
        self._btn_save.clicked.connect(self._save_lifecycle)
        for button in (self._btn_add, self._btn_edit, self._btn_remove,
                       self._btn_save):
            button.setEnabled(self._writable)

        buttons = QHBoxLayout()
        for button in (self._btn_add, self._btn_edit, self._btn_remove):
            buttons.addWidget(button)
        buttons.addStretch(1)
        buttons.addWidget(self._btn_save)

        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Rules the storage service applies on its own schedule. "
            "Transitions and expirations happen without this app running."))
        layout.addWidget(self._rules_list, 1)
        layout.addWidget(self._lifecycle_status)
        layout.addLayout(buttons)
        return page

    def _build_cors(self):
        page = QWidget()
        self._cors_text = QPlainTextEdit()
        self._cors_text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._cors_text.setReadOnly(not self._writable)
        self._cors_status = QLabel("")
        self._cors_status.setWordWrap(True)
        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_cors)
        save.setEnabled(self._writable)
        self._btn_cors_save = save
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "The CORS rules as JSON — a list of rule objects. An empty "
            "document removes the configuration."))
        layout.addWidget(self._cors_text, 1)
        layout.addWidget(self._cors_status)
        layout.addLayout(row)
        return page

    def _build_policy(self):
        page = QWidget()
        self._policy_text = QPlainTextEdit()
        self._policy_text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._policy_text.setReadOnly(True)
        self._policy_status = QLabel("")
        self._policy_status.setWordWrap(True)
        self._policy_edit = QCheckBox("Allow editing")
        self._policy_edit.setEnabled(self._writable)
        self._policy_edit.toggled.connect(self._toggle_policy_edit)
        self._btn_policy_save = QPushButton("Save to bucket")
        self._btn_policy_save.clicked.connect(self._save_policy)
        self._btn_policy_save.setEnabled(False)
        row = QHBoxLayout()
        row.addWidget(self._policy_edit)
        row.addStretch(1)
        row.addWidget(self._btn_policy_save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "The bucket policy decides who can reach this bucket. It opens "
            "read-only on purpose: a wrong policy can expose every object in "
            "it, or lock you out of your own bucket."))
        layout.addWidget(self._policy_text, 1)
        layout.addWidget(self._policy_status)
        layout.addLayout(row)
        return page

    def _build_bucket(self):
        """Default encryption, the bucket's own tags, and acceleration."""
        page = QWidget()
        self._sse = QComboBox()
        self._sse.addItem("(none)", "")
        for mode in DataModel.SSE_MODES:
            if mode:
                self._sse.addItem(mode, mode)
        self._sse_key = QLineEdit()
        self._sse_key.setPlaceholderText("KMS key id or ARN (aws:kms only)")
        self._bucket_key = QCheckBox(
            "Use an S3 Bucket Key (fewer KMS requests)")
        self._sse.currentIndexChanged.connect(self._sync_sse_fields)

        self._bucket_tags = QPlainTextEdit()
        self._bucket_tags.setPlaceholderText("one per line:  key = value")
        self._bucket_tags.setMaximumHeight(90)

        self._acceleration = QCheckBox("Transfer acceleration")
        self._bucket_status = QLabel("")
        self._bucket_status.setWordWrap(True)

        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_bucket)
        save.setEnabled(self._writable)
        self._btn_bucket_save = save
        for widget in (self._sse, self._sse_key, self._bucket_key,
                       self._bucket_tags, self._acceleration):
            widget.setEnabled(self._writable)

        form = QFormLayout()
        form.addRow(QLabel("Default encryption"), self._sse)
        form.addRow(QLabel("KMS key"), self._sse_key)
        form.addRow(self._bucket_key)
        form.addRow(QLabel("Bucket tags"), self._bucket_tags)
        form.addRow(self._acceleration)

        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Default encryption applies to objects uploaded from anywhere, "
            "not just this app. Bucket tags are the bucket's own, separate "
            "from the tags on its objects."))
        layout.addLayout(form)
        layout.addWidget(self._bucket_status)
        layout.addStretch(1)
        layout.addLayout(row)
        return page

    def _build_website(self):
        page = QWidget()
        self._site_index = QLineEdit()
        self._site_index.setPlaceholderText("index.html")
        self._site_error = QLineEdit()
        self._site_error.setPlaceholderText("error.html (optional)")
        self._site_redirect = QLineEdit()
        self._site_redirect.setPlaceholderText(
            "example.com — redirects every request, instead of serving")
        self._site_status = QLabel("")
        self._site_status.setWordWrap(True)
        save = QPushButton("Save to bucket")
        save.clicked.connect(self._save_website)
        save.setEnabled(self._writable)
        self._btn_site_save = save
        for widget in (self._site_index, self._site_error,
                       self._site_redirect):
            widget.setReadOnly(not self._writable)

        form = QFormLayout()
        form.addRow(QLabel("Index document"), self._site_index)
        form.addRow(QLabel("Error document"), self._site_error)
        form.addRow(QLabel("Redirect all requests to"), self._site_redirect)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(save)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Static-website hosting serves this bucket over its website "
            "endpoint. It does not make the objects public on its own — the "
            "policy does that. Leaving everything blank removes the "
            "configuration."))
        layout.addLayout(form)
        layout.addWidget(self._site_status)
        layout.addStretch(1)
        layout.addLayout(row)
        return page

    def _build_events(self):
        page = QWidget()
        self._events = QLabel("")
        self._events.setWordWrap(True)
        self._events.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel(
            "Event notifications, read-only: a destination ARN cannot be "
            "validated from here, and a wrong one silently stops every "
            "event."))
        layout.addWidget(self._events)
        layout.addStretch(1)
        return page

    def _sync_sse_fields(self):
        mode = self._sse.currentData() or ""
        self._sse_key.setEnabled(self._writable and mode == "aws:kms")
        self._bucket_key.setEnabled(self._writable and mode == "aws:kms")

    def _save_bucket(self):
        tags = DataModel.parse_content_type_overrides(
            self._bucket_tags.toPlainText())
        try:
            self._model.put_bucket_encryption(
                sse=self._sse.currentData() or "",
                kms_key=self._sse_key.text().strip(),
                bucket_key=self._bucket_key.isChecked(),
                log_fn=self._mw.log)
            self._model.put_bucket_tags(tags, log_fn=self._mw.log)
            if self._acceleration.isChecked() != self._acceleration_was:
                self._model.set_bucket_acceleration(
                    self._acceleration.isChecked(), log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Bucket settings", str(exc))
            return
        self._bucket_status.setText("Saved.")
        self.reload()

    def _save_website(self):
        try:
            self._model.put_bucket_website(
                index=self._site_index.text(),
                error=self._site_error.text(),
                redirect=self._site_redirect.text(),
                log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Website hosting", str(exc))
            return
        self._site_status.setText("Saved.")
        self.reload()

    def _build_lock(self):
        page = QWidget()
        self._lock_status = QLabel("")
        self._lock_status.setWordWrap(True)
        self._lock_status.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        layout = QVBoxLayout(page)
        layout.addWidget(self._lock_status)
        layout.addStretch(1)
        return page

    @staticmethod
    def read_documents(model) -> dict:
        """
        Fetch all four bucket documents, each failure kept beside its own key.

        One dict rather than four calls from the UI thread: this is five round
        trips, and a dialog that blocks for them is the thing every other
        network path in this app already avoids.
        """
        out = {}
        for name, fetch in (
            ("lifecycle", model.get_bucket_lifecycle),
            ("cors", model.get_bucket_cors),
            ("policy", model.get_bucket_policy),
            ("lock", model.get_object_lock_configuration),
            ("encryption", model.get_bucket_encryption),
            ("tags", model.get_bucket_tags),
            ("website", model.get_bucket_website),
            ("events", model.get_bucket_notifications),
            ("acceleration", model.get_bucket_acceleration),
        ):
            try:
                out[name] = fetch()
            except Exception as exc:
                out[name] = None
                out[name + "_error"] = str(exc)
        try:
            out["policy_status"] = model.get_bucket_policy_status()
        except Exception:
            out["policy_status"] = ""
        return out

    def reload(self):
        """Read all four documents off the UI thread and render them."""
        clone = self._model.clone_for_worker()

        def _read(_worker):
            return self.read_documents(clone)

        payload, exc = run_with_progress(
            self, f"Reading settings for {self._model.bucket}…", _read)
        if payload is None and exc is None:
            return  # cancelled
        if exc is not None:
            payload = {"lifecycle_error": str(exc), "cors_error": str(exc),
                       "policy_error": str(exc), "lock_error": str(exc)}
        self.apply_documents(payload)

    def apply_documents(self, payload):
        """Render what read_documents returned."""
        payload = dict(payload or {})

        if payload.get("lifecycle_error"):
            self._rules = []
            self._lifecycle_status.setText(
                f"Could not read the rules: {payload['lifecycle_error']}")
        else:
            self._rules = list(payload.get("lifecycle") or [])
            self._lifecycle_status.setText(
                "" if self._rules else "No lifecycle rules on this bucket.")
        self._refresh_rules()

        if payload.get("cors_error"):
            self._cors_status.setText(
                f"Could not read CORS: {payload['cors_error']}")
        else:
            rules = list(payload.get("cors") or [])
            self._cors_text.setPlainText(
                json.dumps(rules, indent=2) if rules else "")
            self._cors_status.setText(
                "" if rules else "No CORS configuration on this bucket.")

        if payload.get("policy_error"):
            self._policy_status.setText(
                f"Could not read the policy: {payload['policy_error']}")
        else:
            policy = str(payload.get("policy") or "")
            if policy:
                try:
                    policy = json.dumps(json.loads(policy), indent=2)
                except ValueError:
                    pass
            self._policy_text.setPlainText(policy)
            status = payload.get("policy_status") or ""
            self._policy_status.setText(
                (f"This bucket is {status}." if status else "")
                + ("" if policy else " No bucket policy is set."))

        self._apply_bucket_documents(payload)

        if payload.get("lock_error"):
            self._lock_status.setText(
                f"Could not read Object Lock: {payload['lock_error']}")
            return
        config = dict(payload.get("lock") or {})
        if not config.get("enabled"):
            self._lock_status.setText(
                "Object Lock is not enabled on this bucket.\n\n"
                "It can only be turned on when a bucket is created, and it "
                "makes objects undeletable for the retention period — "
                "including by you.")
            return
        retention = config.get("mode") or "none"
        window = ""
        if config.get("days"):
            window = f"{config['days']} day(s)"
        elif config.get("years"):
            window = f"{config['years']} year(s)"
        self._lock_status.setText(
            "Object Lock is <b>enabled</b> on this bucket.<br><br>"
            f"Default retention mode: {retention}<br>"
            f"Default retention period: {window or 'not set'}<br><br>"
            "Per-object retention and legal holds are shown in Properties, "
            "and a legal hold can be placed from the object's context menu.")

    def _apply_bucket_documents(self, payload):
        """Render the encryption / tags / website / events tabs."""
        encryption = dict(payload.get("encryption") or {})
        index = self._sse.findData(encryption.get("sse", ""))
        self._sse.setCurrentIndex(max(index, 0))
        self._sse_key.setText(encryption.get("kms_key", ""))
        self._bucket_key.setChecked(bool(encryption.get("bucket_key")))
        self._sync_sse_fields()

        tags = dict(payload.get("tags") or {})
        self._bucket_tags.setPlainText(
            "\n".join(f"{key} = {value}" for key, value in sorted(tags.items())))

        status = str(payload.get("acceleration") or "")
        self._acceleration_was = status == "Enabled"
        self._acceleration.setChecked(self._acceleration_was)
        self._acceleration.setEnabled(self._writable and bool(status is not None))

        problems = [payload[name] for name in
                    ("encryption_error", "tags_error", "acceleration_error")
                    if payload.get(name)]
        self._bucket_status.setText(
            "  ".join(problems) if problems else "")

        website = dict(payload.get("website") or {})
        self._site_index.setText(website.get("index", ""))
        self._site_error.setText(website.get("error", ""))
        self._site_redirect.setText(website.get("redirect", ""))
        if payload.get("website_error"):
            self._site_status.setText(
                f"Could not read it: {payload['website_error']}")
        else:
            self._site_status.setText(
                "" if website else "Website hosting is not configured.")

        if payload.get("events_error"):
            self._events.setText(
                f"Could not read them: {payload['events_error']}")
            return
        events = list(payload.get("events") or [])
        if not events:
            self._events.setText("No event notifications are configured.")
            return
        lines = []
        for kind, target, names in events:
            lines.append(
                f"<b>{kind}</b> → {target}"
                + (f"<br>&nbsp;&nbsp;{', '.join(names)}" if names else ""))
        self._events.setText("<br>".join(lines))

    def _refresh_rules(self):
        self._rules_list.clear()
        for rule in self._rules:
            name = str(rule.get("ID") or "(unnamed)")
            state = "" if rule.get("Status") == "Enabled" else "  [disabled]"
            item = QListWidgetItem(
                f"{name}{state}\n    "
                f"{DataModel.summarize_lifecycle_rule(rule)}")
            self._rules_list.addItem(item)

    def _selected_rule_index(self) -> int:
        return self._rules_list.currentRow()

    def _add_rule(self):
        dlg = LifecycleRuleDialog(self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._rules.append(dlg.result_rule())
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _edit_rule(self):
        index = self._selected_rule_index()
        if not (0 <= index < len(self._rules)):
            return
        if not DataModel.lifecycle_filter_is_simple(self._rules[index]):
            # Rebuilding it from the prefix form would drop the tag or size
            # filter, quietly widening the rule to every object under the
            # prefix — for an expiration rule, that deletes more than before.
            QMessageBox.information(
                self, "Lifecycle rule",
                "This rule is filtered by tag or object size, which this "
                "editor cannot represent. It can be removed here, but editing "
                "it would silently widen it to the whole prefix — change it "
                "in the provider's own console instead.")
            return
        dlg = LifecycleRuleDialog(self, self._rules[index])
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._rules[index] = dlg.result_rule()
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _remove_rule(self):
        index = self._selected_rule_index()
        if not (0 <= index < len(self._rules)):
            return
        del self._rules[index]
        self._refresh_rules()
        self._lifecycle_status.setText("Not saved yet.")

    def _save_lifecycle(self):
        summary = "\n".join(
            f"• {rule.get('ID')}: {DataModel.summarize_lifecycle_rule(rule)}"
            for rule in self._rules
        ) or "(no rules — the configuration will be removed)"
        answer = QMessageBox.question(
            self, "Lifecycle rules",
            "Write these rules to "
            f"{self._model.bucket}?\n\n{summary}\n\n"
            "The service will apply them on its own schedule, and "
            "expirations delete objects permanently.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.put_bucket_lifecycle(
                self._rules, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Lifecycle rules", str(exc))
            return
        self._lifecycle_status.setText("Saved.")
        self.reload()

    def _save_cors(self):
        text = self._cors_text.toPlainText().strip()
        rules = []
        if text:
            try:
                rules = json.loads(text)
            except ValueError as exc:
                QMessageBox.warning(self, "CORS", f"Not valid JSON: {exc}")
                return
            if not isinstance(rules, list):
                QMessageBox.warning(
                    self, "CORS", "The document must be a list of rules.")
                return
        try:
            self._model.put_bucket_cors(rules, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "CORS", str(exc))
            return
        self._cors_status.setText("Saved.")
        self.reload()

    def _toggle_policy_edit(self, enabled):
        self._policy_text.setReadOnly(not enabled)
        self._btn_policy_save.setEnabled(bool(enabled))

    def _save_policy(self):
        text = self._policy_text.toPlainText().strip()
        answer = QMessageBox.question(
            self, "Bucket policy",
            ("Remove the bucket policy?" if not text else
             "Replace the policy on "
             f"{self._model.bucket}?\n\nA policy decides who can reach "
             "every object in this bucket."),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.put_bucket_policy(text, log_fn=self._mw.log)
        except Exception as exc:
            QMessageBox.critical(self, "Bucket policy", str(exc))
            return
        self._policy_status.setText("Saved.")
        self.reload()


class CopyMoveDialog(QDialog):
    def __init__(self, parent, model, item_count: int, current_prefix: str):
        super().__init__(parent)
        self.setWindowTitle("Copy / Move")
        self.setMinimumWidth(520)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        self._model = model
        self._source_bucket = model.bucket
        self._thread = None
        self._worker = None

        src_lbl = QLabel(
            f"<b>{item_count} item(s)</b> from "
            f"<code>s3://{self._source_bucket}/{current_prefix}</code>"
        )
        src_lbl.setWordWrap(True)

        # Editable so a bucket can be typed even if ListBuckets is denied or
        # still loading; the dropdown fills in from a background listing.
        self.bucket_combo = QComboBox()
        self.bucket_combo.setEditable(True)
        self.bucket_combo.addItem(self._source_bucket)
        self.bucket_combo.setCurrentText(self._source_bucket)

        self.dst_edit = QLineEdit(current_prefix)
        self.dst_edit.setPlaceholderText("e.g.  archive/2024/  (empty = bucket root)")

        form = QFormLayout()
        form.addRow(QLabel("Destination bucket"), self.bucket_combo)
        form.addRow(QLabel("Destination prefix"), self.dst_edit)

        self._note = QLabel("")
        self._note.setWordWrap(True)
        self.bucket_combo.currentTextChanged.connect(self._on_bucket_changed)

        op_group = QGroupBox("Operation")
        self.rb_copy = QRadioButton("Copy")
        self.rb_move = QRadioButton("Move  (copy then delete originals)")
        self.rb_copy.setChecked(True)
        op_lay = QVBoxLayout()
        op_lay.addWidget(self.rb_copy)
        op_lay.addWidget(self.rb_move)
        op_group.setLayout(op_lay)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        lay = QVBoxLayout()
        lay.addWidget(src_lbl)
        lay.addSpacing(4)
        lay.addLayout(form)
        lay.addWidget(self._note)
        lay.addWidget(op_group)
        lay.addWidget(btns)
        self.setLayout(lay)

        self._load_buckets()

    def _load_buckets(self):
        """Populate the bucket dropdown without blocking the dialog."""
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return [b.name for b in clone.list_buckets()]

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_buckets)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        self._stop_loader()
        super().closeEvent(event)

    def _stop_loader(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_buckets(self, result, exc):
        self._stop_loader()
        if exc is not None or not result:
            return
        typed = self.bucket_combo.currentText()
        self.bucket_combo.blockSignals(True)
        self.bucket_combo.clear()
        self.bucket_combo.addItems(result)
        self.bucket_combo.setCurrentText(typed or self._source_bucket)
        self.bucket_combo.blockSignals(False)

    def _on_bucket_changed(self, name: str):
        if (name or "").strip() != self._source_bucket:
            self._note.setText(
                "Cross-bucket copy is server-side and requires the target "
                "bucket to be reachable on the same endpoint/region."
            )
        else:
            self._note.setText("")

    def destination(self) -> str:
        return self.dst_edit.text().strip()

    def destination_bucket(self) -> str:
        return self.bucket_combo.currentText().strip() or self._source_bucket

    def is_cross_bucket(self) -> bool:
        return self.destination_bucket() != self._source_bucket

    def is_move(self) -> bool:
        return self.rb_move.isChecked()


class TransferSettingsDialog(QDialog):
    """Parallelism plus the storage class / encryption applied to uploads."""

    def __init__(self, parent, *, concurrency, max_concurrency, storage_classes,
                 sse_modes, storage_class="", sse="", kms_key_id="",
                 notify=True, parallel_files=4, max_parallel_files=32,
                 verify_downloads=False, rate_limit_kbps=0,
                 checksum_algorithm="", multipart_threshold_mb=16,
                 multipart_chunksize_mb=8, resumable_uploads=True,
                 detect_content_type=True, content_type_overrides=None,
                 listing_limit=DEFAULT_LISTING_LIMIT, auto_retry=True,
                 upload_rules=None, log_to_file=True):
        super().__init__(parent)
        self.setWindowTitle("Transfer settings")
        self.setMinimumWidth(460)

        self._concurrency = QSpinBox()
        self._concurrency.setRange(1, max_concurrency)
        self._concurrency.setValue(int(concurrency))

        self._parallel_files = QSpinBox()
        self._parallel_files.setRange(1, max_parallel_files)
        self._parallel_files.setValue(int(parallel_files))

        self._storage = QComboBox()
        for name in storage_classes:
            self._storage.addItem(name or "(bucket default)", name)
        self._select_data(self._storage, storage_class)

        self._sse = QComboBox()
        for mode in sse_modes:
            self._sse.addItem(mode or "(none)", mode)
        self._select_data(self._sse, sse)

        self._kms = QLineEdit(kms_key_id)
        self._kms.setPlaceholderText("KMS key id or ARN (aws:kms only)")
        self._sse.currentIndexChanged.connect(self._sync_kms)

        self._notify = QCheckBox(
            "Notify when transfers finish while the window is in the background")
        self._notify.setChecked(bool(notify))

        self._rate_limit = QSpinBox()
        self._rate_limit.setRange(0, 10 * 1024 * 1024)
        self._rate_limit.setSuffix(" KB/s")
        self._rate_limit.setSpecialValueText("unlimited")
        self._rate_limit.setValue(int(rate_limit_kbps or 0))

        self._verify = QCheckBox(
            "Verify downloads against the object's stored digest "
            "(re-reads each file)")
        self._verify.setChecked(bool(verify_downloads))

        self._resumable = QCheckBox(
            "Resume interrupted uploads (multipart, leaves parts on the "
            "server until the transfer finishes)")
        self._resumable.setChecked(bool(resumable_uploads))

        self._chunk = QSpinBox()
        self._chunk.setRange(DataModel.MIN_MULTIPART_CHUNKSIZE_MB,
                             DataModel.MAX_MULTIPART_CHUNKSIZE_MB)
        self._chunk.setSuffix(" MiB")
        self._chunk.setValue(int(multipart_chunksize_mb))

        self._threshold = QSpinBox()
        self._threshold.setRange(DataModel.MIN_MULTIPART_CHUNKSIZE_MB, 4096)
        self._threshold.setSuffix(" MiB")
        self._threshold.setValue(int(multipart_threshold_mb))

        self._checksum = QComboBox()
        self._checksum.addItem("None (ETag only)", "")
        for name in CHECKSUM_ALGORITHMS:
            self._checksum.addItem(name, name)
        index = self._checksum.findData(str(checksum_algorithm or "").upper())
        self._checksum.setCurrentIndex(max(index, 0))

        self._listing_limit = QSpinBox()
        self._listing_limit.setRange(0, 5_000_000)
        self._listing_limit.setSingleStep(1000)
        self._listing_limit.setSpecialValueText("no limit")
        self._listing_limit.setValue(int(listing_limit or 0))

        self._log_to_file = QCheckBox(
            "Write the session log to ~/.config/s3duck/s3duck.log")
        self._log_to_file.setChecked(bool(log_to_file))

        self._auto_retry = QCheckBox(
            "Retry a job once when the service asks for it "
            "(throttling, 5xx, dropped connection)")
        self._auto_retry.setChecked(bool(auto_retry))

        self._detect_type = QCheckBox(
            "Set Content-Type from the file extension when uploading")
        self._detect_type.setChecked(bool(detect_content_type))

        self._type_overrides = QPlainTextEdit(
            DataModel.format_content_type_overrides(content_type_overrides))
        self._type_overrides.setPlaceholderText(
            "one per line:  ext = type\nmd = text/markdown")
        self._type_overrides.setMaximumHeight(72)

        self._upload_rules = QPlainTextEdit(
            DataModel.format_upload_rules(upload_rules))
        self._upload_rules.setPlaceholderText(
            "archive/* -> class=GLACIER\n"
            "logs/ -> class=STANDARD_IA, tag:team=infra")
        self._upload_rules.setMaximumHeight(72)
        self._detect_type.toggled.connect(self._type_overrides.setEnabled)
        self._type_overrides.setEnabled(self._detect_type.isChecked())

        form = QFormLayout()
        form.addRow(QLabel("Files transferred at once"), self._parallel_files)
        form.addRow(QLabel("Connections per file (multipart)"), self._concurrency)
        form.addRow(QLabel("Bandwidth limit (all transfers)"), self._rate_limit)
        form.addRow(QLabel("Multipart part size"), self._chunk)
        form.addRow(QLabel("Use multipart above"), self._threshold)
        form.addRow(QLabel("Upload storage class"), self._storage)
        form.addRow(QLabel("Upload encryption"), self._sse)
        form.addRow(QLabel("KMS key"), self._kms)
        form.addRow(QLabel("Upload checksum"), self._checksum)
        form.addRow(QLabel("Entries loaded per listing"), self._listing_limit)
        form.addRow(self._detect_type)
        form.addRow(QLabel("Content-Type overrides"), self._type_overrides)
        form.addRow(QLabel("Upload rules by destination"), self._upload_rules)
        form.addRow(self._resumable)
        form.addRow(self._auto_retry)
        form.addRow(self._log_to_file)
        form.addRow(self._verify)
        form.addRow(self._notify)

        note = QLabel(
            "Total connections is roughly files × connections-per-file. "
            "Storage class and encryption apply to new uploads; existing "
            "objects are unaffected, use \"Change storage class…\" for those. "
            "Without a detected Content-Type an object is stored as "
            "binary/octet-stream, which a browser downloads instead of showing."
        )
        note.setWordWrap(True)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(note)
        lay.addWidget(btns)
        self._sync_kms()

    @staticmethod
    def _select_data(combo, value):
        idx = combo.findData(value or "")
        combo.setCurrentIndex(idx if idx >= 0 else 0)

    def _sync_kms(self):
        self._kms.setEnabled(self.sse() == "aws:kms")

    def concurrency(self) -> int:
        return self._concurrency.value()

    def parallel_files(self) -> int:
        return self._parallel_files.value()

    def storage_class(self) -> str:
        return self._storage.currentData() or ""

    def sse(self) -> str:
        return self._sse.currentData() or ""

    def kms_key_id(self) -> str:
        return self._kms.text().strip() if self.sse() == "aws:kms" else ""

    def notify(self) -> bool:
        return self._notify.isChecked()

    def verify_downloads(self) -> bool:
        return self._verify.isChecked()

    def checksum_algorithm(self) -> str:
        return self._checksum.currentData() or ""

    def resumable_uploads(self) -> bool:
        return self._resumable.isChecked()

    def multipart_chunksize_mb(self) -> int:
        return self._chunk.value()

    def multipart_threshold_mb(self) -> int:
        return self._threshold.value()

    def rate_limit_kbps(self) -> int:
        return self._rate_limit.value()

    def listing_limit(self) -> int:
        return self._listing_limit.value()

    def auto_retry(self) -> bool:
        return self._auto_retry.isChecked()

    def upload_rules(self) -> list:
        return DataModel.parse_upload_rules(self._upload_rules.toPlainText())

    def log_to_file(self) -> bool:
        return self._log_to_file.isChecked()

    def detect_content_type(self) -> bool:
        return self._detect_type.isChecked()

    def content_type_overrides(self) -> dict:
        return DataModel.parse_content_type_overrides(
            self._type_overrides.toPlainText())


def build_profile_model(profile, bucket=""):
    """A DataModel for another profile's credentials and endpoint."""
    return DataModel(
        profile.url,
        profile.region,
        profile.access_key,
        profile.secret_key,
        bucket,
        profile.no_ssl_check,
        profile.use_path,
        session_token=profile.session_token,
        read_only=profile.read_only,
        requester_pays=getattr(profile, "requester_pays", False),
        public_base_url=getattr(profile, "public_base_url", ""),
        proxy_url=getattr(profile, "proxy_url", ""),
        ca_bundle=getattr(profile, "ca_bundle", ""),
    )


class ProfilePicker(QWidget):
    """
    Pick another profile, one of its buckets and a prefix.

    Shared by the cross-profile copy and sync dialogs. Extracted rather than
    copied because the bucket list is fetched off the GUI thread — a different
    endpoint may be slow or unreachable — and a second copy of that thread
    plumbing is a second chance to leak a QThread.
    """

    def __init__(self, parent, settings, current_profile=""):
        super().__init__(parent)
        self._settings = settings
        self._thread = None
        self._worker = None
        self.note = QLabel("")
        self.note.setWordWrap(True)

        self._raw = [
            raw for raw in load_profiles(settings)
            if str(raw.get("name") or "") != current_profile
        ]

        self.profile = QComboBox()
        for raw in self._raw:
            self.profile.addItem(str(raw.get("name") or "<unnamed>"))
        self.profile.currentIndexChanged.connect(self._on_profile_changed)

        self.bucket = QComboBox()
        self.bucket.setEditable(True)
        self.prefix = QLineEdit()
        self.prefix.setPlaceholderText("prefix (blank = bucket root)")

        form = QFormLayout(self)
        form.setContentsMargins(0, 0, 0, 0)
        form.addRow(QLabel("Profile"), self.profile)
        form.addRow(QLabel("Bucket"), self.bucket)
        form.addRow(QLabel("Prefix"), self.prefix)

        if isinstance(parent, QDialog):
            # accept() does not fire closeEvent, so a dialog dismissed while a
            # bucket listing is in flight would be destroyed with a live
            # QThread — which aborts the process in PyQt6. finished covers
            # accept, reject and close alike.
            parent.finished.connect(lambda _result: self.stop_loader())

        if self._raw:
            self._on_profile_changed(0)

    def has_profiles(self) -> bool:
        return bool(self._raw)

    def selected_profile(self):
        """The decrypted target Profile, or None."""
        index = self.profile.currentIndex()
        if index < 0 or index >= len(self._raw):
            return None
        return decrypt_profile(self._settings, self._raw[index])

    def destination_bucket(self) -> str:
        return self.bucket.currentText().strip()

    def destination_prefix(self) -> str:
        text = self.prefix.text().strip().lstrip("/")
        return prefix_of(text) if text else ""

    def _on_profile_changed(self, _index):
        self.bucket.clear()
        try:
            profile = self.selected_profile()
        except Exception as exc:
            self.note.setText(f"Could not read that profile: {exc}")
            return
        if profile is None:
            return
        if profile.bucket:
            self.bucket.setCurrentText(profile.bucket)
        self._load_buckets(profile)

    def _load_buckets(self, profile):
        self.stop_loader()
        model = build_profile_model(profile)

        def _fetch(_w):
            return [b.name for b in model.list_buckets()]

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_buckets)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_buckets(self, result, exc):
        self.stop_loader()
        if exc is not None:
            self.note.setText(f"Could not list that profile's buckets: {exc}")
            return
        typed = self.bucket.currentText()
        self.bucket.blockSignals(True)
        self.bucket.clear()
        self.bucket.addItems(result or [])
        self.bucket.setCurrentText(typed)
        self.bucket.blockSignals(False)

    def stop_loader(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)


class CrossProfileCopyDialog(QDialog):
    """
    Choose another profile, bucket and prefix to copy the selection into.

    Server-side copy cannot cross credentials, so this is the only way to move
    objects between accounts or providers; the bytes travel through this
    process, which the note makes explicit.
    """

    def __init__(self, parent, settings, count, current_profile=""):
        super().__init__(parent)
        self.setWindowTitle("Copy to another profile")
        self.setMinimumWidth(480)
        self.picker = ProfilePicker(self, settings, current_profile)
        self.picker.note.setText(
            "Objects are streamed through this machine, because a server-side "
            "copy cannot use two sets of credentials.")

        self._buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        self._buttons.accepted.connect(self.accept)
        self._buttons.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(f"Copying {count} item(s)."))
        lay.addWidget(self.picker)
        lay.addWidget(self.picker.note)
        lay.addWidget(self._buttons)

        if not self.picker.has_profiles():
            self.picker.note.setText("No other profile is configured.")
            self._buttons.button(
                QDialogButtonBox.StandardButton.Ok).setEnabled(False)

    def selected_profile(self):
        return self.picker.selected_profile()

    def destination_bucket(self) -> str:
        return self.picker.destination_bucket()

    def destination_prefix(self) -> str:
        return self.picker.destination_prefix()

    def closeEvent(self, event):
        self.picker.stop_loader()
        super().closeEvent(event)


class CrossProfileSyncDialog(QDialog):
    """
    Compare this prefix against a prefix in another profile, then run the plan.

    The dry run is the point: a cross-account sync moves bytes through this
    machine and can delete at the far end, so nothing happens until the plan
    has been seen.
    """

    def __init__(self, parent, main_window, model, prefix, settings,
                 current_profile=""):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._actions = []
        self._dest_model = None
        self.setWindowTitle(f"Sync to another profile — "
                            f"{model.bucket}/{self._prefix}")
        self.resize(820, 560)

        self.picker = ProfilePicker(self, settings, current_profile)

        self._direction = QComboBox()
        self._direction.addItem("Push: this profile → the other", "push")
        self._direction.addItem("Pull: the other → this profile", "pull")

        self._delete_extra = QCheckBox(
            "Delete items at the destination that the source does not have")
        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText("exclude globs, comma separated")

        self._compare = QPushButton("Compare")
        self._compare.clicked.connect(self.compare)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Action", "Path", "Size", "Why"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._summary = QLabel("Nothing compared yet.")
        self._summary.setWordWrap(True)

        self._buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        self._buttons.button(
            QDialogButtonBox.StandardButton.Ok).setText("Run plan")
        self._buttons.accepted.connect(self.accept)
        self._buttons.rejected.connect(self.reject)
        self._ok_enabled(False)

        row = QHBoxLayout()
        row.addWidget(self._direction, 1)
        row.addWidget(self._compare)

        lay = QVBoxLayout(self)
        lay.addWidget(self.picker)
        lay.addLayout(row)
        lay.addWidget(self._exclude)
        lay.addWidget(self._delete_extra)
        lay.addWidget(self._table, 1)
        lay.addWidget(self._summary)
        lay.addWidget(self.picker.note)
        lay.addWidget(self._buttons)

        # A reviewed plan belongs to the inputs it was built from. Leaving it
        # runnable after they change let a plan reviewed as a push execute
        # after switching to pull, deleting on the side believed to be the
        # source.
        self._direction.currentIndexChanged.connect(self.invalidate_plan)
        self.picker.profile.currentIndexChanged.connect(self.invalidate_plan)
        self.picker.bucket.currentTextChanged.connect(self.invalidate_plan)
        self.picker.prefix.textChanged.connect(self.invalidate_plan)
        self._exclude.textChanged.connect(self.invalidate_plan)
        self._delete_extra.toggled.connect(self.invalidate_plan)

        if not self.picker.has_profiles():
            self.picker.note.setText("No other profile is configured.")
            self._compare.setEnabled(False)

    def invalidate_plan(self, *_args):
        """Drop the computed plan and everything derived from it."""
        if not self._actions:
            return
        self._actions = []
        self._dest_model = None
        self._source_model = None
        self._source_prefix = ""
        self._dest_prefix = ""
        self._table.setRowCount(0)
        self._summary.setText("Inputs changed — compare again.")
        self._ok_enabled(False)

    def _ok_enabled(self, enabled):
        self._buttons.button(
            QDialogButtonBox.StandardButton.Ok).setEnabled(bool(enabled))

    def direction(self) -> str:
        return self._direction.currentData() or "push"

    def compare(self):
        """Build the dry-run plan, listing both trees off the GUI thread."""
        bucket = self.picker.destination_bucket()
        if not bucket:
            self.picker.note.setText("Choose a bucket in the other profile.")
            return
        try:
            profile = self.picker.selected_profile()
        except Exception as exc:
            self.picker.note.setText(f"Could not read that profile: {exc}")
            return
        if profile is None:
            return

        other = build_profile_model(profile, bucket)
        here = self._model.clone_for_worker()
        other_prefix = self.picker.destination_prefix()
        mine_prefix = self._prefix
        excludes = self._exclude.text()
        delete_extra = self._delete_extra.isChecked()
        pushing = self.direction() == "push"

        def _scan(_worker):
            mine = here.list_tree(mine_prefix)
            theirs = other.list_tree(other_prefix)
            source, dest = (mine, theirs) if pushing else (theirs, mine)
            return build_sync_plan(
                source, dest, direction="upload",
                delete_extra=delete_extra, exclude=excludes)

        actions, exc = self._mw._run_with_progress("Comparing…", _scan)
        if exc is not None:
            QMessageBox.warning(self, "Sync to another profile",
                                f"Could not compare:\n{exc}")
            return
        if actions is None:
            return

        self._actions = actions
        self._dest_model = other if pushing else self._model.clone_for_worker()
        self._source_prefix = mine_prefix if pushing else other_prefix
        self._dest_prefix = other_prefix if pushing else mine_prefix
        self._source_model = here if pushing else other
        self._render(actions)

    def _render(self, actions):
        self._table.setRowCount(0)
        for action in actions:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._table.setItem(row, 0, QTableWidgetItem(action["action"]))
            self._table.setItem(row, 1, QTableWidgetItem(action["rel"]))
            self._table.setItem(
                row, 2, QTableWidgetItem(_human_bytes(action.get("size") or 0)))
            self._table.setItem(row, 3, QTableWidgetItem(action.get("reason", "")))
        self._summary.setText(summarize_sync_plan(actions))
        self._ok_enabled(any(a["action"] != "skip" for a in actions))

    def plan(self) -> tuple:
        """``(source_model, dest_model, job)`` for the transfer queue."""
        job = build_profile_sync_job(
            self._actions, getattr(self, "_source_prefix", ""),
            getattr(self, "_dest_prefix", ""))
        return getattr(self, "_source_model", None), self._dest_model, job

    def closeEvent(self, event):
        self.picker.stop_loader()
        super().closeEvent(event)


class BulkRenameDialog(QDialog):
    """Rename a whole selection by find-and-replace or a numbering template,
    with a live preview of the resulting names."""

    def __init__(self, parent, items):
        super().__init__(parent)
        self.setWindowTitle("Rename multiple")
        self.resize(660, 520)
        self._items = list(items)   # [(name, is_folder)]
        self._plan = []

        self._mode_find = QRadioButton("Find and replace")
        self._mode_template = QRadioButton("Numbering template")
        self._mode_find.setChecked(True)
        self._mode_find.toggled.connect(self._refresh)

        self._find = QLineEdit()
        self._find.setPlaceholderText("text to find")
        self._replace = QLineEdit()
        self._replace.setPlaceholderText("replacement (may be empty)")
        self._regex = QCheckBox("Regular expression (\\1 backreferences)")
        self._case = QCheckBox("Case sensitive")
        self._case.setChecked(True)

        self._template = QLineEdit("{name}{ext}")
        self._start = QSpinBox()
        self._start.setRange(0, 1000000)
        self._start.setValue(1)
        self._padding = QSpinBox()
        self._padding.setRange(1, 9)
        self._padding.setValue(1)

        for widget in (self._find, self._replace, self._template):
            widget.textChanged.connect(self._refresh)
        for widget in (self._regex, self._case):
            widget.toggled.connect(self._refresh)
        for widget in (self._start, self._padding):
            widget.valueChanged.connect(self._refresh)

        find_form = QFormLayout()
        find_form.addRow(QLabel("Find"), self._find)
        find_form.addRow(QLabel("Replace with"), self._replace)
        find_form.addRow(self._regex)
        find_form.addRow(self._case)
        self._find_box = QGroupBox()
        self._find_box.setLayout(find_form)

        tpl_form = QFormLayout()
        tpl_form.addRow(QLabel("Template"), self._template)
        tpl_form.addRow(QLabel("Start at"), self._start)
        tpl_form.addRow(QLabel("Digits"), self._padding)
        tpl_form.addRow(QLabel(
            "Placeholders: {name} {ext} {n} {orig}"))
        self._tpl_box = QGroupBox()
        self._tpl_box.setLayout(tpl_form)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Current name", "New name"])
        self._table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        self._btns.accepted.connect(self.accept)
        self._btns.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        mode_row = QHBoxLayout()
        mode_row.addWidget(self._mode_find)
        mode_row.addWidget(self._mode_template)
        mode_row.addStretch(1)
        lay.addLayout(mode_row)
        lay.addWidget(self._find_box)
        lay.addWidget(self._tpl_box)
        lay.addWidget(QLabel("Preview:"))
        lay.addWidget(self._table, 1)
        lay.addWidget(self._info)
        lay.addWidget(self._btns)

        self._refresh()

    def _mode(self):
        return BULK_RENAME_FIND if self._mode_find.isChecked() else BULK_RENAME_TEMPLATE

    def _refresh(self):
        is_find = self._mode() == BULK_RENAME_FIND
        self._find_box.setVisible(is_find)
        self._tpl_box.setVisible(not is_find)

        plan, problems = bulk_rename_plan(
            self._items,
            mode=self._mode(),
            find=self._find.text(),
            replace=self._replace.text(),
            regex=self._regex.isChecked(),
            case_sensitive=self._case.isChecked(),
            template=self._template.text(),
            start=self._start.value(),
            padding=self._padding.value(),
        )
        self._plan = plan

        self._table.setRowCount(0)
        for old, new in plan:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(old))
            self._table.setItem(r, 1, QTableWidgetItem(new))

        if problems:
            self._info.setText(
                "<b>Cannot apply:</b><br>" + "<br>".join(problems[:10]))
        elif plan:
            self._info.setText(f"{len(plan)} of {len(self._items)} will be renamed.")
        else:
            self._info.setText("No names would change.")

        ok = self._btns.button(QDialogButtonBox.StandardButton.Ok)
        if ok is not None:
            ok.setEnabled(bool(plan) and not problems)

    def plan(self):
        return list(self._plan)


class SecondPane(QWidget):
    """
    The other half of a two-pane view: a local folder or a remote prefix.

    Deliberately its own small model rather than a second MainWindow: only
    listing and selection are needed here, because every operation the pane
    takes part in is executed by the window's existing transfer queue.
    """

    LOCAL = "local"
    REMOTE = "remote"
    COLUMNS = ("Name", "Size", "Modified")

    def __init__(self, parent, main_window, model):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._thread = None
        self._worker = None
        self._entries = []
        self._bucket = model.bucket or ""
        self._prefix = model.current_folder or ""
        self._local_dir = os.path.expanduser("~")

        self.mode = QComboBox()
        self.mode.addItem("Local folder", self.LOCAL)
        self.mode.addItem("Remote prefix", self.REMOTE)
        self.mode.currentIndexChanged.connect(lambda _i: self.refresh())

        self.path = QLineEdit(self._local_dir)
        self.path.returnPressed.connect(self._go_typed)
        self.up_btn = QPushButton("↑")
        self.up_btn.setFixedWidth(28)
        self.up_btn.setToolTip("Up one level")
        self.up_btn.clicked.connect(self.go_up)
        self.browse_btn = QPushButton("…")
        self.browse_btn.setFixedWidth(28)
        self.browse_btn.clicked.connect(self._browse)

        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.addWidget(self.mode)
        top.addWidget(self.up_btn)
        top.addWidget(self.path, 1)
        top.addWidget(self.browse_btn)

        self.view = QTreeView()
        self.view.setRootIsDecorated(False)
        self.view.setUniformRowHeights(True)
        self.view.setSortingEnabled(False)
        self.view.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self.view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.view.doubleClicked.connect(self._on_activated)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(list(self.COLUMNS))
        self.view.setModel(self.model)

        self.status = QLabel("")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        layout.addLayout(top)
        layout.addWidget(self.view, 1)
        layout.addWidget(self.status)

    def current_mode(self) -> str:
        return self.mode.currentData() or self.LOCAL

    def location(self) -> str:
        if self.current_mode() == self.LOCAL:
            return self._local_dir
        return f"s3://{self._bucket}/{self._prefix}"

    def set_remote(self, bucket, prefix):
        """Point the pane at a remote prefix and show it."""
        self._bucket = bucket or ""
        self._prefix = prefix or ""
        index = self.mode.findData(self.REMOTE)
        if self.mode.currentIndex() != index:
            self.mode.setCurrentIndex(index)   # triggers refresh
        else:
            self.refresh()

    def set_local(self, path):
        self._local_dir = os.path.abspath(path or os.path.expanduser("~"))
        index = self.mode.findData(self.LOCAL)
        if self.mode.currentIndex() != index:
            self.mode.setCurrentIndex(index)
        else:
            self.refresh()

    def remote_target(self) -> tuple:
        return self._bucket, self._prefix

    def local_target(self) -> str:
        return self._local_dir

    def entries(self) -> list:
        """``[(name, is_dir, size)]`` for what is currently listed."""
        return list(self._entries)

    def selected(self) -> list:
        """The selected rows as ``(name, is_dir, size)``."""
        rows = sorted({index.row()
                       for index in self.view.selectionModel().selectedIndexes()})
        return [self._entries[row] for row in rows
                if 0 <= row < len(self._entries)]

    def _browse(self):
        if self.current_mode() == self.LOCAL:
            path = QFileDialog.getExistingDirectory(
                self, "Folder", self._local_dir)
            if path:
                self.set_local(path)
            return
        text, ok = QInputDialog.getText(
            self, "Remote prefix", "s3://bucket/prefix:",
            text=f"s3://{self._bucket}/{self._prefix}")
        if not ok:
            return
        bucket, prefix = MainWindow._parse_s3_location(text, "")
        if bucket:
            self.set_remote(bucket, prefix)

    def _go_typed(self):
        text = self.path.text().strip()
        if self.current_mode() == self.LOCAL:
            if os.path.isdir(text):
                self.set_local(text)
            return
        bucket, prefix = MainWindow._parse_s3_location(text, self._bucket)
        if bucket:
            self.set_remote(bucket, prefix)

    def go_up(self):
        if self.current_mode() == self.LOCAL:
            parent = os.path.dirname(self._local_dir.rstrip(os.sep))
            if parent and parent != self._local_dir:
                self.set_local(parent)
            return
        if not self._prefix:
            return
        trimmed = self._prefix.rstrip("/")
        parent = trimmed.rsplit("/", 1)[0] + "/" if "/" in trimmed else ""
        self.set_remote(self._bucket, parent)

    def _on_activated(self, index):
        row = index.row()
        if not (0 <= row < len(self._entries)):
            return
        name, is_dir, _size = self._entries[row]
        if not is_dir:
            return
        if self.current_mode() == self.LOCAL:
            self.set_local(os.path.join(self._local_dir, name))
        else:
            self.set_remote(self._bucket, (self._prefix or "") + name + "/")

    def refresh(self):
        self.path.setText(
            self._local_dir if self.current_mode() == self.LOCAL
            else f"s3://{self._bucket}/{self._prefix}")
        if self.current_mode() == self.LOCAL:
            self._show(self._scan_local())
            return
        if not self._bucket:
            self._show([])
            self.status.setText("No bucket selected")
            return
        if self._thread is not None:
            return
        clone = self._model.clone_for_worker()
        clone.bucket = self._bucket
        prefix = self._prefix

        def _list(_w):
            return clone.list(prefix)

        self.status.setText("Loading…")
        self._thread = QThread(self)
        self._worker = FuncWorker(_list)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_listed)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _scan_local(self):
        rows = []
        try:
            names = sorted(os.listdir(self._local_dir))
        except OSError as exc:
            self.status.setText(str(exc))
            return []
        for name in names:
            full = os.path.join(self._local_dir, name)
            try:
                is_dir = os.path.isdir(full)
                size = 0 if is_dir else os.path.getsize(full)
            except OSError:
                continue
            rows.append((name, is_dir, size))
        rows.sort(key=lambda row: (not row[1], row[0].lower()))
        return rows

    def _on_listed(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        if exc is not None:
            self.status.setText(f"Could not list: {exc}")
            self._show([])
            return
        rows = [(item.name, item.t == FSObjectType.FOLDER, int(item.size or 0))
                for item in (result or [])]
        rows.sort(key=lambda row: (not row[1], row[0].lower()))
        self._show(rows)

    def _show(self, rows):
        self._entries = list(rows)
        self.model.removeRows(0, self.model.rowCount())
        for name, is_dir, size in self._entries:
            cells = [
                QStandardItem(name),
                QStandardItem("" if is_dir else _human_bytes(size)),
                QStandardItem(""),
            ]
            for cell in cells:
                cell.setEditable(False)
            self.model.appendRow(cells)
        folders = sum(1 for _n, is_dir, _s in self._entries if is_dir)
        self.status.setText(
            f"{folders} folder(s), {len(self._entries) - folders} file(s)")

    def shutdown(self):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)


class PaneCompareDialog(QDialog):
    """
    What differs between the two panes, and a way to copy it either way.

    Both sides are walked recursively, so this also answers "are these two
    prefixes the same" for two locations in one account — which the sync
    dialog could only do by way of a local folder.
    """

    STATUS_LABELS = {
        "only_left": "only on the left",
        "only_right": "only on the right",
        "differs": "differs",
        "same": "same",
    }

    def __init__(self, parent, main_window, left, right):
        super().__init__(parent)
        self._mw = main_window
        # Each side is ("local", path) or ("remote", bucket, prefix).
        self._left = left
        self._right = right
        self._rows = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle("Compare panes")
        self.resize(860, 560)

        self._info = QLabel(
            f"{self.describe(left)}   ⟷   {self.describe(right)}")
        self._info.setWordWrap(True)

        self._hide_same = QCheckBox("Hide identical files")
        self._hide_same.setChecked(True)
        self._hide_same.toggled.connect(self._render)

        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText("Exclude, e.g.  *.tmp  .git/")

        self._btn_compare = QPushButton("Compare")
        self._btn_compare.clicked.connect(self._compare)

        top = QHBoxLayout()
        top.addWidget(self._hide_same)
        top.addWidget(QLabel("Exclude"))
        top.addWidget(self._exclude, 1)
        top.addWidget(self._btn_compare)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Path", "Left", "Right", "Status"])
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)

        self._summary = QLabel("Press Compare.")
        self._summary.setWordWrap(True)

        self._btn_to_right = QPushButton("Copy selected  →")
        self._btn_to_left = QPushButton("←  Copy selected")
        self._btn_to_right.clicked.connect(lambda: self._copy("right"))
        self._btn_to_left.clicked.connect(lambda: self._copy("left"))
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        row = QHBoxLayout()
        row.addWidget(self._btn_to_left)
        row.addWidget(self._btn_to_right)
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self._info)
        layout.addLayout(top)
        layout.addWidget(self._table, 1)
        layout.addWidget(self._summary)
        layout.addLayout(row)
        self._update_buttons()

    @staticmethod
    def describe(side) -> str:
        if side[0] == "local":
            return side[1]
        return f"s3://{side[1]}/{side[2]}"

    @staticmethod
    def read_side(side, model):
        """``{rel: (size, mtime)}`` for one side of the comparison."""
        if side[0] == "local":
            return scan_local_tree(side[1])
        clone = model.clone_for_worker()
        clone.bucket = side[1]
        return clone.list_tree(side[2])

    def _compare(self):
        if self._thread is not None:
            return
        left, right = self._left, self._right
        model = self._mw.data_model
        exclude = self._exclude.text()
        self._summary.setText("Comparing…")
        self._btn_compare.setEnabled(False)

        def _run(_worker):
            return build_compare_plan(
                self.read_side(left, model), self.read_side(right, model),
                exclude=exclude)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_compared)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_compared(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        self._btn_compare.setEnabled(True)
        if exc is not None:
            self._summary.setText(f"Could not compare: {exc}")
            return
        self._rows = list(result or [])
        self._render()

    def visible_rows(self) -> list:
        if self._hide_same.isChecked():
            return [row for row in self._rows if row["status"] != "same"]
        return list(self._rows)

    def _render(self):
        rows = self.visible_rows()
        self._table.setRowCount(0)
        for row in rows:
            index = self._table.rowCount()
            self._table.insertRow(index)
            cells = (
                row["rel"],
                "—" if row["left_size"] is None else _human_bytes(row["left_size"]),
                "—" if row["right_size"] is None else _human_bytes(row["right_size"]),
                self.STATUS_LABELS.get(row["status"], row["status"]),
            )
            for column, text in enumerate(cells):
                self._table.setItem(index, column, QTableWidgetItem(text))
        counts = summarize_compare_plan(self._rows)
        self._summary.setText(
            f"{counts['only_left']} only on the left, "
            f"{counts['only_right']} only on the right, "
            f"{counts['differs']} differ, {counts['same']} identical")
        self._update_buttons()

    def selected_rows(self) -> list:
        rows = self.visible_rows()
        indexes = sorted({index.row()
                          for index in self._table.selectedIndexes()})
        return [rows[index] for index in indexes if 0 <= index < len(rows)]

    def _update_buttons(self):
        selected = self.selected_rows()
        writable = not self._mw.is_read_only()
        # A row that exists only on one side can only travel away from it.
        to_right = [r for r in selected if r["status"] != "only_right"]
        to_left = [r for r in selected if r["status"] != "only_left"]
        self._btn_to_right.setEnabled(
            bool(to_right) and (self._right[0] == "local" or writable))
        self._btn_to_left.setEnabled(
            bool(to_left) and (self._left[0] == "local" or writable))

    def _copy(self, towards):
        source = self._left if towards == "right" else self._right
        target = self._right if towards == "right" else self._left
        skip = "only_right" if towards == "right" else "only_left"
        rows = [row for row in self.selected_rows() if row["status"] != skip]
        if not rows:
            return
        job, method, kwargs = self.build_job(source, target,
                                             [row["rel"] for row in rows])
        if not job:
            self._summary.setText("Nothing that can be copied that way.")
            return
        self.accept()
        self._mw.assign_thread_operation(method, job, **kwargs)
        self._mw.statusBar().showMessage(
            f"Copying {len(job)} item(s) {towards}…", 4000)

    @staticmethod
    def build_job(source, target, rels):
        """
        Turn a set of relative paths into a queue job for this pair of sides.

        Returns ``(job, method, kwargs)``. Which operation it is depends
        entirely on the pair, which is why the panes do not decide it.
        """
        rels = [rel for rel in rels if rel]
        if not rels:
            return [], "", {}
        if source[0] == "local" and target[0] == "remote":
            prefix = target[2] or ""
            return ([(prefix + rel,
                      os.path.join(source[1], rel.replace("/", os.sep)))
                     for rel in rels], "upload", {})
        if source[0] == "remote" and target[0] == "local":
            prefix = source[2] or ""
            job = []
            for rel in rels:
                local = os.path.join(target[1], rel.replace("/", os.sep))
                job.append((prefix + rel, local, None, target[1]))
            return job, "download", {"need_refresh": False}
        if source[0] == "remote" and target[0] == "remote":
            src_prefix, dst_prefix = source[2] or "", target[2] or ""
            cross = source[1] != target[1]
            job = [(src_prefix + rel, dst_prefix + rel, False,
                    target[1] if cross else None) for rel in rels]
            return job, "copy", {
                "source_bucket": source[1] if cross else ""}
        # local -> local is not this app's job.
        return [], "", {}


class WatchFolderDialog(QDialog):
    """Configure the folder that is mirrored up on an interval."""

    INTERVALS = ((30, "30 seconds"), (60, "1 minute"), (300, "5 minutes"),
                 (900, "15 minutes"), (3600, "1 hour"))

    def __init__(self, parent, bucket, prefix, config=None):
        super().__init__(parent)
        config = dict(config or {})
        self.setWindowTitle("Watch a folder")
        self.setMinimumWidth(520)

        self._local = QLineEdit(str(config.get("local") or ""))
        self._local.setPlaceholderText("local folder to mirror upwards…")
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._browse)
        local_row = QHBoxLayout()
        local_row.addWidget(self._local, 1)
        local_row.addWidget(browse)

        self._interval = QComboBox()
        for seconds, label in self.INTERVALS:
            self._interval.addItem(label, seconds)
        index = self._interval.findData(int(config.get("interval") or 300))
        self._interval.setCurrentIndex(max(index, 0))

        self._exclude = QLineEdit(str(config.get("exclude") or ""))
        self._exclude.setPlaceholderText("*.tmp  node_modules/  .git/")

        self._delete_extra = QCheckBox(
            "Delete objects that no longer exist locally")
        self._delete_extra.setChecked(bool(config.get("delete_extra")))

        form = QFormLayout()
        form.addRow(QLabel("Local folder"), local_row)
        form.addRow(QLabel("Destination"),
                    QLabel(f"s3://{bucket}/{prefix}"))
        form.addRow(QLabel("Check every"), self._interval)
        form.addRow(QLabel("Exclude"), self._exclude)
        form.addRow(self._delete_extra)

        note = QLabel(
            "Each check compares the folder with the prefix and queues only "
            "what differs. Watching stops when this window closes, and is "
            "never resumed on its own at startup.")
        note.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(note)
        layout.addWidget(buttons)

    def _browse(self):
        path = QFileDialog.getExistingDirectory(self, "Folder to watch")
        if path:
            self._local.setText(path)

    def _accept(self):
        if not os.path.isdir(self._local.text().strip()):
            QMessageBox.warning(
                self, "Watch a folder", "Choose an existing local folder.")
            return
        self.accept()

    def config(self) -> dict:
        return {
            "local": self._local.text().strip(),
            "interval": int(self._interval.currentData() or 300),
            "exclude": self._exclude.text().strip(),
            "delete_extra": self._delete_extra.isChecked(),
        }


class SyncDialog(QDialog):
    """Compare a local folder with the current prefix, show a dry-run plan,
    then execute it through the transfer queue."""

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._actions = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Sync — {model.bucket}/{self._prefix}")
        self.resize(820, 560)

        self._local = QLineEdit()
        self._local.setPlaceholderText("local folder…")
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._browse)
        local_row = QHBoxLayout()
        local_row.addWidget(self._local, 1)
        local_row.addWidget(browse)

        self._up = QRadioButton("Local → S3 (upload)")
        self._down = QRadioButton("S3 → local (download)")
        self._up.setChecked(True)
        self._delete_extra = QCheckBox(
            "Delete files at the destination that are missing at the source")

        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText(
            "Exclude, e.g.  *.tmp  node_modules/  .git/")
        self._exclude.setToolTip(
            "Space or comma separated globs. A trailing '/' excludes a whole "
            "directory."
        )
        exclude_row = QHBoxLayout()
        exclude_row.addWidget(QLabel("Exclude"))
        exclude_row.addWidget(self._exclude, 1)

        dir_row = QHBoxLayout()
        dir_row.addWidget(self._up)
        dir_row.addWidget(self._down)
        dir_row.addStretch(1)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Action", "Path", "Size", "Why"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._info = QLabel("Pick a local folder and preview the plan.")
        self._info.setWordWrap(True)

        self._btn_preview = QPushButton("Preview (dry run)")
        self._btn_run = QPushButton("Run sync")
        self._btn_run.setEnabled(False)
        close_btn = QPushButton("Close")
        self._btn_preview.clicked.connect(self._preview)
        self._btn_run.clicked.connect(self._run)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_preview)
        row.addWidget(self._btn_run)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(local_row)
        lay.addLayout(dir_row)
        lay.addLayout(exclude_row)
        lay.addWidget(self._delete_extra)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        super().closeEvent(event)

    def _browse(self):
        path = QFileDialog.getExistingDirectory(self, "Select local folder")
        if path:
            self._local.setText(path)

    def direction(self) -> str:
        return "upload" if self._up.isChecked() else "download"

    def exclude_patterns(self) -> list:
        return [p for p in re.split(r"[,\s]+", self._exclude.text() or "") if p]

    def _preview(self):
        if self._thread is not None:
            return
        local_dir = self._local.text().strip()
        if not local_dir or not os.path.isdir(local_dir):
            self._info.setText("Pick an existing local folder first.")
            return
        self._btn_preview.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._info.setText("Scanning both sides…")
        self._table.setRowCount(0)
        self._actions = []

        prefix = self._prefix
        direction = self.direction()
        delete_extra = self._delete_extra.isChecked()
        exclude = self.exclude_patterns()
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _scan(_w):
            remote = clone.list_tree(prefix, cancel_event=cancel)
            local = scan_local_tree(local_dir)
            return build_sync_plan(local, remote, direction=direction,
                                   delete_extra=delete_extra, exclude=exclude)

        self._thread = QThread(self)
        self._worker = FuncWorker(_scan)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_plan)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_plan(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_preview.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Could not build a plan: {exc}")
            return
        self._actions = result or []
        for entry in self._actions:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(entry["action"]))
            self._table.setItem(r, 1, QTableWidgetItem(entry["rel"]))
            self._table.setItem(
                r, 2, QTableWidgetItem(_human_bytes(entry.get("size") or 0)))
            self._table.setItem(r, 3, QTableWidgetItem(entry.get("reason", "")))
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)

        summary = summarize_sync_plan(self._actions)
        bits = [f"{k}: {v}" for k, v in sorted(summary.items())
                if k != "bytes" and v]
        self._info.setText(
            "  ".join(bits) + f"   ({_human_bytes(summary['bytes'])} to transfer)"
        )
        self._btn_run.setEnabled(bool(self.actionable()))

    def actionable(self):
        return [a for a in self._actions if a.get("action") != "skip"]

    def _run(self):
        todo = self.actionable()
        if not todo:
            return
        summary = summarize_sync_plan(todo)
        deletes = summary.get("delete_remote", 0) + summary.get("delete_local", 0)
        message = (
            f"Apply {len(todo)} action(s), transferring "
            f"{_human_bytes(summary['bytes'])}?"
        )
        if deletes:
            message += f"\n\n{deletes} file(s) will be DELETED."
        if QMessageBox.question(
            self, "Run sync", message,
        ) != QMessageBox.StandardButton.Yes:
            return
        self.accept()
        self._mw.start_sync(
            todo, self._local.text().strip(), self._prefix, self.direction())


class BulkTagsDialog(QDialog):
    """Add, overwrite or strip tags across a whole selection."""

    def __init__(self, parent, target_count):
        super().__init__(parent)
        self.setWindowTitle("Edit tags on selection")
        self.setMinimumWidth(520)

        head = QLabel(
            f"Applying to <b>{target_count} selected item(s)</b>. Folders are "
            "expanded to every object beneath them."
        )
        head.setWordWrap(True)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Tag key", "Tag value"])
        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().hide()

        add_btn = QPushButton("Add row")
        del_btn = QPushButton("Remove row")
        add_btn.clicked.connect(lambda: self._insert_row())
        del_btn.clicked.connect(self._remove_row)
        row_btns = QHBoxLayout()
        row_btns.addWidget(add_btn)
        row_btns.addWidget(del_btn)
        row_btns.addStretch(1)

        self._remove_keys = QLineEdit()
        self._remove_keys.setPlaceholderText(
            "Tag keys to delete, comma separated")

        self._replace = QCheckBox(
            "Replace all existing tags (anything not listed above is removed)")

        self.buttonBox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addWidget(head)
        lay.addWidget(QLabel("Tags to set:"))
        lay.addWidget(self._table, 1)
        lay.addLayout(row_btns)
        lay.addWidget(QLabel("Tags to remove:"))
        lay.addWidget(self._remove_keys)
        lay.addWidget(self._replace)
        lay.addWidget(self.buttonBox)

        self._insert_row()

    def _insert_row(self, key="", value=""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(key))
        self._table.setItem(r, 1, QTableWidgetItem(value))

    def _remove_row(self):
        r = self._table.currentRow()
        if r >= 0:
            self._table.removeRow(r)

    def tags_to_add(self) -> dict:
        out = {}
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            key = (k_item.text() if k_item else "").strip()
            if key:
                out[key] = (v_item.text() if v_item else "").strip()
        return out

    def tags_to_remove(self) -> list:
        return [k for k in re.split(r"[,\s]+", self._remove_keys.text() or "")
                if k]

    def replace_all(self) -> bool:
        return self._replace.isChecked()

    def is_noop(self) -> bool:
        return not self.tags_to_add() and not self.tags_to_remove() \
            and not self.replace_all()


class DuplicateFinderDialog(QDialog):
    """
    Find objects holding the same content and delete the redundant copies.

    Grouping uses size + ETag, which ListObjectsV2 already returns, so a scan
    costs no more than a listing. Groups whose ETags cannot settle the question
    are shown separately and are never auto-selected.
    """

    SIZE_UNITS = (("B", 1), ("KB", 1024), ("MB", 1024 ** 2), ("GB", 1024 ** 3))

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._groups = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Find duplicates — {model.bucket}/{self._prefix}")
        self.resize(900, 600)

        self._scope = QCheckBox(
            f"Search the whole bucket (not just {self._prefix or '/'})")
        self._scope.setEnabled(bool(self._prefix))

        self._min_size = QLineEdit("1")
        self._min_size.setMaximumWidth(90)
        self._min_unit = QComboBox()
        for label, factor in self.SIZE_UNITS:
            self._min_unit.addItem(label, factor)
        self._min_unit.setCurrentIndex(2)  # MB — tiny files are mostly noise

        self._btn_scan = QPushButton("Scan")
        self._btn_scan.clicked.connect(self._scan)

        top = QHBoxLayout()
        top.addWidget(QLabel("Ignore files smaller than"))
        top.addWidget(self._min_size)
        top.addWidget(self._min_unit)
        top.addStretch(1)
        top.addWidget(self._btn_scan)

        self._info = QLabel("Scan to look for duplicate objects.")
        self._info.setWordWrap(True)

        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Object", "Size", "Modified", "ETag"])
        self._tree.setColumnWidth(0, 420)
        self._tree.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._tree.itemChanged.connect(self._on_item_changed)

        self._btn_keep_newest = QPushButton("Select all but newest")
        self._btn_keep_oldest = QPushButton("Select all but oldest")
        self._btn_confirm = QPushButton("Confirm candidates")
        self._btn_confirm.setToolTip(
            "Ask the service for each candidate's stored digest — settles a "
            "group without downloading either copy, where the backend "
            "supports it")
        self._btn_confirm.clicked.connect(self._confirm_candidates)
        self._btn_clear = QPushButton("Clear selection")
        self._btn_delete = QPushButton("Delete selected…")
        close_btn = QPushButton("Close")
        self._btn_keep_newest.clicked.connect(
            lambda: self._auto_select("newest"))
        self._btn_keep_oldest.clicked.connect(
            lambda: self._auto_select("oldest"))
        self._btn_clear.clicked.connect(self._clear_selection)
        self._btn_delete.clicked.connect(self._delete_selected)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_keep_newest)
        row.addWidget(self._btn_keep_oldest)
        row.addWidget(self._btn_confirm)
        row.addWidget(self._btn_clear)
        row.addStretch(1)
        row.addWidget(self._btn_delete)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(self._scope)
        lay.addWidget(self._info)
        lay.addWidget(self._tree, 1)
        lay.addLayout(row)

        self._update_buttons()

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        super().closeEvent(event)

    def unconfirmed_groups(self) -> list:
        return [group for group in self._groups if not group.get("confirmed")]

    @staticmethod
    def confirm_groups(model, groups, cancel_event=None) -> list:
        """
        Settle each candidate group from stored digests alone.

        Returns the verdicts in the same order: "same" (a real duplicate),
        "different" (ruled out) or "unknown" (the backend cannot say without
        the bytes). Run off the UI thread — it is one request per member.
        """
        verdicts = []
        for group in groups or []:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("cancelled")
            fingerprints = []
            for key, _modified in group.get("members") or []:
                try:
                    fingerprints.append(model.object_fingerprint(key))
                except Exception:
                    fingerprints.append(("", ""))
            verdicts.append(DataModel.compare_fingerprints(fingerprints))
        return verdicts

    def _confirm_candidates(self):
        if self._thread is not None:
            return
        pending = self.unconfirmed_groups()
        if not pending:
            return
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel
        self._info.setText(
            f"Confirming {len(pending)} candidate group(s) from stored "
            "digests…")
        self._btn_scan.setEnabled(False)
        self._btn_confirm.setEnabled(False)

        def _run(_w):
            return self.confirm_groups(clone, pending, cancel_event=cancel)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(
            lambda result, exc: self._on_confirmed(pending, result, exc))
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_confirmed(self, pending, verdicts, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_scan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Could not confirm: {exc}")
            self._update_buttons()
            return
        confirmed = 0
        ruled_out = 0
        for group, verdict in zip(pending, list(verdicts or [])):
            if verdict == "same":
                group["confirmed"] = True
                confirmed += 1
            elif verdict == "different":
                group["ruled_out"] = True
                ruled_out += 1
        if ruled_out:
            # A group proven to hold different bytes is not a finding.
            self._groups = [group for group in self._groups
                            if not group.get("ruled_out")]
        self._render()
        still = len(self.unconfirmed_groups())
        self._info.setText(
            f"{self._info.text()}  —  confirmed {confirmed}, ruled out "
            f"{ruled_out}, still undecidable {still}")

    def min_size_bytes(self) -> int:
        raw = (self._min_size.text() or "").strip()
        factor = self._min_unit.currentData() or 1
        try:
            value = int(float(raw) * factor)
        except ValueError:
            value = 1
        return max(1, value)

    def _scan(self):
        if self._thread is not None:
            return
        prefix = "" if self._scope.isChecked() else self._prefix
        min_size = self.min_size_bytes()
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel
        self._groups = []
        self._tree.clear()
        self._info.setText("Scanning…")
        self._btn_scan.setEnabled(False)
        self._update_buttons()

        def _run(_w):
            rows = clone.list_object_digests(prefix, cancel_event=cancel)
            return find_duplicate_groups(rows, min_size=min_size)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_scanned)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_scanned(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)
        self._btn_scan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Scan failed: {exc}")
            self._update_buttons()
            return
        self._groups = result or []
        self._render()

    def _render(self):
        self._tree.blockSignals(True)
        self._tree.clear()
        for index, group in enumerate(self._groups):
            confirmed = group["confirmed"]
            title = (
                f"{group['count']} copies · {_human_bytes(group['size'])} each "
                f"· {_human_bytes(group['wasted'])} reclaimable"
            )
            if not confirmed:
                title += "  — same size, ETags cannot confirm"
            parent = QTreeWidgetItem([title, "", "", group["etag"]])
            parent.setFirstColumnSpanned(False)
            parent.setData(0, Qt.ItemDataRole.UserRole, ("group", index))
            self._tree.addTopLevelItem(parent)
            for key, modified in group["members"]:
                child = QTreeWidgetItem([
                    key,
                    _human_bytes(group["size"]),
                    "" if modified is None else str(modified),
                    group["etag"],
                ])
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, Qt.CheckState.Unchecked)
                child.setData(0, Qt.ItemDataRole.UserRole, ("key", key))
                parent.addChild(child)
            parent.setExpanded(True)
        self._tree.blockSignals(False)

        summary = summarize_duplicate_groups(self._groups)
        if not self._groups:
            self._info.setText("No duplicates found.")
        else:
            self._info.setText(
                f"{summary['groups']} group(s), {summary['redundant']} "
                f"redundant copy(ies), {_human_bytes(summary['wasted'])} "
                "reclaimable from confirmed matches. Nothing is selected until "
                "you choose — review before deleting."
            )
        self._update_buttons()

    def _on_item_changed(self, _item, _column):
        self._update_buttons()

    def _iter_key_items(self):
        for i in range(self._tree.topLevelItemCount()):
            parent = self._tree.topLevelItem(i)
            for j in range(parent.childCount()):
                yield parent.child(j)

    def selected_keys(self) -> list:
        out = []
        for item in self._iter_key_items():
            if item.checkState(0) == Qt.CheckState.Checked:
                kind, key = item.data(0, Qt.ItemDataRole.UserRole)
                if kind == "key":
                    out.append(key)
        return out

    def _auto_select(self, keep):
        chosen = select_redundant_keys(self._groups, keep=keep)
        self._tree.blockSignals(True)
        for item in self._iter_key_items():
            _kind, key = item.data(0, Qt.ItemDataRole.UserRole)
            item.setCheckState(
                0,
                Qt.CheckState.Checked if key in chosen else Qt.CheckState.Unchecked,
            )
        self._tree.blockSignals(False)
        self._update_buttons()

    def _clear_selection(self):
        self._tree.blockSignals(True)
        for item in self._iter_key_items():
            item.setCheckState(0, Qt.CheckState.Unchecked)
        self._tree.blockSignals(False)
        self._update_buttons()

    def _selection_would_empty_a_group(self) -> bool:
        """True if every copy in some group is checked — that deletes the
        content outright rather than de-duplicating it."""
        checked = set(self.selected_keys())
        for group in self._groups:
            keys = [k for k, _m in group["members"]]
            if keys and all(k in checked for k in keys):
                return True
        return False

    def _update_buttons(self):
        busy = self._thread is not None
        has_groups = bool(self._groups)
        writable = not self._mw.is_read_only()
        self._btn_keep_newest.setEnabled(has_groups and not busy)
        self._btn_keep_oldest.setEnabled(has_groups and not busy)
        self._btn_clear.setEnabled(has_groups and not busy)
        self._btn_confirm.setEnabled(
            bool(self.unconfirmed_groups()) and not busy)
        self._btn_delete.setEnabled(
            bool(self.selected_keys()) and not busy and writable)

    def _delete_selected(self):
        keys = self.selected_keys()
        if not keys:
            return
        total = 0
        for group in self._groups:
            for key, _m in group["members"]:
                if key in keys:
                    total += group["size"]
        warning = ""
        if self._selection_would_empty_a_group():
            warning = (
                "\n\nWARNING: every copy in at least one group is selected — "
                "that deletes the content entirely, not just the duplicates."
            )
        if QMessageBox.question(
            self, "Delete duplicates",
            f"Permanently delete {len(keys)} object(s), freeing about "
            f"{_human_bytes(total)}?{warning}",
        ) != QMessageBox.StandardButton.Yes:
            return
        self.accept()
        self._mw.delete_duplicate_keys(keys)


class BookmarksDialog(QDialog):
    """Rename or delete saved locations."""

    def __init__(self, parent, bookmarks):
        super().__init__(parent)
        self.setWindowTitle("Bookmarks")
        self.resize(620, 420)
        self._bookmarks = [dict(b) for b in bookmarks or []]

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Name", "Location"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self._render()

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(self._remove)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)

        row = QHBoxLayout()
        row.addWidget(remove_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Edit a name in place, or remove a bookmark."))
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def _render(self):
        self._table.setRowCount(0)
        for entry in self._bookmarks:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(entry.get("name", "")))
            location = QTableWidgetItem(
                f"s3://{entry.get('bucket', '')}/{entry.get('prefix', '')}")
            location.setFlags(location.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(r, 1, location)

    def _sync_names(self):
        """Pull in-place edits back into the backing list.

        _render rebuilds the table from that list, so without this a rename
        followed by a removal would silently lose the rename."""
        for index, entry in enumerate(self._bookmarks):
            item = self._table.item(index, 0)
            if item is not None and item.text():
                entry["name"] = item.text()

    def _remove(self):
        self._sync_names()
        r = self._table.currentRow()
        if 0 <= r < len(self._bookmarks):
            del self._bookmarks[r]
            self._render()

    def bookmarks(self) -> list:
        """The edited list — names are read back out of the table."""
        self._sync_names()
        return [
            {
                "name": entry.get("name", ""),
                "bucket": entry.get("bucket", ""),
                "prefix": entry.get("prefix", ""),
            }
            for entry in self._bookmarks
        ]


class ShortcutsDialog(QDialog):
    """Keyboard reference — plain letters are reserved for type-to-search, so
    every real shortcut carries a modifier and none of them are obvious."""

    def __init__(self, parent, rows):
        super().__init__(parent)
        self.setWindowTitle("Keyboard shortcuts")
        self.resize(560, 560)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Shortcut", "Action"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        for keys, label in rows:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(keys))
            self._table.setItem(r, 1, QTableWidgetItem(label))
        self._table.resizeColumnToContents(0)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)


class TransferHistoryDialog(QDialog):
    """Past transfers, with the option to run one again."""

    def __init__(self, parent, main_window, entries):
        super().__init__(parent)
        self._mw = main_window
        self._entries = list(entries or [])
        self.setWindowTitle("Transfer history")
        self.resize(820, 460)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(
            ["When", "Operation", "Items", "Bytes", "Result"])
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self._table.itemSelectionChanged.connect(self._update_buttons)
        self._render()

        self._btn_rerun = QPushButton("Run again")
        self._btn_rerun.clicked.connect(self._rerun)
        self._btn_rerun.setEnabled(False)
        clear_btn = QPushButton("Clear history")
        clear_btn.clicked.connect(self._clear)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_rerun)
        row.addWidget(clear_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def _render(self):
        self._table.setRowCount(0)
        for entry in self._entries:
            r = self._table.rowCount()
            self._table.insertRow(r)
            cells = [
                entry.get("when", ""),
                entry.get("label", entry.get("method", "")),
                str(entry.get("items", "")),
                _human_bytes(entry.get("bytes") or 0),
                entry.get("status", ""),
            ]
            for c, text in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(text))
        self._table.resizeColumnToContents(0)

    def _selected(self):
        r = self._table.currentRow()
        if 0 <= r < len(self._entries):
            return self._entries[r]
        return None

    def _update_buttons(self):
        entry = self._selected()
        self._btn_rerun.setEnabled(bool(entry and entry.get("job")))

    def _rerun(self):
        entry = self._selected()
        if not entry or not entry.get("job"):
            return
        self.accept()
        self._mw.rerun_history_entry(entry)

    def _clear(self):
        self._entries = []
        self._mw.clear_transfer_history()
        self._render()
        self._update_buttons()


class OverwriteDialog(QDialog):
    """Ask what to do about destinations that already exist."""

    SKIP = "skip"
    OVERWRITE = "overwrite"

    def __init__(self, parent, conflicts, *, total, what="object"):
        super().__init__(parent)
        self.setWindowTitle("Destination already exists")
        self.setMinimumWidth(520)
        self._choice = None

        head = QLabel(
            f"<b>{len(conflicts)} of {total} {what}(s)</b> already exist at the "
            "destination."
        )
        head.setWordWrap(True)

        listing = QPlainTextEdit()
        listing.setReadOnly(True)
        shown = conflicts[:200]
        listing.setPlainText("\n".join(shown) + (
            f"\n… and {len(conflicts) - len(shown)} more"
            if len(conflicts) > len(shown) else ""
        ))
        listing.setMaximumHeight(200)

        skip_btn = QPushButton("Skip existing")
        over_btn = QPushButton("Overwrite")
        cancel_btn = QPushButton("Cancel")
        skip_btn.clicked.connect(lambda: self._pick(self.SKIP))
        over_btn.clicked.connect(lambda: self._pick(self.OVERWRITE))
        cancel_btn.clicked.connect(self.reject)
        skip_btn.setDefault(True)

        row = QHBoxLayout()
        row.addWidget(skip_btn)
        row.addWidget(over_btn)
        row.addStretch(1)
        row.addWidget(cancel_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(head)
        lay.addWidget(listing, 1)
        lay.addLayout(row)

    def _pick(self, choice):
        self._choice = choice
        self.accept()

    def choice(self):
        return self._choice


class TagsDialog(QDialog):
    def __init__(self, parent, model, key: str):
        super().__init__(parent)
        short_name = key.split("/")[-1] or key
        self.setWindowTitle(f"Tags — {short_name}")
        self.setMinimumSize(500, 320)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self._model = model
        self._key = key

        key_lbl = QLabel(f"Object:  {key}")
        key_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Tag key", "Tag value"])
        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().hide()
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        add_btn = QPushButton("Add tag")
        add_btn.clicked.connect(self._add_row)
        rem_btn = QPushButton("Remove selected")
        rem_btn.clicked.connect(self._remove_row)

        btn_row = QHBoxLayout()
        btn_row.addWidget(add_btn)
        btn_row.addWidget(rem_btn)
        btn_row.addStretch()

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self._save)
        btns.rejected.connect(self.reject)
        self._btns = btns

        lay = QVBoxLayout()
        lay.addWidget(key_lbl)
        lay.addWidget(self._table)
        lay.addLayout(btn_row)
        lay.addWidget(btns)
        self.setLayout(lay)

        self._load_tags()

    def _load_tags(self):
        try:
            tags = self._model.get_object_tags(self._key)
        except Exception as exc:
            # Saving now would replace the object's real tags with the empty
            # table, so block it.
            save_btn = self._btns.button(QDialogButtonBox.StandardButton.Save)
            if save_btn is not None:
                save_btn.setEnabled(False)
            QMessageBox.warning(self, "Tags", f"Could not load tags:\n{exc}")
            return
        for tag in tags:
            self._insert_row(tag.get("Key", ""), tag.get("Value", ""))

    def _insert_row(self, k: str = "", v: str = ""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(k))
        self._table.setItem(r, 1, QTableWidgetItem(v))

    def _add_row(self):
        self._insert_row()
        r = self._table.rowCount() - 1
        self._table.scrollToItem(self._table.item(r, 0))
        self._table.editItem(self._table.item(r, 0))

    def _remove_row(self):
        rows = sorted({idx.row() for idx in self._table.selectedIndexes()}, reverse=True)
        for r in rows:
            self._table.removeRow(r)

    def _save(self):
        tags = []
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            k = (k_item.text() if k_item else "").strip()
            v = (v_item.text() if v_item else "").strip()
            if k:
                tags.append({"Key": k, "Value": v})
        try:
            self._model.put_object_tags(self._key, tags)
            self.accept()
        except Exception as exc:
            QMessageBox.critical(self, "Tags", f"Could not save tags:\n{exc}")




class PreviewDialog(QDialog):
    """Inline preview for a single object: images and text render in-app;
    anything else can be opened with the OS default application."""

    IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".ico"}
    TEXT_EXTS = {
        ".txt", ".md", ".log", ".json", ".xml", ".yaml", ".yml", ".csv",
        ".tsv", ".ini", ".cfg", ".conf", ".py", ".js", ".ts", ".html",
        ".htm", ".css", ".sh", ".c", ".h", ".cpp", ".go", ".rs", ".java",
        ".sql", ".toml", ".svg",
    }
    CODE_EXTS = {
        ".py", ".js", ".ts", ".c", ".h", ".cpp", ".go", ".rs", ".java",
        ".sh", ".sql", ".css", ".html", ".htm", ".xml", ".json", ".yaml",
        ".yml", ".toml", ".ini", ".cfg", ".conf",
    }
    PDF_EXTS = {".pdf"}
    IMG_LIMIT = 25 * 1024 * 1024
    TEXT_LIMIT = 1 * 1024 * 1024
    PDF_LIMIT = 50 * 1024 * 1024

    def __init__(self, parent, model, key):
        super().__init__(parent)
        # _open_external stages the download in the window's temp workspace;
        # without this it raised AttributeError from inside a Qt slot.
        self._mw = parent
        self._model = model
        self._key = key
        self._thread = None
        self._worker = None
        self._dl_thread = None
        self._dl_worker = None
        self._save_thread = None
        self._save_worker = None
        self._etag = ""
        self._editable = False
        self._editing = False

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Preview — {base}")
        self.resize(760, 620)

        self._stack = QStackedWidget()

        self._status = QLabel("Loading preview…")
        self._status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._status.setWordWrap(True)

        self._img_label = QLabel()
        self._img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._img_scroll = QScrollArea()
        self._img_scroll.setWidget(self._img_label)
        self._img_scroll.setWidgetResizable(True)

        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._text.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont)
            )
        except Exception:
            pass

        self._hex = QPlainTextEdit()
        self._hex.setReadOnly(True)
        self._hex.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._hex.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        except Exception:
            pass

        self._highlighter = None
        self._pdf_doc = None
        self._pdf_view = None

        self._stack.addWidget(self._status)      # index 0
        self._stack.addWidget(self._img_scroll)  # index 1
        self._stack.addWidget(self._text)        # index 2
        self._stack.addWidget(self._hex)         # index 3
        if QPdfView is not None:
            self._pdf_doc = QPdfDocument(self)
            self._pdf_view = QPdfView(self)
            self._pdf_view.setDocument(self._pdf_doc)
            self._pdf_view.setPageMode(QPdfView.PageMode.MultiPage)
            self._stack.addWidget(self._pdf_view)   # index 4

        self._open_btn = QPushButton("Open with default app")
        self._open_btn.clicked.connect(self._open_external)
        self._edit_btn = QPushButton("Edit")
        self._edit_btn.clicked.connect(self._toggle_edit)
        self._edit_btn.hide()
        self._save_btn = QPushButton("Save")
        self._save_btn.clicked.connect(self._save)
        self._save_btn.hide()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)

        btns = QHBoxLayout()
        btns.addWidget(self._open_btn)
        btns.addWidget(self._edit_btn)
        btns.addWidget(self._save_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._stack, 1)
        lay.addLayout(btns)

        self._start_load()

    def _ext(self):
        return os.path.splitext(self._key)[1].lower()

    def _start_load(self):
        ext = self._ext()
        if ext in self.IMAGE_EXTS:
            max_bytes = self.IMG_LIMIT
        elif ext in self.PDF_EXTS and QPdfView is not None:
            # A PDF cannot be rendered from a partial file.
            max_bytes = self.PDF_LIMIT
        else:
            max_bytes = self.TEXT_LIMIT
        key = self._key
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return clone.get_object_preview(key, max_bytes)

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        if self._editing and self._text.document().isModified():
            answer = QMessageBox.question(
                self, "Preview",
                "Discard the unsaved changes to this object?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if answer != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
        self._stop_load_thread()
        join_qthread(self._save_thread)
        self._save_thread = None
        self._save_worker = None
        super().closeEvent(event)

    def _stop_load_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_loaded(self, result, exc):
        self._stop_load_thread()
        if exc is not None:
            self._status.setText(f"Could not load preview:\n{exc}")
            self._stack.setCurrentIndex(0)
            return

        data = result.get("data") or b""
        ctype = (result.get("content_type") or "").lower()
        truncated = result.get("truncated")
        self._etag = str(result.get("etag") or "")
        ext = self._ext()

        # Raster images render as a pixmap (SVG is XML, shown as text below).
        is_raster = (ext in self.IMAGE_EXTS) or (
            ctype.startswith("image/") and "svg" not in ctype
        )
        if is_raster and ext != ".svg":
            pix = QPixmap()
            if pix.loadFromData(data):
                self._img_label.setPixmap(pix)
                self._stack.setCurrentIndex(1)
                return
            # not a decodable raster -> fall through to text/binary handling

        if ext in self.PDF_EXTS or ctype == "application/pdf":
            if self._pdf_view is not None and not truncated:
                self._pdf_bytes = QByteArray(data)
                self._pdf_buffer = QBuffer(self._pdf_bytes, self)
                self._pdf_buffer.open(QIODevice.OpenModeFlag.ReadOnly)
                self._pdf_doc.load(self._pdf_buffer)
                if self._pdf_doc.status() == QPdfDocument.Status.Ready:
                    self._stack.setCurrentIndex(4)
                    return
            self._status.setText(
                "Could not render this PDF inline"
                + (" (file is larger than the preview limit)." if truncated
                   else ".")
                + '\n\nUse "Open with default app".'
            )
            self._stack.setCurrentIndex(0)
            return

        if b"\x00" in data[:8192]:
            # Binary: show a hex dump rather than refusing outright.
            self._hex.setPlainText(hex_dump(data))
            self._stack.setCurrentIndex(3)
            return

        text = data.decode("utf-8", errors="replace")
        if truncated:
            text += "\n\n… (preview truncated)"
        self._text.setPlainText(text)
        if ext in self.CODE_EXTS and self._highlighter is None:
            self._highlighter = CodeHighlighter(self._text.document())
        self._stack.setCurrentIndex(2)
        # Editing is offered only for text that arrived whole and decoded
        # cleanly: saving a truncated body would silently delete the tail, and
        # saving a replacement-charactered decode would corrupt the file.
        self._editable = (
            not truncated
            and not getattr(self._model, "read_only", False)
            and "\ufffd" not in text)
        self._edit_btn.setVisible(self._editable)
        if truncated:
            self._edit_btn.setToolTip(
                "Too large to edit in-app; the preview is truncated")
        elif getattr(self._model, "read_only", False):
            self._edit_btn.setToolTip("Profile is read-only")

    def _toggle_edit(self):
        if not self._editable:
            return
        self._editing = not self._editing
        self._text.setReadOnly(not self._editing)
        self._edit_btn.setText("Cancel edit" if self._editing else "Edit")
        self._save_btn.setVisible(self._editing)
        if self._editing:
            self._text.setFocus(Qt.FocusReason.OtherFocusReason)
        else:
            # Cancelling reloads rather than keeping the edited buffer around
            # looking authoritative.
            self._start_load()

    def _save(self):
        if self._save_thread is not None:
            return
        payload = self._text.toPlainText().encode("utf-8")
        key = self._key
        etag = self._etag
        clone = self._model.clone_for_worker()

        def _write(_w):
            return clone.put_object_body(key, payload, if_match=etag)

        self._save_btn.setEnabled(False)
        self._save_thread = QThread(self)
        self._save_worker = FuncWorker(_write)
        self._save_worker.moveToThread(self._save_thread)
        self._save_thread.started.connect(self._save_worker.run)
        self._save_worker.done.connect(self._on_saved)
        self._save_worker.done.connect(self._save_thread.quit)
        release_worker_on_finish(self._save_thread, self._save_worker)
        self._save_thread.finished.connect(self._save_thread.deleteLater)
        self._save_thread.start()

    def _on_saved(self, result, exc):
        th, self._save_thread, self._save_worker = self._save_thread, None, None
        join_qthread(th)
        self._save_btn.setEnabled(True)
        if exc is not None:
            if isinstance(exc, DataModel.PreconditionFailed):
                QMessageBox.warning(
                    self, "Save",
                    f"{exc}\n\nNothing was written. Close and reopen the "
                    "preview to see the current contents.")
            else:
                QMessageBox.warning(self, "Save", f"Could not save:\n{exc}")
            return
        self._etag = str(result or "")
        self._editing = False
        self._text.setReadOnly(True)
        self._edit_btn.setText("Edit")
        self._save_btn.hide()
        if getattr(self._mw, "statusBar", None) is not None:
            self._mw.statusBar().showMessage("Saved", 3000)

    def _open_external(self):
        if self._dl_thread is not None:
            return
        base = os.path.basename(self._key.rstrip("/")) or "object"
        tmp_dir = self._mw.temp_workspace.make(prefix="preview_")
        out_path = os.path.join(tmp_dir, base)
        key = self._key
        clone = self._model.clone_for_worker()
        cancel = threading.Event()

        prog = QProgressDialog("Downloading…", "Cancel", 0, 100, self)
        prog.setWindowTitle("Open with default app")
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.setAutoClose(False)
        prog.setAutoReset(False)
        prog.canceled.connect(cancel.set)

        def _dl(w):
            def _cb(total, cur, _k):
                w.progress.emit(int(cur), int(total or 0))
            clone.download_file(
                key, out_path, tmp_dir, progress_cb=_cb, cancel_event=cancel
            )
            return out_path

        self._dl_thread = QThread(self)
        self._dl_worker = FuncWorker(_dl)
        self._dl_worker.moveToThread(self._dl_thread)
        self._dl_thread.started.connect(self._dl_worker.run)

        def _on_prog(cur, total):
            if total > 0:
                prog.setMaximum(100)
                prog.setValue(min(100, int(cur * 100 / total)))
            else:
                prog.setMaximum(0)  # indeterminate

        def _on_done(result, exc):
            prog.close()
            self._dl_thread.quit()
            self._dl_thread = None
            self._dl_worker = None
            self._open_btn.setEnabled(True)
            if exc is not None:
                if not cancel.is_set():
                    QMessageBox.warning(
                        self, "Open", f"Could not download file:\n{exc}"
                    )
                return
            QDesktopServices.openUrl(QUrl.fromLocalFile(result))

        self._dl_worker.progress.connect(_on_prog)
        self._dl_worker.done.connect(_on_done)
        release_worker_on_finish(self._dl_thread, self._dl_worker)
        self._dl_thread.finished.connect(self._dl_thread.deleteLater)
        self._open_btn.setEnabled(False)
        self._dl_thread.start()
        prog.show()


class VersionDiffDialog(QDialog):
    """
    A unified diff between two versions of one object.

    The versions list can already download any version; what it could not do
    is answer the question anyone actually opens it with — what changed.
    """

    MAX_BYTES = 512 * 1024

    def __init__(self, parent, model, key, older, newer):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._older = older
        self._newer = newer
        self._thread = None
        self._worker = None

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Diff — {base}")
        self.resize(880, 620)

        self._info = QLabel("Loading both versions…")
        self._info.setWordWrap(True)
        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        try:
            self._text.setFont(
                QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        except Exception:
            pass
        self._highlighter = DiffHighlighter(self._text.document())

        copy_btn = QPushButton("Copy diff")
        copy_btn.clicked.connect(self._copy)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addWidget(copy_btn)
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addWidget(self._info)
        layout.addWidget(self._text, 1)
        layout.addLayout(row)
        self._load()

    @staticmethod
    def build_diff(older_bytes, newer_bytes, older_label, newer_label,
                   truncated=False) -> tuple:
        """
        ``(text, problem)`` for two version bodies.

        Binary content is refused rather than rendered as mojibake: a diff of
        replacement characters looks like a change on every line.
        """
        for payload in (older_bytes, newer_bytes):
            if b"\x00" in (payload or b"")[:8192]:
                return "", "One of these versions is binary; there is nothing to diff."
        older_text = (older_bytes or b"").decode("utf-8", errors="replace")
        newer_text = (newer_bytes or b"").decode("utf-8", errors="replace")
        if "\ufffd" in older_text or "\ufffd" in newer_text:
            return "", "One of these versions is not UTF-8 text."
        if older_text == newer_text:
            note = " (within the part that was read)" if truncated else ""
            return "", f"These two versions are identical{note}."
        diff = difflib.unified_diff(
            older_text.splitlines(), newer_text.splitlines(),
            fromfile=older_label, tofile=newer_label, lineterm="")
        return "\n".join(diff), ""

    def _load(self):
        model = self._model
        key = self._key
        older, newer = self._older, self._newer
        limit = self.MAX_BYTES

        def _fetch(_worker):
            clone = model.clone_for_worker()
            return (clone.get_object_preview(key, limit, version_id=older),
                    clone.get_object_preview(key, limit, version_id=newer))

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_loaded(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        if exc is not None:
            self._info.setText(f"Could not read the versions: {exc}")
            return
        first, second = result
        truncated = bool(first.get("truncated") or second.get("truncated"))
        text, problem = self.build_diff(
            first.get("data"), second.get("data"),
            f"{self._key}@{self._older}", f"{self._key}@{self._newer}",
            truncated=truncated)
        if problem:
            self._info.setText(problem)
            self._text.setPlainText("")
            return
        self._info.setText(
            "Only the first "
            f"{_human_bytes(self.MAX_BYTES)} of each version was read."
            if truncated else "")
        self._text.setPlainText(text)

    def _copy(self):
        QApplication.clipboard().setText(self._text.toPlainText())

    def closeEvent(self, event):
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)
        super().closeEvent(event)


class DiffHighlighter(QSyntaxHighlighter):
    """Colours a unified diff: added, removed and hunk lines."""

    def __init__(self, document):
        super().__init__(document)
        self._added = QTextCharFormat()
        self._added.setForeground(QColor("#2e7d32"))
        self._removed = QTextCharFormat()
        self._removed.setForeground(QColor("#c62828"))
        self._hunk = QTextCharFormat()
        self._hunk.setForeground(QColor("#1565c0"))

    def highlightBlock(self, text):
        if text.startswith("@@"):
            self.setFormat(0, len(text), self._hunk)
        elif text.startswith("+") and not text.startswith("+++"):
            self.setFormat(0, len(text), self._added)
        elif text.startswith("-") and not text.startswith("---"):
            self.setFormat(0, len(text), self._removed)


class VersionsDialog(QDialog):
    """List, download, restore, and delete individual object versions."""

    def __init__(self, parent, main_window, model, key):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._key = key
        self._versions = []
        self._dl_thread = None
        self._dl_worker = None
        self._list_thread = None
        self._list_worker = None

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Versions — {base}")
        self.resize(720, 420)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(
            ["Modified", "Size", "Storage", "Version", "State"]
        )
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        # Extended so two rows can be picked for a diff; every other action
        # still works on exactly one.
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._btn_diff = QPushButton("Compare two…")
        self._btn_diff.setToolTip(
            "Select two versions to see what changed between them")
        self._btn_diff.clicked.connect(self._diff_selected)
        self._btn_download = QPushButton("Download…")
        self._btn_current = QPushButton("Make current")
        self._btn_delete = QPushButton("Delete version")
        close_btn = QPushButton("Close")
        self._btn_download.clicked.connect(self._download_selected)
        self._btn_current.clicked.connect(self._make_current_selected)
        self._btn_delete.clicked.connect(self._delete_selected)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_diff)
        row.addWidget(self._btn_download)
        row.addWidget(self._btn_current)
        row.addWidget(self._btn_delete)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload()

    def closeEvent(self, event):
        self._stop_list_thread()
        super().closeEvent(event)

    def _stop_list_thread(self):
        th, self._list_thread, self._list_worker = self._list_thread, None, None
        join_qthread(th)

    def _reload(self):
        """Fetch versions + bucket versioning status off the main thread; both
        are network calls and froze the dialog on buckets with many versions."""
        if self._list_thread is not None:
            return
        self._info.setText("Loading versions…")
        self._set_buttons_enabled(False)
        key = self._key
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return (clone.list_object_versions(key),
                    clone.get_bucket_versioning_status())

        self._list_thread = QThread(self)
        self._list_worker = FuncWorker(_fetch)
        self._list_worker.moveToThread(self._list_thread)
        self._list_thread.started.connect(self._list_worker.run)
        self._list_worker.done.connect(self._on_versions_loaded)
        self._list_worker.done.connect(self._list_thread.quit)
        release_worker_on_finish(self._list_thread, self._list_worker)
        self._list_thread.finished.connect(self._list_thread.deleteLater)
        self._list_thread.start()

    def _on_versions_loaded(self, result, exc):
        self._stop_list_thread()
        if exc is not None:
            self._versions = []
            self._render([], "")
            QMessageBox.warning(
                self, "Versions", f"Could not list versions:\n{exc}"
            )
            return
        versions, status = result
        self._versions = versions
        self._render(versions, status)

    def _set_buttons_enabled(self, enabled: bool):
        self._btn_download.setEnabled(enabled)
        self._btn_current.setEnabled(enabled)
        self._btn_delete.setEnabled(enabled)

    def _render(self, versions, status):
        if versions:
            head = f"{len(versions)} version(s)"
        else:
            head = "No stored versions"
        if status:
            head += f" — bucket versioning: {status}"
        elif not versions:
            head += " — this bucket may not have versioning enabled"
        self._info.setText(head)

        self._table.setRowCount(0)
        for v in versions:
            r = self._table.rowCount()
            self._table.insertRow(r)
            when = "" if v["last_modified"] is None else str(v["last_modified"])
            size = "—" if v["is_delete_marker"] else _human_bytes(v["size"])
            storage = "" if v["is_delete_marker"] else v["storage_class"]
            state = []
            if v["is_latest"]:
                state.append("latest")
            if v["is_delete_marker"]:
                state.append("delete-marker")
            cells = [when, size, storage, v["version_id"], ", ".join(state)]
            for c, txt in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(txt))
        self._table.resizeColumnsToContents()
        self._update_buttons()

    def _selected(self):
        rows = self._table.selectionModel().selectedRows()
        if len(rows) != 1:
            return None
        idx = rows[0].row()
        if 0 <= idx < len(self._versions):
            return self._versions[idx]
        return None

    def selected_versions(self) -> list:
        """Every selected row, oldest first — the order a diff reads in."""
        rows = sorted(index.row()
                      for index in self._table.selectionModel().selectedRows())
        picked = [self._versions[row] for row in rows
                  if 0 <= row < len(self._versions)]
        # The listing is newest first, so reverse into chronological order.
        return list(reversed(picked))

    def diffable_pair(self):
        """The two versions to diff, or None when the selection is not two."""
        picked = [v for v in self.selected_versions()
                  if not v["is_delete_marker"]]
        if len(picked) != 2:
            return None
        return picked[0]["version_id"], picked[1]["version_id"]

    def _diff_selected(self):
        pair = self.diffable_pair()
        if pair is None:
            return
        VersionDiffDialog(self, self._model, self._key, *pair).exec()

    def _update_buttons(self):
        v = self._selected()
        has = v is not None
        is_dm = bool(v and v["is_delete_marker"])
        self._btn_download.setEnabled(has and not is_dm)
        self._btn_current.setEnabled(has and not is_dm and not v["is_latest"])
        self._btn_delete.setEnabled(has)
        self._btn_diff.setEnabled(self.diffable_pair() is not None)

    def _make_current_selected(self):
        v = self._selected()
        if not v:
            return
        if QMessageBox.question(
            self, "Make current",
            "Promote this version to be the current object?\n\n"
            "A new current version is created with this version's data; "
            "nothing is deleted.",
        ) != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.make_version_current(
                self._key, v["version_id"], log_fn=self._mw.log
            )
        except Exception as exc:
            QMessageBox.warning(self, "Make current", f"Failed:\n{exc}")
            return
        self._mw.log(f"restored version {v['version_id']} of {self._key}")
        self._reload()
        self._mw.navigate()

    def _delete_selected(self):
        v = self._selected()
        if not v:
            return
        what = "delete marker" if v["is_delete_marker"] else "version"
        if QMessageBox.question(
            self, "Delete version",
            f"Permanently delete this {what}?\nThis cannot be undone.",
        ) != QMessageBox.StandardButton.Yes:
            return
        try:
            self._model.delete_object_version(
                self._key, v["version_id"], log_fn=self._mw.log
            )
        except Exception as exc:
            QMessageBox.warning(self, "Delete version", f"Failed:\n{exc}")
            return
        self._mw.log(f"deleted {what} {v['version_id']} of {self._key}")
        self._reload()
        self._mw.navigate()

    def _download_selected(self):
        if self._dl_thread is not None:
            return
        v = self._selected()
        if not v or v["is_delete_marker"]:
            return
        base = os.path.basename(self._key.rstrip("/")) or "object"
        path, _ = QFileDialog.getSaveFileName(self, "Save version as", base)
        if not path:
            return
        key = self._key
        vid = v["version_id"]
        clone = self._model.clone_for_worker()
        cancel = threading.Event()

        prog = QProgressDialog("Downloading…", "Cancel", 0, 100, self)
        prog.setWindowTitle("Download version")
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.setAutoClose(False)
        prog.setAutoReset(False)
        prog.canceled.connect(cancel.set)

        def _dl(w):
            def _cb(total, cur, _k):
                w.progress.emit(int(cur), int(total or 0))
            clone.download_object_version(
                key, vid, path, progress_cb=_cb, cancel_event=cancel
            )
            return path

        self._dl_thread = QThread(self)
        self._dl_worker = FuncWorker(_dl)
        self._dl_worker.moveToThread(self._dl_thread)
        self._dl_thread.started.connect(self._dl_worker.run)

        def _on_prog(cur, total):
            if total > 0:
                prog.setMaximum(100)
                prog.setValue(min(100, int(cur * 100 / total)))
            else:
                prog.setMaximum(0)

        def _on_done(result, exc):
            prog.close()
            self._dl_thread.quit()
            self._dl_thread = None
            self._dl_worker = None
            if exc is not None:
                if not cancel.is_set():
                    QMessageBox.warning(
                        self, "Download version", f"Failed:\n{exc}"
                    )
                return
            self._mw.statusBar().showMessage(f"Saved version to {result}", 4000)

        self._dl_worker.progress.connect(_on_prog)
        self._dl_worker.done.connect(_on_done)
        release_worker_on_finish(self._dl_thread, self._dl_worker)
        self._dl_thread.finished.connect(self._dl_thread.deleteLater)
        self._dl_thread.start()
        prog.show()


class IncompleteUploadsDialog(QDialog):
    """List and abort in-flight multipart uploads.

    Orphaned uploads (from a cancelled or crashed transfer) keep their already
    uploaded parts stored and billed, are invisible in normal object listings,
    and make DeleteBucket fail."""

    def __init__(self, parent, main_window, model, prefix=""):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._uploads = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Incomplete uploads — {model.bucket}")
        self.resize(820, 460)

        self._info = QLabel("")
        self._info.setWordWrap(True)

        self._scope = QCheckBox(
            f"Only under current prefix ({self._prefix or '/'})"
        )
        self._scope.setChecked(bool(self._prefix))
        self._scope.setEnabled(bool(self._prefix))
        self._scope.toggled.connect(self._reload)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Key", "Initiated", "Size", "Upload ID"]
        )
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._btn_refresh = QPushButton("Refresh")
        self._btn_abort = QPushButton("Abort selected")
        self._btn_abort_old = QPushButton("Abort older than…")
        close_btn = QPushButton("Close")
        self._btn_refresh.clicked.connect(self._reload)
        self._btn_abort.clicked.connect(self._abort_selected)
        self._btn_abort_old.clicked.connect(self._abort_older_than)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._btn_refresh)
        row.addWidget(self._btn_abort)
        row.addWidget(self._btn_abort_old)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._info)
        lay.addWidget(self._scope)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload()

    def closeEvent(self, event):
        self._stop_thread(cancel=True)
        super().closeEvent(event)

    def _stop_thread(self, cancel: bool = False):
        # Sizing every upload costs one ListParts call, so a scan can run for
        # minutes. When the dialog is closing, signal cancellation first —
        # otherwise the join times out and the dialog is destroyed with its
        # thread still running. On the completion path there is nothing left to
        # cancel, so the flag stays untouched.
        if cancel and self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(th)

    def _busy(self, busy: bool):
        self._btn_refresh.setEnabled(not busy)
        self._btn_abort_old.setEnabled(not busy)
        self._scope.setEnabled(not busy and bool(self._prefix))
        if busy:
            self._btn_abort.setEnabled(False)
        else:
            self._update_buttons()

    def _reload(self):
        if self._thread is not None:
            return
        self._info.setText("Scanning for incomplete uploads…")
        self._table.setRowCount(0)
        self._uploads = []
        self._busy(True)
        prefix = self._prefix if self._scope.isChecked() else ""
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _fetch(_w):
            return clone.list_multipart_uploads(
                prefix, with_sizes=True, cancel_event=cancel
            )

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_loaded(self, result, exc):
        self._stop_thread()
        self._busy(False)
        if exc is not None:
            self._info.setText(f"Could not list incomplete uploads: {exc}")
            return
        self._uploads = result or []
        wasted = sum(int(u["size"] or 0) for u in self._uploads)
        for u in self._uploads:
            r = self._table.rowCount()
            self._table.insertRow(r)
            when = "" if u["initiated"] is None else str(u["initiated"])
            size = "?" if u["size"] is None else _human_bytes(u["size"])
            cells = [u["key"], when, size, u["upload_id"]]
            for c, txt in enumerate(cells):
                self._table.setItem(r, c, QTableWidgetItem(txt))
        if self._uploads:
            self._info.setText(
                f"{len(self._uploads)} incomplete upload(s) — "
                f"{_human_bytes(wasted)} of stored parts still billed"
            )
        else:
            self._info.setText("No incomplete multipart uploads. 🎉")
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setStretchLastSection(True)
        self._update_buttons()

    def _update_buttons(self):
        self._btn_abort.setEnabled(
            self._thread is None and bool(self._selected_uploads())
        )

    def _selected_uploads(self):
        rows = {ix.row() for ix in self._table.selectedIndexes()}
        return [self._uploads[r] for r in sorted(rows)
                if 0 <= r < len(self._uploads)]

    def _abort_selected(self):
        self._abort(self._selected_uploads())

    def _abort_older_than(self):
        days, ok = QInputDialog.getInt(
            self, "Abort older than",
            "Abort uploads started more than N days ago:", 7, 0, 3650,
        )
        if not ok:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        targets = []
        for u in self._uploads:
            started = u["initiated"]
            # Only real datetimes are comparable; a backend returning something
            # else must not raise inside this slot.
            if not isinstance(started, datetime):
                continue
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            if started < cutoff:
                targets.append(u)
        if not targets:
            QMessageBox.information(
                self, "Abort older than",
                f"No incomplete uploads are older than {days} day(s).",
            )
            return
        self._abort(targets)

    def _abort(self, uploads):
        if not uploads or self._thread is not None:
            return
        wasted = sum(int(u["size"] or 0) for u in uploads)
        if QMessageBox.question(
            self, "Abort uploads",
            f"Abort {len(uploads)} incomplete upload(s), freeing about "
            f"{_human_bytes(wasted)}?\n\n"
            "Their uploaded parts are discarded permanently. Any transfer "
            "still running for them will fail.",
        ) != QMessageBox.StandardButton.Yes:
            return

        self._info.setText(f"Aborting {len(uploads)} upload(s)…")
        self._busy(True)
        targets = [(u["key"], u["upload_id"]) for u in uploads]
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _run(_w):
            failures = []
            for key, upload_id in targets:
                if cancel.is_set():
                    break
                try:
                    clone.abort_multipart_upload(key, upload_id)
                except Exception as exc:
                    failures.append(f"{key}: {exc}")
            return failures

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_aborted)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_aborted(self, result, exc):
        self._stop_thread()
        self._busy(False)
        if exc is not None:
            QMessageBox.warning(self, "Abort uploads", f"Failed:\n{exc}")
        else:
            failures = result or []
            self._mw.log(
                f"aborted incomplete uploads ({len(failures)} failure(s))"
            )
            if failures:
                QMessageBox.warning(
                    self, "Abort uploads",
                    "Some uploads could not be aborted:\n\n"
                    + "\n".join(failures[:20]),
                )
        self._reload()


class MetadataDialog(QDialog):
    """Edit an object's system headers (Content-Type, Cache-Control, …) and
    custom x-amz-meta-* user metadata."""

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._storage_class = "STANDARD"

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Metadata — {base}")
        self.resize(560, 480)

        self._content_type = QLineEdit()
        self._cache_control = QLineEdit()
        self._content_disposition = QLineEdit()
        self._content_encoding = QLineEdit()

        form = QFormLayout()
        form.addRow(QLabel("Content-Type"), self._content_type)
        form.addRow(QLabel("Cache-Control"), self._cache_control)
        form.addRow(QLabel("Content-Disposition"), self._content_disposition)
        form.addRow(QLabel("Content-Encoding"), self._content_encoding)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Metadata key", "Value"])
        self._table.horizontalHeader().setStretchLastSection(True)

        add_btn = QPushButton("Add")
        del_btn = QPushButton("Remove")
        add_btn.clicked.connect(lambda: self._insert_row("", ""))
        del_btn.clicked.connect(self._remove_row)
        row_btns = QHBoxLayout()
        row_btns.addWidget(add_btn)
        row_btns.addWidget(del_btn)
        row_btns.addStretch(1)

        self.buttonBox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttonBox.accepted.connect(self._save)
        self.buttonBox.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(QLabel("Custom metadata (x-amz-meta-*):"))
        lay.addWidget(self._table, 1)
        lay.addLayout(row_btns)
        lay.addWidget(self.buttonBox)

        self._load()

    def _insert_row(self, k="", v=""):
        r = self._table.rowCount()
        self._table.insertRow(r)
        self._table.setItem(r, 0, QTableWidgetItem(k))
        self._table.setItem(r, 1, QTableWidgetItem(v))

    def _remove_row(self):
        r = self._table.currentRow()
        if r >= 0:
            self._table.removeRow(r)

    def _load(self):
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            meta = self._model.get_object_metadata(self._key)
        except Exception as exc:
            QApplication.restoreOverrideCursor()
            # Saving now would wipe the object's real metadata with the empty
            # form, so block it.
            save_btn = self.buttonBox.button(QDialogButtonBox.StandardButton.Save)
            if save_btn is not None:
                save_btn.setEnabled(False)
            QMessageBox.warning(self, "Metadata", f"Could not load metadata:\n{exc}")
            return
        QApplication.restoreOverrideCursor()
        self._storage_class = meta.get("storage_class") or "STANDARD"
        self._content_type.setText(meta.get("content_type", ""))
        self._cache_control.setText(meta.get("cache_control", ""))
        self._content_disposition.setText(meta.get("content_disposition", ""))
        self._content_encoding.setText(meta.get("content_encoding", ""))
        for k, v in (meta.get("metadata") or {}).items():
            self._insert_row(str(k), str(v))

    def _save(self):
        metadata = {}
        for r in range(self._table.rowCount()):
            k_item = self._table.item(r, 0)
            v_item = self._table.item(r, 1)
            k = (k_item.text() if k_item else "").strip()
            v = (v_item.text() if v_item else "").strip()
            if k:
                metadata[k] = v
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            self._model.set_object_metadata(
                self._key,
                content_type=self._content_type.text().strip(),
                cache_control=self._cache_control.text().strip(),
                content_disposition=self._content_disposition.text().strip(),
                content_encoding=self._content_encoding.text().strip(),
                metadata=metadata,
                storage_class=self._storage_class,
            )
        except Exception as exc:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Metadata", f"Could not save metadata:\n{exc}")
            return
        QApplication.restoreOverrideCursor()
        self.accept()


class SearchDialog(QDialog):
    """Recursively search the current bucket/prefix by key substring."""

    MAX_RESULTS = 1000

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._thread = None
        self._worker = None
        self._cancel = None
        self._results = []

        scope = self._prefix if self._prefix else "(bucket root)"
        self.setWindowTitle(f"Search — {model.bucket}/{self._prefix}")
        self.resize(760, 480)

        self._query = QLineEdit()
        self._query.setPlaceholderText(f"Search under {scope} and below…")
        self._query.returnPressed.connect(self._run_search)
        self._btn_search = QPushButton("Search")
        self._btn_search.clicked.connect(self._run_search)
        top = QHBoxLayout()
        top.addWidget(self._query, 1)
        top.addWidget(self._btn_search)

        # Saved searches: with results now driving the queue, a recurring
        # cleanup is worth one click rather than a re-typed form.
        self._saved = QComboBox()
        self._saved.setMinimumWidth(180)
        self._saved.currentIndexChanged.connect(self._on_saved_chosen)
        self._btn_save_search = QPushButton("Save…")
        self._btn_save_search.clicked.connect(self._save_search)
        self._btn_delete_search = QPushButton("Delete")
        self._btn_delete_search.clicked.connect(self._delete_search)
        saved_row = QHBoxLayout()
        saved_row.addWidget(QLabel("Saved"))
        saved_row.addWidget(self._saved)
        saved_row.addWidget(self._btn_save_search)
        saved_row.addWidget(self._btn_delete_search)
        saved_row.addStretch(1)

        self._regex = QCheckBox("Regex")
        self._case = QCheckBox("Case sensitive")
        self._exts = QLineEdit()
        self._exts.setPlaceholderText("Extensions, e.g. txt, .log")
        self._min_size = QLineEdit()
        self._min_size.setPlaceholderText("min")
        self._min_size.setMaximumWidth(90)
        self._max_size = QLineEdit()
        self._max_size.setPlaceholderText("max")
        self._max_size.setMaximumWidth(90)
        self._size_unit = QComboBox()
        for label, factor in (("B", 1), ("KB", 1024),
                              ("MB", 1024 ** 2), ("GB", 1024 ** 3)):
            self._size_unit.addItem(label, factor)
        self._size_unit.setCurrentIndex(0)

        self._use_after = QCheckBox("Modified after")
        self._after = QDateEdit()
        self._after.setCalendarPopup(True)
        self._after.setDate(QDate.currentDate().addMonths(-1))
        self._after.setEnabled(False)
        self._use_after.toggled.connect(self._after.setEnabled)
        self._use_before = QCheckBox("before")
        self._before = QDateEdit()
        self._before.setCalendarPopup(True)
        self._before.setDate(QDate.currentDate())
        self._before.setEnabled(False)
        self._use_before.toggled.connect(self._before.setEnabled)

        flags_row = QHBoxLayout()
        flags_row.addWidget(self._regex)
        flags_row.addWidget(self._case)
        flags_row.addStretch(1)

        size_row = QHBoxLayout()
        size_row.addWidget(QLabel("Size"))
        size_row.addWidget(self._min_size)
        size_row.addWidget(QLabel("to"))
        size_row.addWidget(self._max_size)
        size_row.addWidget(self._size_unit)
        size_row.addSpacing(12)
        size_row.addWidget(QLabel("Extensions"))
        size_row.addWidget(self._exts, 1)

        date_row = QHBoxLayout()
        date_row.addWidget(self._use_after)
        date_row.addWidget(self._after)
        date_row.addWidget(self._use_before)
        date_row.addWidget(self._before)
        date_row.addStretch(1)

        filters = QGroupBox("Filters")
        filters_lay = QVBoxLayout()
        filters_lay.addLayout(flags_row)
        filters_lay.addLayout(size_row)
        filters_lay.addLayout(date_row)
        filters.setLayout(filters_lay)
        self._filters_box = filters

        self._info = QLabel("")
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["Key", "Size", "Modified"])
        self._table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        # Extended, not single: a search that cannot act on what it found is
        # only half a tool — the queue can already delete, tag, re-type and
        # download any list of keys.
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.doubleClicked.connect(self._goto_selected)
        self._table.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._result_menu)

        self._btn_goto = QPushButton("Go to location")
        self._btn_copy = QPushButton("Copy keys")
        self._btn_actions = QToolButton()
        self._btn_actions.setText("Actions")
        self._btn_actions.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        self._btn_actions.setMenu(QMenu(self._btn_actions))
        self._btn_select_all = QPushButton("Select all")
        close_btn = QPushButton("Close")
        self._btn_goto.clicked.connect(self._goto_selected)
        self._btn_copy.clicked.connect(self._copy_selected)
        self._btn_select_all.clicked.connect(self._table.selectAll)
        close_btn.clicked.connect(self.reject)
        self._btn_goto.setEnabled(False)
        self._btn_copy.setEnabled(False)
        self._btn_actions.setEnabled(False)
        self._btn_select_all.setEnabled(False)
        self._table.itemSelectionChanged.connect(self._update_buttons)
        row = QHBoxLayout()
        row.addWidget(self._btn_goto)
        row.addWidget(self._btn_copy)
        row.addWidget(self._btn_actions)
        row.addWidget(self._btn_select_all)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addLayout(saved_row)
        lay.addWidget(filters)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._reload_saved()
        self._query.setFocus()

    def _update_buttons(self):
        selected = self.selected_keys()
        self._btn_goto.setEnabled(len(selected) == 1)
        self._btn_copy.setEnabled(bool(selected))
        self._btn_actions.setEnabled(bool(selected))
        self._btn_select_all.setEnabled(bool(self._results))
        if selected:
            self._build_actions_menu(self._btn_actions.menu(), selected)

    def selected_keys(self) -> list:
        """Every result row the user has selected, in listing order."""
        rows = sorted({index.row()
                       for index in self._table.selectedIndexes()})
        return [self._results[row][0] for row in rows
                if 0 <= row < len(self._results)]

    def _build_actions_menu(self, menu, keys):
        """
        Fill *menu* with everything the queue can do to these keys.

        Built on demand rather than once, because read-only state and the
        selection size both change what belongs in it.
        """
        menu.clear()
        count = len(keys)
        targets = [(key.rsplit("/", 1)[-1], key, False) for key in keys]
        writable = not self._mw.is_read_only()

        act_download = menu.addAction(f"Download {count} object(s)…")
        act_download.triggered.connect(
            lambda: self._act(self._mw.download_keys, keys))
        act_copy_uri = menu.addAction("Copy s3:// URIs")
        act_copy_uri.triggered.connect(lambda: self._copy_uris(keys))
        if not writable:
            hint = menu.addAction("(read-only profile)")
            hint.setEnabled(False)
            return menu
        menu.addSeparator()
        act_tags = menu.addAction("Add / remove tags…")
        act_tags.triggered.connect(
            lambda: self._act(self._mw.bulk_tags, targets))
        act_class = menu.addAction("Change storage class…")
        act_class.triggered.connect(
            lambda: self._act(self._mw.change_storage_class_ui, targets))
        act_restore = menu.addAction("Restore from Glacier…")
        act_restore.triggered.connect(
            lambda: self._act(self._mw.restore_from_glacier, targets))
        act_type = menu.addAction("Fix Content-Type from extension")
        act_type.triggered.connect(
            lambda: self._act(self._mw.fix_content_type_ui, targets))
        menu.addSeparator()
        act_delete = menu.addAction(f"Delete {count} object(s)…")
        act_delete.triggered.connect(
            lambda: self._act(self._mw.delete_keys, keys))
        return menu

    def _act(self, handler, payload):
        """
        Run a main-window action against the selection.

        The dialog closes first: every one of these queues work that refreshes
        the listing underneath, and a search whose results have just been
        deleted is worse than no search at all.
        """
        self.accept()
        handler(payload)

    def _copy_uris(self, keys):
        bucket = self._model.bucket
        QtWidgets.QApplication.clipboard().setText(
            "\n".join(f"s3://{bucket}/{key}" for key in keys))
        self._mw.statusBar().showMessage(
            f"{len(keys)} URI(s) copied", 2000)

    def _result_menu(self, pos):
        keys = self.selected_keys()
        if not keys:
            return
        menu = QMenu(self)
        if len(keys) == 1:
            act_goto = menu.addAction("Go to location")
            act_goto.triggered.connect(self._goto_selected)
            menu.addSeparator()
        self._build_actions_menu(menu, keys)
        menu.exec(self._table.viewport().mapToGlobal(pos))

    @staticmethod
    def _parse_size(text, factor):
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return int(float(raw) * factor)
        except ValueError:
            return None

    def filter_kwargs(self) -> dict:
        """Collect the filter widgets into search_keys keyword arguments."""
        factor = self._size_unit.currentData() or 1
        exts = [part for part in re.split(r"[,\s]+", self._exts.text() or "")
                if part]
        kwargs = {
            "regex": self._regex.isChecked(),
            "case_sensitive": self._case.isChecked(),
            "min_size": self._parse_size(self._min_size.text(), factor),
            "max_size": self._parse_size(self._max_size.text(), factor),
            "extensions": exts or None,
        }
        if self._use_after.isChecked():
            kwargs["modified_after"] = datetime.combine(
                self._after.date().toPyDate(), dtime.min, tzinfo=timezone.utc)
        if self._use_before.isChecked():
            kwargs["modified_before"] = datetime.combine(
                self._before.date().toPyDate(), dtime.max, tzinfo=timezone.utc)
        return kwargs

    def search_config(self) -> dict:
        """Every filter widget's value, as something JSON can hold."""
        return {
            "query": self._query.text(),
            "regex": self._regex.isChecked(),
            "case": self._case.isChecked(),
            "extensions": self._exts.text(),
            "min_size": self._min_size.text(),
            "max_size": self._max_size.text(),
            "unit": int(self._size_unit.currentData() or 1),
            "use_after": self._use_after.isChecked(),
            "after": self._after.date().toString("yyyy-MM-dd"),
            "use_before": self._use_before.isChecked(),
            "before": self._before.date().toString("yyyy-MM-dd"),
        }

    def apply_search_config(self, config):
        """Fill the form from a saved search."""
        config = dict(config or {})
        self._query.setText(str(config.get("query") or ""))
        self._regex.setChecked(bool(config.get("regex")))
        self._case.setChecked(bool(config.get("case")))
        self._exts.setText(str(config.get("extensions") or ""))
        self._min_size.setText(str(config.get("min_size") or ""))
        self._max_size.setText(str(config.get("max_size") or ""))
        index = self._size_unit.findData(int(config.get("unit") or 1))
        self._size_unit.setCurrentIndex(max(index, 0))
        self._use_after.setChecked(bool(config.get("use_after")))
        self._use_before.setChecked(bool(config.get("use_before")))
        for checked, widget, value in (
            (config.get("use_after"), self._after, config.get("after")),
            (config.get("use_before"), self._before, config.get("before")),
        ):
            if checked and value:
                parsed = QDate.fromString(str(value), "yyyy-MM-dd")
                if parsed.isValid():
                    widget.setDate(parsed)

    def _reload_saved(self, select=""):
        entries = self._mw.load_saved_searches()
        self._saved.blockSignals(True)
        self._saved.clear()
        self._saved.addItem("(none)", None)
        for entry in entries:
            self._saved.addItem(str(entry.get("name") or ""), entry)
        index = self._saved.findText(select) if select else 0
        self._saved.setCurrentIndex(max(index, 0))
        self._saved.blockSignals(False)
        self._btn_delete_search.setEnabled(self._saved.currentIndex() > 0)

    def _on_saved_chosen(self, _index):
        entry = self._saved.currentData()
        self._btn_delete_search.setEnabled(entry is not None)
        if entry is None:
            return
        self.apply_search_config(entry.get("filters") or {})
        self._run_search()

    def _save_search(self):
        name, ok = QInputDialog.getText(
            self, "Save search", "Name:",
            text=self._saved.currentText()
            if self._saved.currentIndex() > 0 else "")
        name = (name or "").strip()
        if not ok or not name:
            return
        entries = [entry for entry in self._mw.load_saved_searches()
                   if str(entry.get("name") or "") != name]
        entries.append({"name": name, "filters": self.search_config()})
        entries.sort(key=lambda entry: str(entry.get("name") or "").lower())
        self._mw.store_saved_searches(entries)
        self._reload_saved(select=name)
        self._info.setText(f"Saved as '{name}'.")

    def _delete_search(self):
        entry = self._saved.currentData()
        if entry is None:
            return
        name = str(entry.get("name") or "")
        entries = [row for row in self._mw.load_saved_searches()
                   if str(row.get("name") or "") != name]
        self._mw.store_saved_searches(entries)
        self._reload_saved()

    def _has_filters(self) -> bool:
        kwargs = self.filter_kwargs()
        return any([
            kwargs["min_size"] is not None, kwargs["max_size"] is not None,
            kwargs["extensions"], "modified_after" in kwargs,
            "modified_before" in kwargs,
        ])

    def _run_search(self):
        if self._thread is not None:
            return
        q = self._query.text().strip()
        # With filters active an empty query is meaningful ("everything of this
        # kind"), so only require text when nothing else narrows the search.
        if not q and not self._has_filters():
            return
        try:
            filters = self.filter_kwargs()
            Model.build_search_matcher(q, **filters)  # validate the regex now
        except ValueError as exc:
            self._info.setText(str(exc))
            return
        self._info.setText("Searching…")
        self._table.setRowCount(0)
        self._results = []
        self._btn_search.setEnabled(False)
        prefix = self._prefix
        limit = self.MAX_RESULTS
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _search(_w):
            return clone.search_keys(prefix, q, cancel_event=cancel,
                                     max_results=limit, **filters)

        self._thread = QThread(self)
        self._worker = FuncWorker(_search)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_results)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        self._stop_search_thread()
        super().closeEvent(event)

    def _stop_search_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_results(self, result, exc):
        self._stop_search_thread()
        self._btn_search.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Search failed: {exc}")
            return
        self._results = result or []
        for key, size, modified in self._results:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(key))
            self._table.setItem(r, 1, QTableWidgetItem(_human_bytes(size)))
            self._table.setItem(
                r, 2, QTableWidgetItem("" if modified is None else str(modified)))
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setStretchLastSection(True)
        capped = len(self._results) >= self.MAX_RESULTS
        self._info.setText(
            f"{len(self._results)} match(es)"
            + (f" — showing first {self.MAX_RESULTS}" if capped else "")
        )
        self._update_buttons()

    def _selected_key(self):
        r = self._table.currentRow()
        if 0 <= r < len(self._results):
            return self._results[r][0]
        return None

    def _copy_selected(self):
        keys = self.selected_keys()
        if not keys:
            return
        QtWidgets.QApplication.clipboard().setText("\n".join(keys))
        self._mw.statusBar().showMessage(
            "Key copied" if len(keys) == 1 else f"{len(keys)} keys copied",
            2000)

    def _goto_selected(self):
        key = self._selected_key()
        if not key:
            return
        self.accept()
        self._mw.goto_key(key)


class PresignedLinkDialog(QDialog):
    """Generate a temporary download (GET) or upload (PUT) link for an object,
    with a configurable expiry."""

    UNITS = (("Minutes", 60), ("Hours", 3600), ("Days", 86400))
    MAX_EXPIRES = 7 * 24 * 3600  # SigV4 presigned-URL maximum (7 days)

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key

        base = key.rstrip("/").split("/")[-1] or key
        self.setWindowTitle(f"Share link — {base}")
        self.resize(640, 220)

        self._type = QComboBox()
        self._type.addItems(["Download (GET)", "Upload (PUT)"])
        self._amount = QSpinBox()
        self._amount.setRange(1, 100000)
        self._amount.setValue(1)
        self._unit = QComboBox()
        self._unit.addItems([u for u, _ in self.UNITS])
        self._unit.setCurrentIndex(1)  # Hours

        exp_row = QHBoxLayout()
        exp_row.addWidget(self._amount)
        exp_row.addWidget(self._unit)
        exp_row.addStretch(1)
        exp_w = QWidget()
        exp_w.setLayout(exp_row)

        form = QFormLayout()
        form.addRow(QLabel("Link type"), self._type)
        form.addRow(QLabel("Expires in"), exp_w)

        self._url = QLineEdit()
        self._url.setReadOnly(True)
        self._note = QLabel("")
        self._note.setWordWrap(True)

        copy_btn = QPushButton("Copy")
        close_btn = QPushButton("Close")
        copy_btn.clicked.connect(self._copy)
        close_btn.clicked.connect(self.reject)
        btns = QHBoxLayout()
        btns.addWidget(copy_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(form)
        lay.addWidget(QLabel("URL:"))
        lay.addWidget(self._url)
        lay.addWidget(self._note)
        lay.addLayout(btns)

        self._type.currentIndexChanged.connect(self._regenerate)
        self._amount.valueChanged.connect(self._regenerate)
        self._unit.currentIndexChanged.connect(self._regenerate)
        self._regenerate()

    def _expires_sec(self):
        return self._amount.value() * self.UNITS[self._unit.currentIndex()][1]

    def _regenerate(self):
        secs = self._expires_sec()
        note = ""
        if secs > self.MAX_EXPIRES:
            secs = self.MAX_EXPIRES
            note = "Clamped to the S3 maximum of 7 days. "
        is_upload = self._type.currentIndex() == 1
        try:
            if is_upload:
                url = self._model.presigned_put_url(self._key, secs)
            else:
                url = self._model.presigned_get_url(self._key, secs)
        except Exception as exc:
            self._url.setText("")
            self._note.setText(f"Could not generate link: {exc}")
            return
        self._url.setText(url)
        if is_upload:
            note += 'Upload with:  curl --upload-file <file> "<URL>"'
        self._note.setText(note)

    def _copy(self):
        if self._url.text():
            QtWidgets.QApplication.clipboard().setText(self._url.text())
            self._note.setText("Link copied to clipboard.")


class _QEntry:
    def __init__(self, entry_id, method, job, need_refresh=True, label="",
                 source_bucket="", dest_model=None, source_model=None):
        self.entry_id = entry_id
        self.method = method
        self.job = job
        self.need_refresh = need_refresh
        self.label = label
        # Set when the job's SOURCE keys live in a different bucket than the
        # one being viewed (cross-bucket paste); the worker's model is bound
        # to it before the transfer starts.
        self.source_bucket = source_bucket or ""
        # Set for a cross-profile copy: a second model, built from another
        # profile's credentials, that the worker writes through.
        self.dest_model = dest_model
        # Set when the job reads from a model other than the window's — a pull
        # from another profile.
        self.source_model = source_model
        self.status = "queued"
        self.thread = None
        self.worker = None
        self.error = None
        # The same failure with the request id and HTTP status attached, kept
        # so a finished job can still be reported accurately hours later.
        self.error_details = ""
        self.error_transient = False
        # How many times this job has been re-queued automatically.
        self.auto_retries = 0


def _scaled_bar_values(done, total, scale=1000):
    """Map raw byte counts onto a small fixed integer range for a QProgressBar.

    QProgressBar stores its range/value as C++ 32-bit ints, so feeding it raw
    byte counts overflows past ~2.1 GB (a single large file or a multi-file
    batch total). Scaling to a fixed range sidesteps that entirely.

    Returns ``(range_max, value)``. A ``range_max`` of 0 means the total size
    is unknown and the caller should render an indeterminate/busy bar.
    """
    if total <= 0:
        return 0, 0
    value = int(done / total * scale)
    return scale, min(scale, max(0, value))


class _QueueRow(QWidget):
    cancel_requested = pyqtSignal(int)
    retry_requested = pyqtSignal(int)
    details_requested = pyqtSignal(int)

    _OP_ICONS = {
        "upload": "⬆", "download": "⬇", "delete": "✕",
        "copy": "⇆", "move": "➜", "delete_buckets": "✕",
        "empty_buckets": "∅", "sync": "⇅", "undelete": "↺",
        "set_tags": "🏷", "zip_download": "🗜",
    }
    _STATUS_COLORS = {
        "queued": "#e4e4e8", "running": "#bbdefb",
        "done": "#c8e8c8", "cancelled": "#ffe0b2", "error": "#ffcdd2",
    }

    def __init__(self, entry, parent=None):
        super().__init__(parent)
        self._entry_id = entry.entry_id

        icon = QLabel(self._OP_ICONS.get(entry.method, "?"))
        icon.setFixedWidth(20)
        icon.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self._desc = QLabel(entry.label)
        self._desc.setMinimumWidth(120)

        self._status = QLabel("queued")
        self._status.setFixedWidth(72)
        self._status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._update_status_style("queued")

        self._bar = QProgressBar()
        self._bar.setRange(0, 0)
        self._bar.setMaximumHeight(14)
        self._bar.setTextVisible(False)
        self._bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        self._cancel_btn = QPushButton("✕")
        self._cancel_btn.setFixedSize(22, 22)
        self._cancel_btn.setToolTip("Cancel")
        self._cancel_btn.clicked.connect(lambda: self.cancel_requested.emit(self._entry_id))

        self._retry_btn = QPushButton("↻")
        self._retry_btn.setFixedSize(22, 22)
        self._retry_btn.setToolTip("Retry")
        self._retry_btn.clicked.connect(lambda: self.retry_requested.emit(self._entry_id))
        self._retry_btn.hide()

        # Shown only on a failed row: what actually came back from the
        # service, which is the thing a bug report needs.
        self._details_btn = QPushButton("ℹ")
        self._details_btn.setFixedSize(22, 22)
        self._details_btn.setToolTip("Why it failed")
        self._details_btn.clicked.connect(
            lambda: self.details_requested.emit(self._entry_id))
        self._details_btn.hide()

        row = QHBoxLayout(self)
        row.setContentsMargins(4, 2, 4, 2)
        row.addWidget(icon)
        row.addWidget(self._desc, 1)
        row.addWidget(self._status)
        row.addWidget(self._bar, 1)
        row.addWidget(self._details_btn)
        row.addWidget(self._retry_btn)
        row.addWidget(self._cancel_btn)

    def _update_status_style(self, status: str):
        bg = self._STATUS_COLORS.get(status, "#e4e4e8")
        self._status.setText(status)
        # Force dark text so the label stays readable on the pastel chip in
        # both light and dark themes.
        self._status.setStyleSheet(
            f"background: {bg}; color: #202020; border-radius: 3px; padding: 1px 4px;"
        )

    def set_status(self, status: str):
        self._update_status_style(status)
        if status in ("done", "cancelled", "error"):
            self._bar.setRange(0, 1)
            self._bar.setValue(1 if status == "done" else 0)
            self._cancel_btn.setEnabled(False)
        # Only an unfinished job is worth re-running.
        self._retry_btn.setVisible(status in ("cancelled", "error"))
        self._details_btn.setVisible(status == "error")

    def set_byte_progress(self, done: int, total: int):
        range_max, value = _scaled_bar_values(done, total)
        self._bar.setRange(0, range_max)
        self._bar.setValue(value)


class TransferQueuePanel(QWidget):
    cancel_requested = pyqtSignal(int)
    retry_requested = pyqtSignal(int)
    details_requested = pyqtSignal(int)
    retry_all_requested = pyqtSignal()
    history_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows: dict = {}

        hdr = QLabel("  Transfer Queue")
        # Plain bold label (no hardcoded background) so it adapts to the theme;
        # QLabel backgrounds are transparent by default.
        hdr.setStyleSheet("padding: 3px 6px; font-weight: bold;")

        clear_btn = QPushButton("Clear done")
        clear_btn.setFlat(True)
        clear_btn.clicked.connect(self._clear_done)

        retry_btn = QPushButton("Retry failed")
        retry_btn.setFlat(True)
        retry_btn.clicked.connect(self.retry_all_requested)

        history_btn = QPushButton("History")
        history_btn.setFlat(True)
        history_btn.clicked.connect(self.history_requested)

        hdr_row = QHBoxLayout()
        hdr_row.setContentsMargins(0, 0, 0, 0)
        hdr_row.setSpacing(0)
        hdr_row.addWidget(hdr, 1)
        hdr_row.addWidget(retry_btn)
        hdr_row.addWidget(history_btn)
        hdr_row.addWidget(clear_btn)

        self._content = QWidget()
        self._content_lay = QVBoxLayout(self._content)
        self._content_lay.setContentsMargins(0, 0, 0, 0)
        self._content_lay.setSpacing(1)
        self._content_lay.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidget(self._content)
        scroll.setWidgetResizable(True)
        scroll.setMaximumHeight(140)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        lay.addLayout(hdr_row)
        lay.addWidget(scroll)

        self.hide()

    def add_entry(self, entry):
        row = _QueueRow(entry)
        row.cancel_requested.connect(self.cancel_requested)
        row.retry_requested.connect(self.retry_requested)
        row.details_requested.connect(self.details_requested)
        self._rows[entry.entry_id] = row
        count = self._content_lay.count()
        self._content_lay.insertWidget(count - 1, row)
        self.show()

    def update_status(self, entry):
        row = self._rows.get(entry.entry_id)
        if row is not None:
            row.set_status(entry.status)

    def update_byte_progress(self, entry_id: int, done: int, total: int):
        row = self._rows.get(entry_id)
        if row is not None:
            row.set_byte_progress(done, total)

    def _clear_done(self):
        to_remove = [
            eid for eid, row in list(self._rows.items())
            if row._status.text() in ("done", "cancelled", "error")
        ]
        for eid in to_remove:
            row = self._rows.pop(eid)
            self._content_lay.removeWidget(row)
            row.deleteLater()
        if not self._rows:
            self.hide()


class Breadcrumb(QScrollArea):
    """A clickable path bar. Each segment navigates to that prefix; the leading
    'Buckets' segment returns to the bucket list."""

    home = pyqtSignal()
    go = pyqtSignal(str)   # target prefix ('' == bucket root)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setFixedHeight(28)
        self._inner = QWidget()
        self._lay = QHBoxLayout(self._inner)
        self._lay.setContentsMargins(4, 0, 4, 0)
        self._lay.setSpacing(1)
        self._lay.addStretch(1)
        self.setWidget(self._inner)
        self._last_key = None

    def _clear(self):
        # Remove everything except the trailing stretch.
        while self._lay.count() > 1:
            item = self._lay.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()

    def _add_segment(self, text, target, *, is_last):
        btn = QToolButton()
        btn.setText(text)
        btn.setAutoRaise(True)
        if is_last:
            btn.setEnabled(False)
        else:
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            if target is None:
                btn.clicked.connect(lambda: self.home.emit())
            else:
                btn.clicked.connect(lambda _c=False, t=target: self.go.emit(t))
        self._lay.insertWidget(self._lay.count() - 1, btn)

    def _add_sep(self):
        sep = QLabel("›")
        sep.setEnabled(False)
        self._lay.insertWidget(self._lay.count() - 1, sep)

    def set_location(self, bucket, current_folder, in_bucket_list):
        # Skip a rebuild when nothing changed (navigation + resize both call us).
        state_key = (bool(in_bucket_list), bucket or "", current_folder or "")
        if state_key == self._last_key:
            return
        self._last_key = state_key

        self._clear()
        self._add_segment("Buckets", None, is_last=in_bucket_list)
        if not in_bucket_list and bucket:
            folder = current_folder or ""
            self._add_sep()
            self._add_segment(bucket, "", is_last=(folder == ""))
            parts = [p for p in folder.split("/") if p]
            acc = ""
            for i, p in enumerate(parts):
                acc += p + "/"
                self._add_sep()
                self._add_segment(p, acc, is_last=(i == len(parts) - 1))
        # Keep the newest (deepest) segment in view.
        QTimer.singleShot(0, lambda: self.horizontalScrollBar().setValue(
            self.horizontalScrollBar().maximum()))


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        self.title = "S3 Duck 🦆 %s" % __VERSION__

        # Profiles created before session-token / read-only support pass fewer
        # fields; pad so old callers keep working.
        settings = tuple(settings) + ("",) * (20 - len(tuple(settings)))
        (
            current_dir,
            settings,
            profile_name,
            url,
            region,
            bucket,
            access_key,
            secret_key,
            no_ssl_check,
            use_path,
            session_token,
            read_only,
            accent,
            session_expires,
            aws_profile,
            credential_process,
            public_base_url,
            requester_pays,
            proxy_url,
            ca_bundle,
        ) = settings
        # Credential lifetime is profile state, not model state: the model
        # only ever sees the keys it was handed.
        self.session_expires = str(session_expires or "")
        self.aws_profile = str(aws_profile or "")
        self.credential_process = str(credential_process or "")
        self.settings = settings
        self.current_dir = current_dir
        # Needed before the settings loaders below, which key off the profile.
        self.profile_name = profile_name
        # Staged object payloads live here; cleared in closeEvent. The sweep
        # reclaims what a previous crash left behind, and only ever touches
        # roots whose owning process is gone.
        self.temp_workspace = TempWorkspace()
        self.temp_workspace.sweep()
        settings.beginGroup("common")
        log_to_file = str(
            settings.value("log_to_file", "true")).lower() in ("true", "1")
        settings.endGroup()
        self.log_file = LogFile(
            LogFile.default_path() if log_to_file else "")
        settings.beginGroup("common")
        try:
            transfer_concurrency = int(settings.value(
                "transfer_concurrency", DataModel.DEFAULT_TRANSFER_CONCURRENCY))
        except (TypeError, ValueError):
            transfer_concurrency = DataModel.DEFAULT_TRANSFER_CONCURRENCY
        try:
            parallel_files = int(settings.value(
                "parallel_files", DataModel.DEFAULT_PARALLEL_FILES))
        except (TypeError, ValueError):
            parallel_files = DataModel.DEFAULT_PARALLEL_FILES
        settings.endGroup()
        self.data_model = DataModel(
            url, region, access_key, secret_key, bucket, no_ssl_check, use_path,
            transfer_concurrency=transfer_concurrency,
            session_token=session_token,
            parallel_files=parallel_files,
            read_only=bool(read_only),
            requester_pays=bool(requester_pays),
            public_base_url=public_base_url,
            proxy_url=proxy_url,
            ca_bundle=ca_bundle,
        )
        self._load_binding_cache()
        self._load_upload_options()
        self._bookmarks = []
        self._load_bookmarks()
        self.logview = QPlainTextEdit(self)
        self.logview.setMaximumBlockCount(3000)  # prevents UI freeze on huge logs

        def _apply_emoji_safe_font(widget):
            pt = widget.font().pointSize()

            def available(cands):
                return [f for f in cands if f in QFontDatabase.families()]

            if sys.platform.startswith("win"):
                base = available(["Consolas", "Segoe UI", "Arial", "Tahoma"])
                emoji = available(["Segoe UI Emoji"])
                stack = base[:1] + emoji + base[1:]
            elif sys.platform == "darwin":
                base = available(["Menlo", "SF Mono", "Monaco"])
                emoji = available(["Apple Color Emoji"])
                stack = base[:1] + emoji + base[1:]
            else:
                base = available([
                    "DejaVu Sans Mono",
                    "Ubuntu Mono",
                    "Liberation Mono",
                    "Monospace",
                    "DejaVu Sans",
                    "Sans Serif",
                ])
                emoji = available(
                    ["Noto Color Emoji", "Emoji One Color", "Segoe UI Emoji"])
                stack = base[:1] + emoji + base[1:]

            if not stack:
                stack = ["Sans-Serif"]

            # Apply the font via QFont, not a stylesheet: any stylesheet makes
            # the widget stop following the palette background, which breaks
            # theming (the log view stayed white in dark mode).
            f = QFont(widget.font())
            f.setFamilies(stack)
            if pt > 0:
                f.setPointSize(pt)
            widget.setFont(f)

        _apply_emoji_safe_font(self.logview)

        self.listview = Tree(self)
        self._menu_click_guard = _OneShotClickGuard(self.listview.viewport())

        # Quick-find search bar (hidden until Ctrl+F). Filters the current
        # bucket/folder listing by name via the proxy model.
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText(
            "Filter by name or glob (*.log)…  "
            "(just type, or Ctrl+F; Esc to close)")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._on_search_text_changed)
        self.search_edit.installEventFilter(self)
        # A filter that matches nothing looks exactly like an empty folder
        # without this.
        self.filter_status = QLabel("")
        self.filter_status.hide()

        self._filter_row = QWidget()
        _filter_lay = QHBoxLayout(self._filter_row)
        _filter_lay.setContentsMargins(0, 0, 0, 0)
        _filter_lay.addWidget(self.search_edit, 1)
        _filter_lay.addWidget(self.filter_status)
        self._filter_row.hide()

        # A capped listing must say so: silently showing the first N of a
        # much larger prefix reads as "this is everything".
        self.listing_notice = QLabel("")
        self.listing_notice.setWordWrap(True)
        self.listing_notice.setStyleSheet("color: #b26a00;")
        self.listing_notice.hide()

        # Location tabs. Each tab is a remembered (bucket, prefix) that the
        # one listing view navigates to — not a second view: the queue, the
        # log and the model are shared, and duplicating those to show two
        # folders would buy nothing.
        self.tabbar = QTabBar()
        self.tabbar.setExpanding(False)
        self.tabbar.setMovable(True)
        self.tabbar.setTabsClosable(True)
        self.tabbar.setDrawBase(False)
        self.tabbar.setUsesScrollButtons(True)
        self.tabbar.currentChanged.connect(self._on_tab_changed)
        self.tabbar.tabCloseRequested.connect(self.close_tab)
        self.tabbar.tabMoved.connect(self._on_tab_moved)
        self.tabbar.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self.tabbar.customContextMenuRequested.connect(self._tab_menu)
        self.tabbar.hide()
        self._tabs = []
        self._tab_switching = False
        self._tab_restore_pending = False
        # Set while a Back/Forward step is in flight, so the resulting
        # navigation is not recorded as a new move.
        self._history_navigating = False

        self._list_container = QWidget()
        _list_lay = QVBoxLayout(self._list_container)
        _list_lay.setContentsMargins(0, 0, 0, 0)
        _list_lay.setSpacing(2)
        _list_lay.addWidget(self.tabbar)
        _list_lay.addWidget(self._filter_row)
        _list_lay.addWidget(self.listing_notice)
        _list_lay.addWidget(self.listview)

        # The second pane lives beside the listing; both sit above the log.
        self.second_pane = SecondPane(self, self, self.data_model)
        self.second_pane.hide()
        self.panes = QSplitter(Qt.Orientation.Horizontal)
        self.panes.addWidget(self._list_container)
        self.panes.addWidget(self.second_pane)
        self.panes.setStretchFactor(0, 1)
        self.panes.setStretchFactor(1, 1)

        self.clip = QApplication.clipboard()
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.addWidget(self.panes)
        self.splitter.addWidget(self.logview)
        # ~75% top / ~25% bottom
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 1)

        self.logview.setReadOnly(True)
        self.logview.appendPlainText(
            "Welcome to S3 Duck 🦆 %s (on %s)"
            % (__VERSION__, OS_FAMILY_MAP.get(DataModel.get_os_family(), "❓"))
        )

        self._queue_panel = TransferQueuePanel(self)
        self._queue_panel.cancel_requested.connect(self._on_queue_cancel_requested)
        self._queue_panel.retry_requested.connect(self._on_queue_retry_requested)
        self._queue_panel.details_requested.connect(self.show_failure_details)
        self._queue_panel.retry_all_requested.connect(
            self.retry_failed_transfers)
        self._queue_panel.history_requested.connect(self.show_transfer_history)
        self.accent_bar = QWidget()
        self.accent_bar.setFixedHeight(4)
        # autoFillBackground + a palette role rather than a stylesheet: any
        # stylesheet makes a widget stop following the palette, which is how
        # theming breaks elsewhere in this app.
        self.accent_bar.setAutoFillBackground(True)
        vlay = QVBoxLayout()
        vlay.setContentsMargins(0, 0, 0, 0)
        vlay.setSpacing(0)
        vlay.addWidget(self.accent_bar, 0)
        vlay.addWidget(self.splitter, 1)
        vlay.addWidget(self._queue_panel, 0)
        wid = QWidget()
        wid.setLayout(vlay)
        self.setCentralWidget(wid)
        self.set_accent(accent)
        self.setGeometry(0, 26, 900, 500)

        self._nav_seq = 0
        self._nav_thread = None
        self._nav_worker = None
        self._nav_orphan_threads = []          # superseded, still running
        self._nav_pending_restore_name = None  # name to reselect after navigation
        self._nav_select_up_entry = False      # select [..] if present after navigation
        self._loading_dialog = None
        self._bucket_enter_thread = None
        self._bucket_enter_worker = None

        self._last_selected_in_prefix = {}  # key: (bucket, prefix) -> name
        self.update_window_title()
        self.createActions()

        self.tBar = self.addToolBar("Tools")
        self.tBar.setContextMenuPolicy(Qt.ContextMenuPolicy.PreventContextMenu)
        self.tBar.setMovable(True)
        self.tBar.setIconSize(QSize(16, 16))
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnHome)
        self.tBar.addAction(self.btnBack)
        self.tBar.addAction(self.btnForward)
        self.tBar.addAction(self.btnUp)
        self.tBar.addAction(self.btnRefresh)
        self.tBar.addAction(self.btnBucketUsage)
        self.tBar.addAction(self.actCopyS3Path)
        self.tBar.addAction(self.actGoToLocation)
        self._build_bookmark_button()
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnDownload)
        self.tBar.addAction(self.btnUpload)
        self.tBar.addAction(self.btnUploadFolder)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnCreateFolder)
        self.tBar.addAction(self.btnRemove)
        self.tBar.addAction(self.btnUndoDelete)
        self.tBar.addAction(self.btnCancel)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnSwitchProfile)
        self.tBar.addAction(self.btnTransferSettings)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnQueuePanel)
        self._build_tools_button()
        self.tBar.addAction(self.btnAbout)
        self._build_theme_button()
        self.tBar.setIconSize(QSize(26, 26))

        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(list(LIST_COLUMNS))

        self.proxy = UpTopProxyModel(UP_ENTRY_LABEL, self)
        self.proxy.setSourceModel(self.model)
        self.listview.setModel(self.proxy)

        # Enable Σ + refresh open usage dialog when selection changes.
        # Single handler — connecting twice was causing duplicate work and
        # could stack if the disconnect() failed silently after a model swap.
        try:
            sm = self.listview.selectionModel()
            if sm is not None:
                try:
                    sm.currentChanged.disconnect(self._on_current_changed_for_usage)
                except Exception:
                    pass
                sm.currentChanged.connect(self._on_current_changed_for_usage)
                sm.selectionChanged.connect(self._update_selection_status)
        except Exception:
            pass

        self.pb = QProgressBar()
        self.pb.setMinimum(0)
        self.pb.setMaximum(100)
        self.pb.hide()
        self.status_text = QLabel("")
        # Temporary credentials used to lapse silently mid-session; the only
        # symptom was an authentication error on the next operation.
        self.credential_status = QLabel("")
        self.credential_status.hide()
        self.statusBar().addPermanentWidget(self.credential_status, 0)
        self.statusBar().addPermanentWidget(self.status_text, 2)
        self.statusBar().addPermanentWidget(self.pb, 1)
        # Set once the window starts tearing down, so a timer that has
        # already fired cannot start new work on the way out.
        self._closing = False
        self._pending_retries = []
        self._retry_timer = QTimer(self)
        self._retry_timer.setSingleShot(True)
        self._retry_timer.timeout.connect(self._run_pending_retries)

        # Interval folder mirror. Never started automatically; see
        # _load_watch_config.
        self._watch = None
        self._watch_config = {}
        self._watch_timer = None
        self._watch_thread = None
        self._watch_worker = None
        self.watch_status = QLabel("")
        self.watch_status.hide()
        self.statusBar().addPermanentWidget(self.watch_status, 0)

        self._credential_timer = QTimer(self)
        self._credential_timer.setInterval(CREDENTIAL_CHECK_INTERVAL_MS)
        self._credential_timer.timeout.connect(self._update_credential_status)
        self._credential_timer.start()
        self._update_credential_status()

        self._smooth_total = 1
        self._smooth_done = 0
        self._rate_samples = []
        self._smooth_rate_bps = 0.0
        self._last_tick_time = 0.0
        self._last_tick_bytes = 0

        self._tick_timer = QTimer(self)
        self._tick_timer.setInterval(TICK_INTERVAL_MS)
        self._tick_timer.timeout.connect(self._on_tick)
        self._status_prefix = "Transferring…"

        self.breadcrumb = Breadcrumb()
        self.breadcrumb.home.connect(self.goHome)
        self.breadcrumb.go.connect(self._breadcrumb_go)
        self.statusBar().addPermanentWidget(self.breadcrumb, 3)

        self._bucket_usage_token = 0
        self._bucket_usage_thread = None
        self._bucket_usage_worker = None
        self._bucket_usage_dialog = None

        self._transfer_queue: list = []
        self._queue_next_id = 0
        self._active_entry = None
        self._queue_entries: dict = {}
        self._batch_stats = {"done": 0, "cancelled": 0, "error": 0}
        self._tray_icon = None
        self._undo_delete = None
        self._clipboard = None

        self.thread = None
        self.worker = None
        self.setWindowIcon(QIcon(os.path.join(self.current_dir, "resources", "ducky.ico")))
        self.listview.installEventFilter(self)

        self._palette_shortcut = QShortcut(QKeySequence("Ctrl+K"), self)
        self._palette_shortcut.activated.connect(self.show_command_palette)

        self._quick_open_shortcut = QShortcut(QKeySequence("Ctrl+P"), self)
        self._quick_open_shortcut.activated.connect(self.show_quick_open)

        self._search_shortcut = QShortcut(QKeySequence.StandardKey.Find, self)
        self._search_shortcut.activated.connect(self._toggle_search)

        self._deep_search_shortcut = QShortcut(QKeySequence("Ctrl+Shift+F"), self)
        self._deep_search_shortcut.activated.connect(self.open_search)

        self._duplicates_shortcut = QShortcut(QKeySequence("Ctrl+Shift+D"), self)
        self._duplicates_shortcut.activated.connect(self.find_duplicates)

        self._bookmark_shortcut = QShortcut(QKeySequence("Ctrl+B"), self)
        self._bookmark_shortcut.activated.connect(self.add_bookmark)

        self._copy_shortcut = QShortcut(QKeySequence.StandardKey.Copy, self)
        self._copy_shortcut.activated.connect(lambda: self.copy_to_clipboard())
        self._cut_shortcut = QShortcut(QKeySequence.StandardKey.Cut, self)
        self._cut_shortcut.activated.connect(
            lambda: self.copy_to_clipboard(cut=True))
        self._paste_shortcut = QShortcut(QKeySequence.StandardKey.Paste, self)
        self._paste_shortcut.activated.connect(self.paste_from_clipboard)

        self._shortcuts_shortcut = QShortcut(QKeySequence("Ctrl+/"), self)
        self._shortcuts_shortcut.activated.connect(self.show_shortcuts)

        self._sync_shortcut = QShortcut(QKeySequence("Ctrl+E"), self)
        self._sync_shortcut.activated.connect(self.open_sync)

        self._bulk_rename_shortcut = QShortcut(QKeySequence("Shift+F2"), self)
        self._bulk_rename_shortcut.activated.connect(self.bulk_rename)

        self._dual_pane_shortcut = QShortcut(QKeySequence("F3"), self)
        self._dual_pane_shortcut.activated.connect(self.toggle_dual_pane)
        # F5 is already Refresh, and two live bindings for one key make Qt
        # treat both as ambiguous — neither fires. The commander keys are
        # therefore only armed while the second pane is on screen, and
        # Refresh keeps Ctrl+R throughout.
        self._pane_copy_shortcut = QShortcut(QKeySequence("F5"), self)
        self._pane_copy_shortcut.activated.connect(
            lambda: self.pane_transfer(move=False))
        self._pane_copy_shortcut.setEnabled(False)
        self._pane_move_shortcut = QShortcut(QKeySequence("F6"), self)
        self._pane_move_shortcut.activated.connect(
            lambda: self.pane_transfer(move=True))
        self._pane_move_shortcut.setEnabled(False)

        self._new_tab_shortcut = QShortcut(QKeySequence("Ctrl+T"), self)
        self._new_tab_shortcut.activated.connect(self.new_tab)
        self._close_tab_shortcut = QShortcut(QKeySequence("Ctrl+W"), self)
        self._close_tab_shortcut.activated.connect(lambda: self.close_tab())
        self._next_tab_shortcut = QShortcut(QKeySequence("Ctrl+Tab"), self)
        self._next_tab_shortcut.activated.connect(lambda: self.next_tab(1))
        self._prev_tab_shortcut = QShortcut(
            QKeySequence("Ctrl+Shift+Tab"), self)
        self._prev_tab_shortcut.activated.connect(lambda: self.next_tab(-1))

        self.menu = QMenu()
        self.menu.setAttribute(Qt.WidgetAttribute.WA_NoMouseReplay, True)

        self._last_selected_bucket = None

        self.restoreSettings()
        self.select_first()
        self._load_watch_config()
        self._load_tabs()

        self.navigate(show_loading=True)
        # After the first listing, so a failed reopen leaves a usable window
        # showing the bucket list rather than nothing at all.
        self.restore_last_location()

        self.listview.header().setSortIndicatorShown(True)
        self.listview.setSortingEnabled(True)
        header = self.listview.header()
        for index, width in enumerate(LIST_COLUMN_DEFAULT_WIDTHS):
            header.resizeSection(index, width)
        for index in LIST_OPTIONAL_COLUMNS:
            self.listview.setColumnHidden(index, True)
        header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        header.customContextMenuRequested.connect(self._column_context_menu)

        self.listview.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.listview.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.listview.setDragDropMode(QAbstractItemView.DragDropMode.DragDrop)
        self.listview.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.listview.setIndentation(10)

        self.listview.doubleClicked.connect(self.list_doubleClicked)

        # After the default column widths above, so saved widths win.
        self._restore_view_state()

        self.logview.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self.logview.customContextMenuRequested.connect(self._log_context_menu)

    def _show_loading(self, title: str = "Loading...", text: str = "Please wait..."):
        try:
            if self._loading_dialog is None:
                dlg = QProgressDialog(text, None, 0, 0, self)
                dlg.setWindowTitle(title)
                dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
                dlg.setCancelButton(None)
                dlg.setMinimumDuration(0)
                dlg.setAutoClose(False)
                dlg.setAutoReset(False)
                self._loading_dialog = dlg
            else:
                self._loading_dialog.setLabelText(text)
                self._loading_dialog.setWindowTitle(title)
            self._loading_dialog.show()
            self._loading_dialog.raise_()
        except Exception:
            self._loading_dialog = None

    def _hide_loading(self):
        try:
            if self._loading_dialog is not None:
                self._loading_dialog.hide()
        except Exception:
            pass

    def _select_by_name(self, name: str) -> bool:
        ix = self.ix_by_name(name)
        if ix:
            self._normalize_selection_to_index(ix)
            return True
        return False

    def select_up_entry(self) -> bool:
        return self._select_by_name(UP_ENTRY_LABEL)

    def _remember_current_selection(self):
        try:
            if self.in_bucket_list_mode():
                _, name, t = self.get_row_primary_item(self.listview.currentIndex())
                if name and t == FSObjectType.BUCKET:
                    self._last_selected_bucket = name
                return
            _, name, _t = self.get_row_primary_item(self.listview.currentIndex())
            if name:
                key = (self.data_model.bucket or "", self.data_model.current_folder or "")
                self._last_selected_in_prefix[key] = name
        except Exception:
            pass



    def log(self, message: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {message}"
        self.logview.appendPlainText(line)
        # The view is capped and dies with the window; the file is what is
        # left to look at afterwards.
        self.log_file.write(f"{line}  [{self.profile_name}]")

    def _begin_model_reset_ui(self):
        self.listview.setUpdatesEnabled(False)

        try:
            self.listview.setSortingEnabled(False)
        except Exception:
            pass

        sm = self.listview.selectionModel()
        if sm is not None:
            sm.blockSignals(True)
            sm.clearSelection()
            sm.clearCurrentIndex()

    def _clear_selection(self):
        sm = self.listview.selectionModel()
        if sm is None:
            return
        sm.blockSignals(True)
        try:
            sm.clearSelection()
            sm.clearCurrentIndex()
        finally:
            sm.blockSignals(False)

    def _show_bucket_usage_dialog(self, bucket_name: str, prefix: str = ""):
        if self._bucket_usage_dialog is None:
            self._bucket_usage_dialog = BucketUsageDialog(bucket_name, prefix, self)
        self._bucket_usage_dialog.set_calculating(bucket_name, prefix)
        self._bucket_usage_dialog.show()
        self._bucket_usage_dialog.raise_()
        self._bucket_usage_dialog.activateWindow()

    def _normalize_selection_to_index(self, proxy_index: QModelIndex):
        if not proxy_index or not proxy_index.isValid():
            return
        sm = self.listview.selectionModel()
        if sm is None:
            self.listview.setCurrentIndex(proxy_index)
            try:
                self.listview.scrollTo(proxy_index)
            except Exception:
                pass
            try:
                self.listview.setFocus(Qt.FocusReason.OtherFocusReason)
            except Exception:
                pass
            return
        sm.blockSignals(True)
        sm.clearSelection()
        sm.setCurrentIndex(
            proxy_index,
            QItemSelectionModel.SelectionFlag.ClearAndSelect | QItemSelectionModel.SelectionFlag.Rows,
        )
        sm.blockSignals(False)
        try:
            self.listview.scrollTo(proxy_index)
        except Exception:
            pass
        try:
            self.listview.setFocus(Qt.FocusReason.OtherFocusReason)
        except Exception:
            pass

    def _end_model_reset_ui(self):
        sm = self.listview.selectionModel()
        if sm is not None:
            sm.blockSignals(False)
        try:
            self.listview.setSortingEnabled(True)
        except Exception:
            pass
        self.listview.setUpdatesEnabled(True)

    def transfers_active(self) -> bool:
        if self.thread is None:
            return False

        if sip is not None:
            try:
                if sip.isdeleted(self.thread):
                    self.thread = None
                    self.worker = None
                    return False
            except Exception:
                pass
        try:
            return self.thread.isRunning()
        except RuntimeError:
            self.thread = None
            self.worker = None
            return False

    def _binding_cache_settings_key(self) -> str:
        return f"bindings/{self.profile_name or 'default'}"

    def _load_binding_cache(self):
        """Restore proven per-bucket endpoint/region bindings for this profile."""
        self.settings.beginGroup("common")
        raw = self.settings.value(self._binding_cache_settings_key(), "") or ""
        self.settings.endGroup()
        cache = {}
        for line in str(raw).splitlines():
            parts = line.split("\t")
            if len(parts) == 4:
                bucket, endpoint, region, use_path = parts
                cache[bucket] = (endpoint, region, use_path == "1")
        self.data_model.binding_cache.clear()
        self.data_model.binding_cache.update(cache)

    def _save_binding_cache(self):
        lines = []
        for bucket, value in sorted(self.data_model.binding_cache.items()):
            endpoint, region, use_path = value
            lines.append(
                "\t".join([bucket, endpoint or "", region or "",
                           "1" if use_path else "0"])
            )
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._binding_cache_settings_key(), "\n".join(lines))
        self.settings.endGroup()

    def _bookmarks_settings_key(self) -> str:
        return f"bookmarks/{self.profile_name or 'default'}"

    def _load_bookmarks(self):
        self.settings.beginGroup("common")
        raw = self.settings.value(self._bookmarks_settings_key(), "") or ""
        self.settings.endGroup()
        self._bookmarks = parse_bookmarks(raw)

    def _save_bookmarks(self):
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._bookmarks_settings_key(), serialize_bookmarks(self._bookmarks))
        self.settings.endGroup()

    def add_bookmark(self):
        """Save the current bucket/prefix for one-click return."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to bookmark it", 2000)
            return
        bucket = self.data_model.bucket
        prefix = self.data_model.current_folder or ""
        suggested = bookmark_label(bucket, prefix)
        name, ok = QInputDialog.getText(
            self, "Add bookmark", "Name:", text=suggested)
        if not ok:
            return
        self._bookmarks, added = add_bookmark_to(
            self._bookmarks, (name or "").strip(), bucket, prefix)
        if not added:
            self.statusBar().showMessage("This location is already bookmarked", 3000)
            return
        self._save_bookmarks()
        self._rebuild_bookmark_menu()
        self.log(f"bookmarked s3://{bucket}/{prefix}")
        self.statusBar().showMessage("Bookmark added", 3000)

    def manage_bookmarks(self):
        dlg = BookmarksDialog(self, self._bookmarks)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._bookmarks = dlg.bookmarks()
        self._save_bookmarks()
        self._rebuild_bookmark_menu()

    def go_to_bookmark(self, entry):
        """Jump to a saved location, crossing buckets when needed."""
        bucket = entry.get("bucket") or ""
        prefix = entry.get("prefix") or ""
        if not bucket:
            return
        if bucket == self.data_model.bucket:
            self.change_current_folder(prefix)
            self.navigate(select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix)

    def _rebuild_bookmark_menu(self):
        menu = self.bookmarkButton.menu()
        menu.clear()
        add_act = menu.addAction("Bookmark this location (Ctrl+B)")
        add_act.triggered.connect(lambda: self.add_bookmark())
        manage_act = menu.addAction("Manage bookmarks…")
        manage_act.triggered.connect(lambda: self.manage_bookmarks())
        manage_act.setEnabled(bool(self._bookmarks))
        if self._bookmarks:
            menu.addSeparator()
            for entry in self._bookmarks:
                act = menu.addAction(entry.get("name") or bookmark_label(
                    entry.get("bucket"), entry.get("prefix")))
                act.setToolTip(
                    f"s3://{entry.get('bucket', '')}/{entry.get('prefix', '')}")
                act.triggered.connect(
                    lambda _checked=False, e=dict(entry): self.go_to_bookmark(e))

    def find_duplicates(self):
        """Tools → Find duplicates: group objects by size + ETag."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to scan", 2000)
            return
        DuplicateFinderDialog(
            self, self, self.data_model, self.data_model.current_folder or ""
        ).exec()

    def delete_duplicate_keys(self, keys):
        """Queue the deletion chosen in the duplicate finder."""
        keys = [k for k in (keys or []) if k]
        if not keys:
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        self.log(f"deleting {len(keys)} duplicate object(s)")
        self.assign_thread_operation("delete", keys)
        self.statusBar().showMessage(
            f"Deleting {len(keys)} duplicate(s)…", 3000)

    def _build_tools_button(self):
        self.toolsButton = QToolButton()
        self.toolsButton.setIcon(
            themed_icon("applications-utilities", os.path.join(self.current_dir, "icons", "settings_24px.svg"))
        )
        self.toolsButton.setIconSize(QSize(26, 26))
        self.toolsButton.setToolButtonStyle(
            Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.toolsButton.setToolTip("Tools")
        self.toolsButton.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        menu = QMenu(self.toolsButton)
        self.actFindDuplicates = menu.addAction("Find duplicates… (Ctrl+Shift+D)")
        self.actFindDuplicates.triggered.connect(lambda: self.find_duplicates())
        menu.addSeparator()
        act_usage = menu.addAction("Bucket usage…")
        act_usage.triggered.connect(lambda: self.request_bucket_usage())
        self.actSizeExplorer = menu.addAction("Size explorer…")
        self.actSizeExplorer.triggered.connect(lambda: self.open_size_explorer())
        self.actExportManifest = menu.addAction("Export manifest…")
        self.actExportManifest.triggered.connect(lambda: self.export_manifest())
        self.actVerifyManifest = menu.addAction("Verify against manifest…")
        self.actVerifyManifest.triggered.connect(lambda: self.verify_manifest())
        act_incomplete = menu.addAction("Incomplete uploads…")
        act_incomplete.triggered.connect(lambda: self.show_incomplete_uploads())
        act_sync = menu.addAction("Sync with local folder…")
        act_sync.triggered.connect(lambda: self.open_sync())
        self.actSyncProfile = menu.addAction("Sync to another profile…")
        self.actSyncProfile.triggered.connect(lambda: self.sync_to_profile())
        self.actDualPane = menu.addAction("Dual pane (F3)")
        self.actDualPane.triggered.connect(lambda: self.toggle_dual_pane())
        self.actComparePanes = menu.addAction("Compare panes…")
        self.actComparePanes.triggered.connect(lambda: self.compare_panes())
        self.actWatchFolder = menu.addAction("Watch folder…")
        self.actWatchFolder.triggered.connect(lambda: self.open_watch())
        self.actBucketSettings = menu.addAction("Bucket settings…")
        self.actBucketSettings.triggered.connect(
            lambda: self.bucket_settings())
        menu.addSeparator()
        self.actRefreshCredentials = menu.addAction("Refresh credentials")
        self.actRefreshCredentials.triggered.connect(
            lambda: self.refresh_credentials())
        self.actRefreshCredentials.setEnabled(self.can_refresh_credentials())
        menu.addSeparator()
        act_settings = menu.addAction("Transfer settings…")
        act_settings.triggered.connect(lambda: self.transfer_settings())
        # The menu is a descendant of the window, so its actions are reachable
        # through findChildren and reach the command palette like the rest.
        self.actDiagnostics = menu.addAction("Diagnostics…")
        self.actDiagnostics.triggered.connect(lambda: self.show_diagnostics())
        self.actReportProblem = menu.addAction("Report a problem…")
        self.actReportProblem.triggered.connect(lambda: self.report_problem())
        self.toolsButton.setMenu(menu)
        self.tBar.addWidget(self.toolsButton)

    def _build_bookmark_button(self):
        self.bookmarkButton = QToolButton()
        self.bookmarkButton.setIcon(
            themed_icon("bookmarks", os.path.join(self.current_dir, "icons", "ok_24px.svg"))
        )
        self.bookmarkButton.setIconSize(QSize(26, 26))
        self.bookmarkButton.setToolButtonStyle(
            Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.bookmarkButton.setToolTip("Bookmarks")
        self.bookmarkButton.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        self.bookmarkButton.setMenu(QMenu(self.bookmarkButton))
        self.tBar.addWidget(self.bookmarkButton)
        self._rebuild_bookmark_menu()

    def _load_upload_options(self):
        self.settings.beginGroup("common")
        self.data_model.verify_downloads = str(
            self.settings.value("verify_downloads", "false")).lower() in ("true", "1")
        self.data_model.resumable_uploads = str(
            self.settings.value("resumable_uploads", "true")).lower() in ("true", "1")
        try:
            self.data_model.set_rate_limit(
                int(self.settings.value("rate_limit_kbps", 0) or 0) * 1024)
        except (TypeError, ValueError):
            pass
        storage_class = self.settings.value("upload_storage_class", "") or ""
        sse = self.settings.value("upload_sse", "") or ""
        kms = self.settings.value("upload_kms_key", "") or ""
        checksum = self.settings.value("upload_checksum", "") or ""
        threshold_mb = self.settings.value("multipart_threshold_mb", None)
        chunk_mb = self.settings.value("multipart_chunksize_mb", None)
        detect_type = str(self.settings.value(
            "detect_content_type", "true")).lower() in ("true", "1")
        type_overrides = self.settings.value("content_type_overrides", "") or ""
        upload_rules = self.settings.value("upload_rules", "") or ""
        self.settings.endGroup()
        self.data_model.set_multipart_sizes(
            threshold_mb=threshold_mb, chunksize_mb=chunk_mb)
        self.data_model.set_upload_options(
            storage_class=storage_class, sse=sse, kms_key_id=kms,
            checksum_algorithm=checksum)
        self.data_model.set_content_type_options(
            detect=detect_type,
            overrides=DataModel.parse_content_type_overrides(type_overrides))
        self.data_model.set_upload_rules(
            DataModel.parse_upload_rules(upload_rules))

    def bucket_enter_active(self) -> bool:
        th = self._bucket_enter_thread
        if th is None:
            return False
        if sip is not None:
            try:
                if sip.isdeleted(th):
                    self._bucket_enter_thread = None
                    self._bucket_enter_worker = None
                    return False
            except Exception:
                pass
        try:
            return th.isRunning()
        except RuntimeError:
            self._bucket_enter_thread = None
            self._bucket_enter_worker = None
            return False

    def is_read_only(self) -> bool:
        return bool(getattr(self.data_model, "read_only", False))

    def update_window_title(self):
        profile = getattr(self, "profile_name", "")
        suffix = "  [read-only]" if self.is_read_only() else ""
        if profile:
            self.setWindowTitle(f"{self.title} — {profile}{suffix}")
        else:
            self.setWindowTitle(f"{self.title}{suffix}")

    def set_accent(self, value) -> str:
        """
        Tint the window for this profile, or hide the band when unset.

        The launcher's read-only badge only helps while picking a profile;
        once a window is open nothing distinguished prod from dev.
        """
        accent = normalize_accent(value)
        self.accent = accent
        if not accent:
            self.accent_bar.hide()
            return ""
        palette = self.accent_bar.palette()
        palette.setColor(QPalette.ColorRole.Window, QColor(accent))
        self.accent_bar.setPalette(palette)
        self.accent_bar.show()
        return accent

    def in_bucket_list_mode(self) -> bool:
        return not bool(self.data_model.bucket)

    def _return_to_bucket_list_mode(self):
        """
        Leave the current bucket and go back to bucket list safely:
        - clear active bucket/prefix
        - restore profile_region so list_buckets() etc. sign correctly
        - restore *root* endpoint & addressing style
        - drop cached client so future self.data_model.client is rebuilt
        """
        # The pending undo belongs to the bucket we are leaving.
        self._clear_undo_delete()

        self.data_model.bucket = ""
        self.data_model.current_folder = ""
        self.data_model.prev_folder = ""

        # restore original root settings
        self.data_model.region_name = self.data_model.profile_region
        self.data_model.endpoint_url = self.data_model.profile_endpoint_url
        self.data_model.use_path = self.data_model.profile_use_path

        self.data_model._client = None  # force rebuild with profile settings on next access

    # Helper to always get the primary (column 0) item for a row,
    # no matter which column was clicked.
    def get_row_primary_item(self, any_index: QModelIndex):
        """
        Returns (item, text, type)
        where 'item' is the QStandardItem from column 0 of that row.
        If index invalid, returns (None, None, None).
        """
        if not any_index or not any_index.isValid():
            return None, None, None
        # map proxy -> source
        ix_src = self.proxy.mapToSource(any_index)
        row = ix_src.row()
        model = ix_src.model()
        primary_idx = model.index(row, 0)
        primary_item = model.itemFromIndex(primary_idx)
        if primary_item is None:
            return None, None, None
        return primary_item, primary_item.text(), getattr(primary_item, "t", None)

    def _on_batch_progress(self, done, total):
        self._smooth_total = max(1, int(total))
        self._smooth_done = max(0, int(done))

    def _on_tick(self):
        now = time.time()

        if not hasattr(self, "_rate_samples"):
            self._rate_samples = []
        self._rate_samples.append((now, self._smooth_done))
        cutoff = now - RATE_WINDOW_SEC
        self._rate_samples = [p for p in self._rate_samples if p[0] >= cutoff]

        inst_rate_bps = 0.0
        if len(self._rate_samples) >= 2:
            t0, b0 = self._rate_samples[0]
            t1, b1 = self._rate_samples[-1]
            dt = max(1e-6, t1 - t0)
            db = max(0, b1 - b0)
            inst_rate_bps = db / dt  # bytes/sec over recent window

        alpha = EMA_ALPHA
        self._smooth_rate_bps = (
            alpha * inst_rate_bps + (1 - alpha) * self._smooth_rate_bps
        )

        if self._last_tick_time == 0.0:
            # first tick init
            self._last_tick_time = now
            self._last_tick_bytes = self._smooth_done
        else:
            dt_long = now - self._last_tick_time
            if dt_long >= STALL_DECAY_INTERVAL_SEC:
                # if no new bytes lately, bleed off displayed rate
                if self._smooth_done <= self._last_tick_bytes:
                    self._smooth_rate_bps *= 0.5
                self._last_tick_time = now
                self._last_tick_bytes = self._smooth_done

        # avoid tiny random noise
        display_rate_bps = self._smooth_rate_bps
        if display_rate_bps < 1:
            display_rate_bps = 0.0

        pct = 0
        if self._smooth_total > 0:
            pct = int((self._smooth_done / self._smooth_total) * 100)
            pct = min(100, max(0, pct))

        self.pb.setMaximum(100)
        self.pb.setValue(pct)

        remaining = max(0, self._smooth_total - self._smooth_done)
        eta_txt = ""
        if display_rate_bps > 1 and remaining > 0 and pct < 100:
            eta_sec = int(remaining / display_rate_bps)
            m, s = divmod(eta_sec, 60)
            h, m = divmod(m, 60)
            eta_txt = f"  ETA {h:02d}:{m:02d}:{s:02d}"
        elif pct >= 100:
            eta_txt = "  Done"

        self.status_text.setText(
            f"{self._status_prefix} "
            f"{_human_bytes(self._smooth_done)} / {_human_bytes(self._smooth_total)}"
            f"  ({_human_bytes(display_rate_bps)}/s){eta_txt}"
        )

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

    def select_first(self):
        if self.proxy.rowCount() > 0:
            index = self.proxy.index(0, 0)
            self.listview.setCurrentIndex(index)

    def ix_by_name(self, name):
        for r in range(self.model.rowCount()):
            ix_src = self.model.index(r, 0)
            if name == self.model.itemFromIndex(ix_src).text():
                ix = self.proxy.mapFromSource(ix_src)
                # mapFromSource yields an invalid index for filtered-out rows
                return ix if ix.isValid() else None
        return None

    def name_by_first_ix(self, ixs):
        """
        Returns (item, display_name, full_key).
        For folders we append '/', but NOT for the special UP_ENTRY_LABEL.
        For buckets, full_key is just the bucket name.
        """
        if ixs:
            primary_item, text, t = self.get_row_primary_item(ixs[0])
            if primary_item is None:
                return None, None, None

            name = text
            if t == FSObjectType.BUCKET:
                return primary_item, name, name

            if t == FSObjectType.FOLDER and name != UP_ENTRY_LABEL:
                name = "%s/" % name

            full_key = (
                self.data_model.current_folder + name
                if self.data_model.bucket
                else name
            )
            return primary_item, name, full_key

        return None, None, None

    def _usage_target_from_selection(self):
        """
        Returns (bucket_name, prefix_for_usage) according to container rules:

        Bucket list:
          - bucket row -> (bucket, "")

        Inside a bucket:
          - [..] at bucket root -> (bucket, "")
          - [..] inside folder -> (bucket, parent_prefix(current_folder))
          - folder selected -> (bucket, current_folder + folder + "/")
          - file selected -> (bucket, current_folder)
          - no/invalid selection -> (bucket, current_folder)
        """
        sel = self.listview.selectionModel()
        if sel is None:
            return "", ""

        if self.in_bucket_list_mode():
            ix = sel.currentIndex()
            if not ix.isValid():
                return "", ""
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None or not name:
                return "", ""
            if name == UP_ENTRY_LABEL:
                return "", ""
            if t != FSObjectType.BUCKET:
                return "", ""
            return name, ""

        if not self.data_model.bucket:
            return "", ""

        cur_folder = (self.data_model.current_folder or "")

        ix = sel.currentIndex()
        if not ix.isValid():
            return self.data_model.bucket, cur_folder

        primary_item, name, t = self.get_row_primary_item(ix)
        if primary_item is None or not name:
            return self.data_model.bucket, cur_folder

        if name == UP_ENTRY_LABEL:
            if not cur_folder:
                return self.data_model.bucket, ""
            return self.data_model.bucket, cur_folder

        if t == FSObjectType.FOLDER:
            prefix = cur_folder + name + "/"
            return self.data_model.bucket, prefix

        return self.data_model.bucket, cur_folder

    def request_bucket_usage(self):
        bucket_name, prefix = self._usage_target_from_selection()
        if not bucket_name:
            self.statusBar().showMessage("Select a bucket first", 2000)
            return

        existing = getattr(self, "_bucket_usage_thread", None)
        if existing is not None:
            try:
                if existing.isRunning():
                    self.statusBar().showMessage("Usage calculation already running…", 2000)
                    return
            except Exception:
                pass
            # Drop the stale reference; the QThread is parented to self and
            # will deleteLater itself via its finished signal.
            self._bucket_usage_thread = None
            self._bucket_usage_worker = None

        self._bucket_usage_token = getattr(self, "_bucket_usage_token", 0) + 1
        token = self._bucket_usage_token

        self.statusBar().showMessage("Calculating usage…", 2000)
        self._show_bucket_usage_dialog(bucket_name, prefix)
        self.btnBucketUsage.setEnabled(False)

        t = QThread(self)
        w = UsageWorker(self.data_model.clone_for_worker(), bucket_name, prefix)
        w.moveToThread(t)

        def _clear_refs():
            self._bucket_usage_thread = None
            self._bucket_usage_worker = None

        def reenable():
            self.btnBucketUsage.setEnabled(True)

        def apply_result(bname, pref, result):
            if token != self._bucket_usage_token:
                return

            if isinstance(result, Exception):
                self.statusBar().showMessage(f"Usage failed: {result}", 4000)
                if self._bucket_usage_dialog is not None:
                    self._bucket_usage_dialog.set_error(bname, pref, result)
                return

            total = int(result.get("total", 0) or 0)
            by_cat = dict(result.get("by_cat", {}) or {})
            by_top = dict(result.get("by_top", {}) or {})

            self.statusBar().showMessage("Usage calculated", 2000)
            if self._bucket_usage_dialog is not None:
                self._bucket_usage_dialog.set_result(
                    bname, pref, total, by_cat, by_top,
                    count=int(result.get("count", 0) or 0),
                    by_class=dict(result.get("by_class", {}) or {}),
                    largest=list(result.get("largest", []) or []),
                    cost=result.get("cost"),
                    colder=list(result.get("colder", []) or []),
                )

        w.finished.connect(apply_result)
        w.finished.connect(t.quit)
        release_worker_on_finish(t, w)

        t.finished.connect(t.deleteLater)
        t.finished.connect(_clear_refs)
        t.finished.connect(reenable)
        t.started.connect(w.run)

        self._bucket_usage_thread = t
        self._bucket_usage_worker = w
        t.start()

    def eventFilter(self, obj, event):

        if obj is self.search_edit and event.type() == QEvent.Type.KeyPress:
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

        if obj == self.listview:
            if event.type() == QEvent.Type.ContextMenu and obj is self.listview:
                ixs = self.listview.selectedIndexes()

                # Use row-primary instead of raw clicked column
                m, raw_name, upload_path = self.name_by_first_ix(ixs)

                up_selected = (
                    m is not None and raw_name and raw_name.rstrip("/") == UP_ENTRY_LABEL
                )
                bucket_list_mode = self.in_bucket_list_mode()
                # A read-only profile shows only the non-mutating entries; the
                # model refuses writes too, this just avoids dead menu items.
                writable = not self.is_read_only()

                if upload_path is None or up_selected:
                    upload_path = self.data_model.current_folder

                self.menu.clear()

                # bucket list mode menu
                if bucket_list_mode:
                    act_new_bucket = None
                    act_empty_bucket = None
                    act_del_bucket = None
                    if not writable:
                        self._menu_click_guard.arm()
                        self.menu.addAction(
                            QAction("(read-only profile)", self)
                        ).setEnabled(False)
                        self.menu.exec(event.globalPos())
                        return True
                    act_new_bucket = QAction(
                        themed_icon("folder-new", os.path.join(
                                    self.current_dir,
                                    "icons",
                                    "create_new_folder_24px.svg",
                                )),
                        "Create bucket…",
                    )
                    self.menu.addAction(act_new_bucket)

                    act_empty_bucket = None
                    act_del_bucket = None
                    act_bucket_tab = None
                    # Only allow delete if selection is actually a bucket row
                    if ixs and m and getattr(m, "t", None) == FSObjectType.BUCKET:
                        act_bucket_tab = QAction(
                            themed_icon("tab-new", os.path.join(
                                self.current_dir, "icons", "folder_24px.svg")),
                            "Open in new tab (Ctrl+T)",
                        )
                        self.menu.addAction(act_bucket_tab)
                        act_empty_bucket = QAction(
                            themed_icon(
                        "edit-clear",
                        os.path.join(self.current_dir, "icons", "delete_24px.svg")),
                            "Empty bucket…",
                        )
                        self.menu.addAction(act_empty_bucket)
                        act_del_bucket = QAction(
                            themed_icon("edit-delete", os.path.join(
                                        self.current_dir, "icons", "delete_24px.svg"
                                    )),
                            "Delete bucket…",
                        )
                        self.menu.addAction(act_del_bucket)

                    self._menu_click_guard.arm()
                    clk = self.menu.exec(event.globalPos())
                    if not clk:
                        return False

                    if act_bucket_tab and clk == act_bucket_tab:
                        self.open_selection_in_new_tab()
                    if clk == act_new_bucket:
                        self.new_bucket()
                    if act_empty_bucket and clk == act_empty_bucket:
                        self.empty_bucket_ui()
                    if act_del_bucket and clk == act_del_bucket:
                        self.delete_bucket_ui()

                    return True

                # inside bucket menu
                upload_selected_action = None
                upload_current_action = None
                create_folder_action = None
                download_action = None
                delete_action = None
                copy_move_action = None
                copy_profile_action = None
                tags_action = None
                properties_selected_action = None
                share_tmp_action = None
                share_public_action = None
                open_action = None
                versions_action = None
                rename_action = None
                restore_action = None
                storage_action = None
                retype_action = None
                new_tab_action = None
                metadata_action = None
                search_action = None
                versioning_action = None
                incomplete_action = None
                bulk_rename_action = None
                sync_action = None
                copy_clip_action = None
                cut_clip_action = None
                paste_clip_action = None
                bulk_tags_action = None
                zip_action = None

                if (
                    m
                    and getattr(m, "t", None) == FSObjectType.FOLDER
                    and not up_selected
                ):
                    upload_selected_action = QAction(
                        themed_icon("network-server", os.path.join(
                                    self.current_dir,
                                    "icons",
                                    "file_upload_24px.svg",
                                )),
                        "Upload -> %s" % upload_path,
                    )
                    self.menu.addAction(upload_selected_action)

                upload_current_action = QAction(
                    themed_icon("network-server", os.path.join(
                                self.current_dir, "icons", "file_upload_24px.svg"
                            )),
                    "Upload -> %s"
                    % (
                        "/"
                        if not self.data_model.current_folder
                        else self.data_model.current_folder
                    ),
                )
                self.menu.addAction(upload_current_action)

                upload_folder_action = QAction(
                    themed_icon("folder", os.path.join(
                                self.current_dir, "icons", "folder_24px.svg"
                            )),
                    "Upload folder -> %s"
                    % (
                        "/"
                        if not self.data_model.current_folder
                        else self.data_model.current_folder
                    ),
                )
                self.menu.addAction(upload_folder_action)

                create_folder_action = QAction(
                    themed_icon("folder-new", os.path.join(
                                self.current_dir,
                                "icons",
                                "create_new_folder_24px.svg",
                            )),
                    "Create folder",
                )
                self.menu.addAction(create_folder_action)

                search_action = QAction(
                    themed_icon(
                        "edit-find",
                        os.path.join(self.current_dir, "icons", "document_24px.svg")),
                    "Search here…",
                )
                self.menu.addAction(search_action)

                if self._clipboard and self._clipboard.get("items"):
                    paste_clip_action = QAction(
                        themed_icon(
                        "edit-paste",
                        os.path.join(self.current_dir, "icons", "copy_24px.svg")),
                        "Paste %d item(s) here (Ctrl+V)"
                        % len(self._clipboard["items"]),
                    )
                    self.menu.addAction(paste_clip_action)

                versioning_action = QAction(
                    themed_icon(
                        "document-open-recent",
                        os.path.join(self.current_dir, "icons", "document_24px.svg")),
                    "Bucket versioning…",
                )
                self.menu.addAction(versioning_action)

                incomplete_action = QAction(
                    themed_icon(
                        "edit-clear-history",
                        os.path.join(self.current_dir, "icons", "delete_24px.svg")),
                    "Incomplete uploads…",
                )
                self.menu.addAction(incomplete_action)

                sync_action = QAction(
                    themed_icon(
                        "folder-sync",
                        os.path.join(self.current_dir, "icons", "refresh_24px.svg")),
                    "Sync with local folder… (Ctrl+E)",
                )
                self.menu.addAction(sync_action)

                if ixs and not up_selected:
                    if m and getattr(m, 't', None) == FSObjectType.FOLDER:
                        new_tab_action = QAction(
                            themed_icon("tab-new", os.path.join(
                                self.current_dir, "icons", "folder_24px.svg")),
                            "Open in new tab (Ctrl+T)",
                        )
                        self.menu.addAction(new_tab_action)
                    download_action = QAction(
                        themed_icon("emblem-downloads", os.path.join(
                                    self.current_dir, "icons", "download_24px.svg"
                                )),
                        "Download",
                    )
                    self.menu.addAction(download_action)
                    if m and getattr(m, 't', None) == FSObjectType.FILE:
                        open_action = QAction(
                            themed_icon(
                        "document-open",
                        os.path.join(self.current_dir, "icons", "folder_24px.svg")),
                            "Open / preview",
                        )
                        self.menu.addAction(open_action)

                        versions_action = QAction(
                            themed_icon(
                        "document-open-recent",
                        os.path.join(self.current_dir, "icons", "document_24px.svg")),
                            "Versions…",
                        )
                        self.menu.addAction(versions_action)

                        share_tmp_action = QAction(
                            themed_icon('insert-link', os.path.join(self.current_dir, "icons", "copy_24px.svg")),
                            'Share link…',
                        )
                        self.menu.addAction(share_tmp_action)

                        share_public_action = QAction(
                            themed_icon('insert-link', os.path.join(self.current_dir, "icons", "copy_24px.svg")),
                            'Make public + copy URL…',
                        )
                        self.menu.addAction(share_public_action)

                    delete_action = QAction(
                        themed_icon("edit-delete", os.path.join(
                                    self.current_dir, "icons", "delete_24px.svg"
                                )),
                        "Delete",
                    )
                    self.menu.addAction(delete_action)

                    self.menu.addSeparator()
                    copy_clip_action = QAction(
                        themed_icon(
                        "edit-copy",
                        os.path.join(self.current_dir, "icons", "copy_24px.svg")), "Copy (Ctrl+C)")
                    self.menu.addAction(copy_clip_action)
                    cut_clip_action = QAction(
                        themed_icon(
                        "edit-cut",
                        os.path.join(self.current_dir, "icons", "copy_24px.svg")), "Cut (Ctrl+X)")
                    self.menu.addAction(cut_clip_action)

                    zip_action = QAction(
                        themed_icon(
                        "package-x-generic",
                        os.path.join(self.current_dir, "icons", "document_24px.svg")),
                        "Download as ZIP…")
                    self.menu.addAction(zip_action)

                    bulk_tags_action = QAction(
                        themed_icon(
                        "document-properties",
                        os.path.join(self.current_dir, "icons", "settings_24px.svg")),
                        "Edit tags on selection…")
                    self.menu.addAction(bulk_tags_action)

                    copy_move_action = QAction(
                        themed_icon("edit-copy", os.path.join(
                                    self.current_dir, "icons", "copy_24px.svg"
                                )),
                        "Copy / Move to…",
                    )
                    self.menu.addAction(copy_move_action)

                    # Deliberately NOT stripped on a read-only profile below:
                    # copying data OUT reads the source and writes elsewhere,
                    # and the destination model enforces its own read-only.
                    copy_profile_action = QAction(
                        themed_icon(
                        "document-send",
                        os.path.join(self.current_dir, "icons", "file_upload_24px.svg")),
                        "Copy to another profile…",
                    )
                    self.menu.addAction(copy_profile_action)

                    rename_action = QAction(
                        themed_icon(
                        "edit-rename",
                        os.path.join(self.current_dir, "icons", "edit_24px.svg")),
                        "Rename…",
                    )
                    self.menu.addAction(rename_action)

                    bulk_rename_action = QAction(
                        themed_icon(
                        "edit-rename",
                        os.path.join(self.current_dir, "icons", "edit_24px.svg")),
                        "Rename multiple… (Shift+F2)",
                    )
                    self.menu.addAction(bulk_rename_action)

                    storage_action = QAction(
                        themed_icon(
                        "drive-harddisk",
                        os.path.join(self.current_dir, "icons", "bucket_24px.svg")),
                        "Change storage class…",
                    )
                    self.menu.addAction(storage_action)

                    retype_action = QAction(
                        themed_icon(
                        "text-x-generic",
                        os.path.join(self.current_dir, "icons", "document_24px.svg")),
                        "Fix Content-Type from extension",
                    )
                    retype_action.setToolTip(
                        "Re-stamp Content-Type on the selection from each "
                        "key's extension")
                    self.menu.addAction(retype_action)

                    restore_action = QAction(
                        themed_icon(
                        "emblem-downloads",
                        os.path.join(self.current_dir, "icons", "download_24px.svg")),
                        "Restore from Glacier…",
                    )
                    self.menu.addAction(restore_action)

                    if m and getattr(m, "t", None) == FSObjectType.FILE:
                        tags_action = QAction(
                            themed_icon(
                        "document-properties",
                        os.path.join(self.current_dir, "icons", "settings_24px.svg")),
                            "Edit tags…",
                        )
                        self.menu.addAction(tags_action)

                        metadata_action = QAction(
                            themed_icon(
                        "document-properties",
                        os.path.join(self.current_dir, "icons", "settings_24px.svg")),
                            "Edit metadata…",
                        )
                        self.menu.addAction(metadata_action)

                m2, name2, key = self.name_by_first_ix(ixs)
                if not key:
                    key = self.data_model.current_folder
                if (
                    name2
                    and m2
                    and name2.rstrip("/") != UP_ENTRY_LABEL
                ):
                    properties_selected_action = QAction(
                        themed_icon("document-properties", os.path.join(
                                    self.current_dir, "icons", "puzzle_24px.svg"
                                )),
                        "Properties",
                    )
                    self.menu.addAction(properties_selected_action)

                if not writable:
                    # Strip every mutating entry in one pass rather than
                    # guarding each addAction above.
                    for act in (
                        upload_selected_action, upload_current_action,
                        upload_folder_action, create_folder_action,
                        versioning_action, incomplete_action, sync_action,
                        share_public_action, delete_action, copy_move_action,
                        rename_action, bulk_rename_action, storage_action,
                        restore_action, tags_action, metadata_action,
                        retype_action, hold_action,
                        cut_clip_action, paste_clip_action, bulk_tags_action,
                    ):
                        if act is not None:
                            self.menu.removeAction(act)
                    hint = QAction("(read-only profile)", self)
                    hint.setEnabled(False)
                    self.menu.addAction(hint)

                self._menu_click_guard.arm()
                clk = self.menu.exec(event.globalPos())
                if not clk:
                    return False

                if clk == upload_selected_action:
                    self.upload(upload_path)
                if clk == upload_current_action:
                    self.upload()
                if clk == upload_folder_action:
                    self.upload_folder()
                if clk == create_folder_action:
                    self.new_folder()
                if search_action and clk == search_action:
                    self.open_search()
                if versioning_action and clk == versioning_action:
                    self.bucket_versioning_ui()
                if incomplete_action and clk == incomplete_action:
                    self.show_incomplete_uploads()
                if clk == download_action:
                    self.download()
                if clk == share_tmp_action:
                    self.share_link(key)
                if clk == share_public_action:
                    self.make_public_and_copy(key)
                if clk == delete_action:
                    self.delete()
                if copy_move_action and clk == copy_move_action:
                    self.copy_move()
                if copy_profile_action and clk == copy_profile_action:
                    self.copy_to_profile()
                if rename_action and clk == rename_action:
                    self.rename_selected()
                if bulk_rename_action and clk == bulk_rename_action:
                    self.bulk_rename()
                if copy_clip_action and clk == copy_clip_action:
                    self.copy_to_clipboard()
                if cut_clip_action and clk == cut_clip_action:
                    self.copy_to_clipboard(cut=True)
                if paste_clip_action and clk == paste_clip_action:
                    self.paste_from_clipboard()
                if bulk_tags_action and clk == bulk_tags_action:
                    self.bulk_tags()
                if zip_action and clk == zip_action:
                    self.download_as_zip()
                if sync_action and clk == sync_action:
                    self.open_sync()
                if new_tab_action and clk == new_tab_action:
                    self.open_selection_in_new_tab()
                if open_action and clk == open_action:
                    self.open_or_preview(key)
                if versions_action and clk == versions_action:
                    self.show_versions(key)
                if tags_action and clk == tags_action:
                    self.edit_tags(key)
                if metadata_action and clk == metadata_action:
                    self.edit_metadata(key)
                if storage_action and clk == storage_action:
                    self.change_storage_class_ui()
                if retype_action and clk == retype_action:
                    self.fix_content_type_ui()
                if hold_action and clk == hold_action:
                    self.toggle_legal_hold(key)
                if restore_action and clk == restore_action:
                    self.restore_from_glacier()
                if clk == properties_selected_action:
                    self.properties(self.data_model, key)

                return True

            if event.type() == QEvent.Type.KeyPress:
                key = event.key()
                mods = event.modifiers()

                # Each handled key returns True so the event does not also fall
                # through to QTreeView's built-in type-ahead search, which would
                # otherwise fire a second, unrelated action on the same press.
                if key == Qt.Key.Key_Escape:
                    # First Esc clears an active quick-find filter, second
                    # (or with no filter) cancels transfers.
                    if self._filter_row.isVisible() or self.search_edit.text():
                        self._hide_search()
                    else:
                        self.cancel_transfers()
                    return True
                if key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                    ix = self.listview.currentIndex()
                    if ix.isValid():
                        self.list_doubleClicked(ix)
                    return True
                if key == Qt.Key.Key_Delete:
                    self.on_toolbar_delete()
                    return True
                if key == Qt.Key.Key_F2:
                    if not self.in_bucket_list_mode():
                        self.rename_selected()
                    return True
                if key == Qt.Key.Key_Backspace:
                    self.goUp()
                    return True
                if key == Qt.Key.Key_Insert:
                    self.on_toolbar_create()
                    return True
                if key == Qt.Key.Key_Home:
                    self.goHome()
                    return True
                if key == Qt.Key.Key_F1:
                    self.about()
                    return True

                # Plain printable characters start the quick-find filter
                # instead of triggering actions. Single-letter actions
                # (refresh, usage, upload, …) live on modifier shortcuts
                # attached to the toolbar QActions — see createActions().
                text = event.text()
                if (
                    text
                    and text.isprintable()
                    and not text.isspace()
                    and not (
                        mods
                        & (
                            Qt.KeyboardModifier.ControlModifier
                            | Qt.KeyboardModifier.AltModifier
                            | Qt.KeyboardModifier.MetaModifier
                        )
                    )
                ):
                    self._open_search_with_text(text)
                    return True
        return super().eventFilter(obj, event)

    def simple(self, title, message):
        QMessageBox(
            QMessageBox.Icon.Information,
            title,
            message,
            QMessageBox.StandardButton.NoButton,
            self,
            Qt.WindowType.Dialog | Qt.WindowType.NoDropShadowWindowHint,
        ).show()

    def switch_profile(self):

        if self.transfers_active():
            QMessageBox.information(
                self,
                "Switch profile",
                "Profile switching is disabled while uploads/downloads are active.",
            )
            return
        dlg = ProfileSwitchWindow(self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        prof = dlg.get_selected_profile()
        if not prof:
            return

        self.apply_profile(prof)

    def apply_profile(self, prof):
        """
        Apply a new profile without restarting the app.
        """
        old_profile = getattr(self, "profile_name", None)
        # Before the name changes: the location is stored per profile, so the
        # one being left behind has to record where it was first.
        self._save_last_location()

        self.profile_name = prof.name

        self.data_model.profile_endpoint_url = prof.url
        self.data_model.profile_use_path = prof.use_path
        self.data_model.profile_region = prof.region

        self.data_model.endpoint_url = prof.url
        self.data_model.use_path = prof.use_path
        self.data_model.region_name = prof.region
        self.data_model.access_key = prof.access_key
        self.data_model.secret_key = prof.secret_key
        self.data_model.no_ssl_check = prof.no_ssl_check
        self.data_model.session_token = getattr(prof, "session_token", "") or ""
        self.data_model.read_only = bool(getattr(prof, "read_only", False))
        self.data_model.requester_pays = bool(
            getattr(prof, "requester_pays", False))
        self.data_model.public_base_url = str(
            getattr(prof, "public_base_url", "") or "").rstrip("/")
        self.data_model.proxy_url = str(getattr(prof, "proxy_url", "") or "")
        self.data_model.ca_bundle = str(getattr(prof, "ca_bundle", "") or "")
        self.session_expires = str(getattr(prof, "session_expires", "") or "")
        self.aws_profile = str(getattr(prof, "aws_profile", "") or "")
        self.credential_process = str(
            getattr(prof, "credential_process", "") or "")
        self.set_accent(getattr(prof, "color", ""))
        self.data_model.binding_cache.clear()
        self._clear_undo_delete()
        self._clipboard = None

        self.data_model._client = None

        self.data_model.current_folder = ""
        self.data_model.prev_folder = ""
        self.data_model.bucket = ""
        self._disable_restore_last_bucket_once = True

        self.statusBar().showMessage(f"[{self.profile_name}][all buckets]", 3000)
        try:
            self._last_selected_bucket = None
            if hasattr(self, "_last_selected_in_prefix") and isinstance(self._last_selected_in_prefix, dict):
                self._last_selected_in_prefix.clear()
        except Exception:
            pass

        self._load_binding_cache()
        self._load_bookmarks()
        self._rebuild_bookmark_menu()
        self._update_credential_status()
        if getattr(self, "actRefreshCredentials", None) is not None:
            self.actRefreshCredentials.setEnabled(
                self.can_refresh_credentials())
        # Tabs are locations inside one account, so the other profile's set
        # is meaningless here.
        self._load_tabs()
        # Same for a watch: its bucket belongs to the account being left.
        if self.watch_active():
            self.stop_watch()
        self._load_watch_config()
        self.navigate(show_loading=True)
        self.update_window_title()

        if old_profile and old_profile != self.profile_name:
            self.log(f"Profile switched: {old_profile} → {self.profile_name}")
        else:
            self.log(f"Profile switched to: {self.profile_name}")

    def about(self):
        sysinfo = QSysInfo()
        sys_info = sysinfo.prettyProductName() + "<br>" + sysinfo.kernelType() + " " + sysinfo.kernelVersion()
        qt_version = QtCore.qVersion()
        title = "S3 Duck 🦆 %s" % __VERSION__
        message = (
            """
            <span style='color: #3465a4; font-size: 20pt;font-weight: bold;text-align: center;'></span>
            <center><h3>S3 Duck 🦆</h3></center>
            <a title='Vladislav Ananev' href='https://github.com/nexusriot' target='_blank'>
            <br><span style='color: #8743e2; font-size: 10pt;'>©2022-2026 Vladislav Ananev</a><br><br></strong></span></p>
            """
            + "Press Ctrl+/ for the keyboard shortcut list<br><br>"
            + "version %s" % __VERSION__
            + "<br>Qt %s" % qt_version
            + "<br><br>"
            + sys_info
        )
        self.simple(title, message)

    def properties(self, model, key):
        PropertiesWindow(self, settings=(model, key)).exec()

    def modelToListView_bucket_mode(self, bucket_items):
        """Populate the view with buckets only (no [..])."""
        self._begin_model_reset_ui()
        try:
            self.model.setRowCount(0)
            bucket_icon = themed_icon("drive-harddisk", os.path.join(self.current_dir, "icons", "bucket_24px.svg"))

            for b in bucket_items:
                self.model.appendRow(
                    [
                        ListItem(0, FSObjectType.BUCKET, bucket_icon, b.name),
                        ListItem(0, FSObjectType.BUCKET, "<BUCKET>"),
                        ListItem(0, FSObjectType.BUCKET, ""),
                        ListItem(0, FSObjectType.BUCKET, ""),
                        ListItem(0, FSObjectType.BUCKET, ""),
                    ]
                )
        finally:
            self._end_model_reset_ui()

    def modelToListView(self, model_result):
        """
        Populate the view for objects inside a selected bucket.
        We inject '[..]' at top.
        """
        self._begin_model_reset_ui()
        try:
            self.model.setRowCount(0)

            if self.data_model.bucket:
                up_icon = themed_icon("go-up", os.path.join(self.current_dir, "icons",
                                       "arrow_upward_24px.svg"))
                self.model.appendRow(
                    [
                        ListItem(0, FSObjectType.FOLDER, up_icon,
                                 UP_ENTRY_LABEL),
                        ListItem(0, FSObjectType.FOLDER, ""),
                        ListItem(0, FSObjectType.FOLDER, ""),
                        ListItem(0, FSObjectType.FOLDER, ""),
                        ListItem(0, FSObjectType.FOLDER, ""),
                    ]
                )

            if model_result:
                for i in model_result:
                    if i.type_ == FSObjectType.FILE:
                        icon = QIcon().fromTheme(
                            "go-first",
                            QIcon(os.path.join(self.current_dir, "icons",
                                               "document_24px.svg")),
                        )
                        size_val = int(i.size or 0)
                        size = _human_bytes(size_val)
                        modified = str(i.modified)
                        storage = getattr(i, "storage_class", "") or ""
                        etag = getattr(i, "etag", "") or ""
                    else:
                        icon = QIcon().fromTheme(
                            "network-server",
                            QIcon(os.path.join(self.current_dir, "icons",
                                               "folder_24px.svg")),
                        )
                        size_val = 0
                        size = "<DIR>"
                        modified = ""
                        storage = ""
                        etag = ""

                    self.model.appendRow(
                        [
                            ListItem(size_val, i.type_, icon, i.name),
                            ListItem(size_val, i.type_, size),
                            ListItem(size_val, i.type_, modified),
                            ListItem(size_val, i.type_, storage),
                            ListItem(size_val, i.type_, etag),
                        ]
                    )

        finally:
            self._end_model_reset_ui()

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder
        return self.data_model.current_folder

    def _selection_summary(self) -> str:
        """'N selected — size' for the current multi-selection, else ''."""
        sm = self.listview.selectionModel()
        if sm is None:
            return ""
        count = 0
        total = 0
        folders = 0
        for ix in sm.selectedIndexes():
            if ix.column() != 0:
                continue
            item, name, t = self.get_row_primary_item(ix)
            if item is None or name == UP_ENTRY_LABEL:
                continue
            count += 1
            if t == FSObjectType.FOLDER:
                folders += 1
            else:
                total += int(getattr(item, "size", 0) or 0)
        if count <= 1 and not folders:
            return ""
        parts = [f"{count} selected"]
        if folders:
            parts.append(f"{folders} dir(s)")
        if total:
            parts.append(_human_bytes(total))
        return " — ".join(parts)

    def _update_selection_status(self, *_args):
        text = self._selection_summary()
        if text:
            self.status_text.setText(text)
        elif not self.transfers_active():
            self.status_text.setText("")

    def _on_current_changed_for_usage(self, current: QModelIndex, previous: QModelIndex):
        # enable/disable Σ based on whether we can compute a target
        try:
            b, _p = self._usage_target_from_selection()
            self.btnBucketUsage.setEnabled(bool(b) and not self.transfers_active())
        except Exception:
            pass

        # If usage window is open, refresh on selection changes (but don't stack threads)
        try:
            if self._bucket_usage_dialog is not None and self._bucket_usage_dialog.isVisible():
                t = getattr(self, "_bucket_usage_thread", None)
                if t is None or not t.isRunning():
                    self.request_bucket_usage()
        except Exception:
            pass

    def list_doubleClicked(self, proxy_index: QModelIndex):

        if self.transfers_active():
            self.statusBar().showMessage("Transfers active — navigation is disabled", 2000)
            return

        if not proxy_index.isValid():
            return

        # Normalize selection
        sm = self.listview.selectionModel()
        if sm is not None:
            sm.blockSignals(True)
            sm.clearSelection()
            sm.setCurrentIndex(
                proxy_index,
                QItemSelectionModel.SelectionFlag.ClearAndSelect | QItemSelectionModel.SelectionFlag.Rows,
            )
            sm.blockSignals(False)

        # Always interpret based on the row's "Name" column.
        primary_item, name, t = self.get_row_primary_item(proxy_index)
        if primary_item is None:
            return

        # Enter bucket (async — enter_bucket makes S3 API calls, must not block main thread)
        if t == FSObjectType.BUCKET:
            self.enter_bucket_async(name)
            return

        if t == FSObjectType.FOLDER and name == UP_ENTRY_LABEL:
            if self.data_model.current_folder:
                self.goUp()
            else:
                # root of bucket -> go back to bucket list
                self._return_to_bucket_list_mode()
                self.navigate(restore_name=self._last_selected_bucket)
            return

        # Normal folder navigation
        if t == FSObjectType.FOLDER:
            self._last_selected_in_prefix[(self.data_model.bucket or '', self.data_model.current_folder or '')] = name
            self.change_current_folder(
                self.data_model.current_folder + f"{name}/")
            self.navigate(select_up_entry=True)
            return

        # File -> open the in-app preview
        if t == FSObjectType.FILE:
            key = (self.data_model.current_folder or "") + name
            self.open_or_preview(key)
            return

    def enter_bucket_async(self, name: str, target_prefix: str = None):
        """
        Open a bucket off the main thread (enter_bucket makes S3 API calls),
        then navigate into it — optionally straight to target_prefix.
        """
        if self.bucket_enter_active():
            return  # already entering a bucket
        if self.transfers_active():
            self.statusBar().showMessage(
                "Transfers active — navigation is disabled", 2000)
            return

        self.listview.setEnabled(False)
        self.statusBar().showMessage(f"Opening bucket '{name}'…", 0)

        th = QThread(self)
        wk = BucketEnterWorker(self.data_model, name)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.log_msg.connect(self.log)

        def _on_enter_success(bucket_name: str):
            self._last_selected_bucket = bucket_name
            self._clear_undo_delete()
            self.listview.setEnabled(True)
            if target_prefix:
                # enter_bucket resets navigation to the bucket root.
                self.data_model.current_folder = target_prefix
                self.data_model.prev_folder = ""
            self.navigate(select_up_entry=True, force=True)

        def _on_enter_failure(bucket_name: str, err_msg: str):
            self.listview.setEnabled(True)
            QMessageBox.critical(
                self,
                "Open bucket failed",
                f"Cannot open bucket '{bucket_name}': {err_msg}",
            )
            self._return_to_bucket_list_mode()
            # navigate() is async — the model is still empty here, so the
            # selection must be restored by the navigation-finished handler.
            self.navigate(restore_name=bucket_name, force=True)

        def _clear_enter_refs():
            self._bucket_enter_thread = None
            self._bucket_enter_worker = None

        wk.success.connect(_on_enter_success)
        wk.failure.connect(_on_enter_failure)
        wk.finished.connect(th.quit)
        release_worker_on_finish(th, wk)
        th.finished.connect(_clear_enter_refs)
        th.finished.connect(th.deleteLater)

        self._bucket_enter_thread = th
        self._bucket_enter_worker = wk
        th.start()

    @staticmethod
    def _thread_is_running(th) -> bool:
        """True only for a QThread whose C++ object is still alive and busy."""
        if sip is not None:
            try:
                if sip.isdeleted(th):
                    return False
            except Exception:
                pass
        try:
            return bool(th.isRunning())
        except RuntimeError:
            return False

    def _park_nav_thread(self):
        """
        Keep a superseded navigation QThread reachable until it has finished.

        REGRESSION (crash): this used to drop the reference outright, on the
        grounds that the QThread is parented to this window and deleteLater()s
        itself once run() returns. Both are true, and neither helps when the
        window closes first — Qt then destroys a child QThread that is still
        running, which aborts the process. Navigating twice and quitting while
        the first listing was in flight was enough to do it. Parked threads are
        joined by _shutdown_threads; finished ones are pruned here.
        """
        self._nav_orphan_threads = [
            th for th in (*self._nav_orphan_threads, self._nav_thread)
            if th is not None and self._thread_is_running(th)
        ]
        self._nav_thread = None
        self._nav_worker = None

    def navigate(self, restore_name: str = None,
                 select_up_entry: bool = False, show_loading: bool = False,
                 force: bool = False):
        """Asynchronous navigation (bucket list / bucket objects) to keep UI responsive."""

        if (not force) and self.transfers_active():
            self.statusBar().showMessage("Transfers active — navigation is disabled", 2000)
            return

        # BucketEnterWorker mutates the *shared* model (endpoint/region/client)
        # rather than a clone, so starting a navigation mid-entry would clone
        # half-updated connection state. Refresh shortcuts stay live even while
        # the list view is disabled, which is how this gets hit.
        #
        # force=True is the entry worker's own callbacks saying the model is
        # settled: success() is emitted before the thread stops running, so
        # without this exemption the guard would swallow the navigation that
        # actually shows the opened bucket.
        if (not force) and self.bucket_enter_active():
            self.statusBar().showMessage("Opening bucket — please wait…", 2000)
            return

        self._reset_search_on_navigate()
        self._remember_current_selection()

        self._nav_seq += 1
        seq = self._nav_seq
        self._nav_pending_restore_name = restore_name
        self._nav_select_up_entry = bool(select_up_entry)

        bucket = self.data_model.bucket or ""
        prefix = self.data_model.current_folder or ""

        self.listview.setEnabled(False)
        if show_loading:
            self._show_loading("Loading", "Loading…")
        else:
            self.statusBar().showMessage("Loading…", 0)

        # don't try to interrupt a previous navigation worker: it may be
        # blocked inside an S3 call that quit() can't cancel. Instead each
        # worker uses its own model clone (private boto3 client), so an
        # orphaned worker cannot race us. Stale results are discarded by the
        # _nav_seq check in _on_navigation_finished.
        self._park_nav_thread()

        th = QThread(self)
        wk = NavigationWorker(self.data_model.clone_for_worker(), seq, bucket,
                              prefix, max_items=self.listing_limit())
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.counted.connect(self._on_navigation_count)
        wk.finished.connect(self._on_navigation_finished)
        wk.finished.connect(th.quit)
        release_worker_on_finish(th, wk)
        th.finished.connect(th.deleteLater)

        self._nav_thread = th
        self._nav_worker = wk
        th.start()

    def tab_locations(self) -> list:
        """The (bucket, prefix) each tab points at, in tab order."""
        return [(entry["bucket"], entry["prefix"]) for entry in self._tabs]

    @staticmethod
    def tab_label(bucket: str, prefix: str) -> str:
        """
        A tab's caption: the deepest name that identifies the location.

        The whole prefix would not fit and the bucket alone cannot tell two
        folders apart, so the last path segment wins and the bucket is the
        fallback.
        """
        if not bucket:
            return "buckets"
        trimmed = (prefix or "").strip("/")
        if not trimmed:
            return bucket
        return trimmed.rsplit("/", 1)[-1]

    def _sync_tab_bar(self):
        """Rebuild the captions and show the bar only when it earns its row."""
        self._tab_switching = True
        try:
            while self.tabbar.count() > len(self._tabs):
                self.tabbar.removeTab(self.tabbar.count() - 1)
            for index, entry in enumerate(self._tabs):
                label = self.tab_label(entry["bucket"], entry["prefix"])
                location = ("s3://%s/%s" % (entry["bucket"], entry["prefix"])
                            if entry["bucket"] else "All buckets")
                if index < self.tabbar.count():
                    self.tabbar.setTabText(index, label)
                else:
                    self.tabbar.addTab(label)
                self.tabbar.setTabToolTip(index, location)
        finally:
            self._tab_switching = False
        self.tabbar.setVisible(len(self._tabs) > 1)

    def _current_location(self) -> dict:
        return self._new_tab_entry(self.data_model.bucket or "",
                                   self.data_model.current_folder or "")

    @staticmethod
    def _new_tab_entry(bucket="", prefix="") -> dict:
        """A tab: where it points, plus the trail it took to get there."""
        return {"bucket": bucket, "prefix": prefix,
                "history": [(bucket, prefix)], "pos": 0}

    @staticmethod
    def _entry_location(entry) -> tuple:
        return (entry.get("bucket", ""), entry.get("prefix", ""))

    def current_tab_entry(self):
        index = self.tabbar.currentIndex()
        if 0 <= index < len(self._tabs):
            return self._tabs[index]
        return None

    @classmethod
    def push_history(cls, entry, location):
        """
        Record a move in a tab's trail.

        A move made after going back truncates the forward branch, the way a
        browser does — otherwise "forward" would offer a route the user has
        already left.
        """
        history = entry.setdefault("history", [])
        position = int(entry.get("pos", len(history) - 1))
        if history and 0 <= position < len(history) \
                and history[position] == location:
            return
        del history[position + 1:]
        history.append(location)
        if len(history) > cls.HISTORY_DEPTH:
            del history[:len(history) - cls.HISTORY_DEPTH]
        entry["pos"] = len(history) - 1

    def can_go_back(self) -> bool:
        entry = self.current_tab_entry()
        return bool(entry) and int(entry.get("pos", 0)) > 0

    def can_go_forward(self) -> bool:
        entry = self.current_tab_entry()
        if not entry:
            return False
        return int(entry.get("pos", 0)) < len(entry.get("history", [])) - 1

    def _step_history(self, delta):
        entry = self.current_tab_entry()
        if not entry:
            return
        history = entry.get("history") or []
        position = int(entry.get("pos", 0)) + int(delta)
        if not (0 <= position < len(history)):
            return
        entry["pos"] = position
        bucket, prefix = history[position]
        # The move itself must not be recorded, or back would bounce between
        # two entries forever.
        self._history_navigating = True
        self.open_location(bucket, prefix)
        self._update_history_buttons()

    def goBack(self):
        """Back to the previous location in this tab's trail (Alt+Left)."""
        self._step_history(-1)

    def goForward(self):
        """Forward again after a Back (Alt+Right)."""
        self._step_history(1)

    def _update_history_buttons(self):
        for name, enabled in (("btnBack", self.can_go_back()),
                              ("btnForward", self.can_go_forward())):
            action = getattr(self, name, None)
            if action is not None:
                action.setEnabled(enabled)

    def _remember_current_tab(self):
        """Store where the view is now on the tab that is showing it."""
        if self._tab_switching or not self._tabs:
            return
        here = (self.data_model.bucket or "",
                self.data_model.current_folder or "")
        if self._tab_restore_pending:
            self._tab_restore_pending = False
            for position, entry in enumerate(self._tabs):
                if self._entry_location(entry) == here:
                    self._tab_switching = True
                    try:
                        self.tabbar.setCurrentIndex(position)
                    finally:
                        self._tab_switching = False
                    self._update_history_buttons()
                    return
        index = self.tabbar.currentIndex()
        if 0 <= index < len(self._tabs):
            entry = self._tabs[index]
            entry["bucket"], entry["prefix"] = here
            if self._history_navigating:
                self._history_navigating = False
            else:
                self.push_history(entry, here)
            self._sync_tab_bar()
            self._save_tabs()
        self._update_history_buttons()

    def new_tab(self):
        """Open another tab on the current location."""
        if not self._tabs:
            self._tabs = [self._current_location()]
        self._tabs.insert(self.tabbar.currentIndex() + 1,
                          self._current_location())
        self._sync_tab_bar()
        self._tab_switching = True
        try:
            self.tabbar.setCurrentIndex(self.tabbar.currentIndex() + 1)
        finally:
            self._tab_switching = False
        self._save_tabs()
        self.statusBar().showMessage("New tab", 2000)

    def close_tab(self, index=None):
        """Close a tab; the last one stays, since the view needs a location."""
        if len(self._tabs) <= 1:
            self.statusBar().showMessage("The last tab stays open", 2000)
            return
        index = self.tabbar.currentIndex() if index is None else int(index)
        if not (0 <= index < len(self._tabs)):
            return
        del self._tabs[index]
        self._tab_switching = True
        try:
            self.tabbar.removeTab(index)
        finally:
            self._tab_switching = False
        self._sync_tab_bar()
        self._save_tabs()
        self._goto_tab(self.tabbar.currentIndex())

    def close_other_tabs(self, index=None):
        index = self.tabbar.currentIndex() if index is None else int(index)
        if not (0 <= index < len(self._tabs)) or len(self._tabs) <= 1:
            return
        self._tabs = [self._tabs[index]]
        self._tab_switching = True
        try:
            while self.tabbar.count() > 1:
                self.tabbar.removeTab(self.tabbar.count() - 1)
            self.tabbar.setCurrentIndex(0)
        finally:
            self._tab_switching = False
        self._sync_tab_bar()
        self._save_tabs()

    def open_in_new_tab(self, bucket, prefix):
        """Add a tab for another location and switch to it."""
        if not self._tabs:
            self._tabs = [self._current_location()]
        self._tabs.insert(self.tabbar.currentIndex() + 1,
                          self._new_tab_entry(bucket or "", prefix or ""))
        self._sync_tab_bar()
        self._save_tabs()
        self.tabbar.setCurrentIndex(self.tabbar.currentIndex() + 1)

    def open_selection_in_new_tab(self):
        """Open the selected bucket or folder in another tab."""
        _item, name, kind = self.get_row_primary_item(
            self.listview.currentIndex())
        if not name or name == UP_ENTRY_LABEL:
            return
        if self.in_bucket_list_mode():
            self.open_in_new_tab(name, "")
            return
        if kind != FSObjectType.FOLDER:
            self.statusBar().showMessage("Only folders open in a tab", 2000)
            return
        self.open_in_new_tab(
            self.data_model.bucket,
            (self.data_model.current_folder or "") + name + "/")

    def _tab_menu(self, pos):
        index = self.tabbar.tabAt(pos)
        menu = QMenu(self)
        act_new = menu.addAction("New tab")
        act_dup = menu.addAction("Duplicate tab")
        menu.addSeparator()
        act_close = menu.addAction("Close tab")
        act_others = menu.addAction("Close other tabs")
        act_close.setEnabled(len(self._tabs) > 1)
        act_others.setEnabled(len(self._tabs) > 1)
        chosen = menu.exec(self.tabbar.mapToGlobal(pos))
        if chosen is None:
            return
        if chosen is act_new or chosen is act_dup:
            self.new_tab()
        elif chosen is act_close:
            self.close_tab(index if index >= 0 else None)
        elif chosen is act_others:
            self.close_other_tabs(index if index >= 0 else None)

    def next_tab(self, step=1):
        if len(self._tabs) <= 1:
            return
        count = len(self._tabs)
        self.tabbar.setCurrentIndex(
            (self.tabbar.currentIndex() + int(step)) % count)

    def _on_tab_moved(self, source, target):
        if not (0 <= source < len(self._tabs)) or not (
                0 <= target < len(self._tabs)):
            return
        self._tabs.insert(target, self._tabs.pop(source))
        self._save_tabs()

    def _on_tab_changed(self, index):
        if self._tab_switching:
            return
        self._goto_tab(index)

    def _goto_tab(self, index):
        """Navigate the shared view to what a tab remembers."""
        if not (0 <= index < len(self._tabs)):
            return
        self._update_history_buttons()
        entry = self._tabs[index]
        if (entry["bucket"] == (self.data_model.bucket or "")
                and entry["prefix"] == (self.data_model.current_folder or "")):
            return
        self.open_location(entry["bucket"], entry["prefix"])

    def open_location(self, bucket, prefix):
        """Go to a bucket/prefix, entering the bucket first when it differs."""
        if self.transfers_active():
            self.statusBar().showMessage(
                "Transfers active — navigation is disabled", 2000)
            return
        if not bucket:
            self._return_to_bucket_list_mode()
            self.navigate()
            return
        if bucket == (self.data_model.bucket or ""):
            self.change_current_folder(prefix or "")
            self.navigate(select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix or "")

    def _tabs_settings_key(self) -> str:
        return f"tabs/{self.profile_name}"

    def _save_tabs(self):
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._tabs_settings_key(),
            json.dumps([[e["bucket"], e["prefix"]] for e in self._tabs]))
        self.settings.endGroup()

    def _load_tabs(self):
        """
        Restore this profile's tabs, falling back to a single one.

        A stored tab is only a location, so a bucket that has since been
        deleted costs one failed navigation, not a broken window.
        """
        self.settings.beginGroup("common")
        raw = self.settings.value(self._tabs_settings_key(), "")
        self.settings.endGroup()
        entries = []
        try:
            for row in json.loads(raw or "[]"):
                if isinstance(row, (list, tuple)) and len(row) == 2:
                    # A trail is session state; only the location is restored.
                    entries.append(self._new_tab_entry(
                        str(row[0] or ""), str(row[1] or "")))
        except (ValueError, TypeError):
            entries = []
        self._tabs = entries or [self._current_location()]
        # The window opens wherever the remembered last location says, which
        # is not necessarily tab 0. The first navigation selects the matching
        # tab instead of overwriting whichever one happens to be current.
        self._tab_restore_pending = True
        self._sync_tab_bar()

    def _set_listing_truncated(self, truncated: bool, shown: int):
        if not truncated:
            self.listing_notice.hide()
            return
        self.listing_notice.setText(
            f"Showing the first {shown:,} entries of a larger prefix — raise "
            "the listing limit in Transfer settings, or use Search "
            "(Ctrl+Shift+F) to find what you need.")
        self.listing_notice.show()
        self.log(f"listing capped at {shown} entries")

    def listing_limit(self) -> int:
        """How many entries one listing may fetch; 0 means no cap."""
        self.settings.beginGroup("common")
        raw = self.settings.value("listing_limit", DEFAULT_LISTING_LIMIT)
        self.settings.endGroup()
        try:
            return max(0, int(raw))
        except (TypeError, ValueError):
            return DEFAULT_LISTING_LIMIT

    @pyqtSlot(int, int)
    def _on_navigation_count(self, seq: int, count: int):
        """Running count while a large prefix is still being listed."""
        if seq != self._nav_seq:
            return
        self.statusBar().showMessage(f"Loading… {count:,} entries", 0)

    @pyqtSlot(int, object, str)
    def _on_navigation_finished(self, seq: int, payload: object, err_str: str):
        if seq != self._nav_seq:
            return

        self._hide_loading()
        self.listview.setEnabled(True)

        try:
            self.activateWindow()
            self.raise_()
        except Exception:
            pass
        try:
            self.listview.setFocus(Qt.FocusReason.OtherFocusReason)
        except Exception:
            pass

        if err_str:
            self.statusBar().showMessage(f"Navigation error: {err_str}", 8000)
            self.log(f"Navigation error: {err_str}")
            return

        # Adopt any region/endpoint promotion the worker discovered on its
        # private client clone, so the next operation on the shared model
        # starts from the same adapted state.
        if isinstance(payload, dict):
            promoted = payload.get("promoted")
            if promoted:
                changed = (
                    self.data_model.endpoint_url != promoted.get("endpoint_url")
                    or self.data_model.region_name != promoted.get("region_name")
                    or self.data_model.use_path != promoted.get("use_path")
                )
                if changed:
                    self.data_model.endpoint_url = promoted.get("endpoint_url")
                    self.data_model.region_name = promoted.get("region_name")
                    self.data_model.use_path = promoted.get("use_path")
                    self.data_model._client = None

        mode = payload.get("mode") if isinstance(payload, dict) else None

        if mode == "bucket_list":
            buckets = payload.get("buckets") or []
            self.modelToListView_bucket_mode(buckets)
            self.listview.setSortingEnabled(True)
            self.listview.sortByColumn(0, Qt.SortOrder.AscendingOrder)
            self.statusBar().showMessage(
                "[%s][all buckets] — %d bucket(s)"
                % (self.profile_name, len(buckets)), 0)
            self._remember_current_tab()
            self.update_s3_path_label()

            if self._nav_pending_restore_name and self._select_by_name(self._nav_pending_restore_name):
                self.enable_action_buttons()
                return

            if getattr(self, "_disable_restore_last_bucket_once", False):
                self._disable_restore_last_bucket_once = False
                self.select_first()
                self.enable_action_buttons()
                return

            if self._last_selected_bucket and self._select_by_name(self._last_selected_bucket):
                self.enable_action_buttons()
                return

            self.select_first()
            self.enable_action_buttons()
            return

        if mode == "bucket_items":
            items = payload.get("items") or []
            hdr = self.listview.header()
            sort_col = hdr.sortIndicatorSection()
            sort_order = hdr.sortIndicatorOrder()

            self.modelToListView(items)

            self.listview.setSortingEnabled(True)
            self.listview.sortByColumn(sort_col, sort_order)

            if not self.data_model.bucket:
                self.statusBar().showMessage(f"[{self.profile_name}][all buckets]", 0)
            else:
                show_folder = self.data_model.current_folder if self.data_model.current_folder else "/"
                self.statusBar().showMessage(
                    f"[{self.profile_name}][{self.data_model.bucket}] {show_folder}"
                    f" — {_listing_summary(items)}", 0)

            self._set_listing_truncated(
                bool(payload.get("truncated")), len(items))
            self._remember_current_tab()

            self.update_s3_path_label()

            if self._nav_pending_restore_name and self._select_by_name(self._nav_pending_restore_name):
                self.enable_action_buttons()
                return

            if self._nav_select_up_entry and self.select_up_entry():
                self.enable_action_buttons()
                return

            key = (self.data_model.bucket or "", self.data_model.current_folder or "")
            last = self._last_selected_in_prefix.get(key)
            if last and self._select_by_name(last):
                self.enable_action_buttons()
                return

            self.select_first()
            self.enable_action_buttons()
            return

    def _resolve_overwrites(self, job, conflicts, *, what, index_of):
        """
        Apply the user's overwrite choice to *job*.

        Returns the job to run (possibly filtered), or None to cancel.
        ``index_of(entry)`` yields the destination identifying an entry.
        """
        if not conflicts:
            return job
        dlg = OverwriteDialog(self, sorted(conflicts), total=len(job), what=what)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return None
        if dlg.choice() == OverwriteDialog.OVERWRITE:
            return job
        remaining = [e for e in job if index_of(e) not in conflicts]
        skipped = len(job) - len(remaining)
        if skipped:
            self.log(f"Skipped {skipped} existing {what}(s)")
        if not remaining:
            self.statusBar().showMessage("Nothing left to do", 2000)
            return None
        return remaining

    def _run_with_progress(self, title, fn):
        """Run fn(worker) on a QThread behind a modal busy dialog."""
        return run_with_progress(self, title, fn)

    def download(self):
        if self.in_bucket_list_mode():
            return
        job = []
        folder_path = QFileDialog.getExistingDirectory(self, "Select Folder")
        if not folder_path:
            return
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None:
                continue
            if name == UP_ENTRY_LABEL:
                continue
            key = self.data_model.current_folder + name
            if t == FSObjectType.FOLDER:
                # Trailing "/" so size accounting doesn't also match sibling
                # prefixes sharing the name (e.g. "photo" vs "photo-old").
                job.append((key + "/", None, None, folder_path))
                continue
            local_name = os.path.join(folder_path, name)
            job.append((key, local_name, primary_item.size, folder_path))

        # Local existence is free to check, so never clobber silently.
        conflicts = {
            entry[1] for entry in job
            if entry[1] and os.path.exists(entry[1])
        }
        # Folders need a listing to know what they would write, so they are
        # checked separately — without this a folder download overwrote a
        # local tree with no prompt at all.
        folder_hits = self._folder_download_conflicts(
            [entry for entry in job if not entry[1]])
        if folder_hits is None:
            return
        conflicts |= folder_hits
        job = self._resolve_overwrites(
            job, conflicts, what="file", index_of=self._download_target)
        if not job:
            return
        self.assign_thread_operation("download", job, need_refresh=False)

    def assign_thread_operation(self, method, job, need_refresh=True,
                                source_bucket="", dest_model=None,
                                source_model=None):
        if not job:
            return
        if not self._credentials_ok_for_transfer():
            self.log(f"{method} not started: credentials")
            return

        label = self._queue_build_label(method, job)
        entry = _QEntry(
            entry_id=self._queue_next_id,
            method=method,
            job=job,
            need_refresh=need_refresh,
            label=label,
            source_bucket=source_bucket,
            dest_model=dest_model,
            source_model=source_model,
        )
        self._queue_next_id += 1
        # Kept so a failed/cancelled row can be re-run from the queue panel.
        self._queue_entries[entry.entry_id] = entry

        self._queue_panel.add_entry(entry)

        if self.transfers_active():
            self._transfer_queue.append(entry)
            self.log(f"queued {method}: {label}")
            return

        self._start_transfer(entry)

    def _queue_build_label(self, method: str, job) -> str:
        n = len(job)
        verb = {
            "upload": "Upload", "download": "Download", "delete": "Delete",
            "copy": "Copy", "move": "Move",
            "restore": "Restore", "set_storage_class": "Set storage class",
            "delete_buckets": "Delete bucket",
            "empty_buckets": "Empty bucket",
            "sync": "Sync",
            "undelete": "Undo delete of",
            "set_tags": "Tag",
            "zip_download": "Zip",
            "copy_to_profile": "Copy to profile",
            "sync_to_profile": "Sync to profile",
        }.get(method, method.capitalize())
        return f"{verb} {n} item(s)"

    def _start_transfer(self, entry: '_QEntry'):
        method = entry.method
        job = entry.job
        need_refresh = entry.need_refresh

        self.log(f"starting {method}")
        entry.status = "running"
        self._queue_panel.update_status(entry)
        self._active_entry = entry

        self.thread = QThread(self)
        # Its own boto3 client, so a region rebind inside the transfer cannot
        # race whatever the main thread reads from the shared model. The
        # binding cache is still shared, so discoveries are not lost.
        self.worker = Worker(self._worker_model_for(entry), job,
                             dest_model=entry.dest_model)
        self.worker.moveToThread(self.thread)

        entry.thread = self.thread
        entry.worker = self.worker

        m = getattr(self.worker, method)
        self.thread.started.connect(m)

        self.worker.progress.connect(self.report_logger_progress)
        self.worker.error.connect(self._on_transfer_error)

        entry.error = None
        entry.error_details = ""
        entry.error_transient = False

        def _record_error(msg: str, _entry=entry):
            _entry.error = msg

        def _record_details(text: str, transient: bool, _entry=entry):
            _entry.error_details = text
            _entry.error_transient = bool(transient)

        self.worker.error.connect(_record_error)
        self.worker.details.connect(_record_details)

        def _transfer_ui_start(prefix_text: str):
            self.pb.reset()
            self.pb.setValue(0)
            self.pb.show()
            self._status_prefix = prefix_text
            self.status_text.setText("Preparing…")
            self._smooth_total = 1
            self._smooth_done = 0
            self._rate_samples = []
            self._smooth_rate_bps = 0.0
            self._last_tick_time = 0.0
            self._last_tick_bytes = 0
            self._tick_timer.start()
            self.worker.batch_progress.connect(self._on_batch_progress)

        def _transfer_ui_stop():
            try:
                self._on_tick()
            except Exception:
                pass
            try:
                self._tick_timer.stop()
            except Exception:
                pass
            self.pb.hide()

        if method == "download":
            _transfer_ui_start("Downloading…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "upload":
            _transfer_ui_start("Uploading…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "sync":
            _transfer_ui_start("Syncing…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method == "zip_download":
            _transfer_ui_start("Archiving…")
            self.thread.finished.connect(_transfer_ui_stop)

        if method in ("upload", "download", "sync", "zip_download"):
            eid = entry.entry_id

            def _on_queue_bytes(done, total, _eid=eid):
                self._queue_panel.update_byte_progress(_eid, done, total)

            self.worker.batch_progress.connect(_on_queue_bytes)

        def _reenable_after_thread():
            QTimer.singleShot(0, self.enable_action_buttons)

        def _clear_thread_refs():
            self.thread = None
            self.worker = None
            self._active_entry = None
            try:
                if getattr(self, "btnCancel", None) is not None:
                    self.btnCancel.setEnabled(False)
            except Exception:
                pass
            QTimer.singleShot(0, self._queue_start_next)
            _reenable_after_thread()

        def _on_worker_finished(cancelled: bool):
            if cancelled:
                self.log(f"{method} cancelled")
                entry.status = "cancelled"
            elif entry.error:
                # A failed job used to be reported as "done".
                self.log(f"{method} failed")
                if entry.error_details:
                    # One line so it stays greppable in the log view.
                    self.log(entry.error_details.replace("\n", " | "))
                entry.status = "error"
                self._maybe_auto_retry(entry)
            else:
                self.log(f"{method} completed")
                entry.status = "done"
            self._batch_stats[entry.status] = (
                self._batch_stats.get(entry.status, 0) + 1)
            self._queue_panel.update_status(entry)

            # _smooth_done only tracks byte-progress methods; for the rest it
            # still holds the previous transfer's total.
            if method in ("upload", "download", "sync", "zip_download"):
                self._record_history(entry, done_bytes=self._smooth_done)
            else:
                self._record_history(entry)

            if method == "delete" and entry.status == "done":
                self._record_undoable_delete(entry.job)

            if need_refresh:
                QTimer.singleShot(0, lambda: self.navigate(force=True))

        self.worker.finished.connect(_on_worker_finished)
        self.worker.finished.connect(self.thread.quit)
        release_worker_on_finish(self.thread, self.worker)
        self.thread.finished.connect(_clear_thread_refs)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(_reenable_after_thread)

        self.thread.start()
        self.disable_action_buttons()

    def _worker_model_for(self, entry: '_QEntry'):
        """
        The model clone a transfer runs against.

        Copy/move workers resolve CopySource — and move's delete pass —
        against their model's bucket, so a cross-bucket paste must run on a
        model bound to the SOURCE bucket. The clone inherits the destination
        bucket's endpoint binding (possibly a virtual-host of the wrong
        bucket), so navigation and connection state are reset to the profile
        root and the usual probing rebinds the source on first use.
        """
        if entry.source_model is not None:
            return entry.source_model
        model = self.data_model.clone_for_worker()
        if entry.source_bucket:
            model.bucket = entry.source_bucket
            model.current_folder = ""
            model.prev_folder = ""
            model.endpoint_url = model.profile_endpoint_url
            model.use_path = model.profile_use_path
            model.region_name = model.profile_region
            model._client = None
        return model

    def _queue_start_next(self):
        if self.transfers_active():
            return
        if not self._transfer_queue:
            # Queue drained — report the batch once, not once per job.
            self._notify_transfers_finished()
            return
        entry = self._transfer_queue.pop(0)
        self._start_transfer(entry)

    def _notify_transfers_finished(self):
        stats = self._batch_stats
        if not any(stats.values()):
            return
        self._batch_stats = {"done": 0, "cancelled": 0, "error": 0}
        title, body = format_completion_notification(stats)
        self.log(f"{title}: {body}")
        self.statusBar().showMessage(f"{title} — {body}", 6000)

        self.settings.beginGroup("common")
        enabled = self.settings.value("notify_on_complete", "true")
        self.settings.endGroup()
        if str(enabled).lower() not in ("true", "1"):
            return
        # A desktop notification only helps when the window is not in front.
        if self.isActiveWindow():
            return
        tray = self._ensure_tray_icon()
        if tray is None:
            return
        try:
            tray.showMessage(
                title, body, QSystemTrayIcon.MessageIcon.Information, 8000)
        except Exception:
            pass

    def _ensure_tray_icon(self):
        """A hidden tray icon used only to raise notifications."""
        if getattr(self, "_tray_icon", None) is not None:
            return self._tray_icon
        try:
            if not QSystemTrayIcon.isSystemTrayAvailable():
                return None
            icon = self.windowIcon()
            if icon.isNull():
                icon = QIcon(os.path.join(self.current_dir, "resources", "ducky.ico"))
            tray = QSystemTrayIcon(icon, self)
            tray.setToolTip(self.title)
            tray.show()
            self._tray_icon = tray
            return tray
        except Exception:
            return None

    def _on_queue_cancel_requested(self, entry_id: int):
        if self._active_entry is not None and self._active_entry.entry_id == entry_id:
            # Cancel only the running entry; queued entries keep waiting and
            # start once this one stops. (cancel_transfers aborts the whole
            # queue and stays on Esc / the toolbar Cancel button.)
            self.statusBar().showMessage("Canceling…", 2000)
            try:
                if self.worker is not None:
                    self.worker.cancel()
            except Exception:
                pass
            return
        for i, e in enumerate(self._transfer_queue):
            if e.entry_id == entry_id:
                e.status = "cancelled"
                self._transfer_queue.pop(i)
                self._queue_panel.update_status(e)
                return

    # Dropping onto a file manager downloads first; warn past this much.
    DRAG_WARN_BYTES = 256 * 1024 * 1024

    HISTORY_LIMIT = 200
    # Locations kept in one tab's back/forward trail.
    HISTORY_DEPTH = 100
    HISTORY_JOB_LIMIT = 100   # only small jobs are worth storing for re-run

    def load_transfer_history(self) -> list:
        self.settings.beginGroup("common")
        raw = self.settings.value("transfer_history", "") or ""
        self.settings.endGroup()
        try:
            entries = json.loads(raw) if raw else []
        except Exception:
            entries = []
        return entries if isinstance(entries, list) else []

    def _save_transfer_history(self, entries):
        self.settings.beginGroup("common")
        self.settings.setValue(
            "transfer_history", json.dumps(entries[:self.HISTORY_LIMIT]))
        self.settings.endGroup()

    def _record_history(self, entry, done_bytes=0):
        """Append a finished job to the persisted history, newest first."""
        job = entry.job or []
        record = {
            "when": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "method": entry.method,
            "label": entry.label,
            "items": len(job),
            "bytes": int(done_bytes or 0),
            "status": entry.status,
        }
        # Big jobs would bloat the settings file; those simply are not re-runnable.
        if len(job) <= self.HISTORY_JOB_LIMIT:
            try:
                record["job"] = json.loads(json.dumps(job))
            except (TypeError, ValueError):
                pass
            else:
                if entry.source_bucket:
                    record["source_bucket"] = entry.source_bucket
        entries = self.load_transfer_history()
        entries.insert(0, record)
        self._save_transfer_history(entries)

    def clear_transfer_history(self):
        self._save_transfer_history([])
        self.log("transfer history cleared")

    def show_transfer_history(self):
        TransferHistoryDialog(self, self, self.load_transfer_history()).exec()

    def rerun_history_entry(self, entry):
        job = entry.get("job") or []
        method = entry.get("method")
        if not job or not method:
            return
        if method in CROSS_PROFILE_METHODS:
            # History is JSON; another profile's live credentials were never in
            # it, so this could only queue a job destined to fail.
            QMessageBox.information(
                self, "Transfer history",
                f"'{method}' cannot be re-run from history because it needs "
                "the other profile's connection. Start it again from the "
                "Tools menu.")
            return
        # JSON turned the tuples into lists; the workers unpack either.
        self.log(f"re-running {method} from history ({len(job)} item(s))")
        self.assign_thread_operation(
            method,
            [tuple(i) if isinstance(i, list) else i for i in job],
            source_bucket=entry.get("source_bucket") or "",
        )

    def _live_actions(self) -> list:
        # Most QActions are created without a parent, so findChildren alone
        # would miss the whole toolbar.
        return list(self.tBar.actions()) + self.findChildren(QAction)

    def show_shortcuts(self):
        ShortcutsDialog(
            self,
            collect_shortcuts(self._live_actions(), LISTVIEW_KEY_HELP)).exec()

    def show_diagnostics(self):
        """Environment facts, gathered off the GUI thread."""
        def _collect(_worker):
            return diagnostics.collect(
                self.current_dir, version=__VERSION__,
                model=self.data_model, profile_name=self.profile_name)

        sections, exc = self._run_with_progress("Collecting diagnostics…",
                                                _collect)
        if exc is not None:
            QMessageBox.warning(self, "Diagnostics",
                                f"Could not collect diagnostics:\n{exc}")
            return
        if sections is None:
            return
        DiagnosticsDialog(self, diagnostics.format_report(sections)).exec()

    def show_quick_open(self):
        """Jump to a bookmark or bucket by name."""
        clone = self.data_model.clone_for_worker()

        def _fetch(_worker):
            # Off the GUI thread: reaching the endpoint from a slot is the
            # freeze this app has been stamping out everywhere else.
            return [item.name for item in clone.list_buckets()]

        buckets, exc = self._run_with_progress("Listing buckets…", _fetch)
        if buckets is None and exc is None:
            return  # cancelled
        if exc is not None:
            self.log(f"quick open: could not list buckets ({exc})")
            buckets = []

        entries = location_entries(
            buckets or [], self._bookmarks, self.data_model.bucket)
        if not entries:
            self.statusBar().showMessage("Nowhere to go yet", 2000)
            return
        dlg = QuickOpenDialog(self, entries)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        target = dlg.chosen_location()
        if target is None:
            return
        bucket, prefix = target
        # Same navigation the bookmark menu uses, including the same-bucket
        # shortcut that avoids a pointless re-entry.
        self.go_to_bookmark({"bucket": bucket, "prefix": prefix})

    def show_command_palette(self):
        """Type-to-run index of every action currently available."""
        dlg = CommandPaletteDialog(self, command_entries(self._live_actions()))
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        action = dlg.chosen_action()
        if action is not None:
            action.trigger()

    def _record_undoable_delete(self, keys):
        """
        Remember what a delete removed so it can be undone.

        On a versioning-enabled bucket the objects are not gone — S3 left a
        delete marker on top of them, and dropping that marker brings them
        back. Whether markers exist is only discovered when undo runs, so no
        extra request is made here.
        """
        keys = [k for k in (keys or []) if k]
        if not keys or not self.data_model.bucket:
            return
        self._undo_delete = {"bucket": self.data_model.bucket, "keys": keys}
        self.btnUndoDelete.setEnabled(not self.is_read_only())
        self.statusBar().showMessage(
            f"Deleted {len(keys)} item(s) — Ctrl+Z undoes this on a "
            "versioned bucket", 8000
        )

    def _clear_undo_delete(self):
        self._undo_delete = None
        try:
            self.btnUndoDelete.setEnabled(False)
        except Exception:
            pass

    def undo_delete(self):
        """Remove the delete markers left by the last delete in this bucket."""
        undo = self._undo_delete
        if not undo:
            self.statusBar().showMessage("Nothing to undo", 2000)
            return
        if undo["bucket"] != self.data_model.bucket:
            self._clear_undo_delete()
            self.statusBar().showMessage(
                "The undo applies to a different bucket", 3000)
            return
        keys = undo["keys"]
        if QMessageBox.question(
            self, "Undo delete",
            f"Attempt to restore {len(keys)} deleted item(s)?\n\n"
            "This works only if the bucket had versioning enabled when they "
            "were deleted; otherwise nothing can be recovered.",
        ) != QMessageBox.StandardButton.Yes:
            return
        self._clear_undo_delete()
        self.assign_thread_operation("undelete", [(k,) for k in keys])
        self.statusBar().showMessage(f"Restoring {len(keys)} item(s)…", 4000)

    def auto_retry_enabled(self) -> bool:
        self.settings.beginGroup("common")
        raw = self.settings.value("auto_retry", "true")
        self.settings.endGroup()
        return str(raw).lower() in ("true", "1")

    def _maybe_auto_retry(self, entry):
        """
        Re-queue a job the service asked us to try again, once.

        Only the service's own "try again" answers qualify, and only once:
        a job retried forever on a timer is a queue that never drains, and
        an AccessDenied retried is just a slower AccessDenied.
        """
        if not entry.error_transient or entry.auto_retries >= 1:
            return False
        if not self.auto_retry_enabled():
            return False
        entry.auto_retries += 1
        self.log(
            f"{entry.method} hit a transient failure; retrying once in "
            f"{AUTO_RETRY_DELAY_MS // 1000}s")
        # An owned timer, not QTimer.singleShot: a static one keeps firing
        # after the window is gone, and its callback would then start a
        # transfer against a torn-down model. A parented QTimer dies with us.
        self._pending_retries.append(entry)
        self._retry_timer.start(AUTO_RETRY_DELAY_MS)
        return True

    def _run_pending_retries(self):
        """Re-queue whatever the retry timer was holding."""
        pending, self._pending_retries = self._pending_retries, []
        if self._closing:
            return
        for entry in pending:
            self._requeue(entry, automatic=True)

    def _requeue(self, entry, automatic=False):
        """Queue the same job again as a fresh entry."""
        if automatic:
            self.log(f"retrying {entry.method}: {entry.label}")
        self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh,
            source_bucket=entry.source_bucket, dest_model=entry.dest_model,
            source_model=entry.source_model)

    def failed_entries(self) -> list:
        """Every queued job that ended in an error, oldest first."""
        return [entry for entry in
                sorted(self._queue_entries.values(),
                       key=lambda e: e.entry_id)
                if entry.status == "error"]

    def retry_failed_transfers(self):
        """Re-queue every failed job in one go."""
        failed = self.failed_entries()
        if not failed:
            self.statusBar().showMessage("Nothing has failed", 2000)
            return
        self.log(f"retrying {len(failed)} failed job(s)")
        for entry in failed:
            self._requeue(entry)
        self.statusBar().showMessage(
            f"Re-queued {len(failed)} failed job(s)", 4000)

    def _on_queue_retry_requested(self, entry_id: int):
        """Re-queue a failed or cancelled job as a fresh entry."""
        entry = self._queue_entries.get(entry_id)
        if entry is None or entry.status not in ("cancelled", "error"):
            return
        self.log(f"retrying {entry.method}: {entry.label}")
        # The cross-profile jobs carry live models; dropping them re-queued a
        # job the worker could only refuse.
        self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh,
            source_bucket=entry.source_bucket, dest_model=entry.dest_model,
            source_model=entry.source_model)

    def _toggle_queue_panel(self):
        if self._queue_panel.isVisible():
            self._queue_panel.hide()
        else:
            self._queue_panel.show()

    def new_folder(self):
        if self.in_bucket_list_mode():
            return
        name, ok = QInputDialog.getText(self, "Create folder", "Folder name")
        name = name.replace("/", "")
        if ok and name:
            key = self.data_model.current_folder + "%s/" % name
            try:
                self.data_model.create_folder(key, log_fn=self.log)
            except Exception as exc:
                self.log(f"Create folder failed: {exc}")
                QMessageBox.critical(
                    self,
                    "Create folder failed",
                    f"Cannot create folder '{name}': {exc}",
                )
                return
            self.log(f"Created folder {name} ({key})")

            self._nav_pending_restore_name = name
            self.navigate(force=True, restore_name=name)

    def new_bucket(self):
        bucket_name, ok = QInputDialog.getText(self, "Create bucket", "Bucket name")
        bucket_name = bucket_name.strip()
        if not ok or not bucket_name:
            return
        try:
            self.data_model.create_bucket(bucket_name)
            self.log(f"Created bucket {bucket_name}")

            # remember this new bucket as "last focused"
            self._last_selected_bucket = bucket_name

            self._nav_pending_restore_name = bucket_name
            self.navigate(force=True, restore_name=bucket_name)

        except Exception as exc:
            QMessageBox.critical(
                self,
                "Create bucket failed",
                f"Cannot create bucket '{bucket_name}': {exc}",
            )

    def delete_bucket_ui(self):
        if not self.in_bucket_list_mode():
            return
        bucket_names = []
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None:
                continue
            if t == FSObjectType.BUCKET:
                bucket_names.append(name)

        if not bucket_names:
            return

        # Custom confirm box with a checkbox
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Icon.Warning)
        box.setWindowTitle("Delete bucket(s)")
        box.setText("Are you sure you want to delete bucket(s):\n\n  %s" % ", ".join(bucket_names))

        cb = QCheckBox("Delete non-empty buckets (recursive delete all objects)")
        cb.setChecked(False)
        box.setCheckBox(cb)

        box.setInformativeText(
            "If unchecked, bucket must be EMPTY.\n"
            "If checked, ALL objects inside the bucket(s) will be deleted first."
        )
        box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        box.setDefaultButton(QMessageBox.StandardButton.No)

        ret = box.exec()
        if ret != QMessageBox.StandardButton.Yes:
            return

        recursive = cb.isChecked()

        if self._last_selected_bucket in bucket_names:
            self._last_selected_bucket = None

        # Emptying a bucket can take minutes; run it through the transfer
        # queue so the UI stays responsive and the job can be cancelled.
        self._return_to_bucket_list_mode()
        job = [(bname, recursive) for bname in bucket_names]
        self.assign_thread_operation("delete_buckets", job)
        self.statusBar().showMessage(
            f"Deleting {len(job)} bucket(s)…", 4000
        )

    def empty_bucket_ui(self):
        """Delete everything inside the selected bucket(s), keeping the bucket."""
        if not self.in_bucket_list_mode():
            return
        bucket_names = []
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None:
                continue
            if t == FSObjectType.BUCKET:
                bucket_names.append(name)
        if not bucket_names:
            return

        if QMessageBox.question(
            self, "Empty bucket(s)",
            "Delete ALL objects (including every version, delete marker and "
            "in-flight multipart upload) from:\n\n  %s\n\n"
            "The bucket itself is kept. This cannot be undone."
            % ", ".join(bucket_names),
        ) != QMessageBox.StandardButton.Yes:
            return

        job = [(bname,) for bname in bucket_names]
        self.assign_thread_operation("empty_buckets", job)
        self.statusBar().showMessage(f"Emptying {len(job)} bucket(s)…", 4000)

    def delete(self):
        if self.in_bucket_list_mode():
            self.delete_bucket_ui()
            return

        names = []
        job = []
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None:
                continue
            if name == UP_ENTRY_LABEL:
                continue
            key = self.data_model.current_folder + name
            if t == FSObjectType.FOLDER:
                key = key + "/"
            job.append(key)
            names.append(name)
        if not names:
            return

        # A folder row can hide any number of objects, so scan before asking.
        detail = ""
        if any(k.endswith("/") for k in job):
            clone = self.data_model.clone_for_worker()
            targets = list(job)

            def _scan(_w):
                count = 0
                total = 0
                for key in targets:
                    if key.endswith("/"):
                        for k, size in clone.get_keys(key):
                            if k and not k.endswith("/"):
                                count += 1
                                total += int(size or 0)
                    else:
                        count += 1
                return count, total

            result, exc = self._run_with_progress("Scanning selection…", _scan)
            if exc is not None:
                detail = f"\n\n(could not size the selection: {exc})"
            elif result is None:
                return  # cancelled
            else:
                count, total = result
                detail = (
                    f"\n\nThis removes {count} object(s), "
                    f"{_human_bytes(total)}."
                )

        qm = QMessageBox
        ret = qm.question(
            self,
            "Delete",
            "Are you sure to delete objects : %s ?%s" % (",".join(names), detail),
            qm.StandardButton.Yes | qm.StandardButton.No,
        )
        if ret == qm.StandardButton.Yes:
            self.assign_thread_operation("delete", job)

    def upload(self, folder=None):
        if self.in_bucket_list_mode():
            return
        job = []
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)
        names = dialog.getOpenFileNames(self, "Open files", "", "All files (*)")
        if not all(map(lambda x: x, names)):
            return
        for name in names[0]:
            basename = os.path.basename(name)
            key = (
                (folder.rstrip("/") + "/" + basename)
                if folder
                else (self.data_model.current_folder + basename)
            )
            job.append((key, name))
        job = self._guard_upload(job)
        if not job:
            return
        self.assign_thread_operation("upload", job)

    def upload_folder(self, folder=None):
        """Upload a whole local directory tree into the current folder (or
        'folder' when given), through the transfer queue. Mirrors what
        drag-and-dropping the directory onto the list does."""
        if self.in_bucket_list_mode():
            return
        path = QFileDialog.getExistingDirectory(self, "Select folder to upload")
        if not path:
            return
        # QAction.triggered passes its 'checked' bool; only a real string is
        # an explicit destination prefix.
        dest = folder if isinstance(folder, str) else self.data_model.current_folder
        job = _build_upload_job_for_path(path, dest)
        job = self._guard_upload(job)
        if not job:
            return
        self.assign_thread_operation("upload", job)

    def transfer_settings(self):
        """Concurrency plus the storage class / encryption applied to uploads."""
        self.settings.beginGroup("common")
        cur_class = self.settings.value("upload_storage_class", "") or ""
        cur_sse = self.settings.value("upload_sse", "") or ""
        cur_kms = self.settings.value("upload_kms_key", "") or ""
        cur_checksum = self.settings.value("upload_checksum", "") or ""
        cur_resumable = str(self.settings.value(
            "resumable_uploads", "true")).lower() in ("true", "1")
        cur_threshold = int(self.settings.value(
            "multipart_threshold_mb",
            DataModel.DEFAULT_MULTIPART_THRESHOLD_MB) or
            DataModel.DEFAULT_MULTIPART_THRESHOLD_MB)
        cur_chunk = int(self.settings.value(
            "multipart_chunksize_mb",
            DataModel.DEFAULT_MULTIPART_CHUNKSIZE_MB) or
            DataModel.DEFAULT_MULTIPART_CHUNKSIZE_MB)
        cur_notify = str(
            self.settings.value("notify_on_complete", "true")).lower() in ("true", "1")
        cur_detect_type = str(self.settings.value(
            "detect_content_type", "true")).lower() in ("true", "1")
        cur_type_overrides = DataModel.parse_content_type_overrides(
            self.settings.value("content_type_overrides", "") or "")
        self.settings.endGroup()
        cur_listing_limit = self.listing_limit()
        cur_auto_retry = self.auto_retry_enabled()
        self.settings.beginGroup("common")
        cur_upload_rules = DataModel.parse_upload_rules(
            self.settings.value("upload_rules", "") or "")
        cur_log_to_file = str(
            self.settings.value("log_to_file", "true")).lower() in ("true", "1")
        self.settings.endGroup()
        cur_files = getattr(self.data_model, "parallel_files",
                            DataModel.DEFAULT_PARALLEL_FILES)
        cur_verify = bool(getattr(self.data_model, "verify_downloads", False))
        self.settings.beginGroup("common")
        cur_rate = int(self.settings.value("rate_limit_kbps", 0) or 0)
        self.settings.endGroup()

        dlg = TransferSettingsDialog(
            self,
            concurrency=getattr(self.data_model, "transfer_concurrency",
                                DataModel.DEFAULT_TRANSFER_CONCURRENCY),
            max_concurrency=DataModel.MAX_TRANSFER_CONCURRENCY,
            storage_classes=("",) + tuple(DataModel.STORAGE_CLASSES),
            sse_modes=DataModel.SSE_MODES,
            storage_class=cur_class,
            sse=cur_sse,
            kms_key_id=cur_kms,
            notify=cur_notify,
            parallel_files=cur_files,
            max_parallel_files=DataModel.MAX_PARALLEL_FILES,
            verify_downloads=cur_verify,
            rate_limit_kbps=cur_rate,
            checksum_algorithm=cur_checksum,
            multipart_threshold_mb=cur_threshold,
            multipart_chunksize_mb=cur_chunk,
            resumable_uploads=cur_resumable,
            detect_content_type=cur_detect_type,
            content_type_overrides=cur_type_overrides,
            listing_limit=cur_listing_limit,
            auto_retry=cur_auto_retry,
            upload_rules=cur_upload_rules,
            log_to_file=cur_log_to_file,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        threshold_mb, chunk_mb = self.data_model.set_multipart_sizes(
            threshold_mb=dlg.multipart_threshold_mb(),
            chunksize_mb=dlg.multipart_chunksize_mb())
        applied = self.data_model.set_transfer_concurrency(dlg.concurrency())
        files = self.data_model.set_parallel_files(dlg.parallel_files())
        self.data_model.verify_downloads = dlg.verify_downloads()
        self.data_model.resumable_uploads = dlg.resumable_uploads()
        self.data_model.set_rate_limit(dlg.rate_limit_kbps() * 1024)
        extra = self.data_model.set_upload_options(
            storage_class=dlg.storage_class(),
            sse=dlg.sse(),
            kms_key_id=dlg.kms_key_id(),
            checksum_algorithm=dlg.checksum_algorithm(),
        )
        overrides = self.data_model.set_content_type_options(
            detect=dlg.detect_content_type(),
            overrides=dlg.content_type_overrides(),
        )
        self.settings.beginGroup("common")
        self.settings.setValue(
            "detect_content_type",
            "true" if dlg.detect_content_type() else "false")
        self.settings.setValue(
            "content_type_overrides",
            DataModel.format_content_type_overrides(overrides))
        self.settings.setValue("listing_limit", dlg.listing_limit())
        self.settings.setValue(
            "auto_retry", "true" if dlg.auto_retry() else "false")
        self.settings.setValue(
            "log_to_file", "true" if dlg.log_to_file() else "false")
        self.log_file = LogFile(
            LogFile.default_path() if dlg.log_to_file() else "")
        rules = self.data_model.set_upload_rules(dlg.upload_rules())
        self.settings.setValue(
            "upload_rules", DataModel.format_upload_rules(rules))
        self.settings.setValue("transfer_concurrency", applied)
        self.settings.setValue("parallel_files", files)
        self.settings.setValue(
            "verify_downloads", "true" if dlg.verify_downloads() else "false")
        self.settings.setValue("rate_limit_kbps", dlg.rate_limit_kbps())
        self.settings.setValue("upload_storage_class", dlg.storage_class())
        self.settings.setValue("upload_sse", dlg.sse())
        self.settings.setValue("upload_kms_key", dlg.kms_key_id())
        self.settings.setValue("upload_checksum", dlg.checksum_algorithm())
        self.settings.setValue(
            "resumable_uploads", "true" if dlg.resumable_uploads() else "false")
        self.settings.setValue("multipart_threshold_mb", threshold_mb)
        self.settings.setValue("multipart_chunksize_mb", chunk_mb)
        self.settings.setValue(
            "notify_on_complete", "true" if dlg.notify() else "false")
        self.settings.endGroup()
        self.log(
            f"Transfer settings: {files} file(s) at once, concurrency {applied}, upload extras "
            f"{extra or '(none)'} (applies to new transfers)"
        )
        self.statusBar().showMessage(f"Transfer concurrency: {applied}", 3000)

    def on_toolbar_create(self):
        if self.in_bucket_list_mode():
            self.new_bucket()
        else:
            self.new_folder()

    def on_toolbar_delete(self):
        if self.in_bucket_list_mode():
            self.delete_bucket_ui()
        else:
            self.delete()

    def cancel_transfers(self):
        for e in list(self._transfer_queue):
            e.status = "cancelled"
            self._queue_panel.update_status(e)
        self._transfer_queue.clear()

        if not self.transfers_active() or self.worker is None:
            return

        self.statusBar().showMessage("Canceling…", 2000)

        try:
            if self.worker is not None:
                self.worker.cancel()
        except Exception:
            pass

        try:
            if self.thread is not None:
                self.thread.requestInterruption()
        except Exception:
            pass
        try:
            self.btnCancel.setEnabled(False)
        except Exception:
            pass

    def enable_action_buttons(self):
        at_root = self.in_bucket_list_mode()
        active = self.transfers_active()
        writable = not self.is_read_only()

        self.btnSwitchProfile.setEnabled(not active)
        self.btnCreateFolder.setEnabled(not active and writable)
        self.btnRemove.setEnabled(writable)
        self.btnUpload.setEnabled(not at_root and writable)
        self.btnUploadFolder.setEnabled(not at_root and writable)
        self.btnDownload.setEnabled(not at_root)
        self.btnCancel.setEnabled(active)
        self.btnUndoDelete.setEnabled(bool(self._undo_delete) and writable)

        try:
            b, _p = self._usage_target_from_selection()
            self.btnBucketUsage.setEnabled(bool(b) and (not active))
        except Exception:
            self.btnBucketUsage.setEnabled(not active and (not at_root))

    def disable_action_buttons(self):
        self.btnCreateFolder.setEnabled(False)
        self.btnSwitchProfile.setEnabled(False)
        self.btnBucketUsage.setEnabled(False)
        self.btnCancel.setEnabled(self.transfers_active())

    def goUp(self):

        if not self.data_model.bucket:
            return

        if self.transfers_active():
            self.statusBar().showMessage("Transfers active — navigation is disabled", 2000)
            return

        self._clear_selection()

        was_sorting = self.listview.isSortingEnabled()
        self.listview.setSortingEnabled(False)
        self.listview.setUpdatesEnabled(False)
        try:
            if not self.data_model.current_folder:
                self._return_to_bucket_list_mode()
                self.navigate()

                ix = self.listview.currentIndex()
                if not ix.isValid() and self.proxy.rowCount() > 0:
                    ix = self.proxy.index(0, 0)

                QTimer.singleShot(0, lambda ix=QModelIndex(ix): self._normalize_selection_to_index(ix))
                return

            p = self.data_model.current_folder
            leaving = p.rstrip("/").split("/")[-1] if p else ""
            new_path_list = p.split("/")[:-2]
            new_path = "/".join(new_path_list)
            if new_path:
                new_path = new_path + "/"

            self.change_current_folder(new_path)
            self.navigate(restore_name=leaving)

            ix = self.listview.currentIndex()
            if not ix.isValid() and self.proxy.rowCount() > 0:
                ix = self.proxy.index(0, 0)

            QTimer.singleShot(0, lambda ix=QModelIndex(ix): self._normalize_selection_to_index(ix))

        finally:
            self.listview.setUpdatesEnabled(True)
            self.listview.setSortingEnabled(was_sorting)

    def goHome(self):
        if self.transfers_active():
            self.statusBar().showMessage("Transfers active — navigation is disabled", 2000)
            return
        self._return_to_bucket_list_mode()
        self.navigate()

    def report_logger_progress(self, msg):
        self.log(msg)

    @pyqtSlot(str)
    def _on_transfer_error(self, msg: str):
        self.statusBar().showMessage(f"Transfer failed: {msg}", 6000)
        QMessageBox.critical(self, "Transfer failed", msg)

    def current_s3_path(self) -> str:
        if not self.data_model.bucket:
            return "s3://"
        prefix = self.data_model.current_folder or ""
        return f"s3://{self.data_model.bucket}/{prefix}"

    def update_s3_path_label(self):
        self.breadcrumb.set_location(
            self.data_model.bucket or "",
            self.data_model.current_folder or "",
            self.in_bucket_list_mode(),
        )
        self.breadcrumb.setToolTip(self.current_s3_path())

    def _breadcrumb_go(self, prefix: str):
        if self.in_bucket_list_mode():
            return
        if self.transfers_active():
            self.statusBar().showMessage(
                "Transfers active — navigation is disabled", 2000
            )
            return
        self.change_current_folder(prefix or "")
        self.navigate(select_up_entry=(not prefix))

    def copy_s3_path_to_clipboard(self):
        self.clip.setText(self.current_s3_path())
        self.statusBar().showMessage("S3 path copied", 2000)

    def _build_theme_button(self):
        self.themeButton = QToolButton()
        self.themeButton.setIcon(
            themed_icon("preferences-desktop-theme", os.path.join(self.current_dir, "icons", "theme_24px.svg"))
        )
        self.themeButton.setIconSize(QSize(26, 26))
        self.themeButton.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.themeButton.setToolTip("Appearance / theme")
        self.themeButton.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup
        )
        menu = QMenu(self.themeButton)
        self._theme_group = QActionGroup(self)
        self._theme_group.setExclusive(True)
        labels = {"system": "System default", "light": "Light", "dark": "Dark"}
        self.settings.beginGroup("common")
        saved = self.settings.value("theme", "system") or "system"
        self.settings.endGroup()
        if saved not in THEMES:
            saved = "system"
        for name in THEMES:
            act = QAction(labels[name], self, checkable=True)
            act.setData(name)
            act.setChecked(name == saved)
            act.triggered.connect(lambda _c=False, n=name: self._set_theme(n))
            self._theme_group.addAction(act)
            menu.addAction(act)
        self.themeButton.setMenu(menu)
        self.tBar.addWidget(self.themeButton)

    def _set_theme(self, name: str):
        applied = apply_theme(QApplication.instance(), name)
        self.settings.beginGroup("common")
        self.settings.setValue("theme", applied)
        self.settings.endGroup()
        for act in self._theme_group.actions():
            act.setChecked(act.data() == applied)
        self.statusBar().showMessage(f"Theme: {applied}", 2000)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self.update_s3_path_label()

    def createActions(self):
        # NOTE: plain letters are reserved for type-to-search in the list view,
        # so all letter shortcuts here carry a modifier.
        self.btnBack = QAction(
            themed_icon("go-previous", os.path.join(self.current_dir, "icons", "arrow_back_24px.svg")),
            "Back (Alt+Left)",
            triggered=self.goBack,
        )
        self.btnBack.setShortcut(QKeySequence("Alt+Left"))
        self.btnBack.setEnabled(False)
        self.btnForward = QAction(
            forward_icon(self.btnBack.icon()),
            "Forward (Alt+Right)",
            triggered=self.goForward,
        )
        self.btnForward.setShortcut(QKeySequence("Alt+Right"))
        self.btnForward.setEnabled(False)
        self.btnUp = QAction(
            themed_icon("go-up", os.path.join(self.current_dir, "icons", "arrow_upward_24px.svg")),
            "Up (Backspace, Alt+Up)",
            triggered=self.goUp,
        )
        self.btnUp.setShortcut(QKeySequence("Alt+Up"))
        self.btnHome = QAction(
            themed_icon("go-home", os.path.join(self.current_dir, "icons", "home_24px.svg")),
            "Home (Home, Alt+Home)",
            triggered=self.goHome,
        )
        self.btnHome.setShortcut(QKeySequence("Alt+Home"))
        self.btnDownload = QAction(
            themed_icon("emblem-downloads", os.path.join(self.current_dir, "icons", "download_24px.svg")),
            "Download (Ctrl+D)",
            triggered=self.download,
        )
        self.btnDownload.setShortcut(QKeySequence("Ctrl+D"))
        self.btnCreateFolder = QAction(
            themed_icon("folder-new", os.path.join(self.current_dir, "icons", "create_new_folder_24px.svg")),
            # dynamic: create bucket (root) OR create folder (inside bucket)
            "Create (Insert, Ctrl+N)",
            triggered=self.on_toolbar_create,
        )
        self.btnCreateFolder.setShortcut(QKeySequence("Ctrl+N"))
        self.btnRemove = QAction(
            themed_icon("edit-delete", os.path.join(self.current_dir, "icons", "delete_24px.svg")),
            # dynamic: delete bucket(s) or delete object(s)
            "Delete (Del)",
            triggered=self.on_toolbar_delete,
        )
        self.btnRefresh = QAction(
            themed_icon("view-refresh", os.path.join(self.current_dir, "icons", "refresh_24px.svg")),
            "Refresh (F5, Ctrl+R)",
            # QAction.triggered passes its 'checked' bool to the first optional
            # parameter, which here would land in navigate(restore_name=...).
            triggered=lambda: self.navigate(),
        )
        self.btnRefresh.setShortcuts([QKeySequence("F5"), QKeySequence("Ctrl+R")])
        self.btnUpload = QAction(
            themed_icon("network-server", os.path.join(self.current_dir, "icons", "file_upload_24px.svg")),
            "Upload (Ctrl+U)",
            triggered=lambda: self.upload(),
        )
        self.btnUpload.setShortcut(QKeySequence("Ctrl+U"))
        self.btnUploadFolder = QAction(
            themed_icon("folder", os.path.join(self.current_dir, "icons", "folder_24px.svg")),
            "Upload folder (Ctrl+Shift+U)",
            triggered=lambda: self.upload_folder(),
        )
        self.btnUploadFolder.setShortcut(QKeySequence("Ctrl+Shift+U"))
        self.btnTransferSettings = QAction(
            themed_icon("preferences-system", os.path.join(self.current_dir, "icons", "settings_24px.svg")),
            "Transfer settings…",
            triggered=self.transfer_settings,
        )
        self.btnCancel = QAction(
            themed_icon("process-stop", os.path.join(self.current_dir, "icons", "cancel_24px.svg")),
            "Cancel (Esc)",
            triggered=self.cancel_transfers,
        )
        self.btnUndoDelete = QAction(
            themed_icon("edit-undo", os.path.join(self.current_dir, "icons", "arrow_back_24px.svg")),
            "Undo delete (Ctrl+Z) — versioned buckets only",
            triggered=lambda: self.undo_delete(),
        )
        self.btnUndoDelete.setShortcut(QKeySequence("Ctrl+Z"))
        self.btnUndoDelete.setEnabled(False)
        self.btnBucketUsage = QAction(
            themed_icon("view-statistics", os.path.join(self.current_dir, "icons", "pie_24px.svg")),
            "Bucket usage Σ (Ctrl+S)",
            triggered=self.request_bucket_usage,
        )
        self.btnBucketUsage.setShortcut(QKeySequence("Ctrl+S"))
        self.btnBucketUsage.setEnabled(False)
        self.btnCancel.setEnabled(False)
        self.btnAbout = QAction(
            themed_icon("help-about", os.path.join(self.current_dir, "icons", "info_24px.svg")),
            "About(F1)",
            triggered=self.about,
        )
        self.btnSwitchProfile = QAction(
            themed_icon("system-switch-user", os.path.join(self.current_dir, "icons", "account-switch_24px.svg")),
            "Switch profile…",
            triggered=self.switch_profile,
        )
        self.actCopyS3Path = QAction(
            themed_icon("edit-copy", os.path.join(self.current_dir, "icons", "copy_24px.svg")),
            "Copy S3 path",
            self,
        )
        self.actCopyS3Path.triggered.connect(self.copy_s3_path_to_clipboard)

        self.actGoToLocation = QAction(
            themed_icon("go-jump", os.path.join(self.current_dir, "icons", "arrow_back_24px.svg")),
            "Go to location… (Ctrl+L)",
            self,
        )
        self.actGoToLocation.setShortcut(QKeySequence("Ctrl+L"))
        self.actGoToLocation.triggered.connect(lambda: self.goto_location())

        self.btnQueuePanel = QAction(
            themed_icon("format-justify-fill", os.path.join(self.current_dir, "icons", "queue_24px.svg")),
            "Transfer Queue (Ctrl+Q)",
            self,
        )
        self.btnQueuePanel.setShortcut(QKeySequence("Ctrl+Q"))
        self.btnQueuePanel.triggered.connect(self._toggle_queue_panel)

    def _restore_view_state(self):
        """Splitter position, column widths and sort order from last session."""
        self.settings.beginGroup("view")
        state = self.settings.value("splitter")
        widths = self.settings.value("columns")
        hidden = self.settings.value("visible_columns")
        sort_col = self.settings.value("sort_column")
        sort_order = self.settings.value("sort_order")
        self.settings.endGroup()

        if state is not None:
            try:
                self.splitter.restoreState(state)
            except Exception:
                pass
        if widths:
            header = self.listview.header()
            for i, raw in enumerate(list(widths)[:len(LIST_COLUMNS)]):
                try:
                    width = int(raw)
                except (TypeError, ValueError):
                    continue
                if width > 0:
                    header.resizeSection(i, width)
        if hidden is not None:
            shown = {str(v) for v in (hidden or [])}
            for index in LIST_OPTIONAL_COLUMNS:
                self.listview.setColumnHidden(index, str(index) not in shown)
        try:
            if sort_col is not None:
                order = (Qt.SortOrder.DescendingOrder
                         if str(sort_order) == "1"
                         else Qt.SortOrder.AscendingOrder)
                self.listview.sortByColumn(int(sort_col), order)
        except Exception:
            pass

    def _save_view_state(self):
        header = self.listview.header()
        self.settings.beginGroup("view")
        self.settings.setValue("splitter", self.splitter.saveState())
        self.settings.setValue(
            "columns",
            [str(header.sectionSize(i)) for i in range(len(LIST_COLUMNS))])
        self.settings.setValue(
            "visible_columns",
            [str(i) for i in LIST_OPTIONAL_COLUMNS
             if not self.listview.isColumnHidden(i)],
        )
        self.settings.setValue("sort_column", header.sortIndicatorSection())
        self.settings.setValue(
            "sort_order",
            "1" if header.sortIndicatorOrder() == Qt.SortOrder.DescendingOrder
            else "0",
        )
        self.settings.endGroup()

    def restoreSettings(self):
        self.settings.beginGroup("geometry")
        if self.settings.contains("pos"):
            pos = self.settings.value("pos", QPoint(200, 200))
            self.move(pos)
        else:
            self.move(0, 26)
        if self.settings.contains("size"):
            size = self.settings.value("size", QSize(800, 600))
            self.resize(size)
        else:
            self.resize(800, 600)
        self.settings.endGroup()

    def closeEvent(self, e):
        self.writeSettings()
        self._shutdown_threads()
        # Downloaded payloads staged for previews and drag-out live here; they
        # have to outlive the operation, so exit is the first safe moment.
        self.temp_workspace.cleanup()
        self.second_pane.shutdown()
        e.accept()

    def _shutdown_threads(self):
        """
        Stop background threads before the window (and its data_model) are torn
        down. A worker still touching data_model after teardown can crash on
        exit. We signal cancellation, then quit()+wait() each QThread with a
        bounded timeout (a worker may be blocked inside an S3 call that quit()
        cannot interrupt; we don't want to hang the close indefinitely).
        """
        for e in list(getattr(self, "_transfer_queue", [])):
            try:
                e.status = "cancelled"
            except Exception:
                pass
        if hasattr(self, "_transfer_queue"):
            self._transfer_queue.clear()

        # Ask any active transfer worker to stop ASAP.
        try:
            if self.worker is not None:
                self.worker.cancel()
        except Exception:
            pass

        self._closing = True
        self._pending_retries = []
        try:
            self._retry_timer.stop()
        except Exception:
            pass

        # A watch keeps queueing work; stop the timer before the threads go.
        try:
            if self._watch_timer is not None:
                self._watch_timer.stop()
            self._watch = None
        except Exception:
            pass

        threads = [getattr(self, attr, None) for attr in (
            "thread",
            "_nav_thread",
            "_bucket_enter_thread",
            "_bucket_usage_thread",
            "_watch_thread",
        )]
        # Navigations superseded before they finished: the window is about to
        # take its children down with it, so these have to be joined too.
        threads.extend(getattr(self, "_nav_orphan_threads", ()))

        for th in threads:
            if th is None or not self._thread_is_running(th):
                continue
            try:
                th.quit()
                th.wait(3000)
            except Exception:
                pass

        # Everything is joined; let the pinned workers go while the
        # interpreter is still in an orderly state.
        reap_finished_workers()

    def writeSettings(self):
        self.settings.beginGroup("geometry")
        self.settings.setValue("pos", self.pos())
        self.settings.setValue("size", self.size())
        self.settings.endGroup()
        self._save_view_state()
        self._save_binding_cache()
        self._save_last_location()

    def _last_location_key(self) -> str:
        return f"last_location/{self.profile_name or 'default'}"

    def _save_last_location(self):
        """Remember where this profile was browsing, per profile."""
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._last_location_key(),
            serialize_location(self.data_model.bucket,
                               self.data_model.current_folder))
        self.settings.endGroup()

    def restore_last_location(self) -> str:
        """
        Reopen the stored location, if there is one.

        Returns the bucket it is opening, or "" when it stays on the bucket
        list. Entering a bucket makes S3 calls, so this goes through the same
        async path as a double-click — and a bucket that has since been
        deleted simply leaves the bucket list showing.
        """
        self.settings.beginGroup("common")
        stored = self.settings.value(self._last_location_key(), "") or ""
        self.settings.endGroup()
        bucket, prefix = parse_location(stored)
        if not bucket:
            return ""
        self.log(f"reopening {serialize_location(bucket, prefix)}")
        self.enter_bucket_async(bucket, target_prefix=prefix)
        return bucket

    def share_link(self, key: str):
        """Open the presigned-link dialog (download/upload, configurable expiry)."""
        if not key or key.endswith("/") or key.rstrip("/") == UP_ENTRY_LABEL:
            self.statusBar().showMessage("Share links are for files only", 2000)
            return
        PresignedLinkDialog(self, self.data_model, key).exec()

    def make_public_and_copy(self, key: str):
        """Try to make object public-read and copy the direct URL anyway."""
        if not key or key.rstrip("/") == UP_ENTRY_LABEL:
            return
        try:
            ok, reason = self.data_model.make_object_public(key)

            # Always copy the direct URL
            url = self.data_model.direct_object_url(key)
            QtWidgets.QApplication.clipboard().setText(url)

            if ok:
                self.statusBar().showMessage("Public URL copied", 3000)
            else:
                # Explain *why* rather than just surfacing the raw error.
                try:
                    summary = self.data_model.public_access_summary()
                except Exception:
                    summary = {"reasons": []}
                why = "\n\n".join(summary.get("reasons") or [])
                QMessageBox.warning(
                    self,
                    "Public link",
                    f"Could not change ACL.\n\n{reason}"
                    + (f"\n\n{why}" if why else "")
                    + "\n\nDirect URL copied anyway "
                    "(will work only if bucket/object is already public).",
                )
                self.statusBar().showMessage("Direct URL copied (ACL not changed)", 4000)

        except Exception as exc:
            QMessageBox.warning(self, "Public URL", str(exc))

    def _collect_selected_targets(self):
        """Return [(name, key, is_folder)] for the current selection, skipping
        the [..] up-entry. Folder keys carry a trailing '/'."""
        items = []
        sm = self.listview.selectionModel()
        if sm is None:
            return items
        for ix in sm.selectedIndexes():
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None or name == UP_ENTRY_LABEL:
                continue
            key = self.data_model.current_folder + name
            is_folder = (t == FSObjectType.FOLDER)
            if is_folder:
                key += "/"
            items.append((name, key, is_folder))
        return items

    def sync_to_profile(self):
        """Compare this prefix with one in another profile and run the plan."""
        if self.in_bucket_list_mode():
            self.statusBar().showMessage("Enter a bucket first", 2000)
            return
        dlg = CrossProfileSyncDialog(
            self, self, self.data_model, self.data_model.current_folder,
            self.settings, current_profile=self.profile_name)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        source_model, dest_model, job = dlg.plan()
        if not job or dest_model is None:
            self.statusBar().showMessage("Nothing to sync", 2000)
            return
        if getattr(dest_model, "read_only", False):
            QMessageBox.warning(
                self, "Sync to another profile",
                "The destination profile is read-only.")
            return
        # The source may be the OTHER profile (a pull), so the worker cannot
        # just use this window's model.
        self.assign_thread_operation(
            "sync_to_profile", job, need_refresh=True, dest_model=dest_model,
            source_model=source_model)
        self.statusBar().showMessage(f"Syncing {len(job)} item(s)…", 3000)

    def copy_to_profile(self):
        """Stream the selection into another profile's bucket."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets()
        if not items:
            return

        dlg = CrossProfileCopyDialog(
            self, self.settings, len(items), current_profile=self.profile_name)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        bucket = dlg.destination_bucket()
        if not bucket:
            QMessageBox.warning(
                self, "Copy to another profile",
                "Choose a destination bucket.")
            return
        try:
            profile = dlg.selected_profile()
        except Exception as exc:
            QMessageBox.critical(
                self, "Copy to another profile",
                f"Could not read that profile's credentials:\n{exc}")
            return
        if profile is None:
            return
        if profile.read_only:
            QMessageBox.warning(
                self, "Copy to another profile",
                f"Profile '{profile.name}' is read-only.")
            return

        dest_model = build_profile_model(profile, bucket)
        prefix = dlg.destination_prefix()
        job = []
        for name, src_key, is_folder in items:
            if is_folder:
                job.append((src_key, prefix + name.rstrip("/") + "/", True))
            else:
                job.append((src_key, prefix + name, False))

        self.assign_thread_operation(
            "copy_to_profile", job, need_refresh=False, dest_model=dest_model)
        self.statusBar().showMessage(
            f"Copying {len(job)} item(s) to {profile.name}/{bucket}…", 3000)

    def copy_move(self):
        if self.in_bucket_list_mode():
            return

        items = self._collect_selected_targets()
        if not items:
            return

        dlg = CopyMoveDialog(
            self,
            self.data_model,
            len(items),
            self.data_model.current_folder,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        dst_prefix = dlg.destination()
        if dst_prefix and not dst_prefix.endswith("/"):
            dst_prefix += "/"
        cross_bucket = dlg.is_cross_bucket()
        dst_bucket = dlg.destination_bucket() if cross_bucket else None

        job = []
        skipped = []
        for name, src_key, is_folder in items:
            if is_folder:
                folder_name = src_key.rstrip("/").split("/")[-1]
                dst_key = dst_prefix + folder_name + "/"
            else:
                dst_key = dst_prefix + name
            # Self-nesting only matters within one bucket; the same prefix in
            # a different bucket is a legitimate destination.
            if not cross_bucket and _dest_inside_source(src_key, dst_key, is_folder):
                skipped.append(name)
                continue
            job.append((src_key, dst_key, is_folder, dst_bucket))

        if skipped:
            self.log(
                "Skipped (destination equals or nests inside source): "
                + ", ".join(skipped)
            )

        if not job:
            self.statusBar().showMessage("Nothing to copy/move", 2000)
            return

        conflicts = self._destination_conflicts(
            [entry[1] for entry in job], dst_bucket)
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda e: e[1])
        if not job:
            return

        operation = "move" if dlg.is_move() else "copy"
        # A cross-bucket move leaves this listing unchanged only for a copy;
        # either way the source listing is refreshed by the queue.
        self.assign_thread_operation(operation, job)
        where = f" to {dst_bucket}" if cross_bucket else ""
        self.statusBar().showMessage(
            f"{'Moving' if operation == 'move' else 'Copying'} "
            f"{len(job)} item(s){where}…", 3000
        )

    def _guard_upload(self, job):
        """
        Apply overwrite protection to an upload job.

        Every other write path (download, copy/move, paste, rename) checked
        its destination; uploads did not, and silently replaced remote
        objects. Directory placeholder entries (local path None) only create a
        prefix, so they are never conflicts.
        """
        if not job:
            return None
        keys = [key for key, local in job if local is not None]
        conflicts = self._destination_conflicts(keys)
        if conflicts is None:
            return None
        job = self._resolve_overwrites(
            job, conflicts, what="object", index_of=lambda e: e[0])
        if not job:
            return None
        if not any(local is not None for _key, local in job):
            # Only folder markers left — nothing would actually be uploaded.
            self.statusBar().showMessage("Nothing left to upload", 2000)
            return None
        return job

    def _download_target(self, entry):
        """The local path an entry writes to — a file, or a folder's root."""
        key, local_name, _size, folder_path = entry
        if local_name:
            return local_name
        return os.path.join(
            folder_path, os.path.basename(str(key).rstrip("/")))

    def _folder_download_conflicts(self, folder_entries):
        """
        Local roots whose contents a folder download would overwrite.

        Reported per folder rather than per file: a folder is one job entry,
        so Skip can only drop the whole folder. Returns a set, or None if the
        scan failed or was cancelled and the caller should abort.
        """
        if not folder_entries:
            return set()
        clone = self.data_model.clone_for_worker()

        def _scan(_w):
            hits = set()
            for entry in folder_entries:
                key, _local, _size, folder_path = entry
                listed = list(clone.get_keys(prefix_of(key)))
                plan = plan_prefix_download(key, folder_path, listed)
                if any(os.path.exists(path) for _k, path, _s in plan.files):
                    hits.add(plan.base_dir)
            return hits

        result, exc = self._run_with_progress("Checking local folder…", _scan)
        if exc is not None:
            if QMessageBox.question(
                self, "Checking local folder",
                f"Could not check the destination for existing files:\n{exc}\n\n"
                "Continue anyway (existing files would be overwritten)?",
            ) != QMessageBox.StandardButton.Yes:
                return None
            return set()
        if result is None:
            return None  # cancelled
        return set(result)

    def _destination_conflicts(self, dst_keys, dst_bucket=None):
        """
        Which of dst_keys already exist. Uses one listing per destination
        prefix (not a HEAD per key) on a worker thread.

        Returns a set, or None if the lookup failed/was cancelled and the
        caller should abort.
        """
        keys = [k for k in dst_keys if k]
        if not keys:
            return set()
        clone = self.data_model.clone_for_worker()
        if dst_bucket:
            clone.bucket = dst_bucket
            clone._client = None

        def _scan(_w):
            return clone.existing_keys(keys)

        result, exc = self._run_with_progress("Checking destination…", _scan)
        if exc is not None:
            if QMessageBox.question(
                self, "Checking destination",
                f"Could not check the destination for existing objects:\n{exc}\n\n"
                "Continue anyway (existing objects would be overwritten)?",
            ) != QMessageBox.StandardButton.Yes:
                return None
            return set()
        if result is None:
            return None  # cancelled
        return set(result)

    def edit_tags(self, key: str):
        if not key or key.endswith("/") or key == UP_ENTRY_LABEL:
            self.statusBar().showMessage("Tags are only supported for files", 2000)
            return
        TagsDialog(self, self.data_model, key).exec()

    def open_or_preview(self, key: str):
        """Open the in-app preview for a single file object."""
        if not key or key.endswith("/") or key == UP_ENTRY_LABEL:
            return
        PreviewDialog(self, self.data_model, key).exec()

    def show_versions(self, key: str):
        """Open the object-version manager for a single file object."""
        if not key or key.endswith("/") or key == UP_ENTRY_LABEL:
            self.statusBar().showMessage("Versions are only available for files", 2000)
            return
        VersionsDialog(self, self, self.data_model, key).exec()

    def restore_from_glacier(self, items=None):
        """Initiate a Glacier / Deep Archive restore for the selection
        (files and/or whole folders), run through the transfer queue.

        ``items`` overrides the listing selection so search results can drive
        the same action."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets() if items is None else items
        if not items:
            self.statusBar().showMessage("Select object(s) to restore", 2000)
            return
        days, ok = QInputDialog.getInt(
            self, "Restore from Glacier",
            "Keep restored copy for (days):", 7, 1, 3650,
        )
        if not ok:
            return
        tiers = ["Standard", "Bulk", "Expedited"]
        tier, ok = QInputDialog.getItem(
            self, "Restore from Glacier", "Retrieval tier:", tiers, 0, False
        )
        if not ok:
            return
        job = [(key, is_folder, days, tier) for _n, key, is_folder in items]
        self.assign_thread_operation("restore", job, need_refresh=False)
        self.statusBar().showMessage(
            f"Restoring {len(job)} target(s) ({tier}, {days}d)…", 4000
        )

    def change_storage_class_ui(self, items=None):
        """Change the storage class of the selection (files and/or whole
        folders), run through the transfer queue."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets() if items is None else items
        if not items:
            self.statusBar().showMessage("Select object(s) to change", 2000)
            return
        classes = list(self.data_model.STORAGE_CLASSES)
        # Preselect the current class of the first concrete file, if any.
        current = "STANDARD"
        first_file = next((k for _n, k, isf in items if not isf), None)
        if first_file:
            try:
                resp = self.data_model.object_properties(first_file)
                if isinstance(resp, dict):
                    current = resp.get("StorageClass") or "STANDARD"
            except Exception:
                pass
        try:
            cur_idx = classes.index(current)
        except ValueError:
            cur_idx = 0
        cls, ok = QInputDialog.getItem(
            self, "Change storage class", "Storage class:", classes, cur_idx, False
        )
        if not ok:
            return
        job = [(key, is_folder, cls) for _n, key, is_folder in items]
        self.assign_thread_operation("set_storage_class", job, need_refresh=False)
        self.statusBar().showMessage(
            f"Setting storage class to {cls} on {len(job)} target(s)…", 4000
        )

    def download_keys(self, keys):
        """
        Download an explicit list of object keys, keeping their prefixes.

        Search results come from all over the bucket, so flattening them into
        one folder would silently overwrite same-named objects from different
        prefixes.
        """
        keys = [k for k in keys if k and not k.endswith("/")]
        if not keys:
            return
        folder_path = QFileDialog.getExistingDirectory(self, "Select Folder")
        if not folder_path:
            return
        job = []
        for key in keys:
            relative = key.replace("\\", "/").lstrip("/")
            local_name = os.path.join(folder_path, *relative.split("/"))
            job.append((key, local_name, None, folder_path))
        conflicts = {entry[1] for entry in job if os.path.exists(entry[1])}
        job = self._resolve_overwrites(
            job, conflicts, what="file", index_of=self._download_target)
        if not job:
            return
        for entry in job:
            try:
                os.makedirs(os.path.dirname(entry[1]), exist_ok=True)
            except OSError as exc:
                QMessageBox.critical(self, "Download", str(exc))
                return
        self.assign_thread_operation("download", job, need_refresh=False)
        self.statusBar().showMessage(
            f"Downloading {len(job)} object(s)…", 4000)

    def delete_keys(self, keys):
        """Delete an explicit list of object keys, after confirmation."""
        keys = [k for k in keys if k]
        if not keys:
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        sample = "\n".join(keys[:10])
        if len(keys) > 10:
            sample += f"\n… and {len(keys) - 10} more"
        answer = QMessageBox.question(
            self, "Delete objects",
            f"Delete {len(keys)} object(s) from {self.data_model.bucket}?"
            f"\n\n{sample}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        self.assign_thread_operation("delete", list(keys))
        self.statusBar().showMessage(f"Deleting {len(keys)} object(s)…", 4000)

    def build_problem_report(self, log_lines=200) -> str:
        """
        Everything worth attaching to a bug report, in one place.

        Assembled rather than asked for: nobody reconstructs a Qt plugin list
        and the last failure's request id from memory.
        """
        sections = [
            diagnostics.format_report(diagnostics.collect(
                self.current_dir, version=__VERSION__,
                model=self.data_model, profile_name=self.profile_name)),
        ]
        failures = [entry for entry in self._queue_entries.values()
                    if entry.status == "error" and entry.error_details]
        if failures:
            latest = max(failures, key=lambda entry: entry.entry_id)
            sections.append(
                "== Last failure ==\n"
                f"{latest.method}: {latest.label}\n{latest.error_details}")
        tail = self.log_file.tail(log_lines)
        if not tail:
            tail = "\n".join(
                self.logview.toPlainText().splitlines()[-log_lines:])
        sections.append(f"== Log (last {log_lines} lines) ==\n{tail}")
        return "\n\n".join(sections)

    def report_problem(self):
        """Show the problem report, ready to copy or save."""
        text = self.build_problem_report()
        dialog = QDialog(self)
        dialog.setWindowTitle("Report a problem")
        dialog.resize(820, 600)
        view = QPlainTextEdit(text)
        view.setReadOnly(True)
        view.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        note = QLabel(
            "Check this over before sharing it: it names your endpoint, "
            "region, buckets and file paths. It never contains your keys.")
        note.setWordWrap(True)
        copy_btn = QPushButton("Copy")
        save_btn = QPushButton("Save…")
        close_btn = QPushButton("Close")
        copy_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(view.toPlainText()))
        save_btn.clicked.connect(lambda: self._save_report(view.toPlainText()))
        close_btn.clicked.connect(dialog.reject)
        row = QHBoxLayout()
        row.addWidget(copy_btn)
        row.addWidget(save_btn)
        row.addStretch(1)
        row.addWidget(close_btn)
        layout = QVBoxLayout(dialog)
        layout.addWidget(view, 1)
        layout.addWidget(note)
        layout.addLayout(row)
        dialog.exec()

    def _save_report(self, text):
        path, _filter = QFileDialog.getSaveFileName(
            self, "Save report", "s3duck-report.txt",
            "Text (*.txt);;All files (*)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(text)
        except OSError as exc:
            QMessageBox.critical(self, "Save report", str(exc))
            return
        self.statusBar().showMessage(f"Report saved to {path}", 4000)

    def show_failure_details(self, entry_id):
        """
        What the service actually said about a failed job.

        The log only ever kept ``str(exc)``, so a report of "it failed" could
        never be traced back to a request on the provider's side.
        """
        entry = self._queue_entries.get(int(entry_id))
        if entry is None:
            return
        text = entry.error_details or entry.error or "No details were recorded."
        header = f"{entry.method}: {entry.label}"
        box = QMessageBox(self)
        box.setWindowTitle("Failure details")
        box.setIcon(QMessageBox.Icon.Warning)
        box.setText(header)
        box.setDetailedText(text)
        copy_btn = box.addButton("Copy", QMessageBox.ButtonRole.ActionRole)
        box.addButton(QMessageBox.StandardButton.Close)
        box.exec()
        if box.clickedButton() is copy_btn:
            QApplication.clipboard().setText(f"{header}\n{text}")
            self.statusBar().showMessage("Failure details copied", 3000)

    def credential_state(self):
        """This profile's credential lifetime as ``(state, label)``."""
        return expiry_state(getattr(self, "session_expires", ""))

    def can_refresh_credentials(self) -> bool:
        return bool(getattr(self, "credential_process", "")
                    or getattr(self, "aws_profile", ""))

    def _update_credential_status(self):
        """Keep the status-bar countdown in step with the clock."""
        state, label = self.credential_state()
        if state == "none":
            self.credential_status.hide()
            return
        if state == "expired":
            text, colour = "credentials expired", "#c62828"
        elif state == "soon":
            text, colour = f"credentials {label}", "#b26a00"
        else:
            text, colour = f"credentials {label}", ""
        self.credential_status.setText(text)
        self.credential_status.setStyleSheet(
            f"color: {colour};" if colour else "")
        tip = "Tools -> Refresh credentials" if self.can_refresh_credentials() \
            else "No credential process configured for this profile"
        self.credential_status.setToolTip(tip)
        self.credential_status.show()

    def _persist_refreshed_credentials(self, minted) -> bool:
        """
        Write freshly minted credentials onto this profile's stored row.

        Without this the refresh would last only until the window closed, and
        the launcher would still show the lapsed expiry.
        """
        try:
            crypto = require_crypto(resolve_credential_key(self.settings))
        except CredentialError:
            return False
        self.settings.beginGroup("profiles")
        count = self.settings.beginReadArray("profiles")
        index = -1
        for position in range(count):
            self.settings.setArrayIndex(position)
            if str(self.settings.value("name", "")) == str(self.profile_name):
                index = position
                break
        self.settings.endArray()
        if index < 0:
            self.settings.endGroup()
            return False
        self.settings.beginWriteArray("profiles", count)
        self.settings.setArrayIndex(index)
        self.settings.setValue(
            "access_key", crypto.encrypt(minted.get("access_key", "")))
        self.settings.setValue(
            "secret_key", crypto.encrypt(minted.get("secret_key", "")))
        self.settings.setValue(
            "session_token", crypto.encrypt(minted.get("session_token", "")))
        self.settings.setValue("session_expires", minted.get("expires", ""))
        self.settings.endArray()
        self.settings.endGroup()
        return True

    def _mint_credentials(self):
        """Fresh credentials from the credential process or ~/.aws."""
        if getattr(self, "credential_process", ""):
            command = self.credential_process

            def _mint(_worker):
                return run_credential_process(command)

            minted, exc = run_with_progress(
                self, "Refreshing credentials…", _mint)
            if minted is None and exc is None:
                return None
            if exc is not None:
                raise CredentialProcessError(str(exc))
            return minted
        name = getattr(self, "aws_profile", "")
        if not name:
            raise CredentialProcessError(
                "This profile has no credential process and was not imported "
                "from ~/.aws.")
        entry = load_aws_profiles().get(name)
        if not entry or not entry.get("access_key"):
            raise CredentialProcessError(
                f"Profile '{name}' has no usable credentials in ~/.aws.")
        return {
            "access_key": entry.get("access_key", ""),
            "secret_key": entry.get("secret_key", ""),
            "session_token": entry.get("session_token", ""),
            "expires": entry.get("expires", ""),
        }

    def refresh_credentials(self):
        """
        Replace this session's credentials without reconnecting the profile.

        The bucket bindings and the current listing survive, because only the
        keys change; the cached client is dropped so the next call builds one
        with them.
        """
        try:
            minted = self._mint_credentials()
        except CredentialProcessError as exc:
            QMessageBox.critical(self, "Refresh credentials", str(exc))
            return False
        if minted is None:
            return False
        self.data_model.access_key = minted.get("access_key", "")
        self.data_model.secret_key = minted.get("secret_key", "")
        self.data_model.session_token = minted.get("session_token", "")
        self.data_model._client = None
        self.session_expires = minted.get("expires", "")
        self._persist_refreshed_credentials(minted)
        self._update_credential_status()
        _state, label = self.credential_state()
        self.log(f"credentials refreshed ({label or 'no expiry reported'})")
        self.statusBar().showMessage("Credentials refreshed", 4000)
        return True

    def _credentials_ok_for_transfer(self) -> bool:
        """
        Guard the queue against credentials that will not outlive the job.

        A transfer that dies half way through an expired session leaves parts
        on the server and a confusing error, so this offers the refresh first.
        """
        state, label = self.credential_state()
        if state in ("none", "ok"):
            return True
        if self.can_refresh_credentials():
            question = (
                f"This profile's credentials {label or 'have expired'}. "
                "Refresh them before starting?")
            answer = QMessageBox.question(
                self, "Credentials", question,
                QMessageBox.StandardButton.Yes
                | QMessageBox.StandardButton.No
                | QMessageBox.StandardButton.Cancel)
            if answer == QMessageBox.StandardButton.Cancel:
                return False
            if answer == QMessageBox.StandardButton.Yes:
                return self.refresh_credentials()
            return True
        if state == "expired":
            answer = QMessageBox.question(
                self, "Credentials",
                "This profile's credentials have expired and cannot be "
                "refreshed from here. Start anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            return answer == QMessageBox.StandardButton.Yes
        self.log(f"warning: credentials {label}")
        return True

    def dual_pane_active(self) -> bool:
        return not self.second_pane.isHidden()

    def toggle_dual_pane(self):
        """Show or hide the second pane (F3)."""
        if self.dual_pane_active():
            self.second_pane.hide()
            self.statusBar().showMessage("Single pane", 2000)
            self.listview.setFocus(Qt.FocusReason.OtherFocusReason)
        else:
            if self.data_model.bucket:
                self.second_pane.set_remote(
                    self.data_model.bucket, self.data_model.current_folder or "")
            else:
                self.second_pane.refresh()
            self.second_pane.show()
            self.statusBar().showMessage(
                "Dual pane — F5 copies to the other pane, F6 moves", 4000)
        self._sync_pane_shortcuts()
        if getattr(self, "actDualPane", None) is not None:
            self.actDualPane.setText(
                "Single pane (F3)" if self.dual_pane_active()
                else "Dual pane (F3)")

    def _sync_pane_shortcuts(self):
        """
        Hand F5/F6 to the panes only while there are two of them.

        Refresh owns F5 the rest of the time; leaving both bound would make Qt
        call the key ambiguous and fire neither.
        """
        active = self.dual_pane_active()
        self._pane_copy_shortcut.setEnabled(active)
        self._pane_move_shortcut.setEnabled(active)
        if getattr(self, "btnRefresh", None) is None:
            return
        self.btnRefresh.setShortcuts(
            [QKeySequence("Ctrl+R")] if active
            else [QKeySequence("F5"), QKeySequence("Ctrl+R")])
        self.btnRefresh.setText(
            "Refresh (Ctrl+R)" if active else "Refresh (F5, Ctrl+R)")

    def pane_sides(self) -> tuple:
        """The two panes as comparable sides, or None when there is only one."""
        if not self.dual_pane_active():
            return None
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            return None
        left = ("remote", self.data_model.bucket,
                self.data_model.current_folder or "")
        if self.second_pane.current_mode() == SecondPane.LOCAL:
            right = ("local", self.second_pane.local_target())
        else:
            bucket, prefix = self.second_pane.remote_target()
            if not bucket:
                return None
            right = ("remote", bucket, prefix)
        return left, right

    def compare_panes(self):
        """Show what differs between the two panes."""
        sides = self.pane_sides()
        if sides is None:
            self.statusBar().showMessage(
                "Open a bucket and the second pane (F3) to compare", 3000)
            return
        left, right = sides
        PaneCompareDialog(self, self, left, right).exec()

    def pane_transfer(self, move=False):
        """
        Send the focused pane's selection to the other pane (F5 / F6).

        Which queue operation that is depends on the pair of sides, so the
        four combinations are resolved here rather than in the panes.
        """
        if not self.dual_pane_active():
            self.statusBar().showMessage("Press F3 for the second pane", 2000)
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        second_focused = self.second_pane.view.hasFocus()
        if second_focused:
            self._transfer_from_second_pane(move)
        else:
            self._transfer_to_second_pane(move)

    def _transfer_to_second_pane(self, move):
        """Main listing (always remote) → the other pane."""
        items = self._collect_selected_targets()
        if not items:
            self.statusBar().showMessage("Nothing selected", 2000)
            return
        if self.second_pane.current_mode() == SecondPane.LOCAL:
            if move:
                self.statusBar().showMessage(
                    "Moving to a local folder is a download plus a delete; "
                    "do them separately", 4000)
                return
            folder = self.second_pane.local_target()
            job = []
            for name, key, is_folder in items:
                if is_folder:
                    job.append((key, None, None, folder))
                else:
                    job.append((key, os.path.join(folder, name), None, folder))
            conflicts = {entry[1] for entry in job
                         if entry[1] and os.path.exists(entry[1])}
            job = self._resolve_overwrites(
                job, conflicts, what="file", index_of=self._download_target)
            if not job:
                return
            self.assign_thread_operation("download", job, need_refresh=False)
            self.statusBar().showMessage(
                f"Downloading {len(job)} item(s) to the other pane…", 4000)
            return

        bucket, prefix = self.second_pane.remote_target()
        if not bucket:
            self.statusBar().showMessage("The other pane has no bucket", 2000)
            return
        # Same shape the clipboard paste builds, so the dst_bucket convention
        # and the destination-inside-source guard are the proven ones.
        clip = {"mode": "cut" if move else "copy",
                "bucket": self.data_model.bucket,
                "items": list(items)}
        job, skipped = build_paste_job(clip, bucket, prefix or "")
        if skipped:
            self.log("Skipped (destination inside source): "
                     + ", ".join(skipped))
        if not job:
            self.statusBar().showMessage("Nothing to send", 2000)
            return
        self.assign_thread_operation("move" if move else "copy", job)
        self.statusBar().showMessage(
            f"{'Moving' if move else 'Copying'} {len(job)} item(s)…", 4000)

    def _transfer_from_second_pane(self, move):
        """The other pane → the main listing (always remote)."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket in the main pane", 2000)
            return
        selection = self.second_pane.selected()
        if not selection:
            self.statusBar().showMessage("Nothing selected", 2000)
            return
        target = self.data_model.current_folder or ""
        if self.second_pane.current_mode() == SecondPane.LOCAL:
            if move:
                self.statusBar().showMessage(
                    "Moving from a local folder would delete the original; "
                    "upload it and remove it yourself", 4000)
                return
            folder = self.second_pane.local_target()
            job = []
            for name, is_dir, _size in selection:
                path = os.path.join(folder, name)
                if is_dir:
                    for root, _dirs, files in os.walk(path):
                        for filename in files:
                            full = os.path.join(root, filename)
                            rel = os.path.relpath(full, folder).replace(
                                os.sep, "/")
                            job.append((target + rel, full))
                else:
                    job.append((target + name, path))
            if not job:
                self.statusBar().showMessage("Nothing to upload", 2000)
                return
            self.assign_thread_operation("upload", job)
            self.statusBar().showMessage(
                f"Uploading {len(job)} file(s) to this prefix…", 4000)
            return

        bucket, prefix = self.second_pane.remote_target()
        if not bucket:
            return
        clip = {
            "mode": "cut" if move else "copy",
            "bucket": bucket,
            "items": [(name, (prefix or "") + name + ("/" if is_dir else ""),
                       is_dir)
                      for name, is_dir, _size in selection],
        }
        job, skipped = build_paste_job(clip, self.data_model.bucket, target)
        if skipped:
            self.log("Skipped (destination inside source): "
                     + ", ".join(skipped))
        if not job:
            self.statusBar().showMessage("Nothing to bring here", 2000)
            return
        # copy/move resolve CopySource against their model's bucket, so a
        # cross-bucket transfer must run on one bound to the SOURCE.
        source_bucket = bucket if bucket != self.data_model.bucket else ""
        self.assign_thread_operation(
            "move" if move else "copy", job, source_bucket=source_bucket)
        self.statusBar().showMessage(
            f"{'Moving' if move else 'Copying'} {len(job)} item(s) here…", 4000)

    def open_watch(self):
        """Start or stop mirroring a local folder up to this prefix."""
        if self.watch_active():
            self.stop_watch()
            return
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to watch a folder", 2000)
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        prefix = self.data_model.current_folder or ""
        dlg = WatchFolderDialog(
            self, self.data_model.bucket, prefix, self._watch_config)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        config = dlg.config()
        config["bucket"] = self.data_model.bucket
        config["prefix"] = prefix
        self._watch_config = config
        self._save_watch_config()
        self.start_watch(config)

    def watch_active(self) -> bool:
        return self._watch is not None

    def start_watch(self, config):
        """Begin the interval mirror described by *config*."""
        self._watch = dict(config)
        if self._watch_timer is None:
            self._watch_timer = QTimer(self)
            self._watch_timer.timeout.connect(self._watch_tick)
        self._watch_timer.setInterval(
            max(10, int(config.get("interval") or 300)) * 1000)
        self._watch_timer.start()
        self._update_watch_status()
        self.log(
            f"watching {config['local']} -> s3://{config['bucket']}/"
            f"{config['prefix']} every {config['interval']}s")
        self._watch_tick()

    def stop_watch(self):
        if self._watch_timer is not None:
            self._watch_timer.stop()
        self._watch = None
        self._update_watch_status()
        self.log("stopped watching")

    def _update_watch_status(self):
        if not self._watch:
            self.watch_status.hide()
        else:
            self.watch_status.setText(
                f"watching {os.path.basename(self._watch['local'].rstrip(os.sep))}")
            self.watch_status.setToolTip(
                f"{self._watch['local']} -> s3://{self._watch['bucket']}/"
                f"{self._watch['prefix']}  (Tools -> Watch folder to stop)")
            self.watch_status.show()
        if getattr(self, "actWatchFolder", None) is not None:
            self.actWatchFolder.setText(
                "Stop watching folder" if self._watch else "Watch folder…")

    def _watch_tick(self):
        """
        One comparison pass. Skipped while the queue is busy, so a slow upload
        cannot pile a second copy of the same work behind itself.
        """
        if not self._watch or self._watch_thread is not None:
            return
        if self.transfers_active():
            return
        config = dict(self._watch)
        clone = self.data_model.clone_for_worker()
        clone.bucket = config["bucket"]

        def _scan(_w):
            local = scan_local_tree(config["local"])
            remote = clone.list_tree(config["prefix"])
            return build_sync_plan(
                local, remote, direction="upload",
                delete_extra=bool(config.get("delete_extra")),
                exclude=config.get("exclude") or "")

        self._watch_thread = QThread(self)
        self._watch_worker = FuncWorker(_scan)
        self._watch_worker.moveToThread(self._watch_thread)
        self._watch_thread.started.connect(self._watch_worker.run)
        self._watch_worker.done.connect(self._on_watch_planned)
        self._watch_worker.done.connect(self._watch_thread.quit)
        release_worker_on_finish(self._watch_thread, self._watch_worker)
        self._watch_thread.finished.connect(self._watch_thread.deleteLater)
        self._watch_thread.start()

    def _on_watch_planned(self, result, exc):
        thread, self._watch_thread = self._watch_thread, None
        self._watch_worker = None
        join_qthread(thread)
        if not self._watch:
            return
        if exc is not None:
            self.log(f"watch: could not compare ({exc})")
            return
        actions = [entry for entry in (result or [])
                   if entry.get("action") != "skip"]
        if not actions:
            return
        self.log(f"watch: {len(actions)} change(s) to send")
        self.start_sync(actions, self._watch["local"], self._watch["prefix"],
                        "upload")

    def _watch_settings_key(self) -> str:
        return f"watch/{self.profile_name}"

    def _save_watch_config(self):
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._watch_settings_key(), json.dumps(self._watch_config or {}))
        self.settings.endGroup()

    def _load_watch_config(self):
        """
        Remember the last watch settings, but never start one on its own.

        A background uploader that resumes at launch without being asked is
        how a folder gets mirrored somewhere the user forgot about.
        """
        self.settings.beginGroup("common")
        raw = self.settings.value(self._watch_settings_key(), "")
        self.settings.endGroup()
        try:
            config = json.loads(raw or "{}")
        except (ValueError, TypeError):
            config = {}
        self._watch_config = config if isinstance(config, dict) else {}

    def export_manifest(self):
        """Write a CSV record of what this prefix holds right now."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return
        prefix = self.data_model.current_folder or ""
        suggested = (f"{self.data_model.bucket}-"
                     f"{prefix.strip('/').replace('/', '-') or 'root'}"
                     "-manifest.csv")
        path, _filter = QFileDialog.getSaveFileName(
            self, "Export manifest", suggested,
            "Manifest (*.csv);;All files (*)")
        if not path:
            return
        clone = self.data_model.clone_for_worker()
        bucket = self.data_model.bucket

        def _run(_worker):
            return clone.list_object_digests(prefix)

        entries, exc = run_with_progress(
            self, f"Listing {bucket}/{prefix}…", _run)
        if entries is None and exc is None:
            return
        if exc is not None:
            QMessageBox.critical(self, "Export manifest", str(exc))
            return
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            with open(path, "w", newline="") as handle:
                handle.write(build_manifest(entries, bucket, prefix, stamp))
        except OSError as exc:
            QMessageBox.critical(self, "Export manifest", str(exc))
            return
        self.log(f"manifest written: {len(entries)} object(s) -> {path}")
        self.statusBar().showMessage(
            f"Manifest of {len(entries)} object(s) written", 4000)

    def verify_manifest(self):
        """Compare a stored manifest against what the prefix holds now."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return
        path, _filter = QFileDialog.getOpenFileName(
            self, "Verify against manifest", "",
            "Manifest (*.csv);;All files (*)")
        if not path:
            return
        try:
            with open(path) as handle:
                meta, stored = parse_manifest(handle.read())
        except (OSError, ValueError) as exc:
            QMessageBox.critical(self, "Verify manifest", str(exc))
            return

        prefix = self.data_model.current_folder or ""
        clone = self.data_model.clone_for_worker()

        def _run(_worker):
            return clone.list_object_digests(prefix)

        entries, exc = run_with_progress(self, "Listing to compare…", _run)
        if entries is None and exc is None:
            return
        if exc is not None:
            QMessageBox.critical(self, "Verify manifest", str(exc))
            return
        current = {key: (size, etag) for key, size, etag, _mod in entries}
        result = compare_manifest(stored, current)
        self._show_manifest_result(meta, result)

    def _show_manifest_result(self, meta, result):
        lines = [
            f"Taken: {meta.get('taken', '(unknown)')}",
            f"Of: {meta.get('bucket', '?')}/{meta.get('prefix', '')}",
            "",
            f"unchanged: {result['same']}",
            f"missing since: {len(result['missing'])}",
            f"added since: {len(result['added'])}",
            f"changed: {len(result['changed'])}",
        ]
        detail = []
        for label, rows in (("MISSING", result["missing"]),
                            ("ADDED", result["added"])):
            for key in rows:
                detail.append(f"{label}  {key}")
        for key, reason in result["changed"]:
            detail.append(f"CHANGED {key}  ({reason})")
        for line in detail:
            self.log(f"manifest: {line}")
        box = QMessageBox(self)
        box.setWindowTitle("Verify manifest")
        box.setIcon(QMessageBox.Icon.Information if not detail
                    else QMessageBox.Icon.Warning)
        box.setText("\n".join(lines))
        if detail:
            box.setDetailedText("\n".join(detail))
        box.exec()

    def open_size_explorer(self):
        """Drill into the bucket by prefix, sized by what it holds."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return
        SizeExplorerDialog(self, self, self.data_model,
                           self.data_model.current_folder or "").exec()

    def bucket_settings(self):
        """Lifecycle, CORS, policy and Object Lock for the current bucket."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Enter a bucket first", 2000)
            return
        BucketSettingsDialog(self, self, self.data_model).exec()

    def toggle_legal_hold(self, key: str):
        """Place or clear a legal hold on one object."""
        if not key or key.endswith("/"):
            self.statusBar().showMessage("Legal holds are for files only", 2000)
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        clone = self.data_model.clone_for_worker()
        state, exc = run_with_progress(
            self, "Reading the lock state…",
            lambda _w: clone.get_object_lock_state(key))
        if state is None and exc is None:
            return  # cancelled
        if exc is not None:
            QMessageBox.warning(self, "Legal hold", str(exc))
            return
        if not state.get("supported"):
            QMessageBox.information(
                self, "Legal hold",
                "Object Lock is not enabled on this bucket, so a legal hold "
                "cannot be placed. It can only be turned on when a bucket is "
                "created.")
            return
        currently_on = str(state.get("legal_hold") or "").upper() == "ON"
        answer = QMessageBox.question(
            self, "Legal hold",
            (f"Remove the legal hold on '{key}'?" if currently_on else
             f"Place a legal hold on '{key}'?\n\nWhile it is on, the object "
             "cannot be deleted or overwritten — including by you."),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self.data_model.set_object_legal_hold(
                key, not currently_on, log_fn=self.log)
        except Exception as exc:
            QMessageBox.critical(self, "Legal hold", str(exc))
            return
        self.statusBar().showMessage(
            "Legal hold removed" if currently_on else "Legal hold placed", 4000)

    def fix_content_type_ui(self, items=None):
        """
        Re-stamp Content-Type across the selection from each key's extension.

        Objects uploaded before content-type detection existed (or by another
        tool that did not set one) are served as binary/octet-stream, which a
        browser downloads instead of rendering.
        """
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets() if items is None else items
        if not items:
            self.statusBar().showMessage("Select object(s) to re-type", 2000)
            return
        job = [(key, is_folder) for _n, key, is_folder in items]
        self.assign_thread_operation("set_content_type", job, need_refresh=False)
        self.statusBar().showMessage(
            f"Fixing Content-Type on {len(job)} target(s)…", 4000)

    def edit_metadata(self, key: str):
        """Edit an object's Content-Type / headers / custom metadata."""
        if not key or key.endswith("/") or key == UP_ENTRY_LABEL:
            self.statusBar().showMessage("Metadata editing is for files only", 2000)
            return
        MetadataDialog(self, self.data_model, key).exec()

    def _saved_searches_key(self) -> str:
        return f"searches/{self.profile_name}"

    def load_saved_searches(self) -> list:
        """
        This profile's saved searches, newest format only.

        A corrupt value costs the list, not the dialog — the search form still
        opens with nothing saved.
        """
        self.settings.beginGroup("common")
        raw = self.settings.value(self._saved_searches_key(), "")
        self.settings.endGroup()
        try:
            entries = json.loads(raw or "[]")
        except (ValueError, TypeError):
            return []
        if not isinstance(entries, list):
            return []
        return [entry for entry in entries
                if isinstance(entry, dict) and entry.get("name")]

    def store_saved_searches(self, entries):
        self.settings.beginGroup("common")
        self.settings.setValue(
            self._saved_searches_key(), json.dumps(list(entries or [])))
        self.settings.endGroup()

    def open_search(self):
        """Recursively search the current bucket/prefix by key substring."""
        if self.in_bucket_list_mode():
            self.statusBar().showMessage("Open a bucket to search", 2000)
            return
        SearchDialog(self, self, self.data_model,
                     self.data_model.current_folder or "").exec()

    def _column_context_menu(self, pos):
        """Toggle the optional listing columns from the header."""
        menu = QMenu(self)
        for index in LIST_OPTIONAL_COLUMNS:
            act = menu.addAction(LIST_COLUMNS[index])
            act.setCheckable(True)
            act.setChecked(not self.listview.isColumnHidden(index))
            act.setData(index)
        chosen = menu.exec(self.listview.header().mapToGlobal(pos))
        if chosen is None:
            return
        index = chosen.data()
        self.listview.setColumnHidden(index, not chosen.isChecked())

    def _log_context_menu(self, pos):
        """Standard log actions plus Clear / Save.

        The view is capped at 3000 blocks; the rotating log file keeps the
        rest, and this saves whatever is currently on screen.
        """
        menu = self.logview.createStandardContextMenu()
        menu.addSeparator()
        act_clear = menu.addAction("Clear log")
        act_save = menu.addAction("Save log…")
        chosen = menu.exec(self.logview.mapToGlobal(pos))
        if chosen is act_clear:
            self.logview.clear()
            self.log("log cleared")
        elif chosen is act_save:
            self.save_log()

    def save_log(self):
        default = os.path.join(
            os.path.expanduser("~"),
            f"s3duck-log-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt",
        )
        path, _ = QFileDialog.getSaveFileName(
            self, "Save log", default, "Text files (*.txt);;All files (*)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(self.logview.toPlainText())
        except OSError as exc:
            QMessageBox.warning(self, "Save log", f"Could not write file:\n{exc}")
            return
        self.statusBar().showMessage(f"Log saved to {path}", 4000)

    def goto_location(self):
        """Jump to a pasted s3://bucket/prefix (or a prefix in this bucket)."""
        current = self.current_s3_path()
        text, ok = QInputDialog.getText(
            self, "Go to location",
            "s3://bucket/prefix  (or just a prefix in the current bucket):",
            text=current,
        )
        if not ok:
            return
        bucket, prefix = self._parse_s3_location(text, self.data_model.bucket)
        if not bucket:
            self.statusBar().showMessage("Nothing to go to", 2000)
            return
        if bucket == self.data_model.bucket:
            self.change_current_folder(prefix)
            self.navigate(select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix)

    @staticmethod
    def _parse_s3_location(text: str, current_bucket: str = ""):
        """
        Parse 's3://bucket/prefix', 'bucket/prefix' or a bare 'prefix' into
        (bucket, prefix). A bare prefix keeps the current bucket. The returned
        prefix is '' or ends with '/'.
        """
        raw = (text or "").strip()
        if not raw:
            return "", ""
        for scheme in ("s3://", "s3a://", "S3://"):
            if raw.lower().startswith(scheme.lower()):
                raw = raw[len(scheme):]
                bucket, _, prefix = raw.partition("/")
                prefix = prefix.strip("/")
                return bucket.strip("/"), (prefix + "/" if prefix else "")
        raw = raw.lstrip("/")
        if current_bucket:
            prefix = raw.strip("/")
            return current_bucket, (prefix + "/" if prefix else "")
        bucket, _, prefix = raw.partition("/")
        prefix = prefix.strip("/")
        return bucket.strip("/"), (prefix + "/" if prefix else "")

    def goto_key(self, key: str):
        """Navigate to the folder containing 'key' and select it."""
        if self.in_bucket_list_mode() or not key:
            return
        k = key.rstrip("/")
        if "/" in k:
            parent, name = k.rsplit("/", 1)
            parent += "/"
        else:
            parent, name = "", k
        self.change_current_folder(parent)
        self.navigate(restore_name=name)

    def bucket_versioning_ui(self):
        """Enable or suspend versioning on the current bucket."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            return
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            status = self.data_model.get_bucket_versioning_status()
        except Exception:
            status = ""
        QApplication.restoreOverrideCursor()

        box = QMessageBox(self)
        box.setWindowTitle("Bucket versioning")
        box.setIcon(QMessageBox.Icon.Question)
        box.setText(
            f"Versioning for '{self.data_model.bucket}' is currently: "
            f"{status or 'not enabled'}."
        )
        enable_btn = box.addButton("Enable", QMessageBox.ButtonRole.AcceptRole)
        suspend_btn = box.addButton("Suspend", QMessageBox.ButtonRole.DestructiveRole)
        box.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
        box.exec()
        clicked = box.clickedButton()
        if clicked is enable_btn:
            target = "Enabled"
        elif clicked is suspend_btn:
            target = "Suspended"
        else:
            return
        try:
            self.data_model.set_bucket_versioning(target, log_fn=self.log)
        except Exception as exc:
            QMessageBox.warning(self, "Bucket versioning", f"Failed:\n{exc}")
            return
        self.log(f"bucket versioning {target.lower()} for {self.data_model.bucket}")
        self.statusBar().showMessage(f"Versioning {target.lower()}", 4000)

    def copy_to_clipboard(self, cut=False):
        """Remember the selection for a later paste, and put the s3:// URIs on
        the system clipboard so they are useful outside the app too."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets()
        if not items:
            self.statusBar().showMessage("Nothing selected", 2000)
            return
        if cut and self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        self._clipboard = {
            "mode": "cut" if cut else "copy",
            "bucket": self.data_model.bucket,
            "items": items,
        }
        bucket = self.data_model.bucket
        self.clip.setText(
            "\n".join(f"s3://{bucket}/{key}" for _n, key, _f in items))
        verb = "Cut" if cut else "Copied"
        self.statusBar().showMessage(
            f"{verb} {len(items)} item(s) — Ctrl+V to paste", 4000)
        self.log(f"{verb.lower()} {len(items)} item(s) from s3://{bucket}/")

    def paste_from_clipboard(self):
        """Copy or move the clipboard contents into the current folder."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to paste into", 2000)
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        clip = self._clipboard
        if not clip or not clip.get("items"):
            self.statusBar().showMessage("Clipboard is empty", 2000)
            return

        job, skipped = build_paste_job(
            clip, self.data_model.bucket, self.data_model.current_folder or "")
        if skipped:
            self.log(
                "Skipped (destination equals or nests inside source): "
                + ", ".join(skipped))
        if not job:
            self.statusBar().showMessage("Nothing to paste here", 2000)
            return

        conflicts = self._destination_conflicts([entry[1] for entry in job])
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda e: e[1])
        if not job:
            return

        is_cut = clip.get("mode") == "cut"
        src_bucket = clip.get("bucket") or ""
        source_bucket = (
            src_bucket if src_bucket != self.data_model.bucket else "")
        # A cut across buckets still copies then deletes the source.
        self.assign_thread_operation(
            "move" if is_cut else "copy", job, source_bucket=source_bucket)
        if is_cut:
            self._clipboard = None
        self.statusBar().showMessage(
            f"{'Moving' if is_cut else 'Copying'} {len(job)} item(s) here…",
            3000)

    def bulk_tags(self, items=None):
        """Add / replace / remove tags across the whole selection."""
        if self.in_bucket_list_mode():
            return
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return
        items = self._collect_selected_targets() if items is None else items
        if not items:
            self.statusBar().showMessage("Select object(s) to tag", 2000)
            return
        dlg = BulkTagsDialog(self, len(items))
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        if dlg.is_noop():
            self.statusBar().showMessage("No tag changes requested", 2000)
            return
        add = dlg.tags_to_add()
        remove = dlg.tags_to_remove()
        replace = dlg.replace_all()
        job = [(key, is_folder, add, remove, replace)
               for _n, key, is_folder in items]
        self.assign_thread_operation("set_tags", job, need_refresh=False)
        self.statusBar().showMessage(f"Tagging {len(job)} target(s)…", 3000)

    def download_as_zip(self):
        """Stream the selection into a single zip archive."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets()
        if not items:
            self.statusBar().showMessage("Select object(s) to archive", 2000)
            return
        base = (self.data_model.current_folder or "").rstrip("/").split("/")[-1]
        default = os.path.join(
            os.path.expanduser("~"),
            f"{base or self.data_model.bucket or 'objects'}.zip")
        path, _ = QFileDialog.getSaveFileName(
            self, "Download as ZIP", default, "Zip archives (*.zip)")
        if not path:
            return
        if not path.lower().endswith(".zip"):
            path += ".zip"
        prefix = self.data_model.current_folder or ""
        job = [(path, prefix, key, is_folder) for _n, key, is_folder in items]
        self.assign_thread_operation("zip_download", job, need_refresh=False)
        self.statusBar().showMessage(f"Archiving {len(job)} item(s)…", 3000)

    def prepare_drag_files(self):
        """
        Download the selection to a temp folder so it can be dropped on a file
        manager. Qt drags are synchronous, so the bytes must exist before the
        drag starts — the wait is shown and cancellable.
        """
        items = [(name, key, is_folder)
                 for name, key, is_folder in self._collect_selected_targets()]
        if not items:
            return []

        clone = self.data_model.clone_for_worker()
        keys = [(name, key, is_folder) for name, key, is_folder in items]

        def _measure(_w):
            total = 0
            for _name, key, is_folder in keys:
                if is_folder:
                    for _k, size in clone.get_keys(key):
                        total += int(size or 0)
                else:
                    total += int(clone.get_size(key) or 0)
            return total

        total, exc = self._run_with_progress("Measuring selection…", _measure)
        if exc is not None or total is None:
            return []
        if total > self.DRAG_WARN_BYTES:
            if QMessageBox.question(
                self, "Drag out",
                f"Dropping this selection downloads {_human_bytes(total)} "
                "first. Continue?",
            ) != QMessageBox.StandardButton.Yes:
                return []

        tmp_dir = self.temp_workspace.make(prefix="drag_")

        def _fetch(_w):
            paths = []
            for name, key, is_folder in keys:
                if is_folder:
                    clone.download_file(key, None, tmp_dir)
                    paths.append(os.path.join(tmp_dir, name))
                else:
                    out = os.path.join(tmp_dir, name)
                    clone.download_file(key, out, tmp_dir)
                    paths.append(out)
            return paths

        paths, exc = self._run_with_progress("Preparing files…", _fetch)
        if exc is not None:
            QMessageBox.warning(self, "Drag out", f"Could not prepare files:\n{exc}")
            return []
        return paths or []

    def bulk_rename(self):
        """Rename every selected item via find/replace or a numbering template."""
        if self.in_bucket_list_mode():
            return
        targets = self._collect_selected_targets()
        if len(targets) < 1:
            self.statusBar().showMessage("Select item(s) to rename", 2000)
            return

        items = [(name, is_folder) for name, _key, is_folder in targets]
        dlg = BulkRenameDialog(self, items)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        plan = dlg.plan()
        if not plan:
            return

        cur = self.data_model.current_folder or ""
        folder_of = {name: is_folder for name, is_folder in items}
        job = []
        for old, new in plan:
            is_folder = folder_of.get(old, False)
            suffix = "/" if is_folder else ""
            job.append((cur + old + suffix, cur + new + suffix, is_folder, None))

        conflicts = self._destination_conflicts([entry[1] for entry in job])
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda e: e[1])
        if not job:
            return

        self.assign_thread_operation("move", job)
        self.statusBar().showMessage(f"Renaming {len(job)} item(s)…", 3000)

    def open_sync(self):
        """Compare the current prefix with a local folder and sync."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket to sync", 2000)
            return
        SyncDialog(self, self, self.data_model,
                   self.data_model.current_folder or "").exec()

    def start_sync(self, actions, local_dir, prefix, direction):
        """Turn an approved sync plan into a queued transfer job."""
        job = []
        for entry in actions:
            rel = entry["rel"]
            local_path = os.path.join(local_dir, rel.replace("/", os.sep))
            key = (prefix or "") + rel
            job.append((entry["action"], rel, local_path, key,
                        int(entry.get("size") or 0)))
        if not job:
            return
        # A sync can delete or overwrite locally, so refresh the listing after.
        self.assign_thread_operation("sync", job)
        summary = summarize_sync_plan(actions)
        self.log(
            f"sync {direction}: {len(job)} action(s), "
            f"{_human_bytes(summary['bytes'])} to transfer"
        )

    def show_incomplete_uploads(self):
        """List and abort in-flight multipart uploads for the current bucket."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return
        IncompleteUploadsDialog(
            self, self, self.data_model, self.data_model.current_folder or ""
        ).exec()

    def rename_selected(self):
        """Rename the first selected file or folder in place (copy + delete)."""
        if self.in_bucket_list_mode():
            return
        sm = self.listview.selectionModel()
        ixs = sm.selectedIndexes() if sm is not None else []

        target = None
        for ix in ixs:
            if ix.column() != 0:
                continue
            primary_item, name, t = self.get_row_primary_item(ix)
            if primary_item is None or name == UP_ENTRY_LABEL:
                continue
            target = (name, t)
            break
        if target is None:
            self.statusBar().showMessage("Select a file or folder to rename", 2000)
            return

        old_name, t = target
        is_folder = (t == FSObjectType.FOLDER)
        new_name, ok = QInputDialog.getText(
            self, "Rename", "New name:", text=old_name
        )
        if not ok:
            return
        new_name = (new_name or "").strip().strip("/")
        if not new_name or new_name == old_name:
            return
        if "/" in new_name:
            QMessageBox.warning(self, "Rename", "Name cannot contain '/'.")
            return

        cur = self.data_model.current_folder or ""
        if is_folder:
            src_key = cur + old_name + "/"
            dst_key = cur + new_name + "/"
        else:
            src_key = cur + old_name
            dst_key = cur + new_name

        conflicts = self._destination_conflicts([dst_key])
        if conflicts is None:
            return
        if conflicts:
            if QMessageBox.question(
                self, "Rename",
                f"'{new_name}' already exists here.\n\nOverwrite it?",
            ) != QMessageBox.StandardButton.Yes:
                return

        self.assign_thread_operation("move", [(src_key, dst_key, is_folder, None)])
        self.statusBar().showMessage(f"Renaming to '{new_name}'…", 3000)
