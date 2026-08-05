import os
import re
import sys
import pathlib
import time
import tempfile
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
import threading

try:
    from PyQt6 import sip
except ImportError:
    sip = None

from PyQt6 import QtCore
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import *
from PyQt6.QtCore import *
from PyQt6.QtGui import QIcon, QStandardItemModel, QStandardItem, QAction
from PyQt6.QtGui import QFontDatabase, QShortcut, QKeySequence, QPainter, QPen, QColor, QFont
from PyQt6.QtGui import QDesktopServices, QPixmap, QActionGroup
from PyQt6.QtCore import QRectF, QUrl
from model import Model as DataModel
from model import FSObjectType
from model import TransferCancelled
from utils import scan_local_tree
from properties_window import PropertiesWindow
from profile_switcher import ProfileSwitchWindow
from theme import apply_theme, THEMES


OS_FAMILY_MAP = {"Linux": "🐧", "Windows": "⊞ Win", "Darwin": " MacOS"}
__VERSION__ = "0.11.0"

UP_ENTRY_LABEL = "[..]"  # special row to go one level up

PROGRESS_EMIT_INTERVAL_SEC = 0.6   # ~1.6 updates/sec
PROGRESS_MIN_BYTE_DELTA = 1 * 1024 * 1024  # also emit if at least 1MB progressed
TICK_INTERVAL_MS = 600             # UI tick
EMA_ALPHA = 0.15                   # smoother rate
RATE_WINDOW_SEC = 2.0              # window for instantaneous rate


class NavigationWorker(QObject):
    finished = pyqtSignal(int, object, str)  # seq, payload, err_str

    def __init__(self, data_model_clone, seq: int, bucket: str, prefix: str):
        super().__init__()
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
                items = self._dm.list(self._prefix)
                payload = {
                    "mode": "bucket_items",
                    "items": items,
                    "bucket": self._bucket,
                    "prefix": self._prefix,
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


def build_sync_plan(local_entries, remote_entries, *, direction,
                    delete_extra=False, tolerance=SYNC_MTIME_TOLERANCE_SEC):
    """
    Compare two ``{rel_path: (size, mtime_epoch)}`` maps and return the list of
    actions needed to make the destination match the source.

    direction "upload" treats local as the source, "download" treats remote as
    the source. Each action is a dict with keys: action
    (upload/download/delete_remote/delete_local/skip), rel, size, reason.
    """
    if direction not in ("upload", "download"):
        raise ValueError("direction must be 'upload' or 'download'")

    if direction == "upload":
        source, dest = local_entries, remote_entries
        transfer, delete_action = "upload", "delete_remote"
    else:
        source, dest = remote_entries, local_entries
        transfer, delete_action = "download", "delete_local"

    actions = []
    for rel in sorted(source):
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
        if rel in source:
            continue
        size = dest[rel][0]
        if delete_extra:
            actions.append({"action": delete_action, "rel": rel,
                            "size": size, "reason": "not at source"})
        else:
            actions.append({"action": "skip", "rel": rel, "size": size,
                            "reason": "extra at destination (kept)"})
    return actions


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


def _join_qthread(th, timeout_ms: int = 2000):
    """Quit and join a worker QThread.

    Dialog worker threads are parented to the dialog, so one still running when
    the dialog is destroyed aborts the process ("QThread: Destroyed while
    thread is still running"). Callers join in the done handler (the work is
    over by then, so this returns immediately) and again on close, which covers
    a dialog dismissed mid-load.
    """
    if th is None:
        return
    try:
        if th.isRunning():
            th.quit()
            th.wait(timeout_ms)
    except RuntimeError:
        pass  # already deleted by Qt


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

    def set_filter_text(self, text):
        self._filter_text = (text or "").strip().lower()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        if not self._filter_text:
            return True
        model = self.sourceModel()
        idx = model.index(source_row, 0, source_parent)
        name = str(model.data(idx) or "")
        # Always keep the "[..]" up-entry visible while filtering.
        if name == self.up_label:
            return True
        return self._filter_text in name.lower()

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

    def __init__(self, data_model, job):
        super().__init__()
        self.data_model = data_model
        self.job = job
        self._cancel_event = threading.Event()

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

            for key, local_name, size, folder_path in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                if local_name:
                    msg = "downloading %s -> %s (%s)" % (key, local_name, size)
                else:
                    msg = "downloading directory: %s -> %s" % (key, folder_path)
                self.progress.emit(msg)

                cb = make_cb()
                self.data_model.download_file(
                    key, local_name, folder_path,
                    progress_cb=cb,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )

            self.batch_progress.emit(int(done_all), int(total_bytes_all))

        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"download failed: {msg}")
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

            for key, local_name in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")

                if local_name is not None:
                    msg = "uploading %s -> %s" % (local_name, key)
                else:
                    msg = "creating folder %s" % key
                self.progress.emit(msg)

                cb = make_cb() if local_name else None
                self.data_model.upload_file(
                    local_name, key,
                    progress_cb=cb,
                    cancel_event=self._cancel_event,
                    log_fn=self.progress.emit,
                )

            self.batch_progress.emit(int(done_all), int(total_bytes_all))

        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"upload failed: {msg}")
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

            for action, rel, local_path, key, _size in self.job:
                if self._cancel_event.is_set():
                    raise TransferCancelled("cancelled")
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

            self.batch_progress.emit(int(done_all), int(total_bytes_all))
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "cancelled" in msg.lower():
                cancelled = True
            else:
                self.progress.emit(f"sync failed: {msg}")
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
            by_cat = {"Documents": 0, "Media": 0, "Other": 0}
            by_top = {}

            pref = self.prefix or ""
            pref_len = len(pref)

            for k, s in self.data_model.get_keys_for_bucket(self.bucket_name, pref):
                if not k or str(k).endswith("/"):
                    continue
                key = str(k)
                sz = int(s or 0)
                total += sz

                cat = categorize_key(key)
                by_cat[cat] = by_cat.get(cat, 0) + sz

                rel = key[pref_len:] if key.startswith(pref) else key
                top = rel.split("/", 1)[0] if "/" in rel else "(files)"
                by_top[top] = by_top.get(top, 0) + sz

            by_top = dict(sorted(by_top.items(), key=lambda kv: kv[1], reverse=True)[:12])

            self.finished.emit(self.bucket_name, self.prefix, {"total": total, "by_cat": by_cat, "by_top": by_top})
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

    def set_error(self, bucket_name: str, prefix: str, err: Exception):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText("Total: <b>n/a</b>")
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: n/a")
        self.top_groups.setText(f"<b>Top groups</b><br><pre>n/a\n{err}</pre>")

    def set_result(self, bucket_name: str, prefix: str, total: int, by_cat: dict, by_top: dict):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText(f"Total: <b>{_human_bytes(int(total))}</b>")

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
        self._worker = _FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_buckets)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        self._stop_loader()
        super().closeEvent(event)

    def _stop_loader(self):
        th, self._thread, self._worker = self._thread, None, None
        _join_qthread(th)

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
                 notify=True):
        super().__init__(parent)
        self.setWindowTitle("Transfer settings")
        self.setMinimumWidth(460)

        self._concurrency = QSpinBox()
        self._concurrency.setRange(1, max_concurrency)
        self._concurrency.setValue(int(concurrency))

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

        form = QFormLayout()
        form.addRow(QLabel("Parallel connections per transfer"), self._concurrency)
        form.addRow(QLabel("Upload storage class"), self._storage)
        form.addRow(QLabel("Upload encryption"), self._sse)
        form.addRow(QLabel("KMS key"), self._kms)
        form.addRow(self._notify)

        note = QLabel(
            "Storage class and encryption apply to new uploads. Existing "
            "objects are unaffected; use \"Change storage class…\" for those."
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

    def storage_class(self) -> str:
        return self._storage.currentData() or ""

    def sse(self) -> str:
        return self._sse.currentData() or ""

    def kms_key_id(self) -> str:
        return self._kms.text().strip() if self.sse() == "aws:kms" else ""

    def notify(self) -> bool:
        return self._notify.isChecked()


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
        lay.addWidget(self._delete_extra)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        _join_qthread(th)
        super().closeEvent(event)

    def _browse(self):
        path = QFileDialog.getExistingDirectory(self, "Select local folder")
        if path:
            self._local.setText(path)

    def direction(self) -> str:
        return "upload" if self._up.isChecked() else "download"

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
        clone = self._model.clone_for_worker()
        cancel = threading.Event()
        self._cancel = cancel

        def _scan(_w):
            remote = clone.list_tree(prefix, cancel_event=cancel)
            local = scan_local_tree(local_dir)
            return build_sync_plan(local, remote, direction=direction,
                                   delete_extra=delete_extra)

        self._thread = QThread(self)
        self._worker = _FuncWorker(_scan)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_plan)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_plan(self, result, exc):
        th, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        _join_qthread(th)
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


class _FuncWorker(QObject):
    """Run one function on a QThread and report its result or exception.

    The function receives this worker as its only argument so it can emit
    byte progress via ``worker.progress`` while it runs.
    """
    done = pyqtSignal(object, object)   # (result, exception)
    progress = pyqtSignal(int, int)     # (current_bytes, total_bytes)

    def __init__(self, fn):
        super().__init__()
        self._fn = fn

    @pyqtSlot()
    def run(self):
        try:
            res = self._fn(self)
            self.done.emit(res, None)
        except Exception as exc:
            self.done.emit(None, exc)


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
    IMG_LIMIT = 25 * 1024 * 1024
    TEXT_LIMIT = 1 * 1024 * 1024

    def __init__(self, parent, model, key):
        super().__init__(parent)
        self._model = model
        self._key = key
        self._thread = None
        self._worker = None
        self._dl_thread = None
        self._dl_worker = None

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

        self._stack.addWidget(self._status)      # index 0
        self._stack.addWidget(self._img_scroll)  # index 1
        self._stack.addWidget(self._text)        # index 2

        self._open_btn = QPushButton("Open with default app")
        self._open_btn.clicked.connect(self._open_external)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)

        btns = QHBoxLayout()
        btns.addWidget(self._open_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addWidget(self._stack, 1)
        lay.addLayout(btns)

        self._start_load()

    def _ext(self):
        return os.path.splitext(self._key)[1].lower()

    def _start_load(self):
        is_image = self._ext() in self.IMAGE_EXTS
        max_bytes = self.IMG_LIMIT if is_image else self.TEXT_LIMIT
        key = self._key
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return clone.get_object_preview(key, max_bytes)

        self._thread = QThread(self)
        self._worker = _FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        self._stop_load_thread()
        super().closeEvent(event)

    def _stop_load_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        _join_qthread(th)

    def _on_loaded(self, result, exc):
        self._stop_load_thread()
        if exc is not None:
            self._status.setText(f"Could not load preview:\n{exc}")
            self._stack.setCurrentIndex(0)
            return

        data = result.get("data") or b""
        ctype = (result.get("content_type") or "").lower()
        truncated = result.get("truncated")
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

        if b"\x00" in data[:8192]:
            self._status.setText(
                "No inline preview for this binary file.\n\n"
                'Use "Open with default app".'
            )
            self._stack.setCurrentIndex(0)
            return

        text = data.decode("utf-8", errors="replace")
        if truncated:
            text += "\n\n… (preview truncated)"
        self._text.setPlainText(text)
        self._stack.setCurrentIndex(2)

    def _open_external(self):
        if self._dl_thread is not None:
            return
        base = os.path.basename(self._key.rstrip("/")) or "object"
        tmp_dir = tempfile.mkdtemp(prefix="s3duck_")
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
        self._dl_worker = _FuncWorker(_dl)
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
            self._dl_worker.deleteLater()
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
        self._dl_thread.finished.connect(self._dl_thread.deleteLater)
        self._open_btn.setEnabled(False)
        self._dl_thread.start()
        prog.show()


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
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection
        )
        self._table.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._btn_download = QPushButton("Download…")
        self._btn_current = QPushButton("Make current")
        self._btn_delete = QPushButton("Delete version")
        close_btn = QPushButton("Close")
        self._btn_download.clicked.connect(self._download_selected)
        self._btn_current.clicked.connect(self._make_current_selected)
        self._btn_delete.clicked.connect(self._delete_selected)
        close_btn.clicked.connect(self.reject)

        row = QHBoxLayout()
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
        _join_qthread(th)

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
        self._list_worker = _FuncWorker(_fetch)
        self._list_worker.moveToThread(self._list_thread)
        self._list_thread.started.connect(self._list_worker.run)
        self._list_worker.done.connect(self._on_versions_loaded)
        self._list_worker.done.connect(self._list_thread.quit)
        self._list_worker.done.connect(self._list_worker.deleteLater)
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
        if not rows:
            return None
        idx = rows[0].row()
        if 0 <= idx < len(self._versions):
            return self._versions[idx]
        return None

    def _update_buttons(self):
        v = self._selected()
        has = v is not None
        is_dm = bool(v and v["is_delete_marker"])
        self._btn_download.setEnabled(has and not is_dm)
        self._btn_current.setEnabled(has and not is_dm and not v["is_latest"])
        self._btn_delete.setEnabled(has)

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
        self._dl_worker = _FuncWorker(_dl)
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
            self._dl_worker.deleteLater()
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
        _join_qthread(th)

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
        self._worker = _FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_loaded)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
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
        self._worker = _FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_aborted)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
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
        self._table.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection
        )
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.doubleClicked.connect(self._goto_selected)

        self._btn_goto = QPushButton("Go to location")
        self._btn_copy = QPushButton("Copy key")
        close_btn = QPushButton("Close")
        self._btn_goto.clicked.connect(self._goto_selected)
        self._btn_copy.clicked.connect(self._copy_selected)
        close_btn.clicked.connect(self.reject)
        self._btn_goto.setEnabled(False)
        self._btn_copy.setEnabled(False)
        self._table.itemSelectionChanged.connect(self._update_buttons)
        row = QHBoxLayout()
        row.addWidget(self._btn_goto)
        row.addWidget(self._btn_copy)
        row.addStretch(1)
        row.addWidget(close_btn)

        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(filters)
        lay.addWidget(self._info)
        lay.addWidget(self._table, 1)
        lay.addLayout(row)

        self._query.setFocus()

    def _update_buttons(self):
        has = self._table.currentRow() >= 0 and bool(self._results)
        self._btn_goto.setEnabled(has)
        self._btn_copy.setEnabled(has)

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
        self._worker = _FuncWorker(_search)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_results)
        self._worker.done.connect(self._thread.quit)
        self._worker.done.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def closeEvent(self, event):
        if self._cancel is not None:
            self._cancel.set()
        self._stop_search_thread()
        super().closeEvent(event)

    def _stop_search_thread(self):
        th, self._thread, self._worker = self._thread, None, None
        _join_qthread(th)

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
        key = self._selected_key()
        if key:
            QtWidgets.QApplication.clipboard().setText(key)
            self._mw.statusBar().showMessage("Key copied", 2000)

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
    def __init__(self, entry_id, method, job, need_refresh=True, label=""):
        self.entry_id = entry_id
        self.method = method
        self.job = job
        self.need_refresh = need_refresh
        self.label = label
        self.status = "queued"
        self.thread = None
        self.worker = None
        self.error = None


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

    _OP_ICONS = {
        "upload": "⬆", "download": "⬇", "delete": "✕",
        "copy": "⇆", "move": "➜", "delete_buckets": "✕",
        "empty_buckets": "∅", "sync": "⇅",
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

        row = QHBoxLayout(self)
        row.setContentsMargins(4, 2, 4, 2)
        row.addWidget(icon)
        row.addWidget(self._desc, 1)
        row.addWidget(self._status)
        row.addWidget(self._bar, 1)
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

    def set_byte_progress(self, done: int, total: int):
        range_max, value = _scaled_bar_values(done, total)
        self._bar.setRange(0, range_max)
        self._bar.setValue(value)


class TransferQueuePanel(QWidget):
    cancel_requested = pyqtSignal(int)
    retry_requested = pyqtSignal(int)

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

        hdr_row = QHBoxLayout()
        hdr_row.setContentsMargins(0, 0, 0, 0)
        hdr_row.setSpacing(0)
        hdr_row.addWidget(hdr, 1)
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
        self.setWindowIcon(QIcon.fromTheme("applications-internet"))

        # Profiles created before session-token support pass 10 fields.
        settings = tuple(settings) + ("",) * (11 - len(tuple(settings)))
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
        ) = settings
        self.settings = settings
        self.current_dir = current_dir
        # Needed before the settings loaders below, which key off the profile.
        self.profile_name = profile_name
        settings.beginGroup("common")
        try:
            transfer_concurrency = int(settings.value(
                "transfer_concurrency", DataModel.DEFAULT_TRANSFER_CONCURRENCY))
        except (TypeError, ValueError):
            transfer_concurrency = DataModel.DEFAULT_TRANSFER_CONCURRENCY
        settings.endGroup()
        self.data_model = DataModel(
            url, region, access_key, secret_key, bucket, no_ssl_check, use_path,
            transfer_concurrency=transfer_concurrency,
            session_token=session_token,
        )
        self._load_binding_cache()
        self._load_upload_options()
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
        self.search_edit.setPlaceholderText("Filter by name…  (just type, or Ctrl+F; Esc to close)")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._on_search_text_changed)
        self.search_edit.installEventFilter(self)
        self.search_edit.hide()

        self._list_container = QWidget()
        _list_lay = QVBoxLayout(self._list_container)
        _list_lay.setContentsMargins(0, 0, 0, 0)
        _list_lay.setSpacing(2)
        _list_lay.addWidget(self.search_edit)
        _list_lay.addWidget(self.listview)

        self.clip = QApplication.clipboard()
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.addWidget(self._list_container)
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
        vlay = QVBoxLayout()
        vlay.setContentsMargins(0, 0, 0, 0)
        vlay.setSpacing(0)
        vlay.addWidget(self.splitter, 1)
        vlay.addWidget(self._queue_panel, 0)
        wid = QWidget()
        wid.setLayout(vlay)
        self.setCentralWidget(wid)
        self.setGeometry(0, 26, 900, 500)

        self._nav_seq = 0
        self._nav_thread = None
        self._nav_worker = None
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
        self.tBar.addAction(self.btnUp)
        self.tBar.addAction(self.btnRefresh)
        self.tBar.addAction(self.btnBucketUsage)
        self.tBar.addAction(self.actCopyS3Path)
        self.tBar.addAction(self.actGoToLocation)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnDownload)
        self.tBar.addAction(self.btnUpload)
        self.tBar.addAction(self.btnUploadFolder)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnCreateFolder)
        self.tBar.addAction(self.btnRemove)
        self.tBar.addAction(self.btnCancel)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnSwitchProfile)
        self.tBar.addAction(self.btnTransferSettings)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnQueuePanel)
        self.tBar.addAction(self.btnAbout)
        self._build_theme_button()
        self.tBar.setIconSize(QSize(26, 26))

        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(["Name", "Size", "Modified"])

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
        self.statusBar().addPermanentWidget(self.status_text, 2)
        self.statusBar().addPermanentWidget(self.pb, 1)

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

        self.thread = None
        self.worker = None
        self.setWindowIcon(QIcon(os.path.join(self.current_dir, "resources", "ducky.ico")))
        self.listview.installEventFilter(self)

        self._search_shortcut = QShortcut(QKeySequence.StandardKey.Find, self)
        self._search_shortcut.activated.connect(self._toggle_search)

        self._deep_search_shortcut = QShortcut(QKeySequence("Ctrl+Shift+F"), self)
        self._deep_search_shortcut.activated.connect(self.open_search)

        self._sync_shortcut = QShortcut(QKeySequence("Ctrl+E"), self)
        self._sync_shortcut.activated.connect(self.open_sync)

        self._bulk_rename_shortcut = QShortcut(QKeySequence("Shift+F2"), self)
        self._bulk_rename_shortcut.activated.connect(self.bulk_rename)

        self.menu = QMenu()
        self.menu.setAttribute(Qt.WidgetAttribute.WA_NoMouseReplay, True)

        self._last_selected_bucket = None

        self.restoreSettings()
        self.select_first()

        self.navigate(show_loading=True)

        self.listview.header().setSortIndicatorShown(True)
        self.listview.setSortingEnabled(True)
        self.listview.header().resizeSection(0, 320)
        self.listview.header().resizeSection(1, 80)
        self.listview.header().resizeSection(2, 80)

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
        self.logview.appendPlainText(f"[{ts}] {message}")

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

    def _load_upload_options(self):
        self.settings.beginGroup("common")
        storage_class = self.settings.value("upload_storage_class", "") or ""
        sse = self.settings.value("upload_sse", "") or ""
        kms = self.settings.value("upload_kms_key", "") or ""
        self.settings.endGroup()
        self.data_model.set_upload_options(
            storage_class=storage_class, sse=sse, kms_key_id=kms)

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

    def update_window_title(self):
        profile = getattr(self, "profile_name", "")
        if profile:
            self.setWindowTitle(f"{self.title} — {profile}")
        else:
            self.setWindowTitle(self.title)

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
        if self.search_edit.isVisible() and self.search_edit.hasFocus():
            self._hide_search()
        else:
            self.search_edit.show()
            self.search_edit.setFocus(Qt.FocusReason.ShortcutFocusReason)
            self.search_edit.selectAll()

    def _open_search_with_text(self, text: str):
        """Open the quick-find bar and append typed text (type-to-search)."""
        self.search_edit.show()
        self.search_edit.setFocus(Qt.FocusReason.ShortcutFocusReason)
        self.search_edit.setText(self.search_edit.text() + text)

    def _hide_search(self):
        self.search_edit.blockSignals(True)
        self.search_edit.clear()
        self.search_edit.blockSignals(False)
        self.search_edit.hide()
        self.proxy.set_filter_text("")
        self.listview.setFocus(Qt.FocusReason.OtherFocusReason)

    def _on_search_text_changed(self, text):
        self.proxy.set_filter_text(text)
        # Keep a valid selection among the visible (filtered) rows.
        if self.proxy.rowCount() > 0 and not self.listview.currentIndex().isValid():
            self.listview.setCurrentIndex(self.proxy.index(0, 0))

    def _reset_search_on_navigate(self):
        # The filter is per-listing; clear it whenever the listing changes.
        if self.search_edit.text() or self.search_edit.isVisible():
            self.search_edit.blockSignals(True)
            self.search_edit.clear()
            self.search_edit.blockSignals(False)
            self.search_edit.hide()
            self.proxy.set_filter_text("")

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
                self._bucket_usage_dialog.set_result(bname, pref, total, by_cat, by_top)

        w.finished.connect(apply_result)
        w.finished.connect(t.quit)
        w.finished.connect(w.deleteLater)

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

                if upload_path is None or up_selected:
                    upload_path = self.data_model.current_folder

                self.menu.clear()

                # bucket list mode menu
                if bucket_list_mode:
                    act_new_bucket = QAction(
                        QIcon.fromTheme(
                            "folder-new",
                            QIcon(
                                os.path.join(
                                    self.current_dir,
                                    "icons",
                                    "create_new_folder_24px.svg",
                                )
                            ),
                        ),
                        "Create bucket…",
                    )
                    self.menu.addAction(act_new_bucket)

                    act_empty_bucket = None
                    act_del_bucket = None
                    # Only allow delete if selection is actually a bucket row
                    if ixs and m and getattr(m, "t", None) == FSObjectType.BUCKET:
                        act_empty_bucket = QAction(
                            QIcon.fromTheme("edit-clear"),
                            "Empty bucket…",
                        )
                        self.menu.addAction(act_empty_bucket)
                        act_del_bucket = QAction(
                            QIcon.fromTheme(
                                "edit-delete",
                                QIcon(
                                    os.path.join(
                                        self.current_dir, "icons", "delete_24px.svg"
                                    )
                                ),
                            ),
                            "Delete bucket…",
                        )
                        self.menu.addAction(act_del_bucket)

                    self._menu_click_guard.arm()
                    clk = self.menu.exec(event.globalPos())
                    if not clk:
                        return False

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
                tags_action = None
                properties_selected_action = None
                share_tmp_action = None
                share_public_action = None
                open_action = None
                versions_action = None
                rename_action = None
                restore_action = None
                storage_action = None
                metadata_action = None
                search_action = None
                versioning_action = None
                incomplete_action = None
                bulk_rename_action = None
                sync_action = None

                if (
                    m
                    and getattr(m, "t", None) == FSObjectType.FOLDER
                    and not up_selected
                ):
                    upload_selected_action = QAction(
                        QIcon.fromTheme(
                            "network-server",
                            QIcon(
                                os.path.join(
                                    self.current_dir,
                                    "icons",
                                    "file_upload_24px.svg",
                                )
                            ),
                        ),
                        "Upload -> %s" % upload_path,
                    )
                    self.menu.addAction(upload_selected_action)

                upload_current_action = QAction(
                    QIcon.fromTheme(
                        "network-server",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "file_upload_24px.svg"
                            )
                        ),
                    ),
                    "Upload -> %s"
                    % (
                        "/"
                        if not self.data_model.current_folder
                        else self.data_model.current_folder
                    ),
                )
                self.menu.addAction(upload_current_action)

                upload_folder_action = QAction(
                    QIcon.fromTheme(
                        "folder",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "folder_24px.svg"
                            )
                        ),
                    ),
                    "Upload folder -> %s"
                    % (
                        "/"
                        if not self.data_model.current_folder
                        else self.data_model.current_folder
                    ),
                )
                self.menu.addAction(upload_folder_action)

                create_folder_action = QAction(
                    QIcon.fromTheme(
                        "folder-new",
                        QIcon(
                            os.path.join(
                                self.current_dir,
                                "icons",
                                "create_new_folder_24px.svg",
                            )
                        ),
                    ),
                    "Create folder",
                )
                self.menu.addAction(create_folder_action)

                search_action = QAction(
                    QIcon.fromTheme("edit-find"),
                    "Search here…",
                )
                self.menu.addAction(search_action)

                versioning_action = QAction(
                    QIcon.fromTheme("document-open-recent"),
                    "Bucket versioning…",
                )
                self.menu.addAction(versioning_action)

                incomplete_action = QAction(
                    QIcon.fromTheme("edit-clear-history"),
                    "Incomplete uploads…",
                )
                self.menu.addAction(incomplete_action)

                sync_action = QAction(
                    QIcon.fromTheme("folder-sync"),
                    "Sync with local folder… (Ctrl+E)",
                )
                self.menu.addAction(sync_action)

                if ixs and not up_selected:
                    download_action = QAction(
                        QIcon.fromTheme(
                            "emblem-downloads",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "download_24px.svg"
                                )
                            ),
                        ),
                        "Download",
                    )
                    self.menu.addAction(download_action)
                    if m and getattr(m, 't', None) == FSObjectType.FILE:
                        open_action = QAction(
                            QIcon.fromTheme("document-open"),
                            "Open / preview",
                        )
                        self.menu.addAction(open_action)

                        versions_action = QAction(
                            QIcon.fromTheme("document-open-recent"),
                            "Versions…",
                        )
                        self.menu.addAction(versions_action)

                        share_tmp_action = QAction(
                            QIcon.fromTheme('insert-link'),
                            'Share link…',
                        )
                        self.menu.addAction(share_tmp_action)

                        share_public_action = QAction(
                            QIcon.fromTheme('insert-link'),
                            'Make public + copy URL…',
                        )
                        self.menu.addAction(share_public_action)

                    delete_action = QAction(
                        QIcon.fromTheme(
                            "edit-delete",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "delete_24px.svg"
                                )
                            ),
                        ),
                        "Delete",
                    )
                    self.menu.addAction(delete_action)

                    self.menu.addSeparator()
                    copy_move_action = QAction(
                        QIcon.fromTheme(
                            "edit-copy",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "copy_24px.svg"
                                )
                            ),
                        ),
                        "Copy / Move to…",
                    )
                    self.menu.addAction(copy_move_action)

                    rename_action = QAction(
                        QIcon.fromTheme("edit-rename"),
                        "Rename…",
                    )
                    self.menu.addAction(rename_action)

                    bulk_rename_action = QAction(
                        QIcon.fromTheme("edit-rename"),
                        "Rename multiple… (Shift+F2)",
                    )
                    self.menu.addAction(bulk_rename_action)

                    storage_action = QAction(
                        QIcon.fromTheme("drive-harddisk"),
                        "Change storage class…",
                    )
                    self.menu.addAction(storage_action)

                    restore_action = QAction(
                        QIcon.fromTheme("emblem-downloads"),
                        "Restore from Glacier…",
                    )
                    self.menu.addAction(restore_action)

                    if m and getattr(m, "t", None) == FSObjectType.FILE:
                        tags_action = QAction(
                            QIcon.fromTheme("document-properties"),
                            "Edit tags…",
                        )
                        self.menu.addAction(tags_action)

                        metadata_action = QAction(
                            QIcon.fromTheme("document-properties"),
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
                        QIcon.fromTheme(
                            "document-properties",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "puzzle_24px.svg"
                                )
                            ),
                        ),
                        "Properties",
                    )
                    self.menu.addAction(properties_selected_action)

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
                if rename_action and clk == rename_action:
                    self.rename_selected()
                if bulk_rename_action and clk == bulk_rename_action:
                    self.bulk_rename()
                if sync_action and clk == sync_action:
                    self.open_sync()
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
                    if self.search_edit.isVisible() or self.search_edit.text():
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
            bucket_icon = QIcon.fromTheme(
                "drive-harddisk",
                QIcon(os.path.join(self.current_dir, "icons", "bucket_24px.svg")),
            )

            for b in bucket_items:
                self.model.appendRow(
                    [
                        ListItem(0, FSObjectType.BUCKET, bucket_icon, b.name),
                        ListItem(0, FSObjectType.BUCKET, "<BUCKET>"),
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
                up_icon = QIcon.fromTheme(
                    "go-up",
                    QIcon(os.path.join(self.current_dir, "icons",
                                       "arrow_upward_24px.svg")),
                )
                self.model.appendRow(
                    [
                        ListItem(0, FSObjectType.FOLDER, up_icon,
                                 UP_ENTRY_LABEL),
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
                    else:
                        icon = QIcon().fromTheme(
                            "network-server",
                            QIcon(os.path.join(self.current_dir, "icons",
                                               "folder_24px.svg")),
                        )
                        size_val = 0
                        size = "<DIR>"
                        modified = ""

                    self.model.appendRow(
                        [
                            ListItem(size_val, i.type_, icon, i.name),
                            ListItem(size_val, i.type_, size),
                            ListItem(size_val, i.type_, modified),
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
            self.listview.setEnabled(True)
            if target_prefix:
                # enter_bucket resets navigation to the bucket root.
                self.data_model.current_folder = target_prefix
                self.data_model.prev_folder = ""
            self.navigate(select_up_entry=True)

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
            self.navigate(restore_name=bucket_name)

        def _clear_enter_refs():
            self._bucket_enter_thread = None
            self._bucket_enter_worker = None

        wk.success.connect(_on_enter_success)
        wk.failure.connect(_on_enter_failure)
        wk.finished.connect(th.quit)
        wk.finished.connect(wk.deleteLater)
        th.finished.connect(_clear_enter_refs)
        th.finished.connect(th.deleteLater)

        self._bucket_enter_thread = th
        self._bucket_enter_worker = wk
        th.start()

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
        if self.bucket_enter_active():
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
        # _nav_seq check in _on_navigation_finished. The previous QThread is
        # parented to self and will deleteLater itself when its run() returns.
        self._nav_thread = None
        self._nav_worker = None

        th = QThread(self)
        wk = NavigationWorker(self.data_model.clone_for_worker(), seq, bucket, prefix)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.finished.connect(self._on_navigation_finished)
        wk.finished.connect(th.quit)
        wk.finished.connect(wk.deleteLater)
        th.finished.connect(th.deleteLater)

        self._nav_thread = th
        self._nav_worker = wk
        th.start()

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

    def goBack(self):
        if not self.data_model.bucket:
            return
        if self.transfers_active():
            self.statusBar().showMessage("Transfers active — navigation is disabled", 2000)
            return
        self.change_current_folder(self.data_model.prev_folder)
        self.navigate()

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
        """
        Run fn(worker) on a QThread while showing a modal busy dialog.
        Returns (result, exception); result is None if the user cancelled.
        """
        prog = QProgressDialog(title, "Cancel", 0, 0, self)
        prog.setWindowTitle(title)
        prog.setWindowModality(Qt.WindowModality.ApplicationModal)
        prog.setMinimumDuration(0)
        prog.setAutoClose(False)
        prog.setAutoReset(False)

        state = {"result": None, "exc": None, "done": False}
        thread = QThread(self)
        worker = _FuncWorker(fn)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)

        def _on_done(result, exc):
            state.update(result=result, exc=exc, done=True)
            prog.reset()

        worker.done.connect(_on_done)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.start()

        prog.exec()  # returns when reset() above runs, or the user cancels
        if not state["done"]:
            state["exc"] = None
            state["result"] = None
        _join_qthread(thread)
        return state["result"], state["exc"]

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
        job = self._resolve_overwrites(
            job, conflicts, what="file", index_of=lambda e: e[1])
        if not job:
            return
        self.assign_thread_operation("download", job, need_refresh=False)

    def assign_thread_operation(self, method, job, need_refresh=True):
        if not job:
            return

        label = self._queue_build_label(method, job)
        entry = _QEntry(
            entry_id=self._queue_next_id,
            method=method,
            job=job,
            need_refresh=need_refresh,
            label=label,
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
        self.worker = Worker(self.data_model.clone_for_worker(), job)
        self.worker.moveToThread(self.thread)

        entry.thread = self.thread
        entry.worker = self.worker

        m = getattr(self.worker, method)
        self.thread.started.connect(m)

        self.worker.progress.connect(self.report_logger_progress)
        self.worker.error.connect(self._on_transfer_error)

        entry.error = None

        def _record_error(msg: str, _entry=entry):
            _entry.error = msg

        self.worker.error.connect(_record_error)

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

        if method in ("upload", "download", "sync"):
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
                entry.status = "error"
            else:
                self.log(f"{method} completed")
                entry.status = "done"
            self._batch_stats[entry.status] = (
                self._batch_stats.get(entry.status, 0) + 1)
            self._queue_panel.update_status(entry)

            if need_refresh:
                QTimer.singleShot(0, lambda: self.navigate(force=True))

        self.worker.finished.connect(_on_worker_finished)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(_clear_thread_refs)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(_reenable_after_thread)

        self.thread.start()
        self.disable_action_buttons()

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
                icon = QIcon.fromTheme("applications-internet")
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

    def _on_queue_retry_requested(self, entry_id: int):
        """Re-queue a failed or cancelled job as a fresh entry."""
        entry = self._queue_entries.get(entry_id)
        if entry is None or entry.status not in ("cancelled", "error"):
            return
        self.log(f"retrying {entry.method}: {entry.label}")
        self.assign_thread_operation(
            entry.method, entry.job, need_refresh=entry.need_refresh)

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
        if not job:
            return
        self.assign_thread_operation("upload", job)

    def transfer_settings(self):
        """Concurrency plus the storage class / encryption applied to uploads."""
        self.settings.beginGroup("common")
        cur_class = self.settings.value("upload_storage_class", "") or ""
        cur_sse = self.settings.value("upload_sse", "") or ""
        cur_kms = self.settings.value("upload_kms_key", "") or ""
        cur_notify = str(
            self.settings.value("notify_on_complete", "true")).lower() in ("true", "1")
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
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        applied = self.data_model.set_transfer_concurrency(dlg.concurrency())
        extra = self.data_model.set_upload_options(
            storage_class=dlg.storage_class(),
            sse=dlg.sse(),
            kms_key_id=dlg.kms_key_id(),
        )
        self.settings.beginGroup("common")
        self.settings.setValue("transfer_concurrency", applied)
        self.settings.setValue("upload_storage_class", dlg.storage_class())
        self.settings.setValue("upload_sse", dlg.sse())
        self.settings.setValue("upload_kms_key", dlg.kms_key_id())
        self.settings.setValue(
            "notify_on_complete", "true" if dlg.notify() else "false")
        self.settings.endGroup()
        self.log(
            f"Transfer settings: concurrency {applied}, upload extras "
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

        self.btnSwitchProfile.setEnabled(not active)
        self.btnCreateFolder.setEnabled(not active)
        self.btnRemove.setEnabled(True)
        self.btnUpload.setEnabled(not at_root)
        self.btnUploadFolder.setEnabled(not at_root)
        self.btnDownload.setEnabled(not at_root)
        self.btnCancel.setEnabled(active)

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
            QIcon.fromTheme(
                "preferences-desktop-theme",
                QIcon(os.path.join(self.current_dir, "icons", "theme_24px.svg")),
            )
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
            QIcon.fromTheme("go-previous", QIcon(os.path.join(self.current_dir, "icons", "arrow_back_24px.svg"))),
            "Back (Alt+Left)",
            triggered=self.goBack,
        )
        self.btnBack.setShortcut(QKeySequence("Alt+Left"))
        self.btnUp = QAction(
            QIcon.fromTheme("go-up", QIcon(os.path.join(self.current_dir, "icons", "arrow_upward_24px.svg"))),
            "Up (Backspace, Alt+Up)",
            triggered=self.goUp,
        )
        self.btnUp.setShortcut(QKeySequence("Alt+Up"))
        self.btnHome = QAction(
            QIcon.fromTheme("go-home", QIcon(os.path.join(self.current_dir, "icons", "home_24px.svg"))),
            "Home (Home, Alt+Home)",
            triggered=self.goHome,
        )
        self.btnHome.setShortcut(QKeySequence("Alt+Home"))
        self.btnDownload = QAction(
            QIcon.fromTheme("emblem-downloads", QIcon(os.path.join(self.current_dir, "icons", "download_24px.svg"))),
            "Download (Ctrl+D)",
            triggered=self.download,
        )
        self.btnDownload.setShortcut(QKeySequence("Ctrl+D"))
        self.btnCreateFolder = QAction(
            QIcon.fromTheme(
                "folder-new",
                QIcon(os.path.join(self.current_dir, "icons", "create_new_folder_24px.svg")),
            ),
            # dynamic: create bucket (root) OR create folder (inside bucket)
            "Create (Insert, Ctrl+N)",
            triggered=self.on_toolbar_create,
        )
        self.btnCreateFolder.setShortcut(QKeySequence("Ctrl+N"))
        self.btnRemove = QAction(
            QIcon.fromTheme("edit-delete", QIcon(os.path.join(self.current_dir, "icons", "delete_24px.svg"))),
            # dynamic: delete bucket(s) or delete object(s)
            "Delete (Del)",
            triggered=self.on_toolbar_delete,
        )
        self.btnRefresh = QAction(
            QIcon.fromTheme("view-refresh", QIcon(os.path.join(self.current_dir, "icons", "refresh_24px.svg"))),
            "Refresh (F5, Ctrl+R)",
            # QAction.triggered passes its 'checked' bool to the first optional
            # parameter, which here would land in navigate(restore_name=...).
            triggered=lambda: self.navigate(),
        )
        self.btnRefresh.setShortcuts([QKeySequence("F5"), QKeySequence("Ctrl+R")])
        self.btnUpload = QAction(
            QIcon.fromTheme("network-server", QIcon(os.path.join(self.current_dir, "icons", "file_upload_24px.svg"))),
            "Upload (Ctrl+U)",
            triggered=lambda: self.upload(),
        )
        self.btnUpload.setShortcut(QKeySequence("Ctrl+U"))
        self.btnUploadFolder = QAction(
            QIcon.fromTheme("folder", QIcon(os.path.join(self.current_dir, "icons", "folder_24px.svg"))),
            "Upload folder (Ctrl+Shift+U)",
            triggered=lambda: self.upload_folder(),
        )
        self.btnUploadFolder.setShortcut(QKeySequence("Ctrl+Shift+U"))
        self.btnTransferSettings = QAction(
            QIcon.fromTheme("preferences-system", QIcon(os.path.join(self.current_dir, "icons", "settings_24px.svg"))),
            "Transfer settings…",
            triggered=self.transfer_settings,
        )
        self.btnCancel = QAction(
            QIcon.fromTheme("process-stop",  QIcon(os.path.join(self.current_dir, "icons", "cancel_24px.svg"))),
            "Cancel (Esc)",
            triggered=self.cancel_transfers,
        )
        self.btnBucketUsage = QAction(
            QIcon.fromTheme("view-statistics", QIcon(os.path.join(self.current_dir, "icons", "pie_24px.svg"))),
            "Bucket usage Σ (Ctrl+S)",
            triggered=self.request_bucket_usage,
        )
        self.btnBucketUsage.setShortcut(QKeySequence("Ctrl+S"))
        self.btnBucketUsage.setEnabled(False)
        self.btnCancel.setEnabled(False)
        self.btnAbout = QAction(
            QIcon.fromTheme("help-about", QIcon(os.path.join(self.current_dir, "icons", "info_24px.svg"))),
            "About(F1)",
            triggered=self.about,
        )
        self.btnSwitchProfile = QAction(
            QIcon.fromTheme("system-switch-user", QIcon(os.path.join(self.current_dir, "icons", "account-switch_24px.svg"))),
            "Switch profile…",
            triggered=self.switch_profile,
        )
        self.actCopyS3Path = QAction(
            QIcon.fromTheme(
                "edit-copy", QIcon(os.path.join(self.current_dir, "icons", "copy_24px.svg"))
            ),
            "Copy S3 path",
            self,
        )
        self.actCopyS3Path.triggered.connect(self.copy_s3_path_to_clipboard)

        self.actGoToLocation = QAction(
            QIcon.fromTheme(
                "go-jump", QIcon(os.path.join(self.current_dir, "icons", "arrow_back_24px.svg"))
            ),
            "Go to location… (Ctrl+L)",
            self,
        )
        self.actGoToLocation.setShortcut(QKeySequence("Ctrl+L"))
        self.actGoToLocation.triggered.connect(lambda: self.goto_location())

        self.btnQueuePanel = QAction(
            QIcon.fromTheme(
                "format-justify-fill",
                QIcon(os.path.join(self.current_dir, "icons", "queue_24px.svg")),
            ),
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
            for i, raw in enumerate(list(widths)[:3]):
                try:
                    width = int(raw)
                except (TypeError, ValueError):
                    continue
                if width > 0:
                    header.resizeSection(i, width)
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
            "columns", [str(header.sectionSize(i)) for i in range(3)])
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

        for attr in (
            "thread",
            "_nav_thread",
            "_bucket_enter_thread",
            "_bucket_usage_thread",
        ):
            th = getattr(self, attr, None)
            if th is None:
                continue
            if sip is not None:
                try:
                    if sip.isdeleted(th):
                        continue
                except Exception:
                    pass
            try:
                if th.isRunning():
                    th.quit()
                    th.wait(3000)
            except Exception:
                pass

    def writeSettings(self):
        self.settings.beginGroup("geometry")
        self.settings.setValue("pos", self.pos())
        self.settings.setValue("size", self.size())
        self.settings.endGroup()
        self._save_view_state()
        self._save_binding_cache()

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

    def restore_from_glacier(self):
        """Initiate a Glacier / Deep Archive restore for the selection
        (files and/or whole folders), run through the transfer queue."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets()
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

    def change_storage_class_ui(self):
        """Change the storage class of the selection (files and/or whole
        folders), run through the transfer queue."""
        if self.in_bucket_list_mode():
            return
        items = self._collect_selected_targets()
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

    def edit_metadata(self, key: str):
        """Edit an object's Content-Type / headers / custom metadata."""
        if not key or key.endswith("/") or key == UP_ENTRY_LABEL:
            self.statusBar().showMessage("Metadata editing is for files only", 2000)
            return
        MetadataDialog(self, self.data_model, key).exec()

    def open_search(self):
        """Recursively search the current bucket/prefix by key substring."""
        if self.in_bucket_list_mode():
            self.statusBar().showMessage("Open a bucket to search", 2000)
            return
        SearchDialog(self, self, self.data_model,
                     self.data_model.current_folder or "").exec()

    def _log_context_menu(self, pos):
        """Standard log actions plus Clear / Save — the view is capped at 3000
        blocks and is otherwise lost on exit."""
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
