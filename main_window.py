import os
import sys
import glob
import pathlib
import time
from datetime import datetime
import threading

try:
    import sip
except ImportError:
    sip = None


from PyQt5 import QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtGui import QFontDatabase
from model import Model as DataModel
from model import FSObjectType
from model import TransferCancelled
from properties_window import PropertiesWindow
from profile_switcher import ProfileSwitchWindow


OS_FAMILY_MAP = {"Linux": "🐧", "Windows": "⊞ Win", "Darwin": " MacOS"}
__VERSION__ = "0.4.4"

UP_ENTRY_LABEL = "[..]"  # special row to go one level up

PROGRESS_EMIT_INTERVAL_SEC = 0.6   # ~1.6 updates/sec
PROGRESS_MIN_BYTE_DELTA = 1 * 1024 * 1024  # also emit if at least 1MB progressed
TICK_INTERVAL_MS = 600             # UI tick
EMA_ALPHA = 0.15                   # smoother rate
RATE_WINDOW_SEC = 2.0              # window for instantaneous rate
STALL_DECAY_INTERVAL_SEC = 2.0     # when no progress, decay displayed rate


def _human_bytes(n):
    n = float(n or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    return f"{n:.1f} {units[i]}"


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
        if et == QEvent.MouseButtonPress and self._need_press:
            self._need_press = False
            return True
        if et == QEvent.MouseButtonRelease and self._need_release:
            self._need_release = False
            QTimer.singleShot(0, self.disarm)
            return True
        return False


class Tree(QTreeView):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.enable_drag_drop()

    def enable_drag_drop(self):
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)

    def disable_drag_drop(self):
        self.setAcceptDrops(False)
        self.setDropIndicatorShown(False)

    def dragEnterEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return
        if event.mimeData().hasUrls:
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
        if event.mimeData().hasUrls:
            if not self.parent.in_bucket_list_mode():
                event.setDropAction(Qt.MoveAction)
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

        if event.mimeData().hasUrls:
            event.setDropAction(Qt.CopyAction)
            event.accept()

            job = []
            for url in event.mimeData().urls():
                path = str(url.toLocalFile())
                base_path, tail = os.path.split(path)
                if os.path.isdir(path):
                    for filename in glob.iglob(path + "**/**", recursive=True):
                        key = pathlib.Path(
                            os.path.join(
                                self.parent.data_model.current_folder,
                                os.path.relpath(filename, base_path),
                            )
                        ).as_posix()
                        if os.path.isdir(filename):
                            job.append((key, None))
                        else:
                            job.append((key, filename))
                else:
                    key = pathlib.Path(
                        os.path.join(
                            self.parent.data_model.current_folder,
                            os.path.relpath(path, base_path),
                        )
                    ).as_posix()
                    job.append((key, path))
            self.disable_drag_drop()
            self.parent.assign_thread_operation("upload", job)
            self.parent.thread.finished.connect(lambda: self.enable_drag_drop())
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
        self._order = Qt.AscendingOrder  # remember current sort order

    def sort(self, column, order=Qt.AscendingOrder):
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
            if self._order == Qt.AscendingOrder:
                # [..] should be the smallest element
                return left_is_up and not right_is_up
            else:
                # Qt will invert our result for descending; return the opposite
                # so that after inversion [..] is still the smallest element.
                return not left_is_up and right_is_up

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
        ld = left.data()
        rd = right.data()

        if col == 0:  # Name
            return str(ld).lower() < str(rd).lower()

        if col == 1:  # Size (use numeric payload; tie-break by name)
            model = self.sourceModel()

            l_item = model.itemFromIndex(left)
            r_item = model.itemFromIndex(right)

            # 'size' is set when you build ListItem(size_val, ...)
            ln = getattr(l_item, "size", 0) or 0
            rn = getattr(r_item, "size", 0) or 0

            if ln != rn:
                return ln < rn

            # tie-breaker: Name (column 0)
            l_name = left.sibling(left.row(), 0).data()
            r_name = right.sibling(right.row(), 0).data()
            return str(l_name).lower() < str(r_name).lower()


class Worker(QObject):
    finished = pyqtSignal(bool) # cancelled?
    progress = pyqtSignal(str)
    refresh = pyqtSignal()
    file_progress = pyqtSignal(object, object, str)
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
                    for k, s in self.data_model.get_keys(key):
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
                    self.file_progress.emit(int(file_cur), int(file_total or 1), key)
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
        for key in self.job:
            msg = "moving %s -> /dev/null" % key
            self.progress.emit(msg)
            self.data_model.delete(key)
            self.refresh.emit()
        self.finished.emit(False)

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
                    self.file_progress.emit(int(file_cur), int(file_total or 1), key)
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
                )
                self.refresh.emit()

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

class BucketUsageWorker(QObject):
    finished = pyqtSignal(str, object)  # bucket_name, size_bytes_or_exc

    def __init__(self, data_model, bucket_name):
        super().__init__()
        self.data_model = data_model
        self.bucket_name = bucket_name

    @pyqtSlot()
    def run(self):
        try:
            # compute using model method
            sz = self.data_model.bucket_total_size_bytes()
            self.finished.emit(self.bucket_name, int(sz))
        except Exception as exc:
            self.finished.emit(self.bucket_name, exc)

class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        self.title = "S3 Duck 🦆 %s" % __VERSION__
        # self.setWindowTitle(self.title)
        self.setWindowIcon(QIcon.fromTheme("applications-internet"))

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
        ) = settings
        self.settings = settings
        self.current_dir = current_dir
        self.data_model = DataModel(
            url, region, access_key, secret_key, bucket, no_ssl_check, use_path
        )
        self.logview = QPlainTextEdit(self)
        self.logview.setMaximumBlockCount(3000)  # prevents UI freeze on huge logs

        def _apply_emoji_safe_font(widget):
            pt = widget.font().pointSize()
            db = QFontDatabase()

            def available(cands):
                return [f for f in cands if f in db.families()]

            if sys.platform.startswith("win"):
                # Windows
                base = available(["Consolas", "Segoe UI", "Arial", "Tahoma"])
                emoji = available(["Segoe UI Emoji"])
                stack = base[:1] + emoji + base[1:]
            elif sys.platform == "darwin":
                # macOS(?)

                # Menlo (default monospace) + Apple Color Emoji
                base = available(["Menlo", "SF Mono", "Monaco"])
                emoji = available(["Apple Color Emoji"])
                stack = base[:1] + emoji + base[1:]
            else:
                # Linux
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

            families = ",".join(f"'{f}'" for f in stack)
            widget.setStyleSheet(
                f"font-family: {families}; font-size: {pt}pt;")

        # apply to the log view
        _apply_emoji_safe_font(self.logview)

        self.listview = Tree(self)
        self._menu_click_guard = _OneShotClickGuard(self.listview.viewport())
        self._suppress_next_activate = False


        self.clip = QApplication.clipboard()
        self.splitter = QSplitter(Qt.Vertical)
        self.splitter.addWidget(self.listview)
        self.splitter.addWidget(self.logview)
        # ~75% top / ~25% bottom
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 1)

        self.logview.setReadOnly(True)
        self.logview.appendPlainText(
            "Welcome to S3 Duck 🦆 %s (on %s)"
            % (__VERSION__, OS_FAMILY_MAP.get(DataModel.get_os_family(), "❓"))
        )

        hlay = QHBoxLayout()
        hlay.addWidget(self.splitter)
        wid = QWidget()
        wid.setLayout(hlay)
        self.setCentralWidget(wid)
        self.setGeometry(0, 26, 900, 500)
        self.profile_name = profile_name
        self.update_window_title()
        self.copyPath = ""
        self.copyList = []
        self.copyListNew = ""
        self.createActions()



        self.tBar = self.addToolBar("Tools")
        self.tBar.setContextMenuPolicy(Qt.PreventContextMenu)
        self.tBar.setMovable(True)
        self.tBar.setIconSize(QSize(16, 16))
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnHome)
        self.tBar.addAction(self.btnBack)
        self.tBar.addAction(self.btnUp)
        self.tBar.addAction(self.btnRefresh)
        self.tBar.addAction(self.btnBucketUsage)
        self.tBar.addAction(self.actCopyS3Path)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnDownload)
        self.tBar.addAction(self.btnUpload)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnCreateFolder)
        self.tBar.addAction(self.btnRemove)
        self.tBar.addAction(self.btnCancel)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnSwitchProfile)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnAbout)
        self.tBar.setIconSize(QSize(26, 26))

        # Source model
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(["Name", "Size", "Modified"])

        # Proxy model to pin [..] and keep folders / buckets first
        self.proxy = UpTopProxyModel(UP_ENTRY_LABEL, self)
        self.proxy.setSourceModel(self.model)
        self.listview.setModel(self.proxy)

        self.pb = QProgressBar()
        self.pb.setMinimum(0)
        self.pb.setMaximum(100)
        self.pb.hide()
        self.status_text = QLabel("")
        self.statusBar().addPermanentWidget(self.status_text, 2)
        self.statusBar().addPermanentWidget(self.pb, 1)

        # bytes progress state
        self._smooth_total = 1
        self._smooth_done = 0

        # rolling samples for instantaneous measurement
        self._rate_samples = []

        # exponentially-smoothed bytes/sec for display
        self._smooth_rate_bps = 0.0

        # helpers to detect stalls and decay _smooth_rate_bps
        self._last_tick_time = 0.0
        self._last_tick_bytes = 0

        self._tick_timer = QTimer(self)
        self._tick_timer.setInterval(TICK_INTERVAL_MS)
        self._tick_timer.timeout.connect(self._on_tick)
        self._status_prefix = "Transferring…"

        # Read-only S3 path + copy button
        self.s3PathEdit = QLineEdit()
        self.s3PathEdit.setReadOnly(True)
        self.s3PathEdit.setStyleSheet("font-family: monospace; background: #f0f0f0;")
        self.statusBar().addPermanentWidget(self.s3PathEdit, 3)

        self.bucketUsageLabel = QLabel("Bucket usage: —")
        self.bucketUsageLabel.setToolTip("Click Bucket usage (Σ) to calculate total size")
        self.statusBar().addPermanentWidget(self.bucketUsageLabel, 1)
        self._bucket_usage_token = 0
        self._bucket_usage_bucket = ""
        self._bucket_usage_thread = None
        self._bucket_usage_worker = None


        self.thread = None
        self.worker = None
        self.map = dict()
        self.setWindowIcon(QIcon(os.path.join(self.current_dir, "resources", "ducky.ico")))
        self.listview.installEventFilter(self)

        # context menu for listview
        self.menu = QMenu()
        self.menu.setAttribute(Qt.WA_NoMouseReplay, True)

        # remember last bucket we successfully entered
        self._last_selected_bucket = None

        self.restoreSettings()
        self.select_first()

        # Initial populate
        self.navigate()

        self.listview.header().setSortIndicatorShown(True)
        self.listview.setSortingEnabled(True)
        self.listview.header().resizeSection(0, 320)
        self.listview.header().resizeSection(1, 80)
        self.listview.header().resizeSection(2, 80)

        self.listview.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.listview.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.listview.setDragDropMode(QAbstractItemView.DragDrop)
        self.listview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.listview.setIndentation(10)

        # Double-click: proxy-aware handler
        self.listview.doubleClicked.connect(self.list_doubleClicked)

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
            blocker = QSignalBlocker(sm)
            sm.clearSelection()
            sm.clearCurrentIndex()
            return blocker
        return None

    def _clear_selection(self):
        sm = self.listview.selectionModel()
        if sm is None:
            return
        blocker = QSignalBlocker(sm)
        try:
            sm.clearSelection()
            sm.clearCurrentIndex()
        finally:
            blocker = None

    def _normalize_selection_to_index(self, proxy_index: QModelIndex):
        if not proxy_index or not proxy_index.isValid():
            return
        sm = self.listview.selectionModel()
        if sm is None:
            self.listview.setCurrentIndex(proxy_index)
            return
        sm.blockSignals(True)
        sm.clearSelection()
        sm.setCurrentIndex(
            proxy_index,
            QItemSelectionModel.ClearAndSelect | QItemSelectionModel.Rows,
        )
        sm.blockSignals(False)

    def _end_model_reset_ui(self):
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

    def _on_file_progress(self, cur, total, key):
        return

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

        # live TX/RX stays raw so user sees bytes climb smoothly
        self.status_text.setText(
            f"{self._status_prefix} "
            f"{_human_bytes(self._smooth_done)} / {_human_bytes(self._smooth_total)}"
            f"  ({_human_bytes(display_rate_bps)}/s){eta_txt}"
        )

    # ====== selection helpers (proxy-aware) ======
    def select_first(self):
        if self.proxy.rowCount() > 0:
            index = self.proxy.index(0, 0)
            self.listview.setCurrentIndex(index)

    def ix_by_name(self, name):
        for r in range(self.model.rowCount()):
            ix_src = self.model.index(r, 0)
            if name == self.model.itemFromIndex(ix_src).text():
                return self.proxy.mapFromSource(ix_src)
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

    def reset_bucket_usage(self):
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self._bucket_usage_bucket = ""
            self.bucketUsageLabel.setText("Bucket usage: —")
            return

        b = self.data_model.bucket
        self._bucket_usage_bucket = b
        self.bucketUsageLabel.setText(f"Bucket usage: {b}: —")

    def request_bucket_usage(self):
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Select a bucket first", 2000)
            return

        # already running?
        if getattr(self, "_bucket_usage_thread", None) is not None:
            try:
                if self._bucket_usage_thread.isRunning():
                    self.btnBucketUsage.setEnabled(False)
                    self.statusBar().showMessage("Bucket usage calculation already running…", 2000)
                    return
            except Exception:
                pass

        bucket_name = self.data_model.bucket

        self._bucket_usage_token = getattr(self, "_bucket_usage_token", 0) + 1
        token = self._bucket_usage_token

        self._bucket_usage_bucket = bucket_name
        self.bucketUsageLabel.setText(f"Bucket usage: {bucket_name}: …")
        self.statusBar().showMessage("Calculating bucket usage…", 2000)

        self.btnBucketUsage.setEnabled(False)

        t = QThread(self)
        w = BucketUsageWorker(self.data_model, bucket_name)
        w.moveToThread(t)

        def apply_result(bname, result):
            if token != self._bucket_usage_token:
                return
            if self.data_model.bucket != bname:
                return
            if getattr(self, "_bucket_usage_bucket", "") != bname:
                return

            if isinstance(result, Exception):
                self.bucketUsageLabel.setText(f"Bucket usage: {bname}: n/a")
                self.statusBar().showMessage(f"Bucket usage failed: {result}", 4000)
            else:
                self.bucketUsageLabel.setText(f"Bucket usage: {bname}: {_human_bytes(int(result))}")
                self.statusBar().showMessage("Bucket usage calculated", 2000)

        def reenable():
            self.btnBucketUsage.setEnabled(not self.in_bucket_list_mode())

        w.finished.connect(apply_result)
        w.finished.connect(reenable)

        w.finished.connect(t.quit)
        w.finished.connect(w.deleteLater)
        t.finished.connect(t.deleteLater)
        t.finished.connect(reenable)  # safety
        t.started.connect(w.run)

        self._bucket_usage_thread = t
        self._bucket_usage_worker = w

        t.start()

    def eventFilter(self, obj, event):
        if obj == self.listview:
            if event.type() == QEvent.ContextMenu and obj is self.listview:
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

                    act_del_bucket = None
                    # Only allow delete if selection is actually a bucket row
                    if ixs and m and getattr(m, "t", None) == FSObjectType.BUCKET:
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
                    clk = self.menu.exec_(event.globalPos())
                    if not clk:
                        return False

                    if clk == act_new_bucket:
                        self.new_bucket()
                    if act_del_bucket and clk == act_del_bucket:
                        self.delete_bucket_ui()

                    return True

                # inside bucket menu
                upload_selected_action = None
                upload_current_action = None
                create_folder_action = None
                download_action = None
                delete_action = None
                properties_selected_action = None
                share_tmp_action = None
                share_public_action = None

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
                        share_tmp_action = QAction(
                            QIcon.fromTheme('insert-link'),
                            'Copy share link…',
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
                clk = self.menu.exec_(event.globalPos())
                if not clk:
                    return False


                if clk == upload_selected_action:
                    self.upload(upload_path)
                if clk == upload_current_action:
                    self.upload()
                if clk == create_folder_action:
                    self.new_folder()
                if clk == download_action:
                    self.download()
                if clk == share_tmp_action:
                    self.copy_presigned_link(key)
                if clk == share_public_action:
                    self.make_public_and_copy(key)
                if clk == delete_action:
                    self.delete()
                if clk == properties_selected_action:
                    self.properties(self.data_model, key)

                return True

            if event.type() == QEvent.KeyPress:
                if event.key() == Qt.Key_Escape:
                    self.cancel_transfers()
                    return True
                if event.key() == Qt.Key_Return:
                    ix = self.listview.currentIndex()
                    if ix.isValid():
                        self.list_doubleClicked(ix)
                    return True
                if event.key() == Qt.Key_Delete:
                    if self.in_bucket_list_mode():
                        self.delete_bucket_ui()
                    else:
                        self.delete()
                if event.key() == Qt.Key_Backspace:
                    self.goUp()
                if event.key() in [Qt.Key_Insert, Qt.Key_C]:
                    if self.in_bucket_list_mode():
                        self.new_bucket()
                    else:
                        self.new_folder()
                if event.key() == Qt.Key_B:
                    self.goBack()
                if event.key() in [Qt.Key_H, Qt.Key_Home]:
                    self.goHome()
                if event.key() == Qt.Key_F1:
                    self.about()
                if event.key() == Qt.Key_U and not self.in_bucket_list_mode():
                    self.upload()
                if event.key() == Qt.Key_D and not self.in_bucket_list_mode():
                    self.download()
        return super().eventFilter(obj, event)

    def simple(self, title, message):
        QMessageBox(
            QMessageBox.Information,
            title,
            message,
            QMessageBox.NoButton,
            self,
            Qt.Dialog | Qt.NoDropShadowWindowHint,
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
        if dlg.exec_() != QDialog.Accepted:
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

        # Update UI-visible profile name
        self.profile_name = prof.name

        # Update datamodel "root/profile" fields (important for bucket list mode)
        self.data_model.profile_endpoint_url = prof.url
        self.data_model.profile_use_path = prof.use_path
        self.data_model.profile_region = prof.region

        # Update current connection fields
        self.data_model.endpoint_url = prof.url
        self.data_model.use_path = prof.use_path
        self.data_model.region_name = prof.region
        self.data_model.access_key = prof.access_key
        self.data_model.secret_key = prof.secret_key
        self.data_model.no_ssl_check = prof.no_ssl_check

        # Reset cached client so it reconnects with new creds
        self.data_model._client = None

        # Reset navigation
        self.data_model.current_folder = ""
        self.data_model.prev_folder = ""
        # Start either in bucket list mode OR inside selected bucket (if profile has bucket)
        self.data_model.bucket = ""
        self.statusBar().showMessage(f"[{self.profile_name}][all buckets]", 3000)

        # Refresh view
        self.navigate()
        self.update_window_title()
        if old_profile and old_profile != self.profile_name:
            self.log(f"Profile switched: {old_profile} → {self.profile_name}")
        else:
            self.log(f"Profile switched to: {self.profile_name}")

    def about(self):
        sysinfo = QSysInfo()
        sys_info = sysinfo.prettyProductName() + "<br>" + sysinfo.kernelType() + " " + sysinfo.kernelVersion()
        title = "S3 Duck 🦆 %s" % __VERSION__
        message = (
            """
            <span style='color: #3465a4; font-size: 20pt;font-weight: bold;text-align: center;'></span>
            <center><h3>S3 Duck 🦆</h3></center>
            <a title='Vladislav Ananev' href='https://github.com/nexusriot' target='_blank'>
            <br><span style='color: #8743e2; font-size: 10pt;'>©2022-2025 Vladislav Ananev</a><br><br></strong></span></p>
            """
            + "version %s" % __VERSION__
            + "<br><br>"
            + sys_info
        )
        self.simple(title, message)

    def properties(self, model, key):
        PropertiesWindow(self, settings=(model, key)).exec_()

    def modelToListView_bucket_mode(self, bucket_items):
        """Populate the view with buckets only (no [..])."""
        blocker = self._begin_model_reset_ui()
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
            # ensure blocker is released before re-enabling updates
            blocker = None
            self._end_model_reset_ui()

    def modelToListView(self, model_result):
        """
        Populate the view for objects inside a selected bucket.
        We inject '[..]' at top.
        """
        self.listview.setUpdatesEnabled(False)
        self.listview.setSortingEnabled(False)

        sm = self.listview.selectionModel()
        blocker = QSignalBlocker(sm) if sm is not None else None
        if sm is not None:
            sm.clearSelection()
            sm.clearCurrentIndex()

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
            blocker = None  # release QSignalBlocker
            self.listview.setSortingEnabled(True)
            self.listview.setUpdatesEnabled(True)

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder
        return self.data_model.current_folder

    def navigate(self, restore_last_index=False):
        """
        If no active bucket: show list of buckets.
        Else: show objects in that bucket/prefix.
        """
        if not self.data_model.bucket:
            buckets = self.data_model.list_buckets()
            self.modelToListView_bucket_mode(buckets)
            self.listview.sortByColumn(0, Qt.AscendingOrder)
            self.statusBar().showMessage("[%s][all buckets]" % (self.profile_name,), 0)
            self.reset_bucket_usage()
            self.update_s3_path_label()
            self.enable_action_buttons()

            selected = False
            if self._last_selected_bucket:
                ix = self.ix_by_name(self._last_selected_bucket)
                if ix:
                    self._normalize_selection_to_index(ix)
                    selected = True
                else:
                    self.select_first()

            if not selected and buckets:
                self.select_first()
            return

        # we're in a bucket:
        try:
            items = self.data_model.list(self.data_model.current_folder)
        except Exception as exc:
            # Surface boto3/botocore error details and jump back to buckets
            QMessageBox.critical(
                self,
                "List failed",
                f"Cannot list '{self.data_model.bucket}'"
                f"{' at ' + self.data_model.current_folder if self.data_model.current_folder else ''}:\n\n{exc}",
            )

            # Return to bucket list mode safely (also restores region & client)
            self._return_to_bucket_list_mode()
            self.reset_bucket_usage()
            self.navigate()
            # Try to reselect the bucket we failed in, for user convenience
            if self._last_selected_bucket:
                ix = self.ix_by_name(self._last_selected_bucket)
                if ix:
                    self._normalize_selection_to_index(ix)
            return

        self.modelToListView(items)
        self.listview.sortByColumn(0, Qt.AscendingOrder)
        show_folder = self.data_model.current_folder if self.data_model.current_folder else "/"
        self.statusBar().showMessage(
            "[%s][%s] %s"
            % (
                self.profile_name,
                self.data_model.bucket,
                show_folder,
            ),
            0,
        )
        self.update_s3_path_label()
        self.enable_action_buttons()

        if restore_last_index and self.data_model.prev_folder:
            name = self.map.get(self.data_model.current_folder)
            if name:
                ix = self.ix_by_name(name)
                if ix:
                    self._normalize_selection_to_index(ix)

        self.reset_bucket_usage()

    def get_elem_name(self):
        index = self.listview.currentIndex()
        if index.isValid():
            primary_item, text, t = self.get_row_primary_item(index)
            return text, t
        return None, None

    def list_doubleClicked(self, proxy_index: QModelIndex):
        if not proxy_index.isValid():
            return

        # If we just closed a context menu, ignore one activation
        if getattr(self, "_suppress_next_activate", False):
            self._suppress_next_activate = False
            return

        # Normalize selection
        sm = self.listview.selectionModel()
        if sm is not None:
            sm.blockSignals(True)
            sm.clearSelection()
            sm.setCurrentIndex(
                proxy_index,
                QItemSelectionModel.ClearAndSelect | QItemSelectionModel.Rows,
            )
            sm.blockSignals(False)

        # Always interpret based on the row's "Name" column.
        primary_item, name, t = self.get_row_primary_item(proxy_index)
        if primary_item is None:
            return

        # Enter bucket
        if t == FSObjectType.BUCKET:
            try:
                self.data_model.enter_bucket(name)
            except Exception as exc:
                self.log(f"Open bucket failed for '{name}': {exc}")

                # Try to fetch hints (region/endpoint)
                try:
                    region_hint, endpoint_hint = self.data_model.get_bucket_hints(
                        name)
                except Exception as _hint_exc:
                    region_hint, endpoint_hint = None, None
                    self.log(f"While probing hints: {_hint_exc}")

                if region_hint:
                    self.log(
                        f"Hint: bucket '{name}' region may be '{region_hint}'")
                else:
                    self.log(
                        f"Hint: bucket '{name}' region unknown (no header)")

                if endpoint_hint:
                    self.log(
                        f"Hint: suggested endpoint for '{name}': {endpoint_hint}")

                retried = False
                retry_err = None
                # Only attempt if we actually got a region hint
                if region_hint:
                    # derive a candidate endpoint from the *root* endpoint
                    base_endpoint = self.data_model.profile_endpoint_url or self.data_model.endpoint_url
                    swapped = self.data_model.build_region_swapped_endpoint(
                        base_endpoint, region_hint)

                    # If server already gave an explicit endpoint, prefer it
                    candidate_endpoint = endpoint_hint or swapped

                    if candidate_endpoint:
                        # Save current connection state
                        old_endpoint = self.data_model.endpoint_url
                        old_region = self.data_model.region_name
                        old_use_path = self.data_model.use_path
                        old_client = self.data_model._client

                        try:
                            self.log(
                                f"Retry: temporarily switching endpoint to '{candidate_endpoint}' "
                                f"and region to '{region_hint}' for bucket '{name}'"
                            )
                            # temporarily seed the client config used by enter_bucket()
                            self.data_model.endpoint_url = candidate_endpoint
                            self.data_model.region_name = region_hint
                            self.data_model._client = None  # rebuild with new seed

                            # Re-try enter (it will still probe styles and may further adjust)
                            self.data_model.enter_bucket(name)
                            retried = True
                        except Exception as rexc:
                            retry_err = rexc
                            self.log(f"Retry failed for '{name}': {rexc}")
                            # restore originals on failure
                            self.data_model.endpoint_url = old_endpoint
                            self.data_model.region_name = old_region
                            self.data_model.use_path = old_use_path
                            self.data_model._client = old_client

                if retried:
                    # success after temporary switch
                    self._last_selected_bucket = name
                    # Note: when the user exits this bucket, goHome()/goUp() call
                    # _return_to_bucket_list_mode(), which restores profile endpoint/region.
                    self.navigate()
                    self.select_first()
                    return

                # If we couldn't recover, show the dialog and go back to the bucket list
                QMessageBox.critical(
                    self,
                    "Open bucket failed",
                    f"Cannot open bucket '{name}': {exc if retry_err is None else retry_err}",
                )

                # Return to bucket list mode safely (also restores region & client)
                self._return_to_bucket_list_mode()
                # re-render bucket list
                self.navigate()
                # reselect the bucket that failed (don't jump cursor to last success)
                ix = self.ix_by_name(name)
                if ix:
                    self.listview.setCurrentIndex(ix)
                return

            # success (no failure path)
            self._last_selected_bucket = name
            self.navigate()
            self.select_first()
            return

        # Special [..] entry
        if t == FSObjectType.FOLDER and name == UP_ENTRY_LABEL:
            if self.data_model.current_folder:
                self.goUp()
            else:
                # root of bucket -> go back to bucket list
                self._return_to_bucket_list_mode()
                self.navigate()
            return

        # Normal folder navigation
        if t == FSObjectType.FOLDER:
            self.map[self.data_model.current_folder] = name
            self.change_current_folder(
                self.data_model.current_folder + f"{name}/")
            self.navigate()
            self.select_first()
            return

    def goBack(self):
        if not self.data_model.bucket:
            return
        self.change_current_folder(self.data_model.prev_folder)
        self.navigate()

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
                job.append((key, None, None, folder_path))
                continue
            local_name = os.path.join(folder_path, name)
            job.append((key, local_name, primary_item.size, folder_path))
        self.assign_thread_operation("download", job, need_refresh=False)

        b = self.data_model.bucket
        self._bucket_usage_bucket = b
        self.bucketUsageLabel.setText(f"Bucket usage: {b}: —")

    def assign_thread_operation(self, method, job, need_refresh=True):
        if not job:
            return

        self.log(f"starting {method}")

        self.thread = QThread()
        self.worker = Worker(self.data_model, job)
        self.worker.moveToThread(self.thread)

        def _clear_thread_refs():
            self.thread = None
            self.worker = None
            if getattr(self, "btnCancel", None) is not None:
                self.btnCancel.setEnabled(False)

        # start worker method
        m = getattr(self.worker, method)
        self.thread.started.connect(m)

        # cleanup plumbing
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(_clear_thread_refs)
        self.thread.finished.connect(self.thread.deleteLater)

        # log/progress wiring
        self.worker.progress.connect(self.report_logger_progress)
        if need_refresh:
            self.worker.refresh.connect(self.navigate)

        if method == "download":
            self.pb.reset()
            self.pb.setValue(0)
            self.pb.show()
            self._status_prefix = "Downloading…"
            self.status_text.setText("Preparing…")

            self._smooth_total = 1
            self._smooth_done = 0
            self._rate_samples = []
            self._smooth_rate_bps = 0.0
            self._last_tick_time = 0.0
            self._last_tick_bytes = 0

            self._tick_timer.start()
            self.worker.batch_progress.connect(self._on_batch_progress)
            self.worker.file_progress.connect(self._on_file_progress)

            def _hide():
                self._on_tick()
                self._tick_timer.stop()
                self.pb.hide()

            self.thread.finished.connect(_hide)

        if method == "upload":
            self.pb.reset()
            self.pb.setValue(0)
            self.pb.show()
            self._status_prefix = "Uploading…"
            self.status_text.setText("Preparing…")

            self._smooth_total = 1
            self._smooth_done = 0
            self._rate_samples = []
            self._smooth_rate_bps = 0.0
            self._last_tick_time = 0.0
            self._last_tick_bytes = 0

            self._tick_timer.start()
            self.worker.batch_progress.connect(self._on_batch_progress)
            self.worker.file_progress.connect(self._on_file_progress)

            def _hide():
                self._on_tick()
                self._tick_timer.stop()
                self.pb.hide()
            self.thread.finished.connect(_hide)

        def _on_worker_finished(cancelled: bool):
            if cancelled:
                self.log(f"{method} cancelled")
            else:
                self.log(f"{method} completed")
            self.enable_action_buttons()

        self.worker.finished.connect(_on_worker_finished)

        self.thread.start()
        self.disable_action_buttons()

    def new_folder(self):
        if self.in_bucket_list_mode():
            return
        name, ok = QInputDialog.getText(self, "Create folder", "Folder name")
        name = name.replace("/", "")
        if ok and name:
            key = self.data_model.current_folder + "%s/" % name
            self.data_model.create_folder(key)
            self.log(f"Created folder {name} ({key})")
            self.navigate()
            ix = self.ix_by_name(name)
            if ix:
                self.listview.setCurrentIndex(ix)

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
            self.navigate()
            ix = self.ix_by_name(bucket_name)
            if ix:
                self.listview.setCurrentIndex(ix)
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

        qm = QMessageBox
        ret = qm.question(
            self,
            "",
            "Are you sure to delete bucket(s): %s ?\n\nNote: bucket must be EMPTY."
            % ", ".join(bucket_names),
            qm.Yes | qm.No,
        )
        if ret != qm.Yes:
            return

        errors = []
        for bname in bucket_names:
            try:
                self.data_model.delete_bucket(bname)
                self.log(f"Deleted bucket {bname}")
                if self._last_selected_bucket == bname:
                    self._last_selected_bucket = None
            except Exception as exc:
                errors.append(f"{bname}: {exc}")

        # After deleting buckets, we are definitely in bucket list mode,
        # so restore model's region/client for safety and refresh UI
        self._return_to_bucket_list_mode()
        self.navigate()

        if errors:
            QMessageBox.warning(
                self,
                "Delete bucket issues",
                "Some buckets could not be deleted:\n\n"
                + "\n".join(errors),
            )

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
        if names:
            qm = QMessageBox
            ret = qm.question(
                self,
                "",
                "Are you sure to delete objects : %s ?" % ",".join(names),
                qm.Yes | qm.No,
            )
            if ret == qm.Yes:
                self.assign_thread_operation("delete", job)

    def upload(self, folder=None):
        if self.in_bucket_list_mode():
            return
        job = []
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.ExistingFiles)
        names = dialog.getOpenFileNames(self, "Open files", "", "All files (*)")
        if not all(map(lambda x: x, names)):
            return
        for name in names[0]:
            basename = os.path.basename(name)
            key = (
                (folder + "/" + basename)
                if folder
                else (self.data_model.current_folder + basename)
            )
            job.append((key, name))
        self.assign_thread_operation("upload", job)

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

        if not self.transfers_active() or self.worker is None:
            return

        self.statusBar().showMessage("Canceling…", 2000)

        try:
            if self.worker is not None:
                # QMetaObject.invokeMethod(self.worker, "cancel", Qt.QueuedConnection)
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
        # In bucket list mode:
        #  - Create/Delete are for buckets,
        #  - Upload/Download disabled
        self.btnCreateFolder.setEnabled(True)  # create bucket OR create folder
        self.btnRemove.setEnabled(True)        # delete bucket OR delete object
        self.btnSwitchProfile.setEnabled(True)
        self.btnUpload.setEnabled(not at_root)
        self.btnDownload.setEnabled(not at_root)
        self.btnCancel.setEnabled(False)
        if hasattr(self, "menu"):
            self.menu.setEnabled(True)
        self.btnBucketUsage.setEnabled(not at_root)
        if at_root:
            self.bucketUsageLabel.setText("Bucket usage: —")

    def disable_action_buttons(self):
        if hasattr(self, "menu"):
            self.menu.setEnabled(False)
        self.btnCreateFolder.setEnabled(False)
        self.btnUpload.setEnabled(False)
        self.btnDownload.setEnabled(False)
        self.btnRemove.setEnabled(False)
        self.btnSwitchProfile.setEnabled(False)
        self.btnCancel.setEnabled(True)

    def goUp(self):
        if not self.data_model.bucket:
            return

        self._clear_selection()

        was_sorting = self.listview.isSortingEnabled()
        self.listview.setSortingEnabled(False)

        self.listview.setUpdatesEnabled(False)
        try:
            if not self.data_model.current_folder:
                self._return_to_bucket_list_mode()
                self.navigate(True)

                ix = self.listview.currentIndex()
                if not ix.isValid() and self.proxy.rowCount() > 0:
                    ix = self.proxy.index(0, 0)

                QTimer.singleShot(0, lambda ix=QModelIndex(ix): self._normalize_selection_to_index(ix))
                return

            p = self.data_model.current_folder
            new_path_list = p.split("/")[:-2]
            new_path = "/".join(new_path_list)
            if new_path:
                new_path = new_path + "/"

            self.change_current_folder(new_path)
            self.navigate(True)
            self.map.pop(p, None)

            ix = self.listview.currentIndex()
            if not ix.isValid() and self.proxy.rowCount() > 0:
                ix = self.proxy.index(0, 0)

            QTimer.singleShot(0, lambda ix=QModelIndex(ix): self._normalize_selection_to_index(ix))

        finally:
            self.listview.setUpdatesEnabled(True)
            self.listview.setSortingEnabled(was_sorting)

    def goHome(self):
        self._return_to_bucket_list_mode()
        self.reset_bucket_usage()
        self.navigate()

    def report_logger_progress(self, msg):
        # All progress lines from the worker get a timestamp
        self.log(msg)

    def current_s3_path(self) -> str:
        if not self.data_model.bucket:
            return "s3://"
        prefix = self.data_model.current_folder or ""
        return f"s3://{self.data_model.bucket}/{prefix}"

    def update_s3_path_label(self):
        full = self.current_s3_path()
        self.s3PathEdit.setText(full)
        self.s3PathEdit.setToolTip(full)

    def copy_s3_path_to_clipboard(self):
        self.clip.setText(self.current_s3_path())
        self.statusBar().showMessage("S3 path copied", 2000)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self.update_s3_path_label()

    def createActions(self):
        self.btnBack = QAction(
            QIcon.fromTheme("go-previous", QIcon(os.path.join(self.current_dir, "icons", "arrow_back_24px.svg"))),
            "Back(B)",
            triggered=self.goBack,
        )
        self.btnUp = QAction(
            QIcon.fromTheme("go-up", QIcon(os.path.join(self.current_dir, "icons", "arrow_upward_24px.svg"))),
            "Up(Backspace)",
            triggered=self.goUp,
        )
        self.btnHome = QAction(
            QIcon.fromTheme("go-home", QIcon(os.path.join(self.current_dir, "icons", "home_24px.svg"))),
            "Home(Home, H)",
            triggered=self.goHome,
        )
        self.btnDownload = QAction(
            QIcon.fromTheme("emblem-downloads", QIcon(os.path.join(self.current_dir, "icons", "download_24px.svg"))),
            "Download(D)",
            triggered=self.download,
        )
        self.btnCreateFolder = QAction(
            QIcon.fromTheme(
                "folder-new",
                QIcon(os.path.join(self.current_dir, "icons", "create_new_folder_24px.svg")),
            ),
            # dynamic: create bucket (root) OR create folder (inside bucket)
            "Create (Insert, C)",
            triggered=self.on_toolbar_create,
        )
        self.btnRemove = QAction(
            QIcon.fromTheme("edit-delete", QIcon(os.path.join(self.current_dir, "icons", "delete_24px.svg"))),
            # dynamic: delete bucket(s) or delete object(s)
            "Delete(Delete)",
            triggered=self.on_toolbar_delete,
        )
        self.btnRefresh = QAction(
            QIcon.fromTheme("view-refresh", QIcon(os.path.join(self.current_dir, "icons", "refresh_24px.svg"))),
            "Refresh(R)",
            triggered=self.navigate,
        )
        self.btnUpload = QAction(
            QIcon.fromTheme("network-server", QIcon(os.path.join(self.current_dir, "icons", "file_upload_24px.svg"))),
            "Upload(U)",
            triggered=self.upload,
        )
        self.btnCancel = QAction(
            QIcon.fromTheme("process-stop",  QIcon(os.path.join(self.current_dir, "icons", "cancel_24px.svg"))),
            "Cancel(Esc)",
            triggered=self.cancel_transfers,
        )
        self.btnBucketUsage = QAction(
            QIcon.fromTheme("view-statistics", QIcon(os.path.join(self.current_dir, "icons", "pie_24px.svg"))),
            "Bucket usage (Σ)",
            triggered=self.request_bucket_usage,
        )
        self.btnBucketUsage.setShortcut("S")
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

    def writeSettings(self):
        self.settings.beginGroup("geometry")
        self.settings.setValue("pos", self.pos())
        self.settings.setValue("size", self.size())
        self.settings.endGroup()

    def copy_presigned_link(self, key: str):
        """Copy a temporary presigned URL for the selected object."""
        if not key or key.rstrip("/") == UP_ENTRY_LABEL:
            return
        try:
            url = self.data_model.presigned_get_url(key, 3600)  # 1 hour
            QtWidgets.QApplication.clipboard().setText(url)
            self.statusBar().showMessage("Share link copied", 3000)
        except Exception as exc:
            QMessageBox.warning(self, "Share link", str(exc))

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
                QMessageBox.warning(
                    self,
                    "Public link",
                    f"Could not change ACL.\n\n{reason}\n\nDirect URL copied anyway "
                    "(will work only if bucket/object is already public).",
                )
                self.statusBar().showMessage("Direct URL copied (ACL not changed)", 4000)

        except Exception as exc:
            QMessageBox.warning(self, "Public URL", str(exc))
