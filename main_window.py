import os
import sys
from datetime import datetime, timezone

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
# Spelled out rather than star-imported: with two wildcards in play there was
# no way to tell which module a name came from, and no way to split this file
# without guessing. The list is what the module actually uses.
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QCheckBox, QDialog, QFileDialog,
    QHBoxLayout, QInputDialog, QLabel, QLineEdit, QMainWindow, QMenu,
    QMessageBox, QPlainTextEdit, QProgressBar, QProgressDialog,
    QPushButton, QSplitter, QSystemTrayIcon, QTabBar, QToolButton,
    QVBoxLayout, QWidget,
)
from PyQt6.QtCore import (
    QEvent, QModelIndex, QPoint, QSize, QSysInfo, QTimer, Qt,
)
from PyQt6.QtGui import QIcon, QStandardItemModel, QAction
from PyQt6.QtGui import (
    QFontDatabase, QShortcut, QKeySequence, QColor, QFont, QPalette,
)
from PyQt6.QtGui import QActionGroup
from model import Model as DataModel
from model import FSObjectType
from model import safe_local_path
from utils import (
    TempWorkspace, normalize_accent, reap_finished_workers, forward_icon,
    join_qthread,
    LogFile, run_with_progress, themed_icon,
)
from properties_window import PropertiesWindow
import diagnostics
from theme import apply_theme, THEMES
from version import __VERSION__
from settings_store import SettingsStore
# The widgets and dialogs the window builds. They live under ui/ now;
# re-exported here because main_window is still what the app and the
# suite import them from.
from ui.context_menu import show_listing_menu
from ui.window.bookmarks import BookmarksMixin
from ui.window.credentials import CredentialsMixin
from ui.window.history import HistoryMixin
from ui.window.navigation import NavigationMixin
from ui.window.quickfind import QuickFindMixin
from ui.window.tabs import TabsMixin
from ui.window.transfers import TransferQueueMixin
from ui.window.usage import UsageMixin
from ui.window.watch import WatchMixin
from ui.constants import (
    CREDENTIAL_CHECK_INTERVAL_MS, LIST_COLUMNS, LIST_COLUMN_DEFAULT_WIDTHS,
    LIST_OPTIONAL_COLUMNS, LIST_STORAGE_CLASS_COLUMN, OS_FAMILY_MAP,
    TICK_INTERVAL_MS, UP_ENTRY_LABEL,
)
from ui.dialogs.bookmarks import BookmarksDialog
from ui.dialogs.bucket_settings import BucketSettingsDialog
from ui.dialogs.bulk import BulkRenameDialog, BulkTagsDialog
from ui.dialogs.compare import PaneCompareDialog, SyncDialog
from ui.dialogs.copy_move import (
    CopyMoveDialog, CrossProfileCopyDialog, CrossProfileSyncDialog,
)
from ui.dialogs.diagnostics import DiagnosticsDialog
from ui.dialogs.duplicates import DuplicateFinderDialog
from ui.dialogs.filter_list import CommandPaletteDialog, QuickOpenDialog
from ui.dialogs.history import OverwriteDialog
from ui.dialogs.incomplete_uploads import IncompleteUploadsDialog
from ui.dialogs.presigned import PresignedLinkDialog
from ui.dialogs.preview import PreviewDialog
from ui.dialogs.search import SearchDialog
from ui.dialogs.shortcuts import ShortcutsDialog
from ui.dialogs.tags import MetadataDialog, TagsDialog
from ui.dialogs.transfer_settings import TransferSettingsDialog
from ui.dialogs.usage import BucketUsageDialog
from ui.dialogs.versions import VersionDiffDialog, VersionsDialog
from ui.widgets.breadcrumb import Breadcrumb
from ui.widgets.charts import TreemapWidget
from ui.widgets.click_guard import _OneShotClickGuard
from ui.widgets.listing import Tree, UpTopProxyModel
from ui.widgets.profile_picker import ProfilePicker, build_profile_model
from ui.widgets.queue_panel import (
    TransferQueuePanel, _QEntry, _QueueRow,
)
from ui.widgets.second_pane import SecondPane  # noqa: F401
from workers.transfer import Worker  # noqa: F401  (re-exported)
# Pure domain logic, extracted to core/. Re-exported here because the
# window and the dialogs are still its callers; the names are unchanged.
from core.naming import (  # noqa: F401
    _to_epoch, categorize_key, _human_bytes, _dest_inside_source,
)
from core.transfer_jobs import (  # noqa: F401
    _build_upload_job_for_path, build_paste_job,
)
from core.bulk_rename import (  # noqa: F401
    BULK_RENAME_FIND, BULK_RENAME_TEMPLATE, bulk_rename_plan,
)
from core.dedup import (  # noqa: F401
    find_duplicate_groups, summarize_duplicate_groups,
    select_redundant_keys,
)
from core.bookmarks import (  # noqa: F401
    bookmark_label, parse_bookmarks, serialize_bookmarks, add_bookmark_to,
)
from core.sync_plan import (  # noqa: F401
    build_exclude_matcher, build_sync_plan, build_compare_plan,
    summarize_compare_plan, summarize_sync_plan,
)
from core.manifest import (  # noqa: F401
    MANIFEST_HEADER, build_manifest, parse_manifest, compare_manifest,
)
from core.treemap import (  # noqa: F401
    build_prefix_tree, node_at_path, treemap_children, squarify,
)
from core.commands import (  # noqa: F401
    LISTVIEW_KEY_HELP, collect_shortcuts, command_entries, palette_score,
    filter_commands,
)
from core.locations import (  # noqa: F401
    parse_s3_location, serialize_location, parse_location,
    location_entries, build_profile_sync_job,
)
from core.formatting import (  # noqa: F401
    _listing_summary,
    format_completion_notification, hex_dump, _scaled_bar_values,
)


class MainWindow(
    NavigationMixin,
    HistoryMixin,
    TabsMixin,
    TransferQueueMixin,
    QuickFindMixin,
    BookmarksMixin,
    WatchMixin,
    UsageMixin,
    CredentialsMixin,
    QMainWindow,
):
    def __init__(self, *args, current_dir, qsettings, profile, **kwargs):
        """
        ``profile`` is a ProfileSettings; ``qsettings`` is the app's QSettings.

        These were one 20-element tuple, padded on arrival so a short caller
        would not raise. Inserting a field in the middle of it shifted every
        later field without a word -- see core/profile.py.
        """
        super().__init__(*args, **kwargs)
        self.title = "S3 Duck 🦆 %s" % __VERSION__

        self.profile = profile
        # Credential lifetime is profile state, not model state: the model
        # only ever sees the keys it was handed.
        self.session_expires = str(profile.session_expires or "")
        self.aws_profile = str(profile.aws_profile or "")
        self.credential_process = str(profile.credential_process or "")
        self.settings = qsettings
        self.current_dir = current_dir
        # Needed before the settings loaders below, which key off the profile.
        self.profile_name = profile.profile_name
        # A callable, not the name: switching profiles renames the window in
        # place and the per-profile keys have to follow it.
        self.store = SettingsStore(qsettings, lambda: self.profile_name)
        # Staged object payloads live here; cleared in closeEvent. The sweep
        # reclaims what a previous crash left behind, and only ever touches
        # roots whose owning process is gone.
        self.temp_workspace = TempWorkspace()
        self.temp_workspace.sweep()
        self.log_file = LogFile(
            LogFile.default_path()
            if self.store.get_bool("log_to_file", True) else "")
        # get_int, not int(value(...)) with its own try/except at each site:
        # QSettings answers with a string on some backends.
        transfer_concurrency = self.store.get_int(
            "transfer_concurrency", DataModel.DEFAULT_TRANSFER_CONCURRENCY)
        parallel_files = self.store.get_int(
            "parallel_files", DataModel.DEFAULT_PARALLEL_FILES)
        self.data_model = DataModel(
            profile.url, profile.region, profile.access_key,
            profile.secret_key, profile.bucket, profile.no_ssl_check,
            profile.use_path,
            transfer_concurrency=transfer_concurrency,
            session_token=profile.session_token,
            parallel_files=parallel_files,
            read_only=bool(profile.read_only),
            requester_pays=bool(profile.requester_pays),
            public_base_url=profile.public_base_url,
            proxy_url=profile.proxy_url,
            ca_bundle=profile.ca_bundle,
        )
        self._load_binding_cache()
        self._load_upload_options()
        self._bookmarks = []
        self._load_bookmarks()
        self._build_log_view()

        self._build_listing_pane()

        # The second pane lives beside the listing; both sit above the log.
        self._build_central_widget(profile.accent)

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

        self._build_toolbar()

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
        self._watch_scan_config = None
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

        self._build_shortcuts()

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

    def _build_log_view(self):
        """The log pane, in a font that can draw an emoji."""
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

    def _build_listing_pane(self):
        """The listing, its tab bar, its filter row and its notice."""
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

    def _build_central_widget(self, accent):
        """The two panes, the log below them and the queue strip under that."""
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

    def _build_toolbar(self):
        """The toolbar, in the order the buttons appear."""
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

    def _build_shortcuts(self):
        """Window-wide keys. The listing's own keys are in _listing_key."""
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





    def log(self, message: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {message}"
        self.logview.appendPlainText(line)
        # The view is capped and dies with the window; the file is what is
        # left to look at afterwards.
        self.log_file.write(f"{line}  [{self.profile_name}]")


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





    def _binding_cache_settings_key(self) -> str:
        return self.store.profile_key("bindings")

    def _load_binding_cache(self):
        """Restore proven per-bucket endpoint/region bindings for this profile."""
        with self.store.group():
            raw = self.settings.value(self._binding_cache_settings_key(), "") or ""
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
        with self.store.group():
            self.settings.setValue(
                self._binding_cache_settings_key(), "\n".join(lines))








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


    def _load_upload_options(self):
        with self.store.group():
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

    def row_storage_class(self, any_index: QModelIndex) -> str:
        """
        The storage class the listing reported for that row, or "".

        ListObjectsV2 named it when the listing was built, so a copy, move
        or rename started from a selection already knows which tier to keep
        the object in and never spends a HEAD asking. A hidden column still
        holds its data, so this answers the same whether or not the user has
        the column switched on.
        """
        if not any_index or not any_index.isValid():
            return ""
        ix_src = self.proxy.mapToSource(any_index)
        model = ix_src.model()
        item = model.itemFromIndex(
            model.index(ix_src.row(), LIST_STORAGE_CLASS_COLUMN))
        return item.text() if item is not None else ""










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




    def _listing_context_menu(self, event) -> bool:
        """The listing's right-click menu -- see ui/context_menu.py."""
        return show_listing_menu(self, event)

    def _listing_key(self, event) -> bool:
        """Keys typed in the listing. True when handled."""
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

    def eventFilter(self, obj, event):
        """Route to the three things this filter is actually for.

        This was one 546-line method, of which the context menu was 467, so
        the two keyboard branches it bracketed were effectively unreadable.
        """
        if obj is self.search_edit and event.type() == QEvent.Type.KeyPress:
            return self._filter_box_key(event)
        if obj == self.listview:
            if event.type() == QEvent.Type.ContextMenu:
                return self._listing_context_menu(event)
            if event.type() == QEvent.Type.KeyPress:
                return self._listing_key(event)
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




    # Pure string work, so it lives in core/locations.py -- which is also what
    # lets SecondPane use it without importing the window it sits beside.
    _parse_s3_location = staticmethod(parse_s3_location)




















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


    # Dropping onto a file manager downloads first; warn past this much.
    DRAG_WARN_BYTES = 256 * 1024 * 1024

    HISTORY_LIMIT = 200
    # Locations kept in one tab's back/forward trail.
    HISTORY_DEPTH = 100
    HISTORY_JOB_LIMIT = 100   # only small jobs are worth storing for re-run


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
        job, if_none_match = self._guard_upload(job)
        if not job:
            return
        self.assign_thread_operation("upload", job,
                                     if_none_match=if_none_match)

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
        job, if_none_match = self._guard_upload(job)
        if not job:
            return
        self.assign_thread_operation("upload", job,
                                     if_none_match=if_none_match)

    def transfer_settings(self):
        """Concurrency plus the storage class / encryption applied to uploads."""
        with self.store.group():
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
        cur_listing_limit = self.listing_limit()
        cur_auto_retry = self.auto_retry_enabled()
        with self.store.group():
            cur_upload_rules = DataModel.parse_upload_rules(
                self.settings.value("upload_rules", "") or "")
            cur_log_to_file = str(
                self.settings.value("log_to_file", "true")).lower() in ("true", "1")
        cur_files = getattr(self.data_model, "parallel_files",
                            DataModel.DEFAULT_PARALLEL_FILES)
        cur_verify = bool(getattr(self.data_model, "verify_downloads", False))
        with self.store.group():
            cur_rate = int(self.settings.value("rate_limit_kbps", 0) or 0)

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
        with self.store.group():
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
        with self.store.group():
            saved = self.settings.value("theme", "system") or "system"
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
        self.store.set("theme", applied)
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
        with self.store.group("view"):
            state = self.settings.value("splitter")
            widths = self.settings.value("columns")
            hidden = self.settings.value("visible_columns")
            sort_col = self.settings.value("sort_column")
            sort_order = self.settings.value("sort_order")

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
        with self.store.group("view"):
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

    def restoreSettings(self):
        with self.store.group("geometry"):
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
                join_qthread(th, timeout_ms=3000)
            except Exception:
                pass

        # Release finished workers. Slow workers remain pinned on detached
        # threads, so destroying the window cannot destroy a running QThread.
        reap_finished_workers()

    def writeSettings(self):
        with self.store.group("geometry"):
            self.settings.setValue("pos", self.pos())
            self.settings.setValue("size", self.size())
        self._save_view_state()
        self._save_binding_cache()
        self._save_last_location()




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

    def _selected_rows(self):
        """
        Yield (name, key, is_folder, storage_class) for the current
        selection, skipping the [..] up-entry. Folder keys carry a trailing
        '/'; a folder has no class of its own.
        """
        sm = self.listview.selectionModel()
        if sm is None:
            return
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
            yield name, key, is_folder, self.row_storage_class(ix)

    def _collect_selected_targets(self):
        """Return [(name, key, is_folder)] for the current selection, skipping
        the [..] up-entry. Folder keys carry a trailing '/'."""
        return [(name, key, is_folder)
                for name, key, is_folder, _storage_class
                in self._selected_rows()]

    def _selected_storage_classes(self) -> dict:
        """
        ``{key: storage_class}`` for the current selection.

        Alongside the targets rather than inside them: a dozen callers, the
        context menu and the search dialog all pass (name, key, is_folder)
        around, and a copy is the only one of them that cares which tier the
        object is in.
        """
        return {key: storage_class
                for _name, key, _is_folder, storage_class
                in self._selected_rows() if storage_class}

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

        conflicts = self._destination_conflicts(
            [row[1] for row in job], dest_model=dest_model)
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda row: row[1])
        if not job:
            return
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
        # What the listing already said each object's tier is, so the copy
        # lands it there instead of in STANDARD.
        classes = self._selected_storage_classes()
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
            job.append((src_key, dst_key, is_folder, dst_bucket,
                        classes.get(src_key, "")))

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

    def _guard_upload(self, job, dst_bucket=None):
        """
        Apply overwrite protection to an upload job.

        Every other write path (download, copy/move, paste, rename) checked
        its destination; uploads did not, and silently replaced remote
        objects. Directory placeholder entries (local path None) only create a
        prefix, so they are never conflicts.

        Returns ``(job, if_none_match)``, or ``(None, False)`` when there is
        nothing left to do. The destination check is a snapshot, so when the
        answer was "do not overwrite" the writes themselves carry a
        precondition; the only way to tell that apart from "overwrite" is
        whether a known conflict survived into the job.
        """
        if not job:
            return None, False
        keys = [key for key, local in job if local is not None]
        conflicts = self._destination_conflicts(keys, dst_bucket)
        if conflicts is None:
            return None, False
        job = self._resolve_overwrites(
            job, conflicts, what="object", index_of=lambda e: e[0])
        if not job:
            return None, False
        if not any(local is not None for _key, local in job):
            # Only folder markers left — nothing would actually be uploaded.
            self.statusBar().showMessage("Nothing left to upload", 2000)
            return None, False
        overwriting = any(key in conflicts for key, _local in job)
        return job, not overwriting


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
        unsafe = []
        for key in keys:
            # Search spans the whole bucket, so a key with ".." segments is
            # exactly the kind of thing that turns up here.
            local_name = safe_local_path(folder_path, key)
            if not local_name:
                unsafe.append(key)
                continue
            job.append((key, local_name, None, folder_path))
        if unsafe:
            QMessageBox.warning(
                self, "Download",
                f"{len(unsafe)} object(s) were skipped because their key "
                f"would write outside {folder_path}:\n\n"
                + "\n".join(unsafe[:10])
            )
            if not job:
                return
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
                "items": list(items),
                "classes": self._selected_storage_classes()}
        job, skipped = build_paste_job(clip, bucket, prefix or "")
        if skipped:
            self.log("Skipped (destination inside source): "
                     + ", ".join(skipped))
        if not job:
            self.statusBar().showMessage("Nothing to send", 2000)
            return
        conflicts = self._destination_conflicts([row[1] for row in job], bucket)
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda row: row[1])
        if not job:
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
            # The same guard the dialog, the folder upload and a drag-in get.
            # Without it F5 was the one upload route that replaced existing
            # objects without asking.
            job, if_none_match = self._guard_upload(job)
            if not job:
                return
            self.assign_thread_operation("upload", job,
                                         if_none_match=if_none_match)
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
        conflicts = self._destination_conflicts([row[1] for row in job])
        if conflicts is None:
            return
        job = self._resolve_overwrites(
            job, conflicts, what="destination", index_of=lambda row: row[1])
        if not job:
            return
        # copy/move resolve CopySource against their model's bucket, so a
        # cross-bucket transfer must run on one bound to the SOURCE.
        source_bucket = bucket if bucket != self.data_model.bucket else ""
        self.assign_thread_operation(
            "move" if move else "copy", job, source_bucket=source_bucket)
        self.statusBar().showMessage(
            f"{'Moving' if move else 'Copying'} {len(job)} item(s) here…", 4000)


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
            # Read now, while the listing that knows them is still on screen:
            # a paste can happen in another bucket, after navigating away.
            "classes": self._selected_storage_classes(),
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
        classes = self._selected_storage_classes()
        job = []
        for old, new in plan:
            is_folder = folder_of.get(old, False)
            suffix = "/" if is_folder else ""
            src_key = cur + old + suffix
            # A rename is a copy; without the class it would quietly re-tier
            # every IA object it touched.
            job.append((src_key, cur + new + suffix, is_folder, None,
                        classes.get(src_key, "")))

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

    def start_sync(self, actions, local_dir, prefix, direction, bucket=""):
        """
        Turn an approved sync plan into a queued transfer job.

        *bucket* is the bucket the plan was compared against. It is carried
        explicitly because a queued job is bound to a model when it STARTS:
        without it the job ran against whatever bucket happened to be open by
        then, which a folder watch (planned on a timer, long after the dialog
        closed) hits every time the user has navigated elsewhere — uploading
        into the wrong bucket and, with "delete extras" on, deleting from it.
        """
        job = []
        unsafe = []
        for entry in actions:
            rel = entry["rel"]
            # A remote key may hold ".." segments or a leading "/", which
            # would put the local side of the action outside the folder the
            # user chose. Such an action is dropped, not silently redirected.
            local_path = safe_local_path(local_dir, rel)
            if not local_path:
                unsafe.append(rel)
                continue
            key = (prefix or "") + rel
            job.append((entry["action"], rel, local_path, key,
                        int(entry.get("size") or 0)))
        if unsafe:
            self.log(
                f"sync: skipped {len(unsafe)} object(s) whose key escapes "
                f"'{local_dir}': " + ", ".join(unsafe[:5])
            )
            QMessageBox.warning(
                self, "Sync",
                f"{len(unsafe)} object(s) were skipped because their key "
                f"would write outside {local_dir}:\n\n"
                + "\n".join(unsafe[:10])
            )
        if not job:
            return
        # A sync can delete or overwrite locally, so refresh the listing after.
        self.assign_thread_operation(
            "sync", job, source_bucket=bucket or self.data_model.bucket or "")
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
            target = (name, t, self.row_storage_class(ix))
            break
        if target is None:
            self.statusBar().showMessage("Select a file or folder to rename", 2000)
            return

        old_name, t, storage_class = target
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

        self.assign_thread_operation(
            "move", [(src_key, dst_key, is_folder, None, storage_class)])
        self.statusBar().showMessage(f"Renaming to '{new_name}'…", 3000)
