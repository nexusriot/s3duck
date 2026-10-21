"""Where the listing is, and how it gets there."""

import os
from PyQt6.QtCore import (
    QItemSelectionModel, QModelIndex, QThread, QTimer, Qt, pyqtSlot,
)
from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QInputDialog, QMessageBox
from core.formatting import _listing_summary
from core.locations import parse_location, serialize_location
from core.naming import _human_bytes
from model import FSObjectType
from ui.constants import DEFAULT_LISTING_LIMIT, UP_ENTRY_LABEL
from ui.widgets.listing import ListItem
from utils import release_worker_on_finish, themed_icon
from workers.navigation import BucketEnterWorker, NavigationWorker

try:
    from PyQt6 import sip
except ImportError:
    sip = None


class NavigationMixin:
    """Where the listing is, and how it gets there."""

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

    def in_bucket_list_mode(self) -> bool:
        return not bool(self.data_model.bucket)

    def _navigation_refusal(self) -> str:
        """Why a navigation cannot start right now, or "" when it can.

        Asked BEFORE a move commits its target to the model. Committing first
        and letting navigate() refuse afterwards left the model on the new
        prefix while the listing still showed the old one -- and every
        selection-based action then resolved the rows on screen under the
        wrong prefix: Delete on a visible "report.csv" removed
        "archive/report.csv".
        """
        if self.transfers_active():
            return "Transfers active — navigation is disabled"
        if self.bucket_enter_active():
            return "Opening bucket — please wait…"
        return ""

    def _refuse_navigation(self) -> bool:
        """Show why a navigation cannot start; True when it cannot."""
        reason = self._navigation_refusal()
        if reason:
            self.statusBar().showMessage(reason, 2000)
            return True
        return False

    def _go_to_prefix(self, prefix, **navigate_kwargs) -> bool:
        """Move inside the open bucket: check, then commit, then list."""
        if self._refuse_navigation():
            return False
        self.change_current_folder(prefix or "")
        self.navigate(**navigate_kwargs)
        return True

    def listing_settled(self) -> bool:
        """True when the rows on screen are the listing of the model's
        location, so a selected row resolves to the key it shows.

        False while a listing or a bucket entry is in flight: the model has
        already moved, and the old rows -- still selected -- would resolve
        under the new prefix.
        """
        if getattr(self, "_nav_in_flight", False) or self.bucket_enter_active():
            return False
        shown = getattr(self, "_listing_location", None)
        here = (self.data_model.bucket or "",
                self.data_model.current_folder or "")
        return shown is None or shown == here

    def _refuse_unsettled_listing(self) -> bool:
        """Refuse a selection-based action while the listing is moving."""
        if self.listing_settled():
            return False
        self.statusBar().showMessage(
            "The listing is still loading — try again once it has", 3000)
        return True

    def _recover_from_failed_listing(self) -> bool:
        """
        A listing failed: make the model describe the rows on screen again.

        Within one bucket the old listing is still true, so the model goes
        back to its prefix (True). Rows from another bucket (or a bucket
        list) have nothing they resolve under any more, so they are dropped
        instead (False) -- the view has genuinely moved.
        """
        shown = getattr(self, "_listing_location", None)
        bucket = self.data_model.bucket or ""
        if shown is not None and shown[0] == bucket:
            self.data_model.current_folder = shown[1]
            return True
        if bucket:
            self.modelToListView([])
        else:
            self.modelToListView_bucket_mode([])
        self.update_s3_path_label()
        return False

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

    def select_first(self):
        if self.proxy.rowCount() > 0:
            index = self.proxy.index(0, 0)
            self.listview.setCurrentIndex(index)

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
        self._listing_location = ("", "")

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
        # What the rows now on screen are a listing OF. A failed navigation
        # puts the model back here, and listing_settled() compares against it.
        self._listing_location = (self.data_model.bucket or "",
                                  self.data_model.current_folder or "")

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder
        return self.data_model.current_folder

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
                if self._refuse_navigation():
                    return
                self._return_to_bucket_list_mode()
                self.navigate(restore_name=self._last_selected_bucket)
            return

        # Normal folder navigation
        if t == FSObjectType.FOLDER:
            self._last_selected_in_prefix[(self.data_model.bucket or '', self.data_model.current_folder or '')] = name
            self._go_to_prefix(self.data_model.current_folder + f"{name}/",
                               select_up_entry=True)
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
            # Already entering a bucket. Said rather than swallowed: a jump
            # dropped here without a word reads as the app ignoring the click.
            self.statusBar().showMessage("Opening bucket — please wait…", 2000)
            return
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
            if self._closing:
                return
            self._last_selected_bucket = bucket_name
            self._clear_undo_delete()
            self.listview.setEnabled(True)
            if target_prefix:
                # enter_bucket resets navigation to the bucket root.
                self.data_model.current_folder = target_prefix
                self.data_model.prev_folder = ""
            self.navigate(select_up_entry=True, force=True)

        def _on_enter_failure(bucket_name: str, err_msg: str):
            if self._closing:
                return
            self.listview.setEnabled(True)
            QMessageBox.critical(
                self,
                "Open bucket failed",
                f"Cannot open bucket '{bucket_name}': {err_msg}",
            )
            # A Back/Forward into this bucket never arrived; its trail
            # position must not claim it did -- and a reopen that failed is
            # no longer what the tab restore should wait for.
            self._abandon_history_step()
            self._tab_restore_target = None
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
        # Until this listing lands, the rows on screen belong to where the
        # model WAS; listing_settled() refuses selection actions meanwhile.
        self._nav_in_flight = True
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

    def _current_location(self) -> dict:
        return self._new_tab_entry(self.data_model.bucket or "",
                                   self.data_model.current_folder or "")

    @staticmethod
    def _entry_location(entry) -> tuple:
        return (entry.get("bucket", ""), entry.get("prefix", ""))

    def open_location(self, bucket, prefix):
        """Go to a bucket/prefix, entering the bucket first when it differs."""
        if self._refuse_navigation():
            return
        if not bucket:
            self._return_to_bucket_list_mode()
            self.navigate()
            return
        if bucket == (self.data_model.bucket or ""):
            self._go_to_prefix(prefix or "", select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix or "")

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
        raw = self.store.get("listing_limit", DEFAULT_LISTING_LIMIT)
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
        if self._closing:
            return
        if seq != self._nav_seq:
            return

        self._nav_in_flight = False
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
            # REGRESSION (critical): this used to return with the model on
            # the prefix that failed and the previous rows re-enabled, so
            # Delete on a visible root "report.csv" removed
            # "private/report.csv" and a double-click descended from the
            # wrong place. The model goes back to what is on screen.
            stayed = self._recover_from_failed_listing()
            self._abandon_history_step()
            if stayed:
                self._revert_tab_to_view()
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
            # The rows are the listing of the payload's prefix, whatever the
            # model was nudged to since; key resolution follows the rows.
            if payload.get("bucket") == (self.data_model.bucket or ""):
                self.data_model.current_folder = payload.get("prefix") or ""
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

    def goUp(self):

        if not self.data_model.bucket:
            return

        if self._refuse_navigation():
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
        if self._refuse_navigation():
            return
        self._return_to_bucket_list_mode()
        self.navigate()

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
        self._go_to_prefix(prefix or "", select_up_entry=(not prefix))

    def _last_location_key(self) -> str:
        return self.store.profile_key("last_location")

    def _save_last_location(self):
        """Remember where this profile was browsing, per profile."""
        with self.store.group():
            self.settings.setValue(
                self._last_location_key(),
                serialize_location(self.data_model.bucket,
                                   self.data_model.current_folder))

    def restore_last_location(self) -> str:
        """
        Reopen the stored location, if there is one.

        Returns the bucket it is opening, or "" when it stays on the bucket
        list. Entering a bucket makes S3 calls, so this goes through the same
        async path as a double-click — and a bucket that has since been
        deleted simply leaves the bucket list showing.
        """
        with self.store.group():
            stored = self.settings.value(self._last_location_key(), "") or ""
        bucket, prefix = parse_location(stored)
        if not bucket:
            return ""
        self.log(f"reopening {serialize_location(bucket, prefix)}")
        # The tab restore waits for this landing rather than settling on the
        # bucket list that usually arrives first (see _restore_tab_for).
        self._tab_restore_target = (bucket, prefix)
        self.enter_bucket_async(bucket, target_prefix=prefix)
        return bucket

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
            self._go_to_prefix(prefix, select_up_entry=True)
            return
        self.enter_bucket_async(bucket, target_prefix=prefix)

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
        self._go_to_prefix(parent, restore_name=name)
