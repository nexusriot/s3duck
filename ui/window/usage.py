"""Asking what a bucket holds, and showing the answer."""

from PyQt6.QtCore import QModelIndex, QThread
from model import FSObjectType
from ui.constants import UP_ENTRY_LABEL
from ui.dialogs.usage import BucketUsageDialog, SizeExplorerDialog
from utils import release_worker_on_finish
from workers.usage import UsageWorker


class UsageMixin:
    """Asking what a bucket holds, and showing the answer."""

    def _show_bucket_usage_dialog(self, bucket_name: str, prefix: str = ""):
        if self._bucket_usage_dialog is None:
            self._bucket_usage_dialog = BucketUsageDialog(bucket_name, prefix, self)
        self._bucket_usage_dialog.set_calculating(bucket_name, prefix)
        self._bucket_usage_dialog.show()
        self._bucket_usage_dialog.raise_()
        self._bucket_usage_dialog.activateWindow()

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

    def open_size_explorer(self):
        """Drill into the bucket by prefix, sized by what it holds."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return
        SizeExplorerDialog(self, self, self.data_model,
                           self.data_model.current_folder or "").exec()
