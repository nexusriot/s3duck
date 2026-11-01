"""Inventory reports and S3 Select, opened from the window."""

from PyQt6.QtWidgets import QMessageBox
from ui.dialogs.reports import InventoryDialog, SelectDialog, select_format
from utils import run_with_progress


class ReportsWindowMixin:
    """Inventory reports and S3 Select, opened from the window."""

    def open_inventory(self):
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return None
        clone = self.data_model.clone_for_worker()
        configs, exc = run_with_progress(
            self, "Reading inventory configurations…",
            lambda _w: clone.list_inventory_configurations())
        if configs is None and exc is None:
            return None
        if exc is not None:
            QMessageBox.warning(self, "Inventory", str(exc))
            configs = []
        dlg = InventoryDialog(self, self.data_model, configs)
        dlg.exec()
        return dlg

    def query_with_select(self, key):
        if not select_format(key):
            self.statusBar().showMessage(
                "S3 Select reads CSV, TSV and JSON objects", 3000)
            return None
        dlg = SelectDialog(self, self.data_model, key)
        dlg.exec()
        return dlg
