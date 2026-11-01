"""Opening the point-in-time restore for a prefix."""

from PyQt6.QtWidgets import QMessageBox
from ui.dialogs.point_in_time import PointInTimeDialog
from utils import run_with_progress


class PointInTimeMixin:
    """Opening the point-in-time restore for a prefix."""

    def open_point_in_time(self, prefix=None):
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return None
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return None
        if prefix is None:
            prefix = self.data_model.current_folder or ""
        clone = self.data_model.clone_for_worker()
        status, exc = run_with_progress(
            self, "Checking versioning…",
            lambda _w: clone.get_bucket_versioning_status() or "off")
        if status is None and exc is None:
            return None
        if exc is not None:
            QMessageBox.warning(self, "Restore to a point in time", str(exc))
            return None
        dlg = PointInTimeDialog(self, self, self.data_model, prefix,
                                versioning=status)
        dlg.exec()
        return dlg
