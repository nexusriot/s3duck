"""The file listing: the view, its rows and the sort that pins [..] on top.
"""

import fnmatch
import os
from PyQt6.QtCore import (
    QMimeData, QModelIndex, QSortFilterProxyModel, QUrl, Qt,
)
from PyQt6.QtGui import QDrag, QStandardItem
from PyQt6.QtWidgets import QAbstractItemView, QTreeView
from core.naming import _to_epoch
from core.transfer_jobs import _build_upload_job_for_path
from model import FSObjectType


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
            job, if_none_match = self.parent._guard_upload(job)
            if not job:
                return
            self.parent.assign_thread_operation(
                "upload", job, if_none_match=if_none_match)
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
