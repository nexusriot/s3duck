"""Delete-to-trash for buckets without versioning, and the way back."""

from PyQt6.QtWidgets import QMessageBox
from core.naming import _human_bytes
from core.trash import TRASH_PREFIX, in_trash, restore_rows, split_for_trash


class TrashMixin:
    """Delete-to-trash for buckets without versioning, and the way back."""

    def trash_enabled(self) -> bool:
        return self.store.get_bool(self.store.profile_key("use_trash"), False)

    def set_trash_enabled(self, enabled):
        self.store.set(self.store.profile_key("use_trash"),
                       "true" if enabled else "false")
        if getattr(self, "actUseTrash", None) is not None:
            self.actUseTrash.setChecked(bool(enabled))
        self.log(f"delete moves to {TRASH_PREFIX}: {'on' if enabled else 'off'}")

    def queue_delete(self, keys):
        """
        Queue a confirmed delete, honouring the trash convention.

        Returns ``(moved, deleted)`` counts. A key already in the trash is
        deleted outright; everything else is moved under ``.trash/``.

        A trashed copy from an earlier delete of the same key is asked about
        rather than replaced: the confirmation promised the user it could be
        restored, and the second delete would otherwise destroy it.
        """
        keys = [k for k in keys if k]
        if not self.trash_enabled():
            if keys:
                self.assign_thread_operation("delete", keys)
            return 0, len(keys)
        moves, deletes = split_for_trash(keys, self._selected_storage_classes())
        if moves:
            conflicts = self._destination_conflicts([row[1] for row in moves])
            if conflicts is None:
                return 0, 0
            moves = self._resolve_overwrites(
                moves, conflicts, what="trashed copy",
                index_of=lambda row: row[1]) or []
            if not moves and not deletes:
                return 0, 0
        if moves:
            self.assign_thread_operation("move", moves)
        if deletes:
            self.assign_thread_operation("delete", deletes)
        return len(moves), len(deletes)

    def delete_verb(self, keys) -> str:
        """What the confirmation should call this delete."""
        if not self.trash_enabled() or all(in_trash(k) for k in keys):
            return "Delete"
        if any(in_trash(k) for k in keys):
            # A mixed selection (from search, say): the part already in the
            # trash is gone for good, and the prompt has to say so.
            return (f"Move to {TRASH_PREFIX} (what is already in it is "
                    "deleted for good)")
        return f"Move to {TRASH_PREFIX}"

    def restore_from_trash(self, keys=None):
        """Move the selected trashed objects back to their original keys."""
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return False
        if keys is None:
            keys = [key for _name, key, _folder in self._collect_selected_targets()]
        rows = restore_rows(keys, self._selected_storage_classes())
        if not rows:
            self.statusBar().showMessage(
                f"Select something inside {TRASH_PREFIX}", 3000)
            return False
        conflicts = self._destination_conflicts([row[1] for row in rows])
        if conflicts is None:
            return False
        rows = self._resolve_overwrites(
            rows, conflicts, what="destination", index_of=lambda row: row[1])
        if not rows:
            return False
        self.assign_thread_operation("move", rows)
        self.statusBar().showMessage(
            f"Restoring {len(rows)} item(s) from the trash…", 4000)
        return True

    def empty_trash(self):
        """Delete everything under ``.trash/`` in this bucket, for good."""
        if self.in_bucket_list_mode() or not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return False
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return False
        clone = self.data_model.clone_for_worker()

        def _scan(_w):
            count = total = 0
            for key, size in clone.get_keys(TRASH_PREFIX):
                if key and not key.endswith("/"):
                    count += 1
                    total += int(size or 0)
            return count, total

        result, exc = self._run_with_progress("Measuring the trash…", _scan)
        if result is None and exc is None:
            return False
        if exc is not None:
            QMessageBox.warning(self, "Empty trash", str(exc))
            return False
        count, total = result
        if not count:
            self.statusBar().showMessage("The trash is empty", 3000)
            return False
        if QMessageBox.question(
                self, "Empty trash",
                f"Permanently delete {count} object(s), {_human_bytes(total)}, "
                f"from {self.data_model.bucket}/{TRASH_PREFIX}?\n\n"
                "This cannot be undone.") != QMessageBox.StandardButton.Yes:
            return False
        self.assign_thread_operation("delete", [TRASH_PREFIX])
        return True
