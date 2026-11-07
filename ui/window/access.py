"""Permissions and protection: ACLs, retention and the session's SSE-C key."""

from PyQt6.QtWidgets import QDialog, QMessageBox
from core.retention import parse_retain_until
from ui.constants import UP_ENTRY_LABEL
from ui.dialogs.access import AclDialog, RetentionDialog, SseCKeyDialog, sse_c_fingerprint
from utils import run_with_progress


class AccessWindowMixin:
    """Permissions and protection: ACLs, retention and the session's SSE-C key."""

    def _read_off_thread(self, title, fn):
        """``fn(clone)`` behind a busy dialog; None when cancelled or failed."""
        clone = self.data_model.clone_for_worker()
        result, exc = run_with_progress(self, title, lambda _w: fn(clone))
        if result is None and exc is None:
            return None
        if exc is not None:
            QMessageBox.warning(self, title.rstrip("…"), str(exc))
            return None
        return result

    def _explain_acl_refusal(self, exc):
        """An ACL write refused because ACLs are switched off says so."""
        text = str(exc)
        if "AccessControlListNotSupported" in text:
            return ("This bucket's Object Ownership is BucketOwnerEnforced, "
                    "which disables ACLs entirely. Use the bucket policy "
                    "instead.")
        return text

    def edit_object_acl(self, key):
        if not key or key.rstrip("/") == UP_ENTRY_LABEL:
            return None
        acl = self._read_off_thread("Reading the ACL…",
                                    lambda clone: clone.get_object_acl(key))
        if acl is None:
            return None

        def _save(owner, grants):
            try:
                self.data_model.put_object_acl_grants(
                    key, owner, grants, log_fn=self.log)
            except Exception as exc:
                raise RuntimeError(self._explain_acl_refusal(exc)) from exc

        dlg = AclDialog(self, key, acl, writable=not self.is_read_only(),
                        save=_save)
        dlg.exec()
        return dlg

    def edit_bucket_acl(self, parent=None):
        if not self.data_model.bucket:
            self.statusBar().showMessage("Open a bucket first", 2000)
            return None
        acl = self._read_off_thread("Reading the bucket ACL…",
                                    lambda clone: clone.get_bucket_acl())
        if acl is None:
            return None

        def _save(owner, grants):
            try:
                self.data_model.put_bucket_acl(owner, grants, log_fn=self.log)
            except Exception as exc:
                raise RuntimeError(self._explain_acl_refusal(exc)) from exc

        dlg = AclDialog(parent or self, self.data_model.bucket, acl,
                        writable=not self.is_read_only(), save=_save)
        dlg.exec()
        return dlg

    def set_object_retention_ui(self, key):
        """Choose a retention mode and date for one object, then apply it."""
        if not key or key.endswith("/"):
            self.statusBar().showMessage("Retention is for files only", 2000)
            return False
        if self.is_read_only():
            self.statusBar().showMessage("Profile is read-only", 2000)
            return False
        state = self._read_off_thread(
            "Reading the lock state…",
            lambda clone: clone.get_object_lock_state(key))
        if state is None:
            return False
        if not state.get("supported"):
            QMessageBox.information(
                self, "Retention",
                "Object Lock is not enabled on this bucket, so no retention "
                "can be set. It can only be turned on when a bucket is "
                "created.")
            return False
        state = dict(state)
        state["retain_until_dt"] = parse_retain_until(state.get("retain_until"))
        dlg = RetentionDialog(self, key, state)
        if dlg.exec() != QDialog.DialogCode.Accepted or not dlg.assess():
            return False
        mode, until, bypass = dlg.chosen()
        try:
            self.data_model.set_object_retention(
                key, mode, until, bypass_governance=bypass, log_fn=self.log)
        except Exception as exc:
            QMessageBox.critical(self, "Retention", str(exc))
            return False
        self.statusBar().showMessage(
            f"Retention {mode} until {until:%Y-%m-%d %H:%M} UTC", 5000)
        return True

    def sse_c_key_ui(self):
        """Set or clear the session's customer-provided encryption key."""
        dlg = SseCKeyDialog(self, getattr(self.data_model, "sse_c_key", b""))
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return False
        try:
            key = dlg.key_bytes()
        except ValueError as exc:
            QMessageBox.warning(self, "SSE-C", str(exc))
            return False
        self.apply_sse_c_key(key)
        return True

    def apply_sse_c_key(self, key):
        self.invalidate_listing_cache()
        self.data_model.set_sse_c_key(key)
        if key:
            self.log(f"SSE-C on for this session (key MD5 {sse_c_fingerprint(key)})")
        else:
            self.log("SSE-C off")
        self.update_window_title()
