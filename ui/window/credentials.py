"""Session credentials: how long they last and how they are renewed."""

from PyQt6.QtWidgets import QDialog, QMessageBox
from profile_switcher import ProfileSwitchWindow
from utils import (
    CredentialError, CredentialProcessError, expiry_state, load_aws_profiles,
    require_crypto, resolve_credential_key, run_credential_process,
    run_with_progress,
)


class CredentialsMixin:
    """Session credentials: how long they last and how they are renewed."""

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
        # Before the name changes: the location is stored per profile, so the
        # one being left behind has to record where it was first.
        self._save_last_location()

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
        self.data_model.session_token = getattr(prof, "session_token", "") or ""
        self.data_model.read_only = bool(getattr(prof, "read_only", False))
        self.data_model.requester_pays = bool(
            getattr(prof, "requester_pays", False))
        self.data_model.public_base_url = str(
            getattr(prof, "public_base_url", "") or "").rstrip("/")
        self.data_model.proxy_url = str(getattr(prof, "proxy_url", "") or "")
        self.data_model.ca_bundle = str(getattr(prof, "ca_bundle", "") or "")
        self.session_expires = str(getattr(prof, "session_expires", "") or "")
        self.aws_profile = str(getattr(prof, "aws_profile", "") or "")
        self.credential_process = str(
            getattr(prof, "credential_process", "") or "")
        self.set_accent(getattr(prof, "color", ""))
        self.data_model.binding_cache.clear()
        self._clear_undo_delete()
        self._clipboard = None

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

        self._load_binding_cache()
        self._load_bookmarks()
        self._rebuild_bookmark_menu()
        self._update_credential_status()
        if getattr(self, "actRefreshCredentials", None) is not None:
            self.actRefreshCredentials.setEnabled(
                self.can_refresh_credentials())
        # Tabs are locations inside one account, so the other profile's set
        # is meaningless here.
        self._load_tabs()
        # Same for a watch: its bucket belongs to the account being left.
        if self.watch_active():
            self.stop_watch()
        self._load_watch_config()
        self.navigate(show_loading=True)
        self.update_window_title()

        if old_profile and old_profile != self.profile_name:
            self.log(f"Profile switched: {old_profile} → {self.profile_name}")
        else:
            self.log(f"Profile switched to: {self.profile_name}")

    def credential_state(self):
        """This profile's credential lifetime as ``(state, label)``."""
        return expiry_state(getattr(self, "session_expires", ""))

    def can_refresh_credentials(self) -> bool:
        return bool(getattr(self, "credential_process", "")
                    or getattr(self, "aws_profile", ""))

    def _update_credential_status(self):
        """Keep the status-bar countdown in step with the clock."""
        state, label = self.credential_state()
        if state == "none":
            self.credential_status.hide()
            return
        if state == "expired":
            text, colour = "credentials expired", "#c62828"
        elif state == "soon":
            text, colour = f"credentials {label}", "#b26a00"
        else:
            text, colour = f"credentials {label}", ""
        self.credential_status.setText(text)
        self.credential_status.setStyleSheet(
            f"color: {colour};" if colour else "")
        tip = "Tools -> Refresh credentials" if self.can_refresh_credentials() \
            else "No credential process configured for this profile"
        self.credential_status.setToolTip(tip)
        self.credential_status.show()

    def _persist_refreshed_credentials(self, minted) -> bool:
        """
        Write freshly minted credentials onto this profile's stored row.

        Without this the refresh would last only until the window closed, and
        the launcher would still show the lapsed expiry.
        """
        try:
            crypto = require_crypto(resolve_credential_key(self.settings))
        except CredentialError:
            return False
        # The group is held by a context manager rather than closed by hand on
        # each path: there were two endGroup calls, one per exit, and neither
        # ran if crypto.encrypt raised -- which left the group open, so every
        # later setting in this session read and wrote under "profiles/".
        with self.store.group("profiles"):
            count = self.settings.beginReadArray("profiles")
            index = -1
            for position in range(count):
                self.settings.setArrayIndex(position)
                if str(self.settings.value("name", "")) == str(self.profile_name):
                    index = position
                    break
            self.settings.endArray()
            if index < 0:
                return False
            self.settings.beginWriteArray("profiles", count)
            try:
                self.settings.setArrayIndex(index)
                self.settings.setValue(
                    "access_key", crypto.encrypt(minted.get("access_key", "")))
                self.settings.setValue(
                    "secret_key", crypto.encrypt(minted.get("secret_key", "")))
                self.settings.setValue(
                    "session_token",
                    crypto.encrypt(minted.get("session_token", "")))
                self.settings.setValue("session_expires",
                                       minted.get("expires", ""))
            finally:
                self.settings.endArray()
        return True

    def _mint_credentials(self):
        """Fresh credentials from the credential process or ~/.aws."""
        if getattr(self, "credential_process", ""):
            command = self.credential_process

            def _mint(_worker):
                return run_credential_process(command)

            minted, exc = run_with_progress(
                self, "Refreshing credentials…", _mint)
            if minted is None and exc is None:
                return None
            if exc is not None:
                raise CredentialProcessError(str(exc))
            return minted
        name = getattr(self, "aws_profile", "")
        if not name:
            raise CredentialProcessError(
                "This profile has no credential process and was not imported "
                "from ~/.aws.")
        entry = load_aws_profiles().get(name)
        if not entry or not entry.get("access_key"):
            raise CredentialProcessError(
                f"Profile '{name}' has no usable credentials in ~/.aws.")
        return {
            "access_key": entry.get("access_key", ""),
            "secret_key": entry.get("secret_key", ""),
            "session_token": entry.get("session_token", ""),
            "expires": entry.get("expires", ""),
        }

    def refresh_credentials(self):
        """
        Replace this session's credentials without reconnecting the profile.

        The bucket bindings and the current listing survive, because only the
        keys change; the cached client is dropped so the next call builds one
        with them.
        """
        try:
            minted = self._mint_credentials()
        except CredentialProcessError as exc:
            QMessageBox.critical(self, "Refresh credentials", str(exc))
            return False
        if minted is None:
            return False
        self.data_model.access_key = minted.get("access_key", "")
        self.data_model.secret_key = minted.get("secret_key", "")
        self.data_model.session_token = minted.get("session_token", "")
        self.data_model._client = None
        self.session_expires = minted.get("expires", "")
        self._persist_refreshed_credentials(minted)
        self._update_credential_status()
        _state, label = self.credential_state()
        self.log(f"credentials refreshed ({label or 'no expiry reported'})")
        self.statusBar().showMessage("Credentials refreshed", 4000)
        return True
