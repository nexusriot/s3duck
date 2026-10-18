"""Credential storage, expiry and refresh."""

import base64
import inspect
import json
import main_window
import os
import profile_switcher
import s3duck
import shutil
import tempfile
import time
import unittest
import utils
from PyQt6.QtCore import QByteArray, QEvent, QRect, QSettings, Qt
from PyQt6.QtGui import (
    QColor, QFont, QFontMetrics, QKeyEvent, QPainter, QPalette, QPixmap,
)
from PyQt6.QtWidgets import QMessageBox, QStyle, QStyleOptionViewItem
from core.profile import ProfileSettings
from cryptography.fernet import Fernet
from datetime import datetime, timezone
from s3duck import (
    SettingsItem, load_profile_secrets, profile_badges, profile_summary,
    selected_row_index,
)
from tests.support import (
    PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window, patch_app_wide,
)
from unittest.mock import patch
from utils import (
    BundleError, CredentialError, Crypto, export_profile_bundle,
    import_profile_bundle, require_crypto,
)


class ProfileSettingsTests(unittest.TestCase):
    """MainWindow takes its profile by name, not by position."""

    def test_the_constructor_refuses_a_positional_settings_tuple(self):
        """The padded 20-tuple is gone and must not come back."""
        sig = inspect.signature(main_window.MainWindow.__init__)
        self.assertNotIn("settings", sig.parameters)
        for required in ("current_dir", "qsettings", "profile"):
            self.assertIn(required, sig.parameters)
            self.assertEqual(sig.parameters[required].kind,
                             inspect.Parameter.KEYWORD_ONLY)

    def test_every_field_has_a_default_so_a_short_caller_is_not_padded(self):
        blank = ProfileSettings()
        self.assertEqual(blank.profile_name, "")
        self.assertIs(blank.read_only, False)
        self.assertIs(blank.requester_pays, False)

    def test_the_profile_is_frozen(self):
        profile = ProfileSettings(profile_name="prod")
        with self.assertRaises(Exception):
            profile.profile_name = "dev"
        self.assertEqual(profile.replace(profile_name="dev").profile_name,
                         "dev")
        self.assertEqual(profile.profile_name, "prod")

    def test_secrets_can_be_dropped_for_logging(self):
        profile = ProfileSettings(access_key="AK", secret_key="SK",
                                  session_token="TOK", region="eu-west-1")
        safe = profile.without_secrets()
        self.assertEqual((safe.access_key, safe.secret_key,
                          safe.session_token), ("", "", ""))
        self.assertEqual(safe.region, "eu-west-1")
class ProfileBundleTests(unittest.TestCase):
    """Export must not write credentials in the clear, and import must reject
    a wrong passphrase or a tampered file."""

    PROFILES = [
        {"name": "prod", "url": "https://s3.amazonaws.com",
         "access_key": "AKPROD", "secret_key": "SUPERSECRET",
         "session_token": "", "read_only": "true"},
        {"name": "dev", "url": "https://minio.local", "access_key": "AKDEV",
         "secret_key": "devsecret", "session_token": "tok"},
    ]

    def test_round_trip(self):
        blob = export_profile_bundle(self.PROFILES, "correct horse")
        restored = import_profile_bundle(blob, "correct horse")
        self.assertEqual(restored, self.PROFILES)

    def test_secrets_are_not_in_the_file(self):
        blob = export_profile_bundle(self.PROFILES, "pw")
        self.assertNotIn(b"SUPERSECRET", blob)
        self.assertNotIn(b"AKPROD", blob)
        # the envelope itself stays readable JSON
        document = json.loads(blob.decode())
        self.assertEqual(document["version"], 1)
        self.assertIn("salt", document)

    def test_wrong_passphrase_is_rejected(self):
        blob = export_profile_bundle(self.PROFILES, "right")
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(blob, "wrong")
        self.assertIn("passphrase", str(ctx.exception).lower())

    def test_tampered_payload_is_rejected(self):
        blob = export_profile_bundle(self.PROFILES, "pw")
        document = json.loads(blob.decode())
        document["data"] = document["data"][:-6] + "AAAAA="
        with self.assertRaises(BundleError):
            import_profile_bundle(json.dumps(document).encode(), "pw")

    def test_salt_differs_between_exports(self):
        first = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        second = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        self.assertNotEqual(first["salt"], second["salt"])

    def test_garbage_and_missing_fields(self):
        with self.assertRaises(BundleError):
            import_profile_bundle(b"not json at all", "pw")
        with self.assertRaises(BundleError):
            import_profile_bundle(b'{"hello": 1}', "pw")

    def test_unsupported_version(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["version"] = 99
        with self.assertRaises(BundleError):
            import_profile_bundle(json.dumps(document).encode(), "pw")

    def test_export_requires_a_passphrase(self):
        with self.assertRaises(BundleError):
            export_profile_bundle(self.PROFILES, "")

    @staticmethod
    def _forge(payload, passphrase="pw"):
        """A well-formed, correctly-encrypted bundle wrapping arbitrary bytes,
        so the checks *after* decryption can be reached."""
        salt = b"0123456789abcdef"
        token = Fernet(utils._bundle_key(passphrase, salt)).encrypt(payload)
        return json.dumps({
            "version": utils.PROFILE_BUNDLE_VERSION,
            "salt": base64.b64encode(salt).decode(),
            "data": token.decode(),
        }).encode()

    def test_non_string_data_field_is_rejected(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["data"] = 12345
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(json.dumps(document).encode(), "pw")
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_payload_that_is_not_json_is_rejected(self):
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(self._forge(b"\x00not json"), "pw")
        self.assertIn("corrupt", str(ctx.exception).lower())

    def test_payload_that_is_not_a_list_is_rejected(self):
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(self._forge(b'{"name": "prod"}'), "pw")
        self.assertIn("list of profiles", str(ctx.exception))

    def test_corrupt_salt_is_rejected(self):
        document = json.loads(export_profile_bundle(self.PROFILES, "pw").decode())
        document["salt"] = "!!! not base64 !!!"
        with self.assertRaises(BundleError) as ctx:
            import_profile_bundle(json.dumps(document).encode(), "pw")
        self.assertIn("salt", str(ctx.exception).lower())

    def test_unicode_passphrase_round_trips(self):
        blob = export_profile_bundle(self.PROFILES, "паролü-🦆")
        self.assertEqual(import_profile_bundle(blob, "паролü-🦆"), self.PROFILES)
        with self.assertRaises(BundleError):
            import_profile_bundle(blob, "паролu-🦆")

    def test_bundle_key_is_bound_to_the_salt(self):
        """Why every export re-salts: same passphrase must not yield the same
        key twice, or two bundles would be interchangeable ciphertexts."""
        salt = b"0123456789abcdef"
        self.assertEqual(utils._bundle_key("pw", salt), utils._bundle_key("pw", salt))
        self.assertNotEqual(utils._bundle_key("pw", salt),
                            utils._bundle_key("pw", b"fedcba9876543210"))
        self.assertNotEqual(utils._bundle_key("pw", salt),
                            utils._bundle_key("other", salt))

    def test_key_derivation_is_not_weakened(self):
        """A security parameter, not an implementation detail — a careless edit
        that drops it turns the passphrase into a fast offline guess."""
        self.assertGreaterEqual(utils.PROFILE_BUNDLE_ITERATIONS, 480000)
        self.assertEqual(len(base64.urlsafe_b64decode(
            utils._bundle_key("pw", b"0123456789abcdef"))), 32)

    def test_a_bundle_carries_profiles_to_another_installation(self):
        """The end-to-end story the bundle exists for: credentials encrypted
        under this machine's Fernet key are exported under a passphrase and
        re-encrypted under a *different* machine's key, still readable."""
        source_key, target_key = Crypto.generate_key(), Crypto.generate_key()
        self.assertNotEqual(source_key, target_key)
        source = Crypto(source_key)
        stored = SettingsItem(
            name="prod", url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="b", enc_access_key=source.encrypt("AKPROD"),
            enc_secret_key=source.encrypt("SUPERSECRET"), no_ssl_check="false",
            use_path="false", enc_session_token=source.encrypt("tok"))

        access, secret, token = load_profile_secrets(source_key, stored)
        blob = export_profile_bundle(
            [{"name": stored.name, "access_key": access,
              "secret_key": secret, "session_token": token}], "transfer pw")
        self.assertNotIn(b"SUPERSECRET", blob)

        restored = import_profile_bundle(blob, "transfer pw")[0]
        target = Crypto(target_key)
        migrated = SettingsItem(
            name=restored["name"], url="", region="", bucket_name="",
            enc_access_key=target.encrypt(restored["access_key"]),
            enc_secret_key=target.encrypt(restored["secret_key"]),
            no_ssl_check="false", use_path="false",
            enc_session_token=target.encrypt(restored["session_token"]))

        self.assertEqual(load_profile_secrets(target_key, migrated),
                         ("AKPROD", "SUPERSECRET", "tok"))
        # and the old key is now useless against the migrated copy
        with self.assertRaises(CredentialError):
            load_profile_secrets(source_key, migrated)
class LauncherAccentTests(unittest.TestCase):
    """The same colour marks the row in the picker and the open window."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, items):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests", "s3duck-tests")
        win.items = list(items)
        win.populate_list()
        self.addCleanup(win.close)
        return win

    @staticmethod
    def _item(color=""):
        return SettingsItem(
            "prod", "https://s3.amazonaws.com", "us-east-1", "", b"", b"",
            "false", "false", b"", "false", color)

    def test_the_row_carries_the_colour(self):
        win = self._launcher([self._item("#c62828")])
        self.assertEqual(
            win.listWidget.item(0).data(Qt.ItemDataRole.UserRole + 1),
            "#c62828")

    def _swatches(self, win):
        filled = []
        delegate = win.listWidget.itemDelegate()
        pixmap = QPixmap(400, 60)
        pixmap.fill()
        painter = QPainter(pixmap)
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, 400, 60)
        option.font = win.listWidget.font()
        option.palette = win.listWidget.palette()
        with patch.object(QPainter, "fillRect",
                          lambda _s, rect, colour: filled.append(
                              (rect.width(), QColor(colour).name()))):
            delegate.paint(painter, option,
                           win.listWidget.model().index(0, 0))
        painter.end()
        return filled

    def test_a_coloured_profile_gets_a_swatch(self):
        win = self._launcher([self._item("#c62828")])
        self.assertIn((4, "#c62828"), self._swatches(win))

    def test_an_uncoloured_profile_gets_no_swatch(self):
        win = self._launcher([self._item("")])
        self.assertEqual(self._swatches(win), [])

    def test_a_malformed_colour_paints_nothing(self):
        win = self._launcher([self._item("nonsense")])
        self.assertEqual(self._swatches(win), [])
class CredentialGuardTests(unittest.TestCase):
    """REGRESSION (crash): a missing, malformed or mismatched encryption key
    raised AttributeError / ValueError / InvalidToken straight out of a Qt slot
    (onStart, onEdit, check_profile, export), and PyQt6 turns an exception that
    escapes a slot into a process ABORT — the launcher died instead of saying
    the credentials could not be read. profile_switcher.py already handled the
    same failure gracefully; this path did not."""

    def _crypto(self):
        return Crypto(Fernet.generate_key().decode())

    def _item(self, crypto):
        return SettingsItem(
            "prod", "https://s3.amazonaws.com", "us-east-1", "",
            crypto.encrypt("AKID"), crypto.encrypt("SECRET"),
            "false", "true", crypto.encrypt("TOKEN"), "false",
        )

    def test_missing_key_reports_instead_of_crashing(self):
        """A missing key and a corrupt key need different remedies, so the two
        messages must stay distinct rather than collapsing into one."""
        item = self._item(self._crypto())
        for missing in (None, ""):
            with self.subTest(key=missing):
                with self.assertRaises(CredentialError) as ctx:
                    load_profile_secrets(missing, item)
                self.assertIn("missing", str(ctx.exception).lower())
                self.assertIn("common/key", str(ctx.exception))

    def test_malformed_key_reports_instead_of_crashing(self):
        item = self._item(self._crypto())
        with self.assertRaises(CredentialError) as ctx:
            load_profile_secrets("this-is-not-a-fernet-key", item)
        # not the "missing key" wording — the key is present but unusable
        self.assertNotIn("missing", str(ctx.exception).lower())

    def test_wrong_key_reports_instead_of_crashing(self):
        """Settings restored from another machine, or a regenerated key."""
        item = self._item(self._crypto())
        other = Fernet.generate_key().decode()
        with self.assertRaises(CredentialError) as ctx:
            load_profile_secrets(other, item)
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_corrupt_blob_reports_instead_of_crashing(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_secret_key = b"not-a-token"
        with self.assertRaises(CredentialError):
            load_profile_secrets(crypto.key, item)

    def test_absent_blob_reports_instead_of_crashing(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_access_key = None
        with self.assertRaises(CredentialError):
            load_profile_secrets(crypto.key, item)

    def test_good_key_round_trips(self):
        crypto = self._crypto()
        item = self._item(crypto)
        self.assertEqual(
            load_profile_secrets(crypto.key, item), ("AKID", "SECRET", "TOKEN"))

    def test_optional_token_absent_is_not_an_error(self):
        crypto = self._crypto()
        item = self._item(crypto)
        item.enc_session_token = ""
        self.assertEqual(load_profile_secrets(crypto.key, item)[2], "")

    def test_encrypting_with_a_bad_key_reports_instead_of_crashing(self):
        for bad in (None, "", "not-a-fernet-key"):
            with self.subTest(key=bad):
                with self.assertRaises(CredentialError):
                    Crypto(bad).encrypt("secret")

    def test_require_crypto_validates_eagerly(self):
        """The write paths must fail before a half-written profile is stored."""
        with self.assertRaises(CredentialError):
            require_crypto(None)
        with self.assertRaises(CredentialError):
            require_crypto("not-a-fernet-key")
        good = Fernet.generate_key().decode()
        self.assertEqual(require_crypto(good).decrypt(
            require_crypto(good).encrypt("x")), "x")
class SharedCryptoTests(unittest.TestCase):
    """The launcher and the runtime profile switcher must use ONE Crypto.
    They previously had separate classes: only the launcher's was hardened,
    and the switcher's coerced values differently."""

    def test_one_class_shared_by_every_module(self):
        self.assertIs(s3duck.Crypto, utils.Crypto)
        self.assertIs(profile_switcher.Crypto, utils.Crypto)
        self.assertIs(s3duck.CredentialError, utils.CredentialError)

    def test_decrypt_accepts_bytes_str_and_qbytearray(self):
        """QSettings hands back bytes or QByteArray depending on the backend.
        REGRESSION: the switcher coerced with `str(value).encode()`, which for
        a QByteArray yields the literal repr b\"b'…'\" and never decrypts."""
        crypto = Crypto(Fernet.generate_key().decode())
        token = crypto.encrypt("secret")
        self.assertEqual(crypto.decrypt(token), "secret")
        self.assertEqual(crypto.decrypt(token.decode()), "secret")
        self.assertEqual(crypto.decrypt(QByteArray(token)), "secret")

    def test_missing_value_is_an_error_for_a_required_field(self):
        """An absent stored value and a key mismatch need different remedies,
        so blaming the key for an empty field would misdirect the user."""
        crypto = Crypto(Fernet.generate_key().decode())
        for empty in (None, ""):
            with self.subTest(value=empty):
                with self.assertRaises(CredentialError) as ctx:
                    crypto.decrypt(empty)
                self.assertIn("empty", str(ctx.exception).lower())
                self.assertNotIn("different one", str(ctx.exception))

    def test_decrypt_optional_is_shared_and_forgiving(self):
        crypto = Crypto(Fernet.generate_key().decode())
        self.assertEqual(utils.decrypt_optional(crypto, None), "")
        self.assertEqual(utils.decrypt_optional(crypto, b"garbage"), "")
        self.assertEqual(
            utils.decrypt_optional(crypto, crypto.encrypt("tok")), "tok")
class ProfileSummaryTests(unittest.TestCase):
    """The launcher list showed names only, so profiles differing only by
    endpoint/region were indistinguishable and the read-only and
    TLS-unverified flags were invisible until after connecting."""

    @staticmethod
    def _item(**kw):
        fields = dict(
            name="prod", url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="", enc_access_key=b"", enc_secret_key=b"",
            no_ssl_check="false", use_path="false", enc_session_token=b"",
            read_only="false")
        fields.update(kw)
        return SettingsItem(**fields)

    def test_endpoint_and_region(self):
        self.assertEqual(profile_summary(self._item()),
                         "https://s3.amazonaws.com · us-east-1")

    def test_bucket_is_included_when_pinned(self):
        self.assertIn("logs", profile_summary(self._item(bucket_name="logs")))

    def test_flags_are_badges_not_summary_text(self):
        """A safety flag appended to the dim endpoint line reads as more
        metadata, so the flags live beside the name instead."""
        item = self._item(read_only="true", no_ssl_check="true")
        self.assertEqual(profile_badges(item), ["read-only", "TLS unverified"])
        summary = profile_summary(item)
        self.assertNotIn("read-only", summary)
        self.assertNotIn("TLS unverified", summary)

    def test_badges_are_absent_when_flags_are_off(self):
        self.assertEqual(profile_badges(self._item()), [])

    def test_each_flag_stands_alone(self):
        self.assertEqual(profile_badges(self._item(read_only="true")),
                         ["read-only"])
        self.assertEqual(profile_badges(self._item(no_ssl_check="true")),
                         ["TLS unverified"])

    def test_missing_endpoint_is_called_out(self):
        self.assertIn("(no endpoint)", profile_summary(self._item(url="")))

    def test_blank_optional_fields_leave_no_empty_separators(self):
        summary = profile_summary(self._item(region="", bucket_name=""))
        self.assertEqual(summary, "https://s3.amazonaws.com")
        self.assertNotIn("··", summary)
        self.assertFalse(summary.endswith("·"))
class LauncherListTests(unittest.TestCase):
    """Rows, keyboard handling and the non-blocking connect probe."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _launcher(self, items=()):
        win = s3duck.Profiles()
        # Never write to the real config from a test.
        win.settings = QSettings("s3duck-tests", "s3duck-tests")
        win.items = list(items)
        win.populate_list()
        if win.items:
            win.listWidget.setCurrentRow(0)
        self.addCleanup(win.close)
        return win

    @staticmethod
    def _item(name="prod", **kw):
        fields = dict(
            name=name, url="https://s3.amazonaws.com", region="us-east-1",
            bucket_name="", enc_access_key=b"", enc_secret_key=b"",
            no_ssl_check="false", use_path="false", enc_session_token=b"",
            read_only="false")
        fields.update(kw)
        return SettingsItem(**fields)

    def test_rows_show_the_name_and_the_summary(self):
        win = self._launcher([self._item(read_only="true")])
        row = win.listWidget.item(0)
        first, _, second = row.text().partition("\n")
        self.assertEqual(first, "prod")
        self.assertIn("us-east-1", second)

    def test_rows_carry_their_badges_for_the_delegate(self):
        win = self._launcher([self._item(read_only="true", no_ssl_check="true"),
                              self._item("plain")])
        self.assertEqual(
            win.listWidget.item(0).data(Qt.ItemDataRole.UserRole),
            ["read-only", "TLS unverified"])
        self.assertEqual(
            win.listWidget.item(1).data(Qt.ItemDataRole.UserRole), [])

    def test_the_tooltip_still_carries_everything(self):
        """The row elides; the tooltip is the fallback for the full truth."""
        win = self._launcher([self._item(read_only="true")])
        tip = win.listWidget.item(0).toolTip()
        self.assertIn("s3.amazonaws.com", tip)
        self.assertIn("us-east-1", tip)
        self.assertIn("read-only", tip)

    def _paint_row(self, win, row, selected=False, width=400):
        """Drive the delegate directly. Qt swallows a Python error raised
        inside a virtual call into a process abort, so calling paint() from
        the test is what turns a mistake there into a test failure."""
        delegate = win.listWidget.itemDelegate()
        pixmap = QPixmap(width, 60)
        pixmap.fill()
        painter = QPainter(pixmap)
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, width, 60)
        option.font = win.listWidget.font()
        option.palette = win.listWidget.palette()
        if selected:
            option.state |= QStyle.StateFlag.State_Selected
        try:
            delegate.paint(painter, option,
                           win.listWidget.model().index(row, 0))
        finally:
            painter.end()
        return delegate, option

    def test_rows_paint_in_both_states(self):
        """REGRESSION: paint() referenced a name the launcher never imported;
        every unit test passed while the real window crashed on first draw."""
        win = self._launcher([self._item(read_only="true"),
                              self._item("dev", url="", region="")])
        for row in (0, 1):
            for selected in (False, True):
                with self.subTest(row=row, selected=selected):
                    self._paint_row(win, row, selected)

    def test_the_summary_font_is_smaller_but_stays_legible(self):
        base = QFont()
        base.setPointSizeF(11.0)
        self.assertLess(s3duck._summary_font(base).pointSizeF(), 11.0)

        tiny = QFont()
        tiny.setPointSizeF(7.0)
        self.assertGreaterEqual(s3duck._summary_font(tiny).pointSizeF(), 7.0)

    def test_a_pixel_sized_font_is_left_alone(self):
        """pointSizeF() is -1 for a pixel-sized font; subtracting from it
        would ask Qt for a negative point size."""
        pixels = QFont()
        pixels.setPixelSize(14)
        self.assertEqual(s3duck._summary_font(pixels).pixelSize(), 14)

    def test_the_list_uses_the_profile_delegate(self):
        win = self._launcher([self._item()])
        self.assertIsInstance(win.listWidget.itemDelegate(),
                              s3duck.ProfileRowDelegate)

    def test_badges_paint_and_keep_their_own_colours(self):
        win = self._launcher([self._item(read_only="true", no_ssl_check="true")])
        for selected in (False, True):
            with self.subTest(selected=selected):
                self._paint_row(win, 0, selected)

        palette = QPalette()
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        read_only = s3duck.badge_color("read-only", palette, False)
        insecure = s3duck.badge_color("TLS unverified", palette, False)
        self.assertNotEqual(read_only, insecure)
        self.assertEqual(read_only, s3duck.BADGE_COLORS["read-only"])

    def test_a_selected_badge_stays_readable_on_the_highlight(self):
        """Its own colour would sit on the highlight brush and lose contrast."""
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        self.assertEqual(s3duck.badge_color("read-only", palette, True),
                         QColor("#ffffff"))

    def test_a_long_name_cannot_push_a_badge_off_the_row(self):
        """A truncated safety flag is worse than a truncated name, so badge
        width is reserved before the name is elided."""
        long_name = "a" * 400
        win = self._launcher([self._item(long_name, read_only="true")])
        badge_width = QFontMetrics(
            s3duck._summary_font(win.listWidget.font())
        ).horizontalAdvance("[read-only]")

        painted = []
        with patch.object(QPainter, "drawText",
                          lambda self, x, y, text: painted.append((x, text))):
            _, option = self._paint_row(win, 0, width=240)

        drawn = [t for _, t in painted]
        self.assertIn("[read-only]", drawn)
        name_text = painted[0][1]
        self.assertTrue(name_text.endswith("…"), f"name not elided: {name_text}")
        # The badge must end inside the row. paint() insets by 6px a side, so
        # the usable right edge is rect.right() - 6.
        badge_x = next(x for x, t in painted if t == "[read-only]")
        self.assertLessEqual(badge_x + badge_width, option.rect.right() - 6)

    def test_a_selected_row_uses_the_highlight_pen(self):
        """Without the switch the name is drawn in the ordinary text colour on
        top of the highlight brush — dark on dark on most themes."""
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Text, QColor("#101010"))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
        self.assertEqual(s3duck.row_text_color(palette, True),
                         QColor("#ffffff"))
        self.assertEqual(s3duck.row_text_color(palette, False),
                         QColor("#101010"))

    def test_a_row_is_tall_enough_for_both_lines(self):
        win = self._launcher([self._item()])
        delegate, option = self._paint_row(win, 0)
        index = win.listWidget.model().index(0, 0)
        two_line = delegate.sizeHint(option, index).height()
        single = QFontMetrics(win.listWidget.font()).height()
        self.assertGreater(two_line, single * 1.5)

    def test_a_row_without_a_summary_stays_compact(self):
        win = self._launcher([self._item()])
        win.listWidget.item(0).setText("just-a-name")
        delegate, option = self._paint_row(win, 0)
        index = win.listWidget.model().index(0, 0)
        bare = delegate.sizeHint(option, index).height()
        win.listWidget.item(0).setText("just-a-name\nwith a summary")
        tall = delegate.sizeHint(option, index).height()
        self.assertLess(bare, tall)

    def test_buttons_do_not_steal_enter(self):
        """REGRESSION: QDialog buttons are autoDefault, so Enter on the list
        fired Add — the first button — instead of opening the profile."""
        win = self._launcher([self._item()])
        for button in (win.btnRun, win.btnAdd, win.btnEdit, win.btnDelete):
            with self.subTest(button=button.text()):
                self.assertFalse(button.autoDefault())
                self.assertFalse(button.isDefault())

    def _press(self, win, key):
        event = QKeyEvent(QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier)
        return win.eventFilter(win.listWidget, event)

    def test_keys_drive_the_matching_actions(self):
        for key, handler in ((Qt.Key.Key_Return, "onStart"),
                             (Qt.Key.Key_Enter, "onStart"),
                             (Qt.Key.Key_Delete, "onDelete"),
                             (Qt.Key.Key_F2, "onEdit")):
            with self.subTest(key=key):
                win = self._launcher([self._item()])
                with patch.object(s3duck.Profiles, handler) as called:
                    self.assertTrue(self._press(win, key))
                called.assert_called_once_with()

    def test_other_keys_are_left_to_the_list(self):
        """Arrow keys and type-ahead must keep working."""
        win = self._launcher([self._item()])
        for key in (Qt.Key.Key_Down, Qt.Key.Key_A, Qt.Key.Key_Escape):
            with self.subTest(key=key):
                self.assertFalse(self._press(win, key))

    def test_connect_probe_runs_off_the_gui_thread(self):
        """REGRESSION (freeze): list_buckets() ran inline in the slot, so an
        unreachable endpoint locked the launcher for the whole connect
        timeout with no way to cancel."""
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel") as model, \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(True, None)) as runner:
            # A stand-in model still has to answer the pre-flight check.
            model.return_value.check_tls_settings.return_value = ""
            win.onStart()
        runner.assert_called_once()
        self.assertIs(runner.call_args.args[0], win)
        # the probe must be handed over as a callable, not already invoked
        self.assertFalse(model.return_value.list_buckets.called)
        main.assert_called_once()

    def test_cancelling_the_probe_does_not_open_a_window(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, None)):
            win.onStart()
        main.assert_not_called()
        box.assert_not_called()          # a cancel is not an error

    def test_a_failed_probe_reports_and_does_not_open_a_window(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "MainWindow") as main, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, RuntimeError("host down"))):
            win.onStart()
        main.assert_not_called()
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("host down", shown)

    def test_check_profile_runs_off_the_gui_thread(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel") as model, \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=((False, "denied"), None)) as runner:
            win.check_profile()
        runner.assert_called_once()
        self.assertFalse(model.return_value.check_profile.called)
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("denied", shown)

    def test_check_profile_reports_a_raised_error(self):
        """run_with_progress returns the exception rather than raising it, and
        an unhandled one here would abort the process from a Qt slot."""
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, RuntimeError("no route"))):
            win.check_profile()
        shown = " ".join(str(c) for c in box.return_value.setText.call_args_list)
        self.assertIn("no route", shown)

    def test_cancelling_check_profile_reports_nothing(self):
        win = self._launcher([self._item()])
        win._secrets_or_warn = lambda item: ("AK", "SK", "")
        with patch.object(s3duck, "DataModel"), \
                patch.object(s3duck, "QMessageBox") as box, \
                patch.object(s3duck, "run_with_progress",
                             return_value=(None, None)):
            win.check_profile()
        box.assert_not_called()
class ProfileSwitcherCryptoTests(unittest.TestCase):
    """The runtime profile switcher shows `str(exc)` in a warning box."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _settings(self, key):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        settings.beginGroup("common")
        settings.setValue("key", key)
        settings.endGroup()
        return settings

    def _raw(self, crypto):
        return {
            "name": "prod", "url": "https://s3.amazonaws.com",
            "region": "us-east-1", "bucket_name": "",
            "access_key": crypto.encrypt("AKID"),
            "secret_key": crypto.encrypt("SECRET"),
            "no_ssl_check": "false", "use_path": "true",
            "session_token": crypto.encrypt("TOK"), "read_only": "false",
        }

    def test_good_key_round_trips(self):
        crypto = Crypto(Fernet.generate_key().decode())
        prof = profile_switcher.decrypt_profile(
            self._settings(crypto.key), self._raw(crypto))
        self.assertEqual(prof.access_key, "AKID")
        self.assertEqual(prof.secret_key, "SECRET")
        self.assertEqual(prof.session_token, "TOK")

    def test_wrong_key_error_message_is_not_blank(self):
        """REGRESSION: a mismatched key raised InvalidToken, whose str() is
        EMPTY — the switcher then showed a warning box with no text at all."""
        crypto = Crypto(Fernet.generate_key().decode())
        raw = self._raw(crypto)
        other = Fernet.generate_key().decode()
        with self.assertRaises(CredentialError) as ctx:
            profile_switcher.decrypt_profile(self._settings(other), raw)
        self.assertTrue(str(ctx.exception).strip())
        self.assertIn("decrypt", str(ctx.exception).lower())

    def test_missing_key_is_reported(self):
        crypto = Crypto(Fernet.generate_key().decode())
        with self.assertRaises(CredentialError) as ctx:
            profile_switcher.decrypt_profile(
                self._settings(""), self._raw(crypto))
        self.assertIn("missing", str(ctx.exception).lower())
class SelectedRowIndexTests(unittest.TestCase):
    """REGRESSION: Qt returns row -1 with no selection, and `items[-1]` would
    silently act on the LAST profile — copy/check operated on the wrong one."""

    def test_no_selection_returns_minus_one(self):
        self.assertEqual(selected_row_index(-1, 3), -1)
        self.assertEqual(selected_row_index(None, 3), -1)

    def test_stale_row_past_the_end_is_rejected(self):
        self.assertEqual(selected_row_index(5, 3), -1)
        self.assertEqual(selected_row_index(0, 0), -1)

    def test_valid_rows_pass_through(self):
        self.assertEqual(selected_row_index(0, 3), 0)
        self.assertEqual(selected_row_index(2, 3), 2)

    def test_empty_list_is_never_indexable(self):
        for row in (-1, 0, 1):
            self.assertEqual(selected_row_index(row, 0), -1)
class ExpiryHelperTests(unittest.TestCase):
    """Credential lifetimes, as the various helpers actually write them."""

    def test_parses_rfc3339_with_z_and_offset(self):
        moment = datetime(2026, 9, 2, 18, 30, tzinfo=timezone.utc).timestamp()
        self.assertAlmostEqual(
            utils.parse_expiry("2026-09-02T18:30:00Z"), moment, places=0)
        self.assertAlmostEqual(
            utils.parse_expiry("2026-09-02T20:30:00+02:00"), moment, places=0)

    def test_parses_a_bare_epoch(self):
        self.assertEqual(utils.parse_expiry("1788296309"), 1788296309.0)
        self.assertEqual(utils.parse_expiry(1788296309), 1788296309.0)

    def test_absent_or_unparseable_is_zero_not_an_error(self):
        for value in (None, "", "   ", "whenever", 0, -5):
            self.assertEqual(utils.parse_expiry(value), 0.0)

    def test_states_split_at_the_warning_window(self):
        now = 1_000_000.0
        self.assertEqual(utils.expiry_state("", now=now)[0], "none")
        self.assertEqual(utils.expiry_state(now - 1, now=now)[0], "expired")
        self.assertEqual(utils.expiry_state(now + 60, now=now)[0], "soon")
        self.assertEqual(utils.expiry_state(now + 7200, now=now)[0], "ok")

    def test_label_reads_as_a_countdown(self):
        now = 1_000_000.0
        self.assertEqual(utils.expiry_state(now + 600, now=now)[1],
                         "expires in 10m")
        self.assertEqual(utils.expiry_state(now + 7200, now=now)[1],
                         "expires in 2h")
        self.assertEqual(utils.expiry_state(now - 1, now=now)[1], "expired")

    def test_duration_formats(self):
        self.assertEqual(utils.format_duration(45), "45s")
        self.assertEqual(utils.format_duration(90), "1m")
        self.assertEqual(utils.format_duration(3900), "1h 5m")
        self.assertEqual(utils.format_duration(90000), "1d 1h")
        self.assertEqual(utils.format_duration(-5), "0s")
class CredentialProcessTests(unittest.TestCase):
    """The AWS SDKs' own refresh mechanism, which is the only way an SSO or
    assumed-role session stays usable."""

    def _script(self, body):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = os.path.join(directory, "creds.sh")
        with open(path, "w") as handle:
            handle.write("#!/bin/sh\n" + body + "\n")
        os.chmod(path, 0o755)
        return path

    def test_reads_the_documented_json_shape(self):
        path = self._script(
            "echo '{\"Version\":1,\"AccessKeyId\":\"AKIA1\","
            "\"SecretAccessKey\":\"SEC\",\"SessionToken\":\"TOK\","
            "\"Expiration\":\"2026-09-02T18:30:00Z\"}'")
        out = utils.run_credential_process(path)
        self.assertEqual(out, {
            "access_key": "AKIA1", "secret_key": "SEC",
            "session_token": "TOK", "expires": "2026-09-02T18:30:00Z",
        })

    def test_permanent_keys_without_a_token_are_fine(self):
        path = self._script(
            "echo '{\"AccessKeyId\":\"AKIA1\",\"SecretAccessKey\":\"SEC\"}'")
        out = utils.run_credential_process(path)
        self.assertEqual(out["session_token"], "")
        self.assertEqual(out["expires"], "")

    def test_a_failing_command_reports_its_last_stderr_line(self):
        path = self._script("echo 'sso session expired' >&2; exit 3")
        with self.assertRaises(utils.CredentialProcessError) as caught:
            utils.run_credential_process(path)
        self.assertIn("sso session expired", str(caught.exception))

    def test_non_json_output_is_an_error_not_a_crash(self):
        path = self._script("echo hello")
        with self.assertRaises(utils.CredentialProcessError):
            utils.run_credential_process(path)

    def test_json_without_keys_is_rejected(self):
        path = self._script("echo '{\"Version\":1}'")
        with self.assertRaises(utils.CredentialProcessError):
            utils.run_credential_process(path)

    def test_missing_binary_is_reported_by_name(self):
        with self.assertRaises(utils.CredentialProcessError) as caught:
            utils.run_credential_process("/nonexistent/creds-helper --json")
        self.assertIn("not found", str(caught.exception))

    def test_empty_command_is_refused(self):
        with self.assertRaises(utils.CredentialProcessError):
            utils.run_credential_process("   ")

    def test_the_command_is_not_run_through_a_shell(self):
        """A stored profile must not be able to smuggle in a pipeline; shlex
        splitting means the metacharacter becomes a literal argument."""
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        marker = os.path.join(directory, "pwned")
        with self.assertRaises(utils.CredentialProcessError):
            utils.run_credential_process(f"/bin/echo x > {marker}")
        self.assertFalse(os.path.exists(marker))
class AwsProfileExpiryTests(unittest.TestCase):
    """~/.aws parsing: no standard expiry key exists, so several are read."""

    def _files(self, credentials="", config=""):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        cred_path = os.path.join(directory, "credentials")
        cfg_path = os.path.join(directory, "config")
        with open(cred_path, "w") as handle:
            handle.write(credentials)
        with open(cfg_path, "w") as handle:
            handle.write(config)
        return cred_path, cfg_path

    def test_expiry_is_read_from_any_of_the_known_keys(self):
        for key in utils.AWS_EXPIRY_KEYS:
            cred, cfg = self._files(
                "[temp]\naws_access_key_id = AK\n"
                "aws_secret_access_key = SK\n"
                f"{key} = 2026-09-02T18:30:00Z\n")
            profiles = utils.load_aws_profiles(cred, cfg)
            self.assertEqual(profiles["temp"]["expires"],
                             "2026-09-02T18:30:00Z", key)

    def test_credential_process_profile_survives_without_keys(self):
        cred, cfg = self._files(
            config="[profile sso]\ncredential_process = aws-helper --json\n")
        profiles = utils.load_aws_profiles(cred, cfg)
        self.assertIn("sso", profiles)
        self.assertEqual(profiles["sso"]["credential_process"],
                         "aws-helper --json")
        self.assertEqual(profiles["sso"]["access_key"], "")

    def test_a_config_only_profile_with_neither_is_still_dropped(self):
        cred, cfg = self._files(config="[profile empty]\nregion = eu-west-1\n")
        self.assertEqual(utils.load_aws_profiles(cred, cfg), {})
class ProfileExpiryBadgeTests(unittest.TestCase):
    """A lapsed session used to look exactly like a working profile."""

    def _item(self, expires):
        return s3duck.SettingsItem(
            "prod", "https://s3.amazonaws.com", "us-east-1", "",
            "enc", "enc", "false", "false", "enc", "false", "",
            session_expires=expires)

    def test_no_expiry_means_no_badge(self):
        self.assertEqual(s3duck.expiry_badge(self._item("")), "")

    def test_expired_and_expiring_badges(self):
        now = 1_000_000.0
        self.assertEqual(
            s3duck.expiry_badge(self._item(now - 1), now=now), "expired")
        self.assertEqual(
            s3duck.expiry_badge(self._item(now + 300), now=now),
            "expires in 5m")
        self.assertEqual(
            s3duck.expiry_badge(self._item(now + 86400), now=now), "")

    def test_badge_joins_the_safety_flags(self):
        item = self._item(1.0)
        item.read_only = "true"
        self.assertEqual(
            s3duck.profile_badges(item),
            [s3duck.READ_ONLY_BADGE, s3duck.EXPIRED_BADGE])

    def test_generated_badge_gets_the_warning_colour(self):
        app = _ensure_qapp()
        colour = s3duck.badge_color(
            "expires in 5m", app.palette(), selected=False)
        self.assertEqual(colour.name(), "#b26a00")
class MainWindowCredentialTests(unittest.TestCase):
    """The session-level half of credential expiry: the countdown, the
    transfer guard and the in-place refresh."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, *, expires="", aws_profile="", credential_process=""):
        settings = QSettings("s3duck-tests-cred", "s3duck-tests-cred")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(
                self.ROOT,
                settings,
                profile_name='prod',
                session_token='TOK',
                session_expires=expires,
                aws_profile=aws_profile,
                credential_process=credential_process,
                public_base_url='',
                requester_pays=False)
        self.addCleanup(win.close)
        return win, settings

    def test_permanent_keys_show_no_countdown(self):
        win, _ = self._window()
        self.assertFalse(win.credential_status.isVisible())
        self.assertEqual(win.credential_state()[0], "none")

    def test_expired_session_is_shown_in_the_status_bar(self):
        win, _ = self._window(expires=time.time() - 60)
        win._update_credential_status()
        self.assertEqual(win.credential_status.text(), "credentials expired")

    def test_refreshability_follows_the_profile(self):
        win, _ = self._window()
        self.assertFalse(win.can_refresh_credentials())
        win2, _ = self._window(aws_profile="prod")
        self.assertTrue(win2.can_refresh_credentials())
        win3, _ = self._window(credential_process="helper --json")
        self.assertTrue(win3.can_refresh_credentials())

    def test_healthy_credentials_do_not_interrupt_a_transfer(self):
        win, _ = self._window(expires=time.time() + 86400)
        self.assertTrue(win._credentials_ok_for_transfer())

    def test_expired_and_unrefreshable_asks_before_starting(self):
        win, _ = self._window(expires=time.time() - 60)
        asked = []

        def _question(_parent, _title, text, *_a, **_kw):
            asked.append(text)
            return QMessageBox.StandardButton.No

        with patch.object(main_window.QMessageBox, "question", _question):
            self.assertFalse(win._credentials_ok_for_transfer())
        self.assertIn("expired", asked[0])

    def test_queueing_is_abandoned_when_the_guard_says_no(self):
        win, _ = self._window(expires=time.time() - 60)
        with patch.object(main_window.QMessageBox, "question",
                          lambda *a, **kw: QMessageBox.StandardButton.No):
            win.assign_thread_operation("delete", [("a.txt", False)])
        self.assertEqual(win._queue_entries, {})

    def test_refresh_from_the_aws_file_updates_model_and_settings(self):
        win, settings = self._window(aws_profile="prod",
                                     expires=time.time() - 60)
        # A stored row to write the refreshed secrets back onto.
        settings.beginGroup("common")
        settings.setValue("key", Fernet.generate_key().decode())
        settings.endGroup()
        settings.beginGroup("profiles")
        settings.beginWriteArray("profiles", 1)
        settings.setArrayIndex(0)
        settings.setValue("name", "prod")
        settings.endArray()
        settings.endGroup()

        fresh = {"prod": {
            "access_key": "AKIA-NEW", "secret_key": "SEC-NEW",
            "session_token": "TOK-NEW",
            "expires": "2099-01-01T00:00:00Z",
        }}
        with patch_app_wide("load_aws_profiles", lambda: fresh):
            self.assertTrue(win.refresh_credentials())

        self.assertEqual(win.data_model.access_key, "AKIA-NEW")
        self.assertEqual(win.data_model.session_token, "TOK-NEW")
        self.assertIsNone(win.data_model._client)
        self.assertEqual(win.session_expires, "2099-01-01T00:00:00Z")
        self.assertEqual(win.credential_state()[0], "ok")

        settings.beginGroup("profiles")
        settings.beginReadArray("profiles")
        settings.setArrayIndex(0)
        stored = settings.value("session_expires")
        cipher = settings.value("access_key")
        settings.endArray()
        settings.endGroup()
        self.assertEqual(stored, "2099-01-01T00:00:00Z")
        self.assertNotIn("AKIA-NEW", str(cipher))

    def test_a_missing_aws_profile_reports_instead_of_raising(self):
        win, _ = self._window(aws_profile="gone")
        shown = []
        with patch_app_wide("load_aws_profiles", lambda: {}), \
                patch.object(main_window.QMessageBox, "critical",
                             lambda _p, _t, text: shown.append(text)):
            self.assertFalse(win.refresh_credentials())
        self.assertIn("gone", shown[0])
class CredentialStoreTests(unittest.TestCase):
    """Where the Fernet key lives. The default keeps it beside the ciphertext,
    which is obfuscation; the other two modes are the actual protection."""

    def setUp(self):
        self.settings = QSettings("s3duck-tests-store", "s3duck-tests-store")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        utils.set_session_key("")
        self.addCleanup(utils.set_session_key, "")

    def _store(self, ask=None):
        return utils.CredentialStore(self.settings, ask_passphrase=ask)

    def test_default_mode_is_the_historical_one(self):
        self.assertEqual(self._store().mode(), utils.CREDENTIAL_STORE_LOCAL)

    def test_an_unknown_stored_mode_falls_back_to_local(self):
        self.settings.beginGroup("common")
        self.settings.setValue("credential_store", "nonsense")
        self.settings.endGroup()
        self.assertEqual(self._store().mode(), utils.CREDENTIAL_STORE_LOCAL)

    def test_first_run_generates_and_stores_a_key(self):
        store = self._store()
        key = store.ensure_key()
        self.assertTrue(key)
        self.settings.beginGroup("common")
        stored = self.settings.value("key")
        self.settings.endGroup()
        self.assertEqual(stored, key)
        # and it is a usable Fernet key
        utils.require_crypto(key)

    def test_ensure_key_is_idempotent(self):
        store = self._store()
        self.assertEqual(store.ensure_key(), store.ensure_key())

    def test_passphrase_mode_removes_the_plaintext_key(self):
        store = self._store()
        original = store.ensure_key()
        store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE, passphrase="hunter2")

        self.settings.beginGroup("common")
        plain = self.settings.value("key")
        wrapped = self.settings.value("key_wrapped")
        self.settings.endGroup()
        self.assertIn(plain, (None, ""))
        self.assertTrue(wrapped)
        self.assertNotIn(original, str(wrapped))

    def test_passphrase_mode_round_trips_the_same_key(self):
        """The stored profiles must stay readable across the switch."""
        store = self._store()
        original = store.ensure_key()
        store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE, passphrase="hunter2")
        utils.set_session_key("")
        reopened = self._store(ask=lambda _prompt: "hunter2")
        self.assertEqual(reopened.load_key(), original)

    def test_wrong_passphrase_reports_instead_of_returning_garbage(self):
        store = self._store()
        store.ensure_key()
        store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE, passphrase="hunter2")
        utils.set_session_key("")
        reopened = self._store(ask=lambda _prompt: "wrong")
        with self.assertRaises(CredentialError) as caught:
            reopened.load_key()
        self.assertIn("passphrase", str(caught.exception).lower())

    def test_a_declined_prompt_is_an_error_not_an_empty_key(self):
        store = self._store()
        store.ensure_key()
        store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE, passphrase="hunter2")
        utils.set_session_key("")
        reopened = self._store(ask=lambda _prompt: None)
        with self.assertRaises(CredentialError):
            reopened.load_key()

    def test_switching_back_to_local_restores_the_same_key(self):
        store = self._store()
        original = store.ensure_key()
        store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE, passphrase="pw")
        store.set_mode(utils.CREDENTIAL_STORE_LOCAL)
        self.settings.beginGroup("common")
        self.assertEqual(self.settings.value("key"), original)
        self.assertIn(self.settings.value("key_wrapped"), (None, ""))
        self.settings.endGroup()

    def test_passphrase_mode_without_a_passphrase_is_refused(self):
        store = self._store()
        store.ensure_key()
        with self.assertRaises(CredentialError):
            store.set_mode(utils.CREDENTIAL_STORE_PASSPHRASE)
        # and the mode did not move, so nothing became unreadable
        self.assertEqual(store.mode(), utils.CREDENTIAL_STORE_LOCAL)

    def test_keyring_mode_round_trips_through_the_os_store(self):
        vault = {}

        class _FakeKeyring:
            @staticmethod
            def get_keyring():
                return _FakeKeyring()

            @staticmethod
            def get_password(service, entry):
                return vault.get((service, entry))

            @staticmethod
            def set_password(service, entry, value):
                vault[(service, entry)] = value

            @staticmethod
            def delete_password(service, entry):
                vault.pop((service, entry), None)

        with patch.object(utils, "keyring", _FakeKeyring):
            store = self._store()
            original = store.ensure_key()
            store.set_mode(utils.CREDENTIAL_STORE_KEYRING)
            self.assertEqual(
                vault[(utils.KEYRING_SERVICE, utils.KEYRING_ENTRY)], original)
            self.settings.beginGroup("common")
            self.assertIn(self.settings.value("key"), (None, ""))
            self.settings.endGroup()

            utils.set_session_key("")
            self.assertEqual(self._store().load_key(), original)

    def test_keyring_mode_is_refused_when_no_backend_exists(self):
        with patch.object(utils, "keyring", None):
            store = self._store()
            store.ensure_key()
            with self.assertRaises(CredentialError):
                store.set_mode(utils.CREDENTIAL_STORE_KEYRING)

    @staticmethod
    def _backend(module_name, class_name="Keyring", base=object):
        """A stand-in shaped like a real keyring backend: every one of them
        is a class called "Keyring", told apart only by its module."""
        cls = type(class_name, (base,), {})
        cls.__module__ = module_name
        return cls

    def _with_backend(self, cls):
        class _Stub:
            @staticmethod
            def get_keyring():
                return cls()

        return patch.object(utils, "keyring", _Stub)

    def test_keyring_availability_rejects_the_null_backend(self):
        """keyring.backends.fail.Keyring and keyring.backends.null.Keyring are
        both named plain "Keyring" — exactly like the backends that work — so
        only the module tells them apart."""
        for module in ("keyring.backends.fail", "keyring.backends.null"):
            with self.subTest(module=module):
                with self._with_backend(self._backend(module)):
                    self.assertFalse(utils.keyring_available())

    def test_a_subclass_of_the_fail_backend_is_rejected_too(self):
        base = self._backend("keyring.backends.fail")
        with self._with_backend(self._backend("somewhere.else", base=base)):
            self.assertFalse(utils.keyring_available())

    def test_a_real_backend_is_accepted(self):
        for module in ("keyring.backends.SecretService", "keyring.backends.macOS",
                       "keyring.backends.Windows", "keyring.backends.kwallet",
                       "keyring.backends.chainer"):
            with self.subTest(module=module):
                with self._with_backend(self._backend(module)):
                    self.assertTrue(utils.keyring_available())

    def test_a_backend_that_cannot_even_be_asked_is_unavailable(self):
        class _Stub:
            @staticmethod
            def get_keyring():
                raise RuntimeError("no backend")

        with patch.object(utils, "keyring", _Stub):
            self.assertFalse(utils.keyring_available())

    def test_wrap_is_salted_so_two_seals_differ(self):
        key = utils.Crypto.generate_key()
        self.assertNotEqual(utils.wrap_key(key, "pw"), utils.wrap_key(key, "pw"))
        self.assertEqual(utils.unwrap_key(utils.wrap_key(key, "pw"), "pw"), key)

    def test_a_corrupt_wrapped_key_is_reported(self):
        with self.assertRaises(CredentialError):
            utils.unwrap_key("not json", "pw")
class ResolveCredentialKeyTests(unittest.TestCase):
    """Components other than the launcher have to find the key too."""

    def setUp(self):
        self.settings = QSettings("s3duck-tests-resolve", "s3duck-tests-resolve")
        self.settings.clear()
        self.addCleanup(self.settings.clear)
        utils.set_session_key("")
        self.addCleanup(utils.set_session_key, "")

    def test_the_settings_file_wins_when_it_holds_one(self):
        self.settings.beginGroup("common")
        self.settings.setValue("key", "STORED")
        self.settings.endGroup()
        utils.set_session_key("UNLOCKED")
        self.assertEqual(utils.resolve_credential_key(self.settings), "STORED")

    def test_the_unlocked_key_answers_when_the_file_has_none(self):
        """This is the keyring / passphrase case: nothing is in the file."""
        utils.set_session_key("UNLOCKED")
        self.assertEqual(
            utils.resolve_credential_key(self.settings), "UNLOCKED")

    def test_nothing_anywhere_is_an_empty_string(self):
        self.assertEqual(utils.resolve_credential_key(self.settings), "")
