"""core/: naming, formatting, locations, commands."""

import main_window
import unittest
from PyQt6.QtCore import QEvent, QSettings, Qt
from PyQt6.QtGui import QAction, QKeyEvent, QKeySequence
from datetime import datetime, timezone
from main_window import (
    _human_bytes, _listing_summary, _scaled_bar_values, _to_epoch,
    add_bookmark_to, bookmark_label, categorize_key, collect_shortcuts,
    command_entries, filter_commands, format_completion_notification,
    hex_dump, palette_score, parse_bookmarks, serialize_bookmarks,
)
from model import FSObjectType, Item, Model
from tests.support import (
    FakeS3Client, PROJECT_ROOT, _StubModel, _dt, _ensure_qapp,
    make_main_window, make_model,
)
from unittest.mock import patch
from utils import str_to_bool


class StrToBoolTests(unittest.TestCase):
    def test_true_values(self):
        self.assertIs(str_to_bool("true"), True)
        self.assertIs(str_to_bool("True"), True)
        self.assertIs(str_to_bool("TRUE"), True)

    def test_false_values(self):
        self.assertIs(str_to_bool("false"), False)
        self.assertIs(str_to_bool("False"), False)

    def test_garbage_and_none_default_false(self):
        self.assertIs(str_to_bool("yes"), False)
        self.assertIs(str_to_bool(""), False)
        self.assertIs(str_to_bool(None), False)
        self.assertIs(str_to_bool(1), False)
class ToEpochTests(unittest.TestCase):
    def test_none_and_empty(self):
        self.assertEqual(_to_epoch(None), 0)
        self.assertEqual(_to_epoch(""), 0)
        self.assertEqual(_to_epoch("   "), 0)

    def test_numeric_passthrough(self):
        self.assertEqual(_to_epoch(1700000000), 1700000000)
        self.assertEqual(_to_epoch(1700000000.9), 1700000000)

    def test_datetime_object(self):
        dt = datetime(2026, 2, 8, 18, 59, 33, tzinfo=timezone.utc)
        self.assertEqual(_to_epoch(dt), int(dt.timestamp()))

    def test_plain_formats(self):
        self.assertGreater(_to_epoch("2026-02-08 18:59:33"), 0)
        self.assertGreater(_to_epoch("2026-02-08T18:59:33"), 0)
        self.assertGreater(_to_epoch("2026-02-08 18:59:33.123"), 0)

    def test_garbage_string(self):
        self.assertEqual(_to_epoch("not-a-date"), 0)

    def test_tz_aware_string_is_parsed(self):
        """REGRESSION: boto3 LastModified stringified as
        '2026-02-08 18:59:33+00:00' previously returned 0 (every file
        compared equal), so the Modified column would not sort."""
        self.assertGreater(_to_epoch("2026-02-08 18:59:33+00:00"), 0)
        self.assertGreater(_to_epoch("2026-02-08 18:59:33.123456+00:00"), 0)
        self.assertGreater(_to_epoch("2026-02-08T18:59:33Z"), 0)

    def test_tz_aware_strings_order_correctly(self):
        """REGRESSION: the Modified-column sort must produce a strict
        ordering for real S3-style timestamps."""
        older = str(datetime(2026, 2, 8, 18, 59, 33, tzinfo=timezone.utc))
        newer = str(datetime(2026, 2, 8, 19, 0, 0, tzinfo=timezone.utc))
        self.assertLess(_to_epoch(older), _to_epoch(newer))

    def test_tz_offset_respected(self):
        a = _to_epoch("2026-02-08 18:59:33+00:00")
        b = _to_epoch("2026-02-08 18:59:33+01:00")  # same wall clock, 1h earlier
        self.assertEqual(a - b, 3600)
class CategorizeKeyTests(unittest.TestCase):
    def test_documents(self):
        for k in ("a/b/report.pdf", "x.DOCX", "notes.txt", "data.csv"):
            self.assertEqual(categorize_key(k), "Documents", k)

    def test_media(self):
        for k in ("img.JPG", "clip.mp4", "song.flac", "a/b/pic.png"):
            self.assertEqual(categorize_key(k), "Media", k)

    def test_other(self):
        self.assertEqual(categorize_key("archive.zip"), "Other")
        self.assertEqual(categorize_key("no_extension"), "Other")
        self.assertEqual(categorize_key(""), "Other")
        self.assertEqual(categorize_key(None), "Other")
class HumanBytesTests(unittest.TestCase):
    def test_zero_and_none(self):
        self.assertEqual(_human_bytes(0), "0.0 B")
        self.assertEqual(_human_bytes(None), "0.0 B")

    def test_scaling(self):
        self.assertEqual(_human_bytes(512), "512.0 B")
        self.assertEqual(_human_bytes(1024), "1.0 KB")
        self.assertEqual(_human_bytes(1024 * 1024), "1.0 MB")
        self.assertEqual(_human_bytes(1024 ** 3), "1.0 GB")

    def test_caps_at_tb(self):
        self.assertTrue(_human_bytes(1024 ** 5).endswith(" TB"))
class ScaledBarValuesTests(unittest.TestCase):
    INT32_MAX = 2147483647

    def test_normal_progress_halfway(self):
        self.assertEqual(_scaled_bar_values(50, 100), (1000, 500))

    def test_zero_and_complete(self):
        self.assertEqual(_scaled_bar_values(0, 100), (1000, 0))
        self.assertEqual(_scaled_bar_values(100, 100), (1000, 1000))

    def test_unknown_total_is_indeterminate(self):
        self.assertEqual(_scaled_bar_values(0, 0), (0, 0))
        self.assertEqual(_scaled_bar_values(123, -1), (0, 0))

    def test_value_never_exceeds_range(self):
        # done > total (e.g. slightly over-reported) must still clamp to full
        self.assertEqual(_scaled_bar_values(150, 100), (1000, 1000))

    def test_large_transfer_stays_within_int32(self):
        """REGRESSION: a multi-file download whose byte total exceeds ~2.1 GB
        overflowed QProgressBar.setRange (C++ int), raising OverflowError.
        The scaled range and value must always fit in a 32-bit signed int."""
        done = 3 * 1024 ** 3          # 3 GB downloaded
        total = 10 * 1024 ** 3        # 10 GB total  (> INT32_MAX)
        range_max, value = _scaled_bar_values(done, total)
        self.assertLessEqual(range_max, self.INT32_MAX)
        self.assertLessEqual(value, self.INT32_MAX)
        self.assertEqual((range_max, value), (1000, 300))

    def test_petabyte_scale_does_not_overflow(self):
        done = 5 * 1024 ** 5          # 5 PB
        total = 8 * 1024 ** 5         # 8 PB
        range_max, value = _scaled_bar_values(done, total)
        self.assertLessEqual(range_max, self.INT32_MAX)
        self.assertLessEqual(value, self.INT32_MAX)
        self.assertEqual((range_max, value), (1000, 625))
class ListingColumnDataTests(unittest.TestCase):
    """Storage class and ETag come back with ListObjectsV2, so the optional
    columns must cost no extra request."""

    def test_listing_captures_storage_class_and_etag(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{
            "CommonPrefixes": [{"Prefix": "dir/sub/"}],
            "Contents": [{
                "Key": "dir/a.txt", "Size": 12, "LastModified": _dt(1),
                "StorageClass": "GLACIER", "ETag": '"abc123"',
            }],
        }])
        items = m.list("dir/")
        by_name = {i.name: i for i in items}
        self.assertEqual(by_name["a.txt"].storage_class, "GLACIER")
        self.assertEqual(by_name["a.txt"].etag, "abc123")
        # folders carry no object metadata
        self.assertEqual(by_name["sub"].storage_class, "")

    def test_storage_class_defaults_to_standard(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 1, "LastModified": _dt(1)}]}])
        self.assertEqual(m.list("")[0].storage_class, "STANDARD")

    def test_usage_listing_returns_storage_class(self):
        m = make_model(bucket="b")
        c = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 5, "StorageClass": "STANDARD_IA"}]}])
        m._try_bind_bucket = lambda name: (c, "ep", "r", True)
        self.assertEqual(
            m.get_keys_for_bucket("b", ""), [("a.txt", 5, "STANDARD_IA")])
class ListingSummaryTests(unittest.TestCase):
    def test_counts_and_size(self):
        items = [
            Item("a", FSObjectType.FOLDER, "", 0),
            Item("b.txt", FSObjectType.FILE, "", 1024),
            Item("c.txt", FSObjectType.FILE, "", 1024),
        ]
        self.assertEqual(_listing_summary(items),
                         "1 dir(s), 2 file(s), 2.0 KB")

    def test_empty_and_none(self):
        self.assertEqual(_listing_summary([]), "0 dir(s), 0 file(s), 0.0 B")
        self.assertEqual(_listing_summary(None), "0 dir(s), 0 file(s), 0.0 B")

    def test_buckets_are_not_counted(self):
        items = [Item("bk", FSObjectType.BUCKET, "", 0)]
        self.assertEqual(_listing_summary(items),
                         "0 dir(s), 0 file(s), 0.0 B")
class CommandPaletteTests(unittest.TestCase):
    """The action surface outgrew the toolbar: toolbar + Tools menu + several
    context menus + ~20 shortcuts, with no way to reach a rare command."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    @staticmethod
    def _action(text, keys=(), enabled=True, visible=True, separator=False):
        action = QAction(text)
        action.setShortcuts([QKeySequence(k) for k in keys])
        action.setEnabled(enabled)
        action.setVisible(visible)
        action.setSeparator(separator)
        return action

    def test_entries_are_built_from_live_actions(self):
        entries = command_entries([
            self._action("Refresh", ["F5", "Ctrl+R"]),
            self._action("Upload…"),
        ])
        self.assertEqual([(label, keys) for label, keys, _a in entries],
                         [("Refresh", "F5, Ctrl+R"), ("Upload", "")])

    def test_unusable_actions_are_left_out(self):
        """Running a disabled or hidden action would do nothing. Qt reports a
        hidden action as disabled too, so one check covers both."""
        hidden = self._action("Hidden", visible=False)
        self.assertFalse(hidden.isEnabled())
        entries = command_entries([
            self._action("Disabled", enabled=False),
            hidden,
            self._action("Real"),
        ])
        self.assertEqual([label for label, _k, _a in entries], ["Real"])

    def test_separators_are_left_out_even_when_they_carry_text(self):
        """A separator is enabled and visible, so only the separator flag
        keeps it out of the list."""
        entries = command_entries([
            self._action("Divider", separator=True),
            self._action("Real"),
        ])
        self.assertEqual([label for label, _k, _a in entries], ["Real"])

    def test_labels_are_cleaned_up(self):
        entries = command_entries([self._action("&Copy (Ctrl+C)…")])
        self.assertEqual(entries[0][0], "Copy")

    def test_duplicate_labels_collapse(self):
        """The same action reaches the palette from the toolbar and from
        findChildren; it should be offered once."""
        entries = command_entries(
            [self._action("Refresh", ["F5"]), self._action("refresh")])
        self.assertEqual(len(entries), 1)

    def test_matches_are_ranked_not_left_in_input_order(self):
        """Deliberately fed worst-first, so returning the input order fails."""
        entries = [("Clean old parts", "", None), ("Recopy files", "", None),
                   ("Copy", "", None), ("Delete", "", None)]
        self.assertEqual(
            [label for label, _k, _a in filter_commands(entries, "co")],
            ["Copy", "Recopy files", "Clean old parts"])

    def test_a_contiguous_match_beats_a_scattered_one(self):
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Clean old parts", "co"))

    def test_a_contiguous_match_wins_even_when_it_is_the_longer_label(self):
        """Guards the tier itself: 'Clo' is shorter, and would sort first if
        substring and subsequence shared a tier."""
        self.assertLess(palette_score("Incomplete uploads", "co"),
                        palette_score("Clo", "co"))
        self.assertEqual(
            [label for label, _k, _a in filter_commands(
                [("Clo", "", None), ("Incomplete uploads", "", None)], "co")],
            ["Incomplete uploads", "Clo"])

    def test_an_earlier_match_wins_within_a_tier(self):
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Recopy files", "co"))

    def test_position_beats_alphabetical_order(self):
        """Guards the position component: 'Abort copy' sorts first
        alphabetically but matches later."""
        self.assertLess(palette_score("Copy", "co"),
                        palette_score("Abort copy", "co"))
        self.assertEqual(
            [label for label, _k, _a in filter_commands(
                [("Abort copy", "", None), ("Copy", "", None)], "co")],
            ["Copy", "Abort copy"])

    def test_a_subsequence_must_be_in_order(self):
        """'yc' appears in 'Copy' only out of order, and must not match."""
        self.assertIsNone(palette_score("Copy", "yc"))
        self.assertIsNotNone(palette_score("Copy", "cy"))

    def test_an_empty_query_keeps_everything(self):
        entries = [("B", "", None), ("A", "", None)]
        self.assertEqual(len(filter_commands(entries, "")), 2)
        self.assertEqual(len(filter_commands(entries, "   ")), 2)

    def test_matching_ignores_case(self):
        entries = [("Upload folder", "", None)]
        self.assertEqual(len(filter_commands(entries, "UPLOAD")), 1)

    def test_a_query_that_matches_nothing_returns_nothing(self):
        entries = [("Copy", "", None)]
        self.assertEqual(filter_commands(entries, "zzz"), [])
        self.assertIsNone(palette_score("Copy", "zzz"))

    def test_the_dialog_runs_the_selected_action(self):
        fired = []
        action = self._action("Refresh", ["F5"])
        action.triggered.connect(lambda: fired.append(1))
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([action, self._action("Delete")]))
        self.addCleanup(dlg.close)
        dlg._query.setText("refr")
        chosen = dlg.chosen_action()
        self.assertIs(chosen, action)
        chosen.trigger()
        self.assertEqual(fired, [1])

    def test_the_highlighted_row_is_what_runs(self):
        """Not simply the first match — the arrows move the selection."""
        wanted = self._action("Download")
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Delete"), wanted]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.currentRow(), 0)
        dlg._list.setCurrentRow(1)
        self.assertIs(dlg.chosen_action(), wanted)

    def test_typing_narrows_the_list(self):
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Copy"), self._action("Delete"),
            self._action("Download")]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.count(), 3)
        dlg._query.setText("del")
        self.assertEqual(dlg._list.count(), 1)
        dlg._query.setText("")
        self.assertEqual(dlg._list.count(), 3)

    def test_arrow_keys_move_the_selection_while_typing(self):
        """The cursor stays in the box, so the list needs the arrows."""
        dlg = main_window.CommandPaletteDialog(None, command_entries([
            self._action("Copy"), self._action("Delete"),
            self._action("Download")]))
        self.addCleanup(dlg.close)
        self.assertEqual(dlg._list.currentRow(), 0)
        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Down,
                          Qt.KeyboardModifier.NoModifier)
        self.assertTrue(dlg.eventFilter(dlg._query, event))
        self.assertEqual(dlg._list.currentRow(), 1)
        up = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Up,
                       Qt.KeyboardModifier.NoModifier)
        dlg.eventFilter(dlg._query, up)
        self.assertEqual(dlg._list.currentRow(), 0)

    def test_the_selection_cannot_run_off_the_ends(self):
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([self._action("Copy")]))
        self.addCleanup(dlg.close)
        for key in (Qt.Key.Key_Up, Qt.Key.Key_Down, Qt.Key.Key_Down):
            dlg.eventFilter(dlg._query, QKeyEvent(
                QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier))
            self.assertEqual(dlg._list.currentRow(), 0)

    def test_nothing_is_chosen_when_the_query_matches_nothing(self):
        dlg = main_window.CommandPaletteDialog(
            None, command_entries([self._action("Copy")]))
        self.addCleanup(dlg.close)
        dlg._query.setText("zzzz")
        self.assertEqual(dlg._list.count(), 0)
        self.assertIsNone(dlg.chosen_action())

    def test_the_window_binds_ctrl_k(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        self.assertEqual(win._palette_shortcut.key(), QKeySequence("Ctrl+K"))

    def test_the_palette_offers_the_windows_real_actions(self):
        """Built from the live QActions, so it cannot drift from the app —
        and most are created parentless, which findChildren alone misses."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        labels = [label for label, _k, _a in
                  command_entries(win._live_actions())]
        self.assertTrue(labels)
        self.assertTrue(any("refresh" in label.lower() for label in labels),
                        f"toolbar actions missing from {labels}")
class ParseS3LocationTests(unittest.TestCase):
    def test_full_uri(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://bkt/a/b/"),
            ("bkt", "a/b/"),
        )
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://bkt"), ("bkt", ""))

    def test_uri_wins_over_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("s3://other/x", "cur"),
            ("other", "x/"),
        )

    def test_bare_prefix_keeps_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("a/b", "cur"),
            ("cur", "a/b/"),
        )
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("/a/b/", "cur"),
            ("cur", "a/b/"),
        )

    def test_bucket_slash_prefix_without_current_bucket(self):
        self.assertEqual(
            main_window.MainWindow._parse_s3_location("bkt/a", ""), ("bkt", "a/"))

    def test_empty(self):
        self.assertEqual(main_window.MainWindow._parse_s3_location("", "cur"),
                         ("", ""))
        self.assertEqual(main_window.MainWindow._parse_s3_location("   "),
                         ("", ""))
class BookmarkStorageTests(unittest.TestCase):
    def test_round_trip(self):
        entries = [
            {"name": "prod logs", "bucket": "bkt", "prefix": "logs/2026/"},
            {"name": "root", "bucket": "other", "prefix": ""},
        ]
        self.assertEqual(parse_bookmarks(serialize_bookmarks(entries)), entries)

    def test_malformed_lines_are_dropped_not_fatal(self):
        raw = "good\tbkt\tp/\nbroken line\n\t\t\nalso\tb2\t"
        parsed = parse_bookmarks(raw)
        self.assertEqual([b["bucket"] for b in parsed], ["bkt", "b2"])

    def test_tabs_and_newlines_in_a_name_cannot_split_a_record(self):
        raw = serialize_bookmarks(
            [{"name": "a\tb\nc", "bucket": "bkt", "prefix": "p/"}])
        self.assertEqual(len(raw.splitlines()), 1)
        self.assertEqual(len(parse_bookmarks(raw)), 1)

    def test_missing_name_falls_back_to_the_location(self):
        parsed = parse_bookmarks("\tbkt\tlogs/")
        self.assertEqual(parsed[0]["name"], "bkt/logs/")

    def test_label_formatting(self):
        self.assertEqual(bookmark_label("bkt", "a/b/"), "bkt/a/b/")
        self.assertEqual(bookmark_label("bkt", ""), "bkt")
        self.assertEqual(bookmark_label("", ""), "/")

    def test_empty_input(self):
        self.assertEqual(parse_bookmarks(""), [])
        self.assertEqual(parse_bookmarks(None), [])
        self.assertEqual(serialize_bookmarks([]), "")

    def test_add_rejects_a_duplicate_location(self):
        entries, added = add_bookmark_to([], "one", "bkt", "p/")
        self.assertTrue(added)
        entries, added = add_bookmark_to(entries, "other name", "bkt", "p/")
        self.assertFalse(added)
        self.assertEqual(len(entries), 1)

    def test_add_allows_the_same_bucket_at_another_prefix(self):
        entries, _ = add_bookmark_to([], "root", "bkt", "")
        entries, added = add_bookmark_to(entries, "deep", "bkt", "a/")
        self.assertTrue(added)
        self.assertEqual(len(entries), 2)

    def test_add_defaults_the_name(self):
        entries, _ = add_bookmark_to([], "", "bkt", "a/")
        self.assertEqual(entries[0]["name"], "bkt/a/")
class HexDumpTests(unittest.TestCase):
    def test_layout(self):
        out = hex_dump(b"AB\x00\xff")
        self.assertTrue(out.startswith("00000000  41 42 00 ff"))
        self.assertIn("|AB..|", out)

    def test_multiple_rows_and_offsets(self):
        out = hex_dump(bytes(range(48)), width=16)
        lines = out.splitlines()
        self.assertEqual(len(lines), 3)
        self.assertTrue(lines[1].startswith("00000010"))
        self.assertTrue(lines[2].startswith("00000020"))

    def test_truncation_is_announced(self):
        out = hex_dump(b"z" * 100, max_bytes=32)
        self.assertIn("68 more byte(s) not shown", out)

    def test_empty(self):
        self.assertEqual(hex_dump(b""), "")
        self.assertEqual(hex_dump(None), "")
class CompletionNotificationTests(unittest.TestCase):
    def test_success(self):
        title, body = format_completion_notification({"done": 3})
        self.assertIn("finished", title)
        self.assertEqual(body, "3 completed")

    def test_failure_wins_the_title(self):
        title, body = format_completion_notification({"done": 1, "error": 2})
        self.assertIn("failed", title)
        self.assertEqual(body, "1 completed, 2 failed")

    def test_cancelled_only(self):
        title, body = format_completion_notification({"cancelled": 2})
        self.assertIn("cancelled", title)
        self.assertEqual(body, "2 cancelled")

    def test_nothing(self):
        _title, body = format_completion_notification({})
        self.assertEqual(body, "nothing to do")
class CollectShortcutsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _action(self, text, keys=""):
        act = QAction(text)
        if keys:
            act.setShortcut(QKeySequence(keys))
        self.addCleanup(act.deleteLater)
        return act

    def test_only_actions_with_shortcuts_are_listed(self):
        rows = collect_shortcuts([
            self._action("Download (Ctrl+D)", "Ctrl+D"),
            self._action("No shortcut here"),
        ])
        self.assertEqual(rows, [("Ctrl+D", "Download")])

    def test_parenthetical_hint_is_stripped(self):
        rows = collect_shortcuts([self._action("Refresh (F5, Ctrl+R)", "F5")])
        self.assertEqual(rows[0][1], "Refresh")

    def test_duplicates_removed_and_sorted_by_label(self):
        rows = collect_shortcuts([
            self._action("Zebra", "Ctrl+Z"),
            self._action("Alpha", "Ctrl+A"),
            self._action("Alpha", "Ctrl+A"),
        ])
        self.assertEqual([label for _k, label in rows], ["Alpha", "Zebra"])

    def test_extra_rows_are_appended_verbatim(self):
        rows = collect_shortcuts([], [("Enter", "Open")])
        self.assertEqual(rows, [("Enter", "Open")])
class StorageCostTests(unittest.TestCase):
    """A size figure cannot answer 'is this bucket costing me pennies or
    hundreds'."""

    GIB = 1024 ** 3

    def test_standard_bytes_are_costed_at_the_standard_price(self):
        out = Model.estimate_storage_cost({"STANDARD": 100 * self.GIB})
        self.assertAlmostEqual(out["total"], 2.3, places=6)
        self.assertAlmostEqual(out["by_class"]["STANDARD"], 2.3, places=6)
        self.assertEqual(out["unpriced"], [])

    def test_classes_are_summed(self):
        out = Model.estimate_storage_cost({
            "STANDARD": 100 * self.GIB, "DEEP_ARCHIVE": 1000 * self.GIB})
        self.assertAlmostEqual(out["total"], 2.3 + 0.99, places=6)

    def test_an_unknown_class_is_reported_not_costed_at_zero(self):
        out = Model.estimate_storage_cost({"SOMETHING_NEW": 10 * self.GIB})
        self.assertEqual(out["unpriced"], ["SOMETHING_NEW"])
        self.assertEqual(out["total"], 0.0)

    def test_empty_breakdown_is_zero(self):
        self.assertEqual(Model.estimate_storage_cost({})["total"], 0.0)

    def test_colder_options_are_offered_for_standard_bytes_only(self):
        options = Model.colder_class_options({"STANDARD": 1000 * self.GIB})
        names = [name for name, _cost, _saving, _caveat in options]
        self.assertEqual(names, ["STANDARD_IA", "GLACIER_IR", "DEEP_ARCHIVE"])
        for _name, cost, saving, caveat in options:
            self.assertGreater(saving, 0)
            self.assertLess(cost, 23.0)
            self.assertTrue(caveat)

    def test_nothing_in_standard_means_nothing_to_suggest(self):
        self.assertEqual(
            Model.colder_class_options({"GLACIER": 10 * self.GIB}), [])

    def test_prices_are_only_quoted_for_aws(self):
        self.assertTrue(make_model().is_aws_endpoint())
        self.assertFalse(
            make_model(endpoint_url="https://minio.local:9000").is_aws_endpoint())
        self.assertFalse(make_model(endpoint_url="").is_aws_endpoint())

    def test_the_panel_is_empty_without_a_cost(self):
        self.assertEqual(main_window.BucketUsageDialog.cost_html(None, []), "")

    def test_the_panel_names_its_limits(self):
        html = main_window.BucketUsageDialog.cost_html(
            Model.estimate_storage_cost({"STANDARD": 100 * self.GIB}),
            Model.colder_class_options({"STANDARD": 100 * self.GIB}))
        self.assertIn("$2.30", html)
        self.assertIn("storage only", html)
        self.assertIn("DEEP_ARCHIVE", html)
        self.assertIn("180-day minimum", html)
