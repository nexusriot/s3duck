"""ui/widgets/ and the chrome around the listing."""

import ast
import contextlib
import diagnostics
import main_window
import os
import profile_switcher
import theme
import threading
import time
import unittest
import utils
from PyQt6.QtCore import QEvent, QPointF, QSettings, Qt
from PyQt6.QtGui import QColor, QIcon, QMouseEvent, QPalette, QPixmap
from PyQt6.QtWidgets import QApplication, QToolButton, QWidgetAction
from core.entries import FSObjectType, Item
from cryptography.fernet import Fernet
from main_window import Breadcrumb, SecondPane, command_entries
from model import Model
from settings import SettingsWindow
from tests.support import (
    PROJECT_ROOT, _HollowIconEngine, _StubModel, _ensure_qapp,
    make_main_window, make_model, read_app_sources,
)
from unittest.mock import patch
from utils import (
    Crypto, bundled_icon, icon_is_visible, normalize_accent, themed_icon,
)


class BreadcrumbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _segments(self, bc):
        out = []
        for i in range(bc._lay.count()):
            w = bc._lay.itemAt(i).widget()
            if isinstance(w, QToolButton):
                out.append(w)
        return out

    def test_bucket_list_mode_only_home(self):
        bc = Breadcrumb()
        bc.set_location("", "", True)
        segs = self._segments(bc)
        self.assertEqual([s.text() for s in segs], ["Buckets"])
        self.assertFalse(segs[0].isEnabled())  # current location, not clickable

    def test_nested_prefix_segments(self):
        bc = Breadcrumb()
        bc.set_location("mybucket", "a/b/", False)
        segs = self._segments(bc)
        self.assertEqual([s.text() for s in segs], ["Buckets", "mybucket", "a", "b"])
        # only the last ("b") is the current location
        self.assertTrue(segs[0].isEnabled())    # Buckets
        self.assertTrue(segs[1].isEnabled())    # mybucket
        self.assertTrue(segs[2].isEnabled())    # a
        self.assertFalse(segs[3].isEnabled())   # b (current)

    def test_segment_click_emits_prefix(self):
        bc = Breadcrumb()
        bc.set_location("mybucket", "a/b/", False)
        got = []
        bc.go.connect(lambda p: got.append(p))
        segs = self._segments(bc)
        # click "a" -> navigate to "a/"; click bucket -> navigate to ""
        segs[2].click()
        segs[1].click()
        self.assertEqual(got, ["a/", ""])

    def test_home_click_emits_home(self):
        bc = Breadcrumb()
        bc.set_location("mybucket", "", False)
        fired = []
        bc.home.connect(lambda: fired.append(True))
        self._segments(bc)[0].click()  # "Buckets"
        self.assertEqual(fired, [True])
class _PaneModel:
    """Just what SecondPane asks of a model: a bucket, and clones that list."""

    def __init__(self, listings, gates=None):
        self.bucket = "bkt"
        self.current_folder = ""
        self.listings = listings
        self.gates = gates or {}

    def clone_for_worker(self):
        return _PaneClone(self)


class _PaneClone:
    def __init__(self, source):
        self._source = source
        self.bucket = source.bucket

    def list(self, prefix):
        gate = self._source.gates.get((self.bucket, prefix))
        if gate is not None:
            gate.wait(5)
        return [Item(name, kind, "", size) for name, kind, size
                in self._source.listings.get((self.bucket, prefix), [])]


class SecondPaneListingTests(unittest.TestCase):
    """The remote side of the dual pane, listed for real."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _pane(self, listings, gates=None):
        pane = SecondPane(None, None, _PaneModel(listings, gates))
        self.addCleanup(pane.shutdown)
        return pane

    def _wait(self, condition, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            self._app.processEvents()
        self._app.processEvents()

    def test_a_listing_with_entries_is_shown(self):
        """REGRESSION (crash): _on_listed read item.t, which the Items
        model.list() returns do not have. Raised inside a slot, so pressing
        F3 in any non-empty bucket aborted the whole app."""
        pane = self._pane({})
        pane._bucket, pane._prefix = "bkt", ""
        pane.mode.blockSignals(True)
        pane.mode.setCurrentIndex(pane.mode.findData(SecondPane.REMOTE))
        pane.mode.blockSignals(False)
        pane._listing_for = pane._target()
        pane._on_listed([Item("a.txt", FSObjectType.FILE, "", 5),
                         Item("dir", FSObjectType.FOLDER, "", 0)], None)
        self.assertEqual(pane.entries(),
                         [("dir", True, 0), ("a.txt", False, 5)])

    def test_a_remote_listing_runs_end_to_end(self):
        pane = self._pane({("bkt", "p/"): [("a.txt", FSObjectType.FILE, 5),
                                           ("d", FSObjectType.FOLDER, 0)]})
        pane.set_remote("bkt", "p/")
        self._wait(lambda: pane.entries())
        self.assertEqual(pane.entries(), [("d", True, 0), ("a.txt", False, 5)])

    def test_a_listing_for_a_place_the_pane_has_left_is_dropped(self):
        """It used to be shown under the new prefix, and F5/F6 resolved its
        rows there -- keys that were never listed in that folder."""
        gate = threading.Event()
        self.addCleanup(gate.set)
        pane = self._pane(
            {("bkt", "a/"): [("old.txt", FSObjectType.FILE, 1)],
             ("bkt", "b/"): [("new.txt", FSObjectType.FILE, 2)]},
            gates={("bkt", "a/"): gate})
        pane.set_remote("bkt", "a/")
        pane.set_remote("bkt", "b/")      # while a/ is still listing
        # Nothing selectable from the place being left.
        self.assertEqual(pane.entries(), [])
        gate.set()
        self._wait(lambda: pane.entries())
        self.assertEqual(pane.entries(), [("new.txt", False, 2)])
        self.assertEqual(pane.remote_target(), ("bkt", "b/"))

    def test_switching_to_local_mid_listing_keeps_the_local_rows(self):
        gate = threading.Event()
        self.addCleanup(gate.set)
        pane = self._pane({("bkt", "a/"): [("remote.txt", FSObjectType.FILE, 1)]},
                          gates={("bkt", "a/"): gate})
        pane.set_remote("bkt", "a/")
        directory = os.path.dirname(os.path.abspath(__file__))
        pane.set_local(directory)
        local = pane.entries()
        gate.set()
        self._wait(lambda: pane._thread is None)
        self.assertEqual(pane.current_mode(), SecondPane.LOCAL)
        self.assertEqual(pane.entries(), local)
        self.assertNotIn("remote.txt", [name for name, _d, _s in pane.entries()])
class DiagnosticsTests(unittest.TestCase):
    """Exists because a blank-icon report took three rounds to explain: the
    venv's PyQt6 wheel bundles QtSvg, the distribution package does not, and
    nothing in the app said so."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = PROJECT_ROOT

    def test_the_report_names_the_facts_that_explain_icon_trouble(self):
        sections = diagnostics.collect(self.ROOT, version="9.9.9")
        text = diagnostics.format_report(sections)
        for label in ("SVG icons render", "QtSvg module",
                      "QtPdf module (PDF preview)", "Icon theme",
                      "Would render blank"):
            self.assertIn(label, text)

    def test_the_version_reaches_the_report(self):
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9"))
        self.assertIn("9.9.9", text)

    def test_svg_support_is_measured_by_rendering_not_by_format_list(self):
        """REGRESSION: the report read "svg" out of supportedImageFormats and
        said SVG was unreadable on a Qt where every SVG icon drew fine — QIcon
        reaches SVG through the icon-engine plugin, not the image format."""
        self.assertTrue(diagnostics.svg_supported(self.ROOT))
        # The format list alone must not decide it.
        with patch.object(diagnostics, "image_formats",
                          staticmethod(lambda: ["png"])):
            self.assertTrue(diagnostics.svg_supported(self.ROOT))
        # A Qt that cannot draw the sample reports no support.
        with patch.object(diagnostics, "icon_is_visible",
                          staticmethod(lambda *a, **kw: False)):
            self.assertFalse(diagnostics.svg_supported(self.ROOT))

    def test_svg_support_falls_back_to_the_format_list_without_a_sample(self):
        with patch.object(diagnostics.os.path, "exists",
                          staticmethod(lambda _p: False)), \
             patch.object(diagnostics, "image_formats",
                          staticmethod(lambda: ["png"])):
            self.assertFalse(diagnostics.svg_supported(self.ROOT))

    def test_icon_status_counts_every_requested_icon(self):
        status = diagnostics.icon_status(self.ROOT)
        self.assertGreater(status["total"], 30)
        self.assertEqual(status["blank"], 0)
        self.assertEqual(status["themed"] + status["bundled"] + status["blank"],
                         status["total"])

    def test_icon_status_counts_hollow_theme_entries(self):
        """The Mint case: the theme claims the name, so no fallback is used,
        yet nothing is painted. Counting these is what points at the cause."""
        hollow = QIcon(_HollowIconEngine())
        with patch.object(diagnostics.QIcon, "hasThemeIcon",
                          staticmethod(lambda name: True)), \
             patch.object(diagnostics.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: hollow)):
            status = diagnostics.icon_status(self.ROOT)
        self.assertEqual(status["hollow"], status["total"])
        self.assertEqual(status["themed"], 0)
        self.assertEqual(status["blank"], 0, "bundled art should cover them")

    def test_icon_status_reports_blanks_when_nothing_can_render(self):
        """The symptom the user actually saw."""
        with patch.object(diagnostics, "bundled_icon",
                          staticmethod(lambda *a, **kw: QIcon())):
            status = diagnostics.icon_status(self.ROOT)
        self.assertEqual(status["bundled"], 0)
        self.assertGreater(status["blank"], 0)

    def test_icon_calls_reads_multi_line_calls(self):
        """A line-oriented regex reported a present fallback as missing."""
        source = (
            'x = themed_icon("go-home", os.path.join(\n'
            '    self.current_dir,\n'
            '    "icons",\n'
            '    "home_24px.svg",\n'
            '))\n'
            'y = themed_icon("no-fallback")\n')
        self.assertEqual(
            sorted(diagnostics.icon_calls(source)),
            [("go-home", "home_24px.svg"), ("no-fallback", "")])

    def test_profile_and_transfer_settings_are_included(self):
        model = Model("https://minio.local", "us-east-1", "AK", "SK",
                      "bkt", True, True)
        model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        model.read_only = True
        sections = diagnostics.collect(
            self.ROOT, model=model, profile_name="prod")
        text = diagnostics.format_report(sections)
        self.assertIn("prod", text)
        self.assertIn("32 MiB", text)
        self.assertIn("64 MiB", text)
        # no_ssl_check=True means verification is OFF; the report must not
        # invert a security-relevant flag
        rows = dict(next(r for t, r in sections if t == "Profile"))
        self.assertEqual(rows["TLS verification"], "no")
        self.assertEqual(rows["Read-only"], "yes")

    def test_a_model_is_optional(self):
        """The launcher may want a report before any profile is opened."""
        titles = [title for title, _rows in diagnostics.collect(self.ROOT)]
        self.assertNotIn("Profile", titles)
        self.assertIn("Qt", titles)

    def test_the_report_is_aligned_plain_text(self):
        text = diagnostics.format_report(
            [("Bit", [("a", "1"), ("longer label", "2")])])
        self.assertEqual(
            text, "== Bit ==\na            : 1\nlonger label : 2\n")

    def test_the_dialog_copies_the_report_to_the_clipboard(self):
        dlg = main_window.DiagnosticsDialog(None, "== X ==\nk : v\n")
        self.addCleanup(dlg.close)
        dlg.copy_report()
        self.assertEqual(QApplication.clipboard().text(), "== X ==\nk : v\n")

    def test_the_action_is_reachable_from_the_command_palette(self):
        """Tools-menu actions are descendants of the window, so findChildren
        reaches them; this pins that the new entry is genuinely offered."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings)
        self.addCleanup(win.close)
        labels = [label for label, _k, _a in
                  command_entries(win._live_actions())]
        self.assertIn("Diagnostics", labels)
class IconFallbackTests(unittest.TestCase):
    """REGRESSION (blank toolbar buttons on Linux Mint): every icon went
    through QIcon.fromTheme, which uses its fallback only when the theme has NO
    entry for the name. A theme that registers the name but ships nothing
    drawable at our size therefore rendered an empty button, and 19 call sites
    passed no fallback at all."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ICONS = os.path.join(
        PROJECT_ROOT, "icons")

    def _bundled(self, name="home_24px.svg"):
        return os.path.join(self.ICONS, name)

    def _renderable(self, name="home_24px.svg"):
        """
        A bundled icon path that this Qt can actually draw.

        Distributions ship Qt without SVG support, so a test that needs "an
        icon which renders" must not assume the .svg does — that assumption is
        the very bug this class exists to guard.
        """
        path = self._bundled(name)
        if icon_is_visible(QIcon(path)):
            return path
        twin = os.path.splitext(path)[0] + ".png"
        self.assertTrue(icon_is_visible(QIcon(twin)),
                        f"neither {name} nor its PNG twin renders")
        return twin

    def _require_svg(self):
        if not icon_is_visible(QIcon(self._bundled("pie_24px.svg"))):
            self.skipTest("this Qt has no SVG support")

    def test_a_bundled_icon_is_used_when_the_theme_has_nothing(self):
        icon = themed_icon("definitely-not-a-theme-icon", self._bundled())
        self.assertFalse(icon.isNull())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def setUp(self):
        # The resolver caches, so each test starts from a clean slate.
        utils._icon_cache.clear()
        self.addCleanup(utils._icon_cache.clear)

    def test_a_transparent_theme_icon_is_rejected(self):
        """The remaining Mint case: the theme resolves the name to a fully
        transparent placeholder. It is not null, its pixmap is not null, and it
        still paints an empty button — only real alpha proves otherwise."""
        blank = QPixmap(24, 24)
        blank.fill(Qt.GlobalColor.transparent)
        invisible = QIcon(blank)
        self.assertFalse(invisible.isNull())
        self.assertFalse(invisible.pixmap(24, 24).isNull())
        self.assertFalse(icon_is_visible(invisible))
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: invisible)):
            icon = themed_icon("view-statistics", self._bundled())
        self.assertTrue(icon_is_visible(icon))

    def test_visibility_accepts_a_real_icon(self):
        self.assertTrue(icon_is_visible(QIcon(self._renderable())))
        self.assertFalse(icon_is_visible(QIcon()))
        self.assertFalse(icon_is_visible(None))

    def test_every_bundled_icon_paints_visible_pixels(self):
        """A fallback that renders nothing is no fallback at all. Asserted
        through bundled_icon, so a Qt without SVG passes on the PNG twins —
        which is the whole point of having them."""
        invisible = []
        for name in sorted(os.listdir(self.ICONS)):
            if not name.endswith(".svg"):
                continue
            if not icon_is_visible(bundled_icon(os.path.join(self.ICONS, name))):
                invisible.append(name)
        self.assertEqual(invisible, [])

    @staticmethod
    @contextlib.contextmanager
    def no_svg_support():
        """
        Simulate a Qt build with no SVG plugin.

        Debian/Mint's python3-pyqt6 ships no QtSvg, and there QIcon("x.svg")
        is NOT null — it simply paints nothing. That is why the venv (whose
        PyQt6 wheel bundles QtSvg) showed everything fine while the packaged
        app had blank buttons.
        """
        real = utils.QIcon

        class SvgBlindIcon(real):
            def __init__(self, *args):
                if args and isinstance(args[0], str) and args[0].endswith(".svg"):
                    super().__init__()          # unreadable: paints nothing
                else:
                    super().__init__(*args)

        with patch.object(utils, "QIcon", SvgBlindIcon):
            yield

    def test_a_png_twin_is_used_when_qt_cannot_read_svg(self):
        """THE PACKAGED-APP BUG: SVG fallbacks silently painted nothing."""
        with self.no_svg_support():
            icon = bundled_icon(self._bundled("pie_24px.svg"))
        self.assertTrue(icon_is_visible(icon))

    def test_svg_is_preferred_when_it_works(self):
        """The PNG twin is a fixed 48px raster; the SVG stays crisp at any
        size and DPI, so it must be tried first."""
        self._require_svg()
        tried = []
        real = utils.QIcon

        class Recording(real):
            def __init__(self, *args):
                if args and isinstance(args[0], str):
                    tried.append(os.path.basename(args[0]))
                super().__init__(*args)

        with patch.object(utils, "QIcon", Recording):
            icon = bundled_icon(self._bundled("pie_24px.svg"))
        self.assertTrue(icon_is_visible(icon))
        self.assertEqual(tried[0], "pie_24px.svg")
        self.assertNotIn("pie_24px.png", tried)

    def test_an_empty_path_yields_an_empty_icon(self):
        self.assertTrue(bundled_icon("").isNull())
        self.assertTrue(bundled_icon(None).isNull())

    def test_every_bundled_svg_has_a_png_twin(self):
        """The twin is what keeps the app usable without the SVG plugin."""
        missing = [
            name for name in sorted(os.listdir(self.ICONS))
            if name.endswith(".svg")
            and not os.path.exists(
                os.path.join(self.ICONS, name[:-4] + ".png"))
        ]
        self.assertEqual(missing, [])

    def test_every_png_twin_paints_visible_pixels(self):
        invisible = [
            name for name in sorted(os.listdir(self.ICONS))
            if name.endswith(".png")
            and not icon_is_visible(QIcon(os.path.join(self.ICONS, name)))
        ]
        self.assertEqual(invisible, [])

    def test_no_toolbar_icon_is_blank_without_svg_support(self):
        """The end-to-end check against the reported environment: build the
        real window on an SVG-blind Qt and assert every button still draws."""
        with self.no_svg_support():
            win = self._win()
            blank, checked = [], 0
            for action in win.tBar.actions():
                if action.isSeparator():
                    continue
                holder = action
                if isinstance(action, QWidgetAction):
                    widget = action.defaultWidget()
                    if widget is None:
                        continue
                    holder = widget
                checked += 1
                if not icon_is_visible(holder.icon()):
                    blank.append(action.text() or type(holder).__name__)
        self.assertEqual(blank, [], f"blank without SVG: {blank}")
        self.assertGreaterEqual(checked, 15)

    def test_the_cache_keys_on_the_name(self):
        """Two names sharing one fallback must not collide — otherwise the
        first answer is reused for every later icon."""
        blank = QPixmap(24, 24)
        blank.fill(Qt.GlobalColor.transparent)
        answers = {"hollow-name": QIcon(blank),
                   "good-name": QIcon(self._renderable("bucket_24px.svg"))}
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda n, *a, **kw: answers[n])):
            first = themed_icon("hollow-name", self._bundled("home_24px.svg"))
            second = themed_icon("good-name", self._bundled("home_24px.svg"))
        self.assertIsNot(first, second)
        self.assertIs(second, answers["good-name"])

    def test_results_are_cached_so_menus_stay_cheap(self):
        """Context menus rebuild their icons on every right-click."""
        calls = []
        real = utils.QIcon.fromTheme

        def counting(*args, **kwargs):
            calls.append(args[:1])
            return real(*args, **kwargs)

        with patch.object(utils.QIcon, "fromTheme", staticmethod(counting)):
            themed_icon("go-home", self._bundled())
            themed_icon("go-home", self._bundled())
            themed_icon("go-home", self._bundled())
        self.assertEqual(len(calls), 1)

    def test_a_theme_icon_that_draws_nothing_is_rejected(self):
        """The actual Mint failure: the theme HAS the name, so the icon is not
        null, but nothing is drawable at the size we paint. isNull() alone
        cannot see that, which is why the pixmap is probed."""
        hollow = QIcon(_HollowIconEngine())
        self.assertFalse(hollow.isNull())          # the trap: it looks fine
        self.assertTrue(hollow.pixmap(24, 24).isNull())
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: hollow)):
            icon = themed_icon("go-home", self._bundled())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def test_a_null_theme_icon_is_also_rejected(self):
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: QIcon())):
            icon = themed_icon("go-home", self._bundled())
        self.assertFalse(icon.pixmap(24, 24).isNull())

    def test_a_usable_theme_icon_is_preferred(self):
        """Desktop integration still wins when the theme actually delivers."""
        real = QIcon(self._renderable("bucket_24px.svg"))
        with patch.object(utils.QIcon, "fromTheme",
                          staticmethod(lambda *a, **kw: real)):
            icon = themed_icon("go-home", self._bundled("home_24px.svg"))
        self.assertIs(icon, real)

    def test_no_fallback_and_no_theme_yields_an_explicit_empty_icon(self):
        self.assertTrue(themed_icon("nope-not-here", "").isNull())

    ICON_SUFFIXES = (".svg", ".png", ".ico")

    @classmethod
    def _bundled_names(cls, text):
        """Every bundled icon filename a source names, by parsing.

        Was a regex for `"icons", "<name>"`, which matched any two adjacent
        string literals that happened to read that way -- a set of directory
        names containing "icons" next to "resources" was enough to report a
        missing icon called "resources". The path is built with os.path.join,
        so the join is what to look at.
        """
        found = set()
        for node in ast.walk(ast.parse(text)):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Attribute)
                    and node.func.attr == "join"):
                continue
            parts = [a.value for a in node.args
                     if isinstance(a, ast.Constant) and isinstance(a.value, str)]
            if "icons" not in parts:
                continue
            found |= {p for p in parts if p.endswith(cls.ICON_SUFFIXES)}
        return found

    def test_every_bundled_icon_file_referenced_actually_exists(self):
        """A typo'd filename is a silent blank, since QIcon(bad_path) is
        simply null."""
        missing = []
        seen = 0
        for source, text in read_app_sources(os.path.dirname(self.ICONS)):
            referenced = self._bundled_names(text)
            seen += len(referenced)
            for name in referenced:
                if not os.path.exists(os.path.join(self.ICONS, name)):
                    missing.append(f"{source}: {name}")
        self.assertTrue(seen, "no bundled icon references found -- the "
                              "parse or the file discovery broke")
        self.assertEqual(missing, [])

    def test_the_context_menus_icons_are_checked_too(self):
        """They are named as builder arguments, not inside an os.path.join."""
        path = os.path.join(os.path.dirname(self.ICONS),
                            "ui", "context_menu.py")
        with open(path, encoding="utf-8") as handle:
            calls = diagnostics.icon_calls(handle.read())
        names = {bundled for _name, bundled in calls if bundled}
        self.assertGreaterEqual(len(names), 8)
        missing = [n for n in sorted(names)
                   if not os.path.exists(os.path.join(self.ICONS, n))]
        self.assertEqual(missing, [])

    def test_nothing_calls_fromTheme_without_going_through_the_helper(self):
        """Any direct fromTheme call re-opens the hole, because it trusts the
        theme's claim that the name exists."""
        offenders = []
        checked = 0
        for source, text in read_app_sources(os.path.dirname(self.ICONS)):
            tree = ast.parse(text)
            # The enclosing function of every call, so a fromTheme can be
            # judged by whether its own function validates the result.
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                body = ast.unparse(node)
                if "QIcon.fromTheme" not in body:
                    continue
                checked += 1
                # Validating the icon before trusting it IS the fix: that is
                # what themed_icon, forward_icon and the diagnostics probe do.
                # Anything else is a caller taking the theme at its word.
                if "icon_is_visible" in body or "hasThemeIcon" in body:
                    continue
                offenders.append(f"{source}:{node.lineno}:{node.name}")
            # A call outside any function has nothing to validate it.
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                     ast.ClassDef)):
                    continue
                if "QIcon.fromTheme" in ast.unparse(node):
                    offenders.append(f"{source}:{node.lineno}:<module>")
        self.assertTrue(checked, "no fromTheme call found at all -- the "
                                 "audit scanned nothing")
        self.assertEqual(offenders, [])

    def _win(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(os.path.dirname(self.ICONS), settings)
        self.addCleanup(win.close)
        return win

    def test_every_toolbar_button_draws_something(self):
        """The failure the screenshots showed: buttons present, icons blank.

        A QWidgetAction (the Theme / Bookmarks / Tools dropdowns) carries no
        icon itself — the QToolButton it wraps does — so those are checked
        through their widget rather than skipped.
        """
        win = self._win()
        blank, checked = [], 0
        for action in win.tBar.actions():
            if action.isSeparator():
                continue
            holder = action
            if isinstance(action, QWidgetAction):
                widget = action.defaultWidget()
                if widget is None:
                    continue
                holder = widget
            checked += 1
            if holder.icon().pixmap(24, 24).isNull():
                blank.append(action.text() or type(holder).__name__)
        self.assertEqual(blank, [], f"blank toolbar icons: {blank}")
        # Guard the guard: an empty toolbar would pass the assertion above.
        self.assertGreaterEqual(checked, 15)
class ProfileAccentTests(unittest.TestCase):
    """The launcher's badges only help while picking a profile; once a window
    is open nothing distinguished prod from dev."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_normalize_accepts_hex_and_rejects_everything_else(self):
        self.assertEqual(normalize_accent("#C62828"), "#c62828")
        self.assertEqual(normalize_accent("#abc"), "#abc")
        self.assertEqual(normalize_accent("  #abc  "), "#abc")
        for bad in ("", None, "red", "c62828", "#12345", "#gggggg",
                    "#abc; drop table", 42):
            with self.subTest(value=bad):
                self.assertEqual(normalize_accent(bad), "")

    def _win(self, accent=""):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        root = PROJECT_ROOT
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(
                root,
                settings,
                read_only='false',
                accent=accent)
        self.addCleanup(win.close)
        return win

    def test_an_accent_bands_the_window(self):
        win = self._win("#c62828")
        self.assertEqual(win.accent, "#c62828")
        self.assertFalse(win.accent_bar.isHidden())
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#c62828"))

    def test_no_accent_hides_the_band(self):
        win = self._win("")
        self.assertEqual(win.accent, "")
        self.assertTrue(win.accent_bar.isHidden())

    def test_a_malformed_accent_hides_the_band(self):
        """Settings are user-editable text; garbage must not paint a band that
        looks like a rendering fault."""
        win = self._win("not-a-colour")
        self.assertEqual(win.accent, "")
        self.assertTrue(win.accent_bar.isHidden())

    def test_the_band_uses_the_palette_not_a_stylesheet(self):
        """Any stylesheet makes a widget stop following the palette, which is
        how theming breaks elsewhere in this app."""
        win = self._win("#1565c0")
        self.assertEqual(win.accent_bar.styleSheet(), "")
        self.assertTrue(win.accent_bar.autoFillBackground())

    def test_the_accent_can_be_changed_at_runtime(self):
        """Switching profiles without restarting must re-mark the window."""
        win = self._win("#c62828")
        self.assertEqual(win.set_accent("#2e7d32"), "#2e7d32")
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#2e7d32"))
        win.set_accent("")
        self.assertTrue(win.accent_bar.isHidden())
        # Back to a colour: the band was explicitly hidden above, so this is
        # the only path where showing it again actually matters.
        win.set_accent("#1565c0")
        self.assertFalse(win.accent_bar.isHidden())
        self.assertEqual(
            win.accent_bar.palette().color(QPalette.ColorRole.Window),
            QColor("#1565c0"))

    def test_a_profile_without_an_accent_still_opens(self):
        """Profiles saved before this feature pass a shorter tuple."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        self.assertEqual(win.accent, "")

    def test_the_switcher_carries_the_colour(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        crypto = Crypto(Fernet.generate_key().decode())
        settings.beginGroup("common")
        settings.setValue("key", crypto.key)
        settings.endGroup()
        raw = {"name": "prod", "url": "u", "region": "r", "bucket_name": "b",
               "access_key": crypto.encrypt("AK"),
               "secret_key": crypto.encrypt("SK"),
               "no_ssl_check": "false", "use_path": "false",
               "session_token": "", "read_only": "false", "color": "#C62828"}
        profile = profile_switcher.decrypt_profile(settings, raw)
        self.assertEqual(profile.color, "#c62828")

    def test_the_switcher_normalizes_a_bad_colour(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        crypto = Crypto(Fernet.generate_key().decode())
        settings.beginGroup("common")
        settings.setValue("key", crypto.key)
        settings.endGroup()
        raw = {"name": "p", "url": "u", "region": "r", "bucket_name": "",
               "access_key": crypto.encrypt("AK"),
               "secret_key": crypto.encrypt("SK"),
               "no_ssl_check": "false", "use_path": "false",
               "session_token": "", "read_only": "false", "color": "bogus"}
        self.assertEqual(
            profile_switcher.decrypt_profile(settings, raw).color, "")

    def test_the_profile_form_round_trips_the_colour(self):
        dlg = SettingsWindow(settings=(
            "n", "u", "r", "b", "AK", "SK", "false", "true", "", "false",
            "#2e7d32"))
        self.addCleanup(dlg.close)
        dlg.setRetVal()
        self.assertEqual(dlg.retrunVal[10], "#2e7d32")

    def test_the_profile_form_defaults_to_no_colour(self):
        dlg = SettingsWindow()
        self.addCleanup(dlg.close)
        dlg.setRetVal()
        self.assertEqual(dlg.retrunVal[10], "")
class ThemeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_apply_returns_normalized_name(self):
        self.assertEqual(theme.apply_theme(self._app, "dark"), "dark")
        self.assertEqual(theme.apply_theme(self._app, "LIGHT"), "light")
        self.assertEqual(theme.apply_theme(self._app, "bogus"), "system")

    def test_dark_palette_is_dark(self):
        theme.apply_theme(self._app, "dark")
        window = self._app.palette().color(QPalette.ColorRole.Window)
        self.assertLess(window.lightness(), 128)
        # restore for other tests
        theme.apply_theme(self._app, "system")
class DiagnosticsSettingsTests(unittest.TestCase):
    """The report claims to show the active transfer settings, so a setting
    that can change what an upload does belongs in it."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_report_carries_the_upload_and_profile_settings(self):
        model = make_model(bucket="bkt", requester_pays=True,
                           public_base_url="https://cdn.example.com")
        model.set_content_type_options(
            detect=True, overrides={"md": "text/markdown"})
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9", model=model))
        self.assertIn("Requester pays", text)
        self.assertIn("cdn.example.com", text)
        self.assertIn("Detect Content-Type", text)
        self.assertIn("Content-Type overrides", text)

    def test_a_profile_without_them_still_renders(self):
        model = make_model(bucket="bkt")
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9", model=model))
        self.assertIn("(endpoint)", text)

    def test_a_proxy_password_never_reaches_the_report(self):
        """The report is written to be pasted into a bug tracker, and a
        corporate proxy is routinely http://user:password@proxy:3128."""
        model = make_model(bucket="bkt",
                           proxy_url="http://alice:s3cr3t@proxy.corp:3128")
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9", model=model))
        self.assertNotIn("s3cr3t", text)
        self.assertNotIn("alice", text)
        self.assertIn("proxy.corp:3128", text)

    def test_a_proxy_without_credentials_is_shown_as_configured(self):
        model = make_model(bucket="bkt", proxy_url="http://proxy.corp:3128")
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9", model=model))
        self.assertIn("http://proxy.corp:3128", text)

    def test_no_proxy_still_reads_as_direct(self):
        model = make_model(bucket="bkt")
        text = diagnostics.format_report(
            diagnostics.collect(self.ROOT, version="9.9.9", model=model))
        self.assertIn("(direct)", text)
class ForwardIconTests(unittest.TestCase):
    """Forward is Back pointing the other way — no second asset, so no second
    PNG twin to keep in step for the no-SVG builds."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = PROJECT_ROOT

    def test_a_mirrored_icon_still_draws_something(self):
        back = utils.themed_icon(
            "go-previous",
            os.path.join(self.ROOT, "icons", "arrow_back_24px.svg"))
        mirrored = utils.mirrored_icon(back)
        self.assertTrue(utils.icon_is_visible(mirrored))

    def test_mirroring_actually_flips_the_pixels(self):
        back = utils.themed_icon(
            "go-previous",
            os.path.join(self.ROOT, "icons", "arrow_back_24px.svg"))
        original = back.pixmap(24, 24).toImage()
        flipped = utils.mirrored_icon(back).pixmap(24, 24).toImage()
        self.assertNotEqual(original, flipped)

    def test_an_empty_icon_is_returned_unchanged(self):
        self.assertTrue(utils.mirrored_icon(QIcon()).isNull())
        self.assertTrue(utils.mirrored_icon(None).isNull())

    def test_the_forward_icon_is_never_blank(self):
        back = utils.themed_icon(
            "go-previous",
            os.path.join(self.ROOT, "icons", "arrow_back_24px.svg"))
        self.assertTrue(utils.icon_is_visible(utils.forward_icon(back)))
class TreemapWidgetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _widget(self):
        widget = main_window.TreemapWidget()
        self.addCleanup(widget.deleteLater)
        widget.resize(400, 200)
        return widget

    def test_rows_become_rectangles(self):
        widget = self._widget()
        widget.set_rows([("a", 60, False), ("b", 40, False)])
        self.assertEqual(len(widget._rects), 2)

    def test_clicking_a_tile_asks_to_drill(self):
        widget = self._widget()
        widget.set_rows([("a", 100, False)])
        seen = []
        widget.drilled.connect(seen.append)
        widget.mouseReleaseEvent(QMouseEvent(
            QEvent.Type.MouseButtonRelease, QPointF(10, 10), QPointF(10, 10),
            Qt.MouseButton.LeftButton, Qt.MouseButton.NoButton,
            Qt.KeyboardModifier.NoModifier))
        self.assertEqual(seen, ["a"])

    def test_the_folded_group_is_not_drillable(self):
        widget = self._widget()
        widget.set_rows([("… 40 more", 100, True)])
        seen = []
        widget.drilled.connect(seen.append)
        widget.mouseReleaseEvent(QMouseEvent(
            QEvent.Type.MouseButtonRelease, QPointF(10, 10), QPointF(10, 10),
            Qt.MouseButton.LeftButton, Qt.MouseButton.NoButton,
            Qt.KeyboardModifier.NoModifier))
        self.assertEqual(seen, [])

    def test_painting_a_full_map_does_not_raise(self):
        """A Python error inside paint() aborts the process, so it is only
        ever caught by actually painting."""
        widget = self._widget()
        widget.set_rows([(f"prefix-{i}", 100 - i, False) for i in range(40)])
        pixmap = QPixmap(400, 200)
        widget.render(pixmap)
