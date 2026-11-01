"""Editing an object in another application, and the thumbnail grid."""

import main_window
import os
import time
import types
import unittest
import ui.window.external_edit as external_edit
from datetime import datetime
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, QSettings
from PyQt6.QtGui import QColor, QImage
from PyQt6.QtWidgets import QMessageBox
from core.entries import Item
from model import FSObjectType, Model
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from ui.widgets.thumbnails import (
    THUMB_MAX_BYTES, ThumbnailCache, ThumbnailLoader, thumbnail_image,
    wants_thumbnail,
)
from ui.window.external_edit import conflict_copy_key
from unittest.mock import patch


def _png(width=40, height=20, colour="red"):
    image = QImage(width, height, QImage.Format.Format_RGB32)
    image.fill(QColor(colour))
    data = QByteArray()
    buffer = QBuffer(data)
    buffer.open(QIODevice.OpenModeFlag.WriteOnly)
    image.save(buffer, "PNG")
    return bytes(data)


def _inline(_parent, _title, fn, *a, **kw):
    try:
        return fn(None), None
    except Exception as exc:
        return None, exc


class _EditModel:
    """A bucket of one object, with conditional writes."""

    bucket = "bkt"
    normalize_etag = staticmethod(Model.normalize_etag)

    def __init__(self, body=b"original", size=None):
        self.body = body
        self.etag = "e1"
        self.size = len(body) if size is None else size
        self.puts = []
        self.change_during_download = False

    def clone_for_worker(self):
        return self

    def object_properties(self, key):
        return {"ETag": f'"{self.etag}"', "ContentLength": self.size}

    def download_file(self, key, path, folder, **kw):
        with open(path, "wb") as handle:
            handle.write(self.body)
        if self.change_during_download:
            self.etag = "moved"

    def put_object_body(self, key, data, if_match=""):
        self.puts.append((key, data, if_match))
        if if_match and if_match != self.etag:
            raise Model.PreconditionFailed("changed")
        self.body = data
        self.etag = f"e{len(self.puts) + 1}"
        return f'"{self.etag}"'


class ExternalEditTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False, model=None):
        settings = QSettings("s3duck-tests-edit", "s3duck-tests-edit")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings, read_only=read_only)
        win.data_model.bucket = "bkt"
        self.model = model or _EditModel()
        win.data_model.clone_for_worker = lambda: self.model
        self.addCleanup(lambda: (win.stop_external_edits(ask=False), win.close()))
        return win

    def _open(self, win, key="doc.txt"):
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QDesktopServices, "openUrl") as opened:
            path = win.edit_externally(key)
        return path, opened

    def _settle(self, win):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            self._app.processEvents()
            if not win.__dict__.get("_edit_threads") and not win._edit_pending:
                break
            time.sleep(0.005)
        self._app.processEvents()

    def _save(self, win, path, body):
        time.sleep(0.01)
        with open(path, "wb") as handle:
            handle.write(body)
        win._on_edit_file_changed(path)
        win.flush_external_edits()
        self._settle(win)

    def test_conflict_copy_names(self):
        when = datetime(2026, 1, 2, 3, 4, 5)
        self.assertEqual(conflict_copy_key("a/b/report.txt", when),
                         "a/b/report (conflict 2026-01-02 030405).txt")
        self.assertEqual(conflict_copy_key("README", when),
                         "README (conflict 2026-01-02 030405)")
        self.assertEqual(conflict_copy_key(".env", when),
                         ".env (conflict 2026-01-02 030405)")

    def test_a_save_is_uploaded_with_the_etag_it_was_opened_at(self):
        win = self._window()
        path, opened = self._open(win)
        opened.assert_called_once()
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), b"original")
        self.assertIn(path, win._edit_watcher.files())
        self.assertTrue(win.actStopEdits.isEnabled())
        self._save(win, path, b"edited once")
        self.assertEqual(self.model.puts[-1], ("doc.txt", b"edited once", "e1"))
        self._save(win, path, b"edited twice")
        self.assertEqual(self.model.puts[-1][2], "e2",
                         "the second save did not carry the new ETag")
        self.assertEqual(self.model.body, b"edited twice")

    def test_an_unchanged_file_is_not_uploaded(self):
        win = self._window()
        path, _ = self._open(win)
        win._on_edit_file_changed(path)
        win.flush_external_edits()
        self._settle(win)
        self.assertEqual(self.model.puts, [])

    def test_opening_twice_reuses_the_copy(self):
        win = self._window()
        path, _ = self._open(win)
        again, opened = self._open(win)
        self.assertEqual(path, again)
        opened.assert_called_once()

    def _conflict(self, choice):
        win = self._window()
        path, _ = self._open(win)
        self.model.etag = "someone-else"

        class _Box:
            Icon = QMessageBox.Icon
            ButtonRole = QMessageBox.ButtonRole

            def __init__(self, _parent):
                self.buttons = {}

            def setWindowTitle(self, _t):
                pass

            def setIcon(self, _i):
                pass

            def setText(self, _t):
                pass

            def addButton(self, label, _role):
                self.buttons[label] = object()
                return self.buttons[label]

            def exec(self):
                return 0

            def clickedButton(self):
                return self.buttons.get(choice)

        with patch.object(external_edit, "QMessageBox", _Box):
            self._save(win, path, b"mine")
            self._settle(win)
        return win

    def test_a_conflict_can_overwrite(self):
        self._conflict("Overwrite")
        self.assertEqual(self.model.puts[0][2], "e1")
        self.assertEqual(self.model.puts[-1], ("doc.txt", b"mine", ""))

    def test_a_conflict_can_save_beside(self):
        win = self._conflict("Save as a copy")
        key = self.model.puts[-1][0]
        self.assertTrue(key.startswith("doc (conflict "), key)
        self.assertEqual(self.model.puts[-1][2], "")
        state = next(iter(win.external_edits().values()))
        self.assertEqual(state["key"], key, "later saves would still hit the original")

    def test_a_conflict_left_alone_uploads_nothing_more(self):
        self._conflict("Not now")
        self.assertEqual(len(self.model.puts), 1)

    def test_guards(self):
        win = self._window()
        self.assertIsNone(win.edit_externally("folder/"))
        big = self._window(model=_EditModel(size=external_edit.ExternalEditMixin
                                            .EXTERNAL_EDIT_LIMIT + 1))
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QMessageBox, "warning") as warn:
            self.assertIsNone(big.edit_externally("big.bin"))
        self.assertIn("too large", warn.call_args.args[2])
        racy_model = _EditModel()
        racy_model.change_during_download = True
        racy = self._window(model=racy_model)
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QMessageBox, "warning") as warn:
            self.assertIsNone(racy.edit_externally("doc.txt"))
        self.assertIn("changed while", warn.call_args.args[2])

    def test_a_read_only_profile_opens_without_watching(self):
        win = self._window(read_only=True)
        path, _ = self._open(win)
        self.assertNotIn(path, win._edit_watcher.files())
        self._save(win, path, b"local only")
        self.assertEqual(self.model.puts, [])

    def test_an_editor_that_saves_by_rename_is_followed(self):
        win = self._window()
        path, _ = self._open(win)
        os.remove(path)
        with open(path, "wb") as handle:
            handle.write(b"renamed in")
        win._on_edit_dir_changed(os.path.dirname(path))
        win.flush_external_edits()
        self._settle(win)
        self.assertEqual(self.model.puts[-1][1], b"renamed in")
        self.assertIn(path, win._edit_watcher.files())

    def test_stopping_offers_unsent_changes(self):
        win = self._window()
        path, _ = self._open(win)
        time.sleep(0.01)
        with open(path, "wb") as handle:
            handle.write(b"not yet sent")
        with patch.object(external_edit, "run_with_progress", _inline), \
                patch.object(external_edit.QMessageBox, "question",
                             return_value=QMessageBox.StandardButton.Yes):
            win.stop_external_edits()
        self.assertEqual(self.model.puts[-1], ("doc.txt", b"not yet sent", "e1"))
        self.assertEqual(win.external_edits(), {})
        self.assertFalse(win.actStopEdits.isEnabled())

    def test_a_failed_upload_is_reported(self):
        win = self._window()
        path, _ = self._open(win)

        def refuse(*a, **kw):
            raise RuntimeError("AccessDenied")

        self.model.put_object_body = refuse
        logged = []
        win.log = logged.append
        self._save(win, path, b"x")
        self.assertTrue(any("not saved" in m for m in logged), logged)


class ThumbnailPieceTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_which_objects_get_one(self):
        self.assertTrue(wants_thumbnail("a.JPG", 100))
        self.assertFalse(wants_thumbnail("a.txt", 100))
        self.assertFalse(wants_thumbnail("a.png", 0))
        self.assertFalse(wants_thumbnail("a.png", THUMB_MAX_BYTES + 1))

    def test_decode_and_scale(self):
        image = thumbnail_image(_png(400, 200))
        self.assertEqual((image.width(), image.height()), (128, 64))
        self.assertIsNone(thumbnail_image(b"not an image"))

    def test_cache_is_lru(self):
        cache = ThumbnailCache(limit=2)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.get("a")
        cache.put("c", 3)
        self.assertIsNone(cache.get("b"))
        self.assertEqual((cache.get("a"), cache.get("c"), len(cache)), (1, 3, 2))

    def test_the_loader_drops_stale_requests(self):
        loader = ThumbnailLoader()
        self.addCleanup(loader.shutdown)
        got = []
        loader.loaded.connect(lambda g, k, img: got.append((g, k, img is not None)))
        model = types.SimpleNamespace(get_object_preview=lambda key, cap: {
            "data": _png(), "truncated": False})
        generation = loader.new_generation()
        loader.request(model, "a.png")
        deadline = time.monotonic() + 5
        while not got and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self.assertEqual(got, [(generation, "a.png", True)])
        broken = types.SimpleNamespace(get_object_preview=lambda key, cap: (_ for _ in ()).throw(RuntimeError("x")))
        loader.request(broken, "b.png")
        deadline = time.monotonic() + 5
        while len(got) < 2 and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self.assertEqual(got[-1], (generation, "b.png", False))


class ThumbnailWindowTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.settings = QSettings("s3duck-tests-thumbs", "s3duck-tests-thumbs")
        self.settings.clear()
        self.addCleanup(self.settings.clear)

    def _window(self):
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, self.settings)
        self.addCleanup(win.close)
        # The window's own first listing would land on top of ours.
        self._wait(lambda: win.listview.isEnabled() and not (
            win._nav_thread and win._thread_is_running(win._nav_thread)))
        win.data_model.bucket = "bkt"
        win.data_model.current_folder = "pics/"
        win.in_bucket_list_mode = lambda: False
        self.fetched = []

        class _Clone:
            def get_object_preview(_s, key, cap):
                self.fetched.append(key)
                return {"data": _png(), "truncated": False}

        win.data_model.clone_for_worker = lambda: _Clone()
        items = [Item("a.png", FSObjectType.FILE, "", 100,
                                  etag="ea"),
                 Item("notes.txt", FSObjectType.FILE, "", 5),
                 Item("dir", FSObjectType.FOLDER, "", 0)]
        win.modelToListView(items)
        return win

    def _wait(self, predicate):
        deadline = time.monotonic() + 5
        while not predicate() and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def _row(self, win, name):
        return next(win.model.item(r, 0) for r in range(win.model.rowCount())
                    if win.model.item(r, 0).text() == name)

    def test_the_grid_shares_the_listing_selection(self):
        win = self._window()
        self.assertIs(win.thumb_grid.selectionModel(), win.listview.selectionModel())
        self.assertIs(win.thumb_grid.model(), win.proxy)

    def test_switching_on_fetches_only_images_and_paints_them(self):
        win = self._window()
        before = self._row(win, "a.png").icon().cacheKey()
        win.actThumbnails.setChecked(True)
        self.assertTrue(win.thumbnail_view_active())
        self.assertTrue(win.listview.isHidden())
        self._wait(lambda: len(win._thumb_cache) == 1)
        self.assertEqual(self.fetched, ["pics/a.png"])
        self.assertNotEqual(self._row(win, "a.png").icon().cacheKey(), before)
        self.assertTrue(self.settings.value("common/thumbnail_view") in ("true", True))

    def test_the_cache_serves_a_relisting_without_a_fetch(self):
        win = self._window()
        win.set_thumbnail_view(True)
        self._wait(lambda: len(win._thumb_cache) == 1)
        self.fetched.clear()
        win.modelToListView([Item(
            "a.png", FSObjectType.FILE, "", 100, etag="ea")])
        self.assertEqual(self.fetched, [])

    def test_a_result_for_a_left_folder_is_ignored(self):
        win = self._window()
        win.set_thumbnail_view(True)
        stale = win._thumb_loader.generation
        win._thumb_loader.new_generation()
        win._on_thumbnail_loaded(stale, "pics/a.png", thumbnail_image(_png()))
        self.assertEqual(len(win._thumb_cache), 0)

    def test_nothing_is_fetched_while_the_listing_moves(self):
        """Switched on mid-navigation, the view fetched the old rows' names
        under the new folder: objects nobody was looking at."""
        win = self._window()
        win.data_model.current_folder = "elsewhere/"
        self.assertFalse(win.listing_settled())
        win.set_thumbnail_view(True)
        self.assertEqual(win.request_thumbnails(), 0)
        self._app.processEvents()
        self.assertEqual(self.fetched, [])

    def test_switching_off_and_the_setting_reopens_it(self):
        win = self._window()
        win.set_thumbnail_view(True)
        win.set_thumbnail_view(False)
        self.assertFalse(win.listview.isHidden())
        self.assertFalse(win.actThumbnails.isChecked())
        win.set_thumbnail_view(True)
        again = self._window()
        self.assertTrue(again.thumbnail_view_active(),
                        "the view was not restored at startup")


if __name__ == "__main__":
    unittest.main()
