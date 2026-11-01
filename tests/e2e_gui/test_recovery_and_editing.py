"""Point-in-time restore, two-way sync, external editing and thumbnails,
driven through the real window against a real server.

Each one is a plan the window builds and a background job it runs; what
only a server settles is the end state -- the bucket after the restore, the
deletion that the second two-way run propagates, the object a save in
another program rewrites, and the tile a real image fills.
"""

import os
import tempfile
import time

from PyQt6.QtCore import QBuffer, QByteArray, QDateTime, QIODevice
from PyQt6.QtGui import QColor, QImage
from unittest.mock import patch

import ui.window.external_edit as external_edit
from core.two_way import (
    CONFLICT, DELETE_REMOTE, DOWNLOAD, UPLOAD, load_state, resolve_conflict,
)
from tests.e2e_gui.harness import GuiServerCase
from ui.dialogs.point_in_time import PointInTimeDialog
from ui.window.two_way import plan_two_way_pass


def _png():
    image = QImage(64, 32, QImage.Format.Format_RGB32)
    image.fill(QColor("blue"))
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


class PointInTimeWindowTests(GuiServerCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.s3.put_bucket_versioning(
            Bucket=cls.bucket, VersioningConfiguration={"Status": "Enabled"})

    def test_the_dialog_plans_and_the_queue_restores(self):
        self.put("pt/a.txt", b"v1")
        time.sleep(1.2)
        moment = QDateTime.currentDateTime()
        time.sleep(1.2)
        self.put("pt/a.txt", b"v2")
        self.put("pt/new.txt", b"later")
        self.navigate("pt/")
        dlg = PointInTimeDialog(self.window, self.window, self.window.data_model,
                                "pt/", versioning="Enabled")
        self.addCleanup(dlg.close)
        dlg.moment.setDateTime(moment)
        self.assertTrue(dlg.preview())
        self.pump(lambda: dlg._thread is None, what="the version history")
        self.assertIn("1 to restore", dlg.info.text())
        self.assertIn("1 to mark deleted", dlg.info.text())
        with self.recording_messages(), self.confirming():
            self.assertTrue(dlg.run())
            self.await_transfers()
        self.assertEqual(self.last_queue_entry().status, "done")
        self.assertEqual(self.body("pt/a.txt"), b"v1")
        self.assertNotIn("pt/new.txt", self.keys("pt/"))


class TwoWaySyncWindowTests(GuiServerCase):

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.window.TWO_WAY_STATE_DIR = os.path.join(self.tmp.name, "state")
        self.local = os.path.join(self.tmp.name, "local")
        os.makedirs(self.local)
        # The class shares one bucket, so each test syncs its own prefix.
        self.prefix = f"tw-{self._testMethodName[-12:]}/"
        self.navigate("")

    def _write(self, rel, body):
        with open(os.path.join(self.local, rel), "wb") as handle:
            handle.write(body)

    def _pass(self):
        state_file = self.window.two_way_state_file(self.local, self.prefix)
        result = plan_two_way_pass(self.window.data_model.clone_for_worker(),
                                   self.local, self.prefix, state_file)
        return result, state_file

    def _run(self, result):
        with self.recording_messages():
            self.window.start_two_way_sync(result["plan"], self.local, self.prefix,
                                           result["state"])
            self.await_transfers()
            self.pump(lambda: not self.window.__dict__.get("_two_way_threads"),
                      what="the state to be recorded")

    def test_two_runs_copy_both_ways_then_propagate_a_deletion(self):
        self._write("mine.txt", b"local side")
        self.put(self.prefix + "theirs.txt", b"bucket side")
        result, state_file = self._pass()
        actions = {a["rel"]: a["action"] for a in result["plan"]}
        self.assertEqual(actions, {"mine.txt": UPLOAD, "theirs.txt": DOWNLOAD})
        self._run(result)
        self.assertEqual(self.body(self.prefix + "mine.txt"), b"local side")
        with open(os.path.join(self.local, "theirs.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"bucket side")
        self.assertEqual(sorted(load_state(state_file)), ["mine.txt", "theirs.txt"])

        os.remove(os.path.join(self.local, "mine.txt"))
        result, _ = self._pass()
        actions = {a["rel"]: a["action"] for a in result["plan"]}
        self.assertEqual(actions["mine.txt"], DELETE_REMOTE)
        self.assertEqual(actions["theirs.txt"], "skip")
        self._run(result)
        self.assertNotIn(self.prefix + "mine.txt", self.keys(self.prefix))


    def test_a_change_on_both_sides_is_a_conflict_until_resolved(self):
        """The state file records the server's own ETags; if their form
        differed from the listing's on the next run, every file would read
        as changed in the bucket. A real edit on each side must read as a
        conflict -- and nothing else must."""
        self._write("clash.txt", b"v1")
        self._write("calm.txt", b"calm")
        result, _ = self._pass()
        self._run(result)
        time.sleep(1.1)
        self._write("clash.txt", b"local edit")
        self.put(self.prefix + "clash.txt", b"bucket edit")
        result, _ = self._pass()
        actions = {a["rel"]: a for a in result["plan"]}
        self.assertEqual(actions["clash.txt"]["action"], CONFLICT)
        self.assertEqual(actions["calm.txt"]["action"], "skip",
                         "an untouched file read as changed after a run")
        # A run that leaves the conflict open keeps it a conflict.
        self._run(result)
        result, _ = self._pass()
        actions = {a["rel"]: a for a in result["plan"]}
        self.assertEqual(actions["clash.txt"]["action"], CONFLICT)
        resolved = [resolve_conflict(a, "local") if a["rel"] == "clash.txt" else a
                    for a in result["plan"]]
        self._run({"plan": resolved, "state": result["state"]})
        self.assertEqual(self.body(self.prefix + "clash.txt"), b"local edit")
        result, _ = self._pass()
        self.assertEqual({a["action"] for a in result["plan"]}, {"skip"})


class ExternalEditWindowTests(GuiServerCase):

    def setUp(self):
        super().setUp()
        self.navigate("")
        self.addCleanup(self.window.stop_external_edits, False)

    def test_a_save_in_another_program_lands_in_the_bucket(self):
        key = self.put("ed/notes.txt", b"first draft")
        with patch.object(external_edit, "run_with_progress", _inline):
            path = self.window.edit_externally(key, open_file=False)
        self.assertIsNotNone(path)
        time.sleep(0.05)
        with open(path, "wb") as handle:
            handle.write(b"second draft")
        # The real QFileSystemWatcher, the real settle timer, a real PUT.
        self.pump(lambda: self.body(key) == b"second draft",
                  what="the save to be uploaded")

    def test_a_server_side_change_is_not_overwritten(self):
        key = self.put("ed/shared.txt", b"theirs v1")
        with patch.object(external_edit, "run_with_progress", _inline):
            path = self.window.edit_externally(key, open_file=False)
        self.put(key, b"theirs v2")
        chosen = []

        def _decline(box):
            chosen.append(box.text())
            return 0

        with patch.object(external_edit.QMessageBox, "exec", _decline):
            time.sleep(0.05)
            with open(path, "wb") as handle:
                handle.write(b"mine")
            self.pump(lambda: chosen, what="the conflict to be reported")
        self.assertIn("changed on the server", chosen[0])
        self.assertEqual(self.body(key), b"theirs v2")


class ThumbnailWindowTests(GuiServerCase):

    def test_an_image_object_fills_its_tile(self):
        self.put("thumbs/blue.png", _png(), ContentType="image/png")
        self.put("thumbs/notes.txt", b"x")
        self.navigate("thumbs/")
        window = self.window
        self.addCleanup(window.set_thumbnail_view, False)
        window.set_thumbnail_view(True)
        self.pump(lambda: len(window._thumb_cache) >= 1,
                  what="the thumbnail to be fetched")
        cached = [ident for ident in window._thumb_cache._items]
        self.assertEqual([ident[1] for ident in cached], ["thumbs/blue.png"])
        image = window._thumb_cache.get(cached[0])
        self.assertEqual((image.width(), image.height()), (128, 64))

