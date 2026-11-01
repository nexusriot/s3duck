"""Two-way sync: the planner, its state file, the dialog and the window."""

import hashlib
import main_window
import os
import tempfile
import time
import types
import unittest
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QMessageBox
from core.two_way import (
    CONFLICT, DELETE_LOCAL, DELETE_REMOTE, DOWNLOAD, SKIP, UPLOAD,
    build_state, load_state, md5_matches, plan_two_way, resolve_conflict,
    save_state, state_path,
)
from tests.support import PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window
from ui.dialogs.compare import SyncDialog
from ui.window.two_way import plan_two_way_pass, remote_digest_tree
from unittest.mock import Mock, patch


def _state(*rels, size=1, mtime=100.0, etag="e"):
    return {rel: {"local": [size, mtime], "remote": [size, etag]} for rel in rels}


class PlannerTests(unittest.TestCase):

    def _plan(self, local, remote, state, **kw):
        return {a["rel"]: (a["action"], a["reason"])
                for a in plan_two_way(local, remote, state, **kw)}

    def test_changes_on_one_side_flow_to_the_other(self):
        state = _state("a", "b", "c")
        plan = self._plan({"a": (2, 200.0), "b": (1, 100.0), "c": (1, 100.0)},
                          {"a": (1, "e"), "b": (5, "new"), "c": (1, "e")}, state)
        self.assertEqual(plan["a"][0], UPLOAD)
        self.assertEqual(plan["b"][0], DOWNLOAD)
        self.assertEqual(plan["c"][0], SKIP)

    def test_deletions_need_history(self):
        state = _state("gone-here", "gone-there")
        plan = self._plan({"gone-there": (1, 100.0), "fresh": (1, 1.0)},
                          {"gone-here": (1, "e"), "new-there": (1, "x")}, state)
        self.assertEqual(plan["gone-here"][0], DELETE_REMOTE)
        self.assertEqual(plan["gone-there"][0], DELETE_LOCAL)
        self.assertEqual(plan["fresh"], (UPLOAD, "new locally"))
        self.assertEqual(plan["new-there"], (DOWNLOAD, "new in the bucket"))

    def test_the_first_run_never_deletes(self):
        plan = self._plan({"only-here": (1, 1.0)}, {"only-there": (1, "x")}, {})
        self.assertEqual({p[0] for p in plan.values()}, {UPLOAD, DOWNLOAD})

    def test_conflicts(self):
        state = _state("both", "edited-deleted", "deleted-edited")
        plan = self._plan(
            {"both": (9, 900.0), "edited-deleted": (9, 900.0)},
            {"both": (8, "x"), "deleted-edited": (8, "x")}, state)
        self.assertEqual(plan["both"], (CONFLICT, "changed on both sides"))
        self.assertEqual(plan["edited-deleted"][1],
                         "changed locally, deleted in the bucket")
        self.assertEqual(plan["deleted-edited"][1],
                         "changed in the bucket, deleted locally")

    def test_no_history_on_both_sides_asks_the_content(self):
        local, remote = {"x": (1, 1.0)}, {"x": (1, "e")}
        self.assertEqual(self._plan(local, remote, {})["x"][0], CONFLICT)
        self.assertEqual(self._plan(local, remote, {}, identical=lambda r: True)["x"][0], SKIP)
        self.assertEqual(self._plan(local, remote, {}, identical=lambda r: False)["x"],
                         (CONFLICT, "differs on both sides"))

    def test_mtime_tolerance_and_excludes(self):
        state = _state("a")
        self.assertEqual(self._plan({"a": (1, 101.0)}, {"a": (1, "e")}, state)["a"][0], SKIP)
        plan = self._plan({"a": (2, 1.0), "tmp/x": (1, 1.0)}, {"a": (1, "e")}, state,
                          exclude="tmp/")
        self.assertNotIn("tmp/x", plan)

    def test_resolving(self):
        both = {"action": CONFLICT, "rel": "x", "size": 1, "reason": "changed on both sides"}
        self.assertEqual(resolve_conflict(both, "local")["action"], UPLOAD)
        self.assertEqual(resolve_conflict(both, "remote")["action"], DOWNLOAD)
        here = dict(both, reason="changed locally, deleted in the bucket")
        self.assertEqual(resolve_conflict(here, "local")["action"], UPLOAD)
        self.assertEqual(resolve_conflict(here, "remote")["action"], DELETE_LOCAL)
        there = dict(both, reason="changed in the bucket, deleted locally")
        self.assertEqual(resolve_conflict(there, "local")["action"], DELETE_REMOTE)
        self.assertEqual(resolve_conflict(there, "remote")["action"], DOWNLOAD)
        self.assertEqual(resolve_conflict({"action": UPLOAD}, "remote"), {"action": UPLOAD})


class StateTests(unittest.TestCase):

    def test_build_records_only_agreement_and_keeps_open_conflicts(self):
        old = _state("conflict", etag="old")
        state = build_state({"a": (3, 1.0), "b": (3, 1.0), "conflict": (9, 9.0)},
                            {"a": (3, "ea"), "b": (4, "eb"), "conflict": (8, "z")},
                            keep_old={"conflict"}, old_state=old)
        self.assertEqual(state["a"], {"local": [3, 1.0], "remote": [3, "ea"]})
        self.assertNotIn("b", state)
        self.assertEqual(state["conflict"], old["conflict"])

    def test_file_round_trip_and_damage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = state_path(tmp, "http://e/", "b", "p/", "/data")
            self.assertEqual(path, state_path(tmp, "http://e", "b", "p/", "/data/"))
            self.assertNotEqual(path, state_path(tmp, "http://e", "b", "q/", "/data"))
            self.assertEqual(load_state(path), {})
            save_state(os.path.join(tmp, "deep", "s.json"), {"a": 1})
            self.assertEqual(load_state(os.path.join(tmp, "deep", "s.json")), {"a": 1})
            with open(path, "w") as handle:
                handle.write("{broken")
            self.assertEqual(load_state(path), {})

    def test_md5_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f")
            with open(path, "wb") as handle:
                handle.write(b"hello")
            digest = hashlib.md5(b"hello").hexdigest()
            self.assertIs(md5_matches(path, f'"{digest}"'), True)
            self.assertIs(md5_matches(path, "0" * 32), False)
            self.assertIsNone(md5_matches(path, "abc-2"))
            self.assertIsNone(md5_matches(path, digest, limit=1))
            self.assertIsNone(md5_matches(os.path.join(tmp, "missing"), digest))


class _DigestModel:
    def __init__(self, objects):
        self.objects = objects
        self.bucket = "bkt"

    def clone_for_worker(self):
        return self

    def list_object_digests(self, prefix, cancel_event=None):
        return [(k, len(v), hashlib.md5(v).hexdigest(), None)
                for k, v in self.objects.items() if k.startswith(prefix)]


class PassTests(unittest.TestCase):

    def test_remote_tree_skips_keys_without_a_local_name(self):
        model = _DigestModel({"p/a": b"x", "p//b": b"y", "p/c\\d": b"z"})
        tree, skipped = remote_digest_tree(model, "p/")
        self.assertEqual(list(tree), ["a"])
        self.assertEqual(sorted(skipped), ["/b", "c\\d"])

    def test_a_first_pass_settles_identical_files_by_md5(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name, body in (("same", b"one"), ("diff", b"two")):
                with open(os.path.join(tmp, name), "wb") as handle:
                    handle.write(body)
            model = _DigestModel({"p/same": b"one", "p/diff": b"TWO"})
            result = plan_two_way_pass(model, tmp, "p/", os.path.join(tmp, "nostate"))
            plan = {a["rel"]: a["action"] for a in result["plan"]}
            self.assertEqual(plan, {"same": SKIP, "diff": CONFLICT})


class _WindowCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        settings = QSettings("s3duck-tests-2way", "s3duck-tests-2way")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            self.win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(self.win.close)
        self.win.TWO_WAY_STATE_DIR = os.path.join(self.tmp.name, "state")
        self.win.data_model.bucket = "bkt"
        self.local = os.path.join(self.tmp.name, "local")
        os.makedirs(self.local)

    def _wait(self):
        deadline = time.monotonic() + 5
        while self.win.__dict__.get("_two_way_threads") and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()


class WindowTwoWayTests(_WindowCase):

    def test_nothing_to_run_still_records_the_state(self):
        with open(os.path.join(self.local, "a"), "wb") as handle:
            handle.write(b"x")
        remote = _DigestModel({"p/a": b"x"})
        self.win.data_model.clone_for_worker = lambda: remote
        self.assertIsNone(self.win.start_two_way_sync(
            [{"action": SKIP, "rel": "a", "size": 1, "reason": ""}],
            self.local, "p/", {}))
        self._wait()
        state = load_state(self.win.two_way_state_file(self.local, "p/"))
        self.assertEqual(list(state), ["a"])

    def test_a_run_is_queued_and_records_when_it_ends(self):
        started = []
        with patch.object(main_window.MainWindow, "_start_transfer",
                          lambda _s, e: started.append(e)):
            entry = self.win.start_two_way_sync(
                [{"action": UPLOAD, "rel": "n", "size": 1, "reason": ""},
                 {"action": CONFLICT, "rel": "c", "size": 1, "reason": ""}],
                self.local, "p/", _state("c", etag="old"))
        self.assertIs(started[0], entry)
        self.assertEqual([row[1] for row in entry.job], ["n"],
                         "a conflict was queued")
        with open(os.path.join(self.local, "n"), "wb") as handle:
            handle.write(b"x")
        remote = _DigestModel({"p/n": b"x", "p/c": b"zz"})
        self.win.data_model.clone_for_worker = lambda: remote
        entry.on_finished(entry)
        self._wait()
        state = load_state(self.win.two_way_state_file(self.local, "p/"))
        self.assertEqual(state["c"]["remote"][1], "old", "the conflict lost its history")
        self.assertIn("n", state)

    def test_the_queue_calls_the_follow_up_once_the_job_ends(self):
        called = []

        class _Deletes:
            bucket = "bkt"

            def delete(self, key, **kw):
                pass

        with patch.object(type(self.win.data_model), "clone_for_worker",
                          lambda _s: _Deletes()):
            entry = self.win.assign_thread_operation("delete", ["x"], need_refresh=False)
            entry.on_finished = called.append
            deadline = time.monotonic() + 5
            while self.win._active_entry is not None and time.monotonic() < deadline:
                self._app.processEvents()
            self._app.processEvents()
        self.assertEqual(called, [entry])

    def test_a_failed_state_write_is_only_logged(self):
        self.win.data_model.clone_for_worker = lambda: _DigestModel({})
        logged = []
        self.win.log = logged.append
        self.win._record_two_way_state(os.path.join(self.tmp.name, "missing"),
                                       "p/", "bkt", "/x/y", set(), {})
        self._wait()
        self.assertTrue(any("could not record" in m for m in logged), logged)


class SyncDialogTwoWayTests(_WindowCase):

    def _dialog(self, model):
        dlg = SyncDialog(None, self.win, model, "p/")
        self.addCleanup(dlg.close)
        dlg._local.setText(self.local)
        dlg._both.setChecked(True)
        return dlg

    def _preview(self, dlg):
        dlg._preview()
        deadline = time.monotonic() + 5
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def test_both_ways_plans_resolves_and_runs(self):
        with open(os.path.join(self.local, "mine"), "wb") as handle:
            handle.write(b"local")
        with open(os.path.join(self.local, "clash"), "wb") as handle:
            handle.write(b"aaa")
        model = _DigestModel({"p/theirs": b"remote", "p/clash": b"bbb"})
        dlg = self._dialog(model)
        self.assertFalse(dlg._delete_extra.isEnabled())
        self.assertFalse(dlg._btn_keep_local.isHidden())
        self._preview(dlg)
        actions = {a["rel"]: a["action"] for a in dlg._actions}
        self.assertEqual(actions, {"mine": UPLOAD, "theirs": DOWNLOAD, "clash": CONFLICT})
        self.assertIn("1 conflict(s)", dlg._info.text())
        self.assertEqual(len(dlg.actionable()), 2)
        row = [a["rel"] for a in dlg._actions].index("clash")
        dlg._table.selectRow(row)
        self.assertEqual(dlg.resolve_selected("local"), 1)
        self.assertEqual(len(dlg.actionable()), 3)
        self.win.start_two_way_sync = Mock()
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            dlg._run()
        args = self.win.start_two_way_sync.call_args.args
        self.assertEqual(args[1:3], (self.local, "p/"))
        self.assertEqual({a["action"] for a in args[0]}, {UPLOAD, DOWNLOAD})

    def test_switching_back_to_one_way_restores_the_controls(self):
        dlg = self._dialog(_DigestModel({}))
        dlg._up.setChecked(True)
        self.assertTrue(dlg._delete_extra.isEnabled())
        self.assertTrue(dlg._btn_keep_local.isHidden())
        self.assertEqual(dlg.direction(), "upload")


if __name__ == "__main__":
    unittest.main()
