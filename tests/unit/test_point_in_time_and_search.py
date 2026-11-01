"""Point-in-time restore of a prefix, and search by tag and Content-Type."""

import main_window
import time
import types
import ui.context_menu as context_menu
import ui.window.point_in_time as window_pit
import unittest
from datetime import datetime, timedelta, timezone
from PyQt6.QtCore import QDateTime, QPoint, QSettings
from PyQt6.QtWidgets import QDialog, QMenu, QMessageBox
from core.point_in_time import (
    DELETE, RESTORE, SKIP, plan_point_in_time, restore_job,
    summarize_point_in_time,
)
from core.search import content_type_matches, parse_tag_filter, tags_match
from model import FSObjectType
from tests.support import (
    PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window, make_model,
)
from ui.dialogs.point_in_time import PointInTimeDialog
from ui.dialogs.search import SearchDialog
from unittest.mock import Mock, patch
from workers.transfer import Worker

T0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _v(key, vid, minutes, latest=False, marker=False, size=1):
    return {"key": key, "version_id": vid, "last_modified": T0 + timedelta(minutes=minutes),
            "is_latest": latest, "is_delete_marker": marker, "size": size}


class PlanTests(unittest.TestCase):

    VERSIONS = [
        _v("changed.txt", "c1", 0, size=10), _v("changed.txt", "c2", 30, latest=True),
        _v("same.txt", "s1", 0, latest=True),
        _v("new.txt", "n1", 30, latest=True),
        _v("gone.txt", "g1", 0, size=7), _v("gone.txt", "gm", 30, latest=True, marker=True),
        _v("was-gone.txt", "w1", 0), _v("was-gone.txt", "wm", 5, marker=True),
        _v("was-gone.txt", "w2", 30, latest=True),
        _v("never.txt", "x1", 20), _v("never.txt", "xm", 25, latest=True, marker=True),
    ]

    def _by_key(self, moment):
        return {a["key"]: a for a in plan_point_in_time(self.VERSIONS, moment)}

    def test_every_case(self):
        plan = self._by_key(T0 + timedelta(minutes=10))
        self.assertEqual((plan["changed.txt"]["action"], plan["changed.txt"]["version_id"]),
                         (RESTORE, "c1"))
        self.assertEqual(plan["changed.txt"]["reason"], "changed since")
        self.assertEqual(plan["same.txt"]["action"], SKIP)
        self.assertEqual((plan["new.txt"]["action"], plan["new.txt"]["reason"]),
                         (DELETE, "did not exist yet"))
        self.assertEqual((plan["gone.txt"]["action"], plan["gone.txt"]["version_id"],
                          plan["gone.txt"]["reason"]), (RESTORE, "g1", "deleted since"))
        self.assertEqual((plan["was-gone.txt"]["action"], plan["was-gone.txt"]["reason"]),
                         (DELETE, "deleted at that time"))
        self.assertEqual(plan["never.txt"]["action"], SKIP)

    def test_the_present_moment_changes_nothing(self):
        plan = self._by_key(T0 + timedelta(days=1))
        self.assertEqual({a["action"] for a in plan.values()}, {SKIP})

    def test_summary_and_job(self):
        actions = plan_point_in_time(self.VERSIONS, T0 + timedelta(minutes=10))
        summary = summarize_point_in_time(actions)
        self.assertEqual((summary[RESTORE], summary[DELETE], summary["bytes"]), (2, 2, 17))
        self.assertEqual(sorted(restore_job(actions)), [
            ("changed.txt", RESTORE, "c1"), ("gone.txt", RESTORE, "g1"),
            ("new.txt", DELETE, ""), ("was-gone.txt", DELETE, "")])

    def test_a_null_version_is_restorable(self):
        versions = [_v("k", "null", 0), _v("k", "v2", 10, latest=True)]
        self.assertEqual(restore_job(plan_point_in_time(versions, T0 + timedelta(minutes=5))),
                         [("k", RESTORE, "null")])


class _Pages:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **_kw):
        return self.pages


class _VersionClient:
    def __init__(self):
        self.calls = []

    def get_paginator(self, name):
        return _Pages([{"Versions": [
            {"Key": "a", "VersionId": "1", "IsLatest": True, "Size": 4,
             "LastModified": T0}],
            "DeleteMarkers": [{"Key": "b", "VersionId": "m", "IsLatest": True,
                               "LastModified": T0}]}])

    def delete_object(self, **kw):
        self.calls.append(("delete_object", kw))

    def copy_object(self, **kw):
        self.calls.append(("copy_object", kw))


class ModelAndWorkerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_prefix_versions_are_flattened(self):
        model = make_model(bucket="b")
        model._client = _VersionClient()
        rows = model.list_prefix_versions("")
        self.assertEqual([(r["key"], r["is_delete_marker"], r["size"]) for r in rows],
                         [("a", False, 4), ("b", True, 0)])
        with self.assertRaises(ValueError):
            make_model(bucket="").list_prefix_versions("")

    def test_mark_deleted_never_walks_a_prefix(self):
        model = make_model(bucket="b")
        model._client = _VersionClient()
        model.mark_deleted("folder/")
        self.assertEqual(model._client.calls,
                         [("delete_object", {"Bucket": "b", "Key": "folder/"})])
        with self.assertRaises(Exception):
            make_model(bucket="b", read_only=True).mark_deleted("x")

    def test_the_worker_restores_and_marks(self):
        calls = []
        source = types.SimpleNamespace(
            parallel_files=1,
            make_version_current=lambda k, v, **kw: calls.append(("restore", k, v)),
            mark_deleted=lambda k: calls.append(("delete", k)))
        worker = Worker(source, [("a", RESTORE, "v1"), ("dir/", DELETE, "")])
        done = []
        worker.item_done.connect(done.append)
        worker.restore_versions()
        self.assertEqual(calls, [("restore", "a", "v1"), ("delete", "dir/")])
        self.assertEqual(done, [0, 1])


class _PlanModel:
    bucket = "bkt"

    def __init__(self, versions=None, fail=None):
        self.versions = versions or []
        self.fail = fail

    def clone_for_worker(self):
        return self

    def list_prefix_versions(self, prefix, cancel_event=None):
        if self.fail:
            raise self.fail
        return self.versions


class DialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self, model, versioning="Enabled", read_only=False):
        mw = types.SimpleNamespace(is_read_only=lambda: read_only,
                                   assign_thread_operation=Mock())
        dlg = PointInTimeDialog(None, mw, model, "p/", versioning=versioning)
        self.addCleanup(dlg.close)
        dlg.moment.setDateTime(QDateTime.fromSecsSinceEpoch(
            int((T0 + timedelta(minutes=10)).timestamp())))
        return dlg, mw

    def _wait(self, dlg):
        deadline = time.monotonic() + 5
        while dlg._thread is not None and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.005)
        self._app.processEvents()

    def test_preview_then_restore(self):
        dlg, mw = self._dialog(_PlanModel(PlanTests.VERSIONS))
        self.assertEqual(dlg.versioning_note.text(), "")
        self.assertTrue(dlg.preview())
        self._wait(dlg)
        self.assertEqual(dlg.table.rowCount(), 4)
        self.assertIn("2 to restore", dlg.info.text())
        self.assertTrue(dlg.run_btn.isEnabled())
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes):
            self.assertTrue(dlg.run())
        method, job = mw.assign_thread_operation.call_args.args
        self.assertEqual(method, "restore_versions")
        self.assertEqual(len(job), 4)
        self.assertEqual(mw.assign_thread_operation.call_args.kwargs["source_bucket"], "bkt")

    def test_a_changed_moment_needs_a_new_preview(self):
        dlg, mw = self._dialog(_PlanModel(PlanTests.VERSIONS))
        dlg.preview()
        self._wait(dlg)
        dlg.moment.setDateTime(dlg.moment.dateTime().addSecs(60))
        self.assertFalse(dlg.run_btn.isEnabled())
        self.assertFalse(dlg.run())
        mw.assign_thread_operation.assert_not_called()

    def test_declining_and_read_only_and_errors(self):
        dlg, mw = self._dialog(_PlanModel(PlanTests.VERSIONS))
        dlg.preview()
        self._wait(dlg)
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            self.assertFalse(dlg.run())
        mw.assign_thread_operation.assert_not_called()
        ro, _ = self._dialog(_PlanModel(PlanTests.VERSIONS), read_only=True)
        ro.preview()
        self._wait(ro)
        self.assertFalse(ro.run_btn.isEnabled())
        bad, _ = self._dialog(_PlanModel(fail=RuntimeError("AccessDenied")))
        bad.preview()
        self._wait(bad)
        self.assertIn("AccessDenied", bad.info.text())
        off, _ = self._dialog(_PlanModel(), versioning="off")
        self.assertIn("not enabled", off.versioning_note.text())


class WindowTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self, read_only=False):
        settings = QSettings("s3duck-tests-pit", "s3duck-tests-pit")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings, read_only=read_only)
        self.addCleanup(win.close)
        win.data_model.bucket = "bkt"
        win.data_model.current_folder = "here/"
        win.data_model.clone_for_worker = lambda: win.data_model
        win.in_bucket_list_mode = lambda: False
        return win

    def test_the_opener_checks_versioning_and_targets_the_prefix(self):
        win = self._window()
        win.data_model.get_bucket_versioning_status = lambda: "Enabled"
        with patch.object(window_pit, "run_with_progress",
                          lambda _p, _t, fn: (fn(None), None)), \
                patch.object(window_pit, "PointInTimeDialog") as dialog:
            win.open_point_in_time()
            win.open_point_in_time("other/")
        self.assertEqual(dialog.call_args_list[0].args[3], "here/")
        self.assertEqual(dialog.call_args_list[1].args[3], "other/")
        self.assertEqual(dialog.call_args.kwargs["versioning"], "Enabled")

    def test_guards(self):
        win = self._window(read_only=True)
        with patch.object(window_pit, "PointInTimeDialog") as dialog:
            self.assertIsNone(win.open_point_in_time())
        dialog.assert_not_called()
        win = self._window()
        with patch.object(window_pit, "run_with_progress",
                          return_value=(None, RuntimeError("x"))), \
                patch.object(window_pit.QMessageBox, "warning"), \
                patch.object(window_pit, "PointInTimeDialog") as dialog:
            self.assertIsNone(win.open_point_in_time())
        dialog.assert_not_called()

    def test_menu_entries(self):
        win = self._window()
        menu = QMenu(win)
        builder = context_menu.MenuBuilder(win, menu)
        shown = []
        with patch.object(context_menu.MenuBuilder, "run",
                          lambda self_, pos: shown.append(
                              [a.text() for a in menu.actions()]) or True):
            context_menu._listing_menu(
                win, builder, [object()],
                types.SimpleNamespace(t=FSObjectType.FOLDER),
                "dir/", "here/dir/", "here/dir/", False, True, QPoint())
        self.assertIn("Restore folder to a point in time…", shown[0])
        self.assertTrue(win.actPointInTime.text().startswith("Restore to a point"))


class SearchHelperTests(unittest.TestCase):

    def test_tag_filter(self):
        self.assertEqual(parse_tag_filter("team=infra, archived; owner=Jane Doe"),
                         [("team", "infra"), ("archived", None), ("owner", "Jane Doe")])
        self.assertEqual(parse_tag_filter(""), [])
        with self.assertRaises(ValueError):
            parse_tag_filter("=x")
        tags = [{"Key": "team", "Value": "infra"}, {"Key": "archived", "Value": ""}]
        self.assertTrue(tags_match(tags, [("team", "infra"), ("archived", None)]))
        self.assertFalse(tags_match(tags, [("team", "web")]))
        self.assertFalse(tags_match(tags, [("missing", None)]))

    def test_content_type(self):
        self.assertTrue(content_type_matches("image/png", "image/"))
        self.assertTrue(content_type_matches("IMAGE/PNG", "image/*"))
        self.assertTrue(content_type_matches("text/csv; charset=utf-8", "text/csv"))
        self.assertFalse(content_type_matches("text/csvx", "text/csv"))
        self.assertFalse(content_type_matches("", "image/"))
        self.assertTrue(content_type_matches("anything", ""))


class _SearchClient:
    def __init__(self, objects):
        self.objects = objects
        self.heads = []
        self.taggings = []

    def get_paginator(self, _name):
        return _Pages([{"Contents": [{"Key": k, "Size": 1, "LastModified": T0}
                                     for k in self.objects]}])

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
        return {"ContentType": self.objects[Key][0]}

    def get_object_tagging(self, Bucket, Key):
        self.taggings.append(Key)
        return {"TagSet": [{"Key": k, "Value": v} for k, v in self.objects[Key][1].items()]}


class MetadataSearchTests(unittest.TestCase):

    OBJECTS = {
        "a.png": ("image/png", {"team": "infra"}),
        "b.png": ("image/png", {"team": "web"}),
        "c.csv": ("text/csv", {"team": "infra"}),
    }

    def _model(self):
        model = make_model(bucket="b")
        model._client = _SearchClient(dict(self.OBJECTS))
        return model

    def test_tags_and_type_narrow_the_results(self):
        model = self._model()
        hits = [k for k, _s, _m in model.search_keys("", "", tags="team=infra")]
        self.assertEqual(hits, ["a.png", "c.csv"])
        self.assertEqual(model._client.heads, [], "a tag-only search sent HEADs")
        hits = [k for k, _s, _m in model.search_keys("", "", tags="team=infra",
                                                     content_type="image/")]
        self.assertEqual(hits, ["a.png"])

    def test_cheap_filters_run_first(self):
        model = self._model()
        model.search_keys("", ".csv", content_type="text/")
        self.assertEqual(model._client.heads, ["c.csv"])

    def test_the_probe_budget_stops_the_walk(self):
        model = self._model()
        model.MAX_SEARCH_PROBES = 2
        hits = model.search_keys("", "", tags="team", content_type="image/")
        self.assertTrue(model.last_search_probe_limited)
        self.assertEqual([k for k, _s, _m in hits], ["a.png"])

    def test_no_metadata_filter_costs_nothing(self):
        model = self._model()
        model.search_keys("", "png")
        self.assertEqual((model._client.heads, model._client.taggings), ([], []))
        self.assertFalse(model.last_search_probe_limited)


class SearchDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dialog(self):
        mw = types.SimpleNamespace(load_saved_searches=lambda: [],
                                   store_saved_searches=lambda e: None)
        model = types.SimpleNamespace(bucket="b", clone_for_worker=lambda: None)
        dlg = SearchDialog(None, mw, model, "")
        self.addCleanup(dlg.close)
        return dlg

    def test_the_fields_reach_the_search_and_the_saved_form(self):
        dlg = self._dialog()
        dlg._tags.setText("team=infra")
        dlg._ctype.setText("image/")
        kwargs = dlg.filter_kwargs()
        self.assertEqual((kwargs["tags"], kwargs["content_type"]), ("team=infra", "image/"))
        self.assertTrue(dlg._has_filters(), "a metadata-only search was refused")
        config = dlg.search_config()
        other = self._dialog()
        other.apply_search_config(config)
        self.assertEqual((other._tags.text(), other._ctype.text()), ("team=infra", "image/"))

    def test_a_bad_tag_term_is_reported_before_searching(self):
        dlg = self._dialog()
        dlg._tags.setText("=oops")
        dlg._run_search()
        self.assertIn("tag name", dlg._info.text())
        self.assertIsNone(dlg._thread)

    def test_results_say_when_the_probe_budget_ran_out(self):
        dlg = self._dialog()
        dlg._on_results(([("a.png", 1, None)], True), None)
        self.assertIn("tag/type lookups", dlg._info.text())
        dlg._on_results(([("a.png", 1, None)], False), None)
        self.assertNotIn("lookups", dlg._info.text())


if __name__ == "__main__":
    unittest.main()
