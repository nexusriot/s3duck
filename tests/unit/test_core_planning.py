"""core/: sync, compare, manifest, dedup, rename."""

import main_window
import os
import tempfile
import unittest
from main_window import (
    BULK_RENAME_FIND, BULK_RENAME_TEMPLATE, _build_upload_job_for_path,
    _dest_inside_source, build_exclude_matcher, build_paste_job,
    build_sync_plan, bulk_rename_plan, find_duplicate_groups,
    select_redundant_keys, summarize_duplicate_groups, summarize_sync_plan,
)
from tests.support import FakeS3Client, _dt, make_model


class RenameBuildingBlocksTests(unittest.TestCase):
    """In-place rename reuses server-side copy + delete; verify those calls."""

    def setUp(self):
        self.m = make_model(bucket="b")

    def test_file_rename_copies_then_deletes(self):
        c = FakeS3Client()
        self.m._client = c
        # what MainWindow.rename_selected drives for a file in prefix "a/"
        self.m.copy_object("a/old.txt", "a/new.txt")
        self.m.delete("a/old.txt")
        copies = c.calls_of("copy_object")
        self.assertEqual(copies[0]["CopySource"], {"Bucket": "b", "Key": "a/old.txt"})
        self.assertEqual(copies[0]["Key"], "a/new.txt")
        dels = c.calls_of("delete_object")
        self.assertEqual(dels, [{"Bucket": "b", "Key": "a/old.txt"}])
class DestInsideSourceTests(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(_dest_inside_source("a/f.txt", "a/f.txt", False))
        self.assertTrue(_dest_inside_source("a/", "a/", True))

    def test_folder_nesting(self):
        self.assertTrue(
            _dest_inside_source("photos/", "photos/2024/photos/", True))
        self.assertFalse(
            _dest_inside_source("photos/", "archive/photos/", True))
        self.assertFalse(
            _dest_inside_source("photos/", "photos-old/", True))

    def test_files_do_not_nest(self):
        # 'a/f.txt.bak' starts with 'a/f.txt' as a string, but a file is not
        # a prefix — copying next to it must stay allowed.
        self.assertFalse(_dest_inside_source("a/f.txt", "a/f.txt.bak", False))
class BuildUploadJobTests(unittest.TestCase):
    def test_directory_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "proj")
            os.makedirs(os.path.join(root, "sub"))
            with open(os.path.join(root, "a.txt"), "w") as f:
                f.write("x")
            with open(os.path.join(root, "sub", "b.txt"), "w") as f:
                f.write("y")
            job = _build_upload_job_for_path(root, "dest/")
            entries = dict(job)
            # placeholder entries for each directory level (local side None)
            self.assertIsNone(entries["dest/proj"])
            self.assertIsNone(entries["dest/proj/sub"])
            self.assertEqual(entries["dest/proj/a.txt"],
                             os.path.join(root, "a.txt"))
            self.assertEqual(entries["dest/proj/sub/b.txt"],
                             os.path.join(root, "sub", "b.txt"))
            self.assertEqual(len(job), 4)

    def test_single_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "f.bin")
            open(p, "wb").close()
            self.assertEqual(_build_upload_job_for_path(p, ""), [("f.bin", p)])
            self.assertEqual(_build_upload_job_for_path(p, "a/b/"),
                             [("a/b/f.bin", p)])

    def test_trailing_separator_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "d")
            os.makedirs(root)
            job = _build_upload_job_for_path(root + os.sep, "pre/")
            self.assertEqual(job, [("pre/d", None)])

    def test_empty_path_yields_nothing(self):
        self.assertEqual(_build_upload_job_for_path("", "dest/"), [])
class BulkRenamePlanTests(unittest.TestCase):
    def test_find_replace_only_changed_entries(self):
        items = [("IMG_1.jpg", False), ("IMG_2.jpg", False), ("notes.txt", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_FIND, find="IMG_", replace="photo-")
        self.assertEqual(plan, [("IMG_1.jpg", "photo-1.jpg"),
                                ("IMG_2.jpg", "photo-2.jpg")])
        self.assertEqual(problems, [])

    def test_find_replace_is_case_sensitive_by_default(self):
        items = [("img_1.jpg", False)]
        plan, _ = bulk_rename_plan(items, find="IMG_", replace="x")
        self.assertEqual(plan, [])
        plan, _ = bulk_rename_plan(
            items, find="IMG_", replace="x", case_sensitive=False)
        self.assertEqual(plan, [("img_1.jpg", "x1.jpg")])

    def test_regex_with_backreference(self):
        items = [("2024-report.pdf", False)]
        plan, problems = bulk_rename_plan(
            items, find=r"(\d{4})-(\w+)", replace=r"\2-\1", regex=True)
        self.assertEqual(plan, [("2024-report.pdf", "report-2024.pdf")])
        self.assertEqual(problems, [])

    def test_invalid_regex_is_reported_not_raised(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="([", replace="", regex=True)
        self.assertEqual(plan, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("Invalid regular expression", problems[0])

    def test_literal_replacement_keeps_backslashes(self):
        plan, _ = bulk_rename_plan(
            [("a b.txt", False)], find=" ", replace="\\", case_sensitive=False)
        self.assertEqual(plan, [("a b.txt", "a\\b.txt")])

    def test_template_numbering_and_extension(self):
        items = [("b.txt", False), ("a.log", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_TEMPLATE,
            template="file-{n}{ext}", start=7, padding=3)
        self.assertEqual(plan, [("b.txt", "file-007.txt"),
                                ("a.log", "file-008.log")])
        self.assertEqual(problems, [])

    def test_template_uses_whole_name_for_folders(self):
        plan, _ = bulk_rename_plan(
            [("v1.2", True)], mode=BULK_RENAME_TEMPLATE,
            template="{name}-old{ext}")
        # a dotted folder name must not be split into stem/extension
        self.assertEqual(plan, [("v1.2", "v1.2-old")])

    def test_unknown_placeholder_reported(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False)], mode=BULK_RENAME_TEMPLATE, template="{nope}")
        self.assertEqual(plan, [])
        self.assertIn("Unknown placeholder", problems[0])

    def test_duplicate_targets_are_flagged(self):
        items = [("a.txt", False), ("b.txt", False)]
        plan, problems = bulk_rename_plan(
            items, mode=BULK_RENAME_TEMPLATE, template="same.txt")
        self.assertEqual(len(plan), 2)
        self.assertEqual(len(problems), 1)
        self.assertIn("same.txt", problems[0])

    def test_slash_and_empty_results_rejected(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False), ("b.txt", False)],
            mode=BULK_RENAME_TEMPLATE, template="{name}")
        # "{name}" for b.txt -> "b" is fine; check slash/empty separately
        self.assertEqual(len(plan), 2)

        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="a.txt", replace="sub/a.txt")
        self.assertEqual(plan, [])
        self.assertIn("cannot contain '/'", problems[0])

        plan, problems = bulk_rename_plan(
            [("a.txt", False)], find="a.txt", replace="")
        self.assertEqual(plan, [])
        self.assertIn("empty", problems[0])

    def test_empty_find_is_a_no_op(self):
        self.assertEqual(
            bulk_rename_plan([("a.txt", False)], find="", replace="x"),
            ([], []),
        )

    def test_malformed_template_is_reported_not_raised(self):
        """REGRESSION (crash): str.format raises ValueError -- not just the
        KeyError/IndexError that were caught -- for a stray brace, an unknown
        conversion and a format code a string cannot take. The preview
        recomputes on every keystroke in the template box, so "{" as the
        first character of a new template reached a Qt slot as an unhandled
        ValueError, and PyQt6 aborts the process when one escapes a slot."""
        for template in ("{", "}", "{name!z}", "{name:d}", "{name:{}}"):
            with self.subTest(template=template):
                plan, problems = bulk_rename_plan(
                    [("a.txt", False)], mode=BULK_RENAME_TEMPLATE,
                    template=template)
                self.assertEqual(plan, [])
                self.assertEqual(len(problems), 1)
                self.assertTrue(problems[0])

    def test_a_valid_template_still_works_after_the_guard(self):
        plan, problems = bulk_rename_plan(
            [("a.txt", False)], mode=BULK_RENAME_TEMPLATE,
            template="{name}-{n}{ext}")
        self.assertEqual(plan, [("a.txt", "a-1.txt")])
        self.assertEqual(problems, [])
class BuildSyncPlanTests(unittest.TestCase):
    def test_upload_direction(self):
        local = {"new.txt": (10, 100.0), "same.txt": (5, 100.0),
                 "bigger.txt": (20, 100.0), "newer.txt": (5, 500.0)}
        remote = {"same.txt": (5, 100.0), "bigger.txt": (10, 100.0),
                  "newer.txt": (5, 100.0), "extra.txt": (7, 100.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        by_rel = {a["rel"]: a for a in actions}
        self.assertEqual(by_rel["new.txt"]["action"], "upload")
        self.assertEqual(by_rel["new.txt"]["reason"], "missing at destination")
        self.assertEqual(by_rel["bigger.txt"]["reason"], "size differs")
        self.assertEqual(by_rel["newer.txt"]["reason"], "source is newer")
        self.assertEqual(by_rel["same.txt"]["action"], "skip")
        # extras are kept unless explicitly asked for
        self.assertEqual(by_rel["extra.txt"]["action"], "skip")

    def test_delete_extra_opt_in(self):
        actions = build_sync_plan(
            {}, {"gone.txt": (1, 1.0)}, direction="upload", delete_extra=True)
        self.assertEqual(
            [(a["action"], a["rel"]) for a in actions],
            [("delete_remote", "gone.txt")],
        )

    def test_download_direction_mirrors(self):
        local = {"old.txt": (5, 100.0), "onlylocal.txt": (1, 1.0)}
        remote = {"old.txt": (5, 500.0), "fresh.txt": (9, 100.0)}
        actions = build_sync_plan(
            local, remote, direction="download", delete_extra=True)
        by_rel = {a["rel"]: a for a in actions}
        self.assertEqual(by_rel["fresh.txt"]["action"], "download")
        self.assertEqual(by_rel["old.txt"]["action"], "download")
        self.assertEqual(by_rel["old.txt"]["reason"], "source is newer")
        self.assertEqual(by_rel["onlylocal.txt"]["action"], "delete_local")

    def test_mtime_tolerance_absorbs_clock_granularity(self):
        local = {"a.txt": (5, 101.0)}
        remote = {"a.txt": (5, 100.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        self.assertEqual(actions[0]["action"], "skip")  # 1s < 2s tolerance

    def test_older_source_is_not_transferred(self):
        local = {"a.txt": (5, 100.0)}
        remote = {"a.txt": (5, 900.0)}
        actions = build_sync_plan(local, remote, direction="upload")
        self.assertEqual(actions[0]["action"], "skip")

    def test_bad_direction_rejected(self):
        with self.assertRaises(ValueError):
            build_sync_plan({}, {}, direction="sideways")

    def test_summary_counts_and_bytes(self):
        actions = build_sync_plan(
            {"a": (10, 5.0), "b": (20, 5.0)}, {}, direction="upload")
        summary = summarize_sync_plan(actions)
        self.assertEqual(summary["upload"], 2)
        self.assertEqual(summary["bytes"], 30)
class ExcludeMatcherTests(unittest.TestCase):
    def test_no_patterns_excludes_nothing(self):
        match = build_exclude_matcher([])
        self.assertFalse(match("anything/at/all.txt"))
        self.assertFalse(build_exclude_matcher(None)("x"))

    def test_glob_matches_at_any_depth(self):
        match = build_exclude_matcher(["*.tmp"])
        self.assertTrue(match("a.tmp"))
        self.assertTrue(match("deep/nested/b.tmp"))
        self.assertFalse(match("a.txt"))

    def test_directory_pattern_excludes_contents(self):
        match = build_exclude_matcher(["node_modules/"])
        self.assertTrue(match("node_modules/pkg/index.js"))
        self.assertTrue(match("app/node_modules/pkg/index.js"))
        self.assertFalse(match("src/app.js"))
        # a file merely starting with the same characters is kept
        self.assertFalse(match("node_modules_notes.txt"))

    def test_bare_directory_name_also_excludes_contents(self):
        match = build_exclude_matcher([".git"])
        self.assertTrue(match(".git/config"))
        self.assertTrue(match("sub/.git/HEAD"))

    def test_path_glob(self):
        match = build_exclude_matcher(["build/*.o"])
        self.assertTrue(match("build/main.o"))
        self.assertFalse(match("build/main.c"))

    def test_blank_patterns_ignored(self):
        match = build_exclude_matcher(["", "  ", "*.log"])
        self.assertTrue(match("x.log"))
        self.assertFalse(match("x.txt"))
class SyncPlanExcludeTests(unittest.TestCase):
    def test_excluded_paths_never_appear_in_the_plan(self):
        local = {"keep.txt": (1, 1.0), "skip.tmp": (1, 1.0),
                 "node_modules/a.js": (1, 1.0)}
        actions = build_sync_plan(
            local, {}, direction="upload",
            exclude=["*.tmp", "node_modules/"])
        self.assertEqual([a["rel"] for a in actions], ["keep.txt"])

    def test_excluded_extras_are_not_deleted(self):
        """The dangerous case: with delete_extra on, an excluded remote file
        must not be swept away just because it is absent locally."""
        actions = build_sync_plan(
            {}, {"secrets.env": (1, 1.0), "junk.txt": (1, 1.0)},
            direction="upload", delete_extra=True, exclude=["*.env"])
        self.assertEqual(
            [(a["action"], a["rel"]) for a in actions],
            [("delete_remote", "junk.txt")],
        )

    def test_callable_exclude_is_accepted(self):
        actions = build_sync_plan(
            {"a.txt": (1, 1.0), "b.txt": (1, 1.0)}, {},
            direction="upload", exclude=lambda rel: rel == "a.txt")
        self.assertEqual([a["rel"] for a in actions], ["b.txt"])
class FindDuplicateGroupsTests(unittest.TestCase):
    MD5_A = "a" * 32
    MD5_B = "b" * 32

    def test_same_size_and_etag_is_a_confirmed_group(self):
        groups = find_duplicate_groups([
            ("one.txt", 100, self.MD5_A, _dt(1)),
            ("two.txt", 100, self.MD5_A, _dt(2)),
            ("other.txt", 100, self.MD5_B, _dt(3)),
        ])
        self.assertEqual(len(groups), 1)
        group = groups[0]
        self.assertTrue(group["confirmed"])
        self.assertEqual([k for k, _m in group["members"]],
                         ["one.txt", "two.txt"])
        self.assertEqual(group["wasted"], 100)

    def test_distinct_plain_etags_are_never_grouped(self):
        """Two different MD5s prove the contents differ — grouping them would
        invite deleting a file that is not a duplicate."""
        groups = find_duplicate_groups([
            ("one.txt", 100, self.MD5_A, _dt(1)),
            ("two.txt", 100, self.MD5_B, _dt(2)),
        ])
        self.assertEqual(groups, [])

    def test_multipart_etags_that_match_are_confirmed(self):
        etag = self.MD5_A + "-4"
        groups = find_duplicate_groups([
            ("one.bin", 10 ** 8, etag, _dt(1)),
            ("two.bin", 10 ** 8, etag, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertTrue(groups[0]["confirmed"])

    def test_multipart_vs_single_part_is_unconfirmed(self):
        """Identical bytes uploaded with different part sizes yield different
        ETags, so same-size-different-ETag is a candidate, not a finding."""
        groups = find_duplicate_groups([
            ("one.bin", 500, self.MD5_A, _dt(1)),
            ("two.bin", 500, self.MD5_B + "-2", _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(groups[0]["etag"], "")

    def test_two_blank_etags_are_never_confirmed(self):
        """A backend that returns no ETag tells us nothing about the content;
        grouping such objects as confirmed would make them auto-selectable for
        deletion on nothing more than a matching size."""
        groups = find_duplicate_groups([
            ("a.bin", 500, "", _dt(1)),
            ("b.bin", 500, "", _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(groups[0]["etag"], "")
        self.assertEqual(select_redundant_keys(groups), set())

    def test_missing_etag_makes_a_size_match_unconfirmed(self):
        groups = find_duplicate_groups([
            ("one.bin", 500, "", _dt(1)),
            ("two.bin", 500, self.MD5_A, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["confirmed"])

    def test_confirmed_and_unconfirmed_coexist_at_one_size(self):
        groups = find_duplicate_groups([
            ("a1.bin", 500, self.MD5_A, _dt(1)),
            ("a2.bin", 500, self.MD5_A, _dt(2)),
            ("b.bin", 500, self.MD5_B + "-3", _dt(3)),
            ("c.bin", 500, "", _dt(4)),
        ])
        kinds = sorted(g["confirmed"] for g in groups)
        self.assertEqual(kinds, [False, True])
        unconfirmed = next(g for g in groups if not g["confirmed"])
        self.assertEqual([k for k, _m in unconfirmed["members"]],
                         ["b.bin", "c.bin"])

    def test_zero_byte_objects_are_excluded_by_default(self):
        self.assertEqual(find_duplicate_groups([
            ("a", 0, self.MD5_A, _dt(1)),
            ("b", 0, self.MD5_A, _dt(2)),
        ]), [])

    def test_min_size_filter(self):
        entries = [("a", 10, self.MD5_A, _dt(1)), ("b", 10, self.MD5_A, _dt(2))]
        self.assertEqual(len(find_duplicate_groups(entries, min_size=10)), 1)
        self.assertEqual(find_duplicate_groups(entries, min_size=11), [])

    def test_folder_placeholders_are_ignored(self):
        self.assertEqual(find_duplicate_groups([
            ("dir/", 100, self.MD5_A, _dt(1)),
            ("dir2/", 100, self.MD5_A, _dt(2)),
        ]), [])

    def test_groups_sorted_by_reclaimable_bytes(self):
        groups = find_duplicate_groups([
            ("s1", 10, self.MD5_A, _dt(1)),
            ("s2", 10, self.MD5_A, _dt(2)),
            ("big1", 900, self.MD5_B, _dt(1)),
            ("big2", 900, self.MD5_B, _dt(2)),
        ])
        self.assertEqual([g["size"] for g in groups], [900, 10])

    def test_three_copies_report_two_wasted(self):
        groups = find_duplicate_groups([
            ("a", 50, self.MD5_A, _dt(1)),
            ("b", 50, self.MD5_A, _dt(2)),
            ("c", 50, self.MD5_A, _dt(3)),
        ])
        self.assertEqual(groups[0]["count"], 3)
        self.assertEqual(groups[0]["wasted"], 100)

    def test_quoted_etags_are_normalized(self):
        groups = find_duplicate_groups([
            ("a", 10, f'"{self.MD5_A}"', _dt(1)),
            ("b", 10, self.MD5_A, _dt(2)),
        ])
        self.assertEqual(len(groups), 1)

    def test_empty_input(self):
        self.assertEqual(find_duplicate_groups([]), [])
        self.assertEqual(find_duplicate_groups(None), [])

    def test_summary_counts_only_confirmed_bytes(self):
        groups = find_duplicate_groups([
            ("a1", 100, self.MD5_A, _dt(1)),
            ("a2", 100, self.MD5_A, _dt(2)),
            ("u1", 700, "", _dt(1)),
            ("u2", 700, self.MD5_B, _dt(2)),
        ])
        summary = summarize_duplicate_groups(groups)
        self.assertEqual(summary["groups"], 2)
        self.assertEqual(summary["confirmed_groups"], 1)
        self.assertEqual(summary["redundant"], 2)
        # unconfirmed bytes are not promised as reclaimable
        self.assertEqual(summary["wasted"], 100)
class SelectRedundantKeysTests(unittest.TestCase):
    MD5 = "c" * 32

    def _groups(self):
        return find_duplicate_groups([
            ("old.txt", 10, self.MD5, _dt(1)),
            ("mid.txt", 10, self.MD5, _dt(2)),
            ("new.txt", 10, self.MD5, _dt(3)),
        ])

    def test_keep_newest_selects_the_rest(self):
        self.assertEqual(
            select_redundant_keys(self._groups(), keep="newest"),
            {"old.txt", "mid.txt"})

    def test_keep_oldest_selects_the_rest(self):
        self.assertEqual(
            select_redundant_keys(self._groups(), keep="oldest"),
            {"mid.txt", "new.txt"})

    def test_unconfirmed_groups_are_never_auto_selected(self):
        groups = find_duplicate_groups([
            ("a", 500, "", _dt(1)),
            ("b", 500, "d" * 32, _dt(2)),
        ])
        self.assertFalse(groups[0]["confirmed"])
        self.assertEqual(select_redundant_keys(groups), set())

    def test_undated_members_are_treated_as_oldest(self):
        groups = find_duplicate_groups([
            ("dated.txt", 10, self.MD5, _dt(5)),
            ("undated.txt", 10, self.MD5, None),
        ])
        self.assertEqual(
            select_redundant_keys(groups, keep="newest"), {"undated.txt"})

    def test_every_group_keeps_one_survivor(self):
        groups = find_duplicate_groups([
            ("g1a", 10, self.MD5, _dt(1)), ("g1b", 10, self.MD5, _dt(2)),
            ("g2a", 20, "e" * 32, _dt(1)), ("g2b", 20, "e" * 32, _dt(2)),
        ])
        selected = select_redundant_keys(groups)
        for group in groups:
            survivors = [k for k, _m in group["members"] if k not in selected]
            self.assertEqual(len(survivors), 1)

    def test_bad_keep_value_rejected(self):
        with self.assertRaises(ValueError):
            select_redundant_keys([], keep="sideways")
class BuildPasteJobTests(unittest.TestCase):
    def _clip(self, mode="copy", bucket="src", items=None):
        return {"mode": mode, "bucket": bucket,
                "items": items or [("a.txt", "from/a.txt", False),
                                   ("dir", "from/dir/", True)]}

    def test_same_bucket_paste_targets_the_current_prefix(self):
        job, skipped = build_paste_job(self._clip(), "src", "to/")
        self.assertEqual(job, [
            ("from/a.txt", "to/a.txt", False, None),
            ("from/dir/", "to/dir/", True, None),
        ])
        self.assertEqual(skipped, [])

    def test_cross_bucket_paste_carries_the_destination(self):
        job, _skipped = build_paste_job(self._clip(), "other", "to/")
        self.assertEqual({entry[3] for entry in job}, {"other"})

    def test_paste_into_the_same_place_is_skipped(self):
        job, skipped = build_paste_job(
            self._clip(items=[("a.txt", "to/a.txt", False)]), "src", "to/")
        self.assertEqual(job, [])
        self.assertEqual(skipped, ["a.txt"])

    def test_folder_cannot_be_pasted_into_itself(self):
        job, skipped = build_paste_job(
            self._clip(items=[("dir", "dir/", True)]), "src", "dir/sub/")
        self.assertEqual(job, [])
        self.assertEqual(skipped, ["dir"])

    def test_same_prefix_in_another_bucket_is_allowed(self):
        job, skipped = build_paste_job(
            self._clip(items=[("a.txt", "to/a.txt", False)]), "other", "to/")
        self.assertEqual(len(job), 1)
        self.assertEqual(skipped, [])

    def test_paste_at_bucket_root(self):
        job, _s = build_paste_job(
            self._clip(items=[("a.txt", "from/a.txt", False)]), "src", "")
        self.assertEqual(job[0][1], "a.txt")

    def test_empty_clipboard(self):
        self.assertEqual(build_paste_job(None, "b", "p/"), ([], []))
        self.assertEqual(build_paste_job({}, "b", "p/"), ([], []))
class ComparePlanTests(unittest.TestCase):
    """A direction-free diff: calling one side "the source" before the user
    has said so is how a compare turns into a sync nobody asked for."""

    def test_every_status_is_recognised(self):
        rows = main_window.build_compare_plan(
            {"same.txt": (10, 100), "diff.txt": (10, 100),
             "mine.txt": (5, 100)},
            {"same.txt": (10, 100), "diff.txt": (20, 100),
             "yours.txt": (5, 100)})
        by_rel = {row["rel"]: row["status"] for row in rows}
        self.assertEqual(by_rel, {
            "same.txt": "same", "diff.txt": "differs",
            "mine.txt": "only_left", "yours.txt": "only_right"})

    def test_a_close_mtime_is_still_the_same_file(self):
        rows = main_window.build_compare_plan(
            {"a": (10, 100.0)}, {"a": (10, 101.0)})
        self.assertEqual(rows[0]["status"], "same")

    def test_a_far_mtime_counts_as_a_difference(self):
        rows = main_window.build_compare_plan(
            {"a": (10, 100.0)}, {"a": (10, 900.0)})
        self.assertEqual(rows[0]["status"], "differs")

    def test_excludes_are_honoured(self):
        rows = main_window.build_compare_plan(
            {"keep.txt": (1, 1), "drop.tmp": (1, 1)}, {}, exclude="*.tmp")
        self.assertEqual([row["rel"] for row in rows], ["keep.txt"])

    def test_the_summary_counts_by_status(self):
        rows = main_window.build_compare_plan(
            {"a": (1, 1), "b": (1, 1)}, {"a": (1, 1)})
        self.assertEqual(main_window.summarize_compare_plan(rows),
                         {"only_left": 1, "only_right": 0, "differs": 0,
                          "same": 1})

    def test_sizes_are_reported_for_both_sides(self):
        rows = main_window.build_compare_plan({"a": (10, 1)}, {"a": (20, 1)})
        self.assertEqual((rows[0]["left_size"], rows[0]["right_size"]),
                         (10, 20))
class CompareJobTests(unittest.TestCase):
    """Which queue operation a compare turns into depends entirely on the
    pair of sides."""

    build = staticmethod(main_window.PaneCompareDialog.build_job)

    def test_local_to_remote_is_an_upload(self):
        job, method, kwargs = self.build(
            ("local", os.path.join(os.sep, "data")),
            ("remote", "bkt", "dst/"), ["a/one.txt"])
        self.assertEqual(method, "upload")
        self.assertEqual(job[0][0], "dst/a/one.txt")
        self.assertEqual(job[0][1],
                         os.path.join(os.sep, "data", "a", "one.txt"))
        self.assertEqual(kwargs, {})

    def test_remote_to_local_is_a_download(self):
        job, method, kwargs = self.build(
            ("remote", "bkt", "src/"),
            ("local", os.path.join(os.sep, "out")), ["a/one.txt"])
        self.assertEqual(method, "download")
        self.assertEqual(job[0][0], "src/a/one.txt")
        self.assertEqual(job[0][1],
                         os.path.join(os.sep, "out", "a", "one.txt"))
        self.assertFalse(kwargs["need_refresh"])

    def test_a_remote_key_may_not_escape_the_local_pane(self):
        """The pane pulls keys straight out of a bucket listing, so '..' in
        one would otherwise be written outside the folder the pane shows."""
        out = os.path.join(os.sep, "out")
        job, method, _kw = self.build(
            ("remote", "bkt", "src/"), ("local", out),
            ["../../escape.txt", "ok.txt"])
        self.assertEqual(method, "download")
        self.assertEqual([entry[0] for entry in job], ["src/ok.txt"])
        for entry in job:
            self.assertTrue(
                os.path.abspath(entry[1]).startswith(
                    os.path.abspath(out) + os.sep))

    def test_same_bucket_remote_to_remote_is_a_plain_copy(self):
        job, method, kwargs = self.build(
            ("remote", "bkt", "a/"), ("remote", "bkt", "b/"), ["one.txt"])
        self.assertEqual(method, "copy")
        self.assertEqual(job[0], ("a/one.txt", "b/one.txt", False, None))
        self.assertEqual(kwargs["source_bucket"], "")

    def test_cross_bucket_names_both_sides(self):
        job, method, kwargs = self.build(
            ("remote", "src", "a/"), ("remote", "dst", "b/"), ["one.txt"])
        self.assertEqual(job[0], ("a/one.txt", "b/one.txt", False, "dst"))
        self.assertEqual(kwargs["source_bucket"], "src")

    def test_local_to_local_is_not_this_apps_job(self):
        job, method, _kw = self.build(
            ("local", "/a"), ("local", "/b"), ["one.txt"])
        self.assertEqual((job, method), ([], ""))

    def test_no_paths_means_no_job(self):
        self.assertEqual(
            self.build(("local", "/a"), ("remote", "b", ""), []),
            ([], "", {}))

    def test_a_side_is_described_for_the_header(self):
        describe = main_window.PaneCompareDialog.describe
        self.assertEqual(describe(("local", "/data")), "/data")
        self.assertEqual(describe(("remote", "bkt", "p/")), "s3://bkt/p/")
class ExcludeMatcherStringTests(unittest.TestCase):
    """REGRESSION: the watch handed its exclude box's text straight to the
    matcher, which iterated it character by character — the lone "*" that
    produced then matched every path, so a watch mirrored nothing at all."""

    def test_a_raw_string_is_split_not_iterated(self):
        match = main_window.build_exclude_matcher("*.tmp")
        self.assertTrue(match("a/b.tmp"))
        self.assertFalse(match("a/b.txt"))

    def test_several_patterns_in_one_string(self):
        match = main_window.build_exclude_matcher("*.tmp, node_modules/ .git/")
        self.assertTrue(match("x.tmp"))
        self.assertTrue(match("node_modules/pkg/index.js"))
        self.assertTrue(match(".git/config"))
        self.assertFalse(match("src/main.py"))

    def test_an_empty_string_excludes_nothing(self):
        for value in ("", "   ", None, []):
            with self.subTest(value=value):
                match = main_window.build_exclude_matcher(value)
                self.assertFalse(match("anything.txt"))

    def test_a_list_still_works(self):
        match = main_window.build_exclude_matcher(["*.log"])
        self.assertTrue(match("deep/a.log"))

    def test_the_watch_actually_excludes_what_it_is_told(self):
        """The end of the same bug: build_sync_plan is what the watch calls."""
        plan = build_sync_plan(
            {"keep.txt": (1, 1), "drop.tmp": (1, 1)}, {},
            direction="upload", exclude="*.tmp")
        self.assertEqual([entry["rel"] for entry in plan], ["keep.txt"])
class PrefixTreeTests(unittest.TestCase):
    """The usage pie says what kind of files; this says which folder, which
    is what a bill is made of."""

    ENTRIES = [("a/b/one.txt", 10), ("a/b/two.txt", 20),
               ("a/c/three.txt", 5), ("top.txt", 1)]

    def test_sizes_roll_up_to_every_level(self):
        tree = main_window.build_prefix_tree(self.ENTRIES)
        self.assertEqual(tree["size"], 36)
        self.assertEqual(tree["count"], 4)
        self.assertEqual(tree["children"]["a"]["size"], 35)
        self.assertEqual(
            main_window.node_at_path(tree, ["a", "b"])["size"], 30)
        self.assertEqual(
            main_window.node_at_path(tree, ["a", "c"])["count"], 1)

    def test_folder_markers_are_not_objects(self):
        tree = main_window.build_prefix_tree([("a/", 0), ("a/x", 5)])
        self.assertEqual(tree["count"], 1)
        self.assertEqual(tree["size"], 5)

    def test_a_missing_path_is_empty_not_an_error(self):
        tree = main_window.build_prefix_tree(self.ENTRIES)
        self.assertEqual(main_window.node_at_path(tree, ["nope", "deep"]), {})

    def test_children_come_back_biggest_first(self):
        tree = main_window.build_prefix_tree(self.ENTRIES)
        rows = main_window.treemap_children(tree)
        self.assertEqual([name for name, _s, _g in rows], ["a", "top.txt"])

    def test_zero_sized_entries_are_dropped(self):
        tree = main_window.build_prefix_tree([("a", 0), ("b", 5)])
        self.assertEqual([n for n, _s, _g in main_window.treemap_children(tree)],
                         ["b"])

    def test_a_long_tail_is_folded_into_one_tile(self):
        """A thousand slivers is not a picture of anything."""
        tree = main_window.build_prefix_tree(
            [(f"f{i}.txt", 100 - i) for i in range(50)])
        rows = main_window.treemap_children(tree, limit=10)
        self.assertEqual(len(rows), 11)
        self.assertTrue(rows[-1][2])            # marked as a group
        self.assertIn("40 more", rows[-1][0])
        self.assertEqual(sum(size for _n, size, _g in rows), tree["size"])
class SquarifyTests(unittest.TestCase):
    """Area is the whole point: if it does not add up, the picture lies."""

    def test_the_rectangles_fill_the_box(self):
        rects = main_window.squarify([50, 25, 15, 10], 0, 0, 200, 100)
        self.assertEqual(len(rects), 4)
        area = sum(w * h for _x, _y, w, h in rects)
        self.assertAlmostEqual(area, 200 * 100, places=3)

    def test_area_is_proportional_to_value(self):
        rects = main_window.squarify([75, 25], 0, 0, 100, 100)
        first = rects[0][2] * rects[0][3]
        second = rects[1][2] * rects[1][3]
        self.assertAlmostEqual(first / second, 3.0, places=3)

    def test_every_rectangle_stays_inside_the_box(self):
        rects = main_window.squarify([5, 4, 3, 2, 1], 0, 0, 300, 120)
        for x, y, width, height in rects:
            self.assertGreaterEqual(round(x, 6), 0)
            self.assertGreaterEqual(round(y, 6), 0)
            self.assertLessEqual(round(x + width, 6), 300)
            self.assertLessEqual(round(y + height, 6), 120)

    def test_degenerate_input_produces_nothing(self):
        self.assertEqual(main_window.squarify([], 0, 0, 10, 10), [])
        self.assertEqual(main_window.squarify([0, 0], 0, 0, 10, 10), [])
        self.assertEqual(main_window.squarify([1], 0, 0, 0, 10), [])

    def test_a_single_value_takes_the_whole_box(self):
        rects = main_window.squarify([1], 0, 0, 40, 20)
        self.assertEqual(len(rects), 1)
        self.assertAlmostEqual(rects[0][2] * rects[0][3], 800, places=3)

    def test_many_values_do_not_recurse_into_a_stack_overflow(self):
        rects = main_window.squarify(list(range(1, 800)), 0, 0, 900, 600)
        self.assertEqual(len(rects), 799)
class ManifestTests(unittest.TestCase):
    """An integrity record nobody can afford to take is not taken, so this
    one costs exactly one listing."""

    ENTRIES = [
        ("logs/b.txt", 20, "etag-b", "2026-01-02"),
        ("logs/a.txt", 10, "etag-a", "2026-01-01"),
    ]

    def test_a_manifest_round_trips(self):
        text = main_window.build_manifest(
            self.ENTRIES, "bkt", "logs/", "2026-09-02T10:00:00Z")
        meta, records = main_window.parse_manifest(text)
        self.assertEqual(meta["bucket"], "bkt")
        self.assertEqual(meta["prefix"], "logs/")
        self.assertEqual(meta["taken"], "2026-09-02T10:00:00Z")
        self.assertEqual(records, {"logs/a.txt": (10, "etag-a"),
                                   "logs/b.txt": (20, "etag-b")})

    def test_entries_are_written_in_key_order(self):
        text = main_window.build_manifest(self.ENTRIES)
        body = [line for line in text.splitlines() if line.startswith("logs/")]
        self.assertEqual([line.split(",")[0] for line in body],
                         ["logs/a.txt", "logs/b.txt"])

    def test_a_foreign_csv_is_refused(self):
        """Verifying against an arbitrary CSV would report every object as
        changed."""
        with self.assertRaises(ValueError):
            main_window.parse_manifest("key,size\na,1\n")

    def test_a_truncated_manifest_is_refused(self):
        with self.assertRaises(ValueError):
            main_window.parse_manifest(main_window.MANIFEST_HEADER + "\n")

    def test_a_key_containing_a_newline_survives_the_round_trip(self):
        """csv quotes such a key correctly; splitting the text into lines
        before the reader saw it tore the record in half, and verification
        then reported the object as both missing AND added."""
        entries = [("odd\nkey.txt", 5, "etag-n", "2026-01-01"),
                   ("plain.txt", 6, "etag-p", "2026-01-02")]
        text = main_window.build_manifest(entries, "bkt", "", "stamp")
        _meta, records = main_window.parse_manifest(text)
        self.assertEqual(sorted(records),
                         sorted(["odd\nkey.txt", "plain.txt"]))
        self.assertEqual(records["odd\nkey.txt"], (5, "etag-n"))

    def test_a_key_starting_with_a_hash_is_not_eaten_as_metadata(self):
        """Only the leading comment block is metadata. Treating any later
        '#' line as a comment silently dropped such an object."""
        entries = [("#notes.txt", 7, "etag-h", "2026-01-01")]
        text = main_window.build_manifest(entries, "bkt", "", "stamp")
        meta, records = main_window.parse_manifest(text)
        self.assertEqual(records, {"#notes.txt": (7, "etag-h")})
        self.assertNotIn("notes.txt", meta)

    def test_a_comma_in_a_key_still_round_trips(self):
        entries = [("a,b.txt", 3, "etag-c", "2026-01-01")]
        _meta, records = main_window.parse_manifest(
            main_window.build_manifest(entries))
        self.assertEqual(records, {"a,b.txt": (3, "etag-c")})

    def test_unparseable_rows_are_skipped_not_fatal(self):
        text = (main_window.build_manifest(self.ENTRIES)
                + "broken,notanumber,x,y\n,,,\n")
        _meta, records = main_window.parse_manifest(text)
        self.assertEqual(len(records), 2)

    def test_an_unchanged_prefix_reports_nothing(self):
        stored = {"a": (1, "e1"), "b": (2, "e2")}
        result = main_window.compare_manifest(stored, dict(stored))
        self.assertEqual(result["same"], 2)
        self.assertEqual(result["missing"], [])
        self.assertEqual(result["added"], [])
        self.assertEqual(result["changed"], [])

    def test_every_kind_of_drift_is_named(self):
        stored = {"gone": (1, "e"), "same": (1, "e"),
                  "grew": (1, "e"), "rewritten": (1, "e1")}
        current = {"same": (1, "e"), "grew": (9, "e"),
                   "rewritten": (1, "e2"), "new": (1, "e")}
        result = main_window.compare_manifest(stored, current)
        self.assertEqual(result["missing"], ["gone"])
        self.assertEqual(result["added"], ["new"])
        self.assertEqual(result["same"], 1)
        reasons = dict(result["changed"])
        self.assertIn("size 1 → 9", reasons["grew"])
        self.assertIn("different content", reasons["rewritten"])

    def test_a_blank_etag_never_claims_a_change(self):
        """Some backends omit the ETag; absent is not different."""
        result = main_window.compare_manifest({"a": (1, "")}, {"a": (1, "e")})
        self.assertEqual(result["changed"], [])
        self.assertEqual(result["same"], 1)

    def test_an_empty_manifest_against_a_full_prefix(self):
        result = main_window.compare_manifest({}, {"a": (1, "e")})
        self.assertEqual(result["added"], ["a"])
