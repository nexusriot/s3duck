"""Finding things, and sizing them, against a real listing.

Deep search, the sync planner's tree, the duplicate finder's fingerprints and
the usage dialog's totals all read a whole bucket and draw a conclusion. The
unit suite feeds each of them a hand-written list, which checks the
conclusion but not the reading -- and the reading is where pagination, folder
placeholders and the ETag's shape live.
"""

import os
import tempfile

from core.dedup import find_duplicate_groups, summarize_duplicate_groups
from core.sync_plan import build_sync_plan
from tests.e2e.harness import ServerCase, make_model, purge_bucket, unique_bucket


class DeepSearchTests(ServerCase):
    """S3 filters by prefix and nothing else, so every other filter is
    applied client-side over the full listing."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.model.client.put_object(
            Bucket=cls.bucket, Key="find/logs/app.log", Body=b"a" * 100)
        cls.model.client.put_object(
            Bucket=cls.bucket, Key="find/logs/old.log", Body=b"b" * 5000)
        cls.model.client.put_object(
            Bucket=cls.bucket, Key="find/data/report.csv", Body=b"x,y\n1,2\n")
        cls.model.client.put_object(
            Bucket=cls.bucket, Key="find/data/notes.txt", Body=b"hello")

    def _keys(self, *args, **kwargs):
        return sorted(key for key, _size, _modified
                      in self.model.search_keys(*args, **kwargs))

    def test_a_substring_matches_across_folders(self):
        self.assertEqual(self._keys("find/", "log"),
                         ["find/logs/app.log", "find/logs/old.log"])

    def test_a_prefix_scopes_the_search(self):
        self.assertEqual(self._keys("find/data/", ""),
                         ["find/data/notes.txt", "find/data/report.csv"])

    def test_an_extension_filter_matches_the_suffix_not_the_name(self):
        self.assertEqual(self._keys("find/", "", extensions=["csv"]),
                         ["find/data/report.csv"])

    def test_a_size_floor_is_applied_against_the_real_sizes(self):
        self.assertEqual(self._keys("find/", "", min_size=1000),
                         ["find/logs/old.log"])

    def test_a_size_ceiling_is_applied_too(self):
        self.assertNotIn("find/logs/old.log",
                         self._keys("find/", "", max_size=200))

    def test_a_regular_expression_is_anchored_the_way_the_user_wrote_it(self):
        self.assertEqual(
            self._keys("find/", r"^find/logs/.*\.log$", regex=True),
            ["find/logs/app.log", "find/logs/old.log"])

    def test_a_search_that_matches_nothing_returns_nothing(self):
        self.assertEqual(self._keys("find/", "no-such-thing"), [])

    def test_the_result_limit_is_honoured(self):
        hits = self.model.search_keys("find/", "", max_results=2)
        self.assertEqual(len(hits), 2)


class SyncTreeTests(ServerCase):
    """list_tree is what the sync and compare planners are fed."""

    def test_the_tree_is_keyed_relative_to_the_prefix(self):
        self.put("tree/a.txt", b"one")
        self.put("tree/sub/b.txt", b"two")
        tree = self.model.list_tree("tree/")
        self.assertEqual(sorted(tree), ["a.txt", "sub/b.txt"])

    def test_each_entry_carries_the_size_the_planner_compares(self):
        self.put("sized/a.txt", b"12345")
        tree = self.model.list_tree("sized/")
        size, modified = tree["a.txt"]
        self.assertEqual(size, 5)
        self.assertGreater(modified, 0)

    def test_a_folder_placeholder_is_not_a_file_in_the_tree(self):
        """An empty-key marker would otherwise show up as a zero-byte file
        the planner wants to download."""
        self.model.create_folder("holder/")
        self.put("holder/real.txt", b"x")
        self.assertEqual(sorted(self.model.list_tree("holder/")), ["real.txt"])

    def test_a_plan_built_from_a_real_listing_moves_what_is_missing(self):
        self.put("plan/here.txt", b"same")
        self.put("plan/only-remote.txt", b"x")
        remote = self.model.list_tree("plan/")
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "here.txt"), "wb") as handle:
                handle.write(b"same")
            with open(os.path.join(tmp, "only-local.txt"), "wb") as handle:
                handle.write(b"y")
            local = {
                name: (os.path.getsize(os.path.join(tmp, name)),
                       os.path.getmtime(os.path.join(tmp, name)))
                for name in sorted(os.listdir(tmp))
            }
        actions = build_sync_plan(local, remote, direction="upload",
                                  delete_extra=True)
        by_rel = {a["rel"]: a["action"] for a in actions}
        self.assertEqual(by_rel.get("only-local.txt"), "upload")
        self.assertEqual(by_rel.get("only-remote.txt"), "delete_remote")
        # Present on both sides and identical: listed, so the dry-run shows
        # it, but as "skip" rather than as work.
        self.assertEqual(by_rel.get("here.txt"), "skip")


class FingerprintTests(ServerCase):
    """What the duplicate finder settles a group on."""

    def test_two_identical_single_part_objects_fingerprint_the_same(self):
        self.put("fp/one.bin", b"identical bytes")
        self.put("fp/two.bin", b"identical bytes")
        self.assertEqual(self.model.object_fingerprint("fp/one.bin"),
                         self.model.object_fingerprint("fp/two.bin"))

    def test_two_different_objects_compare_as_different(self):
        self.put("fp/left.bin", b"left")
        self.put("fp/right.bin", b"right")
        verdict = self.model.compare_fingerprints([
            self.model.object_fingerprint("fp/left.bin"),
            self.model.object_fingerprint("fp/right.bin"),
        ])
        self.assertEqual(verdict, "different")

    def test_two_identical_objects_compare_as_the_same(self):
        self.put("fp/same-a.bin", b"shared")
        self.put("fp/same-b.bin", b"shared")
        verdict = self.model.compare_fingerprints([
            self.model.object_fingerprint("fp/same-a.bin"),
            self.model.object_fingerprint("fp/same-b.bin"),
        ])
        self.assertEqual(verdict, "same")

    def test_the_finder_groups_duplicates_out_of_a_real_listing(self):
        """Through list_object_digests, which is what the dialog calls.

        The ETags come from the server here rather than from a fixture, and
        whether a group forms at all is decided by their shape.
        """
        self.put("dup/a.bin", b"twinned")
        self.put("dup/b.bin", b"twinned")
        self.put("dup/c.bin", b"alone-and-longer")
        rows = self.model.list_object_digests("dup/")
        groups = find_duplicate_groups(rows)
        self.assertEqual(len(groups), 1, groups)
        members = sorted(key for key, _modified in groups[0]["members"])
        self.assertEqual(members, ["dup/a.bin", "dup/b.bin"])
        self.assertTrue(groups[0]["confirmed"],
                        "a single-part ETag should settle this outright")
        summary = summarize_duplicate_groups(groups)
        self.assertEqual(summary["groups"], 1)
        self.assertEqual(summary["redundant"], 1)

    def test_an_object_on_its_own_forms_no_group(self):
        self.put("solo/only.bin", b"unique content here")
        self.assertEqual(
            find_duplicate_groups(self.model.list_object_digests("solo/")), [])

    def test_zero_byte_objects_are_not_reported_as_duplicates(self):
        """Every empty file matches every other, which is noise."""
        self.put("empties/a.bin", b"")
        self.put("empties/b.bin", b"")
        self.assertEqual(
            find_duplicate_groups(self.model.list_object_digests("empties/")),
            [])


class UsageTotalsTests(ServerCase):
    """What the usage dialog adds up."""

    def test_every_object_is_counted_once_with_its_size_and_class(self):
        self.put("use/a.txt", b"12345")
        self.put("use/b.txt", b"123")
        rows = {key: (size, storage)
                for key, size, storage in self.model.get_keys_for_bucket(
                    self.bucket)}
        self.assertEqual(rows["use/a.txt"][0], 5)
        self.assertEqual(rows["use/b.txt"][0], 3)
        self.assertTrue(rows["use/a.txt"][1])

    def test_the_totals_cover_more_than_one_page_of_listing(self):
        """Pagination is the thing a fixture cannot get wrong."""
        name = unique_bucket("paged")
        model = make_model(bucket=name)
        model.client.create_bucket(Bucket=name)
        self.addCleanup(purge_bucket, model.client, name)
        model.enter_bucket(name)
        for index in range(1100):            # over the 1000-key page size
            model.client.put_object(Bucket=name, Key=f"p/{index:05}.txt",
                                    Body=b"z")
        rows = model.get_keys_for_bucket(name)
        self.assertEqual(len(rows), 1100)
        self.assertEqual(sum(size for _k, size, _c in rows), 1100)


class StreamAndPreviewTests(ServerCase):
    """The zip writer streams; the preview pane reads a window."""

    def test_streaming_yields_the_whole_body(self):
        payload = os.urandom(300 * 1024)
        self.put("stream/blob.bin", payload)
        self.assertEqual(
            b"".join(self.model.stream_object("stream/blob.bin")), payload)

    def test_streaming_a_zero_byte_object_yields_nothing_and_does_not_hang(self):
        self.put("stream/empty.bin", b"")
        self.assertEqual(b"".join(self.model.stream_object("stream/empty.bin")),
                         b"")

    def test_a_small_object_previews_whole(self):
        self.put("prev/small.txt", b"hello there")
        preview = self.model.get_object_preview("prev/small.txt")
        self.assertEqual(preview["data"], b"hello there")
        self.assertFalse(preview["truncated"])

    def test_a_large_object_previews_a_window_and_says_so(self):
        """The dialog shows "truncated" on this, and the flag is the
        server's Content-Range talking, not a guess from the size."""
        self.put("prev/large.txt", b"x" * 40000)
        preview = self.model.get_object_preview("prev/large.txt",
                                                max_bytes=1024)
        self.assertEqual(len(preview["data"]), 1024)
        self.assertTrue(preview["truncated"])
        self.assertEqual(preview["size"], 40000)

    def test_an_empty_object_previews_as_empty_rather_than_erroring(self):
        """A byte range cannot be satisfied on a zero-byte object, so the
        ranged read is refused and the app retries unranged."""
        self.put("prev/empty.txt", b"")
        preview = self.model.get_object_preview("prev/empty.txt")
        self.assertEqual(preview["data"], b"")
        self.assertFalse(preview["truncated"])


class ContentTypeAndTagTests(ServerCase):
    """Two selection-wide operations whose semantics are server-side."""

    def test_a_mis_typed_object_is_restamped_from_its_extension(self):
        self.put("fix/data.json", b'{"a":1}',
                 ContentType="application/octet-stream")
        changed, content_type = self.model.retype_object("fix/data.json")
        self.assertTrue(changed)
        self.assertEqual(content_type, "application/json")
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="fix/data.json")
        self.assertEqual(head["ContentType"], "application/json")

    def test_restamping_an_already_correct_object_reports_no_change(self):
        """The bulk operation says how many it touched, and a re-run that
        claimed to rewrite everything would be a lie -- and a copy per
        object that someone pays for."""
        self.put("fix/right.json", b'{"a":1}', ContentType="application/json")
        changed, _ct = self.model.retype_object("fix/right.json")
        self.assertFalse(changed)

    def test_restamping_preserves_the_body(self):
        self.put("fix/body.json", b'{"keep":"me"}',
                 ContentType="application/octet-stream")
        self.model.retype_object("fix/body.json")
        self.assertEqual(self.body("fix/body.json"), b'{"keep":"me"}')

    def test_tags_can_be_added_without_losing_the_existing_ones(self):
        self.put("tag/add.txt", b"x")
        self.model.put_object_tags("tag/add.txt", [{"Key": "a", "Value": "1"}])
        self.model.update_object_tags("tag/add.txt", add={"b": "2"})
        tags = {t["Key"]: t["Value"]
                for t in self.model.get_object_tags("tag/add.txt")}
        self.assertEqual(tags, {"a": "1", "b": "2"})

    def test_tags_can_be_removed_by_key(self):
        self.put("tag/remove.txt", b"x")
        self.model.put_object_tags(
            "tag/remove.txt",
            [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}])
        self.model.update_object_tags("tag/remove.txt", remove=["a"])
        tags = {t["Key"]: t["Value"]
                for t in self.model.get_object_tags("tag/remove.txt")}
        self.assertEqual(tags, {"b": "2"})

    def test_replace_drops_everything_that_was_there(self):
        self.put("tag/replace.txt", b"x")
        self.model.put_object_tags(
            "tag/replace.txt",
            [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}])
        self.model.update_object_tags("tag/replace.txt", add={"z": "9"},
                                      replace=True)
        tags = {t["Key"]: t["Value"]
                for t in self.model.get_object_tags("tag/replace.txt")}
        self.assertEqual(tags, {"z": "9"})
