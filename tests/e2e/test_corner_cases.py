"""Corner cases, asked of a real server.

Keys are arbitrary strings and local filesystems are not, which is where
most of the sharp edges live. Each test here either exercises an edge the
unit suite can only simulate, or records what this backend does with a shape
S3 permits.
"""

import os
import tempfile
import urllib.request

import botocore.exceptions

from model import DownloadIncomplete, local_name_is_writable, safe_local_path
from tests.e2e.harness import ServerCase


class AwkwardKeyTests(ServerCase):
    """Characters that break naive URL building, path joining or listing."""

    KEYS = [
        "plain.txt",
        "with space.txt",
        "with+plus.txt",
        "with#hash.txt",
        "with?query.txt",
        "with%percent.txt",
        "with&amp.txt",
        "with=equals.txt",
        "quote'single.txt",
        "unicode-Ünïcødé.txt",
        "emoji-🦆.txt",
        "cyrillic-Привет.txt",
        "semi;colon.txt",
        "at@sign.txt",
        "brack[et].txt",
        "paren(the).txt",
    ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.stored = []
        for key in cls.KEYS:
            try:
                cls.model.client.put_object(
                    Bucket=cls.bucket, Key=f"odd/{key}", Body=b"payload")
                cls.stored.append(f"odd/{key}")
            except botocore.exceptions.ClientError:
                pass

    def test_every_stored_key_comes_back_in_the_listing(self):
        listed = {name for name in (i.name for i in self.model.list("odd/"))}
        missing = [k for k in self.stored
                   if k.rsplit("/", 1)[-1] not in listed]
        self.assertEqual(missing, [], "keys the listing lost")

    def test_a_presigned_link_fetches_every_one_of_them(self):
        """'#' and '?' truncate a naively built URL at that character, and a
        '+' read as a space fetches the wrong object — or nothing."""
        for key in self.stored:
            with self.subTest(key=key):
                url = self.model.presigned_get_url(key, 300)
                with urllib.request.urlopen(url, timeout=15) as resp:
                    self.assertEqual(resp.read(), b"payload")

    def test_the_direct_url_percent_encodes_the_dangerous_characters(self):
        url = self.model.direct_object_url("odd/with#hash.txt")
        self.assertIn("%23", url)
        self.assertNotIn("#", url.split("://", 1)[1])
        url = self.model.direct_object_url("odd/with?query.txt")
        self.assertIn("%3F", url)
        self.assertNotIn("?", url)
        # '+' must not be left bare: a reader would decode it as a space.
        self.assertIn("%2B", self.model.direct_object_url("odd/with+plus.txt"))

    def test_they_survive_a_whole_prefix_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("odd/", None, tmp)
            landed = []
            for root, _dirs, files in os.walk(tmp):
                landed.extend(files)
            self.assertEqual(len(landed), len(self.stored))

    def test_a_key_can_be_deleted_by_its_awkward_name(self):
        key = "doomed/with#hash and space.txt"
        self.model.client.put_object(Bucket=self.bucket, Key=key, Body=b"x")
        self.model.delete(key)
        self.assertNotIn(key, self.keys("doomed/"))


class EmptyAndBoundaryTests(ServerCase):
    def test_a_zero_byte_object_round_trips_with_verification_on(self):
        """Every digest of nothing is the same digest; the verifier must not
        trip over that."""
        self.put("zero/empty.bin", b"")
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "empty.bin")
            self.model.verify_downloads = True
            try:
                self.model.download_file("zero/empty.bin", out, tmp)
            finally:
                self.model.verify_downloads = False
            self.assertEqual(os.path.getsize(out), 0)

    def test_a_zero_byte_file_uploads(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "zero.bin")
            open(src, "wb").close()
            self.model.upload_file(src, "zero/up.bin")
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="zero/up.bin")
        self.assertEqual(head["ContentLength"], 0)

    def test_a_file_exactly_at_the_multipart_threshold(self):
        """The >= vs > that decides managed-vs-resumable, against a server
        that will reject a malformed part."""
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        self.model.resumable_uploads = True
        for size in (5 * 1024 * 1024, 5 * 1024 * 1024 + 1,
                     5 * 1024 * 1024 - 1):
            with self.subTest(size=size):
                payload = os.urandom(size)
                with tempfile.TemporaryDirectory() as tmp:
                    src = os.path.join(tmp, "b.bin")
                    with open(src, "wb") as handle:
                        handle.write(payload)
                    key = f"boundary/{size}.bin"
                    self.model.upload_file(src, key)
                self.assertEqual(
                    self.model.client.head_object(
                        Bucket=self.bucket, Key=key)["ContentLength"], size)

    def test_a_ranged_download_of_an_exact_chunk_multiple(self):
        """No final short chunk — the loop's off-by-one lives here."""
        payload = os.urandom(4 * 1024 * 1024)
        self.model.client.put_object(
            Bucket=self.bucket, Key="boundary/multiple.bin", Body=payload)
        self.model.resume_threshold = 1024 * 1024
        self.model.resume_chunk_size = 2 * 1024 * 1024
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "m.bin")
            self.model.download_file("boundary/multiple.bin", out, tmp)
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)

    def test_a_file_key_that_prefixes_another_is_sized_alone(self):
        """'a.txt' is also a prefix of 'a.txt.bak'."""
        self.put("size/a.txt", b"1234567890")
        self.put("size/a.txt.bak", b"XXXXX")
        self.assertEqual(self.model.get_size("size/a.txt"), 10)


class LocalFilesystemLimitTests(ServerCase):
    """S3 allows a 1024-byte key with no per-component rule. Local
    filesystems cap ONE component at 255 bytes, so a legal key can name a
    file that cannot be created."""

    def test_the_rule_matches_what_the_filesystem_actually_does(self):
        """Not a guess about the limit — a measurement of it."""
        with tempfile.TemporaryDirectory() as tmp:
            for length, expected in ((200, True), (255, True), (256, False)):
                name = "n" * length
                self.assertEqual(local_name_is_writable(name), expected)
                try:
                    with open(os.path.join(tmp, name), "wb") as handle:
                        handle.write(b"x")
                    really_worked = True
                except OSError:
                    really_worked = False
                self.assertEqual(
                    really_worked, expected,
                    f"the {length}-byte rule disagrees with this filesystem")

    def test_an_over_long_key_is_skipped_not_fatal(self):
        """MinIO refuses such a key outright, so where it does the download
        simply has nothing to skip — the point is that the OTHER objects
        arrive either way, which is what used to fail."""
        long_name = "L" * 300
        stored = True
        try:
            self.put(f"longnames/{long_name}.txt", b"x")
        except botocore.exceptions.ClientError as exc:
            stored = False
            code = exc.response.get("Error", {}).get("Code", "?")
            print(f"\n    over-long component: REFUSED by the backend ({code})")
        self.put("longnames/ok.txt", b"fine")
        self.put("longnames/also-ok.txt", b"fine")
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
            if stored:
                # Skipped, and the job says so instead of ending "done".
                with self.assertRaises(DownloadIncomplete):
                    self.model.download_file("longnames/", None, tmp,
                                             log_fn=logs.append)
            else:
                self.model.download_file("longnames/", None, tmp,
                                         log_fn=logs.append)
            landed = []
            for root, _dirs, files in os.walk(tmp):
                landed.extend(files)
            self.assertIn("ok.txt", landed)
            self.assertIn("also-ok.txt", landed)

    def test_safe_local_path_refuses_it_for_the_other_callers(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(safe_local_path(tmp, "a/" + "L" * 300), "")
            self.assertTrue(safe_local_path(tmp, "a/" + "L" * 200))

    def test_a_file_and_a_folder_of_the_same_name(self):
        """AWS stores and LISTS "clash/logs" beside "clash/logs/2024.txt";
        locally one path cannot be both. A backend that refuses the pair, or
        (MinIO) stores both but lists only "clash/logs", leaves the download
        nothing to clash over -- recorded, and the rest asserted."""
        self.put("clash/ok.txt", b"fine")
        pair = ["clash/logs", "clash/logs/2024.txt"]
        try:
            self.put(pair[0], b"a file called logs")
            self.put(pair[1], b"inside the folder")
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "?")
            print(f"\n    file beside a same-named folder: REFUSED by the "
                  f"backend ({code})")
        listed = [k for k in self.keys("clash/") if k in pair]
        if listed != pair:
            print(f"\n    file beside a same-named folder: the listing shows "
                  f"{listed}")
        with tempfile.TemporaryDirectory() as tmp:
            if listed == pair:
                with self.assertRaises(DownloadIncomplete) as caught:
                    self.model.download_file("clash/", None, tmp)
                self.assertEqual(
                    [k for k, _why in caught.exception.failures],
                    ["clash/logs"])
                with open(os.path.join(tmp, "clash", "logs", "2024.txt"),
                          "rb") as handle:
                    self.assertEqual(handle.read(), b"inside the folder")
            else:
                self.model.download_file("clash/", None, tmp)
            with open(os.path.join(tmp, "clash", "ok.txt"), "rb") as handle:
                self.assertEqual(handle.read(), b"fine")

    def test_a_local_folder_in_the_way_costs_that_object_only(self):
        """What the plan cannot see: the managed transfer writes a temporary
        file and renames it over the destination, and a FOLDER at the
        destination fails that rename. It used to abort the whole folder;
        now it costs the one object, under both parallel settings."""
        for parallel in (1, 4):
            with self.subTest(parallel=parallel), \
                    tempfile.TemporaryDirectory() as tmp:
                prefix = f"inway{parallel}/"
                for name in ("a.txt", "b.txt", "c.txt", "d.txt"):
                    self.put(prefix + name, name.encode())
                os.makedirs(os.path.join(tmp, prefix, "b.txt"))
                before = self.model.parallel_files
                self.model.parallel_files = parallel
                try:
                    with self.assertRaises(DownloadIncomplete) as caught:
                        self.model.download_file(prefix, None, tmp)
                finally:
                    self.model.parallel_files = before
                self.assertEqual(
                    [k for k, _why in caught.exception.failures],
                    [prefix + "b.txt"])
                for name in ("a.txt", "c.txt", "d.txt"):
                    with open(os.path.join(tmp, prefix, name), "rb") as h:
                        self.assertEqual(h.read(), name.encode())


class OverwriteCheckTests(ServerCase):
    """What the Skip/Overwrite prompt is built from."""

    def test_a_missing_file_is_not_reported(self):
        self.put("ow/there.txt", b"x")
        self.assertEqual(
            self.model.existing_keys(["ow/there.txt", "ow/absent.txt"]),
            {"ow/there.txt"})

    def test_a_top_level_folder_is_settled_without_reading_the_bucket(self):
        """Its "parent prefix" is the bucket root, so this used to list
        every object in the bucket to answer one question."""
        self.put("toplevel/a.txt", b"x")
        for n in range(20):
            self.put(f"noise/{n}.txt", b"x")
        seen = []
        real = self.model.client.get_paginator

        class Spy:
            def __init__(self, paginator):
                self._paginator = paginator

            def paginate(self, **kwargs):
                seen.append(kwargs.get("Prefix"))
                return self._paginator.paginate(**kwargs)

        self.model.client.get_paginator = lambda name: Spy(real(name))
        try:
            found = self.model.existing_keys(["toplevel/"])
        finally:
            self.model.client.get_paginator = real
        self.assertEqual(found, {"toplevel/"})
        self.assertEqual(seen, [], "a folder target listed the bucket")

    def test_a_folder_of_placeholders_still_counts_as_occupied(self):
        self.model.client.put_object(
            Bucket=self.bucket, Key="markers/sub/", Body=b"")
        self.assertEqual(self.model.existing_keys(["markers/"]), {"markers/"})

    def test_an_absent_folder_leaves_the_prompt_alone(self):
        self.assertEqual(self.model.existing_keys(["never/"]), set())


class StorageClassSupportTests(ServerCase):
    """What this backend does with the storage classes the app offers.

    Deliberately model-level: the e2e runner holds boto3 and nothing else,
    so nothing here may import main_window (which needs PyQt6). The worker's
    per-object tolerance for a refusal is pinned in the unit suite; what
    only a server can answer is whether a refusal happens at all.
    """

    def test_each_offered_class_is_either_accepted_or_refused_clearly(self):
        self.put("sc/obj.txt", b"x")
        report = {}
        for storage_class in ("STANDARD", "STANDARD_IA", "GLACIER",
                              "DEEP_ARCHIVE", "INTELLIGENT_TIERING"):
            try:
                self.model.change_storage_class("sc/obj.txt", storage_class)
                report[storage_class] = "ok"
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "?")
                report[storage_class] = code
                # A refusal has to name itself; a bare 500 would leave the
                # user with nothing to act on.
                self.assertTrue(code and code != "?")
        print(f"\n    storage classes: {report}")
        self.assertIn("STANDARD", report)

    def test_setting_the_class_an_object_already_has_is_a_no_op(self):
        """S3 refuses a copy onto itself that changes nothing, on AWS and
        MinIO alike, so "normalise this folder to STANDARD" used to fail on
        every object that was already there."""
        self.put("sc/already.txt", b"x")
        self.assertEqual(
            self.model.current_storage_class("sc/already.txt"), "STANDARD")
        logs = []
        moved = self.model.change_storage_class(
            "sc/already.txt", "STANDARD", log_fn=logs.append)
        self.assertIs(moved, False)
        self.assertTrue(any("already" in line for line in logs))
        self.assertEqual(self.body("sc/already.txt"), b"x")

    def test_a_real_class_change_reports_that_it_moved(self):
        """REDUCED_REDUNDANCY is the one non-STANDARD class MinIO accepts,
        which makes it the only way to exercise the positive branch here."""
        self.put("sc/moving.txt", b"x")
        try:
            moved = self.model.change_storage_class(
                "sc/moving.txt", "REDUCED_REDUNDANCY")
        except botocore.exceptions.ClientError as exc:
            self.skipTest(
                "this backend has no second storage class: "
                f"{exc.response.get('Error', {}).get('Code')}")
        self.assertIs(moved, True)
        self.assertEqual(
            self.model.current_storage_class("sc/moving.txt"),
            "REDUCED_REDUNDANCY")
        self.assertIs(
            self.model.change_storage_class(
                "sc/moving.txt", "REDUCED_REDUNDANCY"),
            False, "the second attempt should be a no-op")

    def _second_class(self, key):
        """Put *key* in the one non-STANDARD class this backend accepts, or
        skip. MinIO takes REDUCED_REDUNDANCY and nothing else, which makes
        it the only way to watch a copy preserve a tier for real rather
        than watch it preserve STANDARD."""
        try:
            self.put(key, b"payload", StorageClass="REDUCED_REDUNDANCY")
        except botocore.exceptions.ClientError as exc:
            self.skipTest(
                "this backend has no second storage class: "
                f"{exc.response.get('Error', {}).get('Code')}")
        if self.model.current_storage_class(key) != "REDUCED_REDUNDANCY":
            self.skipTest("this backend does not report a second class back")
        return "REDUCED_REDUNDANCY"

    def test_a_copy_keeps_the_object_in_its_own_class(self):
        """REGRESSION: an unqualified CopyObject carries no class and the
        service creates the new object as STANDARD, so every copy, move and
        rename quietly re-tiered what it touched."""
        held = self._second_class("sc/kept-src.txt")
        self.model.copy_object("sc/kept-src.txt", "sc/kept-dst.txt",
                               storage_class=held)
        self.assertEqual(
            self.model.current_storage_class("sc/kept-dst.txt"), held)
        self.assertEqual(self.body("sc/kept-dst.txt"), b"payload")

    def test_a_copy_told_nothing_asks_the_source_rather_than_assuming(self):
        """The routes with no listing behind them -- the other pane, a job
        replayed from an old history -- must not fall back to STANDARD."""
        held = self._second_class("sc/asked-src.txt")
        self.model.copy_object("sc/asked-src.txt", "sc/asked-dst.txt")
        self.assertEqual(
            self.model.current_storage_class("sc/asked-dst.txt"), held)

    def test_this_backend_keeps_the_class_without_being_asked(self):
        """Why a run here cannot show the bug itself: MinIO's CopyObject
        carries the source's class over on its own, where S3 creates the
        new object as STANDARD unless told otherwise. So the end-to-end
        value of the three tests above is that naming the class is a
        request a real server accepts and honours -- the silent re-tiering
        is AWS's to demonstrate. Pinned, so a backend that starts behaving
        like S3 shows up as a changed fact rather than as a quietly weaker
        suite."""
        held = self._second_class("sc/control-src.txt")
        self.model.copy_object("sc/control-src.txt", "sc/control-dst.txt",
                               storage_class="STANDARD")
        self.assertEqual(
            self.model.current_storage_class("sc/control-dst.txt"), held)

    def test_a_folder_copy_keeps_the_class_of_every_object_under_it(self):
        """The recursive route reads one class per object out of its own
        listing, so a thousand-object move costs no extra request."""
        held = self._second_class("scmove/a.txt")
        self.put("scmove/plain.txt", b"x")
        self.model.copy_prefix("scmove/", "scmoved/")
        self.assertEqual(
            self.model.current_storage_class("scmoved/a.txt"), held)
        self.assertEqual(
            self.model.current_storage_class("scmoved/plain.txt"), "STANDARD")

    def test_a_refused_class_leaves_the_object_intact(self):
        """Whatever the answer, the body must survive the attempt — the
        change is a server-side copy onto itself."""
        self.put("sc/keep.txt", b"original")
        try:
            self.model.change_storage_class("sc/keep.txt", "GLACIER")
        except botocore.exceptions.ClientError:
            pass
        self.assertEqual(self.body("sc/keep.txt"), b"original")
