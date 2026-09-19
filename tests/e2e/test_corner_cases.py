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

from model import local_name_is_writable, safe_local_path
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
        try:
            self.put(f"longnames/{long_name}.txt", b"x")
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "?")
            print(f"\n    over-long component: REFUSED by the backend ({code})")
        self.put("longnames/ok.txt", b"fine")
        self.put("longnames/also-ok.txt", b"fine")
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
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

    def test_a_refused_class_leaves_the_object_intact(self):
        """Whatever the answer, the body must survive the attempt — the
        change is a server-side copy onto itself."""
        self.put("sc/keep.txt", b"original")
        try:
            self.model.change_storage_class("sc/keep.txt", "GLACIER")
        except botocore.exceptions.ClientError:
            pass
        self.assertEqual(self.body("sc/keep.txt"), b"original")
