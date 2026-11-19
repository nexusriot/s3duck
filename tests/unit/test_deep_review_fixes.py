"""Regressions for the findings of the deep review round.

Each class names the failure it pins: what the user would have seen, and why
the code before it let that happen.
"""

import io
import json
import os
import shutil
import stat
import tempfile
import unittest
from unittest.mock import patch

import s3duck
import s3duck_cli
from core import two_way
from core.bulk_rename import bulk_rename_plan
from core.search import build_search_matcher
from core.sync_plan import build_exclude_matcher, build_sync_plan
from core.point_in_time import plan_point_in_time
from core.objects import _object_already_there
from core.profile import profile_name_problem, safe_profile_name
from model import (
    DownloadIncomplete, Model, ObjectExists, plan_prefix_download,
)
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, QSettings
from PyQt6.QtGui import QImage
from PyQt6.QtWidgets import QDialogButtonBox, QMessageBox
from settings import SettingsWindow
from tests.support import (
    FakeS3Client, _MultipartClient, _PrefixDownloadClient, _client_error, _ensure_qapp,
    make_model,
)
from ui.dialogs.point_in_time import PointInTimeDialog
from ui.widgets.thumbnails import decode_raster, thumbnail_image
from utils import (
    Crypto, TempWorkspace, export_profile_bundle, instance_socket_name,
    is_transient_error, pid_is_alive, restrict_config_dir,
)


class OneLocalPathPerDownloadedKeyTests(unittest.TestCase):
    """REGRESSION (silent data loss): relpath normalises a key, so
    "p/a/b.txt", "p/a//b.txt" and "p/a/./b.txt" are three objects and one
    local file. The plan handed all three to the downloader: the last one
    written won, the job reported every object as downloaded, and on the
    ranged path two threads shared one part file."""

    def test_keys_that_normalise_to_one_path_claim_it_once(self):
        plan = plan_prefix_download("p/", "/out", [
            ("p/a//b.txt", 1), ("p/a/./b.txt", 1), ("p/a/b.txt", 1),
            ("p/x/../a/b.txt", 1), ("p/a/c.txt", 1)])
        paths = [path for _k, path, _s in plan.files]
        self.assertEqual(len(paths), len(set(paths)))
        self.assertEqual(sorted(k for k, _p, _s in plan.files),
                         ["p/a/b.txt", "p/a/c.txt"])

    def test_the_key_that_spells_the_path_wins_whatever_the_order(self):
        """"a//b" sorts before "a/b" in a listing; the object a user sees
        in the folder is the one that must arrive."""
        for order in ([("p/a//b.txt", 1), ("p/a/b.txt", 1)],
                      [("p/a/b.txt", 1), ("p/a//b.txt", 1)]):
            plan = plan_prefix_download("p/", "/out", order)
            self.assertEqual([k for k, _p, _s in plan.files], ["p/a/b.txt"])
            self.assertEqual(plan.unsafe, ["p/a//b.txt"])

    def test_the_loser_is_named_with_the_key_it_lost_to(self):
        plan = plan_prefix_download(
            "p/", "/out", [("p/a/b.txt", 1), ("p/a//b.txt", 1)])
        self.assertIn("p/a/b.txt", plan.reasons["p/a//b.txt"])
        self.assertIn("same local file", plan.reasons["p/a//b.txt"])

    def test_case_twins_collide_only_where_the_filesystem_folds_case(self):
        entries = [("p/Report.txt", 1), ("p/report.txt", 1)]
        with patch("s3.common.sys.platform", "darwin"):
            folded = plan_prefix_download("p/", "/out", entries)
        with patch("s3.common.sys.platform", "linux"):
            exact = plan_prefix_download("p/", "/out", entries)
        self.assertEqual(len(folded.files), 1)
        self.assertEqual(len(folded.unsafe), 1)
        self.assertEqual(len(exact.files), 2)

    def test_a_folder_download_writes_the_survivor_and_reports_the_other(self):
        bodies = {"p/a//b.txt": b"B" * 64, "p/a/b.txt": b"A" * 64}
        m = make_model(bucket="b")
        m.resume_threshold = 10 ** 9
        m._client = _PrefixDownloadClient(bodies)
        with tempfile.TemporaryDirectory() as tmp:
            # The job ends as incomplete, naming the key -- not as a success
            # that covered two objects with one file.
            with self.assertRaises(DownloadIncomplete) as caught:
                m.download_file("p/", None, tmp)
            self.assertIn("p/a//b.txt", str(caught.exception))
            with open(os.path.join(tmp, "p", "a", "b.txt"), "rb") as handle:
                self.assertEqual(handle.read(), b"A" * 64)
        fetched = [c[1]["args"][1] for c in m._client.calls
                   if c[0] == "download_file"]
        self.assertEqual(fetched, ["p/a/b.txt"])

    def test_a_key_relpath_cannot_place_is_refused_not_raised(self):
        """On Windows relpath raises for a key that reads as another drive;
        one such key aborted the whole folder and the overwrite scan."""
        real = os.path.relpath

        def relpath(path, start):
            if path.startswith("D:"):
                raise ValueError("path is on mount 'D:', start on mount 'C:'")
            return real(path, start)

        with patch("s3.common.os.path.relpath", relpath):
            plan = plan_prefix_download(
                "p/", "/out", [("D:/x.txt", 1), ("p/ok.txt", 1)])
        self.assertEqual([k for k, _p, _s in plan.files], ["p/ok.txt"])
        self.assertEqual(plan.unsafe, ["D:/x.txt"])


class ConflictIsNotExistsTests(unittest.TestCase):
    """REGRESSION (file never uploaded): every 409 on a conditional write
    was read as "the key is taken". A 409 is the service saying another
    request on that key was in flight -- repeat this one. The worker logged
    "skipped: it already exists" for a file that did not."""

    def test_only_a_412_means_the_key_is_taken(self):
        self.assertTrue(_object_already_there(
            _client_error("PreconditionFailed", 412)))
        self.assertTrue(_object_already_there(_client_error("", 412)))
        for code in ("ConditionalRequestConflict", "OperationAborted", ""):
            self.assertFalse(
                _object_already_there(_client_error(code, 409)), code)

    def test_a_conflict_is_a_failure_the_queue_retries(self):
        self.assertTrue(is_transient_error(
            _client_error("ConditionalRequestConflict", 409)))
        self.assertTrue(is_transient_error(
            _client_error("OperationAborted", 409)))

    def test_an_upload_refused_with_409_is_not_reported_as_skipped(self):
        class _Busy(FakeS3Client):
            def put_object(self, **kw):
                raise _client_error("OperationAborted", 409)

            def upload_file(self, *args, **kw):
                raise _client_error("OperationAborted", 409)

        m = make_model(bucket="b")
        m._client = _Busy()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.txt")
            with open(path, "wb") as handle:
                handle.write(b"data")
            with self.assertRaises(Exception) as caught:
                m.upload_file(path, "f.txt", if_none_match=True)
        self.assertNotIsInstance(caught.exception, ObjectExists)


class VerificationSurvivesAFailedHeadTests(unittest.TestCase):
    """REGRESSION (corruption passed as verified): with "verify downloads"
    on, a HEAD that failed on something transient left nothing to compare
    with, and the file was moved into place unchecked, without a word."""

    class _FlakyHead(FakeS3Client):
        def __init__(self, heads):
            super().__init__()
            self._heads = list(heads)

        def head_object(self, **_kwargs):
            answer = self._heads.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer

        def download_file(self, _bucket, _key, path, **_kwargs):
            with open(path, "wb") as handle:
                handle.write(b"CORRUPT")

    def _model(self, heads):
        m = make_model(bucket="b")
        m.verify_downloads = True
        m.resume_threshold = 10 ** 9
        m._client = self._FlakyHead(heads)
        return m

    def test_the_digest_is_asked_for_again_once_the_bytes_are_here(self):
        good_md5 = "0" * 32
        m = self._model([
            _client_error("SlowDown", 503, "HeadObject"),
            {"ContentLength": 7, "ETag": f'"{good_md5}"'}])
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.bin")
            with self.assertRaisesRegex(Exception, "Checksum mismatch"):
                m.download_file("k", out, tmp)
            self.assertEqual(os.listdir(tmp), [])

    def test_an_unverifiable_download_says_so(self):
        m = self._model([_client_error("SlowDown", 503, "HeadObject"),
                         _client_error("SlowDown", 503, "HeadObject")])
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.bin")
            m.download_file("k", out, tmp, log_fn=logs.append)
            self.assertTrue(os.path.exists(out))
        self.assertTrue(any("NOT verified" in line for line in logs), logs)


class ResumeKeepsToTheUploadsOwnOptionsTests(unittest.TestCase):
    """REGRESSION (object finished under the old settings): encryption,
    storage class, tags and the checksum algorithm are fixed when a
    multipart upload is created, and the resume record did not remember
    them. Switch SSE-KMS on after an interrupted upload, retry, and the
    object was completed unencrypted -- with parts sent under a checksum
    algorithm the upload was never created with."""

    PART = 1024

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.payload = os.urandom(self.PART * 3 + 17)
        self.path = os.path.join(self.dir, "big.bin")
        with open(self.path, "wb") as handle:
            handle.write(self.payload)

    def _model(self, client):
        model = make_model(bucket="bkt")
        model.upload_chunk_size = self.PART
        model.multipart_threshold_mb = 0
        model.transfer_concurrency = 1
        model.upload_state_dir = os.path.join(self.dir, "state")
        model._client = client
        return model

    def _interrupted(self):
        client = _MultipartClient(fail_on_part=2)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 1)
        return client, model

    def test_changed_options_start_a_new_upload_and_abort_the_old(self):
        client, model = self._interrupted()
        model.set_upload_options(
            storage_class="GLACIER", checksum_algorithm="SHA256")
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2)
        self.assertEqual(client.aborted, ["upload-1"])
        created = client.uploads["upload-2"]["kw"]
        self.assertEqual(created.get("StorageClass"), "GLACIER")
        self.assertEqual(created.get("ChecksumAlgorithm"), "SHA256")
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_unchanged_options_still_resume(self):
        client, model = self._interrupted()
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 1)
        self.assertEqual(client.aborted, [])
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_a_record_from_before_the_fingerprint_is_still_resumed(self):
        client, model = self._interrupted()
        state_dir = model.upload_state_dir
        (name,) = os.listdir(state_dir)
        record_path = os.path.join(state_dir, name)
        with open(record_path) as handle:
            record = json.load(handle)
        self.assertIn("options", record)
        del record["options"]
        with open(record_path, "w") as handle:
            json.dump(record, handle)
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 1)

    def test_the_record_never_holds_the_arguments_themselves(self):
        fingerprint = Model._upload_options_fingerprint(
            {"SSECustomerKey": "TOP-SECRET-KEY", "StorageClass": "GLACIER"})
        self.assertNotIn("TOP-SECRET-KEY", fingerprint)
        self.assertNotEqual(
            fingerprint, Model._upload_options_fingerprint(
                {"SSECustomerKey": "another", "StorageClass": "GLACIER"}))


class ExcludePatternsReachDirectoriesTests(unittest.TestCase):
    """REGRESSION (excluded files deleted): a pattern ending in "/" was a
    literal prefix and a glob was tried only on the whole path or the file
    name. "*.egg-info/", ".*/", "/build/" and a nested "cache*" excluded
    nothing -- so with delete on, files the user had excluded were removed
    as extras, by the window, the watch, two-way sync and the CLI alike."""

    def test_a_globbed_directory_pattern_excludes_what_is_under_it(self):
        match = build_exclude_matcher(["*.egg-info/", ".*/"])
        self.assertTrue(match("pkg.egg-info/PKG-INFO"))
        self.assertTrue(match("src/pkg.egg-info/PKG-INFO"))
        self.assertTrue(match(".cache/x/y"))
        self.assertTrue(match("a/.git/config"))
        self.assertFalse(match("src/main.py"))
        self.assertFalse(match("egg-info.txt"))

    def test_a_rooted_or_dotted_directory_pattern_is_understood(self):
        for pattern in ("/build/", "./build/", "build/"):
            match = build_exclude_matcher([pattern])
            self.assertTrue(match("build/out.o"), pattern)
            self.assertFalse(match("rebuild/out.o"), pattern)

    def test_a_directory_path_pattern_may_glob(self):
        match = build_exclude_matcher(["src/gen*/"])
        self.assertTrue(match("src/generated/a.py"))
        self.assertFalse(match("lib/generated/a.py"))
        self.assertFalse(match("src/main.py"))

    def test_a_bare_glob_matches_a_directory_at_any_depth(self):
        match = build_exclude_matcher(["cache*"])
        self.assertTrue(match("cache1/x"))
        self.assertTrue(match("a/cache1/x"))
        self.assertFalse(match("a/b/x"))

    def test_patterns_that_reduce_to_nothing_exclude_nothing(self):
        match = build_exclude_matcher(["/", "./", "//"])
        self.assertFalse(match("a/b"))

    def test_excluded_extras_survive_a_delete_in_both_directions(self):
        kept = {"pkg.egg-info/PKG": (1, 1), ".cache/c": (1, 1)}
        for direction, local, remote in (("download", kept, {}),
                                         ("upload", {}, kept)):
            plan = build_sync_plan(
                local, remote, direction=direction, delete_extra=True,
                exclude=["*.egg-info/", ".*/"])
            self.assertEqual(
                [a for a in plan if a["action"].startswith("delete")], [],
                direction)


class DownloadSyncLandsEachFileOnceTests(unittest.TestCase):
    """REGRESSION (a sync that never settles): "a/b" and "a//b" are two keys
    and one local file; both were planned as downloads onto it, every run."""

    REMOTE = {"a/b": (1, 100.0), "a//b": (2, 100.0)}

    def test_one_key_owns_the_local_file(self):
        plan = build_sync_plan({}, self.REMOTE, direction="download")
        self.assertEqual(
            [a["rel"] for a in plan if a["action"] == "download"], ["a/b"])
        (skipped,) = [a for a in plan if a["action"] == "skip"]
        self.assertEqual(skipped["rel"], "a//b")
        self.assertIn("a/b", skipped["reason"])

    def test_the_second_run_has_nothing_left_to_do(self):
        plan = build_sync_plan(
            {"a/b": (1, 100.0)}, self.REMOTE, direction="download",
            delete_extra=True)
        self.assertEqual({a["action"] for a in plan}, {"skip"})

    def test_an_upload_is_untouched(self):
        local = {"a/b": (1, 100.0), "c": (1, 100.0)}
        plan = build_sync_plan(local, {}, direction="upload")
        self.assertEqual(sorted(a["rel"] for a in plan
                                if a["action"] == "upload"), ["a/b", "c"])


class RegexInputCannotAbortTests(unittest.TestCase):
    """REGRESSION (process abort from a keystroke): re raises OverflowError
    and RecursionError as well as re.error, and the bulk-rename preview
    recompiles on every character typed. From a Qt slot that is an abort."""

    HOSTILE = ("a{99999999999}", "(" * 5000 + ")" * 5000)

    def test_bulk_rename_reports_it_as_an_invalid_expression(self):
        for pattern in self.HOSTILE:
            plan, problems = bulk_rename_plan(
                [("a.txt", False)], find=pattern, replace="x", regex=True)
            self.assertEqual(plan, [])
            self.assertTrue(problems and "Invalid regular expression"
                            in problems[0], pattern[:20])

    def test_search_reports_it_the_way_its_dialog_expects(self):
        for pattern in self.HOSTILE:
            with self.assertRaises(ValueError):
                build_search_matcher(pattern, regex=True)


class TwoWayStateEntriesTests(unittest.TestCase):
    """REGRESSION (a pair that could never sync again): one malformed entry
    in the state file raised out of the planner on every run."""

    def _load(self, paths):
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        path = os.path.join(folder, "state.json")
        with open(path, "w") as handle:
            json.dump({"meta": {}, "paths": paths}, handle)
        return two_way.load_state(path)

    def test_unreadable_entries_are_dropped_and_good_ones_kept(self):
        good = {"local": [3, 10.5], "remote": [3, "etag"]}
        state = self._load({
            "ok": good, "text": "x", "short": {"local": [1]},
            "words": {"local": ["a", "b"], "remote": [1, "e"]},
            "flag": {"local": [True, 1.0]}, "list": [1, 2]})
        self.assertEqual(state, {"ok": good})

    def test_a_dropped_entry_can_never_become_a_delete(self):
        state = self._load({"gone-here": "garbage", "both": {"local": [1]}})
        plan = two_way.plan_two_way(
            {"both": (1, 5.0)}, {"gone-here": (1, "e"), "both": (1, "e")},
            state)
        self.assertFalse(
            [a for a in plan if "delete" in a["action"].lower()], plan)


class CliSyncAndCopyTests(unittest.TestCase):
    """REGRESSIONS in s3duck-cli, the form of the app that runs unattended:
    a recursive download of a prefix that does not exist exited 0, and the
    download containment check was applied to uploads and remote deletes."""

    class _Model:
        def __init__(self, tree=None):
            self.tree = dict(tree or {})
            self.uploaded, self.deleted = [], []
            self.bucket = ""

        def enter_bucket(self, _bucket, **_kw):
            return None

        def list_tree(self, _prefix, **_kw):
            return dict(self.tree)

        def upload_file(self, path, key, **_kw):
            self.uploaded.append((path, key))

        def delete(self, key, **_kw):
            self.deleted.append(key)

        def download_file(self, *_a, **_kw):
            raise AssertionError("nothing to download in these tests")

    def _run(self, argv, model):
        out, err = io.StringIO(), io.StringIO()

        def factory(bucket):
            model.bucket = bucket
            return model

        code = s3duck_cli.main(argv, out=out, err=err, model_factory=factory)
        return code, out.getvalue(), err.getvalue()

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def test_a_recursive_download_of_nothing_is_a_failure(self):
        dst = os.path.join(self.dir, "out")
        code, _out, err = self._run(
            ["--endpoint", "http://x", "cp", "-r", "s3://b/typo", dst],
            self._Model())
        self.assertEqual(code, s3duck_cli.EXIT_FAILED)
        self.assertIn("nothing found under s3://b/typo/", err)

    @unittest.skipUnless(hasattr(os, "symlink"), "needs symlinks")
    def test_a_symlinked_source_file_is_uploaded_by_sync(self):
        src = os.path.join(self.dir, "src")
        os.makedirs(src)
        real = os.path.join(self.dir, "real.txt")
        with open(real, "w") as handle:
            handle.write("data")
        os.symlink(real, os.path.join(src, "link.txt"))
        model = self._Model()
        code, _out, err = self._run(
            ["--endpoint", "http://x", "sync", src, "s3://b/p/"], model)
        self.assertEqual(err, "")
        self.assertEqual(code, s3duck_cli.EXIT_OK)
        self.assertEqual([key for _p, key in model.uploaded], ["p/link.txt"])

    def test_a_remote_extra_with_an_unsafe_name_is_still_deleted(self):
        src = os.path.join(self.dir, "src")
        os.makedirs(src)
        model = self._Model({"../escape.txt": (1, 1.0)})
        code, _out, err = self._run(
            ["--endpoint", "http://x", "sync", "--delete", "--force",
             src, "s3://b/p/"], model)
        self.assertEqual(err, "")
        self.assertEqual(code, s3duck_cli.EXIT_OK)
        self.assertEqual(model.deleted, ["p/../escape.txt"])


class StreamedCopiesKeepTagsAndHeadersTests(unittest.TestCase):
    """REGRESSION (tags lost on a move): CopyObject carries an object's tag
    set and every content header; an upload starts with neither. A copy
    routed through a stream -- another region, another profile -- kept five
    fields and no tags, and a move then deleted the source."""

    class _Source:
        def __init__(self, resp, tags):
            self.resp, self.tags = resp, tags
            self.tag_reads = 0

        def get_object(self, **_kw):
            return dict(self.resp, Body=io.BytesIO(b"payload"))

        def get_object_tagging(self, **_kw):
            self.tag_reads += 1
            return {"TagSet": [{"Key": k, "Value": v}
                               for k, v in self.tags]}

    class _Destination:
        def __init__(self):
            self.extra = None

        def upload_fileobj(self, body, _bucket, _key, ExtraArgs=None, **_kw):
            body.read()
            self.extra = ExtraArgs or {}

    def _pair(self, resp, tags=()):
        src = make_model(bucket="src")
        src._client = self._Source(resp, list(tags))
        dst = make_model(bucket="dst")
        dst._client = self._Destination()
        return src, dst

    def test_a_cross_profile_copy_carries_the_tags(self):
        src, dst = self._pair(
            {"ContentLength": 7, "TagCount": 2},
            [("team", "infra"), ("note", "a b&c")])
        src.copy_to_model("k", dst, "k")
        self.assertEqual(dst._client.extra.get("Tagging"),
                         "team=infra&note=a%20b%26c")

    def test_an_untagged_object_costs_no_extra_request(self):
        src, dst = self._pair({"ContentLength": 7})
        src.copy_to_model("k", dst, "k")
        self.assertEqual(src._client.tag_reads, 0)
        self.assertNotIn("Tagging", dst._client.extra)

    def test_the_destinations_own_tag_rule_wins(self):
        src, dst = self._pair({"ContentLength": 7, "TagCount": 1},
                              [("team", "infra")])
        dst.upload_extra_args = {"Tagging": "owner=dst"}
        src.copy_to_model("k", dst, "k")
        self.assertEqual(dst._client.extra.get("Tagging"), "owner=dst")
        self.assertEqual(src._client.tag_reads, 0)

    def test_language_expiry_and_redirect_survive_the_stream(self):
        src, dst = self._pair({
            "ContentLength": 7, "ContentType": "text/html",
            "ContentLanguage": "de", "Expires": "Wed, 21 Oct 2037 07:28:00 GMT",
            "WebsiteRedirectLocation": "/new"})
        src.copy_to_model("k", dst, "k")
        for field, value in (("ContentLanguage", "de"),
                             ("WebsiteRedirectLocation", "/new"),
                             ("ContentType", "text/html")):
            self.assertEqual(dst._client.extra.get(field), value)
        self.assertIn("Expires", dst._client.extra)

    def test_the_cross_region_fallback_carries_them_too(self):
        src, _dst = self._pair(
            {"ContentLength": 7, "TagCount": 1, "ContentLanguage": "de"},
            [("team", "infra")])
        target = self._Destination()
        src._try_bind_bucket = lambda name: (target, "ep", "eu-west-1", False)
        src._stream_copy("k", "k2", "other-bucket")
        self.assertEqual(target.extra.get("Tagging"), "team=infra")
        self.assertEqual(target.extra.get("ContentLanguage"), "de")


class PointInTimeTieTests(unittest.TestCase):
    """REGRESSION (a deleted key brought back): LastModified has one-second
    resolution, and on a tie the stable sort kept listing order -- versions
    before delete markers -- so a key created and deleted within a second
    was "restored" to the moment it was already deleted at."""

    T0 = 1_700_000_000

    def _history(self):
        return [
            {"key": "k", "version_id": "v1", "is_latest": False,
             "is_delete_marker": False, "last_modified": self.T0, "size": 3},
            {"key": "k", "version_id": "m1", "is_latest": True,
             "is_delete_marker": True, "last_modified": self.T0, "size": 0},
        ]

    def test_the_plan_does_not_depend_on_the_order_of_the_listing(self):
        forward = plan_point_in_time(self._history(), self.T0)
        backward = plan_point_in_time(self._history()[::-1], self.T0)
        self.assertEqual(forward, backward)
        self.assertEqual([a["action"] for a in forward], ["skip"])


class BucketLoggingKeepsItsDocumentTests(unittest.TestCase):
    """REGRESSION (grants deleted by editing a prefix): the logging document
    was rebuilt from the dialog's two fields, dropping the TargetGrants and
    TargetObjectKeyFormat it already had."""

    class _Client(FakeS3Client):
        def __init__(self, stored):
            super().__init__()
            self.stored, self.written = stored, None

        def get_bucket_logging(self, **_kw):
            return dict(self.stored)

        def put_bucket_logging(self, **kw):
            self.written = kw["BucketLoggingStatus"]

    GRANTS = [{"Grantee": {"Type": "CanonicalUser", "ID": "abc"},
               "Permission": "READ"}]
    FORMAT = {"PartitionedPrefix": {"PartitionDateSource": "EventTime"}}

    def _model(self, stored):
        m = make_model(bucket="bkt")
        m._client = self._Client(stored)
        return m

    def test_changing_the_prefix_keeps_grants_and_key_format(self):
        m = self._model({"LoggingEnabled": {
            "TargetBucket": "logs", "TargetPrefix": "old/",
            "TargetGrants": self.GRANTS,
            "TargetObjectKeyFormat": self.FORMAT}})
        m.put_bucket_logging("logs", "new/")
        self.assertEqual(m._client.written, {"LoggingEnabled": {
            "TargetBucket": "logs", "TargetPrefix": "new/",
            "TargetGrants": self.GRANTS,
            "TargetObjectKeyFormat": self.FORMAT}})

    def test_turning_logging_on_from_nothing_writes_the_two_fields(self):
        m = self._model({})
        m.put_bucket_logging("logs", "p/")
        self.assertEqual(m._client.written, {"LoggingEnabled": {
            "TargetBucket": "logs", "TargetPrefix": "p/"}})

    def test_turning_it_off_still_writes_an_empty_status(self):
        m = self._model({"LoggingEnabled": {"TargetBucket": "logs"}})
        m.put_bucket_logging("")
        self.assertEqual(m._client.written, {})


class SuspendedVersioningIsNotSafeTests(unittest.TestCase):
    """REGRESSION (a promise that did not hold): the restore dialog says
    "Nothing is destroyed", and warned only when versioning was off. On a
    SUSPENDED bucket a restore or delete replaces the "null" version."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _note(self, versioning):
        dialog = PointInTimeDialog(
            None, None, make_model(bucket="bkt"), "p/", versioning)
        self.addCleanup(dialog.close)
        return dialog.versioning_note.text()

    def test_a_suspended_bucket_is_warned_about(self):
        self.assertIn("SUSPENDED", self._note("Suspended"))

    def test_an_enabled_bucket_is_not(self):
        self.assertEqual(self._note("Enabled"), "")

    def test_an_unversioned_bucket_keeps_its_own_warning(self):
        self.assertIn("not enabled", self._note(""))


class ImportedBundleCommandsTests(unittest.TestCase):
    """REGRESSION (command execution): a profile bundle is another person's
    file, and its credential_process was imported verbatim. Run then
    executed it unasked whenever the profile had no keys or claimed they had
    expired -- both of which the bundle's author also sets."""

    COMMAND = "/bin/sh -c 'id > /tmp/owned'"

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _import(self, entries, keep):
        win = s3duck.Profiles()
        win.settings = QSettings("s3duck-tests-import", "s3duck-tests-import")
        win.settings.clear()
        self.addCleanup(win.settings.clear)
        self.addCleanup(win.close)
        win.items = []
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        path = os.path.join(tmp, "bundle.json")
        with open(path, "wb") as handle:
            handle.write(export_profile_bundle(entries, "pw"))
        asked = []

        def confirm(commands):
            asked.append(list(commands))
            return keep

        crypto = Crypto(Crypto.generate_key())
        with patch.object(s3duck.QFileDialog, "getOpenFileName",
                          return_value=(path, "")), \
                patch.object(win, "_ask_passphrase", return_value="pw"), \
                patch.object(win, "_crypto_or_warn", return_value=crypto), \
                patch.object(win, "save_settings"), \
                patch.object(win, "_confirm_imported_commands", confirm), \
                patch.object(QMessageBox, "information"), \
                patch.object(QMessageBox, "warning"):
            win.onImport()
        return win, asked

    def _entry(self, name, command=""):
        return {"name": name, "url": "https://s3.example.com",
                "access_key": "", "secret_key": "",
                "credential_process": command}

    def test_a_declined_command_is_not_imported(self):
        win, asked = self._import([self._entry("prod", self.COMMAND)], False)
        self.assertEqual(asked, [[("prod", self.COMMAND)]])
        self.assertEqual([i.name for i in win.items], ["prod"])
        self.assertEqual(win.items[0].credential_process, "")

    def test_an_accepted_command_is_kept(self):
        win, _asked = self._import([self._entry("prod", self.COMMAND)], True)
        self.assertEqual(win.items[0].credential_process, self.COMMAND)

    def test_a_bundle_without_commands_asks_nothing(self):
        win, asked = self._import([self._entry("plain")], False)
        self.assertEqual(asked, [])
        self.assertEqual(len(win.items), 1)

    def test_the_question_defaults_to_no_and_shows_plain_text(self):
        win = s3duck.Profiles()
        self.addCleanup(win.close)
        seen = {}

        def fake_exec(box):
            seen["default"] = box.standardButton(box.defaultButton())
            seen["format"] = box.textFormat()
            seen["text"] = box.text()
            return QMessageBox.StandardButton.No

        with patch.object(QMessageBox, "exec", fake_exec):
            kept = win._confirm_imported_commands(
                [("prod", "<b>" + self.COMMAND)])
        self.assertFalse(kept)
        self.assertEqual(seen["default"], QMessageBox.StandardButton.No)
        self.assertEqual(seen["format"].name, "PlainText")
        self.assertIn(self.COMMAND, seen["text"])

    def test_an_imported_name_cannot_carry_a_settings_separator(self):
        win, _asked = self._import([self._entry("team/prod")], False)
        self.assertEqual([i.name for i in win.items], ["team_prod"])


class ProfileNameTests(unittest.TestCase):
    """REGRESSION (another profile's state erased): a name is a path
    segment in the settings file, so removing "pending_queue/team" removed
    "pending_queue/team/prod" with it."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_separators_are_refused_and_replaced(self):
        self.assertEqual(profile_name_problem("prod"), "")
        self.assertTrue(profile_name_problem("team/prod"))
        self.assertTrue(profile_name_problem("team\\prod"))
        self.assertTrue(profile_name_problem("  "))
        self.assertEqual(safe_profile_name(" team/prod\\x "), "team_prod_x")

    def _ok_enabled(self, dialog):
        return dialog.buttonBox.button(
            QDialogButtonBox.StandardButton.Ok).isEnabled()

    def _filled(self, name, **kwargs):
        dialog = SettingsWindow(**kwargs)
        self.addCleanup(dialog.close)
        dialog.urlLineEdit.setText("https://s3.example.com")
        dialog.accessKeyEdit.setText("AK")
        dialog.secretKeyEdit.setText("SK")
        dialog.nameLineEdit.setText(name)
        return dialog

    def test_the_dialog_refuses_a_new_name_with_a_separator(self):
        self.assertTrue(self._ok_enabled(self._filled("prod")))
        dialog = self._filled("team/prod")
        self.assertFalse(self._ok_enabled(dialog))
        self.assertIn("/", dialog.nameLineEdit.toolTip())

    def test_a_name_saved_before_the_rule_stays_editable(self):
        legacy = ("team/prod", "https://s3.example.com", "", "", "AK", "SK",
                  "false", "true", "", "false")
        dialog = SettingsWindow(settings=legacy)
        self.addCleanup(dialog.close)
        self.assertTrue(self._ok_enabled(dialog))


class TempSweepTests(unittest.TestCase):
    """REGRESSION (other people's folders deleted; startup crash): the
    sweep of a shared temp directory matched by prefix alone, and a number
    no PID can be raised out of the window's constructor."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def test_an_impossible_pid_counts_as_alive(self):
        self.assertTrue(pid_is_alive(99999999999999999999))
        self.assertTrue(pid_is_alive("99999999999999999999"))

    def test_a_planted_name_does_not_stop_the_sweep(self):
        os.makedirs(os.path.join(self.root, "s3duck_99999999999999999999_x"))
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(), [])

    def test_only_the_exact_session_shape_is_reclaimed(self):
        victims = ("s3duck_2024_backup", "s3duck_4242_abc",
                   "s3duck_4242_ABCD1234", "s3duck_4242_abcd1234_more")
        for name in victims:
            os.makedirs(os.path.join(self.root, name))
        ours = os.path.join(self.root, "s3duck_4242_abcd1234")
        os.makedirs(ours)
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [ours])
        for name in victims:
            self.assertTrue(os.path.isdir(os.path.join(self.root, name)), name)

    def test_what_mkdtemp_really_creates_is_still_reclaimed(self):
        dead = TempWorkspace(root=self.root, pid=4242)
        leftover = dead.session_dir
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [leftover])

    @unittest.skipUnless(hasattr(os, "symlink"), "needs symlinks")
    def test_a_link_with_our_name_is_not_followed(self):
        target = os.path.join(self.root, "precious")
        os.makedirs(target)
        with open(os.path.join(target, "file"), "w") as handle:
            handle.write("keep")
        os.symlink(target, os.path.join(self.root, "s3duck_4242_abcd1234"))
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])
        self.assertTrue(os.path.exists(os.path.join(target, "file")))


@unittest.skipUnless(os.name == "posix", "POSIX file modes")
class PrivateConfigDirTests(unittest.TestCase):
    """REGRESSION (credentials readable by every local user): the settings
    file -- the credential key beside the ciphertext, by default -- and the
    log were created with the umask, 0664 inside 0775 on a stock desktop."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _mode(self, path):
        return stat.S_IMODE(os.lstat(path).st_mode)

    def test_an_open_directory_is_closed(self):
        path = os.path.join(self.root, "s3duck")
        os.makedirs(path)
        os.chmod(path, 0o777)
        self.assertTrue(restrict_config_dir(path))
        self.assertEqual(self._mode(path), 0o700)

    def test_a_missing_directory_is_created_private(self):
        path = os.path.join(self.root, "fresh", "s3duck")
        self.assertTrue(restrict_config_dir(path))
        self.assertEqual(self._mode(path), 0o700)

    def test_a_link_is_left_alone(self):
        target = os.path.join(self.root, "elsewhere")
        os.makedirs(target)
        os.chmod(target, 0o755)
        link = os.path.join(self.root, "s3duck")
        os.symlink(target, link)
        self.assertFalse(restrict_config_dir(link))
        self.assertEqual(self._mode(target), 0o755)


@unittest.skipUnless(os.name == "posix", "XDG runtime directory")
class HandoverSocketTests(unittest.TestCase):
    """REGRESSION (locations sent to whoever listens first): the hand-over
    socket was a predictable name in the shared temp directory."""

    def test_the_socket_lives_in_the_private_runtime_directory(self):
        runtime = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, runtime, ignore_errors=True)
        with patch.dict(os.environ, {"XDG_RUNTIME_DIR": runtime}):
            name = instance_socket_name("s3duck-test")
        self.assertEqual(os.path.dirname(name), runtime)

    def test_without_one_the_bare_name_still_works(self):
        with patch.dict(os.environ, {"XDG_RUNTIME_DIR": ""}):
            name = instance_socket_name("s3duck-test")
        self.assertEqual(os.path.basename(name), name)
        with patch.dict(os.environ, {"XDG_RUNTIME_DIR": "/nonexistent-dir"}):
            self.assertEqual(instance_socket_name("s3duck-test"), name)


class RasterOnlyDecodeTests(unittest.TestCase):
    """REGRESSION (UI freeze from a listing): Qt chooses an image decoder
    from the bytes, so an SVG uploaded as "photo.png" reached the SVG
    renderer through the preview and the thumbnail grid. A two-kilobyte
    document of nested <use> elements takes minutes to draw."""

    SVG = (b'<svg xmlns="http://www.w3.org/2000/svg" width="8" height="8">'
           b'<rect width="8" height="8"/></svg>')

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _png(self):
        image = QImage(8, 8, QImage.Format.Format_RGB32)
        image.fill(0xFF3366)
        payload = QByteArray()
        buffer = QBuffer(payload)
        buffer.open(QIODevice.OpenModeFlag.WriteOnly)
        image.save(buffer, "PNG")
        return bytes(payload)

    def test_a_real_raster_still_decodes(self):
        self.assertFalse(decode_raster(self._png()).isNull())
        self.assertIsNotNone(thumbnail_image(self._png()))

    def test_an_svg_is_never_rendered_whatever_it_is_called(self):
        self.assertTrue(decode_raster(self.SVG).isNull())
        self.assertIsNone(thumbnail_image(self.SVG))

    def test_junk_and_nothing_are_not_images(self):
        self.assertTrue(decode_raster(b"not an image").isNull())
        self.assertTrue(decode_raster(b"").isNull())
        self.assertTrue(decode_raster(None).isNull())


if __name__ == "__main__":
    unittest.main()
