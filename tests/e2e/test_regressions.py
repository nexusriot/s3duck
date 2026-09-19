"""The fixes from the 2026-09-19 review, re-asked of a real server.

The unit tests pin these against a stub, which proves the code branches but
not the premise each fix rests on: that a key may contain '..', and that
DeleteObjects answers 200 with per-key refusals in the body. Both premises
are protocol claims, and a stub is exactly the wrong thing to ask.
"""

import datetime
import os
import tempfile

import botocore.exceptions

from model import DeleteRefused, safe_local_path
from tests.e2e.harness import ServerCase


class TraversalTests(ServerCase):
    """An object key is an arbitrary string, and the question a stub cannot
    answer is whether a *server* will store a hostile one.

    MinIO refuses them (XMinioInvalidResourceName: "Resource name contains
    bad components such as '..' or '.'"), which is a good default and also
    means the attack cannot be staged there. AWS S3 accepts them — they are
    legal keys — so the guard still has to exist and still has to be tested.
    These tests therefore record what THIS backend does and assert the
    app-side rule either way, rather than pretending every backend is the
    strict one.
    """

    HOSTILE = "docs/../../escape.txt"

    def _try_put_hostile(self):
        """Store the hostile key, or skip saying the backend refused it."""
        try:
            self.put(self.HOSTILE, b"pwned")
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "?")
            self.skipTest(
                f"this backend refuses '..' in a key ({code}); the traversal "
                "path cannot be staged here, but AWS S3 accepts such keys")

    def test_whether_this_backend_accepts_a_dotdot_key(self):
        """Not an assertion about every server — a recorded answer about
        this one, so a change of backend shows up as a changed result."""
        try:
            self.put(self.HOSTILE, b"pwned")
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "?")
            print(f"\n    '..' keys: REFUSED by the backend ({code})")
            return
        print("\n    '..' keys: ACCEPTED by the backend")
        self.assertIn(self.HOSTILE, self.keys())

    def test_a_whole_prefix_download_skips_an_escaping_key(self):
        self.put("docs/ok.txt", b"fine")
        self._try_put_hostile()
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("docs/", None, tmp, log_fn=logs.append)
            landed = []
            for root, _dirs, files in os.walk(tmp):
                landed.extend(os.path.join(root, f) for f in files)
            for path in landed:
                self.assertTrue(
                    os.path.abspath(path).startswith(
                        os.path.abspath(tmp) + os.sep))
            self.assertEqual([os.path.basename(p) for p in landed],
                             ["ok.txt"])
        self.assertTrue(any("unsafe" in line for line in logs))

    def test_a_single_object_download_refuses_to_leave_the_folder(self):
        """This one needs no hostile key on the server: the destination is
        what escapes, which is the backstop every caller leans on."""
        self.put("docs/ok.txt", b"fine")
        with tempfile.TemporaryDirectory() as tmp:
            outside = os.path.join(os.path.dirname(tmp), "escaped.txt")
            with self.assertRaises(ValueError):
                self.model.download_file("docs/ok.txt", outside, tmp)
            self.assertFalse(os.path.exists(outside))

    def test_the_safe_path_helper_refuses_the_hostile_shape(self):
        """What the sync planner and the panes call, on the key shape a
        permissive backend would hand back."""
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(safe_local_path(tmp, self.HOSTILE), "")
            self.assertTrue(safe_local_path(tmp, "docs/ok.txt"))


class DeleteRefusalTests(ServerCase):
    """DeleteObjects answers HTTP 200 and puts per-key refusals in the body.
    That is the premise the silent-success fix rests on."""

    OBJECT_LOCK = True

    def _locked_version(self, key):
        resp = self.model.client.put_object(
            Bucket=self.bucket, Key=key, Body=b"locked")
        version = resp["VersionId"]
        self.model.client.put_object_retention(
            Bucket=self.bucket, Key=key, VersionId=version,
            Retention={
                "Mode": "COMPLIANCE",
                "RetainUntilDate": datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(days=1),
            },
        )
        return version

    def test_a_refused_key_comes_back_in_errors_not_as_an_exception(self):
        """The shape of the answer is the whole point: botocore does NOT
        raise here, so code that ignores the body sees success."""
        key = "locked/a.txt"
        version = self._locked_version(key)
        resp = self.model.client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": key, "VersionId": version}],
                    "Quiet": True},
        )
        errors = resp.get("Errors") or []
        self.assertTrue(
            errors,
            "the server allowed a COMPLIANCE-locked version to be deleted; "
            "this backend cannot demonstrate the refusal path")
        self.assertEqual(errors[0]["Key"], key)

    def test_the_model_turns_that_answer_into_a_failure(self):
        key = "locked/b.txt"
        version = self._locked_version(key)
        resp = self.model.client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": key, "VersionId": version}],
                    "Quiet": True},
        )
        if not (resp.get("Errors") or []):
            self.skipTest("backend did not refuse the locked version")
        logs = []
        with self.assertRaises(DeleteRefused) as caught:
            self.model.raise_on_delete_errors(resp, log_fn=logs.append)
        self.assertIn(key, str(caught.exception))
        self.assertTrue(logs)

    def test_a_clean_delete_still_reports_no_errors(self):
        self.put("free/a.txt", b"x")
        resp = self.model.client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": "free/a.txt"}], "Quiet": True},
        )
        self.assertFalse(resp.get("Errors") or [])
        self.model.raise_on_delete_errors(resp)   # must not raise


class BucketRootTests(ServerCase):
    """prefix_of("") answers "" — "/" would have matched nothing."""

    def test_listing_the_bucket_root_sees_everything(self):
        self.put("a.txt", b"a")
        self.put("sub/b.txt", b"bb")
        keys = sorted(k for k, _s in self.model.get_keys(""))
        self.assertIn("a.txt", keys)
        self.assertIn("sub/b.txt", keys)

    def test_a_slash_prefix_really_does_match_nothing(self):
        """Why the old answer was wrong, stated as a fact about the server
        rather than an assumption about it."""
        self.put("a.txt", b"a")
        resp = self.model.client.list_objects_v2(Bucket=self.bucket, Prefix="/")
        self.assertEqual(resp.get("KeyCount", 0), 0)


class ListingCapTests(ServerCase):
    def test_a_prefix_of_exactly_the_cap_is_not_reported_truncated(self):
        for n in range(12):
            self.put(f"capped/{n:02d}.txt", b"x")
        items = self.model.list("capped/", max_items=12)
        self.assertEqual(len(items), 12)
        self.assertFalse(self.model.last_listing_truncated)

    def test_one_past_the_cap_is_reported_truncated(self):
        for n in range(13):
            self.put(f"over/{n:02d}.txt", b"x")
        items = self.model.list("over/", max_items=12)
        self.assertEqual(len(items), 12)
        self.assertTrue(self.model.last_listing_truncated)
