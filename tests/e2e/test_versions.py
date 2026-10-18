"""Version operations, asked of a real server.

The existing suite enables versioning and counts what comes back. Everything
the Versions dialog actually does with that list -- fetch one version, make
an old one current again, delete a single version -- goes further than a
listing, and none of it was exercised anywhere.

Each one is a protocol claim. "Make current" is not an operation S3 has: it
is a self-copy with a VersionId, and whether that lands as a new current
version is the backend's answer, not the app's.
"""

import os
import tempfile

import botocore.exceptions

from tests.e2e.harness import ServerCase


class VersionOperationTests(ServerCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.model.set_bucket_versioning("Enabled")

    def _three_versions(self, key):
        """Write v1, v2, v3 and return their ids, oldest first."""
        for body in (b"v1", b"v2", b"v3"):
            self.model.client.put_object(
                Bucket=self.bucket, Key=key, Body=body)
        versions = [v for v in self.model.list_object_versions(key)
                    if not v["is_delete_marker"]]
        self.assertEqual(len(versions), 3, versions)
        # list_object_versions answers newest first.
        return [v["version_id"] for v in reversed(versions)]

    def test_exactly_one_version_is_the_latest(self):
        """The dialog bolds one row; two or none would both be wrong."""
        self._three_versions("ver/latest.txt")
        versions = self.model.list_object_versions("ver/latest.txt")
        latest = [v for v in versions if v["is_latest"]]
        self.assertEqual(len(latest), 1, versions)

    def test_a_specific_version_downloads_its_own_bytes(self):
        """Not the current ones -- which is the entire point of the row."""
        oldest, middle, newest = self._three_versions("ver/download.txt")
        with tempfile.TemporaryDirectory() as tmp:
            for version, expected in ((oldest, b"v1"), (middle, b"v2"),
                                      (newest, b"v3")):
                out = os.path.join(tmp, f"{expected.decode()}.bin")
                self.model.download_object_version(
                    "ver/download.txt", version, out)
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), expected)

    def test_making_an_old_version_current_changes_what_a_plain_get_returns(self):
        oldest, _middle, _newest = self._three_versions("ver/promote.txt")
        self.assertEqual(self.body("ver/promote.txt"), b"v3")
        self.model.make_version_current("ver/promote.txt", oldest)
        self.assertEqual(self.body("ver/promote.txt"), b"v1")

    def test_making_a_version_current_adds_one_rather_than_moving_it(self):
        """A self-copy, so the history grows -- nothing is rewritten. The
        dialog says "make current", and a user who expects the old rows to
        disappear would be surprised by the server, not by the app."""
        oldest, _m, _n = self._three_versions("ver/history.txt")
        before = len(self.model.list_object_versions("ver/history.txt"))
        self.model.make_version_current("ver/history.txt", oldest)
        after = self.model.list_object_versions("ver/history.txt")
        self.assertEqual(len(after), before + 1)
        self.assertIn(oldest, [v["version_id"] for v in after],
                      "the promoted version was consumed rather than copied")

    def test_deleting_one_version_leaves_the_others(self):
        oldest, middle, newest = self._three_versions("ver/prune.txt")
        self.model.delete_object_version("ver/prune.txt", middle)
        left = [v["version_id"]
                for v in self.model.list_object_versions("ver/prune.txt")]
        self.assertNotIn(middle, left)
        self.assertIn(oldest, left)
        self.assertIn(newest, left)

    def test_deleting_the_current_version_uncovers_the_one_beneath(self):
        """No delete marker: naming a version deletes that version."""
        _oldest, middle, newest = self._three_versions("ver/uncover.txt")
        self.model.delete_object_version("ver/uncover.txt", newest)
        self.assertEqual(self.body("ver/uncover.txt"), b"v2")
        self.assertIn(middle, [v["version_id"] for v in
                               self.model.list_object_versions("ver/uncover.txt")])

    def test_a_delete_marker_is_listed_as_one(self):
        """The dialog draws these differently, and cannot download one."""
        self._three_versions("ver/marked.txt")
        self.model.delete("ver/marked.txt")
        versions = self.model.list_object_versions("ver/marked.txt")
        markers = [v for v in versions if v["is_delete_marker"]]
        self.assertEqual(len(markers), 1, versions)
        self.assertTrue(markers[0]["is_latest"])

    def test_undelete_removes_the_marker_and_the_body_returns(self):
        self._three_versions("ver/undo.txt")
        self.model.delete("ver/undo.txt")
        with self.assertRaises(botocore.exceptions.ClientError):
            self.model.client.head_object(Bucket=self.bucket,
                                          Key="ver/undo.txt")
        self.assertEqual(self.model.undelete("ver/undo.txt"), 1)
        self.assertEqual(self.body("ver/undo.txt"), b"v3")

    def test_versions_are_refused_for_a_folder(self):
        """A prefix has no version history, and asking for one would quietly
        return the first object under it."""
        self.put("ver/folder/inside.txt", b"x")
        with self.assertRaises(ValueError):
            self.model.list_object_versions("ver/folder/")

    def test_suspending_versioning_is_reported_back(self):
        """The bucket settings dialog offers the switch both ways."""
        self.model.set_bucket_versioning("Suspended")
        try:
            self.assertEqual(self.model.get_bucket_versioning_status(),
                             "Suspended")
        finally:
            self.model.set_bucket_versioning("Enabled")
