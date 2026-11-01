"""Point-in-time restore and search by tag / Content-Type, on a real server.

The restore plan is pure bookkeeping, but it is bookkeeping over what the
server reports: version ids, IsLatest, delete markers and LastModified at
the server's own clock. Only a real versioned bucket shows that a prefix
put back by the plan really reads as it did at the chosen moment.
"""

import time
from datetime import datetime, timezone

from core.point_in_time import DELETE, RESTORE, plan_point_in_time, restore_job
from tests.e2e.harness import ServerCase


class PointInTimeTests(ServerCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.model.client.put_bucket_versioning(
            Bucket=cls.bucket, VersioningConfiguration={"Status": "Enabled"})

    def _body(self, key):
        return self.model.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def _exists(self, key):
        return key in self.keys(key.rsplit("/", 1)[0] + "/")

    def test_a_prefix_reads_as_it_did(self):
        self.put("pit/changed.txt", b"before")
        self.put("pit/deleted.txt", b"kept")
        self.put("pit/same.txt", b"same")
        time.sleep(1.2)
        moment = datetime.now(timezone.utc)
        time.sleep(1.2)
        self.put("pit/changed.txt", b"after")
        self.model.client.delete_object(Bucket=self.bucket, Key="pit/deleted.txt")
        self.put("pit/added.txt", b"new")
        self.put("pit/other/inside.txt", b"x")

        versions = self.model.list_prefix_versions("pit/")
        plan = {a["key"]: a for a in plan_point_in_time(versions, moment)}
        self.assertEqual(plan["pit/changed.txt"]["action"], RESTORE)
        self.assertEqual(plan["pit/deleted.txt"]["action"], RESTORE)
        self.assertEqual(plan["pit/added.txt"]["action"], DELETE)
        self.assertEqual(plan["pit/same.txt"]["action"], "skip")

        for key, action, version_id in restore_job(plan.values()):
            if action == RESTORE:
                self.model.make_version_current(key, version_id)
            else:
                self.model.mark_deleted(key)

        self.assertEqual(self._body("pit/changed.txt"), b"before")
        self.assertEqual(self._body("pit/deleted.txt"), b"kept")
        self.assertFalse(self._exists("pit/added.txt"))
        self.assertFalse(self._exists("pit/other/inside.txt"))
        self.assertEqual(self._body("pit/same.txt"), b"same")
        # Nothing was destroyed: the "after" version is still in the history.
        bodies = [v for v in self.model.list_object_versions("pit/changed.txt")
                  if not v["is_delete_marker"]]
        self.assertEqual(len(bodies), 3)

    def test_marking_a_folder_marker_leaves_its_contents(self):
        # Child first: MinIO stalls ~30s writing into a versioned prefix that
        # already has a folder-marker object, so the marker is written last.
        self.put("pit2/dir/child.txt", b"child")
        self.put("pit2/dir/", b"")
        self.model.mark_deleted("pit2/dir/")
        self.assertEqual(self._body("pit2/dir/child.txt"), b"child")


class MetadataSearchTests(ServerCase):

    def setUp(self):
        self.put("s/a.png", b"x", ContentType="image/png", Tagging="team=infra")
        self.put("s/b.png", b"x", ContentType="image/png", Tagging="team=web")
        self.put("s/c.csv", b"x", ContentType="text/csv",
                 Tagging="team=infra&archived=yes")

    def _hits(self, **filters):
        return [k for k, _s, _m in self.model.search_keys("s/", "", **filters)]

    def test_tags(self):
        self.assertEqual(self._hits(tags="team=infra"), ["s/a.png", "s/c.csv"])
        self.assertEqual(self._hits(tags="archived"), ["s/c.csv"])

    def test_content_type(self):
        self.assertEqual(self._hits(content_type="image/"), ["s/a.png", "s/b.png"])
        self.assertEqual(self._hits(content_type="text/csv", tags="team=infra"),
                         ["s/c.csv"])
