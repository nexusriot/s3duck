"""Copying between two profiles, against a real server.

CopyObject is server-side and cannot cross credentials, so the app reads with
one client and writes with another. Nothing about that is observable from a
stub: the whole point is that two independent clients are involved, and a
fake that accepts both would prove only that the code calls them.

Two Models against the same server and different buckets stand in for two
profiles. That is the same arrangement the app uses -- it never assumes the
destination is the same provider, let alone the same account.
"""

import botocore.exceptions

from model import ReadOnlyError
from tests.e2e.harness import ServerCase, make_model, purge_bucket, unique_bucket


class CrossProfileCopyTests(ServerCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dest_bucket = unique_bucket("dest")
        cls.dest = make_model(bucket=cls.dest_bucket)
        cls.dest.client.create_bucket(Bucket=cls.dest_bucket)
        cls.dest.enter_bucket(cls.dest_bucket)

    @classmethod
    def tearDownClass(cls):
        purge_bucket(cls.dest.client, cls.dest_bucket)
        super().tearDownClass()

    def dest_keys(self, prefix=""):
        out = []
        paginator = self.dest.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.dest_bucket, Prefix=prefix):
            out.extend(o["Key"] for o in (page.get("Contents") or []))
        return sorted(out)

    def dest_body(self, key):
        return self.dest.client.get_object(
            Bucket=self.dest_bucket, Key=key)["Body"].read()

    def test_one_object_arrives_byte_for_byte(self):
        payload = b"across the wire"
        self.put("xp/one.txt", payload)
        self.model.copy_to_model("xp/one.txt", self.dest, "landed.txt")
        self.assertEqual(self.dest_body("landed.txt"), payload)

    def test_the_content_type_travels_with_it(self):
        """Carried explicitly: a cross-profile copy is a GET and a PUT, and
        a PUT with no Content-Type is stamped binary/octet-stream, which is
        what makes a public link download instead of render."""
        self.put("xp/typed.json", b'{"a":1}', ContentType="application/json")
        self.model.copy_to_model("xp/typed.json", self.dest, "typed.json")
        head = self.dest.client.head_object(Bucket=self.dest_bucket,
                                            Key="typed.json")
        self.assertEqual(head["ContentType"], "application/json")

    def test_the_source_is_left_alone(self):
        self.put("xp/keep.txt", b"original")
        self.model.copy_to_model("xp/keep.txt", self.dest, "copy.txt")
        self.assertEqual(self.body("xp/keep.txt"), b"original")

    def test_a_whole_prefix_keeps_its_shape(self):
        self.put("xp/tree/a.txt", b"a")
        self.put("xp/tree/sub/b.txt", b"b")
        self.put("xp/tree/sub/deeper/c.txt", b"c")
        self.model.copy_prefix_to_model("xp/tree/", self.dest, "landed/")
        self.assertEqual(
            self.dest_keys("landed/"),
            ["landed/a.txt", "landed/sub/b.txt", "landed/sub/deeper/c.txt"])

    def test_a_prefix_copy_moves_the_bodies_not_just_the_names(self):
        self.put("xp/bodies/one.txt", b"first")
        self.put("xp/bodies/two.txt", b"second")
        self.model.copy_prefix_to_model("xp/bodies/", self.dest, "bodies/")
        self.assertEqual(self.dest_body("bodies/one.txt"), b"first")
        self.assertEqual(self.dest_body("bodies/two.txt"), b"second")

    def test_an_empty_prefix_copies_nothing_rather_than_everything(self):
        """The destination prefix is user-typed, and "" has to mean the
        bucket root on the far side, not a wildcard on this one."""
        before = set(self.dest_keys())
        self.model.copy_prefix_to_model("xp/nothing-here/", self.dest, "void/")
        self.assertEqual(set(self.dest_keys()) - before, set())

    def test_a_read_only_destination_is_refused_before_anything_is_read(self):
        """The destination's own read-only flag has to be what stops it: the
        source profile is perfectly writable and knows nothing about it."""
        guarded = make_model(bucket=self.dest_bucket)
        guarded.read_only = True
        self.put("xp/refused.txt", b"x")
        with self.assertRaises(ReadOnlyError):
            self.model.copy_to_model("xp/refused.txt", guarded, "nope.txt")
        self.assertNotIn("nope.txt", self.dest_keys())

    def test_a_read_only_destination_refuses_a_prefix_copy_too(self):
        guarded = make_model(bucket=self.dest_bucket)
        guarded.read_only = True
        self.put("xp/refused-tree/a.txt", b"x")
        with self.assertRaises(ReadOnlyError):
            self.model.copy_prefix_to_model("xp/refused-tree/", guarded, "no/")
        self.assertEqual(self.dest_keys("no/"), [])

    def test_a_missing_source_fails_rather_than_writing_an_empty_object(self):
        """Half a copy is worse than none: the destination would hold a key
        that looks like a successful transfer."""
        with self.assertRaises(botocore.exceptions.ClientError):
            self.model.copy_to_model("xp/not-there.txt", self.dest, "ghost.txt")
        self.assertNotIn("ghost.txt", self.dest_keys())
