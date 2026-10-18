"""Core object operations against a real server.

These are the paths a stub cannot settle: that a listing round-trips the
shape the app drew, that a server-side copy keeps what the app thinks it
keeps, that a delete of a prefix really empties it.
"""

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

from model import FSObjectType  # noqa: F401
from tests.e2e.harness import ServerCase


class ListingTests(ServerCase):
    def test_a_prefix_lists_as_folders_and_files(self):
        self.put("top.txt", b"a")
        self.put("dir/inner.txt", b"bb")
        self.put("dir/deeper/x.txt", b"ccc")
        items = self.model.list("")
        kinds = {item.name: item.type_ for item in items}
        self.assertEqual(kinds.get("top.txt"), FSObjectType.FILE)
        self.assertEqual(kinds.get("dir"), FSObjectType.FOLDER)
        # A delimiter listing must NOT flatten the tree.
        self.assertNotIn("inner.txt", kinds)

    def test_a_listing_carries_size_and_etag(self):
        self.put("sized.bin", b"0123456789")
        item = next(i for i in self.model.list("") if i.name == "sized.bin")
        self.assertEqual(item.size, 10)
        self.assertTrue(item.etag, "the listing gave no ETag to dedupe on")
        self.assertNotIn('"', item.etag, "quotes must be stripped")

    def test_a_sibling_prefix_is_not_swept_in(self):
        """'photos' must not match 'photos-old/' — the trailing slash is
        what keeps a size total or a delete honest."""
        self.put("photos/a.txt", b"a")
        self.put("photos-old/b.txt", b"bb")
        keys = [k for k, _s in self.model.get_keys("photos/")]
        self.assertEqual(keys, ["photos/a.txt"])


class UploadDownloadTests(ServerCase):
    def test_a_file_round_trips_byte_for_byte(self):
        payload = os.urandom(64 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "up.bin")
            with open(src, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(src, "round/up.bin")
            out = os.path.join(tmp, "down.bin")
            self.model.download_file("round/up.bin", out, tmp)
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)

    def test_an_upload_is_stamped_with_a_content_type(self):
        """A public link renders in a browser only if this is right, and the
        stdlib table differs by machine — which is why the app has its own."""
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "page.md")
            with open(src, "w") as handle:
                handle.write("# hi")
            self.model.upload_file(src, "typed/page.md")
        head = self.model.client.head_object(
            Bucket=self.bucket, Key="typed/page.md")
        self.assertEqual(head["ContentType"], "text/markdown")

    def test_a_whole_prefix_downloads_as_a_tree(self):
        self.put("tree/a.txt", b"a")
        self.put("tree/sub/b.txt", b"bb")
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("tree/", None, tmp)
            base = os.path.join(tmp, "tree")
            self.assertTrue(os.path.isfile(os.path.join(base, "a.txt")))
            self.assertTrue(
                os.path.isfile(os.path.join(base, "sub", "b.txt")))

    def test_a_folder_placeholder_is_created_and_listed(self):
        self.model.create_folder("made/")
        self.assertIn("made", [i.name for i in self.model.list("")])


class CopyMoveDeleteTests(ServerCase):
    def test_a_server_side_copy_preserves_the_body(self):
        self.put("cp/src.txt", b"payload")
        self.model.copy_object("cp/src.txt", "cp/dst.txt")
        self.assertEqual(self.body("cp/dst.txt"), b"payload")

    def test_copying_a_prefix_keeps_its_shape(self):
        self.put("shape/a.txt", b"a")
        self.put("shape/sub/b.txt", b"bb")
        self.model.copy_prefix("shape/", "shaped/")
        self.assertEqual(self.keys("shaped/"),
                         ["shaped/a.txt", "shaped/sub/b.txt"])

    def test_copying_a_prefix_into_itself_is_refused(self):
        """On a move the follow-up delete of the source would then wipe the
        fresh copy, so this has to fail before anything is written."""
        self.put("nest/a.txt", b"a")
        with self.assertRaises(ValueError):
            self.model.copy_prefix("nest/", "nest/inner/")
        self.assertEqual(self.keys("nest/"), ["nest/a.txt"])

    def test_a_copy_lands_in_the_class_the_source_was_in(self):
        """REGRESSION: a plain CopyObject carries no StorageClass and the
        service creates the new object as STANDARD, so a copy, move or
        rename re-tiered what it touched. MinIO stores everything as
        STANDARD, so what a real server proves here is the other half: that
        the preserving request is still one it accepts.
        """
        self.put("sc/src.txt", b"payload")
        source = self.storage_class("sc/src.txt")
        self.model.copy_object("sc/src.txt", "sc/dst.txt",
                               storage_class=source)
        self.assertEqual(self.storage_class("sc/dst.txt"), source)
        self.assertEqual(self.body("sc/dst.txt"), b"payload")

    def test_the_listing_a_copy_reads_its_class_from_names_one(self):
        """The whole scheme rests on ListObjectsV2 answering with a class
        in the same element as the key, so nothing costs a HEAD."""
        self.put("sc/listed.txt", b"x")
        listed = {key: storage_class for key, _size, storage_class
                  in self.model.get_keys_with_class("sc/listed")}
        self.assertTrue(listed.get("sc/listed.txt"))

    def test_a_prefix_copy_preserves_every_class_it_listed(self):
        self.put("scdir/a.txt", b"a")
        self.put("scdir/sub/b.txt", b"bb")
        before = {key: storage_class for key, _size, storage_class
                  in self.model.get_keys_with_class("scdir/")}
        self.model.copy_prefix("scdir/", "scdone/")
        after = {key.replace("scdone/", "scdir/", 1): storage_class
                 for key, _size, storage_class
                 in self.model.get_keys_with_class("scdone/")}
        self.assertEqual(after, before)

    def test_the_preserved_class_does_not_cost_the_metadata(self):
        """MetadataDirective stays COPY; a REPLACE would reset the content
        type to binary/octet-stream and drop the user metadata."""
        self.put("sc/typed.txt", b"x", ContentType="text/plain",
                 Metadata={"owner": "duck"})
        self.model.copy_object(
            "sc/typed.txt", "sc/typed-copy.txt",
            storage_class=self.storage_class("sc/typed.txt"))
        head = self.model.client.head_object(
            Bucket=self.bucket, Key="sc/typed-copy.txt")
        self.assertEqual(head["ContentType"], "text/plain")
        self.assertEqual(head.get("Metadata"), {"owner": "duck"})

    def test_a_class_the_server_will_not_take_still_makes_the_copy(self):
        """A backend may name a class in its own listing and then refuse it
        on a copy. Losing the tier is bad; losing the rename the user asked
        for is worse."""
        self.put("sc/odd.txt", b"payload")
        self.model.copy_object("sc/odd.txt", "sc/odd-copy.txt",
                               storage_class="GLACIER_IR")
        self.assertEqual(self.body("sc/odd-copy.txt"), b"payload")

    def test_deleting_a_prefix_removes_every_object_under_it(self):
        for n in range(5):
            self.put(f"doomed/{n}.txt", b"x")
        self.put("kept.txt", b"x")
        self.model.delete("doomed/")
        self.assertEqual(self.keys("doomed/"), [])
        self.assertIn("kept.txt", self.keys())

    def test_a_batch_delete_reports_no_errors_when_it_works(self):
        """The fix for silent partial failures must not turn a clean delete
        into a raise."""
        for n in range(3):
            self.put(f"batch/{n}.txt", b"x")
        self.model.delete("batch/")
        self.assertEqual(self.keys("batch/"), [])

    def test_deleting_more_than_one_batch_worth(self):
        """DELETE_BATCH_SIZE is 1000, so the chunking — and the fact that a
        second DeleteObjects call is even issued — is only ever exercised
        for real here."""
        count = 1100
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(
                lambda n: self.model.client.put_object(
                    Bucket=self.bucket, Key=f"many/{n}.txt", Body=b""),
                range(count)))
        self.assertEqual(len(self.keys("many/")), count)
        self.model.delete("many/")
        self.assertEqual(self.keys("many/"), [])
