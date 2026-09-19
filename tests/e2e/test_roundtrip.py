"""Core object operations against a real server.

These are the paths a stub cannot settle: that a listing round-trips the
shape the app drew, that a server-side copy keeps what the app thinks it
keeps, that a delete of a prefix really empties it.
"""

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

from model import FSObjectType, TransferCancelled     # noqa: F401
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
