"""The transfer settings, exercised against a real server.

Everything here is a knob the user can turn in "Transfer settings…", and
every one of them changes how the bytes are actually sent: how many files
move at once, how many threads work on each, how fast they are allowed to go,
and how a large file is cut into parts. The unit suite proves the arithmetic;
only a server proves that the resulting requests are ones S3 accepts and that
the object on the other side is the one that went in.

The model each transfer really runs on is a clone, not the window's own, so
that is what these drive wherever the application would.
"""

import os
import tempfile
import threading
import unittest.mock

from core import parts as _parts
from model import Model
from tests.e2e.harness import ServerCase

MiB = 1024 * 1024


class WorkerModelCloneTests(ServerCase):
    """Every transfer runs on clone_for_worker(), never on the window's
    model, so that a region rebind inside a transfer cannot race whatever the
    main thread is reading. The clone had no end-to-end coverage: an
    attribute left out of it is invisible until a transfer uses it."""

    def test_a_clone_can_read_the_bucket_on_its_own(self):
        self.put("clone/a.txt", b"hello")
        clone = self.model.clone_for_worker()
        self.assertEqual(
            [k for k, _s in clone.get_keys("clone/")], ["clone/a.txt"])

    def test_a_clone_does_not_share_the_client(self):
        """The whole point: its own boto3 session and its own client."""
        clone = self.model.clone_for_worker()
        self.assertIsNot(clone.client, self.model.client)
        self.assertIsNot(clone.session, self.model.session)

    def test_a_clone_shares_the_binding_cache(self):
        """A binding a worker discovers is worth keeping for the whole app."""
        self.assertIs(self.model.clone_for_worker().binding_cache,
                      self.model.binding_cache)

    def test_a_clone_can_write_and_the_original_sees_it(self):
        clone = self.model.clone_for_worker()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(b"written by the clone")
            clone.upload_file(path, "clone/written.txt")
        self.assertEqual(self.body("clone/written.txt"),
                         b"written by the clone")
        self.assertIn("clone/written.txt",
                      [k for k, _s in self.model.get_keys("clone/")])

    def test_a_clone_carries_every_transfer_setting(self):
        """An attribute the clone forgets is a setting that silently stops
        applying the moment a real transfer starts."""
        self.model.set_parallel_files(3)
        self.model.set_transfer_concurrency(2)
        self.model.set_multipart_sizes(threshold_mb=7, chunksize_mb=6)
        self.model.verify_downloads = True
        self.addCleanup(setattr, self.model, "verify_downloads", False)
        self.addCleanup(self.model.set_parallel_files,
                        Model.DEFAULT_PARALLEL_FILES)
        self.addCleanup(self.model.set_transfer_concurrency,
                        Model.DEFAULT_TRANSFER_CONCURRENCY)
        self.addCleanup(self.model.set_multipart_sizes, 16, 8)

        clone = self.model.clone_for_worker()
        for attribute in ("parallel_files", "transfer_concurrency",
                          "multipart_threshold_mb", "multipart_chunksize_mb",
                          "upload_chunk_size", "resume_chunk_size",
                          "resume_threshold", "verify_downloads", "read_only",
                          "requester_pays", "bucket", "endpoint_url"):
            with self.subTest(attribute=attribute):
                self.assertEqual(getattr(clone, attribute),
                                 getattr(self.model, attribute))


class ParallelTransferTests(ServerCase):
    """parallel_files moves several files at once; transfer_concurrency
    parallelises the parts within one file. Both fan requests out across
    threads that share a client, which is the kind of thing that works in a
    unit test with a fake and deadlocks or interleaves wrongly against a
    real server."""

    FILES = 8

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.bodies = {f"par/f{i}.bin": os.urandom(4096 + i)
                      for i in range(cls.FILES)}
        for key, body in cls.bodies.items():
            cls.model.client.put_object(
                Bucket=cls.bucket, Key=key, Body=body)

    def setUp(self):
        self.addCleanup(self.model.set_parallel_files,
                        Model.DEFAULT_PARALLEL_FILES)
        self.addCleanup(self.model.set_transfer_concurrency,
                        Model.DEFAULT_TRANSFER_CONCURRENCY)

    def test_a_parallel_prefix_download_writes_every_body_intact(self):
        self.model.set_parallel_files(4)
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("par/", "", tmp)
            root = os.path.join(tmp, "par")
            landed = sorted(os.listdir(root))
            self.assertEqual(landed,
                             sorted(k.split("/")[1] for k in self.bodies))
            for key, body in self.bodies.items():
                with open(os.path.join(root, key.split("/")[1]), "rb") as fh:
                    self.assertEqual(fh.read(), body, key)

    def test_a_sequential_download_agrees_with_the_parallel_one(self):
        """parallel_files=1 is the documented "strictly sequential" setting,
        and it has to produce the same tree."""
        self.model.set_parallel_files(1)
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("par/", "", tmp)
            root = os.path.join(tmp, "par")
            for key, body in self.bodies.items():
                with open(os.path.join(root, key.split("/")[1]), "rb") as fh:
                    self.assertEqual(fh.read(), body, key)

    def test_progress_never_reports_more_than_the_total(self):
        """The per-file callbacks are summed under a lock across threads; a
        double-count shows up here as a bar past 100%."""
        self.model.set_parallel_files(4)
        seen = []
        lock = threading.Lock()

        def _progress(total, current, _key):
            with lock:
                seen.append((int(total), int(current)))

        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("par/", "", tmp, progress_cb=_progress)
        self.assertTrue(seen, "no progress was reported at all")
        for total, current in seen:
            self.assertLessEqual(current, total)

    def test_a_parallel_multipart_upload_arrives_byte_for_byte(self):
        """transfer_concurrency parallelises the parts of one file, and the
        completion has to list them in ascending order whatever order the
        threads finished in."""
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        self.model.set_transfer_concurrency(4)
        self.model.resumable_uploads = True
        self.model.set_upload_options(checksum_algorithm="CRC32")
        self.addCleanup(self.model.set_upload_options, checksum_algorithm="")
        self.addCleanup(self.model.set_multipart_sizes, 16, 8)
        payload = os.urandom(17 * MiB)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "big.bin")
            with open(path, "wb") as handle:
                handle.write(payload)
            # Its own prefix: "par/" is the fixture the download tests list,
            # and an extra object there would be a file they did not put.
            self.model.upload_file(path, "parbig/parallel-big.bin")
        self.assertEqual(self.body("parbig/parallel-big.bin"), payload)


class RateLimitedTransferTests(ServerCase):
    """The ceiling is shared by every transfer, so it is charged from inside
    the request body and the progress callback. Charging the same bytes twice
    would halve the real throughput silently, and charging them on a retry
    would stall a transfer that is making progress -- neither is visible
    without moving bytes."""

    def setUp(self):
        self.addCleanup(self.model.set_rate_limit, 0)

    def test_a_limited_upload_and_download_still_round_trip(self):
        payload = os.urandom(512 * 1024)
        # Generous enough that the suite does not wait on it, small enough
        # that the limiter is genuinely in the path.
        self.model.set_rate_limit(8 * MiB)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "limited.bin")
            with open(path, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(path, "rate/limited.bin")
            back = os.path.join(tmp, "back.bin")
            self.model.download_file("rate/limited.bin", back, tmp)
            with open(back, "rb") as handle:
                self.assertEqual(handle.read(), payload)
        self.assertEqual(self.body("rate/limited.bin"), payload)

    def test_removing_the_limit_leaves_transfers_working(self):
        self.model.set_rate_limit(4 * MiB)
        self.assertIsNotNone(self.model.rate_limiter)
        self.model.set_rate_limit(0)
        self.assertIsNone(self.model.rate_limiter)
        payload = os.urandom(64 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "unlimited.bin")
            with open(path, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(path, "rate/unlimited.bin")
        self.assertEqual(self.body("rate/unlimited.bin"), payload)


class MultipartPartCeilingTests(ServerCase):
    """S3 numbers the parts of one upload 1..10 000 and refuses anything
    past that.

    REGRESSION: the resumable writer derived the part count from a fixed
    chunk size with no clamp, so a file over ``chunk x 10 000`` -- 80 GiB at
    the default 8 MiB -- was refused on the part that crossed the line, after
    everything before it had already been sent.

    The ceiling is lowered rather than the file enlarged: a real 80 GiB
    upload is not a test. What stays real is the server, the part sizes (S3
    and MinIO both enforce a 5 MiB floor on every part but the last) and the
    object that comes out the other end.
    """

    def setUp(self):
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        self.model.resumable_uploads = True
        # Without a named algorithm botocore puts a default CRC32 on every
        # UploadPart while CreateMultipartUpload carries none, and MinIO
        # rejects the part for a reason that has nothing to do with this.
        self.model.set_upload_options(checksum_algorithm="CRC32")
        self.addCleanup(self.model.set_upload_options, checksum_algorithm="")
        self.addCleanup(self.model.set_multipart_sizes, 16, 8)

    def _upload(self, key, payload):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(path, key)

    def test_the_part_size_is_raised_so_the_upload_fits_the_ceiling(self):
        payload = os.urandom(21 * MiB)
        # 21 MiB at 5 MiB parts is 5 parts; a ceiling of 3 forces 7 MiB
        # parts, which is still above S3's floor and still legal here.
        with unittest.mock.patch.object(_parts, "MAX_MULTIPART_PARTS", 3):
            self._upload("ceiling/raised.bin", payload)
        self.assertEqual(self.body("ceiling/raised.bin"), payload)

    def test_it_really_was_sent_as_a_multipart_upload(self):
        """Guard the guard: if the file quietly took the single-PUT route the
        assertion above would pass without the part logic running at all."""
        payload = os.urandom(21 * MiB)
        with unittest.mock.patch.object(_parts, "MAX_MULTIPART_PARTS", 3):
            self._upload("ceiling/dashed.bin", payload)
        etag = self.model.client.head_object(
            Bucket=self.bucket, Key="ceiling/dashed.bin")["ETag"]
        self.assertIn("-", etag.strip('"'),
                      "not a multipart upload, so nothing was clamped")

    def test_the_part_count_stays_within_the_ceiling(self):
        """Counted from what the server was actually asked to store."""
        payload = os.urandom(21 * MiB)
        with unittest.mock.patch.object(_parts, "MAX_MULTIPART_PARTS", 3):
            self._upload("ceiling/counted.bin", payload)
        etag = self.model.client.head_object(
            Bucket=self.bucket, Key="ceiling/counted.bin")["ETag"]
        self.assertLessEqual(int(etag.strip('"').rsplit("-", 1)[1]), 3)

    def test_an_upload_that_already_fits_keeps_its_configured_part_size(self):
        """The clamp raises the size only when it has to; otherwise the
        user's setting is what is used."""
        payload = os.urandom(21 * MiB)
        self._upload("ceiling/untouched.bin", payload)
        self.assertEqual(self.body("ceiling/untouched.bin"), payload)
        etag = self.model.client.head_object(
            Bucket=self.bucket, Key="ceiling/untouched.bin")["ETag"]
        # 21 MiB in 5 MiB parts is five of them, the configured size.
        self.assertEqual(int(etag.strip('"').rsplit("-", 1)[1]), 5)
