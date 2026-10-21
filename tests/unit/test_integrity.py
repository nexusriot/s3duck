"""Checksums, ETags and fingerprints."""

import base64
import botocore.session
import contextlib
import hashlib
import model as model_module
import os
import shutil
import struct
import tempfile
import unittest
import zlib
from core.integrity import (
    etag_is_opaque, file_matches_etag, file_part_md5s,
)
from model import CHECKSUM_ALGORITHMS, Model
from tests.support import FakeS3Client, _AttributesClient, _dt, make_model
from unittest.mock import patch

try:
    from awscrt import checksums as _crt_checksums
except ImportError:
    _crt_checksums = None


class ChecksumTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_etag_is_md5_detection(self):
        self.assertTrue(Model.etag_is_md5('"' + "a" * 32 + '"'))
        self.assertFalse(Model.etag_is_md5("a" * 32 + "-3"))  # multipart
        self.assertFalse(Model.etag_is_md5(""))
        self.assertFalse(Model.etag_is_md5("short"))

    def test_matching_file_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"hello world")
            digest = hashlib.md5(b"hello world").hexdigest()
            self.assertTrue(self.m.verify_download(path, f'"{digest}"'))

    def test_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"hello world")
            logs = []
            self.assertFalse(
                self.m.verify_download(path, "b" * 32, log_fn=logs.append))
            self.assertTrue(any("MISMATCH" in line for line in logs))

    def test_multipart_etag_cannot_be_checked_and_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.bin")
            with open(path, "wb") as handle:
                handle.write(b"x")
            logs = []
            self.assertTrue(
                self.m.verify_download(path, "a" * 32 + "-2", log_fn=logs.append))
            self.assertTrue(any("skipped" in line for line in logs))
class AdditionalChecksumTests(unittest.TestCase):
    """Multipart ETags are unverifiable, so verification silently passed for
    exactly the large objects most worth checking. An additional full-object
    checksum stays comparable."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "payload.bin")
        self.data = b"duck" * 5000
        with open(self.path, "wb") as fh:
            fh.write(self.data)
        self.model = Model("https://s3.amazonaws.com", "us-east-1",
                           "AK", "SK", "bkt", False, False)

    def _expected(self, algorithm):
        """What S3 would store, computed without going through the model."""
        if algorithm == "CRC32":
            return base64.b64encode(
                struct.pack(">I", zlib.crc32(self.data) & 0xFFFFFFFF)).decode()
        if algorithm == "CRC32C":
            return base64.b64encode(struct.pack(
                ">I", _crt_checksums.crc32c(self.data) & 0xFFFFFFFF)).decode()
        if algorithm == "CRC64NVME":
            return base64.b64encode(struct.pack(
                ">Q", _crt_checksums.crc64nvme(self.data)
                & 0xFFFFFFFFFFFFFFFF)).decode()
        digest = hashlib.new(algorithm.lower(), self.data).digest()
        return base64.b64encode(digest).decode()

    def test_file_checksum_matches_what_s3_would_store(self):
        for algorithm in CHECKSUM_ALGORITHMS:
            with self.subTest(algorithm=algorithm):
                self.assertEqual(Model.file_checksum(self.path, algorithm),
                                 self._expected(algorithm))

    def test_an_algorithm_we_cannot_compute_is_refused(self):
        """Accepting one would mean 'verified' downloads that were never
        actually checked. Which algorithms those are depends on the install,
        so the list is asked for rather than assumed — only the names that
        are never computable are hardcoded."""
        never = ["MD5", "", None, "SHA512"]
        absent = [name for name in model_module.KNOWN_CHECKSUM_ALGORITHMS
                  if name not in model_module.CHECKSUM_DIGESTS]
        for bad in never + absent:
            with self.subTest(algorithm=bad):
                with self.assertRaises(ValueError):
                    Model.file_checksum(self.path, bad)

    def test_composite_checksums_are_recognised(self):
        self.assertTrue(Model.checksum_is_composite("abcDEF==-4"))
        self.assertFalse(Model.checksum_is_composite("abcDEF=="))
        self.assertFalse(Model.checksum_is_composite("abc-DEF"))
        self.assertFalse(Model.checksum_is_composite(""))
        self.assertFalse(Model.checksum_is_composite(None))

    def test_a_matching_checksum_verifies(self):
        head = {"ChecksumSHA256": self._expected("SHA256")}
        self.assertTrue(self.model.verify_download(self.path, "", head=head))

    def test_a_mismatched_checksum_fails_even_with_a_good_etag(self):
        """The checksum is the stronger signal and must win."""
        head = {"ChecksumSHA256": base64.b64encode(b"x" * 32).decode()}
        etag = hashlib.md5(self.data).hexdigest()
        self.assertFalse(
            self.model.verify_download(self.path, etag, head=head))

    def test_a_full_object_checksum_verifies_a_multipart_object(self):
        """THE POINT: a multipart ETag cannot be checked, but a full-object
        CRC32 on the same object can."""
        multipart_etag = hashlib.md5(self.data).hexdigest() + "-4"
        self.assertFalse(Model.etag_is_md5(multipart_etag))
        head = {"ChecksumCRC32": self._expected("CRC32")}
        logs = []
        self.assertTrue(self.model.verify_download(
            self.path, multipart_etag, head=head, log_fn=logs.append))
        self.assertTrue(any("checksum ok (CRC32)" in m for m in logs))

    def test_a_per_part_checksum_falls_back_to_the_etag(self):
        head = {"ChecksumCRC32": self._expected("CRC32") + "-4"}
        etag = hashlib.md5(self.data).hexdigest()
        logs = []
        self.assertTrue(self.model.verify_download(
            self.path, etag, head=head, log_fn=logs.append))
        self.assertTrue(any("per-part" in m for m in logs))
        self.assertTrue(any("checksum ok" in m for m in logs))

    def test_no_checksum_still_uses_the_etag(self):
        etag = hashlib.md5(self.data).hexdigest()
        self.assertTrue(self.model.verify_download(self.path, etag, head={}))
        self.assertFalse(
            self.model.verify_download(self.path, "0" * 32, head={}))

    def test_stored_checksum_reads_the_head_response(self):
        self.assertEqual(
            Model.stored_checksum({"ChecksumSHA256": "abc"}), ("SHA256", "abc"))
        self.assertEqual(Model.stored_checksum({}), ("", ""))
        self.assertEqual(Model.stored_checksum(None), ("", ""))

    def test_uploads_request_the_checksum(self):
        args = self.model.set_upload_options(checksum_algorithm="SHA256")
        self.assertEqual(args["ChecksumAlgorithm"], "SHA256")
        self.assertEqual(self.model.checksum_algorithm, "SHA256")

    def test_crc32_uploads_ask_for_a_whole_object_checksum(self):
        """Without ChecksumType a multipart upload stores a per-part
        composite, which is as unverifiable as the ETag it replaces."""
        args = self.model.set_upload_options(checksum_algorithm="CRC32")
        self.assertEqual(args["ChecksumType"], "FULL_OBJECT")

    def test_sha_uploads_do_not_ask_for_full_object(self):
        """S3 only supports FULL_OBJECT for the CRC algorithms."""
        args = self.model.set_upload_options(checksum_algorithm="SHA256")
        self.assertNotIn("ChecksumType", args)

    def test_an_unsupported_algorithm_is_not_sent(self):
        """Which algorithms are available depends on the install, so the name
        used here is one that is never computable rather than one that merely
        was not, back when the list was fixed."""
        self.assertNotIn("MD5", model_module.CHECKSUM_DIGESTS)
        args = self.model.set_upload_options(checksum_algorithm="MD5")
        self.assertNotIn("ChecksumAlgorithm", args)
        self.assertEqual(self.model.checksum_algorithm, "")

    def test_the_setting_survives_a_worker_clone(self):
        self.model.set_upload_options(checksum_algorithm="SHA1")
        self.assertEqual(
            self.model.clone_for_worker().checksum_algorithm, "SHA1")

    def test_head_asks_for_the_checksum(self):
        calls = []

        class _Client:
            def head_object(self, **kw):
                calls.append(kw)
                return {"ContentLength": 1}

        self.model._client = _Client()
        self.model.head_with_checksum("k")
        self.assertEqual(calls[0].get("ChecksumMode"), "ENABLED")

    def test_a_backend_rejecting_checksum_mode_still_works(self):
        """Older S3-compatible servers reject the parameter; without the retry
        every download's HEAD would fail."""
        calls = []

        class _Client:
            def head_object(self, **kw):
                calls.append(kw)
                if "ChecksumMode" in kw:
                    raise botocore.exceptions.ParamValidationError(
                        report="unknown parameter ChecksumMode")
                return {"ContentLength": 7}

        self.model._client = _Client()
        self.assertEqual(
            self.model.head_with_checksum("k"), {"ContentLength": 7})
        self.assertEqual(len(calls), 2)
class ChecksumRegistryTests(unittest.TestCase):
    """
    What the UI offers is built from what this machine can recompute. An
    algorithm we can send but not reproduce would make every verified
    download a verification that never happened.
    """

    def test_only_computable_algorithms_are_offered(self):
        for name in CHECKSUM_ALGORITHMS:
            with self.subTest(algorithm=name):
                self.assertIn(name, model_module.CHECKSUM_DIGESTS)
                self.assertIn(name, model_module.KNOWN_CHECKSUM_ALGORITHMS)

    def test_the_stdlib_algorithms_are_always_there(self):
        """They need nothing installed, so their absence would be a bug."""
        for name in ("CRC32", "SHA1", "SHA256"):
            self.assertIn(name, CHECKSUM_ALGORITHMS)

    def test_the_offered_order_is_the_known_order(self):
        """The order decides which stored checksum is picked first, so it has
        to be stable rather than whatever a dict happened to yield."""
        known = list(model_module.KNOWN_CHECKSUM_ALGORITHMS)
        self.assertEqual(list(CHECKSUM_ALGORITHMS),
                         [n for n in known if n in CHECKSUM_ALGORITHMS])

    def test_a_crc_is_packed_the_way_s3_stores_it(self):
        """Big-endian, to the algorithm's own width, then base64 — a 64-bit
        CRC written into four bytes would mismatch every object."""
        wide = model_module._CrcDigest(lambda block, previous: 0x0102030405060708, 8)
        wide.update(b"x")
        self.assertEqual(base64.b64decode(wide.b64()),
                         bytes([1, 2, 3, 4, 5, 6, 7, 8]))
        narrow = model_module._CrcDigest(lambda block, previous: 0x01020304, 4)
        narrow.update(b"x")
        self.assertEqual(base64.b64decode(narrow.b64()), bytes([1, 2, 3, 4]))

    @unittest.skipIf(_crt_checksums is None or
                     not hasattr(_crt_checksums, "crc64nvme"),
                     "awscrt with crc64nvme is not installed")
    def test_crc64nvme_is_offered_when_it_can_be_computed(self):
        self.assertIn("CRC64NVME", CHECKSUM_ALGORITHMS)

    def test_every_crc_asks_for_a_whole_object_checksum(self):
        """A per-part composite is as unverifiable as a multipart ETag, and
        CRC64NVME has no composite form at all."""
        m = make_model(bucket="bkt")
        for name in CHECKSUM_ALGORITHMS:
            with self.subTest(algorithm=name):
                args = m.set_upload_options(checksum_algorithm=name)
                if name.startswith("CRC"):
                    self.assertEqual(args.get("ChecksumType"), "FULL_OBJECT")
                else:
                    self.assertNotIn("ChecksumType", args)

    def test_a_computable_checksum_wins_over_one_we_cannot_reproduce(self):
        """REGRESSION GUARD: an object carrying a CRC64NVME on a build without
        awscrt must not shadow a SHA256 that is perfectly comparable."""
        with patch.object(model_module, "CHECKSUM_ALGORITHMS",
                          ("CRC32", "SHA1", "SHA256")), \
             patch.object(model_module, "CHECKSUM_DIGESTS",
                          {"CRC32": None, "SHA1": None, "SHA256": None}):
            self.assertEqual(
                Model.stored_checksum({"ChecksumCRC64NVME": "aaaaaaaaaaaa=",
                                       "ChecksumSHA256": "sha"}),
                ("SHA256", "sha"))

    def test_an_uncomputable_checksum_is_still_reported(self):
        """So a download can say why nothing was compared, instead of
        reporting a comparison it never made."""
        with patch.object(model_module, "CHECKSUM_ALGORITHMS",
                          ("CRC32", "SHA1", "SHA256")), \
             patch.object(model_module, "CHECKSUM_DIGESTS",
                          {"CRC32": None, "SHA1": None, "SHA256": None}):
            self.assertEqual(
                Model.stored_checksum({"ChecksumCRC64NVME": "nvme"}),
                ("CRC64NVME", "nvme"))
class UncomputableChecksumDownloadTests(unittest.TestCase):
    """AWS now stamps CRC64NVME on uploads by default, so an object arriving
    with one this build cannot reproduce is the ordinary case, not the
    exotic one."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "payload.bin")
        self.data = b"duck" * 100
        with open(self.path, "wb") as handle:
            handle.write(self.data)
        self.model = make_model(bucket="bkt")

    @contextlib.contextmanager
    def _without_awscrt(self):
        with patch.object(model_module, "CHECKSUM_ALGORITHMS",
                          ("CRC32", "SHA1", "SHA256")),              patch.object(model_module, "CHECKSUM_DIGESTS",
                          {name: model_module.CHECKSUM_DIGESTS[name]
                           for name in ("CRC32", "SHA1", "SHA256")}):
            yield

    def test_it_falls_back_to_the_etag_rather_than_passing_blindly(self):
        """THE POINT: the ETag still proves something; the unreadable
        checksum proves nothing, and must not stand in for it."""
        logged = []
        head = {"ChecksumCRC64NVME": "AAAAAAAAAAA="}
        with self._without_awscrt():
            ok = self.model.verify_download(
                self.path, "0" * 32, head=head, log_fn=logged.append)
        self.assertFalse(ok, "a wrong ETag passed")
        self.assertTrue(any("awscrt" in line for line in logged), logged)

    def test_a_matching_etag_still_verifies(self):
        head = {"ChecksumCRC64NVME": "AAAAAAAAAAA="}
        with self._without_awscrt():
            self.assertTrue(self.model.verify_download(
                self.path, hashlib.md5(self.data).hexdigest(), head=head))
class ListObjectDigestsTests(unittest.TestCase):
    def test_returns_key_size_etag_and_modified(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client(list_pages=[{"Contents": [
            {"Key": "a.txt", "Size": 5, "ETag": '"abc"', "LastModified": _dt(1)},
            {"Key": "dir/", "Size": 0, "ETag": '""', "LastModified": _dt(1)},
        ]}])
        rows = m.list_object_digests("")
        self.assertEqual(rows, [("a.txt", 5, "abc", _dt(1))])

    def test_requires_a_bucket(self):
        with self.assertRaises(ValueError):
            make_model(bucket="").list_object_digests("")
class MultipartVerificationTests(unittest.TestCase):
    """A multipart ETag or a per-part checksum cannot be reproduced without
    the original part boundaries — GetObjectAttributes supplies them."""

    def _file(self, chunks):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = os.path.join(directory, "obj.bin")
        with open(path, "wb") as handle:
            for chunk in chunks:
                handle.write(chunk)
        return path

    def _model(self, client=None):
        m = make_model(bucket="bkt")
        m._client = client or _AttributesClient()
        return m

    def test_composite_etag_is_rebuilt_the_way_s3_builds_one(self):
        first, second = b"a" * 16, b"b" * 8
        expected = hashlib.md5(
            hashlib.md5(first).digest() + hashlib.md5(second).digest()
        ).hexdigest() + "-2"
        self.assertEqual(
            Model.composite_etag([hashlib.md5(first).digest(),
                                  hashlib.md5(second).digest()]),
            expected)

    def test_no_parts_means_no_etag(self):
        self.assertEqual(Model.composite_etag([]), "")

    def test_part_digests_split_the_file_at_the_given_sizes(self):
        path = self._file([b"a" * 16, b"b" * 8])
        checksums, md5s = Model.file_part_digests(path, [16, 8], "SHA256")
        self.assertEqual(len(checksums), 2)
        self.assertEqual(
            checksums[0],
            base64.b64encode(hashlib.sha256(b"a" * 16).digest()).decode())
        self.assertEqual(md5s[1], hashlib.md5(b"b" * 8).digest())

    def test_sizes_that_do_not_add_up_are_refused(self):
        """A mismatch means the parts describe a different object; comparing
        anyway would report a false pass."""
        path = self._file([b"a" * 10])
        with self.assertRaises(ValueError):
            Model.file_part_digests(path, [4, 4], "")

    def test_a_matching_multipart_etag_verifies(self):
        first, second = b"a" * 16, b"b" * 8
        path = self._file([first, second])
        etag = Model.composite_etag(
            [hashlib.md5(first).digest(), hashlib.md5(second).digest()])
        m = self._model()
        parts = [{"PartNumber": 1, "Size": 16}, {"PartNumber": 2, "Size": 8}]
        self.assertIs(m.verify_parts(path, parts, etag=etag), True)

    def test_a_corrupted_body_fails_the_multipart_etag(self):
        first, second = b"a" * 16, b"b" * 8
        etag = Model.composite_etag(
            [hashlib.md5(first).digest(), hashlib.md5(second).digest()])
        path = self._file([first, b"X" * 8])
        m = self._model()
        parts = [{"PartNumber": 1, "Size": 16}, {"PartNumber": 2, "Size": 8}]
        self.assertIs(m.verify_parts(path, parts, etag=etag), False)

    def test_per_part_checksums_are_compared_when_present(self):
        first, second = b"a" * 16, b"b" * 8
        path = self._file([first, second])
        m = self._model()
        parts = [
            {"PartNumber": 1, "Size": 16, "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(first).digest()).decode()},
            {"PartNumber": 2, "Size": 8, "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(second).digest()).decode()},
        ]
        self.assertIs(
            m.verify_parts(path, parts, algorithm="SHA256"), True)
        parts[1]["ChecksumSHA256"] = base64.b64encode(
            hashlib.sha256(b"wrong").digest()).decode()
        self.assertIs(
            m.verify_parts(path, parts, algorithm="SHA256"), False)

    def test_no_parts_is_inconclusive_rather_than_a_failure(self):
        path = self._file([b"a" * 8])
        m = self._model()
        self.assertIsNone(m.verify_parts(path, []))
        self.assertIsNone(m.verify_parts(path, [{"PartNumber": 1, "Size": 0}]))

    def test_parts_are_read_across_pages(self):
        pages = {
            0: {"ObjectParts": {"Parts": [{"PartNumber": 1, "Size": 5}],
                                "IsTruncated": True,
                                "NextPartNumberMarker": 1}},
            1: {"ObjectParts": {"Parts": [{"PartNumber": 2, "Size": 3}],
                                "IsTruncated": False}},
        }
        client = _AttributesClient(attributes={
            "k": lambda kw: pages[int(kw.get("PartNumberMarker") or 0)]})
        m = self._model(client)
        self.assertEqual([p["PartNumber"] for p in m.get_object_parts("k")],
                         [1, 2])

    def test_a_marker_that_never_advances_does_not_spin(self):
        client = _AttributesClient(attributes={"k": {
            "ObjectParts": {"Parts": [{"PartNumber": 1, "Size": 5}],
                            "IsTruncated": True, "NextPartNumberMarker": 0}}})
        m = self._model(client)
        self.assertEqual(len(m.get_object_parts("k")), 1)

    def test_a_backend_without_the_operation_returns_nothing(self):
        for code in ("NotImplemented", "MethodNotAllowed", "AccessDenied"):
            client = _AttributesClient(error=botocore.exceptions.ClientError(
                {"Error": {"Code": code, "Message": "no"}},
                "GetObjectAttributes"))
            with self.subTest(code=code):
                self.assertEqual(self._model(client).get_object_parts("k"), [])

    def test_an_unexpected_error_is_not_swallowed(self):
        client = _AttributesClient(error=botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "gone"}},
            "GetObjectAttributes"))
        with self.assertRaises(botocore.exceptions.ClientError):
            self._model(client).get_object_parts("k")

    def test_verify_download_settles_a_multipart_etag(self):
        """The headline: this used to log "checksum skipped" and pass."""
        first, second = b"a" * 16, b"b" * 8
        path = self._file([first, second])
        etag = Model.composite_etag(
            [hashlib.md5(first).digest(), hashlib.md5(second).digest()])
        client = _AttributesClient(attributes={"obj": {"ObjectParts": {
            "Parts": [{"PartNumber": 1, "Size": 16},
                      {"PartNumber": 2, "Size": 8}]}}})
        m = self._model(client)
        messages = []
        self.assertTrue(m.verify_download(
            path, etag, log_fn=messages.append, key="obj"))
        self.assertTrue(any("multipart ETag, 2 parts" in line
                            for line in messages))

    def test_verify_download_fails_a_corrupted_multipart_object(self):
        first, second = b"a" * 16, b"b" * 8
        etag = Model.composite_etag(
            [hashlib.md5(first).digest(), hashlib.md5(second).digest()])
        path = self._file([first, b"X" * 8])
        client = _AttributesClient(attributes={"obj": {"ObjectParts": {
            "Parts": [{"PartNumber": 1, "Size": 16},
                      {"PartNumber": 2, "Size": 8}]}}})
        self.assertFalse(self._model(client).verify_download(
            path, etag, key="obj"))

    def test_without_parts_it_still_degrades_to_skipping(self):
        path = self._file([b"a" * 24])
        client = _AttributesClient(error=botocore.exceptions.ClientError(
            {"Error": {"Code": "NotImplemented", "Message": "no"}},
            "GetObjectAttributes"))
        m = self._model(client)
        messages = []
        self.assertTrue(m.verify_download(
            path, "deadbeef-2", log_fn=messages.append, key="obj"))
        self.assertTrue(any("skipped" in line for line in messages))

    def test_a_composite_checksum_is_settled_by_the_parts(self):
        first, second = b"a" * 16, b"b" * 8
        path = self._file([first, second])
        client = _AttributesClient(attributes={"obj": {"ObjectParts": {
            "Parts": [
                {"PartNumber": 1, "Size": 16,
                 "ChecksumSHA256": base64.b64encode(
                     hashlib.sha256(first).digest()).decode()},
                {"PartNumber": 2, "Size": 8,
                 "ChecksumSHA256": base64.b64encode(
                     hashlib.sha256(second).digest()).decode()},
            ]}}})
        m = self._model(client)
        head = {"ChecksumSHA256": "someCompositeValue-2"}
        messages = []
        self.assertTrue(m.verify_download(
            path, "", head=head, log_fn=messages.append, key="obj"))
        self.assertTrue(any("SHA256, 2 parts" in line for line in messages))

    def test_a_single_part_object_still_uses_the_plain_etag(self):
        path = self._file([b"hello"])
        client = _AttributesClient()
        m = self._model(client)
        self.assertTrue(m.verify_download(
            path, hashlib.md5(b"hello").hexdigest(), key="obj"))
        # no need to ask for parts when the ETag is already an MD5
        self.assertEqual(client.calls_of("get_object_attributes"), [])
class ObjectFingerprintTests(unittest.TestCase):
    """Settling a duplicate candidate without downloading either copy."""

    def _model(self, attributes=None, error=None):
        m = make_model(bucket="bkt")
        m._client = _AttributesClient(attributes=attributes, error=error)
        return m

    def test_a_whole_object_checksum_is_a_full_fingerprint(self):
        m = self._model({"k": {"Checksum": {"ChecksumCRC32": "abc123=="}}})
        self.assertEqual(m.object_fingerprint("k"), ("full", "CRC32:abc123=="))

    def test_a_plain_etag_is_a_full_fingerprint(self):
        digest = hashlib.md5(b"x").hexdigest()
        m = self._model({"k": {"ETag": f'"{digest}"'}})
        self.assertEqual(m.object_fingerprint("k"), ("full", f"MD5:{digest}"))

    def test_per_part_checksums_are_a_partial_fingerprint(self):
        m = self._model({"k": {"ObjectParts": {"Parts": [
            {"PartNumber": 1, "Size": 8, "ChecksumCRC32": "aa"},
            {"PartNumber": 2, "Size": 4, "ChecksumCRC32": "bb"},
        ]}}})
        kind, value = m.object_fingerprint("k")
        self.assertEqual(kind, "parts")
        self.assertEqual(value, "8:CRC32:aa|4:CRC32:bb")

    def test_a_multipart_object_with_no_digests_says_nothing(self):
        m = self._model({"k": {"ETag": '"abc-3"', "ObjectParts": {"Parts": [
            {"PartNumber": 1, "Size": 8}]}}})
        self.assertEqual(m.object_fingerprint("k"), ("", ""))

    def test_an_unsupported_backend_says_nothing(self):
        m = self._model(error=botocore.exceptions.ClientError(
            {"Error": {"Code": "NotImplemented", "Message": "no"}},
            "GetObjectAttributes"))
        self.assertEqual(m.object_fingerprint("k"), ("", ""))

    def test_equal_full_fingerprints_are_the_same_object(self):
        self.assertEqual(
            Model.compare_fingerprints(
                [("full", "MD5:aaa"), ("full", "MD5:aaa")]), "same")

    def test_different_full_fingerprints_rule_the_group_out(self):
        self.assertEqual(
            Model.compare_fingerprints(
                [("full", "MD5:aaa"), ("full", "MD5:bbb")]),
            "different")

    def test_two_algorithms_cannot_rule_anything_out(self):
        """A CRC32 and an ETag-derived MD5 of the very same bytes are two
        different strings, so the group is unknown, not different."""
        self.assertEqual(
            Model.compare_fingerprints(
                [("full", "CRC32:aaa"), ("full", "MD5:bbb")]),
            "unknown")

    def test_a_mixed_group_where_one_pair_matches_is_still_unknown(self):
        self.assertEqual(
            Model.compare_fingerprints(
                [("full", "SHA256:aa"), ("full", "SHA256:aa"),
                 ("full", "MD5:bb")]),
            "unknown")

    def test_identical_values_are_the_same_whatever_the_algorithm_column(self):
        """Equality needs no algorithm agreement: the strings carry it."""
        self.assertEqual(
            Model.compare_fingerprints(
                [("full", "SHA1:aa"), ("full", "SHA1:aa"),
                 ("full", "SHA1:aa")]),
            "same")

    def test_equal_part_fingerprints_still_prove_a_match(self):
        self.assertEqual(
            Model.compare_fingerprints(
                [("parts", "8:MD5:aa"), ("parts", "8:MD5:aa")]),
            "same")

    def test_different_part_fingerprints_prove_nothing(self):
        """The same bytes split at other boundaries digest differently."""
        self.assertEqual(
            Model.compare_fingerprints(
                [("parts", "8:MD5:aa"), ("parts", "4:MD5:bb")]),
            "unknown")

    def test_one_silent_member_makes_the_whole_group_unknown(self):
        self.assertEqual(
            Model.compare_fingerprints([("full", "MD5:aa"), ("", "")]),
            "unknown")

    def test_a_single_member_is_never_a_verdict(self):
        self.assertEqual(
            Model.compare_fingerprints([("full", "MD5:aa")]), "unknown")
class EncryptedObjectEtagTests(unittest.TestCase):
    """
    REGRESSION (false failure): under SSE-KMS, DSSE-KMS and SSE-C the ETag is
    still 32 hex characters but is not an MD5 of the bytes. "Verify
    downloads" compared it anyway and failed every intact download of such
    an object that had no computable checksum of its own.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "payload.bin")
        self.data = b"duck" * 100
        with open(self.path, "wb") as handle:
            handle.write(self.data)
        self.model = make_model(bucket="bkt")
        # 32 hex characters, so etag_is_md5 cannot tell it from a real one.
        self.kms_etag = hashlib.md5(b"not the payload").hexdigest()

    def _crc32(self, data):
        return base64.b64encode(
            struct.pack(">I", zlib.crc32(data) & 0xFFFFFFFF)).decode()

    def test_which_heads_hide_the_content(self):
        self.assertTrue(etag_is_opaque({"ServerSideEncryption": "aws:kms"}))
        self.assertTrue(etag_is_opaque(
            {"ServerSideEncryption": "aws:kms:dsse"}))
        self.assertTrue(etag_is_opaque({"SSECustomerAlgorithm": "AES256"}))
        self.assertFalse(etag_is_opaque({"ServerSideEncryption": "AES256"}))
        self.assertFalse(etag_is_opaque({}))
        self.assertFalse(etag_is_opaque(None))

    def test_an_intact_kms_object_is_skipped_not_failed(self):
        """THE POINT: this used to return False — "Checksum mismatch"."""
        for head in ({"ServerSideEncryption": "aws:kms"},
                     {"ServerSideEncryption": "aws:kms:dsse"},
                     {"SSECustomerAlgorithm": "AES256"}):
            with self.subTest(head=head):
                logged = []
                self.assertTrue(self.model.verify_download(
                    self.path, self.kms_etag, head=head,
                    log_fn=logged.append, key="k"))
                self.assertTrue(any("skipped" in line for line in logged),
                                logged)
                self.assertFalse(any("MISMATCH" in line for line in logged))

    def test_sse_s3_still_compares_the_etag(self):
        """AES256 (SSE-S3) ETags ARE MD5s: a wrong one must still fail."""
        head = {"ServerSideEncryption": "AES256"}
        self.assertFalse(self.model.verify_download(
            self.path, self.kms_etag, head=head))
        self.assertTrue(self.model.verify_download(
            self.path, hashlib.md5(self.data).hexdigest(), head=head))

    def test_a_kms_object_with_a_checksum_is_still_verified(self):
        """Skipping the ETag must not skip the checksum: that one is about
        the bytes whatever the encryption."""
        head = {"ServerSideEncryption": "aws:kms",
                "ChecksumCRC32": self._crc32(self.data)}
        self.assertTrue(self.model.verify_download(
            self.path, self.kms_etag, head=head))
        head["ChecksumCRC32"] = self._crc32(b"something else")
        self.assertFalse(self.model.verify_download(
            self.path, self.kms_etag, head=head))

    def test_a_multipart_kms_object_is_settled_by_its_part_checksums(self):
        """Per-part checksums decide; the composite KMS ETag must not be
        rebuilt from MD5s and compared, which can only ever mismatch."""
        first, second = self.data[:256], self.data[256:]
        client = _AttributesClient(attributes={"k": {"ObjectParts": {
            "Parts": [
                {"PartNumber": 1, "Size": len(first),
                 "ChecksumCRC32": self._crc32(first)},
                {"PartNumber": 2, "Size": len(second),
                 "ChecksumCRC32": self._crc32(second)}]}}})
        self.model._client = client
        head = {"ServerSideEncryption": "aws:kms",
                "ChecksumCRC32": "c29tZQ==-2"}
        logged = []
        self.assertTrue(self.model.verify_download(
            self.path, "f" * 32 + "-2", head=head, log_fn=logged.append,
            key="k"))
        self.assertTrue(any("CRC32, 2 parts" in line for line in logged),
                        logged)

    def test_a_multipart_kms_etag_without_part_checksums_is_skipped(self):
        first, second = self.data[:256], self.data[256:]
        client = _AttributesClient(attributes={"k": {"ObjectParts": {
            "Parts": [{"PartNumber": 1, "Size": len(first)},
                      {"PartNumber": 2, "Size": len(second)}]}}})
        self.model._client = client
        self.assertTrue(self.model.verify_download(
            self.path, "f" * 32 + "-2",
            head={"ServerSideEncryption": "aws:kms"}, key="k"))


class LocalFileEtagTests(unittest.TestCase):
    """What the pane comparison puts a local file against: the ETag the
    listing reported for its remote counterpart."""

    DATA = bytes(range(256)) * 5            # 1280 bytes

    def setUp(self):
        handle, self.path = tempfile.mkstemp()
        with os.fdopen(handle, "wb") as out:
            out.write(self.DATA)
        self.addCleanup(os.remove, self.path)

    def _composite(self, part):
        pieces = [self.DATA[i:i + part] for i in range(0, len(self.DATA), part)]
        return Model.composite_etag(
            [hashlib.md5(piece).digest() for piece in pieces])

    def test_a_plain_md5_settles_it_either_way(self):
        self.assertTrue(file_matches_etag(
            self.path, '"%s"' % hashlib.md5(self.DATA).hexdigest()))
        self.assertFalse(file_matches_etag(self.path, "0" * 32))

    def test_parts_are_cut_at_the_part_size(self):
        self.assertEqual(
            file_part_md5s(self.path, 512),
            [hashlib.md5(self.DATA[:512]).digest(),
             hashlib.md5(self.DATA[512:1024]).digest(),
             hashlib.md5(self.DATA[1024:]).digest()])
        # An exact multiple has no empty trailing part.
        self.assertEqual(len(file_part_md5s(self.path, 640)), 2)

    def test_a_multipart_etag_is_rebuilt_at_the_guessed_part_size(self):
        self.assertTrue(file_matches_etag(
            self.path, self._composite(512), part_sizes=[100, 512]))

    def test_a_multipart_etag_no_guess_reproduces_is_inconclusive(self):
        """Another part size gives the same bytes another ETag."""
        self.assertIsNone(file_matches_etag(
            self.path, self._composite(512), part_sizes=[640]))
        self.assertIsNone(file_matches_etag(self.path, self._composite(512)))

    def test_a_missing_or_malformed_etag_cannot_tell(self):
        for etag in ("", None, "short", "abc-x", "abc-0"):
            with self.subTest(etag=etag):
                self.assertIsNone(file_matches_etag(
                    self.path, etag, part_sizes=[512]))
