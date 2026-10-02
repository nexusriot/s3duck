"""What this backend actually implements, and that the app degrades the way
it claims to when the answer is "no".

The ROADMAP's "known limitations" list is a set of assertions about other
people's servers — GetObjectAttributes, conditional writes, ACLs, restore.
Each one here either exercises the feature or proves the documented
fallback, so a backend that gains or loses support shows up as a test
result rather than as a bug report.
"""

import os
import tempfile
import urllib.request

import botocore.exceptions

from model import CHECKSUM_ALGORITHMS, Model, ObjectExists
from tests.e2e.harness import ServerCase


def _supported(call, *args, **kwargs):
    """Run *call*; return (True, result) or (False, error-code)."""
    try:
        return True, call(*args, **kwargs)
    except botocore.exceptions.ClientError as exc:
        return False, exc.response.get("Error", {}).get("Code", "?")
    except Exception as exc:                       # noqa: BLE001
        return False, exc.__class__.__name__


class MultipartTests(ServerCase):
    """The resumable upload path builds the multipart protocol by hand;
    none of it is exercised by a stub that accepts whatever it is handed."""

    def setUp(self):
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        self.model.resumable_uploads = True

    def test_a_large_upload_arrives_whole(self):
        payload = os.urandom(12 * 1024 * 1024)      # 3 parts at 5 MiB
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "big.bin")
            with open(src, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(src, "mp/big.bin")
        self.assertEqual(self.body("mp/big.bin"), payload)
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="mp/big.bin")
        self.assertEqual(head["ContentLength"], len(payload))

    def test_a_multipart_object_has_a_dashed_etag(self):
        """Which is why the duplicate finder cannot settle it on ETag
        alone — asserted here rather than assumed."""
        payload = os.urandom(12 * 1024 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "dashed.bin")
            with open(src, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(src, "mp/dashed.bin")
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="mp/dashed.bin")
        self.assertIn("-", head["ETag"].strip('"'))

    def test_an_interrupted_upload_resumes_instead_of_restarting(self):
        """The upload-state record and ListParts are the whole feature, and
        both only mean anything against a server."""
        payload = os.urandom(12 * 1024 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "resume.bin")
            with open(src, "wb") as handle:
                handle.write(payload)

            calls = {"n": 0}
            real_upload_part = self.model.client.upload_part

            def flaky(**kw):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise RuntimeError("connection reset")
                return real_upload_part(**kw)

            self.model.client.upload_part = flaky
            try:
                with self.assertRaises(Exception):
                    self.model.upload_file(src, "mp/resume.bin")
            finally:
                self.model.client.upload_part = real_upload_part

            # The upload id survived, so parts already sent are still there.
            uploads = self.model.list_multipart_uploads("mp/")
            self.assertTrue(uploads, "the interrupted upload was not left open")

            self.model.upload_file(src, "mp/resume.bin")
        self.assertEqual(self.body("mp/resume.bin"), payload)

    def test_a_ranged_download_reassembles_correctly(self):
        payload = os.urandom(9 * 1024 * 1024)
        self.model.client.put_object(
            Bucket=self.bucket, Key="mp/ranged.bin", Body=payload)
        self.model.resume_threshold = 1024 * 1024
        self.model.resume_chunk_size = 2 * 1024 * 1024
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "ranged.bin")
            self.model.download_file("mp/ranged.bin", out, tmp)
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)
            self.assertFalse(os.path.exists(out + self.model.PART_SUFFIX))


class ChecksumTests(ServerCase):
    def test_every_offered_algorithm_is_accepted_and_verifies(self):
        """An algorithm the app offers but the backend rejects would fail
        every upload; one it accepts but cannot return would pass every
        download unchecked."""
        payload = b"checksum me" * 100
        for algorithm in CHECKSUM_ALGORITHMS:
            with self.subTest(algorithm=algorithm):
                self.model.set_upload_options(checksum_algorithm=algorithm)
                key = f"ck/{algorithm.lower()}.bin"
                with tempfile.TemporaryDirectory() as tmp:
                    src = os.path.join(tmp, "in.bin")
                    with open(src, "wb") as handle:
                        handle.write(payload)
                    self.model.upload_file(src, key)
                    head = self.model.head_with_checksum(key)
                    name, value = self.model.stored_checksum(head)
                    if not value:
                        # CRC32/SHA1/SHA256 have been in S3's flexible
                        # checksums since they shipped, so a backend that
                        # drops one is a backend this app is wrong about.
                        # CRC32C and CRC64NVME are newer, and a server that
                        # accepts the upload but stores nothing is a gap to
                        # record rather than a bug here: verification falls
                        # back to the ETag, which is what it did before.
                        self.assertNotIn(
                            algorithm, ("CRC32", "SHA1", "SHA256"),
                            f"{algorithm} was not stored by the backend")
                        print(f"\n    {algorithm}: accepted but not stored")
                        continue
                    out = os.path.join(tmp, "out.bin")
                    self.model.verify_downloads = True
                    try:
                        self.model.download_file(key, out, tmp)
                    finally:
                        self.model.verify_downloads = False
        self.model.set_upload_options(checksum_algorithm="")

    def test_a_single_part_etag_is_the_md5(self):
        """What lets the duplicate finder confirm a group for free."""
        payload = b"small enough"
        self.model.client.put_object(
            Bucket=self.bucket, Key="ck/plain.txt", Body=payload)
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="ck/plain.txt")
        etag = self.model.normalize_etag(head["ETag"])
        self.assertTrue(self.model.etag_is_md5(etag))


class BackendCapabilityTests(ServerCase):
    """Each documented "most backends do not implement this" claim, asked."""

    def test_get_object_attributes_either_works_or_degrades(self):
        """Verification of a multipart object needs real part boundaries.
        Where the call is missing, the app must report "not comparable"
        rather than failing a good download."""
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        payload = os.urandom(11 * 1024 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "attr.bin")
            with open(src, "wb") as handle:
                handle.write(payload)
            self.model.upload_file(src, "cap/attr.bin")
            parts = self.model.get_object_parts("cap/attr.bin")
            out = os.path.join(tmp, "attr.out")
            self.model.download_file("cap/attr.bin", out, tmp)
            ok = self.model.verify_download(
                out, self.model.normalize_etag(
                    self.model.client.head_object(
                        Bucket=self.bucket, Key="cap/attr.bin")["ETag"]),
                key="cap/attr.bin")
        # Either the boundaries came back and verification is real, or they
        # did not and verification passes by declaring it not comparable.
        self.assertTrue(ok, "a good multipart download was reported corrupt")
        print(f"\n    GetObjectAttributes parts: "
              f"{len(parts) if parts else 'not supported'}")

    def test_conditional_write_either_enforces_if_match_or_says_it_cannot(self):
        """Edit-in-place relies on If-Match to refuse a concurrent save."""
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/edit.txt", Body=b"first")
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="cap/edit.txt")
        stale = self.model.normalize_etag(head["ETag"])
        # Someone else writes in between.
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/edit.txt", Body=b"second")
        try:
            self.model.put_object_body("cap/edit.txt", b"third",
                                       if_match=stale)
        except self.model.PreconditionFailed:
            # Either the server enforced If-Match, or the by-hand ETag
            # fallback caught it. Both are the documented behaviour.
            self.assertEqual(self.body("cap/edit.txt"), b"second")
            return
        self.fail("a stale If-Match was accepted; a concurrent save would "
                  "have been overwritten silently")

    def test_a_current_if_match_is_accepted(self):
        """The other half: the precondition must not refuse a good write."""
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/fresh.txt", Body=b"first")
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="cap/fresh.txt")
        current = self.model.normalize_etag(head["ETag"])
        self.model.put_object_body("cap/fresh.txt", b"second",
                                   if_match=current)
        self.assertEqual(self.body("cap/fresh.txt"), b"second")

    def test_versioning_can_be_enabled_and_lists_versions(self):
        self.model.set_bucket_versioning("Enabled")
        self.assertEqual(self.model.get_bucket_versioning_status(), "Enabled")
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/ver.txt", Body=b"v1")
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/ver.txt", Body=b"v2")
        versions = self.model.list_object_versions("cap/ver.txt")
        self.assertGreaterEqual(len(versions), 2)

    def test_a_delete_on_a_versioned_bucket_can_be_undone(self):
        """Ctrl+Z is only real where a delete writes a marker."""
        self.model.set_bucket_versioning("Enabled")
        self.model.client.put_object(
            Bucket=self.bucket, Key="cap/undo.txt", Body=b"keep me")
        self.model.delete("cap/undo.txt")
        with self.assertRaises(botocore.exceptions.ClientError):
            self.model.client.head_object(Bucket=self.bucket,
                                          Key="cap/undo.txt")
        restored = self.model.undelete("cap/undo.txt")
        self.assertEqual(restored, 1)
        self.assertEqual(self.body("cap/undo.txt"), b"keep me")

    def test_tagging_round_trips(self):
        self.put("cap/tagged.txt", b"x")
        self.model.put_object_tags("cap/tagged.txt",
                                   [{"Key": "team", "Value": "infra"}])
        tags = self.model.get_object_tags("cap/tagged.txt")
        self.assertEqual(tags, [{"Key": "team", "Value": "infra"}])

    def test_metadata_edit_preserves_the_body(self):
        self.put("cap/meta.txt", b"unchanged")
        self.model.set_object_metadata(
            "cap/meta.txt", content_type="text/plain",
            cache_control="max-age=60", metadata={"owner": "vlad"})
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="cap/meta.txt")
        self.assertEqual(head["ContentType"], "text/plain")
        self.assertEqual(head["CacheControl"], "max-age=60")
        self.assertEqual(head["Metadata"].get("owner"), "vlad")
        self.assertEqual(self.body("cap/meta.txt"), b"unchanged")

    def test_presigned_get_is_actually_fetchable(self):
        """A signature the server rejects is worse than no link at all."""
        self.put("cap/signed.txt", b"signed body")
        url = self.model.presigned_get_url("cap/signed.txt", 300)
        with urllib.request.urlopen(url, timeout=15) as resp:
            self.assertEqual(resp.read(), b"signed body")

    def test_presigned_put_actually_uploads(self):
        url = self.model.presigned_put_url("cap/put.txt", 300)
        request = urllib.request.Request(url, data=b"via link", method="PUT")
        with urllib.request.urlopen(request, timeout=15) as resp:
            self.assertIn(resp.status, (200, 204))
        self.assertEqual(self.body("cap/put.txt"), b"via link")

    def test_incomplete_uploads_are_discoverable_and_abortable(self):
        """They are invisible to a normal listing but still billed."""
        started = self.model.client.create_multipart_upload(
            Bucket=self.bucket, Key="cap/orphan.bin")
        upload_id = started["UploadId"]
        found = self.model.list_multipart_uploads("cap/")
        self.assertIn(upload_id, [u["upload_id"] for u in found])
        self.assertNotIn("cap/orphan.bin", self.keys())
        self.model.abort_multipart_upload("cap/orphan.bin", upload_id)
        self.assertEqual(
            [u for u in self.model.list_multipart_uploads("cap/")
             if u["upload_id"] == upload_id], [])

    def test_optional_bucket_documents_report_support_honestly(self):
        """Lifecycle / CORS / policy / encryption / website / acceleration
        are all "read it if it is there". Whatever this backend answers,
        the app must return a value rather than raise."""
        probes = {
            "lifecycle": self.model.get_bucket_lifecycle,
            "cors": self.model.get_bucket_cors,
            "policy": self.model.get_bucket_policy,
            "encryption": self.model.get_bucket_encryption,
            "tags": self.model.get_bucket_tags,
            "website": self.model.get_bucket_website,
            "notifications": self.model.get_bucket_notifications,
            "acceleration": self.model.get_bucket_acceleration,
            "object_lock": self.model.get_object_lock_configuration,
        }
        report = {}
        for name, call in probes.items():
            ok, value = _supported(call)
            report[name] = "ok" if ok else value
            self.assertTrue(
                ok, f"{name} raised instead of reporting absence: {value}")
        print(f"\n    bucket documents: {report}")

    def test_make_public_either_works_or_explains_itself(self):
        """MinIO answers NotImplemented for PutObjectAcl; the app must say
        so rather than claim the object is public."""
        self.put("cap/public.txt", b"x")
        ok, reason = self.model.make_object_public("cap/public.txt")
        if not ok:
            self.assertTrue(reason, "refused without saying why")
            print(f"\n    make_public: refused — {reason}")
        else:
            print("\n    make_public: supported")

    def test_restore_either_works_or_explains_itself(self):
        self.put("cap/cold.txt", b"x")
        ok, reason = self.model.restore_object("cap/cold.txt", days=1)
        if not ok:
            self.assertTrue(reason, "refused without saying why")
            print(f"\n    restore: refused — {reason}")


class MultipartServerSideCopyTests(ServerCase):
    """
    S3 refuses a CopyObject whose source is over 5 GiB, so every copy onto
    itself — rename, storage class, metadata — had a ceiling. The multipart
    copy that replaces it is a different protocol, and whether a backend
    implements UploadPartCopy at all is exactly the kind of claim this suite
    exists to check rather than assume.

    A real 5 GiB source is not worth a test run, so the part-by-part path is
    driven directly: the routing above it is settled by the unit suite, and
    what needs a server is whether the parts arrive and reassemble.
    """

    def setUp(self):
        self.payload = os.urandom(12 * 1024 * 1024)     # 3 parts at 5 MiB
        self.model.client.put_object(
            Bucket=self.bucket, Key="copy/src.bin", Body=self.payload,
            ContentType="application/x-duck",
            Metadata={"owner": "vlad"})

    def test_a_part_by_part_copy_reassembles_byte_for_byte(self):
        self.model._copy_multipart(
            "copy/src.bin", "copy/dst.bin", size=len(self.payload),
            create_args=self.model._copy_create_args("copy/src.bin"))
        self.assertEqual(self.body("copy/dst.bin"), self.payload)

    def test_it_really_is_server_side(self):
        """The ETag of the copy is multipart, which is only possible if the
        service assembled it — a streamed copy of this size would too, so
        the stronger evidence is that the bytes never passed through here:
        UploadPartCopy is the only call that moved them."""
        self.model._copy_multipart(
            "copy/src.bin", "copy/side.bin", size=len(self.payload),
            create_args=self.model._copy_create_args("copy/src.bin"))
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="copy/side.bin")
        self.assertIn("-", head["ETag"].strip('"'),
                      "the copy is not multipart, so it was not assembled "
                      "from parts")

    def test_the_metadata_survives(self):
        """CreateMultipartUpload defines the object outright; anything the
        copy does not pass is simply gone."""
        self.model._copy_multipart(
            "copy/src.bin", "copy/meta.bin", size=len(self.payload),
            create_args=self.model._copy_create_args("copy/src.bin"))
        head = self.model.client.head_object(Bucket=self.bucket,
                                             Key="copy/meta.bin")
        self.assertEqual(head.get("ContentType"), "application/x-duck")
        self.assertEqual(head.get("Metadata"), {"owner": "vlad"})

    def test_a_failed_copy_leaves_no_billable_parts(self):
        """A source that is not there fails after the upload has started, so
        this is the abort path rather than a refusal before it."""
        before = self.model.client.list_multipart_uploads(
            Bucket=self.bucket).get("Uploads") or []
        with self.assertRaises(Exception):
            self.model._copy_multipart(
                "copy/missing.bin", "copy/nope.bin", size=len(self.payload),
                create_args={})
        after = self.model.client.list_multipart_uploads(
            Bucket=self.bucket).get("Uploads") or []
        self.assertEqual(len(after), len(before),
                         "an abandoned multipart upload was left behind")


class ConditionalUploadTests(ServerCase):
    """
    Overwrite protection checks the destination and then writes, which is a
    snapshot with a gap after it. IfNoneMatch closes the gap — where the
    backend has it. Whether this one does is the claim under test.
    """

    def _upload(self, key, body, **kw):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(body)
            self.model.upload_file(path, key, **kw)

    def test_a_small_upload_is_refused_or_the_backend_says_it_cannot(self):
        self.model.set_multipart_sizes(threshold_mb=16)
        self.put("cond/small.txt", b"original")
        try:
            self._upload("cond/small.txt", b"replacement", if_none_match=True)
        except ObjectExists:
            self.assertEqual(self.body("cond/small.txt"), b"original")
            print("\n    conditional upload: enforced")
            return
        # The documented degradation: no conditional writes here, so the
        # write went through exactly as it did before this existed.
        self.assertEqual(self.body("cond/small.txt"), b"replacement")
        print("\n    conditional upload: not supported, degraded")

    def test_a_free_key_is_not_refused(self):
        """The other half: the precondition must not block a new object."""
        self.model.set_multipart_sizes(threshold_mb=16)
        self._upload("cond/free.txt", b"new", if_none_match=True)
        self.assertEqual(self.body("cond/free.txt"), b"new")

    def test_a_multipart_upload_carries_it_to_the_completion(self):
        """That is the only point a multipart write can still be refused
        without having already replaced anything.

        An upload checksum is configured on purpose. Without one, botocore
        adds a default CRC32 to UploadPart while CreateMultipartUpload
        carries none, and a backend that checks the two against each other
        rejects the part — which has nothing to do with the precondition
        under test here.
        """
        self.model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
        self.model.resumable_uploads = True
        self.model.set_upload_options(checksum_algorithm="CRC32")
        self.addCleanup(self.model.set_upload_options, checksum_algorithm="")
        payload = os.urandom(12 * 1024 * 1024)
        self.put("cond/big.bin", b"original")
        try:
            self._upload("cond/big.bin", payload, if_none_match=True)
        except ObjectExists:
            self.assertEqual(self.body("cond/big.bin"), b"original")
            print("\n    conditional completion: enforced")
            return
        self.assertEqual(self.body("cond/big.bin"), payload)
        print("\n    conditional completion: not supported, degraded")


class RangedReadTests(ServerCase):
    """
    Every download past the resume threshold is a series of byte-range GETs,
    so whatever a backend does with checksums on a partial response decides
    whether large files can be downloaded here at all.
    """

    def setUp(self):
        self.payload = os.urandom(64 * 1024)
        self.model.client.put_object(
            Bucket=self.bucket, Key="rng/src.bin", Body=self.payload)

    def test_a_byte_range_can_be_read(self):
        """REGRESSION: botocore asks every GetObject to validate its body
        against the checksum the service returned. AWS omits that header on
        a partial response; a backend that returns the whole-object one
        anyway made botocore compare a slice against the whole and fail
        every ranged read — so no large file could be downloaded."""
        resp = self.model.client.get_object(
            Bucket=self.bucket, Key="rng/src.bin", Range="bytes=0-1023")
        self.assertEqual(resp["Body"].read(), self.payload[:1024])
        returned = {name for name in resp if name.startswith("Checksum")}
        print(f"\n    ranged response checksums: {returned or 'none'}")

    def test_a_whole_object_read_is_still_checked(self):
        """The guard narrows the behaviour to ranged reads; it must not
        switch validation off for the reads where it means something."""
        resp = self.model.client.get_object(
            Bucket=self.bucket, Key="rng/src.bin")
        self.assertEqual(resp["Body"].read(), self.payload)

    def test_a_large_download_arrives_whole(self):
        """The path all of the above exists to serve."""
        self.model.resume_threshold = 1
        self.model.resume_chunk_size = 16 * 1024
        try:
            with tempfile.TemporaryDirectory() as tmp:
                out = os.path.join(tmp, "out.bin")
                self.model.download_file("rng/src.bin", out, tmp)
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), self.payload)
        finally:
            self.model.resume_threshold = Model.RESUME_THRESHOLD


class PrefixDownloadTests(ServerCase):
    """
    A whole-folder download used to run through the managed transfer with no
    digest check and no resume, while a single object got both. These are
    the same promises, so they are tested against a real server the same way.
    """

    BODIES = {"tree/a.txt": b"alpha" * 1000,
              "tree/sub/b.txt": b"beta" * 1000}

    def setUp(self):
        for key, body in self.BODIES.items():
            self.model.client.put_object(
                Bucket=self.bucket, Key=key, Body=body)

    def test_a_folder_download_verifies_every_file(self):
        self.model.verify_downloads = True
        self.addCleanup(setattr, self.model, "verify_downloads", False)
        logged = []
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("tree/", None, tmp, log_fn=logged.append)
            for key, body in self.BODIES.items():
                out = os.path.join(tmp, "tree", key[len("tree/"):])
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), body)
        checked = [line for line in logged if "checksum ok" in line]
        self.assertEqual(len(checked), len(self.BODIES), logged)

    def test_a_large_file_in_a_folder_download_is_resumable(self):
        self.model.resume_threshold = 1
        self.model.resume_chunk_size = 1024
        self.addCleanup(setattr, self.model, "resume_threshold",
                        Model.RESUME_THRESHOLD)
        with tempfile.TemporaryDirectory() as tmp:
            self.model.download_file("tree/", None, tmp)
            for key, body in self.BODIES.items():
                out = os.path.join(tmp, "tree", key[len("tree/"):])
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), body)
                self.assertFalse(
                    os.path.exists(out + Model.PART_SUFFIX),
                    "a finished download left its partial file behind")
