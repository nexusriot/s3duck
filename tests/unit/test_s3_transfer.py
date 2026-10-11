"""s3/upload.py, download.py, copy.py, delete.py."""

import botocore.session
import io
import os
import shutil
import tempfile
import threading
import types
import unittest
import unittest.mock
from main_window import Worker
from model import (
    DeleteRefused, MAX_LOCAL_NAME_BYTES, Model, ObjectExists, ReadOnlyError,
    TransferCancelled, client_accepts, drop_unsendable_checksum_args,
    local_name_is_writable, plan_prefix_download, prefix_of, safe_local_path,
)
from tests.support import (
    FakeS3Client, _ConditionalPutClient, _CopyPartClient, _FakePaginator,
    _MultipartClient, _PrefixDownloadClient, _RangedClient, _client_error,
    _dt, _ensure_qapp, make_model,
)


class DeleteBucketPurgesVersionsTests(unittest.TestCase):
    """REGRESSION: recursive bucket delete must also purge noncurrent
    versions and delete markers, or AWS refuses DeleteBucket."""

    def test_versions_and_markers_are_deleted(self):
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "cur.txt"}]}],
            versions_pages=[{
                "Versions": [
                    {"Key": "cur.txt", "VersionId": "v1"},
                    {"Key": "old.txt", "VersionId": "v0"},
                ],
                "DeleteMarkers": [
                    {"Key": "gone.txt", "VersionId": "dm0"},
                ],
            }],
        )
        # Bypass network-bound client construction.
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.delete_bucket_recursive("mybucket")

        deleted = []
        for kw in c.calls_of("delete_objects"):
            deleted.extend(kw["Delete"]["Objects"])

        # current object (no VersionId) purged in pass 1
        self.assertIn({"Key": "cur.txt"}, deleted)
        # versions + delete markers purged in pass 2 (with VersionId)
        self.assertIn({"Key": "cur.txt", "VersionId": "v1"}, deleted)
        self.assertIn({"Key": "old.txt", "VersionId": "v0"}, deleted)
        self.assertIn({"Key": "gone.txt", "VersionId": "dm0"}, deleted)
        # and the bucket itself is finally removed
        self.assertEqual(len(c.calls_of("delete_bucket")), 1)
class DeleteBucketUsesBoundClientTests(unittest.TestCase):
    """REGRESSION: the recursive delete emptied the bucket and then issued
    DeleteBucket on a profile/root client, which raises PermanentRedirect for
    a bucket in another region — leaving the bucket wiped but still present."""

    def _model_with_split_clients(self):
        m = make_model(bucket="")
        bound = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "k.txt"}]}],
            versions_pages=[{}],
            mpu_pages=[{}],
        )
        root = FakeS3Client()
        m._try_bind_bucket = lambda name: (bound, "ep", "eu-north-1", True)
        m._make_client = lambda **kw: root
        return m, bound, root

    def test_recursive_delete_uses_the_bound_client(self):
        m, bound, root = self._model_with_split_clients()
        m.delete_bucket_recursive("offregion")
        self.assertEqual(
            bound.calls_of("delete_bucket"), [{"Bucket": "offregion"}])
        self.assertEqual(root.calls_of("delete_bucket"), [])

    def test_plain_delete_uses_the_bound_client(self):
        m = make_model(bucket="")
        bound = FakeS3Client(list_pages=[{}])
        root = FakeS3Client()
        m._try_bind_bucket = lambda name: (bound, "ep", "eu-north-1", True)
        m._make_client = lambda **kw: root
        m.delete_bucket("offregion")
        self.assertEqual(
            bound.calls_of("delete_bucket"), [{"Bucket": "offregion"}])
        self.assertEqual(root.calls_of("delete_bucket"), [])

    def test_recursive_delete_honours_cancellation(self):
        m, bound, _root = self._model_with_split_clients()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            m.delete_bucket_recursive("offregion", cancel_event=cancel)
        # nothing destructive may run once cancellation is already requested
        self.assertEqual(bound.calls_of("delete_objects"), [])
        self.assertEqual(bound.calls_of("delete_bucket"), [])
class BatchDeleteTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_prefix_delete_uses_one_batched_call(self):
        keys = [f"dir/f{i}.txt" for i in range(5)]
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": k, "Size": 1} for k in keys]}])
        self.m._client = c
        self.m.delete("dir/")
        batches = c.calls_of("delete_objects")
        self.assertEqual(len(batches), 1)
        self.assertEqual(
            [o["Key"] for o in batches[0]["Delete"]["Objects"]], keys)
        self.assertEqual(c.calls_of("delete_object"), [])  # no per-object calls

    def test_single_object_uses_delete_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.delete("dir/f.txt")
        self.assertEqual(
            c.calls_of("delete_object"), [{"Bucket": "b", "Key": "dir/f.txt"}])
        self.assertEqual(c.calls_of("delete_objects"), [])

    def test_batches_are_chunked(self):
        keys = [f"dir/f{i}.txt" for i in range(2500)]
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": k, "Size": 1} for k in keys]}])
        self.m._client = c
        self.m.delete("dir/")
        sizes = [len(kw["Delete"]["Objects"]) for kw in c.calls_of("delete_objects")]
        self.assertEqual(sizes, [1000, 1000, 500])

    def test_falls_back_when_batch_delete_unsupported(self):
        class NoBatch(FakeS3Client):
            def delete_objects(self, **kw):
                self.calls.append(("delete_objects", kw))
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "DeleteObjects",
                )

        c = NoBatch(list_pages=[{"Contents": [
            {"Key": "dir/a", "Size": 1}, {"Key": "dir/b", "Size": 1}]}])
        self.m._client = c
        self.m.delete("dir/")
        self.assertEqual(
            [kw["Key"] for kw in c.calls_of("delete_object")], ["dir/a", "dir/b"])

    def test_per_key_refusals_are_not_reported_as_a_successful_delete(self):
        """DeleteObjects answers 200 with the refusals in ``Errors``; with
        Quiet set that is the only thing the body carries. Ignoring it turned
        an object-lock or AccessDenied refusal into a green queue row."""
        class Refusing(FakeS3Client):
            def delete_objects(self, **kw):
                self.calls.append(("delete_objects", kw))
                return {"Errors": [
                    {"Key": "dir/a", "Code": "AccessDenied",
                     "Message": "Access Denied"}]}

        c = Refusing(list_pages=[{"Contents": [
            {"Key": "dir/a", "Size": 1}, {"Key": "dir/b", "Size": 1}]}])
        self.m._client = c
        logged = []
        with self.assertRaises(DeleteRefused) as caught:
            self.m.delete("dir/", log_fn=logged.append)
        self.assertIn("dir/a", str(caught.exception))
        self.assertIn("AccessDenied", str(caught.exception))
        self.assertTrue(any("refused" in line for line in logged))

    def test_an_empty_errors_list_is_still_a_success(self):
        class Quiet(FakeS3Client):
            def delete_objects(self, **kw):
                self.calls.append(("delete_objects", kw))
                return {"Errors": []}

        c = Quiet(list_pages=[{"Contents": [{"Key": "dir/a", "Size": 1}]}])
        self.m._client = c
        self.assertTrue(self.m.delete("dir/"))

    def test_raise_on_delete_errors_summarises_a_long_list(self):
        errors = [{"Key": f"k{i}", "Code": "AccessDenied"} for i in range(9)]
        with self.assertRaises(DeleteRefused) as caught:
            Model.raise_on_delete_errors({"Errors": errors})
        message = str(caught.exception)
        self.assertIn("9 object(s)", message)
        self.assertIn("and 4 more", message)

    def test_raise_on_delete_errors_accepts_a_bodyless_response(self):
        Model.raise_on_delete_errors(None)
        Model.raise_on_delete_errors({})

    def test_purging_a_bucket_stops_when_a_key_is_refused(self):
        """Without this the purge logged every object as deleted and the
        DeleteBucket that follows failed with an unexplained BucketNotEmpty."""
        class Refusing(FakeS3Client):
            def delete_objects(self, **kw):
                self.calls.append(("delete_objects", kw))
                return {"Errors": [{"Key": "a", "Code": "AccessDenied"}]}

        c = Refusing(list_pages=[{"Contents": [{"Key": "a", "Size": 1}]}])
        with self.assertRaises(DeleteRefused):
            self.m._purge_bucket_contents("b", c)
        self.assertEqual(c.calls_of("delete_bucket"), [])

    def test_prefix_delete_is_cancellable(self):
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "dir/a", "Size": 1}]}])
        self.m._client = c
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self.m.delete("dir/", cancel_event=cancel)
        self.assertEqual(c.calls_of("delete_objects"), [])
class EmptyBucketTests(unittest.TestCase):
    def test_contents_purged_but_bucket_kept(self):
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "a.txt"}]}],
            versions_pages=[{"Versions": [{"Key": "a.txt", "VersionId": "v1"}]}],
            mpu_pages=[{"Uploads": [
                {"Key": "half.bin", "UploadId": "u1", "Initiated": _dt(1)}]}],
        )
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.empty_bucket("mybucket")

        deleted = []
        for kw in c.calls_of("delete_objects"):
            deleted.extend(kw["Delete"]["Objects"])
        self.assertIn({"Key": "a.txt"}, deleted)
        self.assertIn({"Key": "a.txt", "VersionId": "v1"}, deleted)
        self.assertEqual(len(c.calls_of("abort_multipart_upload")), 1)
        # the bucket itself must survive
        self.assertEqual(c.calls_of("delete_bucket"), [])

    def test_current_prefix_is_reset_for_the_open_bucket(self):
        m = make_model(bucket="mybucket")
        m.current_folder = "deep/prefix/"
        c = FakeS3Client(list_pages=[{}], versions_pages=[{}], mpu_pages=[{}])
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m.empty_bucket("mybucket")
        self.assertEqual(m.current_folder, "")
        self.assertEqual(m.bucket, "mybucket")  # still inside it

    def test_rejects_empty_name(self):
        with self.assertRaises(ValueError):
            make_model().empty_bucket("")
class UploadOptionsTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_default_sends_no_extra_args(self):
        self.assertEqual(self.m.upload_extra_args, {})

    def test_storage_class_and_sse(self):
        args = self.m.set_upload_options(storage_class="GLACIER", sse="AES256")
        self.assertEqual(args, {
            "StorageClass": "GLACIER", "ServerSideEncryption": "AES256"})

    def test_standard_class_is_not_pinned(self):
        self.assertEqual(self.m.set_upload_options(storage_class="STANDARD"), {})

    def test_kms_key_only_with_kms_mode(self):
        self.assertEqual(
            self.m.set_upload_options(sse="aws:kms", kms_key_id="key-1"),
            {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "key-1"},
        )
        # a key without kms mode is meaningless and must be dropped
        self.assertEqual(
            self.m.set_upload_options(sse="AES256", kms_key_id="key-1"),
            {"ServerSideEncryption": "AES256"},
        )

    def test_extra_args_reach_upload_file(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_upload_options(storage_class="STANDARD_IA")
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.txt")
            with open(path, "w") as handle:
                handle.write("x")
            self.m.upload_file(path, "k.txt")
        self.assertEqual(
            c.calls_of("upload_file")[0]["kwargs"]["ExtraArgs"],
            {"StorageClass": "STANDARD_IA", "ContentType": "text/plain"},
        )
class UndeleteTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(versions_pages=[{
            "DeleteMarkers": [
                {"Key": "dir/a.txt", "VersionId": "dm-new", "IsLatest": True},
                {"Key": "dir/a.txt", "VersionId": "dm-old", "IsLatest": False},
                {"Key": "dir/b.txt", "VersionId": "dm-b", "IsLatest": True},
            ],
            "Versions": [{"Key": "dir/a.txt", "VersionId": "v1"}],
        }])

    def test_single_key_removes_only_its_latest_marker(self):
        c = self._client()
        self.m._client = c
        restored = self.m.undelete("dir/a.txt")
        self.assertEqual(restored, 1)
        self.assertEqual(
            c.calls_of("delete_object"),
            [{"Bucket": "b", "Key": "dir/a.txt", "VersionId": "dm-new"}],
        )

    def test_prefix_restores_everything_underneath(self):
        c = self._client()
        self.m._client = c
        restored = self.m.undelete("dir/")
        self.assertEqual(restored, 2)
        keys = {kw["Key"] for kw in c.calls_of("delete_object")}
        self.assertEqual(keys, {"dir/a.txt", "dir/b.txt"})

    def test_unversioned_bucket_restores_nothing(self):
        self.m._client = FakeS3Client(versions_pages=[{}])
        self.assertEqual(self.m.undelete("dir/a.txt"), 0)

    def test_backend_without_versions_api(self):
        class NoVersions(FakeS3Client):
            def get_paginator(self, name):
                if name == "list_object_versions":
                    raise botocore.exceptions.ClientError(
                        {"Error": {"Code": "NotImplemented", "Message": "no"}},
                        "ListObjectVersions")
                return super().get_paginator(name)

        self.m._client = NoVersions()
        self.assertEqual(self.m.undelete("a.txt"), 0)

    def test_requires_a_bucket(self):
        m = make_model(bucket="")
        with self.assertRaises(ValueError):
            m.undelete("a.txt")
class CrossLocationCopyTests(unittest.TestCase):
    """CopyObject is server-side and cannot reach a bucket in another region
    or account; those copies must stream through instead of failing."""

    def setUp(self):
        self.m = make_model(bucket="src")

    class Client(FakeS3Client):
        def __init__(self, copy_error=None, **kw):
            super().__init__(**kw)
            self._copy_error = copy_error
            self.uploaded = []

        def copy_object(self, **kw):
            self.calls.append(("copy_object", kw))
            if self._copy_error is not None:
                raise self._copy_error
            return {}

        def get_object(self, **kw):
            self.calls.append(("get_object", kw))
            return {"Body": io.BytesIO(b"payload"), "ContentType": "text/plain",
                    "Metadata": {"owner": "vlad"}}

        def upload_fileobj(self, body, bucket, key, **kw):
            self.uploaded.append((bucket, key, body.read(), kw.get("ExtraArgs")))

    @staticmethod
    def _error(code):
        return botocore.exceptions.ClientError(
            {"Error": {"Code": code, "Message": "nope"}}, "CopyObject")

    def test_same_bucket_copy_stays_server_side(self):
        c = self.Client()
        self.m._client = c
        self.m.copy_object("a.txt", "b.txt")
        self.assertEqual(len(c.calls_of("copy_object")), 1)
        self.assertEqual(c.uploaded, [])

    def test_cross_location_error_falls_back_to_streaming(self):
        c = self.Client(copy_error=self._error("InvalidRequest"))
        self.m._client = c
        self.m._try_bind_bucket = lambda name: (c, "ep", "eu-north-1", True)
        logs = []
        self.m.copy_object("a.txt", "a.txt", dst_bucket="other",
                           log_fn=logs.append)
        self.assertEqual(len(c.uploaded), 1)
        bucket, key, data, extra = c.uploaded[0]
        self.assertEqual((bucket, key, data), ("other", "a.txt", b"payload"))
        # content type and user metadata survive the streamed copy
        self.assertEqual(extra["ContentType"], "text/plain")
        self.assertEqual(extra["Metadata"], {"owner": "vlad"})
        self.assertTrue(any("streaming" in line for line in logs))

    def test_permanent_redirect_cross_bucket_streams(self):
        c = self.Client(copy_error=self._error("PermanentRedirect"))
        self.m._client = c
        self.m._try_bind_bucket = lambda name: (c, "ep", "eu-north-1", True)
        self.m.copy_object("a.txt", "a.txt", dst_bucket="other")
        self.assertEqual(len(c.uploaded), 1)

    def test_same_bucket_redirect_still_rebinds_instead_of_streaming(self):
        c = self.Client(copy_error=self._error("PermanentRedirect"))
        self.m._client = c
        rebound = []
        self.m.rebind_bucket = lambda log_fn=None: rebound.append(True)
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "b.txt")  # same bucket
        self.assertEqual(rebound, [True])
        self.assertEqual(c.uploaded, [])

    def test_unrelated_error_is_not_swallowed(self):
        c = self.Client(copy_error=self._error("AccessDenied"))
        self.m._client = c
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "a.txt", dst_bucket="other")
        self.assertEqual(c.uploaded, [])

    def test_streaming_can_be_disabled(self):
        c = self.Client(copy_error=self._error("InvalidRequest"))
        self.m._client = c
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("a.txt", "a.txt", dst_bucket="other",
                               allow_stream=False)
class MultipartServerSideCopyTests(unittest.TestCase):
    """
    S3 refuses a CopyObject whose source is over 5 GiB. Every copy onto
    itself — rename, storage class, metadata, Content-Type — went through
    that one call, so each of them failed outright on a large object while
    the cross-bucket case quietly degraded to pulling the bytes through this
    machine.
    """

    HUGE = Model.MAX_SINGLE_COPY_BYTES + 1

    def setUp(self):
        self.m = make_model(bucket="bkt")
        self.m.transfer_concurrency = 1       # deterministic part order

    @staticmethod
    def _too_big():
        return botocore.exceptions.ClientError(
            {"Error": {"Code": "InvalidRequest",
                       "Message": "The specified copy source is larger than "
                                  "the maximum allowable size for a copy "
                                  "source: 5368709120"}},
            "CopyObject")

    def test_part_size_never_goes_below_the_floor(self):
        self.assertEqual(Model.copy_part_size(1), Model.MIN_COPY_PART_BYTES)
        self.assertEqual(Model.copy_part_size(10, preferred=1024),
                         Model.MIN_COPY_PART_BYTES)

    def test_part_size_grows_so_the_object_fits_in_ten_thousand_parts(self):
        """A 5 TiB object at the 5 MiB floor would need a million parts."""
        size = 5 * 1024 ** 4
        part = Model.copy_part_size(size, preferred=8 * 1024 * 1024)
        self.assertLessEqual((size + part - 1) // part, Model.MAX_MULTIPART_PARTS)

    def test_a_configured_chunk_size_is_honoured_when_it_fits(self):
        self.assertEqual(
            Model.copy_part_size(100 * 1024 * 1024, preferred=32 * 1024 * 1024),
            32 * 1024 * 1024)

    def test_a_known_oversized_source_skips_the_doomed_single_copy(self):
        """THE POINT: the listing already knew the size, so the 5 GiB refusal
        does not have to be bought with a request."""
        c = _CopyPartClient(source_size=self.HUGE)
        self.m._client = c
        self.m.copy_object("big.bin", "copy.bin", size=self.HUGE)
        self.assertEqual(c.calls_of("copy_object"), [])
        self.assertEqual(len(c.completed), 1)

    def test_an_oversized_source_is_copied_in_parts_server_side(self):
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big())
        self.m._client = c
        self.m.copy_object("big.bin", "copy.bin")

        self.assertEqual(len(c.created), 1, "no multipart upload started")
        part = Model.copy_part_size(self.HUGE, self.m.multipart_chunksize_bytes)
        expected = (self.HUGE + part - 1) // part
        self.assertEqual(len(c.ranges), expected)
        # Contiguous, inclusive, and stopping at the last byte of the object.
        first, last = c.ranges[0], c.ranges[-1]
        self.assertEqual(first[1], f"bytes=0-{part - 1}")
        self.assertEqual(last[1], f"bytes={(expected - 1) * part}-{self.HUGE - 1}")
        self.assertEqual(first[2], {"Bucket": "bkt", "Key": "big.bin"})

    def test_the_parts_are_completed_in_order(self):
        """run_parallel finishes them in whatever order it likes; S3 rejects
        a completion whose part numbers are not ascending."""
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big())
        self.m._client = c
        self.m.transfer_concurrency = 4
        self.m.copy_object("big.bin", "copy.bin")
        numbers = [p["PartNumber"]
                   for p in c.completed[0]["MultipartUpload"]["Parts"]]
        self.assertEqual(numbers, sorted(numbers))
        self.assertEqual(numbers[0], 1)

    def test_no_bytes_pass_through_this_process(self):
        """The whole point of the multipart route: it is still server-side,
        so it must not reach for the streaming fallback."""
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big())
        self.m._client = c
        self.m.copy_object("big.bin", "copy.bin")
        self.assertEqual(c.calls_of("get_object"), [])

    def test_a_failed_part_aborts_the_upload(self):
        """Nothing here is resumable — the parts are copies the service made,
        not bytes this machine still holds — so leaving them would bill for
        an upload nobody is coming back to finish."""
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big(),
                            fail_on_part=1)
        self.m._client = c
        with self.assertRaises(RuntimeError):
            self.m.copy_object("big.bin", "copy.bin")
        self.assertEqual([a["UploadId"] for a in c.aborted], ["mpu-1"])
        self.assertEqual(c.completed, [])

    def test_the_source_metadata_is_carried_onto_the_new_object(self):
        """CopyObject preserves metadata by itself; a multipart upload defines
        the object outright, so anything not passed here is lost."""
        c = _CopyPartClient(
            source_size=self.HUGE, copy_error=self._too_big(),
            head={"ContentType": "text/csv", "CacheControl": "max-age=60",
                  "Metadata": {"owner": "vlad"}})
        self.m._client = c
        self.m.copy_object("big.csv", "copy.csv")
        _bucket, _key, kw = c.created[0]
        self.assertEqual(kw.get("ContentType"), "text/csv")
        self.assertEqual(kw.get("CacheControl"), "max-age=60")
        self.assertEqual(kw.get("Metadata"), {"owner": "vlad"})

    def test_a_small_object_refused_for_another_reason_is_not_rerouted(self):
        """REGRESSION GUARD: InvalidRequest is also what a copy onto itself
        that changes nothing returns. Measuring the source is what tells the
        two apart; matching the code alone would reroute both."""
        c = _CopyPartClient(source_size=1024, copy_error=self._too_big())
        self.m._client = c
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.copy_object("small.bin", "copy.bin", allow_stream=False)
        self.assertEqual(c.created, [])

    def test_storage_class_of_a_huge_object_is_changed_in_parts(self):
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big(),
                            head={"ContentType": "text/csv",
                                  "StorageClass": "STANDARD_IA"})
        self.m._client = c
        self.assertTrue(self.m.change_storage_class("big.csv", "GLACIER"))
        _bucket, key, kw = c.created[0]
        self.assertEqual(key, "big.csv", "did not copy onto itself")
        self.assertEqual(kw.get("StorageClass"), "GLACIER",
                         "the source's class won over the requested one")
        self.assertEqual(kw.get("ContentType"), "text/csv")

    def test_metadata_of_a_huge_object_is_replaced_in_parts(self):
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big())
        self.m._client = c
        self.m.set_object_metadata("big.csv", content_type="text/csv",
                                   metadata={"owner": "vlad"})
        _bucket, key, kw = c.created[0]
        self.assertEqual(key, "big.csv")
        self.assertEqual(kw.get("ContentType"), "text/csv")
        self.assertEqual(kw.get("Metadata"), {"owner": "vlad"})
        self.assertNotIn("MetadataDirective", kw,
                         "a multipart upload has no MetadataDirective")
        self.assertNotIn("CopySource", kw)

    def test_an_already_correct_class_is_a_no_op_even_on_a_huge_object(self):
        """REGRESSION GUARD: an object already in the requested class also
        answers InvalidRequest. Rewriting a 50 GiB object in parts to change
        nothing is an expensive way to say no-op."""
        c = _CopyPartClient(source_size=self.HUGE, copy_error=self._too_big(),
                            head={"StorageClass": "GLACIER"})
        self.m._client = c
        self.assertFalse(self.m.change_storage_class("big.csv", "GLACIER"))
        self.assertEqual(c.created, [], "rewrote the object to change nothing")

    def test_a_huge_cross_region_copy_still_falls_back_to_streaming(self):
        """The size route and the location route are independent: a bucket
        this client cannot reach refuses the multipart copy exactly as it
        refuses the single one."""
        c = _CopyPartClient(source_size=self.HUGE)

        def _refuse(**kw):
            c.calls.append(("create_multipart_upload", kw))
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "PermanentRedirect", "Message": "elsewhere"}},
                "CreateMultipartUpload")

        c.create_multipart_upload = _refuse
        c.get_object = lambda **kw: {"Body": io.BytesIO(b"payload")}
        uploaded = []
        c.upload_fileobj = (lambda body, bucket, key, **kw:
                            uploaded.append((bucket, key)))
        self.m._client = c
        self.m._try_bind_bucket = lambda name: (c, "ep", "eu-north-1", True)
        self.m.copy_object("big.bin", "copy.bin", dst_bucket="other",
                           size=self.HUGE)
        self.assertEqual(uploaded, [("other", "copy.bin")])

    def test_a_prefix_copy_passes_the_size_it_already_listed(self):
        """REGRESSION GUARD: a folder copy that re-discovers every size by
        being refused pays a wasted request per large object."""
        sizes = {}
        self.m.get_keys = lambda prefix, log_fn=None: [
            ("src/big.bin", self.HUGE), ("src/small.bin", 10)]
        self.m.copy_object = (
            lambda key, dst, dst_bucket=None, log_fn=None, size=None,
            cancel_event=None: sizes.__setitem__(key, size))
        self.m.copy_prefix("src/", "dst/")
        self.assertEqual(sizes, {"src/big.bin": self.HUGE,
                                 "src/small.bin": 10})
class ResumableDownloadTests(unittest.TestCase):
    """REGRESSION-proofing: a large download that dies part-way must resume
    from the bytes already on disk instead of starting over."""

    def _model(self, payload, fail_on_chunk=None):
        m = make_model(bucket="b")
        m.resume_chunk_size = 1024
        m.resume_threshold = 1
        m.parallel_files = 1          # deterministic chunk order
        m._client = _RangedClient(payload, fail_on_chunk)
        return m

    def test_full_download_assembles_the_object(self):
        payload = os.urandom(4096)
        m = self._model(payload)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            m.download_file("big.bin", out, tmp)
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)
            # the sidecar and part file are cleaned up on success
            self.assertEqual(os.listdir(tmp), ["big.bin"])

    def test_interrupted_download_resumes_from_the_partial(self):
        payload = os.urandom(4096)
        m = self._model(payload, fail_on_chunk=2048)   # 3rd chunk explodes
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(RuntimeError):
                m.download_file("big.bin", out, tmp)
            part = out + Model.PART_SUFFIX
            self.assertTrue(os.path.exists(part))
            self.assertTrue(os.path.exists(part + ".meta"))
            first_pass = len(m._client.ranges)

            m._client.ranges.clear()
            m.download_file("big.bin", out, tmp)      # resume
            with open(out, "rb") as handle:
                self.assertEqual(handle.read(), payload)
            # only the chunks that were missing are re-fetched
            self.assertLess(len(m._client.ranges), first_pass)
            self.assertEqual(
                {start for start, _e in m._client.ranges}, {2048, 3072})

    def test_changed_object_discards_the_stale_partial(self):
        payload = os.urandom(4096)
        m = self._model(payload, fail_on_chunk=2048)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(RuntimeError):
                m.download_file("big.bin", out, tmp)
            # the object is replaced remotely -> a different ETag
            m._client = _RangedClient(os.urandom(4096))
            m.download_file("big.bin", out, tmp)
            starts = {start for start, _e in m._client.ranges}
            self.assertEqual(starts, {0, 1024, 2048, 3072})  # all re-fetched

    def test_small_files_skip_the_ranged_path(self):
        m = make_model(bucket="b")
        m.resume_threshold = 10 * 1024
        c = FakeS3Client(head_object_resp={"ContentLength": 10, "ETag": '"x"'})
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("small.bin", os.path.join(tmp, "small.bin"), tmp)
        self.assertEqual(len(c.calls_of("download_file")), 1)

    def test_verification_failure_after_ranged_download(self):
        payload = os.urandom(2048)
        m = self._model(payload)
        m.verify_downloads = True
        # corrupt what the server hands back so the ETag will not match
        m._client.payload = payload
        original_head = m._client.head_object
        m._client.head_object = lambda **kw: {
            "ContentLength": len(payload), "ETag": '"' + "0" * 32 + '"'}
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "big.bin")
            with self.assertRaises(Exception) as ctx:
                m.download_file("big.bin", out, tmp)
            self.assertIn("Checksum mismatch", str(ctx.exception))
        m._client.head_object = original_head
class RangedResponseChecksumTests(unittest.TestCase):
    """
    REGRESSION (every large download, on some backends): botocore 1.36 and
    newer set ChecksumMode=ENABLED on every GetObject and then compare the
    body they receive against the checksum the service returned. AWS omits
    that header on a partial response — a whole-object digest cannot
    describe a slice of the object — but a backend that returns it anyway
    makes botocore compare the slice against the whole and raise
    FlexibleChecksumError. Every ranged read then fails, which means no file
    over the resume threshold can be downloaded at all.
    """

    def _emit(self, headers):
        """Fire the real event on a real client and report the context."""
        m = make_model(bucket="b")
        context = {"checksum": {"response_algorithms": ["crc32"]}}
        m.client.meta.events.emit(
            "before-call.s3.GetObject",
            model=m.client.meta.service_model.operation_model("GetObject"),
            params={"headers": headers},
            request_signer=None,
            context=context,
        )
        return context["checksum"]

    def test_a_ranged_get_does_not_validate_against_a_whole_object_digest(self):
        self.assertNotIn("response_algorithms",
                         self._emit({"Range": "bytes=0-1023"}))

    def test_the_header_is_matched_however_it_is_spelled(self):
        """botocore normalises header case on its own schedule; a guard that
        only sees one spelling is a guard that stops working quietly."""
        self.assertNotIn("response_algorithms",
                         self._emit({"range": "bytes=0-1023"}))

    def test_a_whole_object_get_still_validates(self):
        """The check is worth keeping where it means something: this must
        narrow the behaviour to ranged reads, not switch it off."""
        self.assertEqual(self._emit({})["response_algorithms"], ["crc32"])

    def test_a_request_without_a_checksum_context_is_left_alone(self):
        m = make_model(bucket="b")
        context = {}
        m.client.meta.events.emit(
            "before-call.s3.GetObject",
            model=m.client.meta.service_model.operation_model("GetObject"),
            params={"headers": {"Range": "bytes=0-9"}},
            request_signer=None,
            context=context,
        )
        self.assertEqual(context, {})
class DownloadTraversalGuardTests(unittest.TestCase):
    """REGRESSION: keys containing '..' segments must never write outside the
    download target directory."""

    def test_unsafe_keys_are_skipped(self):
        m = make_model(bucket="b")
        c = FakeS3Client(list_pages=[{
            "Contents": [
                {"Key": "docs/ok.txt", "Size": 1},
                {"Key": "docs/../../evil.txt", "Size": 1},
                {"Key": "docs/../evil2/", "Size": 0},
            ],
        }])
        m._client = c
        logs = []
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("docs/", None, tmp, log_fn=logs.append)
            base = os.path.abspath(os.path.join(tmp, "docs"))
            downloaded = [kw["args"][2] for kw in c.calls_of("download_file")]
            self.assertEqual(len(downloaded), 1)
            self.assertEqual(os.path.abspath(downloaded[0]),
                             os.path.join(base, "ok.txt"))
            # the placeholder dir with '..' must not be created outside base
            self.assertFalse(os.path.exists(os.path.join(tmp, "evil2")))
        self.assertTrue(any("unsafe" in ln for ln in logs))

    def test_a_single_object_may_not_be_written_outside_the_folder(self):
        """The whole-prefix path is filtered by plan_prefix_download, but a
        single object arrives with its destination already chosen. This is
        the backstop for every caller that builds one."""
        m = make_model(bucket="b")
        c = FakeS3Client()
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            outside = os.path.join(os.path.dirname(tmp), "escaped.txt")
            with self.assertRaises(ValueError) as caught:
                m.download_file("k.txt", outside, tmp)
            self.assertIn("outside", str(caught.exception))
            self.assertEqual(c.calls_of("download_file"), [])

    def test_a_destination_inside_the_folder_is_allowed(self):
        m = make_model(bucket="b")
        c = FakeS3Client()
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("k.txt", os.path.join(tmp, "sub", "k.txt"), tmp)
            self.assertEqual(len(c.calls_of("download_file")), 1)

    def test_no_folder_root_means_no_containment_check(self):
        """Callers that genuinely have no containing root (a bare path from
        the caller's own dialog) are not blocked by it."""
        m = make_model(bucket="b")
        c = FakeS3Client()
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("k.txt", os.path.join(tmp, "k.txt"), "")
            self.assertEqual(len(c.calls_of("download_file")), 1)
class PrefixDownloadParityTests(unittest.TestCase):
    """
    REGRESSION (silent): "Verify downloads" and resumable downloads were
    wired into the single-object path only. A whole-folder download ran
    through the managed transfer and returned without ever comparing a
    digest — so the setting that exists to catch a corrupt download did
    nothing for the downloads most likely to be interrupted.
    """

    BODIES = {"docs/a.txt": b"alpha" * 10,
              "docs/b.txt": b"beta" * 10,
              "docs/sub/c.txt": b"gamma" * 10}

    def _model(self, client, **attrs):
        m = make_model(bucket="b")
        m._client = client
        m.parallel_files = 1            # deterministic order
        m.resume_threshold = 1 << 30    # nothing is "large" unless asked
        for name, value in attrs.items():
            setattr(m, name, value)
        return m

    def test_every_file_in_a_folder_download_is_verified(self):
        c = _PrefixDownloadClient(self.BODIES)
        m = self._model(c, verify_downloads=True)
        logged = []
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("docs/", None, tmp, log_fn=logged.append)
            for key, body in self.BODIES.items():
                out = os.path.join(tmp, "docs", key[len("docs/"):])
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), body)
        checked = [line for line in logged if "checksum ok" in line]
        self.assertEqual(len(checked), len(self.BODIES), logged)

    def test_a_corrupt_file_fails_the_folder_download(self):
        """THE POINT: it used to succeed."""
        c = _PrefixDownloadClient(self.BODIES, corrupt={"docs/b.txt"})
        m = self._model(c, verify_downloads=True)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(Exception) as caught:
                m.download_file("docs/", None, tmp)
            self.assertIn("Checksum mismatch", str(caught.exception))

    def test_verification_off_costs_no_extra_request(self):
        """The cheap path has to stay cheap: a folder of small objects must
        not grow a HEAD per file just because the code paths merged."""
        c = _PrefixDownloadClient(self.BODIES)
        m = self._model(c, verify_downloads=False)
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("docs/", None, tmp)
        self.assertEqual(c.calls_of("head_object"), [])
        self.assertEqual(len(c.calls_of("download_file")), len(self.BODIES))

    def test_a_large_file_in_a_folder_download_takes_the_ranged_path(self):
        """Resumable downloads were single-object only too, so the 8 GiB
        file inside a folder was the one that restarted from zero."""
        c = _PrefixDownloadClient(self.BODIES)
        m = self._model(c, resume_threshold=1)
        m.resume_chunk_size = 16
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("docs/", None, tmp)
            for key, body in self.BODIES.items():
                out = os.path.join(tmp, "docs", key[len("docs/"):])
                with open(out, "rb") as handle:
                    self.assertEqual(handle.read(), body)
        self.assertEqual(c.calls_of("download_file"), [],
                         "still used the managed, non-resumable transfer")
        self.assertTrue(c.ranges, "no ranged requests were made")
class CopyPrefixGuardTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_copy_into_own_subtree_rejected(self):
        """REGRESSION: 'move photos/ -> photos/2024/' copied the tree into
        itself and the follow-up delete of the source prefix then wiped the
        fresh copy (data loss)."""
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.copy_prefix("photos/", "photos/2024/photos/")

    def test_copy_onto_itself_rejected(self):
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.copy_prefix("photos/", "photos/")

    def test_sibling_prefix_sharing_name_is_allowed(self):
        c = FakeS3Client(list_pages=[
            {"Contents": [{"Key": "photos/a.jpg", "Size": 1}]},
        ])
        self.m._client = c
        self.m.copy_prefix("photos/", "photos-old/")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["Key"], "photos-old/a.jpg")

    def test_same_prefix_in_other_bucket_is_allowed(self):
        c = FakeS3Client(list_pages=[
            {"Contents": [{"Key": "photos/a.jpg", "Size": 1}]},
        ])
        self.m._client = c
        self.m.copy_prefix("photos/", "photos/", dst_bucket="b2")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["Bucket"], "b2")
class MultipartUploadTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(
            mpu_pages=[{
                "Uploads": [
                    {"Key": "a/old.bin", "UploadId": "u1",
                     "Initiated": _dt(1), "StorageClass": "STANDARD"},
                    {"Key": "a/new.bin", "UploadId": "u2",
                     "Initiated": _dt(5), "StorageClass": "STANDARD"},
                    {"Key": "a/broken.bin"},  # no UploadId -> skipped
                ],
            }],
            parts_pages=[{"Parts": [{"Size": 100}, {"Size": 50}]}],
        )

    def test_lists_newest_first_and_skips_incomplete_entries(self):
        self.m._client = self._client()
        out = self.m.list_multipart_uploads()
        self.assertEqual([u["key"] for u in out], ["a/new.bin", "a/old.bin"])
        self.assertEqual(out[0]["upload_id"], "u2")
        self.assertIsNone(out[0]["size"])  # sizes not requested

    def test_with_sizes_sums_parts(self):
        c = self._client()
        self.m._client = c
        out = self.m.list_multipart_uploads(with_sizes=True)
        self.assertTrue(all(u["size"] == 150 for u in out))
        self.assertEqual(
            [kw["name"] for kw in c.calls_of("get_paginator")].count("list_parts"),
            2,
        )

    def test_presigned_urls_are_signed_with_sigv4(self):
        """FINDING (found by the MinIO e2e run): with no explicit
        signature_version botocore presigns against a custom endpoint using
        SIGNATURE V2 — while still reporting "s3v4" on the config — and a
        SigV2 presigned PUT is refused outright by MinIO and by every AWS
        region launched after 2014. The upload-link feature was dead
        everywhere but a legacy region."""
        m = make_model(bucket="b", endpoint_url="http://minio.local:9000",
                       use_path=True)
        self.assertEqual(m.client.meta.config.signature_version, "s3v4")
        for url in (m.presigned_get_url("k.txt", 60),
                    m.presigned_put_url("k.txt", 60)):
            self.assertIn("X-Amz-Algorithm=AWS4-HMAC-SHA256", url)
            self.assertNotIn("AWSAccessKeyId=", url)

    def test_a_prefix_is_filtered_locally_not_by_the_server(self):
        """FINDING (found by the MinIO e2e run): MinIO treats the
        ListMultipartUploads Prefix as an EXACT key match — "a/" returns
        nothing while "a/old.bin" returns that one upload. The dialog is
        opened with the current folder as its prefix, so sending it meant
        "no incomplete uploads" everywhere except a bucket root."""
        class PrefixRecording(_FakePaginator):
            def __init__(self, pages):
                super().__init__(pages)
                self.kwargs = []

            def paginate(self, **kwargs):
                self.kwargs.append(kwargs)
                return list(self._pages)

        c = self._client()
        recorder = PrefixRecording(c._paginators["list_multipart_uploads"]._pages)
        c._paginators["list_multipart_uploads"] = recorder
        self.m._client = c

        out = self.m.list_multipart_uploads("a/")
        self.assertEqual([u["key"] for u in out], ["a/new.bin", "a/old.bin"])
        for kwargs in recorder.kwargs:
            self.assertNotIn(
                "Prefix", kwargs,
                "the prefix must not be handed to the server")

    def test_a_prefix_still_narrows_the_result(self):
        c = FakeS3Client(mpu_pages=[{"Uploads": [
            {"Key": "keep/a.bin", "UploadId": "u1", "Initiated": _dt(1)},
            {"Key": "other/b.bin", "UploadId": "u2", "Initiated": _dt(2)},
        ]}])
        self.m._client = c
        self.assertEqual(
            [u["key"] for u in self.m.list_multipart_uploads("keep/")],
            ["keep/a.bin"])
        self.assertEqual(
            len(self.m.list_multipart_uploads("")), 2)

    def test_abort_passes_upload_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.abort_multipart_upload("a/old.bin", "u1")
        self.assertEqual(
            c.calls_of("abort_multipart_upload"),
            [{"Bucket": "b", "Key": "a/old.bin", "UploadId": "u1"}],
        )

    def test_requires_a_bucket(self):
        m = make_model(bucket="")
        m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            m.list_multipart_uploads()
        with self.assertRaises(ValueError):
            m.abort_multipart_upload("k", "u")

    def test_recursive_bucket_delete_aborts_uploads(self):
        """REGRESSION: orphaned multipart parts are invisible to
        ListObjectsV2 and make DeleteBucket fail, so the recursive delete
        must abort them too."""
        m = make_model(bucket="")
        c = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "cur.txt"}]}],
            versions_pages=[{}],
            mpu_pages=[{"Uploads": [
                {"Key": "half.bin", "UploadId": "u9", "Initiated": _dt(1)},
            ]}],
        )
        m._try_bind_bucket = lambda name: (c, "ep", "us-east-1", True)
        m._make_client = lambda **kw: c

        m.delete_bucket_recursive("mybucket")

        self.assertEqual(
            c.calls_of("abort_multipart_upload"),
            [{"Bucket": "mybucket", "Key": "half.bin", "UploadId": "u9"}],
        )
        self.assertEqual(len(c.calls_of("delete_bucket")), 1)
class CrossBucketCopyMoveTests(unittest.TestCase):
    """copy/move jobs carry a destination bucket (None = same bucket)."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.copies = []
            self.prefixes = []
            self.deletes = []

        def copy_object(self, src, dst, dst_bucket=None, log_fn=None):
            self.copies.append((src, dst, dst_bucket))

        def copy_prefix(self, src, dst, dst_bucket=None, log_fn=None,
                        cancel_event=None):
            self.prefixes.append((src, dst, dst_bucket))

        def delete(self, key, log_fn=None):
            self.deletes.append(key)

    def test_copy_forwards_dst_bucket(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "a.txt", False, "other"),
                       ("d/", "d/", True, "other")]).copy()
        self.assertEqual(model.copies, [("a.txt", "a.txt", "other")])
        self.assertEqual(model.prefixes, [("d/", "d/", "other")])
        self.assertEqual(model.deletes, [])

    def test_same_bucket_passes_none(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "b.txt", False, None)]).copy()
        self.assertEqual(model.copies, [("a.txt", "b.txt", None)])

    def test_move_deletes_sources_after_all_copies(self):
        model = self.FakeModel()
        Worker(model, [("a.txt", "a.txt", False, "other"),
                       ("b.txt", "b.txt", False, "other")]).move()
        self.assertEqual(len(model.copies), 2)
        self.assertEqual(model.deletes, ["a.txt", "b.txt"])

    def test_move_failure_leaves_sources_intact(self):
        """A copy error must not fall through to deleting the originals."""
        class Failing(self.FakeModel):
            def copy_object(self, src, dst, dst_bucket=None, log_fn=None):
                raise RuntimeError("access denied")

        model = Failing()
        worker = Worker(model, [("a.txt", "a.txt", False, "other")])
        errors = []
        worker.error.connect(errors.append)
        worker.move()
        self.assertEqual(model.deletes, [])
        self.assertEqual(len(errors), 1)
class ResumableUploadTests(unittest.TestCase):
    """boto3's managed upload restarts from zero, so on a slow link a large
    file was effectively untransferable."""

    PART = 1024

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.payload = os.urandom(self.PART * 3 + 17)   # 4 parts, last short
        self.path = os.path.join(self.dir, "big.bin")
        with open(self.path, "wb") as handle:
            handle.write(self.payload)

    def _model(self, client):
        model = Model("https://s3.amazonaws.com", "us-east-1",
                      "AK", "SK", "bkt", False, False)
        model.upload_chunk_size = self.PART
        model.multipart_threshold_mb = 0        # every file takes this path
        model.transfer_concurrency = 1          # deterministic part order
        model.upload_state_dir = os.path.join(self.dir, "state")
        model._client = client
        return model

    def _state_files(self):
        directory = os.path.join(self.dir, "state")
        return sorted(os.listdir(directory)) if os.path.isdir(directory) else []

    def test_a_large_file_is_assembled_byte_for_byte(self):
        client = _MultipartClient()
        self._model(client).upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(sorted(n for n, _c in client.part_calls),
                         [1, 2, 3, 4])

    def test_the_part_size_grows_rather_than_exceed_ten_thousand_parts(self):
        """REGRESSION: S3 numbers parts 1..10 000, so a fixed part size only
        reaches ``part x 10 000`` -- 80 GiB at the default 8 MiB. Past that
        this writer kept counting and the upload was refused on the part that
        crossed the line, after everything before it had been sent. The
        server-side copy path has clamped for this all along (copy_part_size)
        and boto3's managed upload adjusts too, so turning resumable uploads
        OFF was what made a very large file transferable.

        One byte per part here would need 3 089 parts for this payload;
        the ceiling is lowered instead of the file enlarged."""
        client = _MultipartClient()
        model = self._model(client)
        model.upload_chunk_size = 1
        with unittest.mock.patch(
                "core.parts.MAX_MULTIPART_PARTS", 4):
            model.upload_file(self.path, "big.bin")
        self.assertLessEqual(len(client.part_calls), 4)
        self.assertEqual(client.completed["big.bin"], self.payload,
                         "the clamped part size assembled a different object")

    def test_a_part_size_that_already_fits_is_left_alone(self):
        self.assertEqual(
            Model.upload_part_size(self.PART * 4, self.PART), self.PART)

    def test_the_part_size_rises_just_far_enough(self):
        size = 100 * 1024 ** 3                      # 100 GiB
        part = Model.upload_part_size(size, 8 * 1024 * 1024)
        self.assertLessEqual((size + part - 1) // part,
                             Model.MAX_MULTIPART_PARTS)
        self.assertGreaterEqual(part, 8 * 1024 * 1024)

    def test_the_upload_and_its_parts_agree_on_a_checksum_algorithm(self):
        """
        REGRESSION: botocore 1.36 and newer add a default CRC32 to every
        UploadPart, while CreateMultipartUpload is sent exactly what we pass
        and nothing more. A backend that checks the two against each other
        then rejects every part — "Checksum Type mismatch, expected: null,
        actual: crc32" — and no resumable upload could complete at all.

        Leaving the algorithm to the SDK is what caused it, so the fix is
        to name it: the assertion is that it was named, not merely that the
        two happen to agree on nothing.
        """
        client = _MultipartClient()
        self._model(client).upload_file(self.path, "big.bin")
        created = client.uploads["upload-1"]["kw"].get("ChecksumAlgorithm")
        self.assertTrue(created,
                        "CreateMultipartUpload left the algorithm to the SDK")
        self.assertEqual({algo for _n, algo in client.part_calls}, {created},
                         "the parts carry a different algorithm")

    def test_a_configured_algorithm_is_not_overridden(self):
        """The user's choice wins; the default only fills a gap."""
        client = _MultipartClient()
        model = self._model(client)
        model.set_upload_options(checksum_algorithm="SHA256")
        model.upload_file(self.path, "big.bin")
        created = client.uploads["upload-1"]["kw"]
        self.assertEqual(created.get("ChecksumAlgorithm"), "SHA256")
        self.assertEqual({algo for _n, algo in client.part_calls}, {"SHA256"})

    def test_the_record_is_removed_once_the_upload_completes(self):
        self._model(_MultipartClient()).upload_file(self.path, "big.bin")
        self.assertEqual(self._state_files(), [])

    def test_an_interrupted_upload_resumes_instead_of_restarting(self):
        """THE POINT: the second attempt re-sends only what is missing."""
        client = _MultipartClient(fail_on_part=3)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        self.assertEqual(len(self._state_files()), 1, "no resume record kept")
        first_round = list(client.part_calls)

        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")

        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(client.created, 1, "started a second multipart upload")
        resent = sorted(n for n, _c in client.part_calls)
        self.assertNotIn(1, resent, "re-sent a part the server already had")
        self.assertIn(3, resent, "did not re-send the failed part")
        self.assertLess(len(client.part_calls), len(first_round))

    def test_cancelling_mid_upload_keeps_the_record_so_it_can_resume(self):
        """Cancel is not an abort: the open upload and its record are exactly
        what the next attempt continues from."""
        cancel = threading.Event()
        client = _MultipartClient(cancel_on_part=2, cancel_event=cancel)
        model = self._model(client)
        with self.assertRaises(TransferCancelled):
            model.upload_file(self.path, "big.bin", cancel_event=cancel)
        self.assertEqual(len(self._state_files()), 1)
        self.assertEqual(client.aborted, [], "aborted the resumable upload")
        self.assertNotIn("big.bin", client.completed)

        cancel.clear()
        client.cancel_on_part = None
        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual(client.created, 1)
        self.assertNotIn(1, [n for n, _c in client.part_calls])

    def test_cancelling_before_anything_starts_creates_no_upload(self):
        client = _MultipartClient()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            self._model(client).upload_file(
                self.path, "big.bin", cancel_event=cancel)
        self.assertEqual(client.created, 0)
        self.assertEqual(self._state_files(), [])

    def test_a_changed_local_file_starts_a_fresh_upload(self):
        """Parts already on the server no longer line up, so reusing them
        would assemble a corrupt object."""
        client = _MultipartClient(fail_on_part=2)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        with open(self.path, "wb") as handle:
            handle.write(os.urandom(self.PART * 2))
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a stale upload id")

    def _seed_record(self, client, parts=(), size=None, mtime=None,
                     chunk=None, upload_id=None):
        """A prior attempt: an open upload with some parts already stored."""
        model = self._model(client)
        if upload_id is None:
            upload_id = client.create_multipart_upload("bkt", "big.bin")["UploadId"]
        for number in parts:
            start = (number - 1) * self.PART
            client.uploads[upload_id]["parts"][number] = \
                self.payload[start:start + self.PART]
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            upload_id,
            len(self.payload) if size is None else size,
            os.path.getmtime(self.path) if mtime is None else mtime,
            self.PART if chunk is None else chunk)
        return model

    def test_only_a_size_change_invalidates_the_record(self):
        """Each identity field is checked on its own; the others cannot be
        relied on to notice."""
        client = _MultipartClient()
        mtime = os.path.getmtime(self.path)
        model = self._seed_record(client, parts=[1], mtime=mtime)
        with open(self.path, "wb") as handle:
            handle.write(self.payload + b"appended")
        os.utime(self.path, (mtime, mtime))          # size differs, mtime same
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a record for a resized file")

    def test_only_an_mtime_change_invalidates_the_record(self):
        client = _MultipartClient()
        mtime = os.path.getmtime(self.path)
        model = self._seed_record(client, parts=[1], mtime=mtime)
        os.utime(self.path, (mtime + 60, mtime + 60))   # same bytes, new mtime
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a record for a touched file")

    def test_only_a_part_size_change_invalidates_the_record(self):
        """Parts on the server would no longer line up with what we send."""
        client = _MultipartClient()
        model = self._seed_record(client, parts=[1])
        model.upload_chunk_size = self.PART * 2
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 2, "reused a differently chunked upload")

    def test_completion_lists_parts_in_number_order(self):
        """A resumed upload learns of server parts before it sends the missing
        ones, so insertion order is not part order — and S3 assembles the
        object in the order given."""
        client = _MultipartClient()
        model = self._seed_record(client, parts=[3])
        model.upload_file(self.path, "big.bin")
        numbers = [p["PartNumber"] for p in client.completed_parts]
        self.assertEqual(numbers, [1, 2, 3, 4])
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_cancelling_during_the_last_part_does_not_complete(self):
        """Every part is in flight when the cancel lands, so the parallel run
        finishes normally and only a final check can stop the completion."""
        cancel = threading.Event()
        client = _MultipartClient(cancel_on_part=4, cancel_event=cancel)
        model = self._model(client)
        with self.assertRaises(TransferCancelled):
            model.upload_file(self.path, "big.bin", cancel_event=cancel)
        self.assertNotIn("big.bin", client.completed)
        self.assertEqual(len(self._state_files()), 1)

    def test_a_stored_part_of_the_wrong_length_is_resent(self):
        client = _MultipartClient()
        model = self._model(client)
        upload_id = client.create_multipart_upload("bkt", "big.bin")["UploadId"]
        client.uploads[upload_id]["parts"][1] = b"truncated"
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            upload_id, len(self.payload), os.path.getmtime(self.path),
            self.PART)
        model.upload_file(self.path, "big.bin")
        self.assertIn(1, [n for n, _c in client.part_calls])
        self.assertEqual(client.completed["big.bin"], self.payload)

    def test_an_unusable_upload_id_falls_back_to_a_new_upload(self):
        """The record survives but the service has forgotten the upload."""
        client = _MultipartClient()
        model = self._model(client)
        model._write_upload_state(
            model._upload_state_path("big.bin", self.path), "big.bin",
            "gone", len(self.payload), os.path.getmtime(self.path), self.PART)

        def _boom(**kw):
            raise RuntimeError("NoSuchUpload")

        client.list_parts = _boom
        logs = []
        model.upload_file(self.path, "big.bin", log_fn=logs.append)
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertTrue(any("starting over" in m for m in logs))

    def test_paginated_part_listings_are_followed(self):
        client = _MultipartClient(fail_on_part=4, page_size=1)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        client.part_calls.clear()
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.completed["big.bin"], self.payload)
        self.assertEqual([n for n, _c in client.part_calls], [4])

    def test_progress_is_reported_and_never_exceeds_the_total(self):
        seen = []
        client = _MultipartClient()
        self._model(client).upload_file(
            self.path, "big.bin",
            progress_cb=lambda total, cur, key: seen.append((total, cur)))
        self.assertTrue(seen)
        self.assertTrue(all(cur <= total for total, cur in seen))
        self.assertEqual(seen[-1][1], len(self.payload))

    def test_small_files_stay_on_the_managed_upload(self):
        client = _MultipartClient()
        model = self._model(client)
        model.multipart_threshold_mb = 16
        model.upload_file(self.path, "small.bin")
        self.assertEqual(client.managed, ["small.bin"])
        self.assertEqual(client.created, 0)

    def test_the_feature_can_be_switched_off(self):
        client = _MultipartClient()
        model = self._model(client)
        model.resumable_uploads = False
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.managed, ["big.bin"])

    def test_upload_options_reach_the_multipart_creation(self):
        client = _MultipartClient()
        model = self._model(client)
        model.set_upload_options(storage_class="GLACIER", sse="AES256")
        model.upload_file(self.path, "big.bin")
        created = client.uploads["upload-1"]["kw"]
        self.assertEqual(created["StorageClass"], "GLACIER")
        self.assertEqual(created["ServerSideEncryption"], "AES256")

    def test_a_checksum_algorithm_is_applied_per_part_and_at_completion(self):
        """Otherwise choosing a checksum would silently stop applying to
        exactly the large objects it matters most for."""
        client = _MultipartClient()
        model = self._model(client)
        model.set_upload_options(checksum_algorithm="CRC32")
        model.upload_file(self.path, "big.bin")
        self.assertEqual(client.uploads["upload-1"]["kw"]["ChecksumAlgorithm"],
                         "CRC32")
        self.assertTrue(all(alg == "CRC32" for _n, alg in client.part_calls))
        self.assertTrue(
            all("ChecksumCRC32" in part for part in client.completed_parts))

    def test_a_read_only_profile_refuses(self):
        client = _MultipartClient()
        model = self._model(client)
        model.read_only = True
        with self.assertRaises(ReadOnlyError):
            model.upload_file(self.path, "big.bin")
        self.assertEqual(client.created, 0)

    def test_completion_sends_parts_in_order_without_the_size_field(self):
        """complete_multipart_upload rejects an unexpected Size member."""
        client = _MultipartClient(fail_on_part=2)
        model = self._model(client)
        with self.assertRaises(Exception):
            model.upload_file(self.path, "big.bin")
        model.upload_file(self.path, "big.bin")
        numbers = [p["PartNumber"] for p in client.completed_parts]
        self.assertEqual(numbers, sorted(numbers))
        self.assertTrue(all("Size" not in p for p in client.completed_parts))
class MultipartSizeTests(unittest.TestCase):
    """The part size and the multipart threshold were hardcoded at 8/16 MiB;
    non-AWS backends often need different ones, and the part size also decides
    how a resumable transfer is chunked."""

    def setUp(self):
        self.model = Model("https://s3.amazonaws.com", "us-east-1",
                           "AK", "SK", "bkt", False, False)

    def test_defaults_match_the_previous_hardcoded_values(self):
        self.assertEqual(self.model.multipart_threshold_mb, 16)
        self.assertEqual(self.model.multipart_chunksize_mb, 8)
        self.assertEqual(self.model.multipart_chunksize_bytes, 8 * 1024 * 1024)

    def test_the_sizes_reach_the_transfer_config(self):
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        for cfg in (self.model.transfer_cfg_upload,
                    self.model.transfer_cfg_download):
            self.assertEqual(cfg.multipart_threshold, 64 * 1024 * 1024)
            self.assertEqual(cfg.multipart_chunksize, 32 * 1024 * 1024)

    def test_the_part_size_cannot_go_below_s3s_floor(self):
        """A non-final part under 5 MiB makes every multipart upload fail at
        completion, so the value is clamped rather than trusted."""
        self.model.set_multipart_sizes(chunksize_mb=1)
        self.assertEqual(self.model.multipart_chunksize_mb, 5)

    def test_the_part_size_is_capped(self):
        self.model.set_multipart_sizes(chunksize_mb=99999)
        self.assertEqual(self.model.multipart_chunksize_mb,
                         Model.MAX_MULTIPART_CHUNKSIZE_MB)

    def test_the_threshold_never_falls_below_the_part_size(self):
        """Otherwise boto3 is asked to split a file into a single part."""
        self.model.set_multipart_sizes(threshold_mb=8, chunksize_mb=32)
        self.assertEqual(self.model.multipart_threshold_mb, 32)

    def test_garbage_falls_back_to_the_defaults(self):
        self.model.set_multipart_sizes(threshold_mb="abc", chunksize_mb="xyz")
        self.assertEqual(self.model.multipart_chunksize_mb, 8)
        self.assertEqual(self.model.multipart_threshold_mb, 16)

    def test_ranged_downloads_follow_the_part_size(self):
        """One setting governs how uploads and resumable downloads chunk."""
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=16)
        self.assertEqual(self.model.resume_chunk_size, 16 * 1024 * 1024)
        self.assertEqual(self.model.resume_threshold, 64 * 1024 * 1024)

    def test_the_sizes_survive_a_worker_clone(self):
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        clone = self.model.clone_for_worker()
        self.assertEqual(clone.multipart_threshold_mb, 64)
        self.assertEqual(clone.multipart_chunksize_mb, 32)
        self.assertEqual(clone.resume_chunk_size, 32 * 1024 * 1024)

    def test_changing_concurrency_keeps_the_sizes(self):
        """Both settings rebuild the same TransferConfig pair, so one must not
        reset the other."""
        self.model.set_multipart_sizes(threshold_mb=64, chunksize_mb=32)
        self.model.set_transfer_concurrency(9)
        self.assertEqual(
            self.model.transfer_cfg_upload.multipart_chunksize, 32 * 1024 * 1024)
        self.assertEqual(self.model.transfer_cfg_upload.max_concurrency, 9)
class ConditionalUploadTests(unittest.TestCase):
    """
    Overwrite protection was a snapshot: the destination was listed, the user
    chose, and then the writes went out unconditionally. Anything appearing
    in between was overwritten by a job whose whole instruction was to skip
    what already existed.
    """

    PART = 1024

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.small = os.path.join(self.dir, "small.txt")
        with open(self.small, "wb") as handle:
            handle.write(b"duck")
        self.big = os.path.join(self.dir, "big.bin")
        self.payload = os.urandom(self.PART * 3)
        with open(self.big, "wb") as handle:
            handle.write(self.payload)

    def _model(self, client, threshold_mb=1):
        """threshold_mb=1 leaves small.txt on the single-PUT path;
        threshold_mb=0 puts everything on the multipart one."""
        m = make_model(bucket="bkt")
        m._client = client
        m.upload_chunk_size = self.PART
        m.multipart_threshold_mb = threshold_mb
        m.transfer_concurrency = 1
        m.upload_state_dir = os.path.join(self.dir, "state")
        return m

    def test_a_small_upload_carries_the_precondition(self):
        c = _ConditionalPutClient()
        self._model(c).upload_file(self.small, "small.txt", if_none_match=True)
        self.assertEqual(len(c.puts), 1)
        self.assertEqual(c.puts[0]["IfNoneMatch"], "*")
        self.assertEqual(c.managed, [], "went through the managed upload")

    def test_without_the_flag_nothing_changes(self):
        """The precondition is opt-in: an explicit Overwrite must still
        overwrite, and that path is the managed upload as before."""
        c = _ConditionalPutClient()
        m = self._model(c)
        m.multipart_threshold_mb = 1024
        m.upload_file(self.small, "small.txt")
        self.assertEqual(c.puts, [])
        self.assertEqual(c.managed, ["small.txt"])

    def test_checksum_type_is_not_sent_on_a_single_put(self):
        """REGRESSION GUARD: PutObject has no ChecksumType — a single PUT is a
        whole object by construction — and sending it is a hard
        ParamValidationError, not something the service ignores."""
        c = _ConditionalPutClient()
        m = self._model(c)
        m.set_upload_options(checksum_algorithm="CRC32")
        m.upload_file(self.small, "small.txt", if_none_match=True)
        self.assertEqual(c.puts[0].get("ChecksumAlgorithm"), "CRC32")
        self.assertNotIn("ChecksumType", c.puts[0])

    def test_a_taken_key_raises_instead_of_overwriting(self):
        c = _ConditionalPutClient(
            put_error=_client_error("PreconditionFailed", 412))
        with self.assertRaises(ObjectExists):
            self._model(c).upload_file(self.small, "small.txt",
                                       if_none_match=True)

    def test_a_backend_without_conditional_writes_degrades(self):
        """It is the old check-then-write, which is still what the user got
        yesterday — and it says so rather than failing the upload."""
        logged = []
        c = _ConditionalPutClient(
            put_error=_client_error("NotImplemented", 501))
        m = self._model(c)
        m.multipart_threshold_mb = 1024
        m.upload_file(self.small, "small.txt", if_none_match=True,
                      log_fn=logged.append)
        self.assertEqual(c.managed, ["small.txt"])
        self.assertTrue(any("conditional" in line for line in logged), logged)

    def test_an_unrelated_failure_is_not_retried_unconditionally(self):
        """REGRESSION GUARD: reading every error as 'unsupported' would turn
        the precondition into a slower way of overwriting."""
        c = _ConditionalPutClient(put_error=_client_error("AccessDenied", 403))
        with self.assertRaises(botocore.exceptions.ClientError):
            self._model(c).upload_file(self.small, "small.txt",
                                       if_none_match=True)
        self.assertEqual(c.managed, [])

    def test_a_region_error_is_retried_like_every_other_upload(self):
        """REGRESSION: the conditional path returned early and skipped the
        one-shot rebind every other upload route has, so upload_file's own
        promise — "automatically retries once on region/endpoint errors" —
        stopped being true the moment the user chose to skip existing
        objects."""
        c = _ConditionalPutClient(put_error=botocore.exceptions.ClientError(
            {"Error": {"Code": "PermanentRedirect", "Message": "elsewhere"},
             "ResponseMetadata": {"HTTPStatusCode": 301}}, "PutObject"))
        m = self._model(c)
        rebinds = []
        m.rebind_bucket = lambda log_fn=None: rebinds.append(True)
        m.upload_file(self.small, "small.txt", if_none_match=True)
        self.assertEqual(len(rebinds), 1, "never rebound the bucket")
        self.assertEqual(len(c.puts), 2, "never retried the write")
        self.assertEqual(c.puts[-1]["IfNoneMatch"], "*",
                         "the retry dropped the precondition")

    def test_a_region_error_on_a_large_conditional_upload_is_retried(self):
        c = _ConditionalPutClient()
        c.fail_complete = botocore.exceptions.ClientError(
            {"Error": {"Code": "PermanentRedirect", "Message": "elsewhere"},
             "ResponseMetadata": {"HTTPStatusCode": 301}},
            "CompleteMultipartUpload")
        m = self._model(c, threshold_mb=0)
        rebinds = []
        m.rebind_bucket = lambda log_fn=None: rebinds.append(True)
        m.upload_file(self.big, "big.bin", if_none_match=True)
        self.assertEqual(len(rebinds), 1)
        self.assertEqual(c.completed["big.bin"], self.payload)

    def test_cancelling_a_conditional_upload_stops_it_mid_body(self):
        """REGRESSION: the cancel event was only checked before the request,
        so a conditional upload of a large-but-sub-threshold file ignored
        Cancel until the whole body had been sent."""
        cancel = threading.Event()
        c = _ConditionalPutClient()
        c.on_body_read = lambda sofar: cancel.set()
        m = self._model(c)
        with self.assertRaises(TransferCancelled):
            m.upload_file(self.small, "small.txt", if_none_match=True,
                          cancel_event=cancel)

    def test_a_conditional_upload_reports_progress_as_it_goes(self):
        """Not just 0 and then done: the single PUT is the only path for a
        file just under the multipart threshold, which can be large."""
        c = _ConditionalPutClient()
        seen = []
        m = self._model(c)
        m.upload_file(self.small, "small.txt", if_none_match=True,
                      progress_cb=lambda total, cur, key: seen.append(cur))
        self.assertTrue(seen, "no progress at all")
        self.assertEqual(seen[-1], os.path.getsize(self.small))

    def test_the_rate_limiter_is_charged_for_what_was_actually_sent(self):
        """Charging the whole file up front and then failing would bill the
        shared ceiling for bytes that never left."""
        charged = []
        c = _ConditionalPutClient()
        m = self._model(c)
        m.rate_limiter = types.SimpleNamespace(
            consume=lambda n: charged.append(n))
        m.upload_file(self.small, "small.txt", if_none_match=True)
        self.assertEqual(sum(charged), os.path.getsize(self.small))

    def test_a_large_upload_puts_the_precondition_on_the_completion(self):
        """That is the moment the object appears, and the last point a
        service can refuse without having already replaced anything."""
        c = _ConditionalPutClient()
        self._model(c, threshold_mb=0).upload_file(
            self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.completed_kw.get("IfNoneMatch"), "*")
        self.assertEqual(c.completed["big.bin"], self.payload)

    def test_a_large_upload_is_conditional_even_with_resume_turned_off(self):
        """boto3's managed upload cannot carry the precondition at all, so the
        only multipart path left is the one this file drives itself."""
        c = _ConditionalPutClient()
        m = self._model(c, threshold_mb=0)
        m.resumable_uploads = False
        m.upload_file(self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.completed_kw.get("IfNoneMatch"), "*")
        self.assertEqual(c.managed, [])

    def test_a_taken_key_on_completion_raises_and_keeps_the_parts(self):
        """Aborting would throw away a whole transfer whose outcome the user
        may yet want to retry over the top."""
        c = _ConditionalPutClient()
        c.fail_complete = _client_error("PreconditionFailed", 412,
                                        "CompleteMultipartUpload")
        with self.assertRaises(ObjectExists):
            self._model(c, threshold_mb=0).upload_file(
                self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.aborted, [])

    def test_a_completion_degrades_when_the_backend_cannot_be_conditional(self):
        c = _ConditionalPutClient()
        c.fail_complete = _client_error("NotImplemented", 501,
                                        "CompleteMultipartUpload")
        self._model(c, threshold_mb=0).upload_file(
            self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.completed["big.bin"], self.payload)
class OldBotocoreConditionalWriteTests(unittest.TestCase):
    """
    REGRESSION: every upload failed outright on an installed botocore older
    than IfNoneMatch — "Unknown parameter in input: IfNoneMatch". boto3
    validates arguments against the service model it ships with, so the
    refusal is a client-side ParamValidationError, not a ClientError, and so
    went straight past the fallbacks for a backend that cannot be
    conditional. Overwrite protection is on by default, which made it every
    upload rather than an opt-in corner.
    """

    PART = 1024

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.small = os.path.join(self.dir, "small.txt")
        with open(self.small, "wb") as handle:
            handle.write(b"duck")
        self.big = os.path.join(self.dir, "big.bin")
        self.payload = os.urandom(self.PART * 3)
        with open(self.big, "wb") as handle:
            handle.write(self.payload)

    def _model(self, client, threshold_mb=1):
        m = make_model(bucket="bkt")
        m._client = client
        m.upload_chunk_size = self.PART
        m.multipart_threshold_mb = threshold_mb
        m.transfer_concurrency = 1
        m.upload_state_dir = os.path.join(self.dir, "state")
        return m

    @staticmethod
    def _param_error(parameter="IfNoneMatch"):
        return botocore.exceptions.ParamValidationError(
            report=f'Unknown parameter in input: "{parameter}", must be one '
                   'of: ACL, Body, Bucket, CacheControl, Key, Metadata')

    @staticmethod
    def _service_model():
        return botocore.session.get_session().get_service_model("s3")

    @classmethod
    def _knowing_algorithms(cls, *names):
        """The real service model with PutObject's ChecksumAlgorithm enum
        replaced, so "an algorithm this botocore never heard of" means the
        same thing whatever botocore is installed."""
        real = cls._service_model()

        class _Model:
            def operation_model(self, name):
                op = real.operation_model(name)
                if name != "PutObject":
                    return op
                members = dict(op.input_shape.members)
                members["ChecksumAlgorithm"] = types.SimpleNamespace(
                    enum=list(names))
                return types.SimpleNamespace(
                    input_shape=types.SimpleNamespace(members=members))

        return _Model()

    @classmethod
    def _without(cls, parameter, *operations):
        """The real S3 service model with ``parameter`` taken back out of
        ``operations`` — what an older botocore shipped."""
        real = cls._service_model()

        class _Model:
            def operation_model(self, name):
                op = real.operation_model(name)
                if name not in operations:
                    return op
                return types.SimpleNamespace(
                    input_shape=types.SimpleNamespace(members={
                        k: v for k, v in op.input_shape.members.items()
                        if k != parameter}))

        return _Model()

    def test_the_probe_reads_the_installed_service_model(self):
        c = _ConditionalPutClient()
        m = self._model(c)
        c.meta = types.SimpleNamespace(service_model=self._without(
            "IfNoneMatch", "PutObject", "CompleteMultipartUpload"))
        self.assertFalse(m.supports_conditional_create())
        real = self._service_model()
        c.meta = types.SimpleNamespace(service_model=real)
        self.assertEqual(
            m.supports_conditional_create(),
            "IfNoneMatch" in real.operation_model(
                "PutObject").input_shape.members,
            "the probe disagrees with the installed botocore")

    def test_either_half_missing_is_enough_to_stand_down(self):
        """Both routes a conditional upload may take have to carry it, so
        knowing the parameter on only one of them is not support."""
        for operation in ("PutObject", "CompleteMultipartUpload"):
            with self.subTest(operation=operation):
                c = _ConditionalPutClient()
                c.meta = types.SimpleNamespace(
                    service_model=self._without("IfNoneMatch", operation))
                self.assertFalse(self._model(c).supports_conditional_create())

    def test_a_client_with_no_service_model_is_assumed_capable(self):
        """A model this build cannot describe must not silently disable a
        feature the service does have."""
        self.assertTrue(client_accepts(object(), "PutObject", "IfNoneMatch"))

    def test_one_clients_answer_is_never_handed_to_another(self):
        """REGRESSION GUARD: caching by (operation, parameter) alone made the
        first client asked answer for every client after it."""
        old = types.SimpleNamespace(meta=types.SimpleNamespace(
            service_model=self._without("IfNoneMatch", "PutObject")))
        new = types.SimpleNamespace(
            meta=types.SimpleNamespace(service_model=self._service_model()))
        self.assertFalse(client_accepts(old, "PutObject", "IfNoneMatch"))
        self.assertEqual(
            client_accepts(new, "PutObject", "IfNoneMatch"),
            "IfNoneMatch" in self._service_model().operation_model(
                "PutObject").input_shape.members)
        self.assertTrue(client_accepts(object(), "PutObject", "IfNoneMatch"))

    def test_an_old_botocore_still_uploads_small_files(self):
        c = _ConditionalPutClient()
        c.meta = types.SimpleNamespace(service_model=self._without(
            "IfNoneMatch", "PutObject", "CompleteMultipartUpload"))
        logged = []
        m = self._model(c)
        m.multipart_threshold_mb = 1024
        m.upload_file(self.small, "small.txt", if_none_match=True,
                      log_fn=logged.append)
        self.assertEqual(c.managed, ["small.txt"], "the upload never happened")
        self.assertEqual(c.puts, [], "sent a parameter boto3 would refuse")
        self.assertTrue(any("conditional" in line for line in logged), logged)

    def test_an_old_botocore_still_uploads_large_files(self):
        """And without being dragged onto the resumable writer, which only
        exists to carry a precondition this build cannot send."""
        c = _ConditionalPutClient()
        c.meta = types.SimpleNamespace(service_model=self._without(
            "IfNoneMatch", "PutObject", "CompleteMultipartUpload"))
        m = self._model(c, threshold_mb=0)
        m.resumable_uploads = False
        m.upload_file(self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.managed, ["big.bin"])
        self.assertNotIn("IfNoneMatch", c.completed_kw or {})

    def test_a_refused_parameter_falls_back_instead_of_failing(self):
        """The safety net under the probe: whatever the model claimed, a
        client-side refusal of the parameter is still only a missing
        feature, and the bytes have not been sent yet."""
        c = _ConditionalPutClient(put_error=self._param_error())
        logged = []
        m = self._model(c)
        m.multipart_threshold_mb = 1024
        m.upload_file(self.small, "small.txt", if_none_match=True,
                      log_fn=logged.append)
        self.assertEqual(c.managed, ["small.txt"])
        self.assertTrue(any("conditional" in line for line in logged), logged)

    def test_a_refused_completion_completes_without_the_precondition(self):
        """Here the parts are already uploaded, so falling back beats losing
        the whole transfer."""
        c = _ConditionalPutClient()
        c.fail_complete = self._param_error()
        self._model(c, threshold_mb=0).upload_file(
            self.big, "big.bin", if_none_match=True)
        self.assertEqual(c.completed["big.bin"], self.payload)

    def test_an_unrelated_parameter_error_is_still_a_failure(self):
        """REGRESSION GUARD: reading every ParamValidationError as "no
        conditional writes" would hide a real bug in the arguments and
        overwrite the object as the consolation prize."""
        c = _ConditionalPutClient(put_error=self._param_error("Bogus"))
        with self.assertRaises(botocore.exceptions.ParamValidationError):
            self._model(c).upload_file(self.small, "small.txt",
                                       if_none_match=True)
        self.assertEqual(c.managed, [], "overwrote after an unrelated error")

    def test_checksum_type_is_dropped_when_botocore_cannot_send_it(self):
        """REGRESSION: ChecksumType is the same kind of recent parameter as
        IfNoneMatch, so turning on a CRC checksum failed every multipart
        upload on an older botocore with the identical error."""
        c = _ConditionalPutClient()
        c.meta = types.SimpleNamespace(
            service_model=self._without("ChecksumType",
                                        "CreateMultipartUpload"))
        m = self._model(c)
        m.set_upload_options(checksum_algorithm="CRC32")
        args = m.upload_args_for(self.small, "small.txt")
        self.assertEqual(args.get("ChecksumAlgorithm"), "CRC32",
                         "dropped more than it had to")
        self.assertNotIn("ChecksumType", args)

    def test_checksum_type_survives_where_botocore_knows_it(self):
        c = _ConditionalPutClient()
        c.meta = types.SimpleNamespace(service_model=self._service_model())
        m = self._model(c)
        m.set_upload_options(checksum_algorithm="CRC32")
        args = m.upload_args_for(self.small, "small.txt")
        known = "ChecksumType" in self._service_model().operation_model(
            "CreateMultipartUpload").input_shape.members
        self.assertEqual("ChecksumType" in args, known)

    def test_an_algorithm_this_botocore_never_heard_of_is_dropped(self):
        """An older model lists no CRC64NVME at all, and the whole point of
        the type is the algorithm — so both go.

        The enum is stubbed rather than read from the installed botocore: a
        current one lists CRC64NVME, where keeping it is the right answer,
        and the assertion would then be testing the SDK's age, not the code.
        """
        client = types.SimpleNamespace(
            meta=types.SimpleNamespace(service_model=self._knowing_algorithms(
                "CRC32", "CRC32C", "SHA1", "SHA256")))
        args = drop_unsendable_checksum_args(
            client, {"ChecksumAlgorithm": "CRC64NVME",
                     "ChecksumType": "FULL_OBJECT", "ACL": "private"})
        self.assertNotIn("ChecksumAlgorithm", args)
        self.assertNotIn("ChecksumType", args)
        self.assertEqual(args.get("ACL"), "private",
                         "took an unrelated argument with it")

    def test_an_algorithm_this_botocore_knows_is_kept(self):
        client = types.SimpleNamespace(
            meta=types.SimpleNamespace(service_model=self._knowing_algorithms(
                "CRC32", "CRC64NVME")))
        for algorithm in ("CRC32", "CRC64NVME"):
            with self.subTest(algorithm=algorithm):
                args = drop_unsendable_checksum_args(
                    client, {"ChecksumAlgorithm": algorithm,
                             "ChecksumType": "FULL_OBJECT"})
                self.assertEqual(args.get("ChecksumAlgorithm"), algorithm)

    def test_a_model_that_does_not_constrain_the_algorithm_keeps_it(self):
        """No enum is not an empty enum: nothing is known to be excluded."""
        client = types.SimpleNamespace(
            meta=types.SimpleNamespace(service_model=self._knowing_algorithms()))
        args = drop_unsendable_checksum_args(
            client, {"ChecksumAlgorithm": "CRC64NVME"})
        self.assertEqual(args.get("ChecksumAlgorithm"), "CRC64NVME")

    def test_a_stub_client_keeps_every_checksum_argument(self):
        """Nothing is known about it, so nothing is taken away."""
        args = drop_unsendable_checksum_args(
            object(), {"ChecksumAlgorithm": "CRC64NVME",
                       "ChecksumType": "FULL_OBJECT"})
        self.assertEqual(args, {"ChecksumAlgorithm": "CRC64NVME",
                                "ChecksumType": "FULL_OBJECT"})
class PrefixDownloadPlanTests(unittest.TestCase):
    """The destination mapping shared by the download itself and the overwrite
    check, so the two cannot disagree about what a folder download writes."""

    def test_maps_keys_under_the_prefix_basename(self):
        plan = plan_prefix_download(
            "pics/2024/", "/out", [("pics/2024/a.jpg", 10),
                                   ("pics/2024/sub/b.jpg", 20)])
        self.assertEqual(plan.base_dir, os.path.join("/out", "2024"))
        self.assertEqual(
            [(k, p) for k, p, _s in plan.files],
            [("pics/2024/a.jpg", os.path.join("/out", "2024", "a.jpg")),
             ("pics/2024/sub/b.jpg",
              os.path.join("/out", "2024", "sub", "b.jpg"))])

    def test_a_key_without_a_trailing_slash_still_works(self):
        plan = plan_prefix_download("pics", "/out", [("pics/a.jpg", 1)])
        self.assertEqual(plan.base_dir, os.path.join("/out", "pics"))
        self.assertEqual(len(plan.files), 1)

    def test_directory_markers_become_dirs_not_files(self):
        plan = plan_prefix_download(
            "p/", "/out", [("p/sub/", 0), ("p/sub/f.txt", 3)])
        self.assertEqual(plan.dirs, [os.path.join("/out", "p", "sub")])
        self.assertEqual(len(plan.files), 1)

    def test_the_prefix_itself_is_not_a_file(self):
        plan = plan_prefix_download("p/", "/out", [("p/", 0)])
        self.assertEqual(plan.files, [])
        self.assertEqual(plan.dirs, [])

    def test_keys_escaping_the_target_are_refused(self):
        """S3 keys may contain '..'; a download must not write outside the
        chosen directory."""
        plan = plan_prefix_download(
            "p/", "/out", [("p/../../etc/passwd", 1), ("p/ok.txt", 1)])
        self.assertEqual(plan.unsafe, ["p/../../etc/passwd"])
        self.assertEqual([k for k, _p, _s in plan.files], ["p/ok.txt"])

    def test_a_component_too_long_for_the_filesystem_is_refused(self):
        """S3 allows a 1024-byte key with no per-component rule; ext4, APFS
        and NTFS all stop at 255 bytes. Such a key is an ENAMETOOLONG, and
        because the folder download fans out through run_parallel — which
        re-raises the FIRST error — one of them used to abort the whole
        folder."""
        long_name = "L" * 300
        plan = plan_prefix_download(
            "d/", "/out", [("d/ok.txt", 1), (f"d/{long_name}.txt", 1),
                           ("d/also.txt", 1)])
        self.assertEqual([k for k, _p, _s in plan.files],
                         ["d/ok.txt", "d/also.txt"])
        self.assertEqual(plan.unsafe, [f"d/{long_name}.txt"])

    def test_the_length_rule_counts_bytes_not_characters(self):
        """200 emoji are 800 bytes: a character test would pass them and the
        filesystem would still refuse."""
        self.assertFalse(local_name_is_writable("x/" + "\U0001f986" * 200))
        self.assertTrue(local_name_is_writable("x/" + "a" * MAX_LOCAL_NAME_BYTES))
        self.assertFalse(
            local_name_is_writable("x/" + "a" * (MAX_LOCAL_NAME_BYTES + 1)))

    def test_the_length_rule_is_per_component_not_per_path(self):
        """A deep tree of short names is fine however long the whole path."""
        self.assertTrue(local_name_is_writable("/".join(["dir"] * 200)))

    def test_safe_local_path_applies_the_same_length_rule(self):
        base = os.path.join(os.sep, "out")
        self.assertEqual(safe_local_path(base, "a/" + "L" * 300), "")
        self.assertTrue(safe_local_path(base, "a/" + "L" * 200))

    def test_blank_keys_are_ignored(self):
        plan = plan_prefix_download("p/", "/out", [("", 0), (None, 0)])
        self.assertEqual((plan.files, plan.dirs, plan.unsafe), ([], [], []))

    def test_safe_local_path_refuses_escaping_keys(self):
        """Every other place a key becomes a local path (sync, the second
        pane, search results) shares this rule with the folder download."""
        base = os.path.join(os.sep, "out")
        self.assertEqual(safe_local_path(base, "a/b.txt"),
                         os.path.join(base, "a", "b.txt"))
        for rel in ("../escape.txt", "../../escape.txt",
                    "a/../../escape.txt", "/etc/passwd"):
            with self.subTest(rel=rel):
                landed = safe_local_path(base, rel)
                if landed:
                    self.assertTrue(
                        landed.startswith(os.path.abspath(base) + os.sep),
                        f"{rel!r} escaped to {landed!r}")

    def test_safe_local_path_strips_a_leading_slash_rather_than_absolutising(self):
        base = os.path.join(os.sep, "out")
        self.assertEqual(safe_local_path(base, "/etc/passwd"),
                         os.path.join(base, "etc", "passwd"))

    def test_safe_local_path_rejects_empty_and_self(self):
        base = os.path.join(os.sep, "out")
        self.assertEqual(safe_local_path(base, ""), "")
        self.assertEqual(safe_local_path(base, None), "")
        self.assertEqual(safe_local_path(base, "."), "")

    def test_prefix_of_always_trailing_slashes(self):
        """Listings keyed on a bare name also match sibling prefixes — 'pics'
        matches 'pics-old/…' — so the slash is load-bearing."""
        self.assertEqual(prefix_of("pics"), "pics/")
        self.assertEqual(prefix_of("pics/"), "pics/")

    def test_the_bucket_root_is_the_empty_prefix_not_a_slash(self):
        """"/" as a listing filter matches only keys that literally begin
        with a slash, so a caller reaching here with "" operated on nothing
        instead of on everything."""
        self.assertEqual(prefix_of(""), "")
        self.assertEqual(prefix_of(None), "")
class StreamObjectTests(unittest.TestCase):
    def test_yields_chunks_and_charges_the_limiter(self):
        m = make_model(bucket="b")
        payload = b"x" * 4096

        class Client:
            def get_object(self, **kw):
                return {"Body": io.BytesIO(payload)}

        m._client = Client()
        m.set_rate_limit(10 ** 9)
        chunks = list(m.stream_object("k", chunk_size=1024))
        self.assertEqual(b"".join(chunks), payload)
        self.assertEqual(len(chunks), 4)

    def test_cancellation_stops_the_stream(self):
        m = make_model(bucket="b")

        class Client:
            def get_object(self, **kw):
                return {"Body": io.BytesIO(b"y" * 4096)}

        m._client = Client()
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            list(m.stream_object("k", chunk_size=1024, cancel_event=cancel))

    def test_requires_a_bucket(self):
        with self.assertRaises(ValueError):
            list(make_model(bucket="").stream_object("k"))
class ContentTypeDetectionTests(unittest.TestCase):
    """boto3 never guesses a type, so an upload without this stores every
    object as binary/octet-stream and a browser downloads it unrendered."""

    def test_stdlib_table_answers_the_common_cases(self):
        m = make_model()
        self.assertEqual(m.guess_content_type("index.html"), "text/html")
        self.assertEqual(m.guess_content_type("/tmp/a/photo.PNG"), "image/png")

    def test_builtin_table_fills_the_stdlib_gaps(self):
        m = make_model()
        self.assertEqual(m.guess_content_type("README.md"), "text/markdown")
        self.assertEqual(m.guess_content_type("ci.yaml"), "application/yaml")
        self.assertEqual(m.guess_content_type("app.wasm"), "application/wasm")

    def test_hls_segment_keeps_the_stdlib_answer(self):
        """.ts is deliberately not in the built-in table: stamping a video
        segment as TypeScript would break playback."""
        m = make_model()
        self.assertNotEqual(m.guess_content_type("seg1.ts"),
                            "application/typescript")

    def test_unknown_and_extensionless_are_empty_not_a_fallback(self):
        m = make_model()
        self.assertEqual(m.guess_content_type("blob.qqq"), "")
        self.assertEqual(m.guess_content_type("Makefile"), "")
        self.assertEqual(m.guess_content_type(""), "")

    def test_compressed_archive_declines_rather_than_lying(self):
        """mimetypes reports .tar.gz as x-tar + gzip encoding; sending the
        inner type alone would make a browser silently decompress it."""
        m = make_model()
        self.assertEqual(m.guess_content_type("backup.tar.gz"), "")

    def test_override_beats_both_tables(self):
        m = make_model()
        m.set_content_type_options(overrides={".MD": "text/x-custom"})
        self.assertEqual(m.guess_content_type("README.md"), "text/x-custom")

    def test_overrides_are_normalized_and_blank_rows_dropped(self):
        m = make_model()
        out = m.set_content_type_options(
            overrides={" .Foo ": " text/foo ", "": "x", "bar": "  "})
        self.assertEqual(out, {"foo": "text/foo"})

    def test_upload_args_carry_the_shared_options_plus_the_type(self):
        m = make_model()
        m.set_upload_options(storage_class="GLACIER")
        args = m.upload_args_for("/src/index.html")
        self.assertEqual(args["StorageClass"], "GLACIER")
        self.assertEqual(args["ContentType"], "text/html")
        # the shared dict must not be mutated by a per-file call
        self.assertNotIn("ContentType", m.upload_extra_args)

    def test_detection_can_be_switched_off(self):
        m = make_model()
        m.set_content_type_options(detect=False)
        self.assertEqual(m.upload_args_for("/src/index.html"), {})

    def test_explicit_content_type_is_never_overwritten(self):
        m = make_model()
        m.upload_extra_args = {"ContentType": "application/x-chosen"}
        self.assertEqual(m.upload_args_for("/src/index.html")["ContentType"],
                         "application/x-chosen")

    def test_parse_overrides_reads_both_separators_and_skips_junk(self):
        parsed = Model.parse_content_type_overrides(
            "\n".join([
                "md = text/markdown",
                "  .YAML: application/yaml ",
                "# a comment",
                "nonsense",
                "",
                "*.avif = image/avif",
            ]))
        self.assertEqual(parsed, {
            "md": "text/markdown",
            "yaml": "application/yaml",
            "avif": "image/avif",
        })

    def test_format_overrides_round_trips(self):
        table = {"md": "text/markdown", "avif": "image/avif"}
        self.assertEqual(
            Model.parse_content_type_overrides(
                Model.format_content_type_overrides(table)),
            table)

    def test_managed_upload_sends_the_type(self):
        client = FakeS3Client()
        m = make_model(bucket="bkt")
        m._client = client
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = os.path.join(directory, "page.html")
        with open(path, "w") as handle:
            handle.write("<html></html>")

        m.upload_file(path, "site/page.html")

        extra = client.calls_of("upload_file")[0]["kwargs"]["ExtraArgs"]
        self.assertEqual(extra["ContentType"], "text/html")

    def test_resumable_upload_sends_the_type_on_create(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = os.path.join(directory, "big.json")
        with open(path, "wb") as handle:
            handle.write(b"{}" + b" " * 4096)
        client = _MultipartClient()
        m = make_model(bucket="bkt")
        m._client = client
        m.upload_chunk_size = 1024
        m.multipart_threshold_mb = 0
        m.transfer_concurrency = 1
        m.upload_state_dir = os.path.join(directory, "state")

        m.upload_file(path, "data/big.json")

        created = list(client.uploads.values())[0]
        self.assertEqual(created["kw"].get("ContentType"), "application/json")

    def test_clone_for_worker_carries_the_settings(self):
        m = make_model()
        m.set_content_type_options(detect=False, overrides={"md": "text/x"})
        clone = m.clone_for_worker()
        self.assertFalse(clone.detect_content_type)
        self.assertEqual(clone.content_type_overrides, {"md": "text/x"})
        clone.content_type_overrides["md"] = "changed"
        self.assertEqual(m.content_type_overrides["md"], "text/x")
class ConditionalWriteTests(unittest.TestCase):
    """Editing an object in place is only safe if a concurrent save loses."""

    def _model(self, client=None, etag='"abc"'):
        m = make_model(bucket="bkt")
        m._client = client or FakeS3Client(head_object_resp={"ETag": etag})
        return m

    def test_the_etag_is_sent_as_a_precondition(self):
        client = FakeS3Client()
        client.put_object = lambda **kw: (
            client.calls.append(("put_object", kw)) or {"ETag": '"new"'})
        m = self._model(client)
        new_etag = m.put_object_body("a.txt", b"hello", if_match='"abc"')
        kw = client.calls_of("put_object")[0]
        self.assertEqual(kw["IfMatch"], "abc")
        self.assertEqual(kw["Body"], b"hello")
        self.assertEqual(new_etag, "new")

    def test_the_content_type_is_stamped_from_the_key(self):
        client = FakeS3Client()
        client.put_object = lambda **kw: (
            client.calls.append(("put_object", kw)) or {"ETag": '"n"'})
        m = self._model(client)
        m.put_object_body("site/index.html", b"<p>", if_match="")
        self.assertEqual(
            client.calls_of("put_object")[0]["ContentType"], "text/html")

    def test_a_changed_object_refuses_the_write(self):
        client = FakeS3Client()

        def _put(**_kw):
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "PreconditionFailed", "Message": "no"},
                 "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")

        client.put_object = _put
        m = self._model(client)
        with self.assertRaises(Model.PreconditionFailed):
            m.put_object_body("a.txt", b"x", if_match='"abc"')

    def test_a_backend_without_conditional_writes_checks_by_hand(self):
        """MinIO and friends reject IfMatch outright; the ETag is then
        compared with a HEAD before writing unconditionally."""
        client = FakeS3Client(head_object_resp={"ETag": '"abc"'})
        attempts = []

        def _put(**kw):
            attempts.append(kw)
            if "IfMatch" in kw:
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "PutObject")
            return {"ETag": '"new"'}

        client.put_object = _put
        m = self._model(client)
        self.assertEqual(
            m.put_object_body("a.txt", b"x", if_match='"abc"'), "new")
        self.assertEqual(len(attempts), 2)
        self.assertNotIn("IfMatch", attempts[1])

    def test_an_old_boto3_checks_by_hand_instead_of_failing_the_save(self):
        """REGRESSION: IfMatch is old on GetObject but was added to PutObject
        in the same 2024 wave as IfNoneMatch, so an older botocore refuses it
        client-side — a ParamValidationError, which the ClientError fallback
        below never saw. Save failed outright on every distribution build."""
        client = FakeS3Client(head_object_resp={"ETag": '"abc"'})
        client.meta = types.SimpleNamespace(
            service_model=OldBotocoreConditionalWriteTests._without(
                "IfMatch", "PutObject"))
        attempts = []
        client.put_object = lambda **kw: (
            attempts.append(kw) or {"ETag": '"new"'})
        logged = []
        m = self._model(client)
        self.assertEqual(
            m.put_object_body("a.txt", b"x", if_match='"abc"',
                              log_fn=logged.append), "new")
        self.assertEqual(len(attempts), 1, "sent a doomed request anyway")
        self.assertNotIn("IfMatch", attempts[0])
        self.assertTrue(any("conditional" in line for line in logged), logged)

    def test_an_old_boto3_still_refuses_a_changed_object(self):
        """Standing down from the header must not stand down from the
        promise: the HEAD is what catches the concurrent save instead."""
        client = FakeS3Client(head_object_resp={"ETag": '"different"'})
        client.meta = types.SimpleNamespace(
            service_model=OldBotocoreConditionalWriteTests._without(
                "IfMatch", "PutObject"))

        def _put(**_kw):
            raise AssertionError("must not write after a failed check")

        client.put_object = _put
        m = self._model(client)
        with self.assertRaises(Model.PreconditionFailed):
            m.put_object_body("a.txt", b"x", if_match='"abc"')

    def test_a_refused_if_match_degrades_rather_than_failing(self):
        """The safety net under the probe, for a build whose service model
        and whose validator disagree."""
        client = FakeS3Client(head_object_resp={"ETag": '"abc"'})
        attempts = []

        def _put(**kw):
            attempts.append(kw)
            if "IfMatch" in kw:
                raise botocore.exceptions.ParamValidationError(
                    report='Unknown parameter in input: "IfMatch"')
            return {"ETag": '"new"'}

        client.put_object = _put
        m = self._model(client)
        self.assertEqual(
            m.put_object_body("a.txt", b"x", if_match='"abc"'), "new")
        self.assertEqual(len(attempts), 2)
        self.assertNotIn("IfMatch", attempts[1])

    def test_an_unrelated_parameter_error_still_fails_the_save(self):
        """REGRESSION GUARD: writing unconditionally after any argument
        error would overwrite the very object the precondition protects."""
        client = FakeS3Client(head_object_resp={"ETag": '"abc"'})

        def _put(**_kw):
            raise botocore.exceptions.ParamValidationError(
                report='Unknown parameter in input: "Bogus"')

        client.put_object = _put
        m = self._model(client)
        with self.assertRaises(botocore.exceptions.ParamValidationError):
            m.put_object_body("a.txt", b"x", if_match='"abc"')

    def test_the_hand_check_still_refuses_a_changed_object(self):
        client = FakeS3Client(head_object_resp={"ETag": '"different"'})

        def _put(**kw):
            if "IfMatch" in kw:
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "PutObject")
            raise AssertionError("must not write after a failed check")

        client.put_object = _put
        m = self._model(client)
        with self.assertRaises(Model.PreconditionFailed):
            m.put_object_body("a.txt", b"x", if_match='"abc"')

    def test_a_read_only_profile_refuses(self):
        m = self._model()
        m.read_only = True
        with self.assertRaises(ReadOnlyError):
            m.put_object_body("a.txt", b"x")

    def test_checksum_settings_do_not_leak_into_the_write(self):
        """A put_object carrying ChecksumAlgorithm without the body digest is
        rejected by some backends; the editor never needs one."""
        client = FakeS3Client()
        client.put_object = lambda **kw: (
            client.calls.append(("put_object", kw)) or {"ETag": '"n"'})
        m = self._model(client)
        m.set_upload_options(checksum_algorithm="CRC32", storage_class="GLACIER")
        m.put_object_body("a.txt", b"x")
        kw = client.calls_of("put_object")[0]
        self.assertNotIn("ChecksumAlgorithm", kw)
        self.assertEqual(kw["StorageClass"], "GLACIER")
class UploadRuleTests(unittest.TestCase):
    """"Anything under archive/ goes to Glacier" is a policy; applying it by
    hand on every upload is how it stops being one."""

    def test_a_rule_line_is_parsed(self):
        rules = Model.parse_upload_rules(
            "archive/* -> class=GLACIER, tag:team=infra")
        self.assertEqual(rules, [{
            "pattern": "archive/*", "storage_class": "GLACIER",
            "tags": {"team": "infra"}}])

    def test_junk_and_actionless_lines_are_dropped(self):
        rules = Model.parse_upload_rules("\n".join([
            "# a comment", "no arrow here", "-> class=GLACIER",
            "empty/* ->", "good/* -> class=STANDARD_IA"]))
        self.assertEqual([r["pattern"] for r in rules], ["good/*"])

    def test_rules_round_trip_through_text(self):
        text = "archive/* -> class=GLACIER, tag:a=1, tag:b=2"
        self.assertEqual(
            Model.format_upload_rules(Model.parse_upload_rules(text)), text)

    def test_first_match_wins(self):
        """A table where two rules half-apply is one nobody can predict."""
        m = make_model()
        m.set_upload_rules([
            {"pattern": "logs/2026/*", "storage_class": "STANDARD_IA"},
            {"pattern": "logs/*", "storage_class": "GLACIER"},
        ])
        self.assertEqual(m.rule_for_key("logs/2026/a.log")["storage_class"],
                         "STANDARD_IA")
        self.assertEqual(m.rule_for_key("logs/2025/a.log")["storage_class"],
                         "GLACIER")

    def test_a_bare_prefix_means_everything_under_it(self):
        m = make_model()
        m.set_upload_rules([{"pattern": "archive/", "storage_class": "GLACIER"}])
        self.assertIsNotNone(m.rule_for_key("archive/deep/a.txt"))
        self.assertIsNone(m.rule_for_key("archives/a.txt"))

    def test_no_match_means_no_rule(self):
        m = make_model()
        m.set_upload_rules([{"pattern": "a/*", "storage_class": "GLACIER"}])
        self.assertIsNone(m.rule_for_key("b/x.txt"))
        self.assertIsNone(m.rule_for_key(""))

    def test_a_rule_sets_the_storage_class_and_tags(self):
        m = make_model()
        m.set_upload_rules([{"pattern": "archive/*",
                             "storage_class": "GLACIER",
                             "tags": {"team": "infra", "keep": "yes"}}])
        args = m.upload_args_for("/tmp/a.txt", "archive/a.txt")
        self.assertEqual(args["StorageClass"], "GLACIER")
        self.assertEqual(args["Tagging"], "keep=yes&team=infra")

    def test_a_rule_overrides_the_global_storage_class(self):
        m = make_model()
        m.set_upload_options(storage_class="STANDARD_IA")
        m.set_upload_rules([{"pattern": "archive/*",
                             "storage_class": "DEEP_ARCHIVE"}])
        self.assertEqual(
            m.upload_args_for("/tmp/a", "archive/a")["StorageClass"],
            "DEEP_ARCHIVE")
        self.assertEqual(
            m.upload_args_for("/tmp/a", "other/a")["StorageClass"],
            "STANDARD_IA")

    def test_the_content_type_still_applies_alongside(self):
        m = make_model()
        m.set_upload_rules([{"pattern": "site/*", "storage_class": "GLACIER"}])
        args = m.upload_args_for("/tmp/index.html", "site/index.html")
        self.assertEqual(args["ContentType"], "text/html")
        self.assertEqual(args["StorageClass"], "GLACIER")

    def test_the_rule_reaches_a_real_upload(self):
        client = FakeS3Client()
        m = make_model(bucket="bkt")
        m._client = client
        m.set_upload_rules([{"pattern": "archive/*",
                             "storage_class": "GLACIER"}])
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = os.path.join(directory, "a.txt")
        with open(path, "w") as handle:
            handle.write("x")
        m.upload_file(path, "archive/a.txt")
        extra = client.calls_of("upload_file")[0]["kwargs"]["ExtraArgs"]
        self.assertEqual(extra["StorageClass"], "GLACIER")

    def test_a_worker_clone_carries_the_rules_by_value(self):
        m = make_model()
        m.set_upload_rules([{"pattern": "a/*", "storage_class": "GLACIER"}])
        clone = m.clone_for_worker()
        clone.upload_rules[0]["storage_class"] = "CHANGED"
        self.assertEqual(m.upload_rules[0]["storage_class"], "GLACIER")

    def test_setting_rules_drops_nameless_ones(self):
        m = make_model()
        self.assertEqual(
            m.set_upload_rules([{"pattern": "  ", "storage_class": "X"}]), [])
