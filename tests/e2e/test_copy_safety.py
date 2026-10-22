"""Multipart copy snapshots and cancellation against the real S3 server."""

import os
import tempfile
import threading
from unittest.mock import Mock, patch

import botocore.exceptions

from core.errors import TransferCancelled
from tests.e2e.harness import ServerCase, make_model


class MultipartCopySafetyTests(ServerCase):
    def test_a_stale_listed_size_does_not_truncate_the_copy(self):
        payload = b"a" * (6 * 1024 * 1024) + b"tail"
        self.put("snapshot/size-source", payload)
        self.model._copy_multipart(
            "snapshot/size-source", "snapshot/size-copy", size=len(payload) - 4)
        self.assertEqual(self.body("snapshot/size-copy"), payload)

    def test_a_changed_source_aborts_without_overwriting_destination(self):
        payload = b"a" * (6 * 1024 * 1024)
        self.put("snapshot/changing", payload)
        self.put("snapshot/preserved", b"keep existing destination")
        model = make_model(bucket=self.bucket)
        model.transfer_concurrency = 1
        model.set_multipart_sizes(chunksize_mb=5)
        client = model.client
        original_copy = client.upload_part_copy

        def copy_part(**kwargs):
            result = original_copy(**kwargs)
            if kwargs["PartNumber"] == 1:
                self.put("snapshot/changing", b"b" * len(payload))
            return result

        with patch.object(client, "upload_part_copy", copy_part):
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                model._copy_multipart(
                    "snapshot/changing", "snapshot/preserved", size=len(payload))
        self.assertEqual(caught.exception.response["Error"]["Code"],
                         "PreconditionFailed")
        self.assertEqual(self.body("snapshot/preserved"), b"keep existing destination")
        uploads = client.list_multipart_uploads(Bucket=self.bucket).get("Uploads") or []
        self.assertFalse(any(u["Key"] == "snapshot/preserved" for u in uploads))

    def test_stream_copy_cancels_before_buffering_and_closes_response(self):
        self.put("snapshot/cancel-source", b"source bytes")
        event = threading.Event()
        client = self.model.client
        original_get = client.get_object
        bodies = []

        def get(**kwargs):
            response = original_get(**kwargs)
            response["Body"].close = Mock(wraps=response["Body"].close)
            bodies.append(response["Body"])
            event.set()
            return response

        with patch.object(client, "get_object", get):
            with self.assertRaises(TransferCancelled):
                self.model.copy_to_model(
                    "snapshot/cancel-source", self.model,
                    "snapshot/cancel-dest", cancel_event=event)
        bodies[0].close.assert_called_once()
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            client.head_object(Bucket=self.bucket, Key="snapshot/cancel-dest")
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)


class UploadSourceSafetyTests(ServerCase):
    def test_a_changed_local_source_preserves_the_existing_remote_object(self):
        self.put("changing-upload", b"keep remote object")
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "source")
            with open(path, "wb") as handle:
                handle.write(b"a" * (6 * 1024 * 1024))
            before = os.stat(path)
            model = make_model(bucket=self.bucket)
            model.transfer_concurrency = 1
            model.set_multipart_sizes(threshold_mb=5, chunksize_mb=5)
            model.upload_state_dir = os.path.join(root, "state")
            client = model.client
            original_upload = client.upload_part

            def upload_part(**kwargs):
                result = original_upload(**kwargs)
                if kwargs["PartNumber"] == 1:
                    with open(path, "r+b") as handle:
                        handle.seek(-1, os.SEEK_END)
                        handle.write(b"b")
                    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
                return result

            with patch.object(client, "upload_part", upload_part):
                with self.assertRaisesRegex(IOError, "changed"):
                    model.upload_file(path, "changing-upload")
            self.assertEqual(self.body("changing-upload"), b"keep remote object")
            self.assertFalse(os.path.exists(model._upload_state_path("changing-upload", path)))
            uploads = client.list_multipart_uploads(Bucket=self.bucket).get("Uploads") or []
            self.assertFalse(any(u["Key"] == "changing-upload" for u in uploads))
