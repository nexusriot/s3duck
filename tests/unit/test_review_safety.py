"""Regression coverage for transfer failures that could destroy good data."""

import io
import os
import tempfile
import threading
import types
import unittest
import zipfile
from unittest.mock import Mock, patch

from core.errors import TransferCancelled
from tests.support import _CopyPartClient, _MultipartClient, _ensure_qapp, make_model
from workers.transfer import Worker


class ArchiveSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _ensure_qapp()

    def test_existing_archive_survives_cancellation_and_failure(self):
        for mode in ("cancel", "listing", "stream", "close"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                path = os.path.join(root, "saved.zip")
                with open(path, "wb") as handle:
                    handle.write(b"valuable existing archive")
                def stream(*args, **kwargs):
                    yield b"partial"
                    raise IOError("read failed")
                model = types.SimpleNamespace(
                    get_size=Mock(side_effect=IOError("listing failed"))
                    if mode == "listing" else Mock(return_value=7),
                    stream_object=stream if mode == "stream"
                    else lambda *a, **kw: iter([b"payload"]))
                worker = Worker(model, [(path, "", "key", False)])
                errors, finished = [], []
                worker.error.connect(errors.append)
                worker.finished.connect(finished.append)
                if mode == "cancel":
                    worker.cancel()
                # Closing writes the ZIP central directory: an error here
                # must be reported and must not publish the incomplete file.
                original_close = zipfile.ZipFile.close
                def close(archive):
                    had_file = archive.fp is not None
                    original_close(archive)
                    if had_file:
                        raise IOError("close failed")
                with patch.object(zipfile.ZipFile, "close", close
                                  if mode == "close" else original_close):
                    worker.zip_download()
                with open(path, "rb") as handle:
                    self.assertEqual(handle.read(), b"valuable existing archive")
                self.assertEqual(os.listdir(root), ["saved.zip"])
                self.assertEqual(finished, [mode == "cancel"])
                if mode != "cancel":
                    self.assertTrue(errors)

    def test_success_replaces_existing_archive_after_close(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "saved.zip")
            with open(path, "wb") as handle:
                handle.write(b"old")
            model = types.SimpleNamespace(
                get_size=lambda key: 7,
                stream_object=lambda *a, **kw: iter([b"payload"]))
            worker = Worker(model, [(path, "", "key", False)])
            worker.zip_download()
            with zipfile.ZipFile(path) as archive:
                self.assertEqual(archive.read("key"), b"payload")
            self.assertEqual(os.listdir(root), ["saved.zip"])


class StreamCopySafetyTests(unittest.TestCase):
    def test_cancel_during_copy_without_progress_callback_closes_source(self):
        for route in ("profile", "fallback"):
            with self.subTest(route=route):
                source, dest = make_model(bucket="source"), make_model(bucket="dest")
                event = threading.Event()
                body = io.BytesIO(b"payload")
                source._client = types.SimpleNamespace(
                    get_object=lambda **kw: {"Body": body, "ContentLength": 7})
                def upload(reader, *args, **kwargs):
                    event.set()
                    # Managed uploads read a non-seekable body before they
                    # invoke progress callbacks, so the reader must check too.
                    reader.read()
                dest._client = types.SimpleNamespace(upload_fileobj=upload)
                source._try_bind_bucket = lambda bucket: (dest.client, "ep", "region", True)
                with self.assertRaises(TransferCancelled):
                    if route == "profile":
                        source.copy_to_model("key", dest, "copy", cancel_event=event)
                    else:
                        source._stream_copy("key", "copy", "dest", cancel_event=event)
                self.assertTrue(body.closed)

    def test_source_is_closed_on_upload_failure(self):
        source, dest = make_model(bucket="source"), make_model(bucket="dest")
        body = io.BytesIO(b"payload")
        source._client = types.SimpleNamespace(
            get_object=lambda **kw: {"Body": body, "ContentLength": 7})
        dest._client = types.SimpleNamespace(upload_fileobj=Mock(side_effect=IOError("failed")))
        with self.assertRaises(IOError):
            source.copy_to_model("key", dest, "copy")
        self.assertTrue(body.closed)


class MultipartCopySnapshotTests(unittest.TestCase):
    def test_every_range_is_pinned_to_the_head_snapshot(self):
        for version in (None, "null", "version-1"):
            with self.subTest(version=version):
                model = make_model(bucket="source")
                client = _CopyPartClient(source_size=11, head={
                    "ETag": '"snapshot"', "VersionId": version})
                model._client = client
                model.copy_part_size = lambda *a: 5
                model._copy_multipart("key", "copy", size=8)
                calls = client.calls_of("upload_part_copy")
                self.assertEqual([c["CopySourceRange"] for c in calls],
                                 ["bytes=0-4", "bytes=5-9", "bytes=10-10"])
                for call in calls:
                    self.assertEqual(call["CopySourceIfMatch"], '"snapshot"')
                    self.assertEqual(call["CopySource"].get("VersionId"),
                                     version if version != "null" else None)

    def test_missing_snapshot_identity_refuses_before_creating_upload(self):
        model = make_model(bucket="source")
        client = _CopyPartClient(source_size=11, head={"ETag": ""})
        model._client = client
        with self.assertRaises(ValueError):
            model._copy_multipart("key", "copy", size=11)
        self.assertEqual(client.created, [])

    def test_cancellation_after_last_part_does_not_complete(self):
        model = make_model(bucket="source")
        client = _CopyPartClient(source_size=11)
        event = threading.Event()
        original = client.upload_part_copy
        def copy_part(**kwargs):
            result = original(**kwargs)
            event.set()
            return result
        client.upload_part_copy = copy_part
        model._client = client
        model.transfer_concurrency = 1
        with self.assertRaises(TransferCancelled):
            model._copy_multipart("key", "copy", size=11, cancel_event=event)
        self.assertEqual(client.completed, [])
        self.assertEqual(len(client.aborted), 1)


class UploadSourceMutationTests(unittest.TestCase):
    def test_changed_source_is_never_completed_or_resumed(self):
        for mutation in ("truncate", "rewrite", "last_part"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                path = os.path.join(root, "source")
                with open(path, "wb") as handle:
                    handle.write(b"abcdefgh")
                original_stat = os.stat(path)
                client = _MultipartClient()
                original = client.upload_part
                def upload_part(**kwargs):
                    response = original(**kwargs)
                    if kwargs["PartNumber"] == (2 if mutation == "last_part" else 1):
                        with open(path, "wb") as handle:
                            handle.write(b"ABCDEF" if mutation == "truncate" else b"ABCDEFGH")
                        # Restoring mtime must not hide a rewrite during the
                        # transfer or let a retry reuse the mixed parts.
                        os.utime(path, ns=(original_stat.st_atime_ns,
                                           original_stat.st_mtime_ns))
                    return response
                client.upload_part = upload_part
                model = make_model(bucket="dest")
                model._client = client
                model.upload_chunk_size = 5
                model.transfer_concurrency = 1
                model.upload_state_dir = os.path.join(root, "state")
                with self.assertRaisesRegex(IOError, "changed"):
                    model._upload_resumable(path, "key", 8)
                self.assertNotIn("key", client.completed)
                self.assertFalse(os.path.exists(model._upload_state_path("key", path)))
