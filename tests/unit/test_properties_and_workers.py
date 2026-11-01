"""Properties' encryption and replication rows, the move-to-profile worker's
cancellation, and the preview when QtMultimedia is missing."""

import types
import unittest
import ui.dialogs.preview as preview
from properties_window import PropertiesWindow
from tests.support import _FakePropsModel, _ensure_qapp
from unittest.mock import patch
from workers.transfer import Worker


class PropertiesRowsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _rows(self, head):
        dlg = PropertiesWindow(None, settings=(_FakePropsModel(head=head), "k.txt"))
        self.addCleanup(dlg.close)
        return dlg.encryption.text(), dlg.replication.text()

    def test_encryption_and_replication_come_from_the_head(self):
        self.assertEqual(
            self._rows({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "k1",
                        "ReplicationStatus": "COMPLETED"}),
            ("SSE-KMS (k1)", "COMPLETED"))
        encryption, replication = self._rows({"SSECustomerAlgorithm": "AES256",
                                              "SSECustomerKeyMD5": "md5"})
        self.assertIn("key MD5 md5", encryption)
        self.assertEqual(replication, "—")
        self.assertEqual(self._rows({}), ("—", "—"))


class MoveToProfileCancelTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_cancel_after_the_copies_deletes_nothing(self):
        deleted = []
        worker = None

        class Source:
            parallel_files = 1

            def copy_to_model(self, *a, **kw):
                worker.cancel()

            def delete(self, key, **kw):
                deleted.append(key)

        worker = Worker(Source(), [("a", "x/a", False)],
                        dest_model=types.SimpleNamespace(bucket="d"))
        outcomes = []
        worker.finished.connect(outcomes.append)
        worker.move_to_profile()
        self.assertEqual(outcomes, [True], "a cancel was not reported as one")
        self.assertEqual(deleted, [])


class PreviewWithoutMultimediaTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_video_falls_back_to_the_ordinary_preview(self):
        class Model:
            bucket = "b"

            def clone_for_worker(self):
                return self

            def get_object_preview(self, key, max_bytes):
                return {"data": b"\x00\x01", "content_type": "video/mp4",
                        "size": 2, "truncated": False}

        with patch.object(preview, "QMediaPlayer", None):
            dlg = preview.PreviewDialog(None, Model(), "clip.mp4")
            self.addCleanup(dlg.close)
            self.assertEqual(dlg._media_page, -1)
            dlg._on_loaded(Model().get_object_preview("clip.mp4", 1), None)
        self.assertEqual(dlg._stack.currentIndex(), 3, "no hex fallback")


if __name__ == "__main__":
    unittest.main()
