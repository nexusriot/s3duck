"""The copied-key manifest is the only deletion target for a folder move."""

import threading
import types
import unittest
from unittest.mock import Mock

from model import DeleteRefused, ReadOnlyError, TransferCancelled
from tests.support import _ensure_qapp, make_model
from workers.transfer import Worker


class ExactDeleteTests(unittest.TestCase):
    def test_folder_markers_are_exact_keys_and_batches_never_list(self):
        model = make_model(bucket="bucket")
        model._client = types.SimpleNamespace(delete_objects=Mock(return_value={}))
        model.DELETE_BATCH_SIZE = 2
        model.get_keys = Mock(side_effect=AssertionError("manifest was expanded"))
        keys = ["source/", "source/old.txt", "source/sub/"]
        model.delete_exact(keys)
        batches = model._client.delete_objects.call_args_list
        self.assertEqual([len(c.kwargs["Delete"]["Objects"]) for c in batches], [2, 1])
        deleted = [obj["Key"] for call in batches for obj in call.kwargs["Delete"]["Objects"]]
        self.assertEqual(deleted, keys)
        model.get_keys.assert_not_called()

    def test_cancel_and_read_only_prevent_any_deletion(self):
        model = make_model(bucket="bucket")
        model._client = types.SimpleNamespace(delete_objects=Mock(return_value={}))
        cancelled = threading.Event()
        cancelled.set()
        with self.assertRaises(TransferCancelled):
            model.delete_exact(["a"], cancel_event=cancelled)
        model.read_only = True
        with self.assertRaises(ReadOnlyError):
            model.delete_exact(["a"])
        model._client.delete_objects.assert_not_called()

    def test_partial_service_failure_is_reported(self):
        model = make_model(bucket="bucket")
        model._client = types.SimpleNamespace(delete_objects=Mock(return_value={
            "Errors": [{"Key": "source/old.txt", "Code": "AccessDenied"}]}))
        with self.assertRaises(DeleteRefused):
            model.delete_exact(["source/old.txt"])


class MoveManifestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _ensure_qapp()

    def test_missing_copy_manifest_never_falls_back_to_recursive_delete(self):
        model = types.SimpleNamespace(
            copy_prefix=Mock(return_value=None), delete=Mock(), delete_exact=Mock())
        worker = Worker(model, [("source/", "target/", True, None)])
        errors = []
        worker.error.connect(errors.append)
        worker.move()
        self.assertTrue(errors)
        model.delete.assert_not_called()
        model.delete_exact.assert_not_called()

    def test_empty_folder_manifest_does_not_trigger_recursive_delete(self):
        model = types.SimpleNamespace(
            copy_prefix=Mock(return_value=[]), delete=Mock(), delete_exact=Mock())
        worker = Worker(model, [("source/", "target/", True, None)])
        errors = []
        worker.error.connect(errors.append)
        worker.move()
        self.assertEqual(errors, [])
        model.delete.assert_not_called()
        self.assertEqual(model.delete_exact.call_args.args, ([],))

    def test_profile_copy_keeps_empty_root_folder_marker_in_manifest(self):
        model = make_model(bucket="bucket")
        model.get_keys = Mock(return_value=[("source/", 0)])
        target = types.SimpleNamespace(_guard_write=Mock(), create_folder=Mock())
        self.assertEqual(model.copy_prefix_to_model("source/", target, "target/"),
                         ["source/"])
        target.create_folder.assert_called_once_with("target/", log_fn=None)
