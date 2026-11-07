"""Moves must not delete files that appeared after the copy snapshot."""

from unittest.mock import patch

from botocore.client import BaseClient

from tests.e2e_gui.harness import GuiServerCase


class ConcurrentFolderMoveTests(GuiServerCase):
    def _move_with_new_source(self, cross_profile):
        source = self._testMethodName + "/source/"
        target = self._testMethodName + "/target/"
        self.put(source + "original.txt", b"original")
        self.navigate("")
        original_call = BaseClient._make_api_call
        injected = []

        def insert_after_copy(client, operation, params):
            result = original_call(client, operation, params)
            if operation in ("CopyObject", "PutObject") and not injected:
                # Same-profile copies use CopyObject; profile copies stream
                # the source to a PutObject at the destination.
                if params.get("Key") == target + "original.txt":
                    injected.append(True)
                    self.put(source + "arrived-during-move.txt", b"only copy")
            return result

        with patch.object(BaseClient, "_make_api_call", insert_after_copy):
            if cross_profile:
                dest = self.window.data_model.clone_for_worker()
                entry = self.window.assign_thread_operation(
                    "move_to_profile", [(source, target, True)], dest_model=dest)
            else:
                entry = self.window.assign_thread_operation(
                    "move", [(source, target, True, None)])
            self.await_transfers()
        self.assertEqual(injected, [True], "the concurrent write was not exercised")
        self.assertEqual(entry.status, "done")
        self.assertEqual(self.body(target + "original.txt"), b"original")
        self.assertFalse(self.exists(source + "original.txt"))
        self.assertTrue(self.exists(source + "arrived-during-move.txt"),
                        "move deleted a new source object it never copied")

    def test_same_profile_move_preserves_new_source_objects(self):
        self._move_with_new_source(cross_profile=False)

    def test_cross_profile_move_preserves_new_source_objects(self):
        self._move_with_new_source(cross_profile=True)
