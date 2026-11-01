"""Pause, resume and the persisted queue, against a real server.

Offline, a pause is a cancellation with a different label. What only a server
can settle is the claim the feature makes: that a paused download keeps its
partial file and finishes from it, and that a paused batch does not redo the
objects it had already written.
"""

import os
import tempfile

from tests.e2e_gui.harness import GuiServerCase


class PauseResumeTests(GuiServerCase):

    RATE = 512 * 1024

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        model = self.window.data_model
        saved = (model.resume_threshold, model.resume_chunk_size)
        model.resume_threshold = 1024 * 1024
        model.resume_chunk_size = 1024 * 1024

        def _restore():
            model.resume_threshold, model.resume_chunk_size = saved
            model.set_rate_limit(0)
        self.addCleanup(_restore)
        self.navigate("")

    def _pause_when(self, entry, predicate, what):
        self.pump(predicate, what=what)
        self.assertTrue(self.window.pause_entry(entry.entry_id))
        self.pump(lambda: entry.status == "paused"
                  and not self.window.transfers_active(),
                  what="the job to park")

    def test_a_paused_download_finishes_from_its_partial_file(self):
        payload = os.urandom(6 * 1024 * 1024)
        key = self.put("pause/big.bin", payload)
        target = os.path.join(self.tmp.name, "big.bin")
        part = target + ".s3duckpart"
        self.window.data_model.set_rate_limit(self.RATE)
        with self.recording_messages():
            entry = self.window.assign_thread_operation(
                "download", [(key, target, len(payload), self.tmp.name)],
                need_refresh=False)
            self._pause_when(
                entry,
                lambda: os.path.exists(part) and os.path.getsize(part) > 0,
                "the ranged download to write its partial file")
        self.assertTrue(os.path.exists(part),
                        "pausing threw the partial download away")
        self.assertFalse(os.path.exists(target))
        stored = self.window.load_pending_queue()
        self.assertTrue(any(r["paused"] and r["method"] == "download"
                            for r in stored), stored)

        self.window.data_model.set_rate_limit(0)
        with self.recording_messages() as shown:
            self.assertTrue(self.window.resume_entry(entry.entry_id))
            self.await_transfers()
        self.assertEqual(entry.status, "done", shown)
        with open(target, "rb") as handle:
            self.assertEqual(handle.read(), payload)
        self.assertFalse(os.path.exists(part))
        self.assertFalse(any(r["method"] == "download"
                             for r in self.window.load_pending_queue()))

    def test_a_paused_batch_does_not_redo_finished_objects(self):
        paths = []
        for name, size in (("one.bin", 64 * 1024), ("two.bin", 3 * 1024 * 1024),
                           ("three.bin", 64 * 1024)):
            path = os.path.join(self.tmp.name, name)
            with open(path, "wb") as handle:
                handle.write(os.urandom(size))
            paths.append(path)
        job = [(f"batch/{os.path.basename(p)}", p) for p in paths]
        self.window.data_model.set_rate_limit(self.RATE)
        model = self.window.data_model
        saved_parallel = model.parallel_files
        model.set_parallel_files(1)
        self.addCleanup(model.set_parallel_files, saved_parallel)
        with self.recording_messages():
            entry = self.window.assign_thread_operation(
                "upload", job, need_refresh=False)
            self._pause_when(entry, lambda: 0 in entry.done_items,
                             "the first object to finish")
        self.assertTrue(self.exists("batch/one.bin"))
        first = self.s3.head_object(Bucket=self.bucket,
                                    Key="batch/one.bin")["LastModified"]

        model.set_rate_limit(0)
        with self.recording_messages() as shown:
            self.window.resume_entry(entry.entry_id)
            self.await_transfers()
        self.assertEqual(entry.status, "done", shown)
        self.assertNotIn(job[0], entry.job, "the finished object ran again")
        self.assertEqual(self.keys("batch/"),
                         ["batch/one.bin", "batch/three.bin", "batch/two.bin"])
        again = self.s3.head_object(Bucket=self.bucket,
                                    Key="batch/one.bin")["LastModified"]
        self.assertEqual(first, again, "the finished object was rewritten")


class QueueRestoreTests(GuiServerCase):
    """The record a closed window leaves behind, replayed by the next one.

    The shared window cannot be closed and rebuilt here, so "the next
    window" is this one with its live queue entries forgotten: what remains
    is only what was written to the settings, which is exactly what a fresh
    launch would read.
    """

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.navigate("")

    def test_a_paused_upload_is_restored_and_completes(self):
        path = os.path.join(self.tmp.name, "later.txt")
        with open(path, "wb") as handle:
            handle.write(b"uploaded after a restart")
        key = "restored/later.txt"
        window = self.window
        with self.recording_messages():
            entry = window.assign_thread_operation(
                "upload", [(key, path)], need_refresh=False, start_paused=True)
        self.assertEqual(entry.status, "paused")
        self.assertFalse(self.exists(key))

        window._queue_entries.pop(entry.entry_id)
        window._queue_panel.remove_entry(entry.entry_id)
        self.assertTrue(window.offer_pending_restore(),
                        "the paused job was not offered after a restart")
        with self.recording_messages() as shown:
            self.assertEqual(window.restore_pending_queue(), 1)
            restored = self.last_queue_entry()
            self.assertEqual(restored.status, "paused",
                             "a paused job restarted on its own")
            self.assertEqual(restored.source_bucket, self.bucket)
            window.resume_entry(restored.entry_id)
            self.await_transfers()
        self.assertEqual(restored.status, "done", shown)
        self.assertEqual(self.body(key), b"uploaded after a restart")
        self.assertEqual(window.load_pending_queue(), [],
                         "a finished job was left in the record")

