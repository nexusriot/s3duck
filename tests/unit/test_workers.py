"""workers/: the background operations and their contract."""

import botocore.session
import main_window
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
import utils
import weakref
import zipfile
from PyQt6 import sip
from PyQt6.QtCore import QObject, QSettings, QThread, Qt
from main_window import Worker
from model import ObjectExists, TransferCancelled
from tests.support import (
    MIN_APP_SOURCES, PROJECT_ROOT, _StubModel, _ensure_qapp,
    make_main_window, read_app_sources,
)
from unittest.mock import patch


class WorkerBulkStorageTests(unittest.TestCase):
    """Worker.set_storage_class / restore must fan a folder target out to every
    concrete object under it, and act directly on file targets."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _worker(self, job):
        class FakeWorkerModel:
            def __init__(self):
                self.storage_calls = []
                self.restore_calls = []

            def get_keys(self, prefix, log_fn=None):
                # two files + a placeholder under any folder prefix
                return [(prefix + "a.txt", 1), (prefix + "b.txt", 2),
                        (prefix, 0)]

            def change_storage_class(self, key, storage_class, log_fn=None):
                self.storage_calls.append((key, storage_class))

            def restore_object(self, key, days=7, tier="Standard"):
                self.restore_calls.append((key, days, tier))
                return True, None

        model = FakeWorkerModel()
        return Worker(model, job), model

    def test_storage_class_recurses_folder(self):
        job = [("f.txt", False, "GLACIER"), ("dir/", True, "GLACIER")]
        worker, model = self._worker(job)
        worker.set_storage_class()
        keys = [k for k, _cls in model.storage_calls]
        self.assertIn("f.txt", keys)                 # file target
        self.assertIn("dir/a.txt", keys)             # folder fanned out
        self.assertIn("dir/b.txt", keys)
        self.assertNotIn("dir/", keys)               # placeholder skipped
        self.assertTrue(all(c == "GLACIER" for _k, c in model.storage_calls))

    def test_one_refused_object_does_not_abandon_the_batch(self):
        """FINDING (MinIO e2e): MinIO answers InvalidStorageClass for every
        AWS class, so the first object aborted the whole selection and the
        rest were never attempted. restore and delete_buckets already
        tolerate a per-item failure; this now does too."""
        class Refusing:
            def __init__(self):
                self.attempted = []

            def get_keys(self, prefix, log_fn=None):
                return [(prefix + "a.txt", 1), (prefix + "b.txt", 2)]

            def change_storage_class(self, key, storage_class, log_fn=None):
                self.attempted.append(key)
                if key.endswith("a.txt"):
                    raise RuntimeError("InvalidStorageClass")

        model = Refusing()
        worker = Worker(model, [("dir/", True, "GLACIER")])
        errors = []
        finished = []
        worker.error.connect(errors.append)
        worker.finished.connect(finished.append)
        worker.set_storage_class()
        self.assertEqual(model.attempted, ["dir/a.txt", "dir/b.txt"],
                         "the batch stopped at the first refusal")
        self.assertEqual(finished, [False])       # failed, not cancelled
        self.assertTrue(errors)
        self.assertIn("dir/a.txt", errors[0])

    def test_an_object_already_in_the_class_is_a_no_op_not_a_failure(self):
        """S3 refuses a copy onto itself that changes nothing — an
        InvalidRequest on AWS and MinIO alike — so normalising a selection
        to STANDARD failed on every object already in STANDARD."""
        class AlreadyThere:
            def __init__(self):
                self.calls = []

            def get_keys(self, prefix, log_fn=None):
                return [(prefix + "a.txt", 1), (prefix + "b.txt", 2)]

            def change_storage_class(self, key, storage_class, log_fn=None):
                self.calls.append(key)
                return False            # already in that class

        model = AlreadyThere()
        worker = Worker(model, [("dir/", True, "STANDARD")])
        errors = []
        lines = []
        worker.error.connect(errors.append)
        worker.progress.connect(lines.append)
        worker.set_storage_class()
        self.assertEqual(model.calls, ["dir/a.txt", "dir/b.txt"])
        self.assertEqual(errors, [], "a no-op was reported as a failure")
        self.assertTrue(any("2 already there" in line for line in lines))

    def test_a_clean_batch_reports_no_error(self):
        job = [("dir/", True, "GLACIER")]
        worker, model = self._worker(job)
        errors = []
        worker.error.connect(errors.append)
        worker.set_storage_class()
        self.assertEqual(errors, [])

    def test_cancelling_still_stops_the_batch(self):
        """Per-object tolerance must not swallow a cancellation."""
        job = [("dir/", True, "GLACIER")]
        worker, model = self._worker(job)
        worker._cancel_event.set()
        finished = []
        worker.finished.connect(finished.append)
        worker.set_storage_class()
        self.assertEqual(finished, [True])        # cancelled
        self.assertEqual(model.storage_calls, [])

    def test_restore_recurses_folder(self):
        job = [("dir/", True, 5, "Bulk")]
        worker, model = self._worker(job)
        worker.restore()
        keys = [k for k, _d, _t in model.restore_calls]
        self.assertEqual(set(keys), {"dir/a.txt", "dir/b.txt"})
        self.assertTrue(all((d, t) == (5, "Bulk") for _k, d, t in model.restore_calls))
class DeleteBucketsWorkerTests(unittest.TestCase):
    """Bucket deletion runs through the transfer queue so the UI stays live."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self, failing=()):
            self.recursive = []
            self.plain = []
            self._failing = set(failing)

        def delete_bucket_recursive(self, name, cancel_event=None, log_fn=None):
            if name in self._failing:
                raise RuntimeError("bucket not empty")
            self.recursive.append(name)

        def delete_bucket(self, name):
            if name in self._failing:
                raise RuntimeError("bucket not empty")
            self.plain.append(name)

    def _run(self, job, failing=()):
        model = self.FakeModel(failing)
        worker = Worker(model, job)
        errors, finished = [], []
        worker.error.connect(errors.append)
        worker.finished.connect(finished.append)
        worker.delete_buckets()
        return model, errors, finished

    def test_routes_recursive_and_plain(self):
        model, errors, finished = self._run([("a", True), ("b", False)])
        self.assertEqual(model.recursive, ["a"])
        self.assertEqual(model.plain, ["b"])
        self.assertEqual(errors, [])
        self.assertEqual(finished, [False])

    def test_one_failure_does_not_abort_the_batch(self):
        model, errors, finished = self._run(
            [("a", True), ("bad", True), ("c", True)], failing={"bad"})
        # every healthy bucket still processed
        self.assertEqual(model.recursive, ["a", "c"])
        self.assertEqual(len(errors), 1)
        self.assertIn("bad", errors[0])
        self.assertEqual(finished, [False])

    def test_cancel_stops_early(self):
        model = self.FakeModel()
        worker = Worker(model, [("a", True), ("b", True)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.delete_buckets()
        self.assertEqual(model.recursive, [])
        self.assertEqual(finished, [True])  # cancelled
class WorkerLifetimeTests(unittest.TestCase):
    """release_worker_on_finish — the fix for the intermittent whole-suite
    freeze: a worker deleteLater'd via its own signal is destroyed on the
    worker thread, whose destructor takes a pooled signal-slot mutex and then
    the GIL, deadlocking against a GUI thread that holds the GIL and touches
    any connect/disconnect (Qt pools those mutexes, so unrelated objects
    collide)."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _drain(self, th):
        deadline = time.monotonic() + 5.0
        while not th.isFinished() and time.monotonic() < deadline:
            self._app.processEvents()
        th.wait(2000)

    def test_worker_is_pinned_until_its_thread_finishes(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)

        utils.release_worker_on_finish(th, wk)
        key = id(wk)
        self.assertIn(key, utils._LIVE_WORKERS)

        # Not started yet, so a sweep must not release it: isFinished is the
        # guard that keeps a fresh thread's worker from being swept early.
        utils.reap_finished_workers()
        self.assertIn(key, utils._LIVE_WORKERS)

        th.start()
        self._drain(th)
        utils.reap_finished_workers()

        self.assertNotIn(key, utils._LIVE_WORKERS)
        self.assertFalse(th.isRunning())
        # No deleteLater was queued anywhere: the C++ object is still alive
        # because this test holds the last reference, on the GUI thread.
        self.assertFalse(sip.isdeleted(wk))

    def test_next_pin_sweeps_finished_predecessors(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        th2 = QThread()
        wk2 = utils.FuncWorker(lambda _w: None)
        utils.release_worker_on_finish(th2, wk2)

        self.assertNotIn(id(wk), utils._LIVE_WORKERS)
        self.assertIn(id(wk2), utils._LIVE_WORKERS)
        utils._LIVE_WORKERS.pop(id(wk2), None)

    def test_join_qthread_sweeps_too(self):
        """Dialogs join in their done/close handlers; the sweep must ride
        along so a dialog's last worker is not pinned until the next one."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        utils.join_qthread(th)
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_deleted_thread_counts_as_finished(self):
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        utils.release_worker_on_finish(th, wk)
        sip.delete(th)
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_thread_finished_fallback_without_sip(self):
        """A dead wrapper must count as finished even where sip is missing:
        isFinished on it raises RuntimeError, which means the thread cannot
        run its worker any more."""
        class _Gone:
            def isFinished(self):
                raise RuntimeError("wrapped C/C++ object has been deleted")

        class _Busy:
            def isFinished(self):
                return False

        with patch.object(utils, "sip", None):
            self.assertTrue(utils._thread_finished(_Gone()))
            self.assertFalse(utils._thread_finished(_Busy()))

        # With sip present, a non-wrapper argument makes isdeleted raise;
        # that must fall through to isFinished, not blow up the sweep.
        self.assertFalse(utils._thread_finished(_Busy()))
        self.assertTrue(utils._thread_finished(_Gone()))

    def test_a_thread_that_will_not_stop_is_cut_loose_from_its_parent(self):
        """quit() only asks an event loop to stop; a worker parked in a
        request ignores it. Leaving that thread parented to a dialog on its
        way out is the "QThread: Destroyed while thread is still running"
        abort the parenting was supposed to prevent."""
        parent = QObject()
        gate = threading.Event()
        th = QThread(parent)
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        try:
            utils.join_qthread(th, timeout_ms=50)
            self.assertIsNone(th.parent())
            # Unparenting hands ownership to Python, so something must hold it
            # or the next collection runs the destructor on a running thread.
            self.assertIn(th, utils._DETACHED_THREADS)
        finally:
            gate.set()
        self._drain(th)

        utils.reap_finished_workers()
        self.assertNotIn(th, utils._DETACHED_THREADS)
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_a_detached_thread_is_released_without_any_later_join(self):
        """FINDING (leak): the only sweep ran from join_qthread/
        release_worker_on_finish, so a detached thread's wrapper — and the
        C++ QThread, for callers that do not wire finished to deleteLater —
        stayed pinned for the life of the process if nothing else in the
        session ever started a worker. Detaching now drives its own sweep."""
        gate = threading.Event()
        parent = QObject()
        th = QThread(parent)
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        try:
            utils.join_qthread(th, timeout_ms=50)
            self.assertIn(th, utils._DETACHED_THREADS)
            self.assertTrue(utils._reaper()._sweeper.isActive(),
                            "nothing will ever look at this thread again")
        finally:
            gate.set()
        self._drain(th)

        # No join_qthread, no release_worker_on_finish — only the sweep.
        utils._reaper()._sweep()
        self.assertNotIn(th, utils._DETACHED_THREADS)
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)
        self.assertFalse(utils._reaper()._sweeper.isActive(),
                         "the poll must stop once there is nothing to watch")

    def test_the_sweep_keeps_polling_while_a_thread_is_still_running(self):
        gate = threading.Event()
        parent = QObject()          # must outlive the thread it parents
        th = QThread(parent)
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        try:
            utils.join_qthread(th, timeout_ms=50)
            utils._reaper()._sweep()
            self.assertIn(th, utils._DETACHED_THREADS)
            self.assertTrue(utils._reaper()._sweeper.isActive())
        finally:
            gate.set()
        self._drain(th)
        utils._reaper()._sweep()
        self.assertNotIn(th, utils._DETACHED_THREADS)

    def test_the_sweep_really_fires_on_its_own_timer(self):
        """The wiring, not just the slot: a real event loop must reach it."""
        gate = threading.Event()
        parent = QObject()          # must outlive the thread it parents
        th = QThread(parent)
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        with patch.object(utils, "DETACHED_SWEEP_MS", 10):
            utils._reaper()._sweeper.setInterval(10)
            try:
                utils.join_qthread(th, timeout_ms=50)
            finally:
                gate.set()
            th.wait(5000)
            deadline = time.monotonic() + 5.0
            while (th in utils._DETACHED_THREADS
                   and time.monotonic() < deadline):
                self._app.processEvents()
        utils._reaper()._sweeper.setInterval(utils.DETACHED_SWEEP_MS)
        self.assertNotIn(th, utils._DETACHED_THREADS)

    def test_a_thread_that_stops_in_time_keeps_its_parent(self):
        parent = QObject()
        th = QThread(parent)
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        utils.join_qthread(th)
        self.assertIs(th.parent(), parent)
        self.assertNotIn(th, utils._DETACHED_THREADS)

    def test_detaching_a_dead_wrapper_is_not_an_error(self):
        dead = QThread()
        sip.delete(dead)
        utils.detach_running_thread(dead)   # close paths race Qt's own delete
        self.assertNotIn(dead, utils._DETACHED_THREADS)

    def test_join_qthread_tolerates_none_and_running(self):
        utils.join_qthread(None)   # close paths pass whatever they have

        dead = QThread()
        sip.delete(dead)
        utils.join_qthread(dead)   # wrapper outlived its C++ object

        gate = threading.Event()
        th = QThread()
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        th.start()
        deadline = time.monotonic() + 5.0
        while not th.isRunning() and time.monotonic() < deadline:
            time.sleep(0.01)

        gate.set()
        utils.join_qthread(th, timeout_ms=5000)
        self.assertTrue(th.isFinished())
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_cancelled_run_with_progress_keeps_the_worker_pinned(self):
        """Cancel returns while fn is still executing on the thread — the
        exact case where freeing the worker early is a use-after-free. It must
        stay pinned until the thread really finishes, then be released."""
        class _CancelledDialog:
            def __init__(self, *_a, **_k):
                pass

            def __getattr__(self, _name):
                return lambda *_a, **_k: None

            def wasCanceled(self):
                return True

        gate = threading.Event()
        self.addCleanup(gate.set)
        with patch.object(utils, "QProgressDialog", _CancelledDialog):
            result, exc = utils.run_with_progress(
                None, "t", lambda _w: gate.wait(10))
        self.assertEqual((result, exc), (None, None))

        pinned = [(k, w, th) for k, (w, th) in utils._LIVE_WORKERS.items()
                  if isinstance(w, utils.FuncWorker) and not th.isFinished()]
        self.assertEqual(len(pinned), 1, "cancelled worker not pinned")
        key, worker, th = pinned[0]

        utils.reap_finished_workers()
        self.assertIn(key, utils._LIVE_WORKERS)   # still running: keep it

        gate.set()
        deadline = time.monotonic() + 5.0
        while not th.isFinished() and time.monotonic() < deadline:
            self._app.processEvents()
        utils.reap_finished_workers()
        self.assertNotIn(key, utils._LIVE_WORKERS)

        wref = weakref.ref(worker)
        del worker, pinned
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wref())

    def test_running_thread_keeps_its_worker_pinned(self):
        """A sweep while the worker is mid-run must not release it — that
        would destroy the object whose method is currently executing."""
        gate = threading.Event()
        th = QThread()
        wk = utils.FuncWorker(lambda _w: gate.wait(5))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)
        try:
            th.start()
            deadline = time.monotonic() + 5.0
            while not th.isRunning() and time.monotonic() < deadline:
                time.sleep(0.01)

            utils.reap_finished_workers()
            self.assertIn(id(wk), utils._LIVE_WORKERS)
        finally:
            gate.set()
        self.assertTrue(th.wait(5000))
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

    def test_reaper_is_a_singleton_and_flush_drains_batches(self):
        reaper = utils._reaper()
        self.assertIs(reaper, utils._reaper())

        class _Dummy:
            pass

        a, b = _Dummy(), _Dummy()
        wa, wb = weakref.ref(a), weakref.ref(b)
        reaper.bury((a, None))
        reaper.bury((b, None))
        del a, b
        self.assertIsNotNone(wa())
        self.assertIsNotNone(wb())

        deadline = time.monotonic() + 5.0
        while (wa() is not None or wb() is not None) \
                and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wa())
        self.assertIsNone(wb())

    def test_pending_delivery_still_runs_before_the_release(self):
        """REGRESSION (segfault): the exact crash sequence — the worker emits,
        finishes, and the sweep runs before the GUI queue drains. The queued
        delivery must still reach a live object; only then may the worker (and
        the callables its connections hold) be freed."""
        hits = []
        th = QThread()
        wk = utils.FuncWorker(lambda w: w.progress.emit(7, 9))
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.progress.connect(lambda cur, total: hits.append((cur, total)))
        wk.done.connect(th.quit, Qt.ConnectionType.DirectConnection)
        utils.release_worker_on_finish(th, wk)

        th.start()
        # Deliberately no processEvents: the progress delivery is still queued
        # when the thread reports finished and the sweep runs.
        self.assertTrue(th.wait(5000))
        utils.reap_finished_workers()
        self.assertNotIn(id(wk), utils._LIVE_WORKERS)

        wref = weakref.ref(wk)
        del wk
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertEqual(hits, [(7, 9)])
        self.assertIsNone(wref())

    def test_run_with_progress_releases_its_worker(self):
        """End to end through the most-used call path: the worker must not
        outlive the call by more than the reaper's one queue trip."""
        created = []

        class _Recording(utils.FuncWorker):
            def __init__(self, fn):
                super().__init__(fn)
                created.append(self)

        with patch.object(utils, "FuncWorker", _Recording):
            result, exc = utils.run_with_progress(None, "t", lambda _w: 7)
        self.assertEqual((result, exc), (7, None))
        self.assertEqual(len(created), 1)

        wref = weakref.ref(created[0])
        created.clear()
        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
            utils.reap_finished_workers()
        self.assertIsNone(wref())

    def test_release_waits_out_pending_deliveries(self):
        """REGRESSION (segfault): dropping the last worker reference the
        moment its thread finished freed the callables its connections held
        while a delivery for one of them was still queued. The reaper must
        keep the worker alive until the event queue has cycled once."""
        th = QThread()
        wk = utils.FuncWorker(lambda _w: None)
        wk.moveToThread(th)
        th.started.connect(wk.run)
        wk.done.connect(th.quit)
        utils.release_worker_on_finish(th, wk)
        th.start()
        self._drain(th)

        wref = weakref.ref(wk)
        del wk
        utils.reap_finished_workers()
        # Out of the registry, but the reaper still holds it: nothing that
        # was queued before the flush can call into a freed object.
        self.assertIsNone(utils._LIVE_WORKERS.get(wref and id(wref())))
        self.assertIsNotNone(wref())

        deadline = time.monotonic() + 5.0
        while wref() is not None and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertIsNone(wref())
class WorkerTeardownPatternTests(unittest.TestCase):
    """REGRESSION (deadlock): no sip QObject may be destroyed in a worker
    thread. `x.moveToThread(t)` plus `something.connect(x.deleteLater)` runs
    x's C++ destructor on the worker thread; ~1 full-suite run in 25 froze at
    the GIL. Every such site must use release_worker_on_finish instead."""

    def test_no_deleteLater_on_moved_workers(self):
        offenders = []
        sources = read_app_sources()
        self.assertGreaterEqual(len(sources), MIN_APP_SOURCES,
                                "source discovery collapsed")
        saw_a_moved_worker = False
        for fname, src in sources:
            moved = set(re.findall(r"([A-Za-z_][\w.]*)\.moveToThread\(", src))
            saw_a_moved_worker = saw_a_moved_worker or bool(moved)
            for name in moved:
                esc = re.escape(name)
                if re.search(rf"\.connect\({esc}\.deleteLater\)", src):
                    offenders.append(f"{fname}: .connect({name}.deleteLater)")
                # Direct calls are only checkable for names that are
                # unmistakably workers; short names like `w` also belong to
                # widgets in these files.
                if "worker" in name.lower() and re.search(
                        rf"{esc}\.deleteLater\(\)", src):
                    offenders.append(f"{fname}: {name}.deleteLater()")
        self.assertTrue(saw_a_moved_worker,
                        "no moveToThread call found -- the audit scanned "
                        "nothing and would pass on any code")
        self.assertEqual(offenders, [])
class ConditionalUploadWorkerTests(unittest.TestCase):
    """A key taken mid-job is the outcome the user asked for, not a failure
    to abort the rest of the batch over."""

    class _Model:
        parallel_files = 1

        def __init__(self, taken=()):
            self.taken = set(taken)
            self.uploaded = []
            self.flags = []

        def upload_file(self, local, key, progress_cb=None, cancel_event=None,
                        log_fn=None, if_none_match=False):
            self.flags.append(if_none_match)
            if key in self.taken:
                raise ObjectExists(key)
            if progress_cb is not None:
                size = os.path.getsize(local)
                progress_cb(size, size, key)
            self.uploaded.append(key)

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def _file(self, name, size):
        path = os.path.join(self.dir, name)
        with open(path, "wb") as handle:
            handle.write(b"x" * size)
        return path

    def _run(self, model, job, if_none_match=True):
        worker = main_window.Worker(model, job, if_none_match=if_none_match)
        messages, failures = [], []
        self.batches = []
        worker.progress.connect(messages.append)
        worker.error.connect(failures.append)
        worker.batch_progress.connect(
            lambda done, total: self.batches.append((done, total)))
        worker.upload()
        return messages, failures

    def test_a_skipped_file_still_completes_the_batch_progress(self):
        """REGRESSION: a skipped object's bytes were counted into the batch
        total but never into the done count, so a job that skipped anything
        finished with the progress bar stuck short of the end — looking like
        a transfer that died rather than one that did what was asked."""
        model = self._Model(taken={"b.txt"})
        job = [("a.txt", self._file("a.txt", 100)),
               ("b.txt", self._file("b.txt", 400))]
        self._run(model, job)
        done, total = self.batches[-1]
        self.assertEqual(total, 500)
        self.assertEqual(done, 500, "the skipped file's bytes never landed")

    def test_a_partly_sent_skip_is_credited_once(self):
        """The conditional write reports progress as the body goes out and
        is only refused at the end, so some of a skipped file's bytes are
        already counted. Crediting the whole size again would overshoot."""
        sent = {}

        class _PartialModel(self._Model):
            def upload_file(self, local, key, progress_cb=None, **kw):
                size = os.path.getsize(local)
                if progress_cb is not None:
                    progress_cb(size, size // 2, key)
                    sent[key] = size // 2
                raise ObjectExists(key)

        job = [("a.txt", self._file("a.txt", 400))]
        self._run(_PartialModel(), job)
        done, total = self.batches[-1]
        self.assertEqual((done, total), (400, 400))

    def test_one_taken_key_does_not_stop_the_rest(self):
        model = self._Model(taken={"b.txt"})
        job = [("a.txt", self._file("a.txt", 10)),
               ("b.txt", self._file("b.txt", 10)),
               ("c.txt", self._file("c.txt", 10))]
        messages, failures = self._run(model, job)
        self.assertEqual(model.uploaded, ["a.txt", "c.txt"])
        self.assertEqual(failures, [], "a skip was reported as a failure")
        self.assertTrue(any("already exists" in m for m in messages), messages)

    def test_the_skips_are_counted_in_the_log(self):
        model = self._Model(taken={"a.txt", "b.txt"})
        messages, _ = self._run(
            model, [("a.txt", self._file("a.txt", 10)),
                    ("b.txt", self._file("b.txt", 10))])
        self.assertTrue(any("skipped 2 object(s)" in m for m in messages),
                        messages)

    def test_the_flag_reaches_every_file(self):
        model = self._Model()
        self._run(model, [("a.txt", self._file("a.txt", 10)),
                          ("b.txt", self._file("b.txt", 10))])
        self.assertEqual(model.flags, [True, True])

    def test_without_the_flag_the_model_is_asked_to_overwrite(self):
        model = self._Model()
        self._run(model, [("a.txt", self._file("a.txt", 10))],
                  if_none_match=False)
        self.assertEqual(model.flags, [False])
class SyncWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.uploads = []
            self.downloads = []
            self.deletes = []

        def upload_file(self, local, key, progress_cb=None, cancel_event=None,
                        log_fn=None):
            self.uploads.append((local, key))

        def download_file(self, key, local, folder, progress_cb=None,
                          cancel_event=None, log_fn=None):
            self.downloads.append((key, local))

        def delete(self, key, log_fn=None):
            self.deletes.append(key)

    def test_each_action_routes_to_the_right_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            victim = os.path.join(tmp, "gone.txt")
            with open(victim, "w") as handle:
                handle.write("x")
            up_path = os.path.join(tmp, "a.txt")
            # a nested target proves the parent directory gets created
            down_path = os.path.join(tmp, "nested", "b.txt")
            job = [
                ("upload", "a.txt", up_path, "pre/a.txt", 5),
                ("download", "nested/b.txt", down_path, "pre/b.txt", 7),
                ("delete_remote", "c.txt", os.path.join(tmp, "c.txt"),
                 "pre/c.txt", 0),
                ("delete_local", "gone.txt", victim, "pre/gone.txt", 0),
            ]
            model = self.FakeModel()
            worker = Worker(model, job)
            finished = []
            worker.finished.connect(finished.append)
            worker.sync()
            self.assertEqual(model.uploads, [(up_path, "pre/a.txt")])
            self.assertEqual(model.downloads, [("pre/b.txt", down_path)])
            self.assertTrue(os.path.isdir(os.path.join(tmp, "nested")))
            self.assertEqual(model.deletes, ["pre/c.txt"])
            self.assertFalse(os.path.exists(victim))
            self.assertEqual(finished, [False])

    def test_cancel_stops_before_any_work(self):
        model = self.FakeModel()
        worker = Worker(model, [("upload", "a", "/l/a", "k/a", 1)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.sync()
        self.assertEqual(model.uploads, [])
        self.assertEqual(finished, [True])

    def test_failure_is_reported(self):
        class Failing(self.FakeModel):
            def upload_file(self, *_a, **_kw):
                raise RuntimeError("nope")

        worker = Worker(Failing(), [("upload", "a", "/l/a", "k/a", 1)])
        errors = []
        worker.error.connect(errors.append)
        worker.sync()
        self.assertEqual(errors, ["nope"])
class ZipDownloadWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.blobs = {"pre/a.txt": b"alpha", "pre/dir/b.txt": b"bravo"}

        def get_keys(self, prefix, log_fn=None):
            return [(k, len(v)) for k, v in self.blobs.items()
                    if k.startswith(prefix)]

        def get_size(self, key):
            return len(self.blobs.get(key, b""))

        def stream_object(self, key, chunk_size=1024 * 1024, cancel_event=None):
            yield self.blobs[key]

    def test_files_and_folders_land_in_one_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            job = [
                (zip_path, "pre/", "pre/a.txt", False),
                (zip_path, "pre/", "pre/dir/", True),
            ]
            worker = Worker(self.FakeModel(), job)
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [False])
            with zipfile.ZipFile(zip_path) as zf:
                # arcnames are relative to the browsed prefix
                self.assertEqual(sorted(zf.namelist()), ["a.txt", "dir/b.txt"])
                self.assertEqual(zf.read("dir/b.txt"), b"bravo")

    def test_cancelling_before_it_starts_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            worker = Worker(self.FakeModel(),
                            [(zip_path, "pre/", "pre/a.txt", False)])
            worker.cancel()
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [True])
            self.assertFalse(os.path.exists(zip_path))

    def test_cancelling_midway_deletes_the_partial_archive(self):
        """The archive is already open by then, so the cleanup path has to
        actually remove it — a truncated zip cannot be opened."""
        outer = self

        class CancelMidway(outer.FakeModel):
            def __init__(self):
                super().__init__()
                self.cancel_event = None

            def stream_object(self, key, chunk_size=1024 * 1024,
                              cancel_event=None):
                yield b"partial"
                self.cancel_event.set()
                raise TransferCancelled("cancelled")

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            model = CancelMidway()
            worker = Worker(model, [(zip_path, "pre/", "pre/a.txt", False)])
            model.cancel_event = worker._cancel_event
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [True])
            self.assertFalse(os.path.exists(zip_path))

    def test_failure_is_reported(self):
        class Failing(self.FakeModel):
            def stream_object(self, key, chunk_size=1024 * 1024,
                              cancel_event=None):
                raise RuntimeError("read error")
                yield b""      # pragma: no cover

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            worker = Worker(Failing(), [(zip_path, "pre/", "pre/a.txt", False)])
            errors = []
            worker.error.connect(errors.append)
            worker.zip_download()
            self.assertEqual(errors, ["read error"])

    def test_a_failed_archive_is_removed_not_left_behind(self):
        """A half-written zip opens and is simply missing its tail, which is
        worse than no zip at all. Cancel already cleaned up; failure kept the
        partial file and reported an error beside it."""
        class FailingMidway(self.FakeModel):
            def stream_object(self, key, chunk_size=1024 * 1024,
                              cancel_event=None):
                yield b"partial"
                raise RuntimeError("connection reset")

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "out.zip")
            worker = Worker(FailingMidway(),
                            [(zip_path, "pre/", "pre/a.txt", False)])
            finished = []
            worker.finished.connect(finished.append)
            worker.zip_download()
            self.assertEqual(finished, [False])   # failed, not cancelled
            self.assertFalse(os.path.exists(zip_path))
class SetTagsWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    class FakeModel:
        def __init__(self):
            self.calls = []

        def get_keys(self, prefix, log_fn=None):
            return [(prefix + "a.txt", 1), (prefix + "b.txt", 2), (prefix, 0)]

        def update_object_tags(self, key, add=None, remove=None, replace=False):
            self.calls.append((key, dict(add or {}), list(remove or []), replace))

    def test_folders_fan_out_to_every_object(self):
        model = self.FakeModel()
        job = [("f.txt", False, {"env": "prod"}, [], False),
               ("dir/", True, {"env": "prod"}, [], False)]
        worker = Worker(model, job)
        worker.set_tags()
        keys = [c[0] for c in model.calls]
        self.assertIn("f.txt", keys)
        self.assertIn("dir/a.txt", keys)
        self.assertIn("dir/b.txt", keys)
        self.assertNotIn("dir/", keys)       # placeholder skipped
        self.assertTrue(all(c[1] == {"env": "prod"} for c in model.calls))

    def test_remove_and_replace_are_forwarded(self):
        model = self.FakeModel()
        Worker(model, [("f.txt", False, {}, ["old"], True)]).set_tags()
        self.assertEqual(model.calls, [("f.txt", {}, ["old"], True)])

    def test_cancel_stops_early(self):
        model = self.FakeModel()
        worker = Worker(model, [("f.txt", False, {"a": "b"}, [], False)])
        worker.cancel()
        finished = []
        worker.finished.connect(finished.append)
        worker.set_tags()
        self.assertEqual(model.calls, [])
        self.assertEqual(finished, [True])
class TransientErrorTests(unittest.TestCase):
    """botocore already retries inside one request; this is about the job
    above it, which it gave up on."""

    def _error(self, code="", status=None):
        response = {"Error": {"Code": code, "Message": "x"},
                    "ResponseMetadata": {}}
        if status is not None:
            response["ResponseMetadata"]["HTTPStatusCode"] = status
        return botocore.exceptions.ClientError(response, "PutObject")

    def test_the_services_own_try_again_answers_count(self):
        for code in ("SlowDown", "InternalError", "ServiceUnavailable",
                     "RequestTimeout", "ThrottlingException"):
            with self.subTest(code=code):
                self.assertTrue(utils.is_transient_error(self._error(code)))

    def test_server_side_status_codes_count(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.assertTrue(
                    utils.is_transient_error(self._error("Weird", status)))

    def test_a_refusal_is_not_transient(self):
        """An AccessDenied retried on a timer is just a slower AccessDenied."""
        for code, status in (("AccessDenied", 403), ("NoSuchKey", 404),
                             ("InvalidRequest", 400)):
            with self.subTest(code=code):
                self.assertFalse(
                    utils.is_transient_error(self._error(code, status)))

    def test_connection_failures_never_reached_the_service(self):
        self.assertTrue(utils.is_transient_error(
            botocore.exceptions.EndpointConnectionError(
                endpoint_url="https://s3.example")))

    def test_an_ordinary_exception_is_not_transient(self):
        self.assertFalse(utils.is_transient_error(ValueError("nope")))
class AutoRetryLifetimeTests(unittest.TestCase):
    """REGRESSION (abort at exit): the automatic retry used
    QTimer.singleShot, which keeps firing after the window is gone — five
    seconds later it started a transfer against a torn-down model. The suite
    caught it as an intermittent core dump long after every test had passed."""

    ROOT = PROJECT_ROOT

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _window(self):
        settings = QSettings("s3duck-tests-retrylife", "s3duck-tests-retrylife")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        return win

    def _failed(self, win, entry_id=1):
        entry = main_window._QEntry(entry_id, "upload", [("k", "/tmp/f")],
                                    label="job")
        entry.status = "error"
        entry.error_transient = True
        win._queue_entries[entry_id] = entry
        return entry

    def test_the_retry_timer_belongs_to_the_window(self):
        """A parented timer is destroyed with it; a static one is not."""
        win = self._window()
        self.assertIs(win._retry_timer.parent(), win)
        self.assertTrue(win._retry_timer.isSingleShot())

    def test_scheduling_arms_the_timer_and_holds_the_entry(self):
        win = self._window()
        entry = self._failed(win)
        self.assertTrue(win._maybe_auto_retry(entry))
        self.assertTrue(win._retry_timer.isActive())
        self.assertEqual(win._pending_retries, [entry])

    def test_firing_requeues_what_was_held(self):
        win = self._window()
        entry = self._failed(win)
        win._maybe_auto_retry(entry)
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: queued.append(m)):
            win._run_pending_retries()
        self.assertEqual(queued, ["upload"])
        self.assertEqual(win._pending_retries, [])

    def test_a_closing_window_starts_nothing(self):
        win = self._window()
        entry = self._failed(win)
        win._maybe_auto_retry(entry)
        win._shutdown_threads()
        queued = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, m, j, **kw: queued.append(m)):
            win._run_pending_retries()
        self.assertEqual(queued, [])

    def test_shutdown_disarms_the_timer(self):
        win = self._window()
        win._maybe_auto_retry(self._failed(win))
        win._shutdown_threads()
        self.assertFalse(win._retry_timer.isActive())
        self.assertEqual(win._pending_retries, [])
