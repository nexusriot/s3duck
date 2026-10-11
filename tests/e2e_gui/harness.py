"""Shared setup for the GUI end-to-end suite: a real MainWindow pointed at a
throwaway bucket, and the event-loop pumping that makes asynchronous workers
testable."""

import contextlib
import os
import sys
import time
import unittest
import uuid
from unittest.mock import patch

import boto3
from PyQt6.QtCore import QCoreApplication, QSettings, qInstallMessageHandler
from PyQt6.QtWidgets import QApplication, QDialog, QFileDialog, QMessageBox

from ui.dialogs.history import OverwriteDialog

import main_window
from core.profile import ProfileSettings
from tests.e2e.harness import endpoint, purge_bucket, unique_bucket, _env

# How long a worker gets before a test gives up on it. Generous because the
# first call on a cold MinIO pays for the connection, and a timeout here
# reads as a hang rather than as the assertion that actually failed.
WORKER_TIMEOUT = 30.0

# How often the pump looks at its predicate. Short enough that a fast worker
# does not add perceptible latency to the suite, long enough that the loop is
# not a spin.
POLL = 0.01


# Messages the offscreen platform emits for operations a real window manager
# would handle and it cannot. They say nothing about the application and there
# is one per navigation, which buries the test output.
#
# Filtered by exact text rather than with QT_LOGGING_RULES: these carry no
# logging category, so the only rule that silences them is "*=false", which
# would also hide the Qt warnings that ARE evidence.
_EXPECTED_OFFSCREEN_NOISE = (
    "This plugin does not support raise()",
    "This plugin does not support propagateSizeHints()",
    "This plugin does not support grabWindow()",
)


def _quiet_offscreen_noise(_mode, _context, message):
    if any(known in message for known in _EXPECTED_OFFSCREEN_NOISE):
        return
    print(message, file=sys.stderr)


def _ensure_qapp():
    """The shared QApplication, created once, offscreen.

    Qt reads the platform when the application is constructed, not on import,
    so setting it here still works for a stray invocation that missed
    tests/__init__.py.
    """
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance()
    if app is None:
        qInstallMessageHandler(_quiet_offscreen_noise)
        app = QApplication([])
        # QSettings needs these before it will write anywhere sensible.
        QCoreApplication.setOrganizationName("s3duck-e2e")
        QCoreApplication.setApplicationName("s3duck-e2e")
    return app


class GuiServerCase(unittest.TestCase):
    """A real MainWindow against a real bucket, both thrown away afterwards.

    The window is built per class rather than per test: constructing it costs
    more than everything else here put together, and the tests inside a class
    are written not to collide on keys.
    """

    # Extra profile fields, for a class that needs a different kind of window
    # (read-only, say). Merged over the defaults below.
    PROFILE = {}

    @classmethod
    def setUpClass(cls):
        if not endpoint():
            raise unittest.SkipTest("S3DUCK_TEST_ENDPOINT not set")

        cls.app = _ensure_qapp()
        cls.bucket = unique_bucket("gui")

        # A client of our own, never the window's: a fixture built through the
        # code under test cannot contradict it, which is the whole point of
        # asking the server separately.
        cls.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint(),
            aws_access_key_id=_env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=_env("S3DUCK_TEST_SECRET_KEY", "minioadmin"),
            region_name=_env("S3DUCK_TEST_REGION", "us-east-1"),
        )
        cls.s3.create_bucket(Bucket=cls.bucket)

        # One window for the whole process, re-pointed at each class's bucket.
        #
        # A window per class is what this started as, and it does not survive:
        # tearing several MainWindows down in one interpreter double-frees
        # (each closed window still owns threads, timers and a model), and
        # parking them instead merely trades the crash for a hang. Re-pointing
        # one window is also closer to what the application does — a user
        # switches buckets, they do not get a new window — so the shared path
        # is the realistic one as well as the stable one.
        #
        # A class that needs a *different profile* still gets its own window;
        # see PROFILE.
        if cls.PROFILE:
            cls.window = cls._build_window()
            cls._own_window = True
        else:
            cls.window = _shared_window(cls._project_profile())
            cls._own_window = False
            cls.window.data_model.enter_bucket(cls.bucket)
            cls.window.change_current_folder("")

    def setUp(self):
        # A modal dialog nobody can answer does not fail a headless run — it
        # hangs it, and a hung suite tells you nothing about which gesture
        # opened the dialog. Every QDialog.exec is refused by default, so an
        # unexpected modal is a named failure instead; the context managers
        # below allow the specific ones a test means to answer.
        self._allow_modal(None)

    def _allow_modal(self, allowed):
        """Refuse QDialog.exec, except for the type in ``allowed``."""
        patcher = patch.object(QDialog, "exec", autospec=True,
                               side_effect=self._refuse_modal(allowed))
        patcher.start()
        self.addCleanup(patcher.stop)

    def _refuse_modal(self, allowed):
        test = self

        def guard(dialog, *a, **kw):
            if allowed is not None and isinstance(dialog, allowed):
                return QDialog.DialogCode.Accepted
            test.fail(
                f"{type(dialog).__name__} opened a modal dialog this test did "
                f"not expect; headless there is nobody to answer it. Wrap the "
                f"call in the matching harness context manager.")
        return guard

    @contextlib.contextmanager
    def answering_overwrite(self, choice=OverwriteDialog.OVERWRITE):
        """Answer the destination-already-exists dialog with ``choice``."""
        with patch.object(QDialog, "exec", autospec=True,
                          side_effect=self._refuse_modal(OverwriteDialog)), \
                patch.object(OverwriteDialog, "choice", return_value=choice):
            yield

    @classmethod
    def _build_window(cls, **profile_overrides):
        fields = dict(
            profile_name="e2e",
            url=endpoint(),
            region=_env("S3DUCK_TEST_REGION", "us-east-1"),
            bucket=cls.bucket,
            access_key=_env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
            secret_key=_env("S3DUCK_TEST_SECRET_KEY", "minioadmin"),
            # A MinIO reached by container name has no per-bucket DNS, and its
            # certificate (when there is one) is not ours to trust.
            use_path=True,
            no_ssl_check=True,
        )
        fields.update(cls.PROFILE)
        fields.update(profile_overrides)
        # current_dir is where the window resolves its icons from; the project
        # root keeps a headless run from logging a miss for every one of them.
        window = main_window.MainWindow(
            current_dir=_project_root(),
            qsettings=QSettings(),
            profile=ProfileSettings(**fields),
        )
        window.data_model.enter_bucket(cls.bucket)
        return window

    @classmethod
    def tearDownClass(cls):
        window = getattr(cls, "window", None)
        # Only a window this class built is closed here. The shared one lives
        # until the process ends, which is the arrangement that keeps Qt from
        # tearing down a window while another class still holds it.
        if window is not None and getattr(cls, "_own_window", False):
            close_window(window, cls.app)
        if getattr(cls, "s3", None) is not None:
            purge_bucket(cls.s3, cls.bucket)

    @classmethod
    def _project_profile(cls):
        """The profile fields the shared window is built with — everything but
        the bucket, which is re-pointed per class."""
        return dict(
            profile_name="e2e",
            url=endpoint(),
            region=_env("S3DUCK_TEST_REGION", "us-east-1"),
            access_key=_env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
            secret_key=_env("S3DUCK_TEST_SECRET_KEY", "minioadmin"),
            use_path=True,
            no_ssl_check=True,
        )

    # Driving the event loop.

    def pump(self, predicate, timeout=WORKER_TIMEOUT, what="condition"):
        """Run the Qt event loop until ``predicate()`` is true.

        This is what makes an asynchronous worker testable: the worker runs on
        its own QThread and reports back through queued signals, which are
        only delivered while the receiving thread is in its event loop. A
        plain ``sleep`` would never see the result.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.app.processEvents()
            if predicate():
                return
            time.sleep(POLL)
        # One last look: the predicate may have become true during the final
        # processEvents, and failing there would be a lie about a passing run.
        self.app.processEvents()
        if predicate():
            return
        self.fail(f"timed out after {timeout:g}s waiting for {what}")

    def navigate(self, prefix=None, timeout=WORKER_TIMEOUT):
        """Point the window at ``prefix`` and wait for the listing to land.

        ``force=True`` is what the window's own refresh callbacks pass; it
        bypasses the "a transfer is running" guard, which is about protecting
        the user from a listing that shifts under them rather than about
        correctness.
        """
        if prefix is not None:
            self.window.change_current_folder(prefix)
        # navigate() disables the list view and _on_navigation_finished turns
        # it back on, so this is the window's own definition of "done" rather
        # than a guess at thread state — and it is what the user sees.
        self.window.listview.setEnabled(False)
        self.window.navigate(force=True)
        self.pump(
            lambda: self.window.listview.isEnabled(),
            timeout=timeout,
            what="the navigation worker to finish",
        )

    def listing(self):
        """The labels currently shown in the list view, as the user sees them."""
        model = self.window.model
        return sorted(model.item(i).text() for i in range(model.rowCount()))

    def await_transfers(self, timeout=WORKER_TIMEOUT):
        """Wait for the transfer queue to drain."""
        self.pump(
            lambda: not self.window.transfers_active()
            and not self.window._transfer_queue,
            timeout=timeout,
            what="the transfer queue to drain",
        )

    def queue_statuses(self):
        """Every queue entry's final status, newest last."""
        entries = sorted(self.window._queue_entries.values(),
                         key=lambda e: e.entry_id)
        return [e.status for e in entries]

    def last_queue_entry(self):
        entries = self.window._queue_entries
        if not entries:
            return None
        return entries[max(entries)]

    # Asking the server, not the app.

    def put(self, key, body=b"x", **kw):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, **kw)
        return key

    def keys(self, prefix=""):
        out = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            out.extend(o["Key"] for o in (page.get("Contents") or []))
        return sorted(out)

    def body(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    # Stubbing the operating system's dialogs. These are the only fakes in
    # this suite: a file picker and a message box are the OS asking the user
    # something, headless there is nobody to answer, and faking them is what
    # lets the REAL upload/download/delete paths run.

    @contextlib.contextmanager
    def choosing_files(self, *paths):
        """``QFileDialog.getOpenFileNames`` answers with ``paths``."""
        with patch.object(QFileDialog, "getOpenFileNames",
                          return_value=(list(paths), "All files (*)")):
            yield

    @contextlib.contextmanager
    def choosing_directory(self, path):
        """``QFileDialog.getExistingDirectory`` answers with ``path``."""
        with patch.object(QFileDialog, "getExistingDirectory",
                          return_value=path):
            yield

    @contextlib.contextmanager
    def confirming(self, answer=QMessageBox.StandardButton.Yes):
        """Every confirmation is answered with ``answer``.

        Both ``question`` and ``warning`` are stubbed: which one a path uses is
        a presentation choice, and a test that pinned it would fail on a purely
        cosmetic change.
        """
        with patch.object(QMessageBox, "question", return_value=answer), \
                patch.object(QMessageBox, "warning", return_value=answer), \
                patch.object(QMessageBox, "critical", return_value=answer), \
                patch.object(QMessageBox, "information", return_value=answer):
            yield

    @contextlib.contextmanager
    def recording_messages(self):
        """Collect what the window would have shown, instead of showing it.

        Yields the list; each entry is ``(kind, title, text)``.
        """
        seen = []

        def record(kind):
            def handler(_parent, title, text, *a, **kw):
                seen.append((kind, title, text))
                return QMessageBox.StandardButton.Ok
            return handler

        with patch.object(QMessageBox, "critical", record("critical")), \
                patch.object(QMessageBox, "warning", record("warning")), \
                patch.object(QMessageBox, "information", record("information")):
            yield seen


# Closed windows are kept alive to the end of the process on purpose; see
# close_window.
_CLOSED = []

# The window every class shares unless it asked for its own; see setUpClass.
_SHARED = None


def _shared_window(profile_fields):
    """The process-wide MainWindow, built on first use."""
    global _SHARED
    if _SHARED is None:
        fields = dict(profile_fields)
        fields.setdefault("bucket", "")
        _SHARED = main_window.MainWindow(
            current_dir=_project_root(),
            qsettings=QSettings(),
            profile=ProfileSettings(**fields),
        )
    return _SHARED


def close_window(window, app):
    """Shut a window down the way the application does, and let its threads go.

    Without the pump afterwards, Qt tears the C++ objects down while a worker
    is still running and the run ends in "QThread: Destroyed while thread is
    still running" — which aborts the interpreter often enough to turn a green
    suite red at random.
    """
    try:
        window.close()
    except Exception:
        pass
    deadline = time.monotonic() + WORKER_TIMEOUT
    while time.monotonic() < deadline:
        app.processEvents()
        try:
            if not window.transfers_active():
                break
        except Exception:
            break
        time.sleep(POLL)
    # Deliberately NOT deleteLater(): that schedules the C++ object for
    # deletion while this Python wrapper still owns a reference, and the
    # second teardown — whichever comes first — double-frees. The window is
    # parked instead, and the process reclaims everything at exit.
    _CLOSED.append(window)
    app.processEvents()


def _project_root():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(os.path.dirname(here))


def unique_key(tag="obj"):
    return f"{tag}-{uuid.uuid4().hex[:8]}"
