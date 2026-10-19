"""Process-level concerns: CLI, instance handover, logging."""

import botocore.session
import diagnostics
import main_window
import os
import profile_switcher
import s3duck
import shutil
import tempfile
import time
import unittest
import utils
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QDialogButtonBox
from model import Model, TransferCancelled, installed_sdk_accepts
from tests.support import (
    PROJECT_ROOT, FakeS3Client, _MultipartClient, _StubModel, _ensure_qapp,
    make_main_window, make_model, patch_app_wide,
)
from unittest.mock import patch
from utils import TempWorkspace, pid_is_alive


class TempWorkspaceTests(unittest.TestCase):
    """REGRESSION (data left behind): previews, 'open with default app' and
    drag-out staging wrote decrypted object payloads into the system temp
    directory and nothing ever removed them."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def test_directories_are_created_under_one_session_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        first, second = ws.make(prefix="preview_"), ws.make(prefix="drag_")
        self.assertTrue(os.path.isdir(first))
        self.assertTrue(os.path.isdir(second))
        self.assertEqual(os.path.dirname(first), ws.session_dir)
        self.assertEqual(os.path.dirname(second), ws.session_dir)
        self.assertTrue(
            os.path.basename(ws.session_dir).startswith("s3duck_1234_"))

    def test_cleanup_removes_everything_including_staged_payloads(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        staged = os.path.join(ws.make(), "secret.txt")
        with open(staged, "w") as fh:
            fh.write("payload")
        session = ws.session_dir
        ws.cleanup()
        self.assertFalse(os.path.exists(staged))
        self.assertFalse(os.path.exists(session))

    def test_cleanup_is_safe_to_repeat(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        ws.make()
        ws.cleanup()
        ws.cleanup()          # must not raise on an already-removed root

    def test_a_later_make_after_cleanup_starts_a_new_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        first = ws.session_dir
        ws.cleanup()
        self.assertNotEqual(ws.session_dir, first)
        self.assertTrue(os.path.isdir(ws.session_dir))

    def test_sweep_reclaims_roots_whose_owner_is_gone(self):
        os.makedirs(os.path.join(self.root, "s3duck_4242_abc"))
        ws = TempWorkspace(root=self.root, pid=1234)
        removed = ws.sweep(is_alive=lambda pid: False)
        self.assertEqual(
            removed, [os.path.join(self.root, "s3duck_4242_abc")])
        self.assertFalse(os.path.exists(removed[0]))

    def test_sweep_never_touches_a_running_instance(self):
        """A second s3duck may be using its own staging right now."""
        live = os.path.join(self.root, "s3duck_4242_abc")
        os.makedirs(live)
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: True), [])
        self.assertTrue(os.path.isdir(live))

    def test_sweep_never_touches_our_own_root(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        mine = ws.session_dir
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])
        self.assertTrue(os.path.isdir(mine))

    def test_sweep_ignores_unrelated_directories(self):
        # "aaaaaaa9999_x" is the dangerous shape: drop the prefix check and
        # its 8th character onward parses as a PID, so another application's
        # temp directory would be deleted.
        names = ("not-ours", "s3duck_notanumber_x", "s3duck_", "aaaaaaa9999_x")
        for name in names:
            os.makedirs(os.path.join(self.root, name))
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])
        for name in names:
            self.assertTrue(os.path.isdir(os.path.join(self.root, name)),
                            f"{name} was swept")

    def test_owner_pid_only_reads_our_own_names(self):
        ws = TempWorkspace(root=self.root, pid=1234)
        self.assertEqual(ws.owner_pid("s3duck_42_abc"), 42)
        self.assertIsNone(ws.owner_pid("aaaaaaa9999_x"))
        self.assertIsNone(ws.owner_pid("s3duck_nope_x"))

    def test_sweep_survives_a_missing_root(self):
        ws = TempWorkspace(root=os.path.join(self.root, "gone"), pid=1)
        self.assertEqual(ws.sweep(is_alive=lambda pid: False), [])

    def test_pid_liveness_errs_towards_keeping_files(self):
        """An unknown answer must never authorise a delete."""
        self.assertTrue(pid_is_alive(os.getpid()))
        self.assertTrue(pid_is_alive("not-a-pid"))
        self.assertTrue(pid_is_alive(None))
        with patch.object(utils.os, "kill", side_effect=PermissionError):
            self.assertTrue(pid_is_alive(1))
        with patch.object(utils.os, "kill", side_effect=ProcessLookupError):
            self.assertFalse(pid_is_alive(999999))

    def test_the_window_cleans_up_on_close(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        win.temp_workspace = TempWorkspace(root=self.root, pid=1234)
        staged = win.temp_workspace.make()
        win.close()
        self.assertFalse(os.path.exists(staged))
class ReviewRegressionTests(unittest.TestCase):
    """Findings from the 0.16.0 review, each reproduced before being fixed."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    ROOT = PROJECT_ROOT

    @staticmethod
    def _region_error():
        return botocore.exceptions.ClientError(
            {"Error": {"Code": "PermanentRedirect",
                       "Message": "The bucket is in another region"}},
            "UploadPart")

    def _big_file(self, part=1024, parts=3):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        payload = os.urandom(part * parts)
        path = os.path.join(directory, "big.bin")
        with open(path, "wb") as handle:
            handle.write(payload)
        return path, payload, directory

    def _model(self, client, directory, part=1024):
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        model.upload_chunk_size = part
        model.multipart_threshold_mb = 0
        model.transfer_concurrency = 1
        model.upload_state_dir = os.path.join(directory, "state")
        model._client = client
        return model

    def test_a_resumable_upload_retries_after_a_region_error(self):
        """FINDING: the managed upload rebinds and retries once on a
        region/endpoint mismatch; the multipart path did not, so the SAME
        bucket failed for a large file and succeeded for a small one."""
        path, payload, directory = self._big_file()
        client = _MultipartClient()
        model = self._model(client, directory)

        real_upload_part = client.upload_part
        state = {"raised": False}

        def _once(**kw):
            if not state["raised"]:
                state["raised"] = True
                raise self._region_error()
            return real_upload_part(**kw)

        client.upload_part = _once
        rebinds = []

        def _rebind(inner, log_fn=None):
            # A real rebind MOVES the endpoint, which is what made the resume
            # record unfindable on the retry.
            inner.endpoint_url = "https://s3.eu-central-1.amazonaws.com"
            rebinds.append(1)

        with patch.object(Model, "rebind_bucket", _rebind):
            model.upload_file(path, "big.bin")
        self.assertEqual(len(rebinds), 1, "did not rebind and retry")
        self.assertEqual(client.completed["big.bin"], payload)
        self.assertEqual(client.created, 1,
                         "retry started a second multipart upload, orphaning "
                         "the first one's parts")

    def test_a_non_region_error_is_not_retried(self):
        """Retrying a genuine failure would double every upload."""
        path, _payload, directory = self._big_file()
        client = _MultipartClient(fail_on_part=1)
        model = self._model(client, directory)
        rebinds = []
        with patch.object(Model, "rebind_bucket",
                          lambda _s, log_fn=None: rebinds.append(1)):
            with self.assertRaises(Exception):
                model.upload_file(path, "big.bin")
        self.assertEqual(rebinds, [])

    def test_part_listing_cannot_loop_forever(self):
        """FINDING: a truncated response whose marker does not advance spun
        the transfer thread forever. S3-compatible backends differ in what
        they return, and this app exists to talk to them."""
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        calls = []

        class _Stuck:
            def list_parts(self, **kw):
                calls.append(kw.get("PartNumberMarker"))
                if len(calls) > 5:
                    raise RuntimeError("would have looped forever")
                # Truncated, but the marker never moves on.
                return {"Parts": [{"PartNumber": 1, "ETag": '"e1"', "Size": 4}],
                        "IsTruncated": True}

        model._client = _Stuck()
        parts = model._uploaded_parts("k", "upload-1")
        self.assertEqual(sorted(parts), [1])
        self.assertLessEqual(len(calls), 5)

    def _sync_dialog(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        model = Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False)
        dlg = main_window.CrossProfileSyncDialog(
            None, None, model, "pre/", settings, current_profile="prof")
        self.addCleanup(dlg.close)
        return dlg

    def test_changing_the_direction_invalidates_the_plan(self):
        """FINDING (data loss): the plan stayed runnable after its inputs
        changed, so a plan reviewed as a push could be executed after
        switching to pull — deleting at the side believed to be the source."""
        dlg = self._sync_dialog()
        dlg._actions = [{"action": "upload", "rel": "a", "size": 1,
                         "reason": "missing"}]
        dlg._source_prefix, dlg._dest_prefix = "pre/", "dst/"
        dlg._dest_model = object()
        dlg._ok_enabled(True)

        dlg._direction.setCurrentIndex(1)

        self.assertEqual(dlg._actions, [])
        self.assertEqual(dlg.plan()[2], [])
        self.assertFalse(dlg._buttons.button(
            QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_editing_any_input_invalidates_the_plan(self):
        for change in ("prefix", "exclude", "delete_extra"):
            with self.subTest(change=change):
                dlg = self._sync_dialog()
                dlg._actions = [{"action": "upload", "rel": "a", "size": 1,
                                 "reason": "missing"}]
                dlg._ok_enabled(True)
                if change == "prefix":
                    dlg.picker.prefix.setText("other/")
                elif change == "exclude":
                    dlg._exclude.setText("*.tmp")
                else:
                    dlg._delete_extra.setChecked(True)
                self.assertEqual(dlg._actions, [], f"{change} kept the plan")
                self.assertFalse(dlg._buttons.button(
                    QDialogButtonBox.StandardButton.Ok).isEnabled())

    def test_accepting_a_picker_dialog_stops_its_loader(self):
        """FINDING (crash): the bucket fetch was only stopped in closeEvent,
        which accept() never fires — so a dialog dismissed while the listing
        was in flight was destroyed with a live QThread, which aborts the
        process in PyQt6."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        for factory in (
            lambda: main_window.CrossProfileCopyDialog(
                None, settings, 1, current_profile="prof"),
            lambda: main_window.CrossProfileSyncDialog(
                None, None,
                Model("https://s3.amazonaws.com", "us-east-1", "AK", "SK",
                      "bkt", False, False),
                "pre/", settings, current_profile="prof"),
        ):
            stopped = []
            with patch.object(main_window.ProfilePicker, "stop_loader",
                              lambda _s: stopped.append(1)):
                dlg = factory()
                self.addCleanup(dlg.close)
                dlg.accept()
            self.assertTrue(stopped, "loader left running after accept()")

    def _window(self):
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings)
        self.addCleanup(win.close)
        return win

    def test_retrying_a_cross_profile_job_keeps_its_models(self):
        """FINDING: retry re-queued the job without the destination model, so
        the worker refused it — the retry button could never succeed."""
        win = self._window()
        dest = object()
        source = object()
        started = []
        entry = main_window._QEntry(
            entry_id=1, method="copy_to_profile",
            job=[("a", "b", False)], dest_model=dest, source_model=source)
        entry.status = "error"
        win._queue_entries[1] = entry
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(kw)):
            win._on_queue_retry_requested(1)
        self.assertEqual(len(started), 1)
        self.assertIs(started[0].get("dest_model"), dest)
        self.assertIs(started[0].get("source_model"), source)

    def test_history_refuses_to_rerun_a_cross_profile_job(self):
        """A stored job cannot carry another profile's live credentials, so
        queueing it would produce a job that can only fail."""
        win = self._window()
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(method)), \
             patch_app_wide("QMessageBox") as box:
            win.rerun_history_entry(
                {"method": "sync_to_profile", "job": [["copy", "a", "b"]]})
        self.assertEqual(started, [])
        self.assertTrue(box.information.called or box.warning.called)

    def test_history_still_reruns_ordinary_jobs(self):
        win = self._window()
        started = []
        with patch.object(main_window.MainWindow, "assign_thread_operation",
                          lambda _s, method, job, **kw: started.append(method)):
            win.rerun_history_entry(
                {"method": "upload", "job": [["k", "/tmp/f"]],
                 "origin": win._transfer_origin(),
                 "source_bucket": win.data_model.bucket})
        self.assertEqual(started, ["upload"])

    def test_switching_profiles_saves_where_the_old_one_was(self):
        """FINDING: the location is stored per profile, but a runtime switch
        reassigned profile_name first, so the profile being left behind never
        recorded where it was."""
        settings = QSettings("s3duck-tests", "s3duck-tests")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(self.ROOT, settings, profile_name='prod')
        self.addCleanup(win.close)
        win.data_model.bucket = "logs"
        win.data_model.current_folder = "2026/"

        profile = profile_switcher.Profile(
            name="dev", url="https://minio.local", region="us-east-1",
            bucket="", access_key="AK", secret_key="SK",
            no_ssl_check=False, use_path=True)
        win.apply_profile(profile)

        settings.beginGroup("common")
        self.assertEqual(settings.value("last_location/prod"), "logs/2026/")
        settings.endGroup()
        self.assertEqual(win.profile_name, "dev")
class _DeniedListClient(FakeS3Client):
    """A bucket the credentials cannot read."""

    def get_paginator(self, name):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "ListObjectsV2")


class _RefusingPutClient(FakeS3Client):
    """A bucket that refuses the probe key but answers the cleanup."""

    def put_object(self, **kw):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "QuotaExceeded",
                       "Message": "Bucket quota exceeded"}},
            "PutObject")


class _DeleteOnlyModel:
    """Just enough model for Worker.delete()."""

    parallel_files = 1

    def __init__(self, on_delete):
        self.delete = on_delete


class Review024RegressionTests(unittest.TestCase):
    """Findings from the 0.24.0 review, each reproduced before being fixed."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_sdk_capability_probe_actually_asks_botocore(self):
        """FINDING: installed_sdk_accepts built its throwaway client with
        types.SimpleNamespace, and s3/common.py never imported types. Every
        call raised NameError, which the function's own "anything that cannot
        be asked answers True" handler swallowed -- so the Diagnostics report
        answered "Yes" to "Conditional writes (If-None-Match)" on every
        build, including the distribution botocore the row exists to warn
        about. A parameter that certainly does not exist proves the probe is
        reading the service model rather than defaulting."""
        self.assertFalse(
            installed_sdk_accepts("PutObject", "NoSuchParameterAtAll"))
        self.assertTrue(installed_sdk_accepts("PutObject", "Bucket"))

    def test_an_unaskable_operation_still_answers_yes(self):
        """The documented fallback has to survive the fix: a model this build
        cannot describe must not disable a feature the service does have."""
        self.assertTrue(installed_sdk_accepts("NoSuchOperation", "Whatever"))

    def test_the_diagnostics_report_reflects_the_installed_botocore(self):
        text = diagnostics.format_report(diagnostics.collect(PROJECT_ROOT))
        self.assertIn("Conditional writes (If-None-Match)", text)

    def test_a_read_only_profile_passes_its_own_check(self):
        """FINDING: check_profile proved a bucket by creating and deleting a
        probe key. That write goes through _guard_write, so every correctly
        configured read-only profile reported "Check failed: This profile is
        read-only" -- the check contradicting the setting beside it."""
        model = make_model(bucket="bkt", read_only=True)
        model._client = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "a.txt", "Size": 1}]}])
        ok, reason = model.check_profile()
        self.assertTrue(ok, reason)
        self.assertIsNone(reason)
        self.assertEqual(model._client.calls_of("put_object"), [],
                         "the read-only check wrote to the bucket")

    def test_a_read_only_profile_still_reports_a_real_failure(self):
        model = make_model(bucket="bkt", read_only=True)
        model._client = _DeniedListClient()
        ok, reason = model.check_profile()
        self.assertFalse(ok)
        self.assertIn("denied", (reason or "").lower())

    def test_a_writable_profile_still_gets_the_write_probe(self):
        model = make_model(bucket="bkt")
        model._client = FakeS3Client()
        ok, reason = model.check_profile()
        self.assertTrue(ok, reason)
        self.assertEqual(len(model._client.calls_of("put_object")), 1)
        self.assertEqual(len(model._client.calls_of("delete_object")), 1)

    def test_a_failed_probe_reports_the_creates_reason_not_the_cleanups(self):
        """The cleanup used to run in a finally and raise over the top of the
        reason the create had given, so the only failure ever reported was
        the delete's."""
        model = make_model(bucket="bkt")
        model._client = _RefusingPutClient()
        ok, reason = model.check_profile()
        self.assertFalse(ok)
        self.assertIn("quota", (reason or "").lower())

    def test_a_failure_that_says_cancelled_is_still_a_failure(self):
        """FINDING: job_runner decided "was this a cancellation?" by looking
        for the word in str(exc). A delete refused on a key like
        "cancelled-orders.csv" was therefore reported as the user's own
        cancellation -- no error dialog, no red queue row, nothing in the log
        about what actually went wrong."""
        def _boom(key, **_kw):
            raise RuntimeError("AccessDenied on cancelled-orders.csv")

        worker = main_window.Worker(
            _DeleteOnlyModel(_boom), ["cancelled-orders.csv"])
        errors, finished = [], []
        worker.error.connect(errors.append)
        worker.finished.connect(finished.append)
        worker.delete()
        self.assertEqual(finished, [False], "reported as a cancellation")
        self.assertEqual(len(errors), 1, "the failure was swallowed")

    def test_a_real_cancellation_is_still_silent(self):
        def _cancel(key, **_kw):
            raise TransferCancelled("cancelled")

        worker = main_window.Worker(_DeleteOnlyModel(_cancel), ["a.txt"])
        errors, finished = [], []
        worker.error.connect(errors.append)
        worker.finished.connect(finished.append)
        worker.delete()
        self.assertEqual(finished, [True])
        self.assertEqual(errors, [])


class E2EHarnessTests(unittest.TestCase):
    """
    The end-to-end suite is only worth having if it can start. MinIO
    archived its open-source server in 2025 — anonymous pulls revoked on
    quay.io, the Docker Hub repository gone, dl.min.io answering 410 for
    every community binary — which broke `make e2e` and the CI job that runs
    it, everywhere, with no change on this side.

    These are the cheap parts of that lesson: name a tag, and keep the
    server and the suite's claims about it in step.
    """

    ROOT = PROJECT_ROOT

    def _compose(self):
        with open(os.path.join(self.ROOT, "tests", "e2e",
                               "docker-compose.yml")) as handle:
            return handle.read()

    def _image(self):
        for line in self._compose().splitlines():
            line = line.strip()
            if line.startswith("image:"):
                return line.split(":", 1)[1].strip()
        return ""

    def test_the_server_image_is_pinned(self):
        """A floating tag means the suite measures a different server than
        it did yesterday without anyone deciding to — and the drift shows up
        as a test failure attributed to this code."""
        image = self._image()
        self.assertTrue(image, "no image in the e2e compose file")
        # An ${VAR:-default} override still has to carry a pinned default.
        default = image.split(":-")[-1].rstrip("}") if ":-" in image else image
        self.assertNotIn(":latest", default, "the e2e server floats on latest")
        self.assertRegex(default, r":[^:}]+$", "the e2e server has no tag")

    def test_the_backend_is_still_minio(self):
        """Half the limitations in ROADMAP.md are statements about how MinIO
        behaves. Swapping the backend without revisiting them would leave
        prose that nothing checks any more."""
        self.assertIn("minio", self._image().lower())

    def test_the_healthcheck_uses_something_the_image_has(self):
        """REGRESSION: the old healthcheck called mc, which the image that
        replaced it does not carry — a wrong healthcheck never goes healthy,
        and compose then waits for the full timeout before failing."""
        compose = self._compose()
        self.assertIn("healthcheck", compose)
        self.assertNotIn('"mc"', compose)
class CliParsingTests(unittest.TestCase):
    """`s3duck s3://bucket/prefix` from a terminal, a chat link or a file
    manager — not a second CLI to learn."""

    parse = staticmethod(s3duck.parse_cli)

    def test_no_arguments_is_a_plain_launch(self):
        self.assertEqual(self.parse([]),
                         {"profile": "", "location": "", "help": False,
                          "unknown": []})

    def test_a_bare_location(self):
        self.assertEqual(self.parse(["s3://bkt/logs/"])["location"],
                         "s3://bkt/logs/")

    def test_a_profile_in_both_spellings(self):
        self.assertEqual(self.parse(["-p", "prod"])["profile"], "prod")
        self.assertEqual(self.parse(["--profile", "prod"])["profile"], "prod")
        self.assertEqual(self.parse(["--profile=prod"])["profile"], "prod")

    def test_a_profile_and_a_location_together(self):
        args = self.parse(["--profile=prod", "s3://bkt/x/"])
        self.assertEqual((args["profile"], args["location"]),
                         ("prod", "s3://bkt/x/"))

    def test_help_is_recognised(self):
        self.assertTrue(self.parse(["--help"])["help"])
        self.assertTrue(self.parse(["-h"])["help"])

    def test_a_dangling_profile_flag_does_not_crash(self):
        self.assertEqual(self.parse(["--profile"])["profile"], "")

    def test_extra_arguments_are_reported_not_guessed_at(self):
        args = self.parse(["one", "two", "--nope"])
        self.assertEqual(args["location"], "one")
        self.assertEqual(sorted(args["unknown"]), ["--nope", "two"])
class InstanceHandoverTests(unittest.TestCase):
    """A location handed to a running window opens there; a bare launch still
    gets its own window."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_a_message_reaches_the_listener(self):
        server = utils.InstanceServer()
        name = utils.instance_socket_name("s3duck-test-handover")
        self.assertTrue(server.start(name))
        self.addCleanup(server.stop)
        received = []
        server.received.connect(received.append)
        self.assertTrue(
            utils.send_to_running_instance("s3://bkt/logs/", name))
        deadline = time.monotonic() + 3
        while not received and time.monotonic() < deadline:
            self._app.processEvents()
        self.assertEqual(received, ["s3://bkt/logs/"])

    def test_sending_with_nobody_listening_fails_cleanly(self):
        self.assertFalse(utils.send_to_running_instance(
            "s3://bkt/", utils.instance_socket_name("s3duck-test-nobody"),
            timeout_ms=100))

    def test_a_stale_socket_does_not_block_the_next_listener(self):
        """A crash leaves the socket behind; without removeServer the next
        run could never listen again."""
        name = utils.instance_socket_name("s3duck-test-stale")
        first = utils.InstanceServer()
        self.assertTrue(first.start(name))
        second = utils.InstanceServer()
        self.assertTrue(second.start(name))
        self.addCleanup(second.stop)
        first.stop()

    def test_the_socket_name_is_per_user(self):
        self.assertNotEqual(utils.instance_socket_name("a"),
                            utils.instance_socket_name("b"))
        self.assertIn("a", utils.instance_socket_name("a"))
class LogFileTests(unittest.TestCase):
    """The log view is capped and dies with the window, which is fine until
    someone needs to say what happened an hour ago."""

    def _path(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return os.path.join(directory, "deep", "s3duck.log")

    def test_lines_are_written_and_read_back(self):
        log = utils.LogFile(self._path())
        log.write("first")
        log.write("second")
        self.assertEqual(log.tail().splitlines(), ["first", "second"])

    def test_the_directory_is_created(self):
        path = self._path()
        utils.LogFile(path).write("x")
        self.assertTrue(os.path.isfile(path))

    def test_an_empty_path_disables_it(self):
        log = utils.LogFile("")
        self.assertFalse(log.enabled)
        self.assertFalse(log.write("x"))
        self.assertEqual(log.tail(), "")

    def test_the_file_rotates_once_at_the_cap(self):
        path = self._path()
        log = utils.LogFile(path, max_bytes=200)
        for index in range(200):
            log.write(f"line {index}")
        self.assertTrue(os.path.exists(path + ".1"))
        self.assertLess(os.path.getsize(path), 400)

    def test_rotation_keeps_only_one_backup(self):
        path = self._path()
        log = utils.LogFile(path, max_bytes=100)
        for index in range(300):
            log.write(f"line {index}")
        self.assertFalse(os.path.exists(path + ".2"))

    def test_an_unwritable_path_disables_rather_than_raising(self):
        """A full disk must not take the app down with it."""
        log = utils.LogFile(os.path.join(os.sep, "proc", "nope", "x.log"))
        self.assertFalse(log.write("x"))
        self.assertFalse(log.enabled)

    def test_tail_returns_only_the_last_lines(self):
        log = utils.LogFile(self._path())
        for index in range(50):
            log.write(f"line {index}")
        self.assertEqual(len(log.tail(10).splitlines()), 10)

    def test_tail_of_a_missing_file_is_empty(self):
        self.assertEqual(utils.LogFile(self._path()).tail(), "")
