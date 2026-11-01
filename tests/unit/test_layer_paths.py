"""Branches of the new S3, core and CLI code the feature tests did not walk:
logging callbacks, pagination, absent documents, malformed input, the
passphrase and credential_process routes of the CLI, and the SSE-C filter
for an SDK whose operation lacks the customer-key members."""

import gzip
import io
import json
import os
import tempfile
import threading
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import botocore.exceptions
from PyQt6.QtCore import QSettings

import s3duck_cli
from core import qr
from core.acl import build_access_control_policy
from core.inventory import (
    inventory_duplicates, parse_manifest, read_inventory_file,
    summarize_inventory,
)
from core.policy_templates import grants_public_access
from core.replication import summarize_replication
from core.retention import assess_retention_change
from model import TransferCancelled
from tests.support import make_model
from tests.unit.test_access import _Recorder, _client_error
from tests.unit.test_cli import _FakeModel
from utils import Crypto, set_session_key, wrap_key


def _model(client, bucket="bkt"):
    model = make_model(bucket=bucket)
    model._client = client
    return model


class AccessLayerPathTests(unittest.TestCase):

    def test_every_write_logs_what_it_did(self):
        model = _model(_Recorder())
        logged = []
        owner = {"id": "a" * 64}
        grants = [{"type": "CanonicalUser", "id": "a" * 64,
                   "permission": "FULL_CONTROL"}]
        model.put_public_access_block({}, log_fn=logged.append)
        model.put_public_access_block({"BlockPublicAcls": True}, log_fn=logged.append)
        model.delete_public_access_block(log_fn=logged.append)
        model.put_bucket_logging("logs", "p/", log_fn=logged.append)
        model.put_bucket_logging("", log_fn=logged.append)
        model.put_bucket_acl(owner, grants, log_fn=logged.append)
        model.put_object_acl_grants("k", owner, grants, version_id="v",
                                    log_fn=logged.append)
        model.set_object_retention("k", "GOVERNANCE",
                                   datetime(2030, 1, 1, tzinfo=timezone.utc),
                                   bypass_governance=True, log_fn=logged.append)
        self.assertEqual(len(logged), 8)
        self.assertIn("all off", logged[0])
        self.assertIn("BlockPublicAcls", logged[1])
        self.assertIn("to logs/p/", logged[3])
        self.assertIn(": off", logged[4])
        self.assertIn("governance bypassed", logged[7])
        self.assertEqual(model._client.calls[-2][1]["VersionId"], "v")

    def test_a_bucket_is_required(self):
        model = _model(_Recorder(), bucket="")
        for call in (model.get_public_access_block, model.get_bucket_logging,
                     model.get_bucket_replication, model.get_bucket_acl,
                     lambda: model.get_object_acl("k"),
                     lambda: model.presigned_post("k"),
                     model.list_inventory_configurations,
                     lambda: model.select_object("k", "S")):
            with self.assertRaises(ValueError):
                call()

    def test_a_replication_refusal_is_raised(self):
        model = _model(_Recorder(errors={
            "get_bucket_replication": _client_error("AccessDenied")}))
        with self.assertRaises(botocore.exceptions.ClientError):
            model.get_bucket_replication()
        model = _model(_Recorder({"get_bucket_replication": {
            "ReplicationConfiguration": {"Role": "r", "Rules": []}}}))
        self.assertEqual(model.get_bucket_replication(), {"role": "r", "rules": []})


class _Pages:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def list_bucket_inventory_configurations(self, **kw):
        self.calls.append(kw)
        return self.responses.pop(0)


class ReportsLayerPathTests(unittest.TestCase):

    def test_configurations_are_paginated(self):
        client = _Pages([
            {"InventoryConfigurationList": [{"Id": "a"}], "IsTruncated": True,
             "NextContinuationToken": "t1"},
            {"InventoryConfigurationList": [{"Id": "b"}], "IsTruncated": False}])
        ids = [c["id"] for c in _model(client).list_inventory_configurations()]
        self.assertEqual(ids, ["a", "b"])
        self.assertEqual(client.calls[1]["ContinuationToken"], "t1")

    def test_absent_and_refused_configurations(self):
        model = _model(_Recorder(errors={
            "list_bucket_inventory_configurations": _client_error("NotImplemented")}))
        self.assertEqual(model.list_inventory_configurations(), [])
        model = _model(_Recorder(errors={
            "list_bucket_inventory_configurations": _client_error("AccessDenied")}))
        with self.assertRaises(botocore.exceptions.ClientError):
            model.list_inventory_configurations()

    def test_a_configuration_without_a_destination_finds_nothing(self):
        self.assertEqual(_model(_Recorder()).find_latest_inventory({}), "")

    def test_loading_logs_and_honours_cancel(self):
        manifest = json.dumps({"fileFormat": "CSV", "fileSchema": "Bucket, Key",
                               "files": [{"key": "d/1.csv"}]}).encode()

        class _Body:
            def __init__(self, data):
                self.data = data

            def read(self):
                return self.data

        class _Client:
            def get_object(self, Bucket, Key):
                return {"Body": _Body(manifest if Key == "m.json" else b'"b","k"\n')}

        model = _model(_Client())
        logged = []
        report = model.load_inventory("r", "m.json", log_fn=logged.append)
        self.assertEqual(report["rows"], [{"Bucket": "b", "Key": "k"}],
                         "an uncompressed data file was not read")
        self.assertIn("inventory file 1/1", logged[0])
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TransferCancelled):
            model.load_inventory("r", "m.json", cancel_event=cancel)


class CorePathTests(unittest.TestCase):

    def test_inventory_edge_rows(self):
        with self.assertRaises(ValueError):
            parse_manifest("[1, 2]")
        rows = read_inventory_file(gzip.compress(b'"a","1"\n\n"b","x"\n'),
                                   ["Key", "Size"])
        self.assertEqual(len(rows), 2, "a blank line became a row")
        self.assertEqual(summarize_inventory(rows)["bytes"], 1,
                         "a non-numeric size was not treated as zero")
        rows = [{"Key": "a/x", "Size": "3", "ETag": "e"},
                {"Key": "a/y", "Size": "3", "ETag": "e"},
                {"Key": "b/z", "Size": "3", "ETag": "e"},
                {"Key": "a/old", "Size": "3", "ETag": "e", "IsLatest": "false"}]
        self.assertEqual(inventory_duplicates(rows, "a/")["summary"]["redundant"], 1)

    def test_acl_email_and_empty_id(self):
        policy = build_access_control_policy({"id": "o" * 64}, [{
            "type": "AmazonCustomerByEmail", "email": "x@y",
            "permission": "READ"}])
        self.assertEqual(policy["Grants"][0]["Grantee"]["EmailAddress"], "x@y")
        with self.assertRaises(ValueError) as caught:
            build_access_control_policy({"id": "o"}, [{
                "type": "CanonicalUser", "id": " ", "permission": "READ"}])
        self.assertIn("needs the user's canonical ID", str(caught.exception))

    def test_policy_public_detection(self):
        self.assertFalse(grants_public_access([{"Effect": "Deny", "Principal": "*"}]))
        self.assertTrue(grants_public_access([{"Effect": "Allow",
                                               "Principal": {"AWS": "*"}}]))

    def test_replication_single_tag_filter(self):
        summary = summarize_replication({"Rules": [{
            "ID": "t", "Status": "Enabled",
            "Filter": {"Tag": {"Key": "k", "Value": "v"}},
            "Destination": {"Bucket": "arn:aws:s3:::d"}}]})
        self.assertEqual(summary["rules"][0]["tags"], ["k=v"])
        self.assertEqual(summary["rules"][0]["prefix"], "")

    def test_retention_accepts_naive_datetimes(self):
        now = datetime(2026, 1, 1)
        self.assertEqual(assess_retention_change(
            "", None, "GOVERNANCE", datetime(2026, 2, 1), now=now),
            ("", False, False))


class SseCFilterTests(unittest.TestCase):
    """An installed SDK too old to know a customer-key member of an operation
    refuses the parameter before sending; the hook must leave it out."""

    def _client(self, members_by_op):
        registered = {}

        class _OpModel:
            def __init__(self, members):
                self.input_shape = types.SimpleNamespace(members=members)

        class _ServiceModel:
            def operation_model(self, name):
                if name not in members_by_op:
                    raise KeyError(name)
                return _OpModel(members_by_op[name])

        events = types.SimpleNamespace(
            register=lambda event, fn: registered.setdefault(event, fn))
        client = types.SimpleNamespace(meta=types.SimpleNamespace(
            service_model=_ServiceModel(), events=events))
        return client, registered

    def test_unknown_members_are_dropped_and_unknown_operations_skipped(self):
        model = make_model(bucket="b")
        model.sse_c_key = b"k" * 32
        client, registered = self._client({
            "PutObject": {"Bucket", "Key", "SSECustomerAlgorithm",
                          "SSECustomerKey"},
            "CompleteMultipartUpload": {"Bucket", "Key", "UploadId"},
        })
        model._register_sse_c(client)
        self.assertIn("provide-client-params.s3.PutObject", registered)
        self.assertNotIn("provide-client-params.s3.GetObject", registered,
                         "an operation the SDK does not have got a hook")
        params = {"Bucket": "b", "Key": "k"}
        registered["provide-client-params.s3.PutObject"](params=params)
        self.assertEqual(params["SSECustomerKey"], b"k" * 32)
        params = {"Bucket": "b", "Key": "k"}
        registered["provide-client-params.s3.CompleteMultipartUpload"](params=params)
        self.assertNotIn("SSECustomerKey", params)
        registered["provide-client-params.s3.PutObject"](params=None)


class CliPathTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(set_session_key, "")

    def _settings(self, passphrase=None, credential_process=""):
        ini = os.path.join(self.tmp.name, "s.ini")
        key = Crypto.generate_key()
        crypto = Crypto(key)
        settings = QSettings(ini, QSettings.Format.IniFormat)
        settings.beginGroup("common")
        if passphrase:
            settings.setValue("credential_store", "passphrase")
            settings.setValue("key_wrapped", wrap_key(key, passphrase))
        else:
            settings.setValue("key", key)
        settings.endGroup()
        settings.beginGroup("profiles")
        settings.beginWriteArray("profiles", 1)
        settings.setArrayIndex(0)
        settings.setValue("name", "p")
        settings.setValue("url", "http://minio:9000")
        settings.setValue("access_key", crypto.encrypt("AK"))
        settings.setValue("secret_key", crypto.encrypt("SK"))
        settings.setValue("credential_process", credential_process)
        settings.endArray()
        settings.endGroup()
        settings.sync()
        return ini

    def test_a_passphrase_from_the_environment(self):
        ini = self._settings(passphrase="pw")
        settings = QSettings(ini, QSettings.Format.IniFormat)
        s3duck_cli.unlock(settings, environ={"S3DUCK_PASSPHRASE": "pw"})
        model = s3duck_cli.model_for_profile(settings, "p")
        self.assertEqual(model.access_key, "AK")

    def test_no_terminal_and_no_passphrase_fails_with_a_hint(self):
        ini = self._settings(passphrase="pw")
        settings = QSettings(ini, QSettings.Format.IniFormat)
        with patch.object(s3duck_cli.sys.stdin, "isatty", return_value=False):
            with self.assertRaises(s3duck_cli.CliError) as caught:
                s3duck_cli.unlock(settings, environ={})
        self.assertIn("S3DUCK_PASSPHRASE", str(caught.exception))

    def test_a_terminal_is_asked(self):
        ini = self._settings(passphrase="pw")
        settings = QSettings(ini, QSettings.Format.IniFormat)
        with patch.object(s3duck_cli.sys.stdin, "isatty", return_value=True), \
                patch.object(s3duck_cli.getpass, "getpass", return_value="pw"):
            s3duck_cli.unlock(settings, environ={})

    def test_a_wrong_passphrase_is_a_cli_error(self):
        ini = self._settings(passphrase="pw")
        settings = QSettings(ini, QSettings.Format.IniFormat)
        with self.assertRaises(s3duck_cli.CliError):
            with patch.object(s3duck_cli.sys.stdin, "isatty", return_value=True), \
                    patch.object(s3duck_cli.getpass, "getpass", return_value="wrong"):
                s3duck_cli.unlock(settings, environ={})

    def test_credential_process_mints_the_keys(self):
        ini = self._settings(credential_process="mint-keys")
        settings = QSettings(ini, QSettings.Format.IniFormat)
        minted = {"access_key": "MINT", "secret_key": "S", "session_token": "T",
                  "expires": ""}
        with patch.object(s3duck_cli, "run_credential_process", return_value=minted):
            model = s3duck_cli.model_for_profile(settings, "p")
        self.assertEqual((model.access_key, model.session_token), ("MINT", "T"))
        with patch.object(s3duck_cli, "run_credential_process",
                          side_effect=RuntimeError("exit 1")):
            with self.assertRaises(s3duck_cli.CliError):
                s3duck_cli.model_for_profile(settings, "p")

    def test_an_undecryptable_profile(self):
        ini = self._settings()
        settings = QSettings(ini, QSettings.Format.IniFormat)
        settings.beginGroup("common")
        settings.setValue("key", Crypto.generate_key())
        settings.endGroup()
        with self.assertRaises(s3duck_cli.CliError) as caught:
            s3duck_cli.model_for_profile(settings, "p")
        self.assertIn("could not read profile", str(caught.exception))

    def test_profile_flag_through_main(self):
        ini = self._settings()
        out = io.StringIO()
        with patch.object(s3duck_cli.Model, "list_buckets", return_value=[]):
            code = s3duck_cli.main(["--settings", ini, "--profile", "p", "ls"],
                                   out=out, err=io.StringIO())
        self.assertEqual(code, 0)

    def _run(self, model, *argv):
        out, err = io.StringIO(), io.StringIO()
        code = s3duck_cli.main(["--endpoint", "http://x", *argv], out=out,
                               err=err, model_factory=lambda bucket: model)
        return code, out.getvalue(), err.getvalue()

    def test_sync_down_deletes_local_extras(self):
        local = os.path.join(self.tmp.name, "m")
        os.makedirs(local)
        with open(os.path.join(local, "gone.txt"), "wb") as handle:
            handle.write(b"x")
        model = _FakeModel({"p/kept.txt": b"k"})
        code, _out, err = self._run(model, "sync", "--delete", "s3://b/p/", local)
        self.assertEqual(code, 0, err)
        self.assertEqual(sorted(os.listdir(local)), ["kept.txt"])

    def test_sync_skips_a_key_that_escapes(self):
        model = _FakeModel({"p/../../evil": b"x", "p/ok.txt": b"o"})
        code, _out, err = self._run(model, "sync", "s3://b/p/",
                                    os.path.join(self.tmp.name, "dl"))
        self.assertEqual(code, 1)
        self.assertIn("escapes", err)
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "dl", "ok.txt")))

    def test_sync_into_a_missing_folder_and_usage_errors(self):
        model = _FakeModel({"p/a.txt": b"a"})
        target = os.path.join(self.tmp.name, "new")
        self.assertEqual(self._run(model, "sync", "s3://b/p/", target)[0], 0)
        self.assertEqual(self._run(model, "sync", "a", "b")[0], 2)
        self.assertEqual(self._run(model, "ls", "s3://")[0], 1)
        self.assertEqual(self._run(model, "cp", "s3:///k", self.tmp.name)[0], 1)

    def test_download_to_a_named_file_and_an_unexpected_error(self):
        model = _FakeModel({"k.txt": b"K"})
        target = os.path.join(self.tmp.name, "named.bin")
        self.assertEqual(self._run(model, "cp", "s3://b/k.txt", target)[0], 0)
        with open(target, "rb") as handle:
            self.assertEqual(handle.read(), b"K")
        code, _out, err = self._run(model, "cp", "s3://b/missing", target)
        self.assertEqual(code, 1)
        self.assertIn("missing", err)

    def test_open_settings_default(self):
        self.assertEqual(s3duck_cli.open_settings().applicationName(), "s3duck")


class SmallBehaviourTests(unittest.TestCase):

    def test_an_sse_kms_etag_is_not_compared(self):
        model = make_model(bucket="b")
        logged = []
        self.assertTrue(model.verify_download(
            __file__, "0" * 32, head={"ServerSideEncryption": "aws:kms"},
            log_fn=logged.append))
        self.assertIn("SSE-KMS", logged[-1])

    def test_a_forced_qr_version_that_is_too_small(self):
        with self.assertRaises(ValueError):
            qr.encode("x" * 100, qr.LOW, version=1)


if __name__ == "__main__":
    unittest.main()
