"""s3duck-cli: argument handling, profiles, and the copy/sync routes."""

import io
import os
import tempfile
import unittest
from PyQt6.QtCore import QSettings
from core.entries import Item
from model import FSObjectType
import s3duck_cli
from s3duck_cli import EXIT_FAILED, EXIT_OK, EXIT_USAGE, main
from utils import Crypto, set_session_key


class _FakeModel:
    """An in-memory bucket with the Model calls the CLI makes."""

    def __init__(self, objects=None, read_only=False):
        self.objects = dict(objects or {})
        self.read_only = read_only
        self.bucket = ""

    def enter_bucket(self, bucket):
        self.bucket = bucket

    def list_buckets(self):
        return [Item("one", FSObjectType.BUCKET, "", 0),
                Item("two", FSObjectType.BUCKET, "", 0)]

    def list(self, prefix):
        names, out = set(), []
        for key, body in sorted(self.objects.items()):
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix):]
            head, sep, _tail = rest.partition("/")
            if sep:
                if head not in names:
                    names.add(head)
                    out.append(Item(head, FSObjectType.FOLDER, "", 0))
            else:
                out.append(Item(rest, FSObjectType.FILE, "", len(body)))
        return out

    def list_tree(self, prefix=""):
        return {k[len(prefix):]: (len(v), 0) for k, v in self.objects.items()
                if k.startswith(prefix) and not k.endswith("/")}

    def upload_file(self, path, key, **_kw):
        if self.read_only:
            raise PermissionError("read-only profile")
        with open(path, "rb") as handle:
            self.objects[key] = handle.read()

    def download_file(self, key, local_name, folder_path, **_kw):
        with open(local_name, "wb") as handle:
            handle.write(self.objects[key])

    def delete(self, key, **_kw):
        self.objects.pop(key, None)
        return True


class CliTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.model = _FakeModel({"p/a.txt": b"A", "p/sub/b.txt": b"BB",
                                 "../evil": b"x"})

    def run_cli(self, *argv, model=None):
        out, err = io.StringIO(), io.StringIO()
        target = model or self.model
        code = main(["--endpoint", "http://x", *argv], out=out, err=err,
                    model_factory=lambda bucket: target)
        return code, out.getvalue(), err.getvalue()

    def path(self, *parts):
        return os.path.join(self.tmp.name, *parts)

    def write(self, rel, body):
        path = self.path(*rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(body)
        return path

    def test_usage_errors(self):
        self.assertEqual(main([], out=io.StringIO(), err=io.StringIO()),
                         EXIT_USAGE)
        code, _out, err = self.run_cli("cp", "a", "b")
        self.assertEqual(code, EXIT_USAGE)
        self.assertIn("exactly one side", err)
        code, _out, err = self.run_cli("cp", self.tmp.name, "s3://b/x/")
        self.assertEqual(code, EXIT_USAGE)
        self.assertIn("use -r", err)

    def test_ls(self):
        code, out, _ = self.run_cli("ls")
        self.assertEqual((code, out.split()), (EXIT_OK, ["one", "two"]))
        code, out, _ = self.run_cli("ls", "s3://b/p/")
        self.assertIn("PRE  p/sub/", out)
        self.assertIn("1  p/a.txt", out)
        code, out, _ = self.run_cli("ls", "-r", "s3://b/p/")
        self.assertIn("p/sub/b.txt", out)

    def test_upload_file_and_folder(self):
        path = self.write("one.txt", b"1")
        self.assertEqual(self.run_cli("-q", "cp", path, "s3://b/in/")[0], EXIT_OK)
        self.assertEqual(self.model.objects["in/one.txt"], b"1")
        self.write("tree/x/y.txt", b"Y")
        code, _out, _err = self.run_cli("cp", "-r", self.path("tree"), "s3://b/t")
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(self.model.objects["t/x/y.txt"], b"Y")

    def test_download_file_and_prefix(self):
        code, _o, _e = self.run_cli("cp", "s3://b/p/a.txt", self.tmp.name)
        self.assertEqual(code, EXIT_OK)
        with open(self.path("a.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"A")
        code, _o, _e = self.run_cli("cp", "-r", "s3://b/p/", self.path("dl"))
        self.assertEqual(code, EXIT_OK)
        with open(self.path("dl", "sub", "b.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"BB")

    def test_a_key_escaping_the_folder_is_refused(self):
        code, _out, err = self.run_cli("cp", "-r", "s3://b/", self.path("all"))
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("escapes", err)
        self.assertFalse(os.path.exists(self.path("evil")))

    def test_sync_up_with_delete_and_dry_run(self):
        self.write("src/a.txt", b"A")
        self.write("src/new.txt", b"N")
        code, out, _ = self.run_cli("sync", "--delete", "--dry-run",
                                    self.path("src"), "s3://b/p/")
        self.assertEqual(code, EXIT_OK)
        self.assertIn("upload\tnew.txt", out)
        self.assertIn("delete_remote\tsub/b.txt", out)
        self.assertIn("p/sub/b.txt", self.model.objects, "a dry run changed something")
        code, _out, _err = self.run_cli("sync", "--delete", self.path("src"),
                                        "s3://b/p/")
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(self.model.objects["p/new.txt"], b"N")
        self.assertNotIn("p/sub/b.txt", self.model.objects)

    def test_sync_down_and_exclude(self):
        code, _out, _err = self.run_cli("sync", "--exclude", "sub/", "s3://b/p/",
                                        self.path("mirror"))
        self.assertEqual(code, EXIT_OK)
        self.assertTrue(os.path.exists(self.path("mirror", "a.txt")))
        self.assertFalse(os.path.exists(self.path("mirror", "sub", "b.txt")))

    def test_a_failed_operation_exits_one(self):
        self.write("src/a.txt", b"changed")
        code, _out, err = self.run_cli(
            "sync", self.path("src"), "s3://b/p/",
            model=_FakeModel({"p/a.txt": b"A"}, read_only=True))
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("read-only", err)

    def test_no_connection_is_a_usage_error(self):
        err = io.StringIO()
        self.assertEqual(main(["ls"], out=io.StringIO(), err=err), EXIT_USAGE)
        self.assertIn("--profile", err.getvalue())

    def test_endpoint_needs_keys_in_the_environment(self):
        args = s3duck_cli.build_parser().parse_args(["--endpoint", "http://x", "ls"])
        with self.assertRaises(s3duck_cli.CliError):
            s3duck_cli.model_for_args(args, environ={})
        model = s3duck_cli.model_for_args(args, environ={
            "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"})
        self.assertEqual(model.access_key, "AK")


class CliProfileTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ini = os.path.join(self.tmp.name, "s3duck.ini")
        self.addCleanup(set_session_key, "")
        key = Crypto.generate_key()
        crypto = Crypto(key)
        settings = QSettings(self.ini, QSettings.Format.IniFormat)
        settings.beginGroup("common")
        settings.setValue("key", key)
        settings.endGroup()
        settings.beginGroup("profiles")
        settings.beginWriteArray("profiles", 1)
        settings.setArrayIndex(0)
        for name, value in (("name", "prod"), ("url", "http://minio:9000"),
                            ("region", "eu-west-1"), ("use_path", "true"),
                            ("read_only", "true"), ("no_ssl_check", "false")):
            settings.setValue(name, value)
        settings.setValue("access_key", crypto.encrypt("AKPROD"))
        settings.setValue("secret_key", crypto.encrypt("SKPROD"))
        settings.endArray()
        settings.endGroup()
        settings.sync()

    def test_a_saved_profile_is_decrypted_with_its_flags(self):
        settings = QSettings(self.ini, QSettings.Format.IniFormat)
        model = s3duck_cli.model_for_profile(settings, "prod", "bkt")
        self.assertEqual((model.access_key, model.secret_key),
                         ("AKPROD", "SKPROD"))
        self.assertTrue(model.read_only)
        self.assertEqual(model.bucket, "bkt")
        with self.assertRaises(s3duck_cli.CliError):
            s3duck_cli.model_for_profile(settings, "missing")

    def test_profiles_lists_them(self):
        out = io.StringIO()
        code = main(["--settings", self.ini, "profiles"], out=out,
                    err=io.StringIO())
        self.assertEqual(code, EXIT_OK)
        self.assertIn("prod\thttp://minio:9000\teu-west-1\t[read-only]",
                      out.getvalue())


if __name__ == "__main__":
    unittest.main()
