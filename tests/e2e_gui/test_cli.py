"""s3duck-cli against a real server, through its own connection path.

Here rather than in tests/e2e because the CLI reads the app's saved profiles
through QSettings, so it needs PyQt6's core module, which the model runner
deliberately does not install. It is still driven by --endpoint and
environment keys, the route an unattended cron job takes.
"""

import io
import os
import tempfile

import s3duck_cli
from tests.e2e.harness import ServerCase, _env, endpoint


class CommandLineTests(ServerCase):
    """The CLI's own connection path (--endpoint and environment keys), not a
    model handed to it: that is the route a cron job takes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = {"AWS_ACCESS_KEY_ID": _env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
                    "AWS_SECRET_ACCESS_KEY": _env("S3DUCK_TEST_SECRET_KEY",
                                                  "minioadmin")}
        saved = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)

        def _restore():
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        self.addCleanup(_restore)

    def cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        code = s3duck_cli.main(
            ["--endpoint", endpoint(), "--path-style", "--no-verify-ssl",
             "--region", _env("S3DUCK_TEST_REGION", "us-east-1"), *argv],
            out=out, err=err)
        return code, out.getvalue(), err.getvalue()

    def _tree(self, root, files):
        for rel, body in files.items():
            path = os.path.join(root, *rel.split("/"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(body)

    def test_sync_up_then_down(self):
        src = os.path.join(self.tmp.name, "src")
        self._tree(src, {"a.txt": b"alpha", "deep/b.txt": b"beta"})
        code, _out, err = self.cli("sync", src, f"s3://{self.bucket}/cli/")
        self.assertEqual(code, 0, err)
        self.assertEqual(self.keys("cli/"), ["cli/a.txt", "cli/deep/b.txt"])

        code, out, err = self.cli("sync", src, f"s3://{self.bucket}/cli/")
        self.assertEqual(code, 0, err)
        self.assertIn("0 change(s)", out, "an unchanged tree was sent again")

        dst = os.path.join(self.tmp.name, "dst")
        code, _out, err = self.cli("sync", f"s3://{self.bucket}/cli/", dst)
        self.assertEqual(code, 0, err)
        with open(os.path.join(dst, "deep", "b.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"beta")

    def test_sync_delete_removes_what_the_source_dropped(self):
        src = os.path.join(self.tmp.name, "del")
        self._tree(src, {"keep.txt": b"k", "drop.txt": b"d"})
        self.assertEqual(self.cli("sync", src, f"s3://{self.bucket}/del/")[0], 0)
        os.remove(os.path.join(src, "drop.txt"))
        code, out, _ = self.cli("sync", "--delete", "--dry-run", src,
                                f"s3://{self.bucket}/del/")
        self.assertIn("delete_remote\tdrop.txt", out)
        self.assertEqual(self.keys("del/"), ["del/drop.txt", "del/keep.txt"])
        self.assertEqual(self.cli("sync", "--delete", src,
                                  f"s3://{self.bucket}/del/")[0], 0)
        self.assertEqual(self.keys("del/"), ["del/keep.txt"])

    def test_cp_and_ls(self):
        path = os.path.join(self.tmp.name, "one.txt")
        with open(path, "wb") as handle:
            handle.write(b"one")
        self.assertEqual(self.cli("cp", path, f"s3://{self.bucket}/cp/")[0], 0)
        code, out, _ = self.cli("ls", f"s3://{self.bucket}/cp/")
        self.assertEqual(code, 0)
        self.assertIn("cp/one.txt", out)
        back = os.path.join(self.tmp.name, "back.txt")
        self.assertEqual(self.cli("cp", f"s3://{self.bucket}/cp/one.txt", back)[0], 0)
        with open(back, "rb") as handle:
            self.assertEqual(handle.read(), b"one")

    def test_a_missing_object_exits_one(self):
        code, _out, err = self.cli("cp", f"s3://{self.bucket}/nope/x.txt",
                                   os.path.join(self.tmp.name, "x.txt"))
        self.assertEqual(code, 1)
        self.assertTrue(err)
