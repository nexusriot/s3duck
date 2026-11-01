"""s3duck-cli: the same S3 layer without a window, for scripts and cron.

A scheduled sync needs a program that runs with the app closed; a daemon
inside the GUI would be a second application with none of the first one's
safeguards. This is the other route: the app's own Model, sync planner and
profile store, driven from the command line, so a crontab line can mirror a
folder at 02:00 with the same rules a Sync dialog applies by hand::

    0 2 * * *  s3duck-cli --profile backup sync ~/photos s3://archive/photos/

Exit status: 0 when everything asked for was done, 1 when an operation
failed, 2 for a usage error.
"""

import argparse
import getpass
import os
import sys

from PyQt6.QtCore import QSettings

from core.locations import parse_s3_location
from core.sync_plan import (
    LOCAL_DELETE_FLOOR, LOCAL_DELETE_RATIO, build_exclude_matcher,
    build_sync_plan, local_delete_guard, summarize_sync_plan,
)
from model import FSObjectType, Model, safe_local_path
from profile_switcher import decrypt_profile, load_profiles
from utils import (
    CredentialError, CredentialStore, run_credential_process, scan_local_tree,
)

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2


class CliError(Exception):
    """A failure worth one line on stderr and a non-zero exit."""

    def __init__(self, message, code=EXIT_FAILED):
        super().__init__(message)
        self.code = code


def is_remote(text) -> bool:
    return str(text or "").lower().startswith(("s3://", "s3a://"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="s3duck-cli",
        description="List, copy and sync against S3 with S3 Duck's profiles.")
    who = parser.add_argument_group("connection")
    who.add_argument("--profile", help="a profile saved in the S3 Duck app")
    who.add_argument("--settings", metavar="INI",
                     help="read profiles from this settings file instead of "
                          "the app's own")
    who.add_argument("--endpoint", help="endpoint URL, without a profile; "
                                        "keys come from AWS_ACCESS_KEY_ID / "
                                        "AWS_SECRET_ACCESS_KEY")
    who.add_argument("--region", default="us-east-1")
    who.add_argument("--path-style", action="store_true",
                     help="path-style addressing (MinIO and similar)")
    who.add_argument("--no-verify-ssl", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="print only errors")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("profiles", help="list the saved profiles")

    ls = sub.add_parser("ls", help="list buckets, or a prefix")
    ls.add_argument("location", nargs="?", default="",
                    help="s3://bucket/prefix/ (omit for the bucket list)")
    ls.add_argument("-r", "--recursive", action="store_true")

    cp = sub.add_parser("cp", help="copy a file or, with -r, a folder")
    cp.add_argument("source")
    cp.add_argument("destination")
    cp.add_argument("-r", "--recursive", action="store_true")

    sync = sub.add_parser(
        "sync", help="make the destination match the source")
    sync.add_argument("source")
    sync.add_argument("destination")
    sync.add_argument("--delete", action="store_true",
                      help="also delete what the source no longer has")
    sync.add_argument("--dry-run", action="store_true",
                      help="print the plan and change nothing")
    sync.add_argument("--force", action="store_true",
                      help="with --delete, allow a run that deletes more than "
                           f"{LOCAL_DELETE_FLOOR} files and "
                           f"{int(LOCAL_DELETE_RATIO * 100)}%% of the "
                           "destination")
    sync.add_argument("--exclude", action="append", default=[],
                      metavar="PATTERN", help="glob to leave out (repeatable)")
    return parser


def open_settings(path=None):
    if path:
        return QSettings(path, QSettings.Format.IniFormat)
    return QSettings("s3duck", "s3duck")


def unlock(settings, environ=None):
    """
    Make the credential key available, asking for a passphrase if needed.

    ``S3DUCK_PASSPHRASE`` answers for an unattended run; without it a
    passphrase-sealed store asks on the terminal, and a cron job with no
    terminal fails here with a message rather than hanging.
    """
    environ = os.environ if environ is None else environ

    def ask(prompt):
        if environ.get("S3DUCK_PASSPHRASE"):
            return environ["S3DUCK_PASSPHRASE"]
        if not sys.stdin.isatty():
            raise CliError("the credential store is sealed with a passphrase; "
                           "set S3DUCK_PASSPHRASE for an unattended run")
        return getpass.getpass(prompt + " ")

    try:
        CredentialStore(settings, ask_passphrase=ask).load_key()
    except CredentialError as exc:
        raise CliError(str(exc)) from exc


def model_for_profile(settings, name, bucket=""):
    raws = [r for r in load_profiles(settings) if str(r.get("name")) == name]
    if not raws:
        raise CliError(f"no profile named {name!r}")
    unlock(settings)
    try:
        profile = decrypt_profile(settings, raws[0])
    except Exception as exc:
        raise CliError(f"could not read profile {name!r}: {exc}") from exc
    access, secret, token = (profile.access_key, profile.secret_key,
                             profile.session_token)
    if profile.credential_process:
        try:
            minted = run_credential_process(profile.credential_process)
        except Exception as exc:
            raise CliError(f"credential_process failed: {exc}") from exc
        access, secret, token = (minted["access_key"], minted["secret_key"],
                                 minted["session_token"])
    return Model(profile.url, profile.region, access, secret, bucket,
                 profile.no_ssl_check, profile.use_path, session_token=token,
                 read_only=profile.read_only,
                 requester_pays=profile.requester_pays,
                 proxy_url=profile.proxy_url, ca_bundle=profile.ca_bundle)


def model_for_args(args, bucket="", environ=None):
    environ = os.environ if environ is None else environ
    if args.profile:
        return model_for_profile(open_settings(args.settings), args.profile,
                                 bucket)
    if not args.endpoint:
        raise CliError("name a --profile, or an --endpoint with keys in the "
                       "environment", EXIT_USAGE)
    access = environ.get("AWS_ACCESS_KEY_ID", "")
    secret = environ.get("AWS_SECRET_ACCESS_KEY", "")
    if not access or not secret:
        raise CliError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set")
    return Model(args.endpoint, args.region, access, secret, bucket,
                 args.no_verify_ssl, args.path_style,
                 session_token=environ.get("AWS_SESSION_TOKEN", ""))


def remote(text):
    bucket, prefix = parse_s3_location(text, "")
    if not bucket:
        raise CliError(f"not an s3:// location: {text}")
    return bucket, prefix


def object_key(text):
    """The exact key an s3:// argument names (no trailing slash added)."""
    raw = str(text).split("://", 1)[1]
    bucket, _, key = raw.partition("/")
    if not bucket:
        raise CliError(f"not an s3:// location: {text}")
    return bucket, key


class Cli:
    def __init__(self, args, out=None, err=None, model_factory=None):
        self.args = args
        self.out = out or sys.stdout
        self.err = err or sys.stderr
        self._factory = model_factory or (lambda bucket: model_for_args(args, bucket))

    def say(self, text):
        if not self.args.quiet:
            print(text, file=self.out)

    def model(self, bucket=""):
        model = self._factory(bucket)
        if bucket:
            model.enter_bucket(bucket)
        return model

    def run(self) -> int:
        return getattr(self, "cmd_" + self.args.command)()

    def cmd_profiles(self):
        for raw in load_profiles(open_settings(self.args.settings)):
            flags = []
            if str(raw.get("read_only")).lower() == "true":
                flags.append("read-only")
            print(f"{raw.get('name')}\t{raw.get('url')}\t{raw.get('region')}"
                  + (f"\t[{', '.join(flags)}]" if flags else ""), file=self.out)
        return EXIT_OK

    def cmd_ls(self):
        if not self.args.location:
            for item in self.model().list_buckets():
                print(item.name, file=self.out)
            return EXIT_OK
        bucket, prefix = remote(self.args.location)
        model = self.model(bucket)
        if self.args.recursive:
            for rel, (size, _mtime) in sorted(model.list_tree(prefix).items()):
                print(f"{size:>14}  {prefix}{rel}", file=self.out)
            return EXIT_OK
        for item in model.list(prefix):
            kind = "PRE" if item.type_ == FSObjectType.FOLDER else str(item.size)
            print(f"{kind:>14}  {prefix}{item.name}"
                  + ("/" if kind == "PRE" else ""), file=self.out)
        return EXIT_OK

    def cmd_cp(self):
        src, dst = self.args.source, self.args.destination
        if is_remote(src) == is_remote(dst):
            raise CliError("cp copies between a local path and s3://; "
                           "exactly one side must be remote", EXIT_USAGE)
        if is_remote(dst):
            return self._upload(src, dst)
        return self._download(src, dst)

    def _upload(self, src, dst):
        bucket, key = object_key(dst)
        model = self.model(bucket)
        if os.path.isdir(src):
            if not self.args.recursive:
                raise CliError(f"{src} is a folder; use -r", EXIT_USAGE)
            prefix = key if key.endswith("/") or not key else key + "/"
            tree = scan_local_tree(src, require_complete=True)
            for rel in sorted(tree):
                self.say(f"upload {rel} -> s3://{bucket}/{prefix}{rel}")
                model.upload_file(os.path.join(src, *rel.split("/")),
                                  prefix + rel)
            return EXIT_OK
        if not key or key.endswith("/"):
            key += os.path.basename(src)
        self.say(f"upload {src} -> s3://{bucket}/{key}")
        model.upload_file(src, key)
        return EXIT_OK

    def _download(self, src, dst):
        bucket, key = object_key(src)
        model = self.model(bucket)
        if self.args.recursive or key.endswith("/") or not key:
            prefix = key if not key or key.endswith("/") else key + "/"
            os.makedirs(dst, exist_ok=True)
            tree = model.list_tree(prefix)
            failed = 0
            for rel in sorted(tree):
                path = safe_local_path(dst, rel)
                if not path:
                    self.err.write(f"skipped (escapes {dst}): {rel}\n")
                    failed += 1
                    continue
                os.makedirs(os.path.dirname(path), exist_ok=True)
                self.say(f"download s3://{bucket}/{prefix}{rel} -> {path}")
                model.download_file(prefix + rel, path, dst)
            return EXIT_FAILED if failed else EXIT_OK
        if os.path.isdir(dst):
            target = os.path.join(dst, os.path.basename(key))
            folder = dst
        else:
            target = dst
            folder = os.path.dirname(os.path.abspath(dst))
        self.say(f"download s3://{bucket}/{key} -> {target}")
        model.download_file(key, target, folder)
        return EXIT_OK

    def cmd_sync(self):
        src, dst = self.args.source, self.args.destination
        if is_remote(src) == is_remote(dst):
            raise CliError("sync runs between a local folder and s3://",
                           EXIT_USAGE)
        upward = is_remote(dst)
        local = src if upward else dst
        bucket, prefix = remote(dst if upward else src)
        model = self.model(bucket)
        if upward or os.path.isdir(local):
            local_tree = scan_local_tree(local, require_complete=True)
        else:
            local_tree = {}
        remote_tree = model.list_tree(prefix)
        # The list, not a joined string: the matcher splits a string on
        # whitespace, which turned "My Documents/" into "My" and "Documents/".
        excludes = list(self.args.exclude)
        plan = build_sync_plan(
            local_tree, remote_tree,
            direction="upload" if upward else "download",
            delete_extra=self.args.delete, exclude=excludes)
        actions = [a for a in plan if a.get("action") != "skip"]
        if self.args.delete and not self.args.dry_run and not self.args.force:
            # The same refusal the window's unattended watch makes: a typo'd
            # prefix, an emptied bucket or an unmounted folder makes every
            # destination file an "extra", and cron would delete them all.
            destination = remote_tree if upward else local_tree
            excluded = build_exclude_matcher(excludes)
            managed = sum(1 for rel in destination if not excluded(rel))
            allowed, deletes = local_delete_guard(
                actions, managed,
                action="delete_remote" if upward else "delete_local")
            if not allowed:
                raise CliError(
                    f"refusing to delete {deletes} of {managed} file(s) at "
                    "the destination in one run; check the paths with "
                    "--dry-run, then pass --force if that is really meant")
        summary = ", ".join(f"{count} {name}" for name, count in
                            sorted(summarize_sync_plan(plan).items()) if count)
        if self.args.dry_run or not actions:
            for action in actions:
                print(f"{action['action']}\t{action['rel']}", file=self.out)
            self.say(f"{'dry run: ' if self.args.dry_run else ''}"
                     f"{len(actions)} change(s); {summary}")
            return EXIT_OK
        failed = 0
        for action in actions:
            rel, kind = action["rel"], action["action"]
            path = safe_local_path(local, rel)
            if not path:
                self.err.write(f"skipped (escapes {local}): {rel}\n")
                failed += 1
                continue
            try:
                self.say(f"{kind} {rel}")
                if kind == "upload":
                    model.upload_file(path, prefix + rel)
                elif kind == "download":
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    model.download_file(prefix + rel, path, local)
                elif kind == "delete_remote":
                    model.delete(prefix + rel)
                elif kind == "delete_local":
                    os.remove(path)
            except Exception as exc:
                failed += 1
                self.err.write(f"{kind} {rel} failed: {exc}\n")
        self.say(f"{len(actions) - failed} of {len(actions)} change(s) done")
        return EXIT_FAILED if failed else EXIT_OK


def main(argv=None, out=None, err=None, model_factory=None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return EXIT_USAGE if exc.code else EXIT_OK
    err = err or sys.stderr
    try:
        return Cli(args, out=out, err=err, model_factory=model_factory).run()
    except CliError as exc:
        err.write(f"s3duck-cli: {exc}\n")
        return exc.code
    except Exception as exc:
        err.write(f"s3duck-cli: {exc}\n")
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
