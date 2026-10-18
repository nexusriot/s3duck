"""Guards on the shape of the tree itself."""

import ast
import main_window
import os
import re
import subprocess
import sys
import tempfile
import unittest
from tests.support import (
    APP_MODULE_ROOTS, MIN_APP_SOURCES, PROJECT_ROOT, app_source_files,
    app_source_names, patch_app_wide, read_app_sources,
)


class CorePurityTests(unittest.TestCase):
    """core/ is the part of the app that can be reasoned about on its own.

    It earns that only as long as nothing in it reaches for a GUI, an S3
    client, or the modules it was carved out of. One import is all it takes to
    turn a pure planner back into something that needs a QApplication to test.
    """

    FORBIDDEN = {"PyQt6", "boto3", "botocore", "awscrt",
                 "main_window", "model", "utils", "s3duck", "settings",
                 "properties_window", "profile_switcher", "theme",
                 "diagnostics"}

    def _core_files(self):
        root = os.path.join(PROJECT_ROOT, "core")
        return [p for p in app_source_files(root)]

    def test_core_exists_and_is_being_checked(self):
        self.assertGreaterEqual(len(self._core_files()), 10,
                                "core/ discovery collapsed")

    def test_core_imports_nothing_from_the_gui_or_the_s3_layer(self):
        offenders = []
        for path in self._core_files():
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            rel = os.path.relpath(path, PROJECT_ROOT)
            for node in ast.walk(tree):
                roots = []
                if isinstance(node, ast.Import):
                    roots = [a.name.split(".")[0] for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    roots = [node.module.split(".")[0]]
                for name in roots:
                    if name in self.FORBIDDEN:
                        offenders.append(f"{rel}:{node.lineno}: {name}")
        self.assertEqual(offenders, [])

    def test_core_imports_cleanly_with_no_qt_or_boto3_loaded(self):
        """Not just the import statements -- what actually gets pulled in."""
        names = [os.path.splitext(os.path.basename(p))[0]
                 for p in self._core_files()]
        names = [n for n in names if n != "__init__"]
        self.assertGreaterEqual(len(names), 10)
        # Plain import statements rather than a dynamic import: the
        # dynamic-import guard in this file is a text scan over every source,
        # and it would (rightly) match the helper spelled the other way.
        script = (
            "import sys\n"
            + "".join(f"import core.{m}\n" for m in sorted(names))
            + "bad = sorted({m.split('.')[0] for m in sys.modules\n"
              "              if m.split('.')[0] in "
              "('PyQt6', 'boto3', 'botocore')})\n"
              "print(','.join(bad))\n"
        )
        out = subprocess.run([sys.executable, "-c", script],
                             cwd=PROJECT_ROOT, capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.strip(), "",
                         "core/ dragged a heavy dependency in at import time")
class WindowCompositionTests(unittest.TestCase):
    """MainWindow is assembled from the mixins under ui/window/."""

    EXPECTED = ("NavigationMixin", "HistoryMixin", "TabsMixin",
                "TransferQueueMixin", "QuickFindMixin", "BookmarksMixin",
                "WatchMixin", "UsageMixin", "CredentialsMixin")

    def test_every_mixin_is_in_the_bases(self):
        bases = [c.__name__ for c in main_window.MainWindow.__mro__]
        for name in self.EXPECTED:
            with self.subTest(mixin=name):
                self.assertIn(name, bases)

    def test_no_mixin_shadows_another(self):
        """Two mixins defining the same method would resolve by MRO order,
        silently."""
        seen, clashes = {}, []
        for cls in main_window.MainWindow.__mro__:
            if cls.__name__ not in self.EXPECTED:
                continue
            for name, value in vars(cls).items():
                if name.startswith("__") or not callable(value):
                    continue
                if name in seen:
                    clashes.append(f"{name}: {seen[name]} and {cls.__name__}")
                seen[name] = cls.__name__
        self.assertEqual(clashes, [])

    def test_the_window_still_answers_for_every_moved_method(self):
        """A mixin that is defined but not mixed in is invisible until the
        method is called."""
        for name in ("navigate", "goBack", "new_tab", "cancel_transfers",
                     "open_search", "add_bookmark", "start_watch",
                     "request_bucket_usage", "refresh_credentials"):
            with self.subTest(method=name):
                self.assertTrue(callable(
                    getattr(main_window.MainWindow, name, None)), name)
class PatchAppWideTests(unittest.TestCase):
    """REGRESSION: splitting MainWindow into mixins moved methods into other
    modules, and `from x import Y` binds Y per importing module. A test that
    patched main_window.OverwriteDialog stopped reaching _resolve_overwrites
    once that method lived in ui/window/transfers.py, so a real modal dialog
    opened in an offscreen run and the suite hung rather than failed."""

    def test_the_hazard_is_real(self):
        """More than one application module holds each of these names."""
        for attr in ("QMessageBox", "OverwriteDialog"):
            with self.subTest(attr=attr):
                holders = [name for name, module in list(sys.modules.items())
                           if module is not None
                           and name.split(".")[0] in APP_MODULE_ROOTS
                           and attr in vars(module)]
                self.assertGreater(
                    len(holders), 1,
                    f"{attr} is only in {holders} -- patching one module "
                    "would have been enough and this helper is unnecessary")

    def test_it_patches_every_holder_and_restores_them(self):
        sentinel = object()
        holders = [module for name, module in list(sys.modules.items())
                   if module is not None
                   and name.split(".")[0] in APP_MODULE_ROOTS
                   and "QMessageBox" in vars(module)]
        before = {id(m): getattr(m, "QMessageBox") for m in holders}
        with patch_app_wide("QMessageBox", sentinel):
            for module in holders:
                self.assertIs(getattr(module, "QMessageBox"), sentinel)
        for module in holders:
            self.assertIs(getattr(module, "QMessageBox"), before[id(module)])

    def test_it_refuses_a_name_nothing_imports(self):
        with self.assertRaises(AssertionError):
            with patch_app_wide("NoSuchNameAnywhere"):
                pass
class SuiteIntegrityTests(unittest.TestCase):
    """REGRESSION: two classes in this file were both called QueueRetryTests.
    The second definition replaced the first at import, so one test had never
    run -- and nothing said so, because a suite that silently shrinks still
    reports OK."""

    def _test_modules(self):
        root = os.path.join(PROJECT_ROOT, "tests")
        found = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            found += [os.path.join(dirpath, n) for n in sorted(filenames)
                      if n.endswith(".py")]
        return found

    def test_no_two_test_classes_in_one_module_share_a_name(self):
        offenders = []
        checked = 0
        for path in self._test_modules():
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            seen = {}
            for node in tree.body:
                if not isinstance(node, ast.ClassDef):
                    continue
                checked += 1
                if node.name in seen:
                    offenders.append(
                        f"{os.path.relpath(path, PROJECT_ROOT)}: {node.name} "
                        f"at lines {seen[node.name]} and {node.lineno}")
                seen[node.name] = node.lineno
        self.assertGreater(checked, 100, "test-module discovery collapsed")
        self.assertEqual(offenders, [])

    def test_no_two_test_methods_in_one_class_share_a_name(self):
        """The same shadowing, one level down."""
        offenders = []
        for path in self._test_modules():
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                seen = {}
                for member in node.body:
                    if not isinstance(member, ast.FunctionDef):
                        continue
                    if member.name in seen:
                        offenders.append(
                            f"{os.path.relpath(path, PROJECT_ROOT)}: "
                            f"{node.name}.{member.name} at lines "
                            f"{seen[member.name]} and {member.lineno}")
                    seen[member.name] = member.lineno
        self.assertEqual(offenders, [])
class StarImportTests(unittest.TestCase):
    """``from x import *`` makes a module impossible to split safely.

    With two wildcards in main_window.py there was no way to tell which Qt
    module a bare name came from, so moving a class out meant guessing at its
    imports. Nothing may reintroduce one.
    """

    def test_no_module_uses_a_star_import(self):
        offenders = []
        sources = read_app_sources()
        self.assertGreaterEqual(len(sources), MIN_APP_SOURCES,
                                "source discovery collapsed")
        for name, text in sources:
            for node in ast.walk(ast.parse(text)):
                if isinstance(node, ast.ImportFrom):
                    if any(alias.name == "*" for alias in node.names):
                        offenders.append(f"{name}:{node.lineno}")
        self.assertEqual(offenders, [])

    def test_the_star_import_check_actually_matches_one(self):
        """Guard the guard: a check that matches nothing passes on anything."""
        tree = ast.parse("from os.path import *\n")
        found = [n for n in ast.walk(tree)
                 if isinstance(n, ast.ImportFrom)
                 and any(a.name == "*" for a in n.names)]
        self.assertEqual(len(found), 1)
class SourceDiscoveryTests(unittest.TestCase):
    """Guard the guards' inputs.

    Every audit in this file is only as good as the file list it runs over.
    """

    def test_discovery_finds_the_application_sources(self):
        names = app_source_names()
        self.assertGreaterEqual(
            len(names), MIN_APP_SOURCES,
            "source discovery collapsed -- every audit below is now vacuous")
        joined = " ".join(names)
        for expected in ("model.py", "utils.py", "s3duck.py", "version.py"):
            self.assertIn(expected, joined)

    def test_discovery_excludes_the_suite_and_its_tooling(self):
        names = app_source_names()
        self.assertFalse([n for n in names if n.startswith("tests" + os.sep)])
        self.assertFalse([n for n in names if n.startswith("tools" + os.sep)])
        self.assertNotIn("test_window.py", [os.path.basename(n) for n in names])

    def test_discovery_reaches_into_packages(self):
        """A package directory must be walked, not skipped."""
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "pkg", "deep"))
            for rel in ("top.py", "pkg/__init__.py", "pkg/deep/leaf.py"):
                with open(os.path.join(tmp, rel), "w") as handle:
                    handle.write("x = 1\n")
            os.makedirs(os.path.join(tmp, "tests"))
            with open(os.path.join(tmp, "tests", "test_x.py"), "w") as handle:
                handle.write("x = 1\n")
            found = {os.path.relpath(p, tmp) for p in app_source_files(tmp)}
        self.assertEqual(found, {"top.py",
                                 os.path.join("pkg", "__init__.py"),
                                 os.path.join("pkg", "deep", "leaf.py")})
class CodeStyleTests(unittest.TestCase):
    """
    The project's standing style rules, enforced rather than re-checked by
    hand: every import at the top of its module, and no decorative
    section-divider comments.
    """

    ROOT = PROJECT_ROOT
    SKIP_DIRS = {".venv", "__pycache__", ".git", "build", "dist"}
    # Runs of punctuation are what makes a comment a divider rather than prose.
    BANNER = re.compile(r"^\s*#\s*[-=*~_#+]{3,}\s*$|^\s*#\s*[-=*~_#]{2,}.*[-=*~_#]{2,}\s*$")

    def _python_files(self):
        found = []
        for dirpath, dirnames, filenames in os.walk(self.ROOT):
            dirnames[:] = [d for d in dirnames if d not in self.SKIP_DIRS]
            found += [os.path.join(dirpath, name)
                      for name in sorted(filenames) if name.endswith(".py")]
        return sorted(found)

    def _rel(self, path):
        return os.path.relpath(path, self.ROOT)

    @staticmethod
    def _top_level_imports(tree) -> set:
        """
        The ids of the import nodes that are genuinely at the module top.

        A module-level ``try: import x / except ImportError:`` counts — that
        is the optional-dependency idiom, not a late import.
        """
        allowed = set()
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                allowed.add(id(node))
            elif isinstance(node, ast.Try):
                for sub in ast.walk(node):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        allowed.add(id(sub))
        return allowed

    @classmethod
    def _late_imports(cls, tree) -> list:
        """Every import that is not one of the module's top-level ones."""
        allowed = cls._top_level_imports(tree)
        return [node.lineno for node in ast.walk(tree)
                if isinstance(node, (ast.Import, ast.ImportFrom))
                and id(node) not in allowed]

    def test_no_late_imports_anywhere(self):
        """
        A deferred import hides a dependency and can fail mid-operation.

        Checked against the module body rather than by looking inside
        functions and classes: an import nested in a module-level ``if`` or
        ``with`` is just as late, and the narrower check saw neither.
        """
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                tree = ast.parse(handle.read())
            offenders += [f"{self._rel(path)}:{line}"
                          for line in self._late_imports(tree)]
        self.assertEqual(offenders, [], "imports must be at module top")

    def test_the_late_import_check_sees_every_shape(self):
        """Guard the guard: a check that matches nothing passes no matter what
        the tree contains."""
        tree = ast.parse(
            "import os\n"
            "try:\n"
            "    import sip\n"
            "except ImportError:\n"
            "    sip = None\n"
            "if os.name == 'nt':\n"
            "    import msvcrt\n"
            "with open('x') as handle:\n"
            "    import csv\n"
            "def go():\n"
            "    import json\n"
            "class Thing:\n"
            "    import re\n")
        # the guarded optional import is fine; the other four are not
        self.assertEqual(self._late_imports(tree), [7, 9, 11, 13])

    def test_no_dynamic_imports(self):
        """A dynamic import defers a dependency past every AST check that
        looks for an import statement."""
        # Spelled in pieces so this line is not itself a hit — the scan
        # covers every file in the tree, including this one.
        needles = ("__" + "import__", "import" + "lib")
        pattern = re.compile(
            r"\b(?:%s)\b" % "|".join(re.escape(word) for word in needles))
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                for number, line in enumerate(handle.read().splitlines(), 1):
                    if pattern.search(line):
                        offenders.append(f"{self._rel(path)}:{number}")
        self.assertEqual(offenders, [], "dynamic import")

    def test_no_module_level_import_sits_below_code(self):
        """The stricter half of the same rule. Setup that genuinely must run
        first (a Qt platform hint, a sys.path insert) belongs in a _bootstrap
        module that is simply imported first, so the imports stay contiguous.
        A module-level `try: import x / except ImportError:` still counts as
        top-level — that is the optional-dependency idiom, not a late import.
        """
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                tree = ast.parse(handle.read())
            first_code = None
            for node in tree.body:
                docstring = (isinstance(node, ast.Expr)
                             and isinstance(node.value, ast.Constant)
                             and isinstance(node.value.value, str))
                plain = isinstance(node, (ast.Import, ast.ImportFrom))
                guarded = isinstance(node, ast.Try) and any(
                    isinstance(sub, (ast.Import, ast.ImportFrom))
                    for sub in ast.walk(node))
                if docstring or plain or guarded:
                    continue
                first_code = node.lineno
                break
            if first_code is None:
                continue
            for node in tree.body:
                if (isinstance(node, (ast.Import, ast.ImportFrom))
                        and node.lineno > first_code):
                    offenders.append(f"{self._rel(path)}:{node.lineno}")
        self.assertEqual(offenders, [], "import below code at module level")

    def test_no_banner_comments(self):
        offenders = []
        for path in self._python_files():
            with open(path) as handle:
                for number, line in enumerate(handle.read().splitlines(), 1):
                    if self.BANNER.match(line):
                        offenders.append(f"{self._rel(path)}:{number}")
        self.assertEqual(offenders, [], "decorative divider comment")

    def test_the_banner_pattern_recognises_real_dividers(self):
        """Guard the guard: a pattern that matches nothing would pass the test
        above no matter what the tree contained."""
        for divider in ("# ------------", "#####", "# ==== Section ====",
                        "    # ---- helpers ----", "# ~~~~~~~~"):
            with self.subTest(divider=divider):
                self.assertTrue(self.BANNER.match(divider))
        for prose in ("# store settings in ~/.config/s3duck",
                      "# ~75% top / ~25% bottom",
                      "#!/usr/bin/python",
                      "# navigation state",
                      "# land in navigate(restore_name=...)"):
            with self.subTest(prose=prose):
                self.assertFalse(self.BANNER.match(prose))
class DebianPackagingTests(unittest.TestCase):
    """REGRESSION (blank icons on an installed .deb): the package did not
    depend on python3-pyqt6.qtsvg, so Qt could not render the bundled SVG
    icons and every theme-less button came out empty. A new third-party
    import must not be able to ship a broken package again."""

    ROOT = PROJECT_ROOT

    # Third-party module -> the Debian package that provides it.
    PACKAGES = {
        "PyQt6": "python3-pyqt6",
        "boto3": "python3-boto3",
        "botocore": "python3-botocore",
        "cryptography": "python3-cryptography",
        "urllib3": "python3-urllib3",
    }
    # Imported behind a guard, so the app runs without them: these belong in
    # Recommends, and requiring them in Depends would be wrong.
    OPTIONAL = {"keyring": "python3-keyring", "awscrt": "python3-awscrt"}
    # Needed at runtime without being imported: Qt loads these as plugins.
    RUNTIME_ONLY = {"python3-pyqt6.qtsvg"}

    def _control(self):
        with open(os.path.join(self.ROOT, "DEBIAN", "control")) as handle:
            return handle.read()

    def _field(self, name):
        for line in self._control().splitlines():
            if line.startswith(f"{name}:"):
                return {part.split()[0].strip()
                        for part in line[len(name) + 1:].split(",")
                        if part.strip()}
        return set()

    def _depends(self):
        return self._field("Depends")

    def _recommends(self):
        return self._field("Recommends")

    def _third_party_imports(self):
        found = set()
        sources = read_app_sources(self.ROOT)
        self.assertGreaterEqual(len(sources), MIN_APP_SOURCES,
                                "source discovery collapsed")
        # First-party names to subtract: the top-level modules and the
        # packages they live beside. Walked, not listed, so a module that
        # moves into a package is still recognised as ours.
        local = set()
        for rel, _text in sources:
            parts = rel.split(os.sep)
            local.add(parts[0][:-3] if parts[0].endswith(".py") else parts[0])
        for _rel, text in sources:
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    found.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    found.add(node.module.split(".")[0])
        return {m for m in found
                if m not in sys.stdlib_module_names and m not in local}

    def test_every_third_party_import_is_a_declared_dependency(self):
        depends = self._depends()
        unmapped, undeclared = [], []
        for module in sorted(self._third_party_imports()):
            if module in self.OPTIONAL:
                continue
            package = self.PACKAGES.get(module)
            if package is None:
                unmapped.append(module)
            elif package not in depends:
                undeclared.append(f"{module} -> {package}")
        self.assertEqual(unmapped, [], "no Debian package mapped for these "
                                       "imports; add them to PACKAGES")
        self.assertEqual(undeclared, [], "imported but missing from Depends")

    def test_optional_imports_are_recommended_not_required(self):
        """A guarded import must not become a hard dependency, and must not
        be forgotten either — the OS secret store is unusable without it."""
        recommends = self._recommends()
        depends = self._depends()
        for module, package in sorted(self.OPTIONAL.items()):
            with self.subTest(module=module):
                self.assertIn(package, recommends)
                self.assertNotIn(package, depends)

    def test_the_svg_plugin_is_a_dependency(self):
        """It is never imported, so the import scan above cannot catch it —
        and without it every bundled icon renders blank."""
        self.assertTrue(self.RUNTIME_ONLY <= self._depends())

    def test_the_pdf_module_is_a_dependency(self):
        """The preview import-guards QtPdf, so a missing package degrades
        silently rather than failing loudly."""
        self.assertIn("python3-pyqt6.qtpdf", self._depends())

    def test_the_declared_python_floor_matches_the_documented_one(self):
        """The package said python3 (>=3.6) while the README required 3.10
        and CI tested 3.11 and 3.12. apt would have installed it onto a
        Python the project does not claim to support."""
        depends = self._control()
        match = re.search(r"python3 \(>=\s*(\d+)\.(\d+)\)", depends)
        self.assertTrue(match, "no python3 version floor in Depends")
        declared = (int(match.group(1)), int(match.group(2)))

        readme = os.path.join(self.ROOT, "README.md")
        with open(readme, encoding="utf-8") as handle:
            text = handle.read()
        row = re.search(r"\|\s*Python\s*\|\s*≥\s*(\d+)\.(\d+)\s*\|", text)
        self.assertTrue(row, "no Python row in the README requirements table")
        documented = (int(row.group(1)), int(row.group(2)))
        self.assertEqual(declared, documented)

    def test_the_version_and_size_are_placeholders_for_the_build(self):
        """Both are filled in by build_deb.sh; a literal here is a value that
        will drift, which is how the package once shipped mislabelled."""
        control = self._control()
        self.assertIn("Version: _version_", control)
        self.assertIn("Installed-Size: _size_", control)
        self.assertIn("Architecture: _arch_", control)

    def test_the_build_script_fills_in_every_placeholder(self):
        with open(os.path.join(self.ROOT, "build_deb.sh")) as handle:
            script = handle.read()
        for token in ("_version_", "_arch_", "_size_"):
            self.assertIn(f"s/{token}/", script, f"{token} never substituted")
class DocumentationAccuracyTests(unittest.TestCase):
    """The README is the feature list users read before the app. A menu entry
    it quotes that no longer exists, or a shortcut it names that nothing
    binds, is a bug report waiting to happen."""

    ROOT = PROJECT_ROOT
    # Keys Qt binds through a StandardKey rather than a literal sequence.
    STANDARD_KEYS = {
        "Ctrl+F": "StandardKey.Find",
        "Ctrl+C": "StandardKey.Copy",
        "Ctrl+X": "StandardKey.Cut",
        "Ctrl+V": "StandardKey.Paste",
    }
    # Prose spells arrows as glyphs; Qt spells them as words.
    ARROWS = {"←": "Left", "→": "Right", "↑": "Up", "↓": "Down"}

    def _readme(self):
        with open(os.path.join(self.ROOT, "README.md")) as handle:
            return handle.read()

    def _code(self):
        sources = read_app_sources(self.ROOT)
        assert len(sources) >= MIN_APP_SOURCES, "source discovery collapsed"
        return "".join(text for _name, text in sources)

    def test_every_menu_path_the_readme_names_exists(self):
        """A "Tools → X" the menu no longer has is a wild goose chase."""
        code = self._code()
        paths = set(re.findall(r"Tools → ([^)—,|]+)", self._readme()))
        self.assertTrue(paths, "no menu paths found — the pattern broke")
        missing = []
        for label in sorted(paths):
            label = label.strip().rstrip(".")
            if not label:
                continue
            # The README sometimes drops the ellipsis; the menu keeps it.
            if f'"{label}"' in code or f'"{label}…"' in code:
                continue
            if f'"{label}… (' in code or f'"{label} (' in code:
                continue
            missing.append(label)
        self.assertEqual(missing, [],
                         "README points at menu entries that do not exist")

    def test_every_quoted_label_exists_in_the_code(self):
        code = self._code()
        quoted = set(re.findall(r'"([A-Z][^"]{3,40}?)…?"', self._readme()))
        missing = sorted(label for label in quoted
                         if label.rstrip("…") not in code)
        self.assertEqual(missing, [],
                         "README quotes labels the code no longer has")

    def test_every_documented_shortcut_is_bound(self):
        code = self._code()
        readme = self._readme()
        keys = set(re.findall(
            r'`((?:Ctrl|Shift|Alt)\+[A-Za-z0-9+←→↑↓]+|F\d)`', readme))
        self.assertTrue(keys, "no shortcuts found — the pattern broke")
        # The arrow keys are documented as glyphs; without this they were
        # silently skipped by the pattern and never checked at all.
        self.assertTrue(any(any(arrow in key for arrow in self.ARROWS)
                            for key in keys),
                        "no arrow shortcut found — has the notation changed?")
        unbound = []
        for key in sorted(keys):
            spelled = key
            for glyph, word in self.ARROWS.items():
                spelled = spelled.replace(glyph, word)
            if f'QKeySequence("{spelled}")' in code:
                continue
            if spelled in self.STANDARD_KEYS \
                    and self.STANDARD_KEYS[spelled] in code:
                continue
            if f"({spelled})" in code or f"{spelled})" in code:
                continue
            unbound.append(key)
        self.assertEqual(unbound, [],
                         "README documents shortcuts nothing binds")

    def test_the_roadmap_does_not_still_promise_what_shipped(self):
        """Items move from ROADMAP.md to README.md when they land; a heading
        left behind is how a backlog stops being trusted."""
        with open(os.path.join(self.ROOT, "ROADMAP.md")) as handle:
            roadmap = handle.read()
        for shipped in ("**Dual-pane mode**", "**Lifecycle rules viewer/editor**",
                        "**CORS editor**", "**Requester-pays support**",
                        "**Watch mode**", "**Same-bucket remote↔remote sync**",
                        "**Treemap", "**Saved searches**"):
            self.assertNotIn(shipped, roadmap, f"{shipped} has shipped")

    def test_the_transfer_settings_bullet_is_not_stale(self):
        """It enumerates, which is exactly the shape of claim that drifts —
        three settings had already been added without it noticing."""
        readme = self._readme().lower()
        line = next((row for row in self._readme().splitlines()
                     if row.startswith("- **Transfer settings**")), "")
        self.assertTrue(line, "the Transfer settings bullet is gone")
        line = line.lower()
        for topic in ("files in flight", "multipart", "bandwidth",
                      "content-type", "upload rules", "listing",
                      "retry", "log", "checksum", "encryption"):
            with self.subTest(topic=topic):
                self.assertIn(topic, line)
        self.assertIn("transfer settings", readme)

    def test_model_is_still_the_facade_the_readme_describes(self):
        """The README tells a reader Model holds no bucket calls of its own.

        A method added to the class rather than to an s3/ mixin would make
        that false quietly -- and would be the first step back towards the
        4,000-line class it was.
        """
        source = os.path.join(self.ROOT, "model.py")
        with open(source, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        model = next(n for n in tree.body
                     if isinstance(n, ast.ClassDef) and n.name == "Model")
        own = sorted(n.name for n in model.body
                     if isinstance(n, ast.FunctionDef))
        self.assertEqual(own, ["__init__", "_guard_write", "get_os_family",
                               "set_parallel_files", "set_rate_limit",
                               "set_transfer_concurrency"])

    def test_the_key_components_table_names_the_right_file(self):
        """Every row says where a class lives; the refactor moved 33 of them.

        The table had gone on saying main_window.py for classes that were
        three packages away, which is worse than no table: it sends a reader
        to a file that genuinely used to hold them.
        """
        readme = self._readme()
        rows = re.findall(r"^\| `([A-Za-z_]\w*)` \| ([^|]+?) \| ",
                          readme, re.M)
        rows = [(name, claimed.strip()) for name, claimed in rows]
        self.assertGreater(len(rows), 30, "the table stopped being found")

        homes = {}
        for path in app_source_files(self.ROOT):
            rel = os.path.relpath(path, self.ROOT)
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            for node in tree.body:
                names = []
                if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                    names = [node.name]
                elif isinstance(node, ast.Assign):
                    names = [t.id for t in node.targets
                             if isinstance(t, ast.Name)]
                for name in names:
                    homes.setdefault(name, []).append(rel)

        wrong = []
        for name, claimed in rows:
            real = homes.get(name)
            if real is None:
                continue        # e2e module names and other prose rows
            if len(real) > 1:
                continue        # defined in more than one place; prose wins
            # "model.py + s3/" and the like describe a facade over a package.
            if claimed == real[0] or claimed.startswith(real[0] + " "):
                continue
            wrong.append(f"{name}: table says {claimed}, really {real[0]}")
        self.assertEqual(wrong, [])

    def test_the_architecture_tree_lists_every_module(self):
        """Every top-level module and every package must appear in the tree.

        Driven by what is on disk rather than by a list kept here, so a module
        that is added, moved or split cannot quietly go undocumented.
        """
        readme = self._readme()
        entries = set()
        for rel in app_source_names(self.ROOT):
            parts = rel.split(os.sep)
            # A package is documented by its directory, not file by file.
            entries.add(parts[0] if len(parts) > 1 else rel)
        self.assertGreaterEqual(len(entries), 8,
                                "source discovery collapsed")
        missing = [e for e in sorted(entries)
                   if e not in readme and f"{e}/" not in readme]
        self.assertEqual(missing, [],
                         f"missing from the README architecture tree: {missing}")
    def test_the_dialog_count_in_the_tree_is_the_real_one(self):
        """The README counts ui/dialogs for the reader; a stale number there is
        a small lie that nothing else catches.

        Counted from disk rather than pinned here, so splitting a dialog out
        fails this test instead of silently dating the README.
        """
        readme = self._readme()
        directory = os.path.join(self.ROOT, "ui", "dialogs")
        modules = [name for name in os.listdir(directory)
                   if name.endswith(".py") and name != "__init__.py"]
        classes = 0
        for name in modules:
            with open(os.path.join(directory, name), encoding="utf-8") as handle:
                classes += sum(1 for line in handle
                               if line.startswith("class "))
        self.assertGreaterEqual(len(modules), 5, "dialog discovery collapsed")
        self.assertIn(f"({len(modules)} modules, {classes} dialogs)", readme,
                      f"the README dialog count is stale: ui/dialogs holds "
                      f"{len(modules)} modules and {classes} dialogs")

    def test_the_readme_describes_every_ci_job(self):
        """The CI section counts the jobs and then names them. Adding a job and
        forgetting the prose leaves a reader who trusts it a job short."""
        readme = self._readme()
        with open(os.path.join(self.ROOT, ".github", "workflows", "ci.yml"),
                  encoding="utf-8") as handle:
            workflow = handle.read()
        # Only the keys inside the `jobs:` block: two-space keys also appear
        # under `on:`, and counting those made this test assert on YAML shape
        # rather than on how many jobs there are.
        jobs, inside = [], False
        for line in workflow.splitlines():
            if re.match(r"^jobs:\s*$", line):
                inside = True
                continue
            if inside:
                if line and not line.startswith(" "):
                    break
                match = re.match(r"^  ([a-z][\w-]*):\s*$", line)
                if match:
                    jobs.append(match.group(1))
        self.assertGreaterEqual(len(jobs), 3, "CI job discovery collapsed")

        words = {3: "three", 4: "four", 5: "five", 6: "six"}
        self.assertIn(f"runs {words.get(len(jobs), len(jobs))} jobs", readme,
                      f"the README miscounts the CI jobs; there are {len(jobs)}")

        # Each job's display name must appear, so a renamed job cannot leave
        # the list naming something that no longer runs.
        names = re.findall(r"^    name: (.+)$", workflow, re.M)
        missing = [n for n in names if n.split(" (")[0].strip() not in readme]
        self.assertEqual(missing, [],
                         f"CI jobs missing from the README: {missing}")

    def test_every_test_suite_is_documented(self):
        """A suite nobody knows about is a suite nobody runs.

        Driven by the directories under tests/, so adding one — as the GUI
        end-to-end suite was — fails here until the README says it exists.
        """
        readme = self._readme()
        tests_dir = os.path.join(self.ROOT, "tests")
        suites = sorted(
            name for name in os.listdir(tests_dir)
            if os.path.isdir(os.path.join(tests_dir, name))
            and not name.startswith((".", "__"))
        )
        self.assertGreaterEqual(len(suites), 2, "suite discovery collapsed")
        missing = [name for name in suites if f"{name}/" not in readme]
        self.assertEqual(missing, [],
                         f"test suites missing from the README: {missing}")

    def test_every_make_target_the_readme_names_exists(self):
        """The README tells the reader which make targets to use; one that was
        renamed leaves them with a command that fails."""
        readme = self._readme()
        with open(os.path.join(self.ROOT, "Makefile"), encoding="utf-8") as handle:
            makefile = handle.read()
        declared = set(re.findall(r"^\.PHONY:\s*(\S+)", makefile, re.M))
        self.assertGreaterEqual(len(declared), 5, "target discovery collapsed")

        # Only commands, never prose: "make a request" is a sentence, and
        # matching it would make this test fail on English rather than on a
        # renamed target. A command is a line of its own inside a fenced block,
        # or `make x` in backticks.
        named = set()
        fenced = False
        for line in readme.splitlines():
            if line.startswith("```"):
                fenced = not fenced
                continue
            if fenced:
                match = re.match(r"\s*make\s+([a-z][a-z0-9-]*)", line)
                if match:
                    named.add(match.group(1))
        named.update(re.findall(r"`make ([a-z][a-z0-9-]*)`", readme))
        unknown = sorted(name for name in named if name not in declared)
        self.assertEqual(unknown, [],
                         f"the README names make targets that do not exist: {unknown}")


class DesktopEntryTests(unittest.TestCase):
    """The URL handler only works if the desktop entry claims the scheme."""

    ROOT = PROJECT_ROOT

    def _entry(self):
        with open(os.path.join(self.ROOT, "resources", "s3duck.desktop")) as f:
            return f.read()

    def test_the_s3_scheme_is_claimed(self):
        self.assertIn("MimeType=x-scheme-handler/s3;", self._entry())

    def test_the_exec_line_takes_the_url(self):
        self.assertIn("%U", self._entry())

    def test_every_value_is_free_of_trailing_whitespace(self):
        """desktop-file-validate rejects "true " as a boolean, and the entry
        is installed unchecked by the postinst."""
        for line in self._entry().splitlines():
            with self.subTest(line=line):
                self.assertEqual(line, line.rstrip())

    def test_startup_notify_is_an_exact_boolean(self):
        self.assertIn("\nStartupNotify=true\n", "\n" + self._entry() + "\n")

    def test_the_package_refreshes_the_mime_cache(self):
        """Claiming the scheme does nothing until the cache knows."""
        with open(os.path.join(self.ROOT, "DEBIAN", "postinst")) as handle:
            self.assertIn("update-desktop-database", handle.read())
