# S3 Duck 🦆 — one front door for building, testing and packaging.
#
# The shell scripts stay the source of truth for how a thing is built:
# build_deb.sh, build_linux_bin.sh, build_macos_bin.sh, build_dmg.sh and
# run_e2e.sh are what CI calls and what this file calls. Re-implementing any
# of them here would give the project two answers to the same question, which
# is how the packaged version drifted behind the app version once already.
#
# The version is never written here either — it is read from main_window.py,
# the single source of truth that build_deb.sh also seds.
#
#   make            list the targets
#   make check      everything: unit suite + end-to-end
#   make deb        a .deb for this machine's architecture

APP        := s3duck
VERSION    := $(shell sed -n 's/^__VERSION__ = "\(.*\)"$$/\1/p' main_window.py)
BUILD_DIR  := build
DIST_DIR   := dist

# Prefer the project venv when it exists: PyQt6 lives there, and a bare
# python3 runs the suite without it and fails confusingly.
PYTHON     ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

# `-t .` is load-bearing. Without it `tests` becomes discovery's top-level
# directory, the suite imports as `test_units` rather than `tests.test_units`,
# and tests/__init__.py never runs — see its docstring.
UNITTEST   := $(PYTHON) -m unittest discover -s tests -t .

.DEFAULT_GOAL := help

# ---- help ------------------------------------------------------------------

.PHONY: help
help:
	@echo "S3 Duck $(VERSION) — make targets"
	@echo ""
	@echo "  Testing"
	@echo "    test          offscreen unit suite (no network, no server)"
	@echo "    e2e           end-to-end suite against a throwaway MinIO in Docker"
	@echo "    check         test + e2e — what to run before pushing"
	@echo "    test-v        unit suite, verbose"
	@echo "    test-one T=x  one test, class or module (e.g. T=tests.test_units.SyncPlanTests)"
	@echo ""
	@echo "  Packaging"
	@echo "    deb           .deb for this machine's architecture"
	@echo "    deb-amd64     .deb for amd64"
	@echo "    deb-arm64     .deb for arm64"
	@echo "    debs          both architectures"
	@echo "    check-deb     inspect the built .deb the way CI does"
	@echo ""
	@echo "  Binaries (PyInstaller)"
	@echo "    bin           self-contained Linux binary -> $(DIST_DIR)/"
	@echo "    macos         self-contained macOS binary (run on macOS)"
	@echo "    dmg           pack the macOS binary into a .dmg (run on macOS)"
	@echo "    windows       how to build on Windows"
	@echo ""
	@echo "  Development"
	@echo "    run           launch the app from this checkout"
	@echo "    venv          create .venv and install requirements.txt"
	@echo "    env           report the Python/Qt versions under test"
	@echo "    icons         report how this desktop resolves every icon"
	@echo "    version       print $(VERSION)"
	@echo "    clean         remove build/, dist/, caches and PyInstaller leftovers"

# ---- testing ---------------------------------------------------------------

.PHONY: test
test:
	$(UNITTEST)

.PHONY: test-v
test-v:
	$(UNITTEST) -v

# make test-one T=tests.test_units.BatchDeleteTests
.PHONY: test-one
test-one:
	@test -n "$(T)" || { echo "usage: make test-one T=tests.test_units.SomeTests"; exit 2; }
	$(PYTHON) -m unittest -v $(T)

# Brings up its own MinIO and removes it again; needs Docker and nothing else.
# The suite skips itself when no endpoint is configured, which is why `test`
# stays offline even though it discovers tests/e2e too.
.PHONY: e2e
e2e:
	./run_e2e.sh

.PHONY: check
check: test e2e

# ---- packaging -------------------------------------------------------------

# bash, not ./: the scripts carry a `#!/bin/env bash` shebang that only
# resolves on a usr-merged filesystem.
.PHONY: deb
deb:
	bash build_deb.sh
	@echo ">> $(BUILD_DIR)/$(APP)_$(VERSION)_*.deb"

.PHONY: deb-amd64
deb-amd64:
	bash build_deb.sh amd64
	@echo ">> $(BUILD_DIR)/$(APP)_$(VERSION)_amd64.deb"

.PHONY: deb-arm64
deb-arm64:
	bash build_deb.sh arm64
	@echo ">> $(BUILD_DIR)/$(APP)_$(VERSION)_arm64.deb"

# Pure Python, so the payload is identical and only the control field differs.
.PHONY: debs
debs: deb-amd64 deb-arm64

.PHONY: check-deb
check-deb:
	bash tools/ci_check_deb.sh

# ---- binaries --------------------------------------------------------------

# The build scripts call a bare `pyinstaller`, which requirements.txt installs
# into .venv rather than onto PATH — so without this the target dies with
# "command not found" on a checkout that has never been activated.
VENV_BIN := $(CURDIR)/.venv/bin

.PHONY: bin linux
bin linux: export PATH := $(VENV_BIN):$(PATH)
bin linux:
	@command -v pyinstaller >/dev/null 2>&1 || { \
		echo "pyinstaller not found — run 'make venv' first"; exit 1; }
	bash build_linux_bin.sh
	@echo ">> $(DIST_DIR)/$(APP)"

.PHONY: macos
macos: export PATH := $(VENV_BIN):$(PATH)
macos:
	bash build_macos_bin.sh $(ARCH)

.PHONY: dmg
dmg: export PATH := $(VENV_BIN):$(PATH)
dmg:
	bash build_dmg.sh

.PHONY: windows
windows:
	@echo "Windows builds run build_win.cmd on Windows:"
	@echo "    build_win.cmd"
	@echo "(PyInstaller cannot cross-build a Windows binary from here.)"

# ---- development -----------------------------------------------------------

.PHONY: run
run:
	$(PYTHON) s3duck.py

.PHONY: venv
venv:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@echo ">> .venv ready; make test"

.PHONY: env
env:
	$(PYTHON) tools/ci_env.py

.PHONY: icons
icons:
	$(PYTHON) tools/icon_report.py

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR) $(DIST_DIR) .pytest_cache
	rm -f $(APP).spec
	find . -path ./.venv -prune -o -name __pycache__ -type d -print0 \
		| xargs -0 -r rm -rf
	@echo "cleaned"
