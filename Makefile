# S3 Duck 🦆 — one front door for building, testing and packaging.
#
# The shell scripts stay the source of truth for how a thing is built:
# build_deb.sh, build_linux_bin.sh, build_macos_bin.sh, build_dmg.sh and
# run_e2e.sh are what this file calls, and CI goes through this file
# (`make deb`, `make check-deb`, `make e2e-all`) rather than around it.
# Re-implementing any of them here would give the project two answers to the
# same question, which is how the packaged version drifted behind the app
# version once already.
#
# The version is never written here either — it is read from version.py,
# the single source of truth that build_deb.sh also seds.
#
#   make            list the targets
#   make check      everything: unit suite + end-to-end
#   make deb        a .deb for this machine's architecture

APP        := s3duck
VERSION    := $(shell sed -n 's/^__VERSION__ = "\(.*\)"$$/\1/p' version.py)
BUILD_DIR  := build
DIST_DIR   := dist

# Prefer the project venv when it exists: PyQt6 lives there, and a bare
# python3 runs the suite without it and fails confusingly.
PYTHON     ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

# `-t .` is load-bearing. Without it `tests` becomes discovery's top-level
# directory, the suite modules import as `unit.test_window` rather than
# `tests.unit.test_window`, and tests/__init__.py never runs — see its
# docstring.
UNITTEST   := $(PYTHON) -m unittest discover -s tests -t .

.DEFAULT_GOAL := help

# ---- help ------------------------------------------------------------------

.PHONY: help
help:
	@echo "S3 Duck $(VERSION) — make targets"
	@echo ""
	@echo "  Testing"
	@echo "    test          offscreen unit suite (no network, no server)"
	@echo "    e2e           model suite against a throwaway MinIO in Docker"
	@echo "    e2e-gui       GUI suite: the real window, headless, same MinIO"
	@echo "    e2e-all       both end-to-end suites"
	@echo "    check         test + both e2e suites — what to run before pushing"
	@echo "    test-v        unit suite, verbose"
	@echo "    test-one T=x  one test, class or module (e.g. T=tests.unit.test_core_planning)"
	@echo ""
	@echo "  Everything"
	@echo "    all           every artifact this host can build -> $(DIST_DIR)/"
	@echo "                  Linux: binary, Windows .exe, debs, rpm"
	@echo "                  macOS: universal .app, .pkg and .dmg"
	@echo ""
	@echo "  Packaging"
	@echo "    deb           .deb for this machine's architecture"
	@echo "    deb-amd64     .deb for amd64"
	@echo "    deb-arm64     .deb for arm64"
	@echo "    debs          both architectures"
	@echo "    check-deb     inspect the built .deb the way CI does"
	@echo "    rpm           noarch .rpm, built in a Fedora container -> $(DIST_DIR)/"
	@echo "    pkg           universal (arm64+x86_64) macOS installer .pkg (run on macOS)"
	@echo ""
	@echo "  Binaries (PyInstaller)"
	@echo "    bin, linux    self-contained Linux binary -> $(DIST_DIR)/"
	@echo "    macos         self-contained macOS binary (run on macOS)"
	@echo "    dmg           pack the macOS binary into a .dmg (run on macOS)"
	@echo "    win           Windows .exe via Wine in Docker -> $(DIST_DIR)/"
	@echo "    win-lock      refresh the hash-locked Windows wheel list"
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

# make test-one T=tests.unit.test_s3_transfer.BatchDeleteTests
.PHONY: test-one
test-one:
	@test -n "$(T)" || { echo "usage: make test-one T=tests.unit.test_window.SomeTests"; exit 2; }
	$(PYTHON) -m unittest -v $(T)

# Brings up its own MinIO and removes it again; needs Docker with the Compose
# v2 plugin (`docker compose`) and nothing else.
# Both suites skip themselves when no endpoint is configured, which is why
# `test` stays offline even though it discovers tests/e2e and tests/e2e_gui.
.PHONY: e2e
e2e:
	./run_e2e.sh

# The GUI suite: the real MainWindow against the same throwaway MinIO, on Qt's
# offscreen platform. Separate target because it costs a PyQt6 image to build
# and runs slower than the model suite it complements.
.PHONY: e2e-gui
e2e-gui:
	./run_e2e.sh gui

.PHONY: e2e-all
e2e-all:
	./run_e2e.sh all

.PHONY: check
check: test e2e-all

# ---- packaging -------------------------------------------------------------

# bash, not ./: the scripts carry a `#!/bin/env bash` shebang that only
# resolves on a usr-merged filesystem.
.PHONY: deb
deb:
	bash build_deb.sh
	@echo ">> $(DIST_DIR)/$(APP)_$(VERSION)_*.deb"

.PHONY: deb-amd64
deb-amd64:
	bash build_deb.sh amd64
	@echo ">> $(DIST_DIR)/$(APP)_$(VERSION)_amd64.deb"

.PHONY: deb-arm64
deb-arm64:
	bash build_deb.sh arm64
	@echo ">> $(DIST_DIR)/$(APP)_$(VERSION)_arm64.deb"

# Pure Python, so the payload is identical and only the control field differs.
.PHONY: debs
debs: deb-amd64 deb-arm64

.PHONY: check-deb
check-deb:
	bash tools/ci_check_deb.sh

# Same payload as the .deb (tools/stage_payload.sh), noarch, built with
# Fedora's rpmbuild in a pinned container: packaging/rpm/.
.PHONY: rpm
rpm:
	bash build_rpm.sh
	@echo ">> $(DIST_DIR)/$(APP)-$(VERSION)-1.*.noarch.rpm"

# Universal installer for /Applications. Needs a universal2 python.org Python
# (PYTHON=...); builds in its own venv under build/, not .venv.
.PHONY: pkg
pkg:
	bash build_pkg.sh

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

# A Windows .exe from Linux: Windows Python + PyInstaller under Wine, in a
# pinned Docker image (see packaging/windows/Dockerfile). Only the exe comes
# out; build_win.cmd stays the way to build natively on Windows.
# After changing requirements.txt, refresh the lock with `make win-lock`.
.PHONY: win windows
win windows:
	DOCKER_BUILDKIT=1 docker build \
		-f packaging/windows/Dockerfile \
		--target artifact \
		--output type=local,dest=$(DIST_DIR) \
		.
	@echo ">> $(DIST_DIR)/$(APP).exe"

.PHONY: win-lock
win-lock:
	uv pip compile requirements.txt \
		--python-platform x86_64-pc-windows-msvc --python-version 3.12 \
		--only-binary :all: --generate-hashes --no-header \
		-o packaging/windows/requirements.lock

# ---- everything ------------------------------------------------------------

# Every artifact this host can build, all in dist/. PyInstaller cannot
# cross-build macOS and pkgbuild/create-dmg are macOS tools, so the set
# depends on where it runs; a Linux run plus a macOS run is the full release.
# Not the default goal: bare `make` still prints help.
#
# On macOS the steps are ordered, not prerequisites: `pkg` builds the
# universal dist/s3duck.app that `dmg` then packs, and `make -j` must not
# run them side by side.
.PHONY: all
ifeq ($(shell uname -s),Darwin)
all:
	$(MAKE) pkg
	$(MAKE) dmg
	@$(MAKE) --no-print-directory all-report
else
all: bin win debs rpm
	@$(MAKE) --no-print-directory all-report
	@echo "  (macOS .app/.pkg/.dmg: run 'make all' on a Mac)"
endif

.PHONY: all-report
all-report:
	@echo ""
	@echo "built for $(shell uname -s) in $(DIST_DIR)/:"
	@cd $(DIST_DIR) && ls -1d $(APP) $(APP).exe $(APP).app *.pkg *.dmg *.deb *.rpm \
		2>/dev/null | sed 's/^/  /'

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
