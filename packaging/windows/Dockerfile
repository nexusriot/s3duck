# syntax=docker/dockerfile:1
# Windows .exe build on a Linux host: Windows Python + PyInstaller under Wine.
#
# PyInstaller cannot cross-compile, so this runs the *Windows* interpreter
# under Wine. It installs the win_amd64 wheels and runs the Windows
# PyInstaller, which produces a genuine PE executable. The command at the end
# is build_win.cmd's.
#
# Hermetic by construction. Every input is pinned:
#   - base image by digest
#   - Debian packages from a dated snapshot.debian.org archive
#   - Windows CPython by NuGet package version + sha256
#   - every wheel by packaging/windows/requirements.lock (pip --require-hashes)
# and the PyInstaller step runs with --network=none, so it can only use what
# the layers above it fetched.
#
# Build with `make win` (BuildKit required). The exe lands in dist/.
# `make win-installer` builds the setup program instead: the stages at the
# bottom of this file.

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS toolchain

ARG DEBIAN_SNAPSHOT=20261101T000000Z
ARG PYTHON_NUGET_VERSION=3.12.10
ARG PYTHON_NUGET_SHA256=0eb85c2dfccccf1b17352de4c397f69194035b7d37149eacc16f1147d93de3b8

ENV DEBIAN_FRONTEND=noninteractive \
    WINEPREFIX=/wine \
    WINEARCH=win64 \
    WINEDEBUG=-all \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Point apt at a fixed day of the Debian archive, so `wine` is the same build
# every time rather than whatever the mirror carries today.
RUN rm -f /etc/apt/sources.list.d/debian.sources \
 && printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s trixie main\ndeb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/%s trixie-security main\n' \
        "$DEBIAN_SNAPSHOT" "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list \
 && apt-get -o Acquire::Retries=5 update \
 && apt-get install -y --no-install-recommends \
        ca-certificates curl unzip wine wine64 python3-pip \
 && rm -rf /var/lib/apt/lists/*

# The NuGet "python" package is a plain zip of a full CPython install (pip
# included). That avoids running the MSI-based installer under Wine, which is
# the fragile part of most Wine+Python setups.
RUN wineboot --init \
 && wineserver -w \
 && curl -fsSL -o /tmp/python.nupkg \
        "https://www.nuget.org/api/v2/package/python/${PYTHON_NUGET_VERSION}" \
 && echo "${PYTHON_NUGET_SHA256}  /tmp/python.nupkg" | sha256sum -c - \
 && unzip -q /tmp/python.nupkg 'tools/*' -d /tmp/py \
 && mv /tmp/py/tools "$WINEPREFIX/drive_c/Python" \
 && rm -rf /tmp/python.nupkg /tmp/py \
 && wine 'C:\Python\python.exe' --version \
 && wineserver -w

# Wheels are fetched by the *Linux* pip, asking for Windows/CPython 3.12 tags.
# Wine's socket layer has broken pip's HTTPS (seen on Wine 8), and this keeps
# every Wine step offline. The Windows pip then installs them with no index,
# hash-checked a second time.
#
# `wineserver -w` after each Wine step is load-bearing: Wine keeps the
# registry in memory and writes it out when wineserver exits. Without the
# wait, the layer is committed without system.reg, the prefix has no PATH,
# PyQt6's import fails during PyInstaller analysis, and the exe ships
# without the Qt DLLs.
COPY packaging/windows/requirements.lock /tmp/requirements.lock
RUN python3 -m pip download \
        --require-hashes --only-binary=:all: --no-deps \
        --platform win_amd64 --python-version 3.12 \
        --implementation cp --abi cp312 \
        -d /wheels -r /tmp/requirements.lock
RUN --network=none \
    wine 'C:\Python\python.exe' -m pip install \
        --no-index --find-links 'Z:\wheels' \
        --require-hashes --only-binary=:all: --no-deps \
        -r 'Z:\tmp\requirements.lock' \
 && wine 'C:\Python\python.exe' -c "import PyQt6.QtWidgets" \
 && wineserver -w \
 && rm -rf /wheels

# ---------------------------------------------------------------------------

FROM toolchain AS source

# Fixed hash seed and timestamp for reproducibility.
# PE header timestamps still vary, so builds are close but not byte-identical.
ENV PYTHONHASHSEED=0 \
    SOURCE_DATE_EPOCH=1700000000

WORKDIR /src
COPY . .

# ---------------------------------------------------------------------------

# `make win`: one self-contained exe that unpacks itself on every start.
FROM source AS build

RUN --network=none \
    wine 'C:\Python\python.exe' -m PyInstaller \
        --noconfirm --clean \
        --windowed --onefile \
        --add-data='icons;icons' \
        --add-data='resources/ducky.ico;resources/.' \
        --icon='resources/ducky.ico' \
        --distpath /out --workpath /tmp/pyi-build \
        s3duck.py

# ---------------------------------------------------------------------------

# Only the exe leaves the build: `docker build --output type=local,dest=dist`
# writes this stage's filesystem, owned by the invoking user.
FROM scratch AS artifact
COPY --from=build /out/s3duck.exe /s3duck.exe

# ---------------------------------------------------------------------------

# `make win-installer`: the same app as a folder (s3duck.exe + _internal/).
# An installed program has somewhere to keep its files, so it should not pay
# the one-file exe's unpack-to-%TEMP% on every launch.
FROM source AS build-dir

RUN --network=none \
    wine 'C:\Python\python.exe' -m PyInstaller \
        --noconfirm --clean \
        --windowed --onedir \
        --add-data='icons;icons' \
        --add-data='resources/ducky.ico;resources/.' \
        --icon='resources/ducky.ico' \
        --distpath /out --workpath /tmp/pyi-build \
        s3duck.py \
 && test -f /out/s3duck/s3duck.exe && test -d /out/s3duck/_internal

# ---------------------------------------------------------------------------

# NSIS is a native Linux compiler for Windows installers, so this stage needs
# no Wine. Its own stage, from the same pinned base and archive snapshot,
# rather than one more package in `toolchain`: that would rebuild the whole
# Wine prefix for the sake of a tool the exe build never runs.
FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS installer-build

ARG DEBIAN_SNAPSHOT=20261101T000000Z
ENV DEBIAN_FRONTEND=noninteractive \
    SOURCE_DATE_EPOCH=1700000000

RUN rm -f /etc/apt/sources.list.d/debian.sources \
 && printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s trixie main\ndeb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/%s trixie-security main\n' \
        "$DEBIAN_SNAPSHOT" "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list \
 && apt-get -o Acquire::Retries=5 update \
 && apt-get install -y --no-install-recommends nsis \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /installer
COPY --from=build-dir /out/s3duck ./app
COPY packaging/windows/installer.nsi LICENSE version.py ./
COPY resources/ducky.ico ./

# The version is read from version.py here, the way the Makefile and
# build_deb.sh read it, so it is never written a second time.
RUN --network=none \
    VERSION="$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py)" \
 && test -n "$VERSION" \
 && mkdir /out \
 && makensis -V2 -WX -DVERSION="$VERSION" \
        -DOUTFILE="/out/s3duck-${VERSION}-setup.exe" installer.nsi

FROM scratch AS installer
COPY --from=installer-build /out/ /
