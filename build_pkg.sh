#!/bin/bash
# Universal (arm64 + x86_64) macOS installer: dist/S3Duck-<version>-universal.pkg
# Runs on macOS only (PyInstaller cannot cross-build, pkgbuild is Apple's).
#
# A universal2 PyInstaller build needs every binary in the app to be fat, but
# PyQt6-Qt6 and cffi publish only per-arch macOS wheels. So this downloads
# both arch variants of every wheel, fuses the thin ones with delocate-merge,
# installs the result into a universal2 venv, and only then runs PyInstaller
# with --target-arch universal2.
#
# Needs a universal2 Python 3 -- the python.org installer, not Homebrew's,
# which is single-arch. Point at it with PYTHON=, e.g.
#   PYTHON=/Library/Frameworks/Python.framework/Versions/3.12/bin/python3 make pkg
# Set PKG_SIGN_IDENTITY="Developer ID Installer: ..." to sign the .pkg.
set -euo pipefail

if [ "$(uname -s)" != Darwin ]; then
  echo "build_pkg.sh runs on macOS (make all builds the Linux/Windows set here)"; exit 1
fi

version=$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py)
if [ -z "$version" ]; then
  echo "cannot read __VERSION__ from version.py"; exit 1
fi

PYTHON="${PYTHON:-python3}"
py_exe=$("$PYTHON" -c 'import os, sys; print(os.path.realpath(sys.executable))')
py_archs=$(lipo -archs "$py_exe")
case " $py_archs " in
  *" arm64 "*" x86_64 "*|*" x86_64 "*" arm64 "*) ;;
  *) echo "$py_exe is '$py_archs', not universal2 -- install Python from python.org and pass PYTHON=..."
     exit 1 ;;
esac
py_ver=$("$PYTHON" -c 'import sys; print(f"{sys.version_info[0]}{sys.version_info[1]}")')
echo "building universal s3duck $version with $py_exe (cp$py_ver, $py_archs)"

venv=build/venv-universal2
wheels=build/wheels-universal2
rm -rf "$wheels"
mkdir -p "$wheels/arm64" "$wheels/x86_64" "$wheels/fused"
"$PYTHON" -m venv --clear "$venv"
"$venv/bin/pip" install -q --upgrade pip "delocate==0.12.0"

# Both resolutions accept universal2 wheels, so those come down identical
# for each arch; only the thin ones need fusing.
for arch in arm64 x86_64; do
  "$venv/bin/pip" download -q -r requirements.txt -d "$wheels/$arch" \
    --only-binary=:all: --implementation cp --python-version "$py_ver" \
    --platform "macosx_11_0_$arch"
done

for whl in "$wheels"/arm64/*.whl; do
  name=$(basename "$whl")
  if [ -f "$wheels/x86_64/$name" ]; then
    cp "$whl" "$wheels/fused/"          # universal2 or pure Python
    continue
  fi
  dist_ver=$(cut -d- -f1-2 <<<"$name")   # e.g. PyQt6_Qt6-6.7.3
  other=$(ls "$wheels/x86_64/$dist_ver"-*.whl 2>/dev/null | head -1 || true)
  if [ -z "$other" ]; then
    echo "no x86_64 wheel for $name (different version resolved?)"; exit 1
  fi
  echo "fusing $dist_ver"
  "$venv/bin/delocate-merge" "$whl" "$other" -w "$wheels/fused"
done

"$venv/bin/pip" install -q --no-index --find-links "$wheels/fused" -r requirements.txt

rm -rf build/s3duck dist/s3duck.app
PATH="$PWD/$venv/bin:$PATH" bash build_macos_bin.sh universal2

app=dist/s3duck.app
app_archs=$(lipo -archs "$app/Contents/MacOS/s3duck")
echo "app binary: $app_archs"
case " $app_archs " in
  *" x86_64 "*" arm64 "*|*" arm64 "*" x86_64 "*) ;;
  *) echo "$app is not universal"; exit 1 ;;
esac

# ---- installer -------------------------------------------------------------

identifier="${PKG_IDENTIFIER:-com.nexusriot.s3duck}"
pkg_work=build/pkg
root="$pkg_work/root"
rm -rf "$pkg_work"
mkdir -p "$root/Applications"
ditto "$app" "$root/Applications/s3duck.app"

# Not relocatable: otherwise Installer "upgrades" any s3duck.app it finds
# elsewhere on disk (say, one dragged out of the .dmg) instead of installing
# to /Applications.
pkgbuild --analyze --root "$root" "$pkg_work/component.plist"
plutil -replace 0.BundleIsRelocatable -bool NO "$pkg_work/component.plist"

pkgbuild --root "$root" --install-location / \
  --component-plist "$pkg_work/component.plist" \
  --identifier "$identifier" --version "$version" \
  "$pkg_work/s3duck.pkg"

# Declare both architectures, so Apple silicon installs natively instead of
# asking for Rosetta.
productbuild --synthesize --package "$pkg_work/s3duck.pkg" "$pkg_work/distribution.xml"
if ! grep -q hostArchitectures "$pkg_work/distribution.xml"; then
  perl -pi -e 's/<options /<options hostArchitectures="arm64,x86_64" /' \
    "$pkg_work/distribution.xml"
fi

out="dist/S3Duck-$version-universal.pkg"
sign=()
if [ -n "${PKG_SIGN_IDENTITY:-}" ]; then
  sign=(--sign "$PKG_SIGN_IDENTITY")
fi
productbuild --distribution "$pkg_work/distribution.xml" \
  --package-path "$pkg_work" "${sign[@]+"${sign[@]}"}" "$out"
echo ">> $out"
