#!/usr/bin/env bash
# Check the built .deb: metadata the build fills in, and the payload details
# that past bugs turned into shipped regressions.
set -euo pipefail

# This version's package, not whatever build/ happens to hold. CI checks a
# fresh checkout with exactly one .deb in it, so `ls build/*.deb` worked there
# and broke the moment the version was bumped locally: the glob then matched
# the previous package too and handed dpkg-deb a two-line filename.
version=$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py)
if [ -z "$version" ]; then
  echo "::error::cannot read __VERSION__ from version.py"
  exit 1
fi

check_one() {
  local deb="$1"
  echo "checking $deb"
  dpkg-deb -I "$deb"

  local fields
  fields=$(dpkg-deb -f "$deb")
  for placeholder in _version_ _arch_ _size_; do
    if grep -q "$placeholder" <<<"$fields"; then
      echo "::error::build left $placeholder unsubstituted in the control file"
      exit 1
    fi
  done

  local size
  size=$(dpkg-deb -f "$deb" Installed-Size)
  if [ "$size" -lt 100 ]; then
    echo "::error::Installed-Size is $size kB, which looks hardcoded again"
    exit 1
  fi

  # Qt needs the SVG plugin to draw the bundled .svg icons at all; without this
  # dependency the toolbar came out blank on Linux Mint.
  for dep in python3-pyqt6.qtsvg python3-pyqt6.qtpdf; do
    grep -q "$dep" <<<"$fields" || {
      echo "::error::$dep missing from Depends"; exit 1; }
  done

  # The PNG twins are what keep icons visible on a Qt with no SVG plugin, so
  # one missing from the package is a regression a user would see immediately.
  local contents svgs pngs
  contents=$(dpkg-deb -c "$deb")
  svgs=$(grep -c 'icons/.*\.svg' <<<"$contents" || true)
  pngs=$(grep -c 'icons/.*\.png' <<<"$contents" || true)
  echo "icons packaged: $svgs svg, $pngs png"
  if [ "$pngs" -lt "$svgs" ]; then
    echo "::error::$svgs SVG icons but only $pngs PNG twins packaged"
    exit 1
  fi
}

shopt -s nullglob
debs=(build/s3duck_"${version}"_*.deb)
shopt -u nullglob

if [ "${#debs[@]}" -eq 0 ]; then
  echo "::error::no .deb for ${version} in build/ — run 'make deb' first"
  exit 1
fi

# `make debs` builds one per architecture, and both are worth checking.
for deb in "${debs[@]}"; do
  check_one "$deb"
done

echo "package looks good"
