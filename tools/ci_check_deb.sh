#!/usr/bin/env bash
# Check the built .deb: metadata the build fills in, and the payload details
# that past bugs turned into shipped regressions.
set -euo pipefail

deb=$(ls build/*.deb)
echo "checking $deb"
dpkg-deb -I "$deb"

fields=$(dpkg-deb -f "$deb")
for placeholder in _version_ _arch_ _size_; do
  if grep -q "$placeholder" <<<"$fields"; then
    echo "::error::build left $placeholder unsubstituted in the control file"
    exit 1
  fi
done

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

# The PNG twins are what keep icons visible on a Qt with no SVG plugin, so one
# missing from the package is a regression a user would see immediately.
contents=$(dpkg-deb -c "$deb")
svgs=$(grep -c 'icons/.*\.svg' <<<"$contents" || true)
pngs=$(grep -c 'icons/.*\.png' <<<"$contents" || true)
echo "icons packaged: $svgs svg, $pngs png"
if [ "$pngs" -lt "$svgs" ]; then
  echo "::error::$svgs SVG icons but only $pngs PNG twins packaged"
  exit 1
fi

echo "package looks good"
