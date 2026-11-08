#!/usr/bin/env bash
# Copy the app's installed tree (/usr/lib/s3duck) into DEST.
#
# Shared by build_deb.sh and build_rpm.sh, so the two packages cannot ship
# different file sets. Run from the repository root.
#
#   tools/stage_payload.sh build/s3duck_1.0_amd64/usr/lib/s3duck
set -euo pipefail

lib_dir="${1:?usage: tools/stage_payload.sh DEST}"
res_dir="$lib_dir/resources"

mkdir -p "$lib_dir" "$res_dir"
cp -r icons/ "$lib_dir"
cp resources/ducky.ico "$res_dir"
cp resources/ducky.png "$lib_dir"
cp resources/s3duck.desktop "$lib_dir"
cp LICENSE "$lib_dir"
# The top-level modules, then every package beside them. A flat `cp *.py` was
# what shipped before, and it would drop a package directory silently: the
# install then dies on its first import. Packages are discovered by their
# __init__.py rather than listed, so adding one needs no edit here -- but
# tests/ is a package too and has no business in the payload, so it is named.
cp ./*.py "$lib_dir"
find . -mindepth 2 -maxdepth 2 -name __init__.py \
     -not -path './.venv/*' -not -path './build/*' -not -path './tests/*' \
     -printf '%h\0' \
  | xargs -0 -r -I{} cp -r {} "$lib_dir"
# cp -r brings the build host's bytecode along, which is dead weight in the
# package and stale the moment the interpreter differs.
find "$lib_dir" -name __pycache__ -type d -prune -exec rm -rf {} +
