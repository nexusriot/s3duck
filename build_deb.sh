#!/bin/env bash

# Single source of truth: __VERSION__ in version.py. Hardcoding it here
# let the package version drift behind the app version.
version=$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py)
if [ -z "$version" ]; then
  echo "cannot read __VERSION__ from version.py"; exit 1
fi

# Detect or accept architecture
if [ -n "$1" ]; then
  arch="$1"
else
  machine=$(uname -m)
  case "$machine" in
    x86_64)  arch="amd64" ;;
    aarch64) arch="arm64" ;;
    *)       echo "unsupported architecture: $machine"; exit 1 ;;
  esac
fi

echo "building deb for s3duck $version ($arch)"

if ! type "dpkg-deb" > /dev/null; then
  echo "please install required build tools first"
fi

project="s3duck_${version}_${arch}"
folder_name="build/$project"
echo "crating $folder_name"
mkdir -p $folder_name
cp -r DEBIAN/ $folder_name
bin_dir="$folder_name/usr/bin"
lib_dir="$folder_name/usr/lib/s3duck"
res_dir="$lib_dir/resources"
mkdir -p $bin_dir
mkdir -p $lib_dir
mkdir -p $res_dir
cp s3duck $bin_dir
cp -r icons/ $lib_dir
cp resources/ducky.ico $res_dir
cp resources/ducky.png $lib_dir
cp resources/s3duck.desktop $lib_dir
cp LICENSE $lib_dir
# The top-level modules, then every package beside them. A flat `cp *.py` was
# what shipped before, and it would drop a package directory silently: the
# install then dies on its first import. Packages are discovered by their
# __init__.py rather than listed, so adding one needs no edit here -- but
# tests/ is a package too and has no business in the payload, so it is named.
cp *.py $lib_dir
find . -mindepth 2 -maxdepth 2 -name __init__.py \
     -not -path './.venv/*' -not -path './build/*' -not -path './tests/*' \
     -printf '%h\0' \
  | xargs -0 -r -I{} cp -r {} $lib_dir
# cp -r brings the build host's bytecode along, which is dead weight in the
# package and stale the moment the interpreter differs.
find $lib_dir -name __pycache__ -type d -prune -exec rm -rf {} +


sed -i "s/_version_/$version/g" $folder_name/DEBIAN/control
sed -i "s/_arch_/$arch/g" $folder_name/DEBIAN/control

# Measured, not hardcoded: the old fixed "58" had drifted far behind the real
# tree, exactly like the version string used to.
size=$(du -sk --exclude=DEBIAN "$folder_name" | cut -f1)
sed -i "s/_size_/$size/g" $folder_name/DEBIAN/control

cd build/ && dpkg-deb --build -Z gzip --root-owner-group $project
