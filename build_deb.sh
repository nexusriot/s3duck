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

if ! type "dpkg-deb" > /dev/null 2>&1; then
  echo "dpkg-deb not found: install the dpkg package first"; exit 1
fi

project="s3duck_${version}_${arch}"
folder_name="build/$project"
echo "creating $folder_name"
mkdir -p $folder_name
cp -r DEBIAN/ $folder_name
bin_dir="$folder_name/usr/bin"
mkdir -p $bin_dir
cp s3duck $bin_dir
cp s3duck-cli $bin_dir
# The /usr/lib/s3duck tree; shared with build_rpm.sh so both ship the same.
bash tools/stage_payload.sh "$folder_name/usr/lib/s3duck" || exit 1


sed -i "s/_version_/$version/g" $folder_name/DEBIAN/control
sed -i "s/_arch_/$arch/g" $folder_name/DEBIAN/control

# Measured, not hardcoded: the old fixed "58" had drifted far behind the real
# tree, exactly like the version string used to.
size=$(du -sk --exclude=DEBIAN "$folder_name" | cut -f1)
sed -i "s/_size_/$size/g" $folder_name/DEBIAN/control

# Staged under build/, shipped to dist/ with every other artifact.
mkdir -p dist
dpkg-deb --build -Z gzip --root-owner-group "$folder_name" dist/
