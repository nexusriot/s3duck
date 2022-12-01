#!/bin/env bash

version=0.0.4

echo "building deb for s3duck $version"

if ! type "dpkg-deb" > /dev/null; then
  echo "please install required build tools first"
fi

project="s3duck_${version}_amd64"
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
cp *.py $lib_dir


sed -i "s/_version_/$version/g" $folder_name/DEBIAN/control

cd build/ && dpkg-deb --build --root-owner-group $project
