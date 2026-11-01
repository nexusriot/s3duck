#!/bin/bash

# Requires create-dmg - 'brew install create-dmg'
#
# Packs the bundle build_macos_bin.sh produces. That script passes no --name,
# so PyInstaller names the bundle after the entry script: dist/s3duck.app.
# The same name is used everywhere below; create-dmg positions icons by the
# exact file name, which a case-sensitive volume will not match loosely.
app="s3duck.app"

if [ ! -d "dist/$app" ]; then
  echo "dist/$app not found — run build_macos_bin.sh (make macos) first"; exit 1
fi

# Create a folder (named dmg) to prepare our DMG in (if it doesn't already exist).
mkdir -p dist/dmg
# Empty the dmg folder; -f so a fresh, empty folder is not an error.
rm -rf dist/dmg/*
# Copy the app bundle to the dmg folder.
cp -r "dist/$app" dist/dmg
# If the DMG already exists, delete it.
test -f "dist/S3Duck.dmg" && rm "dist/S3Duck.dmg"
create-dmg \
  --volname "S3Duck" \
  --volicon "resources/ducky.icns" \
  --window-pos 200 120 \
  --window-size 600 300 \
  --icon-size 100 \
  --icon "$app" 175 120 \
  --hide-extension "$app" \
  --app-drop-link 425 120 \
  "dist/S3Duck.dmg" \
  "dist/dmg/"
