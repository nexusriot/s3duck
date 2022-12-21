#!/bin/bash

# Requires create-dmg - 'brew install create-dmg'

# Create a folder (named dmg) to prepare our DMG in (if it doesn't already exist).
mkdir -p dist/dmg
# Empty the dmg folder.
rm -r dist/dmg/*
# Copy the app bundle to the dmg folder.
cp -r "dist/s3duck.app" dist/dmg
# If the DMG already exists, delete it.
test -f "dist/S3Duck.dmg" && rm "dist/S3Duck.dmg"
create-dmg \
  --volname "S3Duck" \
  --volicon "resources/ducky.icns" \
  --window-pos 200 120 \
  --window-size 600 300 \
  --icon-size 100 \
  --icon "S3Duck.app" 175 120 \
  --hide-extension "S3Duck.app" \
  --app-drop-link 425 120 \
  "dist/S3Duck.dmg" \
  "dist/dmg/"