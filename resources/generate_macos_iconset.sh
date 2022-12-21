#!/bin/bash

# small utility script for making MacOS .icns package

# generate different icon sizes
# requires imagemagick to be installed: 'brew install imagemagick'
OUTPUT_PATH=$(pwd)/ducky.iconset

for size in 16 32 64 128 256; do
  half="$(($size / 2))"
  convert ducky.png -resize x$size $OUTPUT_PATH/icon_${size}x${size}.png
  convert ducky.png -resize x$size $OUTPUT_PATH/icon_${half}x${half}@2x.png
done

# pack them into .icns
iconutil -c icns ducky.iconset