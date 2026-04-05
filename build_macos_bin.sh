#!/bin/bash

# Accept target architecture as argument: x86_64, arm64, or universal2
# Defaults to the current machine architecture if not specified
target_arch="${1:-}"

arch_flag=""
if [ -n "$target_arch" ]; then
  echo "building s3duck for macOS ($target_arch)"
  arch_flag="--target-arch $target_arch"
else
  echo "building s3duck for macOS ($(uname -m))"
fi

pyinstaller --windowed --no-confirm --add-data="icons:icons" --add-data="resources/ducky.icns:resources/." --onefile --icon=resources/ducky.icns $arch_flag s3duck.py
