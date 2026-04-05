#!/bin/env bash

# Detect architecture for informational purposes
machine=$(uname -m)
echo "building s3duck binary for $machine"

pyinstaller --windowed --add-data="icons:icons" --add-data="resources/ducky.ico:resources/." --onefile --icon=resources/ducky.ico s3duck.py
