#!/bin/bash

pyinstaller --windowed --no-confirm --add-data="icons:icons" --add-data="resources/ducky.icns:resources/." --onefile --icon=resources/ducky.icns s3duck.py
