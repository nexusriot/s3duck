#!/bin/env bash

mkdir s3duck_0.0-2_amd64
cd s3duck_0.0-2_amd64

dpkg-deb --build --root-owner-group s3duck_0.0-2_amd64
