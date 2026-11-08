#!/usr/bin/env bash
# Build the .rpm (noarch) in a pinned Fedora container: packaging/rpm/.
# The payload is the one build_deb.sh ships, via tools/stage_payload.sh.
set -euo pipefail

version=$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py)
if [ -z "$version" ]; then
  echo "cannot read __VERSION__ from version.py"; exit 1
fi
echo "building rpm for s3duck $version (noarch)"

DOCKER_BUILDKIT=1 docker build \
  -f packaging/rpm/Dockerfile \
  --target artifact \
  --output type=local,dest=dist \
  .
