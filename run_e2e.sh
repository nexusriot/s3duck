#!/bin/sh
# Run the end-to-end suite against a throwaway MinIO in Docker.
#
#   ./run_e2e.sh                 every end-to-end test
#   ./run_e2e.sh -k Traversal    pass extra arguments through to unittest
#
# Nothing is left behind: the server, its data and the private network are
# all removed on the way out, whether the suite passed or not.
#
# To run against a server you already have instead, skip this script and set
# the endpoint directly:
#
#   S3DUCK_TEST_ENDPOINT=http://localhost:9000 \
#   S3DUCK_TEST_ACCESS_KEY=... S3DUCK_TEST_SECRET_KEY=... \
#       python -m unittest discover -s tests/e2e -t .
set -eu

COMPOSE_FILE="$(dirname "$0")/tests/e2e/docker-compose.yml"

if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required (Docker Compose v2)." >&2
    exit 1
fi

cleanup() {
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
# Runs on success, failure and Ctrl-C alike. Note the explicit exit below:
# without it the script's status would be this trap's, not the suite's.
trap cleanup EXIT INT TERM

docker compose -f "$COMPOSE_FILE" build runner

# The suite's own status is the script's status, so `set -e` is lifted for
# exactly this command — otherwise a failing test would exit here and the
# explicit propagation below would be dead code. The cleanup trap ends in
# `|| true` so it can never overwrite the status either.
set +e
if [ "$#" -gt 0 ]; then
    docker compose -f "$COMPOSE_FILE" run --rm runner \
        python -m unittest discover -s tests/e2e -t . -v "$@"
else
    docker compose -f "$COMPOSE_FILE" run --rm runner
fi
status=$?
set -e

exit "$status"
