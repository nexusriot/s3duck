#!/bin/sh
# Run the end-to-end suite against a throwaway MinIO in Docker.
#
#   ./run_e2e.sh                 the model suite, on both SDK versions
#   ./run_e2e.sh gui             the GUI suite: the real window, headless
#   ./run_e2e.sh all             both of the above
#   ./run_e2e.sh -k Traversal    pass extra arguments through to unittest
#
# The two suites cover different seams. tests/e2e drives model.py and needs no
# Qt at all, which is what keeps it small and fast. tests/e2e_gui drives the
# real MainWindow on Qt's offscreen platform, which is the only thing that
# exercises the navigation worker, the transfer queue and the signals that
# carry a worker's result back onto the UI thread.
#
# The model suite runs twice: once on the boto3 in requirements.txt, once on
# the oldest supported, because a parameter the installed boto3 does not know
# is refused client-side and no backend can stand in for that. The GUI suite
# runs once — that axis is about the SDK, not about the window. Narrow a run
# with
#   S3DUCK_TEST_RUNNERS=runner ./run_e2e.sh
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
#
# Swap tests/e2e for tests/e2e_gui to drive the window instead; that one also
# needs PyQt6 and the libraries Qt's offscreen platform links against.
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

# Both SDK axes by default: the pinned boto3 the app develops against, and
# the oldest one it supports. The second is not redundant — boto3 refuses a
# parameter its bundled service model does not know before any request is
# sent, so the code that stands down from IfNoneMatch, IfMatch and
# ChecksumType is unreachable on a current SDK no matter which backend is
# behind it. Set S3DUCK_TEST_RUNNERS to just one of them to narrow a run.
runners="${S3DUCK_TEST_RUNNERS:-runner runner-old}"

# A leading selector picks the suite and overrides S3DUCK_TEST_RUNNERS;
# anything after it still goes to unittest. Explicit rather than inferred,
# because "which suites ran" is the first thing to know about a failing e2e
# report.
case "${1:-}" in
    gui) runners="runner-gui"; shift ;;
    all) runners="runner runner-old runner-gui"; shift ;;
    model) runners="runner runner-old"; shift ;;
esac

# Each runner discovers its own suite. The images' CMDs already say this, but
# an invocation with extra arguments has to spell it out.
suite_for() {
    case "$1" in
        runner-gui) echo "tests/e2e_gui" ;;
        *) echo "tests/e2e" ;;
    esac
}

for runner in $runners; do
    docker compose -f "$COMPOSE_FILE" build "$runner"
done

# The suite's own status is the script's status, so `set -e` is lifted for
# exactly these commands — otherwise a failing test would exit here and the
# explicit propagation below would be dead code. The cleanup trap ends in
# `|| true` so it can never overwrite the status either.
set +e
status=0
for runner in $runners; do
    echo
    echo "=== $runner ==="
    if [ "$#" -gt 0 ]; then
        docker compose -f "$COMPOSE_FILE" run --rm "$runner" \
            python -m unittest discover -s "$(suite_for "$runner")" -t . -v "$@"
    else
        docker compose -f "$COMPOSE_FILE" run --rm "$runner"
    fi
    # First failure wins the status, but every runner still runs: knowing
    # which SDKs a change breaks is the whole point of having two.
    one=$?
    [ "$one" -eq 0 ] || status="$one"
done
set -e

exit "$status"
