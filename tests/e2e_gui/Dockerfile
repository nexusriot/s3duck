# The GUI end-to-end runner.
#
# Unlike tests/e2e, this one drives the real MainWindow, so it needs PyQt6 and
# everything PyQt6 needs to load a platform plugin. It runs on Qt's "offscreen"
# platform: no X server, no window manager, no display — the widgets are real
# and simply render nowhere.
#
# The system packages below are what the offscreen plugin links against. They
# are a deliberate floor rather than a copied-in blob: adding libraries until
# the error goes away is how an image grows to 2 GB and stops being evidence of
# what the application actually requires.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        libxkbcommon0 \
        libdbus-1-3 \
        libegl1 \
        libfontconfig1 \
        libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

# Pinned to requirements.txt: a GUI run that disagrees with the app about its
# own Qt or SDK version is testing something else. There is no old-SDK variant
# of this image — that axis is about parameters boto3 refuses client-side, and
# tests/e2e already covers it without paying for Qt.
ARG BOTO3_VERSION=1.42.44
ARG PYQT_VERSION=6.7.0
ARG CRYPTOGRAPHY_VERSION=46.0.4
RUN pip install --no-cache-dir \
        boto3==${BOTO3_VERSION} \
        PyQt6==${PYQT_VERSION} \
        cryptography==${CRYPTOGRAPHY_VERSION}

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    QT_QPA_PLATFORM=offscreen \
    # Qt writes runtime files here; without it every run logs a warning about
    # XDG_RUNTIME_DIR being unset, which buries the actual test output.
    XDG_RUNTIME_DIR=/tmp/runtime

RUN mkdir -p /tmp/runtime && chmod 700 /tmp/runtime

# The tree is mounted read-only by compose; nothing is copied in, so a run
# always tests the working tree rather than a stale layer.
CMD ["python", "-m", "unittest", "discover", "-s", "tests/e2e_gui", "-t", ".", "-v"]
