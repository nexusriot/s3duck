"""
End-to-end tests against a real S3-compatible server.

Everything in tests/unit/ stubs boto3, which is what makes that suite
fast and offline — but it also means every claim this project makes about how
a *server* behaves is unverified. The README and ROADMAP are full of
"AWS answers this; most S3-compatible backends do not", and there is exactly
one way to know: ask one.

The suite skips itself unless S3DUCK_TEST_ENDPOINT is set, so
``python -m unittest discover -s tests -t .`` stays green and offline. Bring
up a throwaway MinIO and run it with::

    ./run_e2e.sh

or point it at any endpoint you already have::

    S3DUCK_TEST_ENDPOINT=http://localhost:9000 \
    S3DUCK_TEST_ACCESS_KEY=minioadmin \
    S3DUCK_TEST_SECRET_KEY=minioadmin \
        python -m unittest discover -s tests -t .

It targets model.py rather than the GUI on purpose: that module is the whole
S3 surface and imports nothing from Qt, so the runner needs no display and no
Qt at all.

The GUI above it is covered by its sibling, ``tests/e2e_gui``, which drives the
real MainWindow against the same kind of throwaway server on Qt's offscreen
platform. The split is deliberate: everything that can be checked without Qt
is checked here, where it is fast and the image is small, and only the seam
that genuinely needs a window — the navigation worker, the transfer queue, the
signals that carry a worker's result back onto the UI thread — pays for PyQt6.
"""
