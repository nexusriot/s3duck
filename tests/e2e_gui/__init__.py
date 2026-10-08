"""
End-to-end tests that drive the real GUI against a real S3-compatible server.

This is the complement to ``tests/e2e``, not a replacement for it, and the
split is about which seam each one covers:

* ``tests/unit`` builds a real ``QApplication`` and a real ``MainWindow``, but
  against ``FakeS3Client``. It proves the window reacts correctly to answers
  the suite itself wrote.
* ``tests/e2e`` drives ``model.py`` against a real MinIO, with no Qt at all.
  It proves the S3 layer says true things about a real server.
* Neither runs the two together. Everything between a user gesture and
  ``model.py`` — the navigation worker, the transfer queue, the per-worker
  model clones, the signal/slot marshalling back onto the UI thread — is
  exercised only against a fake, and the places where a background thread
  meets a real network are exactly where this kind of application breaks.

So the tests here construct the actual ``MainWindow``, point it at a throwaway
MinIO, call the methods the menus and shortcuts call, pump the Qt event loop
until the real worker threads finish, and then ask the bucket — through a
separate boto3 client — what actually happened.

Headless: ``QT_QPA_PLATFORM=offscreen`` (set in ``tests/__init__.py``), so no
display, no X server and no window manager are involved.

The suite skips itself unless ``S3DUCK_TEST_ENDPOINT`` is set, so
``python -m unittest discover -s tests -t .`` stays green and offline. Bring up
a throwaway MinIO and run it with::

    ./run_e2e.sh gui

What is deliberately *not* faked: the server, the worker threads, the queue,
the model. What is faked: ``QFileDialog`` and ``QMessageBox``, because those
are the operating system's UI rather than this application's logic, and a
headless run has nobody to click them. Each test says which it stubbed.
"""
