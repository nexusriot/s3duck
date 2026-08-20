"""
Package init for the test suite: environment that must exist before any Qt or
application import.

Living here rather than at the top of ``test_units.py`` keeps every import in
that module contiguous at the top. Python runs this file first for both
invocation styles — ``unittest discover -s tests -t .`` and
``unittest tests.test_units.SomeClass`` — which a plain helper module could not
do, because the two put different directories on ``sys.path``.

The ``-t .`` is load-bearing. Left off, ``tests`` becomes discovery's top-level
directory, the suite imports as ``test_units`` instead of ``tests.test_units``,
and this file never runs at all; the first widget test then aborts the
interpreter looking for an X display. ``_ensure_qapp`` sets the platform too,
so a wrong invocation is now merely wrong rather than fatal.

``QT_QPA_PLATFORM`` is read when the QApplication is constructed, so it has to
be set before the first widget test, and the project root has to be importable
before ``main_window`` and friends resolve.
"""
import os
import sys

# Widget tests need a Qt platform plugin; the headless one lets the suite run
# without a display.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
