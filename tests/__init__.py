"""
Package init for the test suite: environment that must exist before any Qt or
application import.

Living here rather than at the top of ``test_units.py`` keeps every import in
that module contiguous at the top. Python runs this file first for both
invocation styles — ``unittest discover -s tests`` and
``unittest tests.test_units.SomeClass`` — which a plain helper module could not
do, because the two put different directories on ``sys.path``.

``QT_QPA_PLATFORM`` has no effect once PyQt6 has been imported, and the project
root has to be importable before ``main_window`` and friends resolve.
"""
import os
import sys

# Widget tests need a Qt platform plugin; the headless one lets the suite run
# without a display.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
