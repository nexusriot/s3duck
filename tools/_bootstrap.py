"""
Put the project root on ``sys.path`` for the scripts in this directory.

Its own module so those scripts can keep every import at the top: the root has
to be importable before ``utils`` resolves, and inlining the path setup would
push the remaining imports below code.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
