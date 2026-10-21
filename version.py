"""The app version — the one place it is written.

It lives in its own module rather than in ``main_window.py`` so that the
packaging scripts can read it without caring how the UI is laid out. Both
``Makefile`` and ``build_deb.sh`` sed this file; a refactor that moves the
window code can no longer strand them.
"""

__VERSION__ = "0.27.0"
