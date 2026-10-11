"""The unit suite, in modules that mirror the source tree.

This was one test_units.py of 16,000 lines and 200 classes -- the same
problem as the 14,000-line main_window.py it was testing. Two classes in it
had even ended up with the same name, so one of them silently never ran.

Shared fakes and helpers live in tests/support.py.
"""
