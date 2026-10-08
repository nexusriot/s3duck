"""Pure domain logic: no Qt, no boto3, no I/O beyond the filesystem.

Everything here was carved out of main_window.py and model.py, where it was
reachable only by importing a GUI or a boto3 client. It is the part of S3 Duck
that can be reasoned about and tested on its own.
"""
