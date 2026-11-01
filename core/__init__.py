"""Pure domain logic: no Qt, no boto3 import, no I/O beyond the filesystem.

Everything here was carved out of main_window.py and model.py, where it was
reachable only by importing a GUI or a boto3 client. It is the part of S3 Duck
that can be reasoned about and tested on its own. The one helper that talks to
S3 at all, parts._multipart_upload_size, does it through a client it is handed.
"""
