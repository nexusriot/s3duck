"""Block-at-a-time digests, answered the way S3 stores them.

Which algorithms are actually offered depends on what awscrt provides and
what this botocore can put on the wire, so the table itself is built in
the S3 layer. These are the primitives it builds from.
"""

import base64
import hashlib
import struct


class _CrcDigest:
    """A CRC accumulated over blocks, answered the way S3 stores it."""

    def __init__(self, fn, width):
        self._fn = fn
        self._width = width
        self._value = 0

    def update(self, block):
        self._value = self._fn(block, self._value)

    def b64(self) -> str:
        fmt = ">I" if self._width == 4 else ">Q"
        mask = (1 << (self._width * 8)) - 1
        return base64.b64encode(
            struct.pack(fmt, self._value & mask)).decode()
class _HashDigest:
    """A hashlib digest, answered the way S3 stores it."""

    def __init__(self, name):
        self._digest = hashlib.new(name)

    def update(self, block):
        self._digest.update(block)

    def b64(self) -> str:
        return base64.b64encode(self._digest.digest()).decode()
