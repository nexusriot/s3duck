"""A QR code encoder for one job: putting a presigned link on a phone.

Byte mode only, all 40 versions, all four error-correction levels -- the
standard algorithm (ISO/IEC 18004) written out here rather than adding a
dependency to a desktop app for a single dialog. A presigned URL with a
session token runs past a thousand characters, which is why the larger
versions matter.
"""

# Error-correction levels, in the order of the tables below.
LOW, MEDIUM, QUARTILE, HIGH = range(4)
_FORMAT_BITS = {LOW: 1, MEDIUM: 0, QUARTILE: 3, HIGH: 2}

_ECC_PER_BLOCK = (
    (-1, 7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28,
     28, 28, 30, 30, 26, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30),
    (-1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26,
     26, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28),
    (-1, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30,
     28, 30, 30, 30, 30, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30),
    (-1, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 26, 28,
     30, 24, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30),
)
_BLOCKS = (
    (-1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 6, 6, 6, 7, 8,
     8, 9, 9, 10, 12, 12, 12, 13, 14, 15, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25),
    (-1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16,
     17, 17, 18, 20, 21, 23, 25, 26, 28, 29, 31, 33, 35, 37, 38, 40, 43, 45, 47, 49),
    (-1, 1, 1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 10, 12, 16, 12, 17, 16, 18, 21, 20,
     23, 23, 25, 27, 29, 34, 34, 35, 38, 40, 43, 45, 48, 51, 53, 56, 59, 62, 65, 68),
    (-1, 1, 1, 2, 4, 4, 4, 5, 6, 8, 8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25,
     25, 34, 30, 32, 35, 37, 40, 42, 45, 48, 51, 54, 57, 60, 63, 66, 70, 74, 77, 81),
)


def _raw_modules(version) -> int:
    """Data-carrying modules once every function pattern is drawn."""
    result = (16 * version + 128) * version + 64
    if version >= 2:
        aligns = version // 7 + 2
        result -= (25 * aligns - 10) * aligns - 55
        if version >= 7:
            result -= 36
    return result


def data_capacity(version, ecl) -> int:
    """Data codewords a version holds at an error-correction level."""
    return (_raw_modules(version) // 8
            - _ECC_PER_BLOCK[ecl][version] * _BLOCKS[ecl][version])


def byte_capacity(version, ecl) -> int:
    """Bytes of payload a version holds in byte mode."""
    header_bits = 4 + (8 if version <= 9 else 16)
    return (data_capacity(version, ecl) * 8 - header_bits) // 8


def _gf_mul(x, y) -> int:
    z = 0
    for i in reversed(range(8)):
        z = (z << 1) ^ ((z >> 7) * 0x11D)
        z ^= ((y >> i) & 1) * x
    return z


def _rs_divisor(degree):
    result = [0] * (degree - 1) + [1]
    root = 1
    for _ in range(degree):
        for j in range(degree):
            result[j] = _gf_mul(result[j], root)
            if j + 1 < degree:
                result[j] ^= result[j + 1]
        root = _gf_mul(root, 0x02)
    return result


def _rs_remainder(data, divisor):
    result = [0] * len(divisor)
    for byte in data:
        factor = byte ^ result.pop(0)
        result.append(0)
        for i, coef in enumerate(divisor):
            result[i] ^= _gf_mul(coef, factor)
    return result


def _codewords(payload, version, ecl):
    """Data plus error correction, split into blocks and interleaved."""
    count_bits = 8 if version <= 9 else 16
    bits = []

    def put(value, length):
        bits.extend((value >> i) & 1 for i in reversed(range(length)))

    put(0b0100, 4)
    put(len(payload), count_bits)
    for byte in payload:
        put(byte, 8)
    capacity = data_capacity(version, ecl) * 8
    put(0, min(4, capacity - len(bits)))
    put(0, (-len(bits)) % 8)
    pad = 0xEC
    while len(bits) < capacity:
        put(pad, 8)
        pad ^= 0xEC ^ 0x11
    data = [int("".join(map(str, bits[i:i + 8])), 2)
            for i in range(0, len(bits), 8)]

    blocks_n = _BLOCKS[ecl][version]
    ecc_len = _ECC_PER_BLOCK[ecl][version]
    raw = _raw_modules(version) // 8
    short_n = blocks_n - raw % blocks_n
    short_len = raw // blocks_n
    divisor = _rs_divisor(ecc_len)
    blocks, k = [], 0
    for i in range(blocks_n):
        size = short_len - ecc_len + (0 if i < short_n else 1)
        chunk = data[k:k + size]
        k += size
        ecc = _rs_remainder(chunk, divisor)
        if i < short_n:
            chunk.append(0)
        blocks.append(chunk + ecc)
    out = []
    for i in range(len(blocks[0])):
        for j, block in enumerate(blocks):
            if i != short_len - ecc_len or j >= short_n:
                out.append(block[i])
    return out


def _alignment_positions(version):
    if version == 1:
        return []
    count = version // 7 + 2
    step = (version * 8 + count * 3 + 5) // (count * 4 - 4) * 2
    size = version * 4 + 17
    return [6] + sorted(size - 7 - i * step for i in range(count - 1))


_MASKS = (
    lambda x, y: (x + y) % 2 == 0,
    lambda x, y: y % 2 == 0,
    lambda x, y: x % 3 == 0,
    lambda x, y: (x + y) % 3 == 0,
    lambda x, y: (x // 3 + y // 2) % 2 == 0,
    lambda x, y: x * y % 2 + x * y % 3 == 0,
    lambda x, y: (x * y % 2 + x * y % 3) % 2 == 0,
    lambda x, y: ((x + y) % 2 + x * y % 3) % 2 == 0,
)


class _Symbol:
    def __init__(self, version):
        self.version = version
        self.size = version * 4 + 17
        self.dark = [[False] * self.size for _ in range(self.size)]
        self.function = [[False] * self.size for _ in range(self.size)]

    def fixed(self, x, y, dark):
        self.dark[y][x] = bool(dark)
        self.function[y][x] = True

    def draw_function_patterns(self):
        size = self.size
        for i in range(size):
            self.fixed(6, i, i % 2 == 0)
            self.fixed(i, 6, i % 2 == 0)
        for cx, cy in ((3, 3), (size - 4, 3), (3, size - 4)):
            for dy in range(-4, 5):
                for dx in range(-4, 5):
                    x, y = cx + dx, cy + dy
                    if 0 <= x < size and 0 <= y < size:
                        self.fixed(x, y, max(abs(dx), abs(dy)) not in (2, 4))
        positions = _alignment_positions(self.version)
        last = len(positions) - 1
        for i, ax in enumerate(positions):
            for j, ay in enumerate(positions):
                if (i, j) in ((0, 0), (0, last), (last, 0)):
                    continue
                for dy in range(-2, 3):
                    for dx in range(-2, 3):
                        self.fixed(ax + dx, ay + dy, max(abs(dx), abs(dy)) != 1)
        self.draw_format(LOW, 0)
        if self.version >= 7:
            rem = self.version
            for _ in range(12):
                rem = (rem << 1) ^ ((rem >> 11) * 0x1F25)
            bits = self.version << 12 | rem
            for i in range(18):
                bit = (bits >> i) & 1
                a, b = size - 11 + i % 3, i // 3
                self.fixed(a, b, bit)
                self.fixed(b, a, bit)

    def draw_format(self, ecl, mask):
        data = _FORMAT_BITS[ecl] << 3 | mask
        rem = data
        for _ in range(10):
            rem = (rem << 1) ^ ((rem >> 9) * 0x537)
        bits = (data << 10 | rem) ^ 0x5412
        size = self.size

        def bit(i):
            return (bits >> i) & 1

        for i in range(6):
            self.fixed(8, i, bit(i))
        self.fixed(8, 7, bit(6))
        self.fixed(8, 8, bit(7))
        self.fixed(7, 8, bit(8))
        for i in range(9, 15):
            self.fixed(14 - i, 8, bit(i))
        for i in range(8):
            self.fixed(size - 1 - i, 8, bit(i))
        for i in range(8, 15):
            self.fixed(8, size - 15 + i, bit(i))
        self.fixed(8, size - 8, True)

    def draw_codewords(self, codewords):
        size, i, total = self.size, 0, len(codewords) * 8
        right = size - 1
        while right >= 1:
            if right == 6:
                right = 5
            for vert in range(size):
                for j in range(2):
                    x = right - j
                    upward = ((right + 1) & 2) == 0
                    y = size - 1 - vert if upward else vert
                    if not self.function[y][x] and i < total:
                        self.dark[y][x] = bool(
                            (codewords[i >> 3] >> (7 - (i & 7))) & 1)
                        i += 1
            right -= 2

    def apply_mask(self, mask):
        test = _MASKS[mask]
        for y in range(self.size):
            for x in range(self.size):
                if not self.function[y][x] and test(x, y):
                    self.dark[y][x] = not self.dark[y][x]

    def penalty(self) -> int:
        """The standard's four penalty rules; lower is easier to scan."""
        size, dark, score = self.size, self.dark, 0
        lines = dark + [list(col) for col in zip(*dark)]
        for line in lines:
            run, colour = 0, None
            for cell in line:
                if cell == colour:
                    run += 1
                else:
                    if run >= 5:
                        score += run - 2
                    colour, run = cell, 1
            if run >= 5:
                score += run - 2
            text = "".join("1" if c else "0" for c in line)
            for pattern in ("10111010000", "00001011101"):
                start = text.find(pattern)
                while start != -1:
                    score += 40
                    start = text.find(pattern, start + 1)
        for y in range(size - 1):
            for x in range(size - 1):
                colour = dark[y][x]
                if colour == dark[y][x + 1] == dark[y + 1][x] == dark[y + 1][x + 1]:
                    score += 3
        total = sum(sum(row) for row in dark)
        k = (abs(total * 20 - size * size * 10) + size * size - 1) // (size * size) - 1
        score += max(0, k) * 10
        return score


def choose_version(length, ecl, minimum=1) -> int:
    for version in range(max(1, minimum), 41):
        if byte_capacity(version, ecl) >= length:
            return version
    raise ValueError(
        f"{length} bytes do not fit in a QR code at this error-correction "
        "level; shorten the link (fewer parameters, no session token).")


def encode(text, ecl=MEDIUM, mask=None, version=None):
    """
    The QR symbol for *text* as rows of booleans (True = dark), no border.

    ``mask`` None picks the lowest-penalty mask, as a scanner-friendly
    encoder should; a fixed mask is accepted so the output can be compared
    module for module with another encoder.
    """
    payload = text.encode("utf-8") if isinstance(text, str) else bytes(text)
    version = version or choose_version(len(payload), ecl)
    if byte_capacity(version, ecl) < len(payload):
        raise ValueError("the payload does not fit in that version")
    codewords = _codewords(payload, version, ecl)
    best = None
    for candidate in ([mask] if mask is not None else range(8)):
        symbol = _Symbol(version)
        symbol.draw_function_patterns()
        symbol.draw_codewords(codewords)
        symbol.apply_mask(candidate)
        symbol.draw_format(ecl, candidate)
        score = symbol.penalty() if mask is None else 0
        if best is None or score < best[0]:
            best = (score, symbol)
    return [list(row) for row in best[1].dark]
