import struct
try:
    from ucollections import namedtuple
except ImportError:
    from collections import namedtuple

VERSION = "0.0.1"
VERSION_BYTE = 0x00

UNSIGNED_CHAR = 'B'
UNSIGNED_LONG_LONG = 'Q'


def unpack_one(fstring, data):
    """Unpack a single line of data"""
    data = struct.unpack(f'<{fstring}', data)
    return data[0]


Header = namedtuple("Header", ("version", "start_date", "end_date", "fstring"))


def read_header(file: str) -> Header:
    """read_header(file: str) -> Header

    Read the header of a database file

    The format of a header is:
    | Version | Start date | End data/incomplete | Format string length | Format string |
    | 1 byte  |  8 bytes   | 8 byte              | 1 byte               | n bytes       |
    Max length of format string is 256 bytes
    """
    with open(file, "rb") as f:
        version = unpack_one(UNSIGNED_CHAR, f.read(1))
        assert(version == VERSION_BYTE)
        (start_date, end_date, fstring_len) = struct.unpack(
            f'<{UNSIGNED_LONG_LONG}{UNSIGNED_LONG_LONG}{UNSIGNED_CHAR}',
            f.read(8+8+1))
        print(version, start_date, end_date, fstring_len)
        fstring = unpack_one(f"{fstring_len}s", f.read(fstring_len))
        return Header(version, start_date, end_date, fstring)


def header_to_bytes(header: Header) -> bytes:
    return struct.pack(f"<{UNSIGNED_CHAR}{UNSIGNED_LONG_LONG}{UNSIGNED_LONG_LONG}{UNSIGNED_CHAR}{len(header.fstring)}s",
                       header.version, header.start_date, header.end_date, len(header.fstring), header.fstring)


class RexDB:

    def __init__(self, file, fstring, lines=100, cursor=0):
        self._fd = open(file, "wb")
        self._fstring = fstring
        self._line_size = struct.calcsize(fstring)
        self._cursor = cursor
        self._file = file
        self._lines = lines

        self._fd.seek(0, self._line_size*self._cursor)

    def log(self, data):
        if self._cursor >= self._lines:
            self._cursor = 0
            self._fd.seek(0, 0)
        data = struct.pack(self._fstring, *data)
        self._fd.write(data)
        self._cursor += 1

    def nth(self, n):
        self._fd.flush()
        with open(self._file, "rb") as fd:
            fd.seek(0, n*self._line_size)
            return struct.unpack(self._fstring, fd.read(self._line_size))

    def col(self, i):
        self._fd.flush()
        data = []
        with open(self._file, "rb") as fd:
            for _ in range(self._lines):
                line = fd.read(self._line_size)
                print(line)
                if len(line) != self._line_size:
                    break
                line = struct.unpack(self._fstring, line)
                data.append(line[i])
        return data[self._cursor:] + data[:self._cursor]
