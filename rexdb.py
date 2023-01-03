import struct


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
