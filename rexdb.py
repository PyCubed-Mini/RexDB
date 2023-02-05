import struct


class DensePacker():
    def __init__(self, fstring: str) -> None:
        self.make_format(fstring)

        # formats for space c: char, i: int32, f: float, ?: bool, h: int16
    def make_format(self, fstring: str) -> str:
        charList = {"c": 1, "?": 0.1, "h": 2, "i": 4, "f": 4.1}
        denseFormat = list(self._user_fstring)
        denseFormat.sort(key=(lambda c: charList[c]), reverse=True)
        return ''.join(denseFormat)

    def pack(self, data: tuple) -> tuple:
        pass

    def unpack(self, data: tuple) -> tuple:
        pass


class RexDB:

    def __init__(self, file, fstring, lines=100, cursor=0):
        self._fd = open(file, "wb")
        self._user_fstring = fstring
        self._dense_fstring = self.make_format()
        self._line_size = struct.calcsize(self._dense_fstring)
        self._cursor = cursor
        self._file = file
        self._lines = lines

        self._fd.seek(0, self._line_size*self._cursor)

    def log(self, data):
        if self._cursor >= self._lines:
            self._cursor = 0
            self._fd.seek(0, 0)
        mapped_data = self.map_user_dense(data)
        data = struct.pack(self._dense_fstring, *mapped_data)
        self._fd.write(data)
        self._cursor += 1

    def nth(self, n):
        self._fd.flush()
        with open(self._file, "rb") as fd:
            fd.seek(0, n*self._line_size)
            return self.map_dense_user(struct.unpack(self._dense_fstring, fd.read(self._line_size)))

    def col(self, i):
        self._fd.flush()
        data = []
        with open(self._file, "rb") as fd:
            for _ in range(self._lines):
                line = fd.read(self._line_size)
                print(line)
                if len(line) != self._line_size:
                    break
                line = self.map_dense_user(struct.unpack(self._dense_fstring, line))
                data.append(line[i])
        return data[self._cursor:] + data[:self._cursor]

    def map_dense_user(self, data: tuple) -> tuple:
        user_format = [*self._user_fstring]
        # input is in dense_format
        input = [d for d in data]
        result = [0] * len(data)
        for i in range(len(input)):
            data_type = self._dense_fstring[i]
            for j in range(len(user_format)):
                if user_format[j] == data_type:
                    user_format[j] = -1
                    result[j] = input[i]
                    break
        return tuple(result)

    # maps data in user_format to dense_format
    def map_user_dense(self, data: tuple) -> tuple:
        dense_format = [*self._dense_fstring]
        # input is in user_format
        input = [d for d in data]
        result = [0] * len(data)
        for i in range(len(input)):
            data_type = self._user_fstring[i]
            for j in range(len(dense_format)):
                if dense_format[j] == data_type:
                    dense_format[j] = -1
                    result[j] = input[i]
                    break
        return tuple(result)
