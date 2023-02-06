import struct


class DensePacker():
    def __init__(self, fstring: str) -> None:
        self.fstring_length = len(fstring)
        self.user_fstring = fstring
        self.dense_fstring = self.make_format(fstring)
        self.user_dense_map = self.create_pack_maps(self.user_fstring, self.dense_fstring)

    '''
    make_format: str -> str
    
    orders the incoming format string based on the byte size of
    the data type. 
    icfc -> ficc
    '''

    def make_format(self, fstring: str) -> str:
        char_list = {"c": 1, "?": 0.1, "h": 2, "i": 4, "f": 4.1}
        denseFormat = list(fstring)
        denseFormat.sort(key=(lambda c: char_list[c]), reverse=True)
        return ''.join(denseFormat)

    '''
    create_pack_map : str * str -> str * str

    Creates a map from the user_fstring to the dense_fstring.
    This maps the last occurence of the data type in user_fstring
    to the first occurence in dense_fstring.
    For Example icfc = [0, 1, 2, 3] -> ficc = [2, 0, 3, 1]
        Notice how the "c" at index 3 comes before the "c" at 
        index 1 
    '''

    def create_pack_maps(self, user_fstring, dense_fstring) -> str:
        pack_map = {"c": [], "?": [], "h": [], "i": [], "f": []}
        length = len(user_fstring)
        # map from the user string to the dense string
        user_dense_map = [0] * length
        for i in range(length):
            pack_map[user_fstring[i]].append(i)
        for i in range(length):
            user_dense_map[i] = pack_map[dense_fstring[i]].pop()

        return user_dense_map

    '''
    pack: tuple -> tuple

    takes input in the user_format and will output that data in the 
    dense_format
    '''

    def pack(self, data: tuple) -> tuple:
        result = [0] * self.fstring_length
        for i in range(self.fstring_length):
            index = self.user_dense_map[i]
            result[i] = data[index]
        return tuple(result)

    def unpack(self, data: tuple) -> tuple:
        result = [0] * self.fstring_length
        for i in range(self.fstring_length):
            index = self.user_dense_map[i]
            result[index] = data[i]
        return tuple(result)


class RexDB:

    def __init__(self, file, fstring, lines=100, cursor=0):
        self._fd = open(file, "wb")
        self._user_fstring = fstring
        packer = DensePacker(fstring)
        self._dense_fstring = packer.dense_fstring
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
