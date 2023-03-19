import struct
import os
import time
try:
    from ucollections import namedtuple
except ImportError:
    from collections import namedtuple

VERSION = "0.0.1"
VERSION_BYTE = 0x00

UNSIGNED_CHAR = 'B'
UNSIGNED_LONG_LONG = 'Q'
FLOAT = 'f'


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
        assert (version == VERSION_BYTE)
        (start_date, end_date, fstring_len) = struct.unpack(
            f'<{FLOAT}{FLOAT}{UNSIGNED_CHAR}',
            f.read(4+4+1))
        print(version, start_date, end_date, fstring_len)
        fstring = unpack_one(f"{fstring_len}s", f.read(fstring_len))
        return Header(version, start_date, end_date, fstring)


def header_to_bytes(header: Header) -> bytes:
    return struct.pack(f"<{UNSIGNED_CHAR}{FLOAT}{FLOAT}{UNSIGNED_CHAR}{len(header.fstring)}s",
                       header.version, header.start_date, header.end_date, len(header.fstring), header.fstring)


class DensePacker():
    def __init__(self, fstring: str) -> None:
        self.fstring_length = len(fstring)
        if self.fstring_length <= 0:
            raise ValueError("fstring must contain at least one data type")
        self.user_fstring = fstring
        self.dense_fstring = self.make_format(fstring)
        self.user_dense_map = self.create_pack_maps(self.user_fstring, self.dense_fstring)
        self.line_size = struct.calcsize(self.dense_fstring)

    def create_pack_maps(self, user_fstring, dense_fstring) -> str:
        '''
        create_pack_map : str * str -> str * str
        Creates a map from the user_fstring to the dense_fstring.
        This maps the last occurence of the data type in user_fstring
        to the first occurence in dense_fstring.
        For Example icfc = [0, 1, 2, 3] -> ficc = [2, 0, 3, 1]
            Notice how the "c" at index 3 comes before the "c" at
            index 1
        '''
        pack_map = {"c": [], "?": [], "h": [], "i": [], "f": [], "Q": [], "d": []}
        length = len(user_fstring)
        # map from the user string to the dense string
        user_dense_map = [0] * length
        for i in range(length):
            pack_map[user_fstring[i]].append(i)
        for i in range(length):
            user_dense_map[i] = pack_map[dense_fstring[i]].pop()

        return user_dense_map

    def pack(self, data: tuple) -> bytes:
        '''
        pack: tuple -> bytes
        takes input in the user_format, converts it to data in dense_format
        and packs that data into bytes.
        '''
        result = [0] * self.fstring_length
        for i in range(self.fstring_length):
            index = self.user_dense_map[i]
            result[i] = data[index]
        return struct.pack(self.dense_fstring, *result)

    def unpack(self, data: bytes) -> tuple:
        '''
        unpack: bytes -> tuple
        takes input in the form of bytes and will unpack the data into a tuple
        that is in the user_format
        '''
        result = [0] * self.fstring_length
        # print(len(data))
        unpacked_data = struct.unpack(self.dense_fstring, data)
        # print(unpacked_data)
        for i in range(self.fstring_length):
            index = self.user_dense_map[i]
            result[index] = unpacked_data[i]
        return tuple(result)

    @staticmethod
    def make_format(fstring: str) -> str:
        '''
        make_format: str -> str
        orders the incoming format string based on the byte size of
        the data type.
        icfc -> ficc
        '''
        char_list = {"c": 1, "?": 0.1, "h": 2, "i": 4, "f": 4.1, "d": 8, "Q": 8.1}
        denseFormat = list(fstring)
        try:
            denseFormat.sort(key=(lambda c: char_list[c]), reverse=True)
        except KeyError as e:
            raise ValueError(f"Invalid fstring contains unsupported data type: {e}")
        return ''.join(denseFormat)

    @staticmethod
    def calc_fstring_size(fstring) -> int:
        '''
        calc_fstring_size: None -> int
        calculates and returns the size in bytes for self.fstring
        '''
        char_list = {"c": 1, "?": 1, "h": 2, "i": 4, "f": 4, "Q": 8, "d": 8}
        sum = 0
        for char in fstring:
            sum += char_list[char]
        return sum


class FileManager:
    def __init__(self, fstring: str, field_names: tuple, bytes_per_file: int = 1000, files_per_folder: int = 100) -> None:
        self.bytes_per_file = bytes_per_file
        self.db_num = 0
        self.fstring = fstring
        self.dense_fstring = DensePacker.make_format(self.fstring)
        self.fstring_size = DensePacker.calc_fstring_size(self.fstring)
        self.lines_per_file = (self.bytes_per_file // self.fstring_size) + 1
        self.files_per_folder = files_per_folder
        self.folders = 0
        self.files = 0
        self.fields = field_names
        self.db_map = f"db_{self.db_num}/db_map.map"
        self.db_info = f"db_{self.db_num}/db_info.info"
        self.setup()

    def setup(self):
        try:
            os.mkdir(f"db_{self.db_num}")
            self.create_new_file()
            self.create_new_folder()
            self.create_db_info()
        except FileExistsError:
            self.db_num += 1
            self.setup()
        except Exception as e:
            print(f"could not setup databse: {e}")

    def create_db_info(self):
        '''
        create_db_info: unit -> void
        Creates a file that will contains the version, format character length, 
        user format, dense format, and field name length followed by field name
        for all field names.
        '''
        field_names = self.fields
        self.info_format = f"Bi{len(self.fstring)}s{len(self.fstring)}s"
        fields = ()
        for field in field_names:
            fields = fields + (len(field), bytes(field, 'utf-8'))
            self.info_format += f"i{len(field)}s"

        data = struct.pack(self.info_format, VERSION_BYTE, len(self.fstring), bytes(
            self.fstring, 'utf-8'), bytes(self.dense_fstring, 'utf-8'), *fields)
        try:
            with open(self.db_info, "wb") as fd:
                fd.write(data)
        except Exception as e:
            print(f"failed to create db info: {e}")

    def create_map_entry(self, start_date) -> Header:
        '''
        create_header: None -> Header
        Creates a header for a new file given the first line of data
        for that file
        '''
        return Header(VERSION_BYTE, start_date, 0, bytes(self.fstring, 'utf-8'))

    # def write_header(self, time) -> None:
    #     try:
    #         with open(self.current_file, "wb") as file:
    #             header = header_to_bytes(self.create_header(time))
    #             file.write(header)
    #     except Exception as e:
    #         print(f"failed to write header: {e}")

    def write_file(self, bytes_data: bytes) -> bool:
        '''
        write_file: bytes -> None
        Takes in data and writes it to the current file. Data should be formatted
        properly accoring to the fstring FileManager was given originally.
        '''
        try:
            with open(self.current_file, "ab") as file:
                file.write(bytes_data)
                return True
        except Exception as e:
            print(f"failed to write to file: {e}")
            return False

    def create_new_file(self) -> bool:
        '''
        create_new_file: time: float -> success: bool
        takes in a header. Iterates file count and creates a file with that new
        count as the name. Writes the header to the new file.
        '''
        self.files += 1
        self.current_file = f'db_{self.db_num}/{self.folders}/{self.folders}.{self.files:03}.db'
        # try:
        #     self.write_to_folder_map(time)
        #     return True
        # except Exception as e:
        #     print(f"Failed to create new file: {e}")
        #     return False

    def create_new_folder(self) -> bool:
        '''
        create_new_folder: time: float -> success: bool
        Iterates the folder count and updates the current file with that new
        folder value. Resets file count to 0.
        '''
        self.files = 0
        self.folders += 1
        try:
            os.mkdir(f'db_{self.db_num}/{self.folders}')
            self.current_file = f'db_{self.db_num}/{self.folders}/{self.folders}.{self.files:03}.db'
        except Exception as e:
            print(f"Failed to create new folder: {e}")
            return False

    def write_to_folder_map(self, time):
        """
        write_to_folder_map: time: float -> success: bool

        When new file is created within a folder, this function is called
        to add the timestamp as the start time for that file in the 
        folder's folder map.

        When a file is finished being written to, this function is called
        to add the timestamp as the endtime for that file in that 
        folder's folder map. 
        """
        pass

    def write_to_db_map(self, time):
        """
        write_to_db_map: time: float -> success: bool
        when new folder is created, adds current timestamp to the 
        database map as a start time.

        When a folder is finished being written to this function will
        add the timestamp as that folder's end time.
        """
        pass


class RexDB:
    def __init__(self, fstring, field_names: tuple, bytes_per_file=1000, files_per_folder=50, cursor=0, time_method=time.localtime):
        # add "f" as time will not be input by caller
        self._packer = DensePacker("f" + fstring)
        self._field_names = ("timestamp", *field_names)
        self._cursor = cursor
        self._timer_function = time_method
        self._prev_timestamp = time.mktime(self._timer_function())
        self._timestamp = 0
        self._file_manager = FileManager("f" + fstring, self._field_names, bytes_per_file, files_per_folder)

    def log(self, data):
        self._timestamp = time.mktime(self._timer_function())

        if (self._timestamp < self._prev_timestamp):
            raise Exception("logging backwards in time")

        # if no more files can be written in a folder, make new folder
        if self._cursor >= self._file_manager.lines_per_file:
            if self._file_manager.files >= self._file_manager.files_per_folder:
                self._file_manager.create_new_folder()
                self._file_manager.write_to_db_map(self._timestamp)
            self._file_manager.create_new_file()
            self._file_manager.write_to_folder_map(self._timestamp)
            self._cursor = 0

        # if no more data can be written to file, update header with an
        # end date and create a new file.

        data_bytes = self._packer.pack((self._timestamp, *data))
        self._file_manager.write_file(data_bytes)
        self._cursor += 1

    def nth(self, n):
        with open(self._file_manager.current_file, "rb") as fd:
            fd.seek(n*self._packer.line_size)
            line = fd.read(self._packer.line_size)
            return self._packer.unpack(line)[1:]

    def col(self, i):
        data = []
        with open(self._file_manager.current_file, "rb") as fd:
            fd.seek(0)
            for _ in range(self._packer.line_size):
                line = fd.read(self._packer.line_size)
                print(line)
                if len(line) != self._packer.line_size:
                    break
                line = self._packer.unpack(line)
                data.append(line[i])
        return data[self._cursor:] + data[:self._cursor]

    def get_data_at_time(time: time.struct_time):
        pass
