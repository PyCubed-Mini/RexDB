import struct
import os
import time
import string
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
    def __init__(self, fstring: str, field_names: tuple,
                 bytes_per_file: int = 1000, files_per_folder: int = 100) -> None:
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
        """
        runs all setup functions that are needed to create the file structure
        """
        try:
            os.mkdir(f"db_{self.db_num}")
            self.create_db_map()
            self.create_new_folder()
            self.create_new_file()
            self.create_db_info()
        except FileExistsError:
            self.db_num += 1
            self.setup()
        except Exception as e:
            print(f"could not setup databse: {e}")

    def create_db_map(self):
        try:
            fd = open(self.db_map, "xb")
            fd.close()
        except Exception as e:
            print(f"Failed to initialize DB map: {e}")

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

    def map_start_entry(self, start_date):
        '''
        create_header: start_date -> None
        Creates a header for a new file given the first line of data
        for that file
        '''
        with open(self.current_map, "ab") as file:
            file.write()
        return Header(VERSION_BYTE, start_date, 0, bytes(self.fstring, 'utf-8'))

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
            self.current_map = f'db_{self.db_num}/{self.folders}/{self.folders}.map'
            try:
                open(self.current_map, "wb")
            except Exception as e:
                print(f"Failed to create folder map: {e}")
        except Exception as e:
            print(f"Failed to create new folder: {e}")
            return False

    def start_folder_entry(self, t):
        """
        stores when a file has started to be used to eventually write to the map
        """
        self.file_start_time = t

    def write_to_folder_map(self, t):
        """
        write_to_folder_map: time: float -> success: bool

        When new file is created within a folder, this function is called
        to add the timestamp as the start time for that file in the
        folder's folder map.

        When a file is finished being written to, this function is called
        to add the timestamp as the endtime for that file in that
        folder's folder map.

        Written as Start Time, End Time, File Number
        """
        try:
            with open(self.current_map, "ab") as fd:
                data = struct.pack("iii", int(self.file_start_time), int(t), self.files)
                fd.write(data)
                # udata = struct.unpack("iii", data)
                # print(udata)
        except Exception as e:
            print(f"could not write to folder map: {e}")

    def start_db_entry(self, time):
        """
        stores start time of file for later use to write to the map file
        """
        self.folder_start_time = time

    def write_to_db_map(self, t):
        """
        write_to_db_map: time: float -> success: bool
        when new folder is created, adds current timestamp to the
        database map as a start time.

        When a folder is finished being written to this function will
        add the timestamp as that folder's end time.

        writes a struct of int (file number), float (start time),
        float (end time) to the map
        """
        # print(self.folder_start_time, t)
        data = struct.pack("iii", int(self.folder_start_time), int(t), self.folders)
        try:
            with open(self.db_map, "ab") as fd:
                fd.write(data)
        except Exception as e:
            print(f"could not write to database map: {e}")

    def location_from_time(self, t: float) -> str:
        """
        returns file path for location given a time in the database
        REQUIRES: first entry time < t
        """
        file = 1
        folder = 1
        found_folder = False
        found_file = False

        # finding folder from DB_map
        try:
            with open(self.db_map, "rb") as fd:
                contents = fd.read()
                if len(contents) != 0:
                    lines = int(len(contents) / 12)
                    for i in range(lines):
                        (start_time, end_time, num) = struct.unpack("iii", contents[i * 12: i * 12 + 12])
                        if (start_time <= t and t < end_time):
                            folder = num
                            found_folder = True
                            break
                    if not found_folder:
                        folder = self.folders
        except Exception as e:
            print(f"couldn't access db map: {e}")

        # finding file from folder_map
        try:
            with open(f"db_{self.db_num}/{folder}/{folder}.map", "rb") as fd:
                contents = fd.read()
                if len(contents) != 0:
                    lines = int(len(contents) / 12)
                    for i in range(lines):
                        (start_time, end_time, num) = struct.unpack("iii", contents[i * 12: i * 12 + 12])
                        if (start_time <= t and t < end_time):
                            file = num
                            found_file = True
                            break
                    if not found_file:
                        file = self.files
        except Exception as e:
            print(f"couldn't access folder map: {e}")

        return f"db_{self.db_num}/{folder}/{folder}.{file:03}.db"

    def locations_from_range(self, start: float, end: float):
        folders = []
        files = []
        found_folder = False

        try:
            with open(self.db_map, "rb") as fd:
                contents = fd.read()
                if len(contents) != 0:
                    lines = int(len(contents) / 12)
                    for i in range(lines):
                        (start_time, end_time, num) = struct.unpack("iii", contents[i * 12: i * 12 + 12])
                        if (start <= start_time and start_time <= end or start <= end_time and end_time <= end):
                            found_folder = True
                            folders.append(num)
                    if not found_folder:
                        folders = [self.folders]
                    if end_time <= end:
                        folders.append(self.folders)
        except Exception as e:
            print(f"couldn't access db map: {e}")

        found_file = True
        for folder in folders:
            try:
                with open(f"db_{self.db_num}/{folder}/{folder}.map", "rb") as fd:
                    contents = fd.read()
                    if len(contents) != 0:
                        lines = int(len(contents) / 12)
                        for i in range(lines):
                            (start_time, end_time, num) = struct.unpack("iii", contents[i * 12: i * 12 + 12])
                            if (start <= start_time and start_time <= end or start <= end_time and end_time <= end):
                                found_file = True
                                files.append(f"db_{self.db_num}/{folder}/{folder}.{num:03}.db")
                        if not found_file:
                            files = [f"db_{self.db_num}/{folder}/{folder}.{lines:03}.db"]
                        if end_time <= end and folder == self.folders:
                            files.append(f"db_{self.db_num}/{folder}/{folder}.{self.files:03}.db")
            except Exception as e:
                print(f"couldn't access folder map for {folder}: {e}")

        return files


class RexDB:
    def __init__(self, fstring, field_names: tuple, bytes_per_file=1000,
                 files_per_folder=50, cursor=0, time_method=time.gmtime):
        # add "f" as time will not be input by caller
        self._packer = DensePacker("i" + fstring)
        self._field_names = ("timestamp", *field_names)
        self._cursor = cursor
        self._timer_function = time_method
        self._init_time = self._timer_function()
        self._prev_timestamp = time.mktime(self._init_time)
        self._timestamp = 0
        self._file_manager = FileManager("i" + fstring, self._field_names, bytes_per_file, files_per_folder)
        self._file_manager.start_db_entry(self._prev_timestamp)
        self._file_manager.start_folder_entry(self._prev_timestamp)

    def log(self, data) -> None:
        """
        log: bytes -> None
        logs the data given into the correct folder and file. Also handles when new
        folders and files need to be created.
        """
        self._timestamp = (int)(time.mktime(self._timer_function()))

        if (self._timestamp < self._prev_timestamp):
            raise ValueError("logging backwards in time")

        if self._cursor >= self._file_manager.lines_per_file:
            self._file_manager.write_to_folder_map(self._timestamp)
            if self._file_manager.files >= self._file_manager.files_per_folder:
                # if no more files can be written in a folder, make new folder
                self._file_manager.write_to_db_map(self._timestamp)
                self._file_manager.create_new_folder()
                self._file_manager.start_db_entry(self._timestamp)
            # if no more lines can be written in a file, make new file
            self._file_manager.create_new_file()
            self._file_manager.start_folder_entry(self._timestamp)
            self._cursor = 0

        data_bytes = self._packer.pack((self._timestamp, *data))
        self._file_manager.write_file(data_bytes)
        self._cursor += 1
        self._prev_timestamp = self._timestamp

    @staticmethod
    def int_cmp(x, y):
        return (x > y) - (x < y)

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

    def get_data_at_time(self, t: time.struct_time):
        """
        struct_time -> tuple
        Given a time struct that was used to log in the database, this will return the first entry
        logged at that time.

        The precision of this function goes only to the nearest second because of the restrictions
        of struct_time
        """
        tfloat = time.mktime(t)
        filepath = self._file_manager.location_from_time(tfloat)
        if tfloat < time.mktime(self._init_time):
            raise ValueError("time is before database init time")
        try:
            with open(filepath, "rb") as fd:
                for _ in range(self._file_manager.lines_per_file):
                    raw_data = fd.read(self._packer.line_size)
                    data = self._packer.unpack(raw_data)
                    if (data[0] == tfloat):
                        return data
        except Exception as e:
            print(f"could not find data: {e}")
        return None

    def get_data_at_range(self, start_time: time.struct_time, end_time: time.struct_time):
        """
        (struct_time * struct_time) -> tuple
        Given a range of time, first argument of start time, second argument of end time
        this function will return all database entries falling within that range

        the struct_time datatype only holds precision of the nearest second, so this
        database only has precision to the nearest second as well.

        O(n) time complexity, but faster than querying based on other fields because the map 
        files will significantly decrease the coefficient.
        """
        start = time.mktime(start_time)
        end = time.mktime(end_time)
        filepaths = self._file_manager.locations_from_range(start, end)
        entries = []
        for filepath in filepaths:
            try:
                with open(filepath, "rb") as file:
                    for _ in range(self._file_manager.lines_per_file):
                        raw_data = file.read(self._packer.line_size)
                        data = self._packer.unpack(raw_data)
                        if (start <= data[0] and data[0] <= end):
                            entries.append(data)
            except Exception as e:
                print(f"could not search file: {e}")

        return entries

    def get_data_at_field_threshold(self, field: str, threshold, goal, cmp_fn=int_cmp, start_time=None, end_time=None):
        """
        string * 'a * ('a * 'a -> ORDER) * struct_time * struct_time -> list

        field is a string and is the name of the field you want to query on.

        Threshold is the value that you want to compare the data to and the
        goal is if you want the data to be less than, equal to, or greater
        than your threshold. Threshold should be the same type as the data stored
        in field, while goal should be -1, 0, or 1. -1 meaning less than, 0 meaning
        equal to, 1 meaning greater than.

        cmp_fn is an optional field. It is the comparison function you are using 
        to compare your threshold with the data stored in the database. The defulat 
        is an integer comparison function.

        the start_time and end_time fields are optional fields to limit your search
        to a specific time range.
        """

        entries = []
        filepaths = []
        # get files to search
        if start_time and end_time:
            # If start and end times were specified only search files that fall within that range.
            filepaths = self._file_manager.locations_from_range(start_time, end_time)
        elif start_time:
            # if only start time specified search from start time to now
            filepaths = self._file_manager.locations_from_range(start_time, self._timer_function())
        else:
            # if neither are specified search from the database's start to now
            filepaths = self._file_manager.locations_from_range(self._init_time, self._timer_function())

        # get the correct field index for comparison
        for f, i in enumerate(self._field_names):
            if string.lower(field) == string.lower(f):
                field_index = i

        # access every file
        for filepath in filepaths:
            try:
                with open(filepath, "rb") as file:
                    for _ in range(self._file_manager.lines_per_file):
                        raw_data = file.read(self._packer.line_size)
                        data = self._packer.unpack(raw_data)
                        # compare entry and threshold and see if they match the goal
                        if (cmp_fn(data[field_index], threshold) == goal):
                            entries.append(data)
            except Exception as e:
                print(f"could not search file: {e}")

        return entries
