import struct
import time
try:
    from ucollections import namedtuple
except ImportError:
    from collections import namedtuple
from src.dense_packer import DensePacker
from src.file_manager import FileManager

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


class RexDB:
    def __init__(self, fstring, field_names: tuple, bytes_per_file=1000,
                 files_per_folder=50, cursor=0, time_method=time.gmtime):
        # add "f" as time will not be input by caller
        self._packer = DensePacker("i" + fstring)
        self._field_names = ("timestamp", *field_names)
        self._cursor = cursor
        self._timer_function = time_method
        self._init_time = time.mktime(self._timer_function())
        self._prev_timestamp = self._init_time
        self._timestamp = 0
        self._file_manager = FileManager(
            "i" + fstring, self._field_names, bytes_per_file, files_per_folder)
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
        if tfloat < self._init_time:
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
