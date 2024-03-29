import struct
import os
from src.dense_packer import DensePacker

VERSION = "0.0.1"
VERSION_BYTE = 0x00

UNSIGNED_CHAR = 'B'
UNSIGNED_LONG_LONG = 'Q'
FLOAT = 'f'


class FileManager:
    """
    Deals with reading and writing to the files.
    """

    @staticmethod
    def unpack_db_info(data: bytes, isBigEndian: bool = False):
        """
        unpack_db_info: bytes -> tuple
        32 bit processors have a 4 byte cycle, while 64 bit process have an 8 byte cycle (clearly)
        however, this will not impact getting info data as no field we use is greater than 4 bytes,
        so at worst, everything will be aligned in 4 byte increments

        we know that the format for data excluding the field names will be:
        int, int, bool, int, string, int, string
        and the field names will be int then string for however many field names there are

        2 ints will not leave any padding

        version byte has 3 bytes of padding beforehand
        """
        length = len(data)
        index = 0
        version_byte = data[index + 3]
        index += 4
        init_time = (data[index + 3] << 24) + (data[index + 2] << 16) + (data[index + 1] << 8) + (data[index + 0])
        index += 4
        bytes_per_file = (data[index + 3] << 24) + (data[index + 2] << 16) + (data[index + 1] << 8) + (data[index + 0])
        index += 4
        files_per_folder = (data[index + 3] << 24) + (data[index + 2] << 16) + (data[index + 1] << 8) + data[index + 0]
        index += 4
        fstring_size = (data[index + 3] << 24) + (data[index + 2] << 16) + (data[index + 1] << 8) + (data[index + 0])
        index += 4
        fstring = ""
        for i in range(fstring_size):
            fstring += chr(data[index + i])
        index += fstring_size
        dense_fstring = ""
        for i in range(fstring_size):
            dense_fstring += chr(data[index + i])
        index += fstring_size + ((4 - ((fstring_size * 2) % 4)) % 4)

        fields = ()
        while index < length:
            field_len = (data[index + 3] << 24) + (data[index + 2] << 16) + (data[index + 1] << 8) + (data[index + 0])
            index += 4
            string = ""
            for i in range(field_len):
                string += chr(data[index + i])
            fields += (string,)
            index += field_len
            index += ((4 - (index % 4)) % 4)
        return init_time, bytes_per_file, files_per_folder, version_byte, fstring_size, fstring, dense_fstring, fields

    def __init__(self, fstring: str, field_names: tuple, bytes_per_file: int,
                 files_per_folder: int, init_time: int, filepath: str, new_db: bool) -> None:
        self.bytes_per_file = bytes_per_file
        self.db_num = 0
        self.fstring = fstring
        self.dense_fstring = DensePacker.make_format(self.fstring)
        self.fstring_size = DensePacker.calc_fstring_size(self.fstring)
        self.lines_per_file = (self.bytes_per_file // self.fstring_size) + 1
        self.files_per_folder = files_per_folder
        self.init_time = init_time
        self.folder_start_time = init_time
        self.file_start_time = init_time
        self.folders = 0
        self.files = 0
        self.fields = field_names
        self.filepath = filepath
        self.db_map = f"{self.filepath}/db_map.map"
        self.db_info = f"{self.filepath}/db_info.info"
        if new_db:
            self.setup()
        else:
            self.read_setup()

    def setup(self):
        """
        runs all setup functions that are needed to create the file structure
        """
        try:
            self.create_db_map()
            self.create_new_folder()
            self.create_new_file()
            self.create_db_info()
            self.write_temp_data_file()
        except FileExistsError:
            raise RuntimeError("database already exists in folder")
        except Exception as e:
            print(f"could not setup databse: {e}")
            raise RuntimeError("no database was initialized")

    def read_setup(self):
        try:
            with open(f"{self.filepath}/temp", "rb") as fd:
                data = fd.read()
            folders, files, folder_start_time, file_start_time = struct.unpack("iiii", data)
            self.folders = folders
            self.files = files
            self.folder_start_time = folder_start_time
            self.file_start_time = file_start_time
            self.current_map = f"{self.filepath}/{self.folders}/.map"
        except Exception as e:
            print(f"could not get existing database: {e}")
            raise RuntimeError("no database was initialized")

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
        self.info_format = f"Biiii{len(self.fstring)}s{len(self.fstring)}s"
        fields = ()
        for field in field_names:
            fields = fields + (len(field), bytes(field, 'utf-8'))
            self.info_format += f"i{len(field)}s"

        data = struct.pack(self.info_format, VERSION_BYTE, self.init_time, self.bytes_per_file, self.files_per_folder,
                           len(self.fstring), bytes(self.fstring, 'utf-8'),
                           bytes(self.dense_fstring, 'utf-8'), *fields)
        try:
            with open(self.db_info, "wb") as fd:
                fd.write(data)
        except Exception as e:
            print(f"failed to create db info: {e}")

    def write_temp_data_file(self):
        packed_data = struct.pack("iiii", self.folders, self.files,
                                  int(self.folder_start_time),
                                  int(self.file_start_time))
        with open(f"{self.filepath}/temp", "wb") as fd:
            fd.write(packed_data)

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
        self.current_file = f'{self.filepath}/{self.folders}/{self.files:05}.db'

    def create_new_folder(self) -> bool:
        '''
        create_new_folder: time: float -> success: bool
        Iterates the folder count and updates the current file with that new
        folder value. Resets file count to 0.
        '''
        self.files = 0
        self.folders += 1
        try:
            os.mkdir(f'{self.filepath}/{self.folders}')
            self.current_file = f'{self.filepath}/{self.folders}/{self.files:05}.db'
            self.current_map = f'{self.filepath}/{self.folders}/.map'
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
        self.write_temp_data_file()

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
        except Exception as e:
            print(f"could not write to folder map: {e}")

    def start_db_entry(self, t):
        """
        stores start time of file for later use to write to the map file
        """
        self.folder_start_time = t
        self.write_temp_data_file()

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
                while (data := fd.read(12)) and len(data) == 12:
                    (start_time, end_time, num) = struct.unpack("iii", data)
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
            with open(f"{self.filepath}/{folder}/.map", "rb") as fd:
                while (data := fd.read(12)) and len(data) == 12:
                    (start_time, end_time, num) = struct.unpack("iii", data)
                    if (start_time <= t and t < end_time):
                        file = num
                        found_file = True
                        break
                if not found_file:
                    file = self.files
        except Exception as e:
            print(f"couldn't access folder map: {e}")

        return f"{self.filepath}/{folder}/{file:05}.db"

    def locations_from_range(self, start: float, end: float):
        folders = []
        files = []
        found_folder = False

        try:
            with open(self.db_map, "rb") as fd:
                while (data := fd.read(12)) and len(data) == 12:
                    (start_time, end_time, num) = struct.unpack("iii", data)
                    if (start <= start_time <= end) or (start <= end_time <= end):
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
                with open(f"{self.filepath}/{folder}/.map", "rb") as fd:
                    while (data := fd.read(12)) and len(data) == 12:
                        (start_time, end_time, num) = struct.unpack("iii", data)
                        if (start <= start_time <= end) or (start <= end_time <= end):
                            found_file = True
                            files.append(f"{self.filepath}/{folder}/{num:05}.db")
                    if not found_file:
                        files = [f"{self.filepath}/{folder}/{self.files:05}.db"]
                    if end_time <= end and folder == self.folders:
                        files.append(f"{self.filepath}/{folder}/{self.files:05}.db")
            except Exception as e:
                print(f"couldn't access folder map for {folder}: {e}")

        return files
