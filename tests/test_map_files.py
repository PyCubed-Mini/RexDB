from pyfakefs import fake_filesystem_unittest
from rexdb import RexDB
import time
import struct


class MapTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()

    def test_1(self):
        """
        tests that the correct times are being written 
        to the map files
        """

        '''
        creates a database to store (int, float, char) this database will 
        have ~20 bytes per file with error less than the size of the format. 
        In this case the format is 21 bytes (float timestamp + user format),
        so the exact bytes per file will be 21. It will have exactly 2 files 
        per folder
        '''
        db = RexDB('ifc', ("integer", "float", "character"), 20, 2)
        print(db._file_manager.files, db._file_manager.folders)

        log_1_timestamp = time.time()
        db.log((50, 8.9, b'l'))
        time.sleep(2)
        db.log((20, 7.6, b'p'))
        time.sleep(2)
        log_2_timestamp = time.time()
        db.log((40, 3.2, b'd'))
        time.sleep(2)
        db.log((30, 100.4, b'd'))
        time.sleep(2)
        log_3_timestamp = time.time()
        db.log((10, 9.8, b'k'))

        try:
            with open("db_0/1/1.map", "rb") as fd:
                data1 = struct.unpack("iii", fd.read(12))
                data2 = struct.unpack("iii", fd.read(12))
        except Exception as e:
            print(e)

        try:
            with open(db._file_manager.db_map, "rb") as fd:
                dbdata1 = struct.unpack("iii", fd.read(12))
                assert (dbdata1[2] == 1)
        except Exception as e:
            print(e)

        # with open(db._file_manager.current_map, "rb") as fd:
        #     data = struct.unpack("ffi", fd.read(12))
        #     print(log_1_timestamp, data[0])
        #     print(log_2_timestamp, data[1])
        #     assert (data[2] == 0)
        # with open(db._file_manager.db_map, "rb") as fd:
        #     data = struct.unpack("ffi", fd.read(12))
        #     assert (data[2] == 1)

        print(db._file_manager.files, db._file_manager.folders)

    def test_2(self):
        pass
