from pyfakefs import fake_filesystem_unittest
from src.rexdb import RexDB
import struct
from tests.faketime import FakeTime


class MapTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.time = FakeTime()

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
        db = RexDB('ifc', ("integer", "float", "character"), 20, 2, time_method=self.time.gmtime)

        # create database of 1 folder and 2 files
        # will enter 2 entries per file
        log_1_timestamp = int(self.time.time())
        db.log((50, 8.9, b'l'))
        self.time.sleep(2)
        db.log((20, 7.6, b'p'))
        self.time.sleep(2)
        log_3_timestamp = int(self.time.time())
        db.log((40, 3.2, b'd'))
        self.time.sleep(2)
        db.log((30, 100.4, b'd'))
        self.time.sleep(2)
        log_5_timestamp = int(self.time.time())
        db.log((10, 9.8, b'k'))

        # correctness of folder 1 map
        try:
            with open("/1/.map", "rb") as fd:
                data1 = struct.unpack("iii", fd.read(12))
                self.assertEqual(data1[0], log_1_timestamp)
                self.assertEqual(data1[1], log_3_timestamp)
                self.assertEqual(data1[2], 1)
                data2 = struct.unpack("iii", fd.read(12))
                self.assertEqual(data2[0], log_3_timestamp)
                self.assertEqual(data2[1], log_5_timestamp)
                self.assertEqual(data2[2], 2)
        except Exception as e:
            raise RuntimeError(f"failed folder 1 tests: {e}")

        # correctness of database map
        try:
            with open(db._file_manager.db_map, "rb") as fd:
                dbdata1 = struct.unpack("iii", fd.read(12))
                self.assertEqual(dbdata1[0], log_1_timestamp)
                self.assertEqual(dbdata1[1], log_5_timestamp)
                self.assertEqual(dbdata1[2], 1)
        except Exception as e:
            raise RuntimeError(f"could not open databse map file: {e}")
