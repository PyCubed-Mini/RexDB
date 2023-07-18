from pyfakefs import fake_filesystem_unittest
from src.rexdb import RexDB
import random
import time
import os
from tests.faketime import FakeTime


class QueryTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.time = FakeTime()

    def test_file_path(self):
        os.mkdir("sd")
        db = RexDB('if', ("integer", "float"), 20, 2, time_method=self.time.gmtime, filepath="sd/")
        times = []
        for i in range(20):
            times.append(self.time.gmtime())
            db.log((i, random.random()))
            self.time.sleep(1)

        filepath0 = db._file_manager.location_from_time(time.mktime(times[0]))
        self.assertEqual(filepath0, "sd/db_0/1/00001.db")

        filepath1 = db._file_manager.location_from_time(time.mktime(times[1]))
        self.assertEqual(filepath1, "sd/db_0/1/00001.db")

        filepath2 = db._file_manager.location_from_time(time.mktime(times[2]))
        self.assertEqual(filepath2, "sd/db_0/1/00002.db")

        filepath3 = db._file_manager.location_from_time(time.mktime(times[3]))
        self.assertEqual(filepath3, "sd/db_0/1/00002.db")

        filepath4 = db._file_manager.location_from_time(time.mktime(times[10]))
        self.assertEqual(filepath4, "sd/db_0/3/00002.db")

        filepath5 = db._file_manager.location_from_time(time.mktime(times[13]))
        self.assertEqual(filepath5, "sd/db_0/4/00001.db")

    def test_file_path_2(self):
        db = RexDB('idf?Q', ("index", "b/t 0 and 1", "b/t 0 and 10", "bool", "index * 10000"),
                   time_method=self.time.gmtime)
        times = []
        # log some stuff
        for i in range(200):
            times.append(self.time.gmtime())
            boolval = True if random.random() < 0.5 else False
            db.log((i, random.random(), random.random() * 10, boolval, i * 10000))
            self.time.sleep(0.5)

        # test returned filepaths
        filepath = db._file_manager.location_from_time(time.mktime(times[5]))
        self.assertEqual(filepath, "db_0/1/00001.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[50]))
        self.assertEqual(filepath, "db_0/1/00002.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[100]))
        self.assertEqual(filepath, "db_0/1/00003.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[150]))
        self.assertEqual(filepath, "db_0/1/00005.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[199]))
        self.assertEqual(filepath, "db_0/1/00006.db")

    def test_data_1(self):
        db = RexDB('if', ("integer", "float"), 20, 2, time_method=self.time.gmtime)
        times = []
        data_entries = []
        for i in range(20):
            times.append(self.time.gmtime())
            data = (i, random.random())
            data_entries.append(data)
            db.log(data)
            self.time.sleep(3)

        # test returned data
        for i in range(0, 20):
            data = db.get_data_at_time(times[i])
            self.assertEqual(data[1], data_entries[i][0])

    def test_file_range(self):
        db = RexDB('idf?Q', ("index", "b/t 0 and 1", "b/t 0 and 10", "bool",
                   "index * 10000"), 20, 2, time_method=self.time.gmtime)
        times = []
        # log some stuff
        for i in range(20):
            times.append(self.time.gmtime())
            boolval = True if random.random() < 0.5 else False
            db.log((i, random.random(), random.random() * 10, boolval, i * 10000))
            self.time.sleep(2)

        # test returned filepaths
        filepaths = db._file_manager.locations_from_range(time.mktime(times[5]), time.mktime(times[10]))
        for i in range(5, 10):
            filepath = db._file_manager.location_from_time(time.mktime(times[i]))
            self.assertTrue(filepath in filepaths)

        data = db.get_data_at_range(times[5], times[10])

        # checks that correct files are obtained
        self.assertEqual(data[0][1], 5)
        self.assertEqual(data[1][1], 6)
        self.assertEqual(data[2][1], 7)
        self.assertEqual(data[3][1], 8)
        self.assertEqual(data[4][1], 9)
        self.assertEqual(data[5][1], 10)

        data = db.get_data_at_range(times[15], times[19])
        self.assertEqual(data[0][1], 15)
        self.assertEqual(data[1][1], 16)
        self.assertEqual(data[2][1], 17)
        self.assertEqual(data[3][1], 18)
        self.assertEqual(data[4][1], 19)
