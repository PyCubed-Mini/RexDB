from pyfakefs import fake_filesystem_unittest
from rexdb import RexDB
import random
import time
import struct


class QueryTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()

    def test_file_path(self):
        db = RexDB('if', ("integer", "float"), 20, 2)
        times = []
        test_time_1 = 0
        test_time_2 = 0
        for i in range(20):
            times.append(time.localtime())
            db.log((i, random.random()))
            time.sleep(1)

        index = 1
        for t in times:
            filepath = db._file_manager.location_from_time(time.mktime(t))
            print(filepath)

        filepath1 = db._file_manager.location_from_time(time.mktime(times[1]))
        print(filepath1)
        assert (filepath1 == "db_0/1/1.001.db")

        filepath2 = db._file_manager.location_from_time(time.mktime(times[10]))
        print(filepath2)
        assert (filepath2 == "db_0/3/3.001.db")

        filepath3 = db._file_manager.location_from_time(time.mktime(times[13]))
        print(filepath3)
        assert (filepath3 == "db_0/4/4.001.db")

    def test_file_path_2(self):
        db = RexDB('idf?Q', ("index", "b/t 0 and 1", "b/t 0 and 10", "bool", "index * 10000"))
        times = []
        for i in range(200):
            times.append(time.localtime())
            boolval = True if random.random() < 0.5 else False
            db.log((i, random.random(), random.random() * 10, boolval, i * 10000))
            time.sleep(0.5)

        filepath = db._file_manager.location_from_time(time.mktime(times[5]))
        self.assertEqual(filepath, "db_0/1/1.001.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[50]))
        self.assertEqual(filepath, "db_0/1/1.002.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[100]))
        self.assertEqual(filepath, "db_0/1/1.003.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[150]))
        self.assertEqual(filepath, "db_0/1/1.005.db")

        filepath = db._file_manager.location_from_time(time.mktime(times[199]))
        self.assertEqual(filepath, "db_0/1/1.006.db")
