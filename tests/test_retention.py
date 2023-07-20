from pyfakefs import fake_filesystem_unittest
import os
import random
from tests.faketime import FakeTime

from src.rexdb import RexDB


class FileManagerTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.time = FakeTime()

    def testBasic(self):
        os.mkdir("sd")
        db = RexDB('if', ("integer", "float"), 20, 2, time_method=self.time.gmtime, filepath="sd")
        times = []
        """
        This loop creates 10 total files, 2 in each folder.
        This means when a new DB is initiated where this one left off, it should create another
        folder (6) and the the number of files should go back down to 1.
        """
        for i in range(20):
            times.append(self.time.gmtime())
            db.log((i, random.random()))
            self.time.sleep(1)

        folders = db._file_manager.folders

        db = RexDB(filepath="sd", new_db=False)
        self.assertEqual(db._file_manager.folders, folders + 1)
        self.assertEqual(db._file_manager.files, 1)

    def test_querying(self):
        os.mkdir("sd")
        db = RexDB('i', ('int'), time_method=self.time.gmtime, filepath="sd")

        logs = 10000
        tests = 100
        time_tests = [0] * int(logs / tests)

        for i in range(10000):
            if (i % 100 == 0):
                time_tests[int(i/100)] = self.time.gmtime()
            db.log((i,))
            self.time.sleep(2)

        folders = db._file_manager.folders
        files = db._file_manager.files

        db = RexDB(filepath="sd", new_db=False)

        self.assertEqual(db._file_manager.folders, folders)
        self.assertEqual(db._file_manager.files, files + 1)

        for i in range(len(time_tests)):
            data = db.get_data_at_time(time_tests[i])
            self.assertEqual(data[1], i * 100)

    def test_querying_more(self):
        os.mkdir("sd")
        fields = ("char", "int", "bool", "float")
        db = RexDB('ci?f', fields, time_method=self.time.gmtime, filepath="sd")

        test_list = [0] * 100

        for i in range(1000):
            rand = random.random()
            char = bytes(chr(random.randint(33, 125)), 'utf-8')
            bool = rand < 0.5
            if i % 10 == 0:
                test_list[int(i/10)] = (self.time.gmtime(), char, i, bool, rand)
            db.log((char, i, bool, rand))
            self.time.sleep(2)

        db = RexDB(filepath="sd", new_db=False)

        for i in range(100):
            data = db.get_data_at_time(test_list[i][0])

            self.assertEqual(test_list[i][1], data[1])
            self.assertEqual(test_list[i][2], data[2])
            self.assertEqual(test_list[i][3], data[3])
            self.assertAlmostEqual(test_list[i][4], data[4])

    def test_logging_then_querying(self):
        os.mkdir("sd")
        fields = ("char", "int", "bool", "float")
        db = RexDB('ci?f', fields, bytes_per_file=50, files_per_folder=5, time_method=self.time.gmtime, filepath="sd")

        test_list = [0] * 200

        for i in range(1000):
            rand = random.random()
            char = bytes(chr(random.randint(33, 125)), 'utf-8')
            bool = rand < 0.5
            if i % 10 == 0:
                test_list[i//10] = (self.time.gmtime(), char, i, bool, rand)
            db.log((char, i, bool, rand))
            self.time.sleep(1)

        db = RexDB(filepath="sd", time_method=self.time.gmtime, new_db=False)

        for i in range(1000):
            rand = random.random()
            char = bytes(chr(random.randint(33, 125)), 'utf-8')
            bool = rand < 0.5
            if i % 10 == 0:
                test_list[(i//10) + 100] = (self.time.gmtime(), char, i + 1000, bool, rand)
            db.log((char, i + 1000, bool, rand))
            self.time.sleep(1)

        for entry in test_list:
            data = db.get_data_at_time(entry[0])
            self.assertEqual(entry[1], data[1])
            self.assertEqual(entry[2], data[2])
            self.assertEqual(entry[3], data[3])
            self.assertAlmostEqual(entry[4], data[4])
