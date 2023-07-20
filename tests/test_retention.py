from pyfakefs import fake_filesystem_unittest
import os
import random
from tests.faketime import FakeTime
import time

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
