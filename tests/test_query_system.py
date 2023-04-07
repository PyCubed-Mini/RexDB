from pyfakefs import fake_filesystem_unittest
from rexdb import RexDB
import random
import time
import struct


class QueryTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()

    def test_file_path(self):
        db = RexDB('if', ("integer", "float"), 20, 2,)
        test_time = 0
        for i in range(20):
            db.log((i, random.random()))
            if i == 1:
                test_time = time.localtime()
            time.sleep(1)

        filepath1 = db._file_manager.location_from_time(time.mktime(test_time))
        print(filepath1)
        assert (filepath1 == "db_0/1/1.000.db")
