from pyfakefs import fake_filesystem_unittest
from src.rexdb import RexDB
import random
from tests.faketime import FakeTime


class QueryTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.time = FakeTime()

    def test_query_basic(self):
        db = RexDB("ifc", ("index", "super precise number", "initial"), 26, 2)
        for i in range(50):
            db.log((i, random.random(), bytes(chr(i % 26 + 65), "utf-8")))
            self.time.sleep(0.5)

        data = db.get_data_at_field_threshold("index", 40, 1)

        self.assertEqual(len(data), 9)
        for i in range(9):
            self.assertEqual(data[i][1], 41 + i)
