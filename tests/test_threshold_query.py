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
        rand_test_answer_key = []
        for i in range(50):
            num = random.random()
            db.log((i, num, bytes(chr(i % 26 + 65), "utf-8")))
            if num > 0.75:
                rand_test_answer_key.append(i)
            self.time.sleep(0.5)

        data = db.get_data_at_field_threshold("index", 40, {1})

        self.assertEqual(len(data), 9)
        for i in range(9):
            self.assertEqual(data[i][1], 41 + i)

        data = db.get_data_at_field_threshold("super precise number", 0.75, {1})

        for i in range(len(rand_test_answer_key)):
            self.assertEqual(data[i][1], rand_test_answer_key[i])

    def test_verbose(self):
        db = RexDB("if", ("index", "rand_num"))
        random_low_answer_key = []
        random_high_answer_key = []
        for i in range(5000):
            num = random.random()
            db.log((i, num))
            if num < 0.1:
                random_low_answer_key.append((i, num))
            elif num > 0.9:
                random_high_answer_key.append((i, num))

            self.time.sleep(1)

        # test that the first entries can be retrieved successfully and less than operation
        data = db.get_data_at_field_threshold("index", 100, {-1})
        self.assertEqual(len(data), 100)
        for i in range(100):
            self.assertEqual(data[i][1], i)

        # test less than or equal to operation
        data = db.get_data_at_field_threshold("index", 100, {-1, 0})
        self.assertEqual(len(data), 101)
        for i in range(101):
            self.assertEqual(data[i][1], i)

        # test that the last entries can be retrieved successfully and greater than operation
        data = db.get_data_at_field_threshold("index", 4899, {1})
        self.assertEqual(len(data), 100)
        for i in range(100):
            self.assertEqual(data[i][1], i + 4900)

        # test greater than or equal to operation
        data = db.get_data_at_field_threshold("index", 4899, {0, 1})
        self.assertEqual(len(data), 101)
        for i in range(101):
            self.assertEqual(data[i][1], i + 4899)

        # test against the random_low_answer_key
        data = db.get_data_at_field_threshold("rand_num", 0.1, {-1})
        self.assertEqual(len(data), len(random_low_answer_key))
        for i in range(len(random_low_answer_key)):
            self.assertEqual(data[i][1], random_low_answer_key[i][0])

        # test against the random_high_answer_key
        data = db.get_data_at_field_threshold("rand_num", 0.9, {1})
        self.assertEqual(len(data), len(random_high_answer_key))
        for i in range(len(random_high_answer_key)):
            self.assertEqual(data[i][1], random_high_answer_key[i][0])
