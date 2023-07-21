from pyfakefs import fake_filesystem_unittest
from src.rexdb import RexDB
import random
from tests.faketime import FakeTime


class QueryTests(fake_filesystem_unittest.TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.time = FakeTime()

    def less_than(self, x):
        return lambda y: y < x

    def greater_than(self, x):
        return lambda y: y > x

    def test_query_basic(self):
        db = RexDB("ifc", ("index", "super precise number", "initial"), 26, 2)
        rand_test_answer_key = []
        f = self.greater_than(0.75)
        g = self.greater_than(40)
        for i in range(50):
            num = random.random()
            db.log((i, num, bytes(chr(i % 26 + 65), "utf-8")))
            if f(num):
                rand_test_answer_key.append(i)
            self.time.sleep(0.5)

        data = db.get_field_filtered("index", g)

        self.assertEqual(len(data), 9)
        for i in range(9):
            self.assertEqual(data[i][1], 41 + i)

        data = db.get_field_filtered("super precise number", f)

        for i in range(len(rand_test_answer_key)):
            self.assertEqual(data[i][1], rand_test_answer_key[i])

    def test_verbose(self):
        db = RexDB("if", ("index", "rand_num"))
        random_low_answer_key = []
        random_high_answer_key = []
        l_10p = self.less_than(0.1)
        g_90p = self.greater_than(0.9)
        l_100 = self.less_than(100)
        g_4899 = self.greater_than(4899)

        for i in range(5000):
            num = random.random()
            db.log((i, num))
            if num < 0.1:
                random_low_answer_key.append((i, num))
            elif num > 0.9:
                random_high_answer_key.append((i, num))

            self.time.sleep(1)

        # test that the first entries can be retrieved successfully and less than operation
        data = db.get_field_filtered("index", l_100)
        self.assertEqual(len(data), 100)
        for i in range(100):
            self.assertEqual(data[i][1], i)

        # test that the last entries can be retrieved successfully and greater than operation
        data = db.get_field_filtered("index", g_4899)
        self.assertEqual(len(data), 100)
        for i in range(100):
            self.assertEqual(data[i][1], i + 4900)

        # test against the random_low_answer_key
        data = db.get_field_filtered("rand_num", l_10p)
        self.assertEqual(len(data), len(random_low_answer_key))
        for i in range(len(random_low_answer_key)):
            self.assertEqual(data[i][1], random_low_answer_key[i][0])

        # test against the random_high_answer_key
        data = db.get_field_filtered("rand_num", g_90p)
        self.assertEqual(len(data), len(random_high_answer_key))
        for i in range(len(random_high_answer_key)):
            self.assertEqual(data[i][1], random_high_answer_key[i][0])
