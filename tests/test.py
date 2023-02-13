from pyfakefs import fake_filesystem_unittest

from rexdb import RexDB


class TestExample(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def test_basic(self):
        """Test that you can read and write without data loss"""
        db = RexDB("test.db", 'icfc', lines=3)

        db.log((1024, b'a', 3.0, b'1'))
        self.assertEqual(db.nth(0), (1024, b'a', 3.0, b'1'))
        db.log((1023, b'b', 4.0, b'2'))
        self.assertEqual(db.nth(1), (1023, b'b', 4.0, b'2'))
        db.log((1022, b'c', 5.0, b'3'))

        # wraping works
        db.log((0, b'\x00', 0, b'\x00'))
        self.assertEqual(db.nth(0), (0, b'\x00', 0, b'\x00'))

        # col query works with wrapping
        self.assertEqual(db.col(3), [b'2', b'3', b'\x00'])

    def test_short_col_query(self):
        """Test column queries work even if the database is not full"""
        db = RexDB("test2.db", 'ii')

        db.log((1, 1))
        db.log((1, 2))
        db.log((1, 3))

        self.assertEqual(db.col(1), [1, 2, 3])

    def test_less_basic(self):
        db = RexDB("test3.db", 'dcichd?dci', lines=5)

        line_one = (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321)
        line_two = (9.1, b'p', 6534, b'p', 0, 1.9, True, 4.5, b'k', 12345)

        db.log(line_one)
        self.assertEqual(db.nth(0), line_one)
        db.log(line_two)
        self.assertEqual(db.nth(1), line_two)
