from pyfakefs import fake_filesystem_unittest

from rexdb import read_header, header_to_bytes, Header


class TestExample(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def test_basic(self):
        """Test that you can read and write headers without error"""
        # self.assertRaises(FileNotFoundError, read_header("test.db"))
        h1 = Header(0, 2, 3, b'ff')
        with open("test.db", "wb") as f:
            f.write(header_to_bytes(h1))
        h2 = read_header("test.db")
        print(h1, h2)
        self.assertEqual(h1, h2)
