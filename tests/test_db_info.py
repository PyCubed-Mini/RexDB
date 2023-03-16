from pyfakefs import fake_filesystem_unittest
from rexdb import FileManager


class DbInfoTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testBasic(self):
        fileManager = FileManager("ifcf", ("money", "volume", "letter", "area"))

        file = fileManager.db_info
        with open(file, "rb") as fd:
            self.assertEqual(0x00, fd.read(1))

    def testAdvanced(self):
        self.assertEqual("stuff", "stuff")
