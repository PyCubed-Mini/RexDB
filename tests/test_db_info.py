from pyfakefs import fake_filesystem_unittest
import struct
from rexdb import FileManager


class DbInfoTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testBasic(self):
        fileManager = FileManager("ifcf", ("money", "volume", "letter", "area"))

        file = fileManager.db_info
        print(struct.calcsize(fileManager.info_format))
        self.assertEqual(fileManager.info_format, "Bi4s4si5si6si6si4s")

        with open(file, "rb") as fd:
            info = struct.unpack(fileManager.info_format, fd.read())
            self.assertEqual(info[0], 0x00)
            self.assertEqual(info[1], len(fileManager.fstring))
            self.assertEqual(info[2].decode('utf-8'), fileManager.fstring)
            self.assertEqual(info[3].decode('utf-8'), fileManager.dense_fstring)
            self.assertEqual(info[4], len(fileManager.fields[0]))
            self.assertEqual(info[5].decode('utf-8'), fileManager.fields[0])
            self.assertEqual(info[6], len(fileManager.fields[1]))
            self.assertEqual(info[7].decode('utf-8'), fileManager.fields[1])
            self.assertEqual(info[8], len(fileManager.fields[2]))
            self.assertEqual(info[9].decode('utf-8'), fileManager.fields[2])
            self.assertEqual(info[10], len(fileManager.fields[3]))
            self.assertEqual(info[11].decode('utf-8'), fileManager.fields[3])

    def testAdvanced(self):
        self.assertEqual("stuff", "stuff")