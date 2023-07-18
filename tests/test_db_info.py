from pyfakefs import fake_filesystem_unittest
import struct
from src.file_manager import FileManager


class DbInfoTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testBasic(self):
        fileManager = FileManager("ifcf", ("money", "volume", "letter", "area"), 1024, 100)

        file = fileManager.db_info
        self.assertEqual(fileManager.info_format, "iiBi4s4si5si6si6si4s")

        with open(file, "rb") as fd:
            info = struct.unpack(fileManager.info_format, fd.read())
            self.assertEqual(info[0], 1024)
            self.assertEqual(info[1], 100)
            self.assertEqual(info[2], 0x00)
            self.assertEqual(info[3], len(fileManager.fstring))
            self.assertEqual(info[4].decode('utf-8'), fileManager.fstring)
            self.assertEqual(info[5].decode('utf-8'), fileManager.dense_fstring)
            self.assertEqual(info[6], len(fileManager.fields[0]))
            self.assertEqual(info[7].decode('utf-8'), fileManager.fields[0])
            self.assertEqual(info[8], len(fileManager.fields[1]))
            self.assertEqual(info[9].decode('utf-8'), fileManager.fields[1])
            self.assertEqual(info[10], len(fileManager.fields[2]))
            self.assertEqual(info[11].decode('utf-8'), fileManager.fields[2])
            self.assertEqual(info[12], len(fileManager.fields[3]))
            self.assertEqual(info[13].decode('utf-8'), fileManager.fields[3])

    def test_info_read(self):
        fields = ("money", "volume", "letter", "area")
        fileManager = FileManager("ifcfc", fields, 1024, 100)

        file = fileManager.db_info
        with open(file, "rb") as fd:
            data = fd.read()
            info = FileManager.unpack_db_info(data, True)

        self.assertEqual(info[0], 1024)
        self.assertEqual(info[1], 100)
        self.assertEqual(info[2], 0x00)
        self.assertEqual(info[3], 5)
        self.assertEqual(info[4], "ifcfc")
        self.assertEqual(info[5], "fficc")
        self.assertEqual(info[6][0], "money")
        self.assertEqual(info[6][1], "volume")
        self.assertEqual(info[6][2], "letter")
        self.assertEqual(info[6][3], "area")

    def test_info_read_large(self):
        fields = ("char", "int", "bool", "ulonglong", "double", "float", "char2", "short", "int2", "float2", "short2")
        fileManager = FileManager("ci?Qdfchifh", fields, 5096, 50)

        file = fileManager.db_info
        with open(file, "rb") as fd:
            data = fd.read()
            info = FileManager.unpack_db_info(data, True)

        self.assertEqual(info[0], 5096)
        self.assertEqual(info[1], 50)
        self.assertEqual(info[2], 0x00)
        self.assertEqual(info[3], 11)
        self.assertEqual(info[4], "ci?Qdfchifh")
        self.assertEqual(info[5], "Qdffiihhcc?")
        self.assertEqual(info[6][0], "char")
        self.assertEqual(info[6][1], "int")
        self.assertEqual(info[6][2], "bool")
        self.assertEqual(info[6][3], "ulonglong")
        self.assertEqual(info[6][4], "double")
        self.assertEqual(info[6][5], "float")
        self.assertEqual(info[6][6], "char2")
        self.assertEqual(info[6][7], "short")
        self.assertEqual(info[6][8], "int2")
        self.assertEqual(info[6][9], "float2")
        self.assertEqual(info[6][10], "short2")
