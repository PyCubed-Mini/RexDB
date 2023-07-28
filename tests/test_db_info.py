from pyfakefs import fake_filesystem_unittest
import struct
from src.file_manager import FileManager
import os


class DbInfoTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testBasic(self):
        os.mkdir("sd")
        fileManager = FileManager("ifcf", ("money", "volume", "letter", "area"), 1024, 100, 90, "sd/", True)

        file = fileManager.db_info
        self.assertEqual(fileManager.info_format, "Biiii4s4si5si6si6si4s")

        with open(file, "rb") as fd:
            info = struct.unpack(fileManager.info_format, fd.read())
            self.assertEqual(info[0], 0x00)
            self.assertEqual(info[1], 90)
            self.assertEqual(info[2], 1024)
            self.assertEqual(info[3], 100)
            self.assertEqual(info[4], len(fileManager.fstring))
            self.assertEqual(info[5].decode('utf-8'), fileManager.fstring)
            self.assertEqual(info[6].decode('utf-8'), fileManager.dense_fstring)
            self.assertEqual(info[7], len(fileManager.fields[0]))
            self.assertEqual(info[8].decode('utf-8'), fileManager.fields[0])
            self.assertEqual(info[9], len(fileManager.fields[1]))
            self.assertEqual(info[10].decode('utf-8'), fileManager.fields[1])
            self.assertEqual(info[11], len(fileManager.fields[2]))
            self.assertEqual(info[12].decode('utf-8'), fileManager.fields[2])
            self.assertEqual(info[13], len(fileManager.fields[3]))
            self.assertEqual(info[14].decode('utf-8'), fileManager.fields[3])

    def test_info_read(self):
        fields = ("money", "volume", "letter", "area")
        fileManager = FileManager("ifcfc", fields, 1024, 100, 900, "", True)

        file = fileManager.db_info
        with open(file, "rb") as fd:
            data = fd.read()
            info = FileManager.unpack_db_info(data, True)

        self.assertEqual(info[0], 900)
        self.assertEqual(info[1], 1024)
        self.assertEqual(info[2], 100)
        self.assertEqual(info[3], 0x00)
        self.assertEqual(info[4], 5)
        self.assertEqual(info[5], "ifcfc")
        self.assertEqual(info[6], "fficc")
        self.assertEqual(info[7][0], "money")
        self.assertEqual(info[7][1], "volume")
        self.assertEqual(info[7][2], "letter")
        self.assertEqual(info[7][3], "area")

    def test_info_read_small(self):
        fields = ("int",)
        os.mkdir("sd")
        fileManager = FileManager("fi", fields, 5096, 50, 123, "sd", True)
        file = fileManager.db_info
        with open(file, "rb") as fd:
            data = fd.read()
        info = FileManager.unpack_db_info(data, True)
        self.assertEqual(info[0], 123)
        self.assertEqual(info[1], 5096)
        self.assertEqual(info[2], 50)
        self.assertEqual(info[3], 0x00)
        self.assertEqual(info[4], 2)
        self.assertEqual(info[5], "fi")
        self.assertEqual(info[6], "fi")
        self.assertEqual(info[7][0], "int")

    def test_info_read_large(self):
        fields = ("char", "int", "bool", "ulonglong", "double", "float", "char2", "short", "int2", "float2", "short2")
        fileManager = FileManager("ci?Qdfchifh", fields, 5096, 50, 19191900, "", True)

        file = fileManager.db_info
        with open(file, "rb") as fd:
            data = fd.read()
            info = FileManager.unpack_db_info(data, True)

        self.assertEqual(info[0], 19191900)
        self.assertEqual(info[1], 5096)
        self.assertEqual(info[2], 50)
        self.assertEqual(info[3], 0x00)
        self.assertEqual(info[4], 11)
        self.assertEqual(info[5], "ci?Qdfchifh")
        self.assertEqual(info[6], "Qdffiihhcc?")
        self.assertEqual(info[7][0], "char")
        self.assertEqual(info[7][1], "int")
        self.assertEqual(info[7][2], "bool")
        self.assertEqual(info[7][3], "ulonglong")
        self.assertEqual(info[7][4], "double")
        self.assertEqual(info[7][5], "float")
        self.assertEqual(info[7][6], "char2")
        self.assertEqual(info[7][7], "short")
        self.assertEqual(info[7][8], "int2")
        self.assertEqual(info[7][9], "float2")
        self.assertEqual(info[7][10], "short2")
