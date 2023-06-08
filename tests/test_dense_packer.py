from pyfakefs import fake_filesystem_unittest

from src.rexdb import RexDB
from src.dense_packer import DensePacker


class DensePacketTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testFormatter(self):
        # filemanager automatically adds a float to store Timestamp
        db = RexDB('icfc', ("money", "volume", "letter", "area"), bytes_per_file=10, files_per_folder=2)
        self.assertEqual(db._packer.dense_fstring, "fiicc")

        db = RexDB("cifh?c", ("money", "volume", "letter", "area"), bytes_per_file=10, files_per_folder=2)
        self.assertEqual(db._packer.dense_fstring, "fiihcc?")

    def testEdgeCases(self):
        with self.assertRaises(ValueError):
            DensePacker("")
        with self.assertRaises(ValueError):
            DensePacker("goose")

    def testPacker(self):
        packer = DensePacker("i")
        self.assertEqual(packer.dense_fstring, "i")
        self.assertEqual(packer.user_dense_map, [0])
        self.assertEqual(packer.unpack(packer.pack((32,))), (32,))

        packer = DensePacker("icdc")
        self.assertEqual(packer.dense_fstring, "dicc")
        self.assertEqual(packer.user_dense_map, [2, 0, 3, 1])
        self.assertEqual(packer.unpack(packer.pack((32, b'f', 8.9, b'p'))), (32, b'f', 8.9, b'p'))

        packer = DensePacker("dcichd?dci")
        self.assertEqual(packer.dense_fstring, "dddiihccc?")
        self.assertEqual(packer.user_dense_map, [7, 5, 0, 9, 2, 4, 8, 3, 1, 6])
        self.assertEqual(packer.unpack(packer.pack((9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))),
                         (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))

        packer = DensePacker("dcichd?dci")
        self.assertEqual(packer.dense_fstring, "dddiihccc?")
        self.assertEqual(packer.user_dense_map, [7, 5, 0, 9, 2, 4, 8, 3, 1, 6])
        self.assertEqual(packer.unpack(packer.pack((9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))),
                         (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))
        self.assertEqual(packer.unpack(packer.pack((9.1, b'p', 6534, b'p', 0, 1.9, True, 4.5, b'k', 12345))),
                         (9.1, b'p', 6534, b'p', 0, 1.9, True, 4.5, b'k', 12345))
