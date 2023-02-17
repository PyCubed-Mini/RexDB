from pyfakefs import fake_filesystem_unittest

from rexdb import RexDB, DensePacker


class DensePacketTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testFormatter(self):
        db = RexDB("test3.db", 'icfc', lines=1)
        self.assertEqual(db.packer.dense_fstring, "ficc")

        db = RexDB("test3.db", "cifh?c", lines=1)
        self.assertEqual(db.packer.dense_fstring, "fihcc?")

    def testEdgeCases(self):
        with self.assertRaises(ValueError):
            DensePacker("")
        with self.assertRaises(ValueError):
            DensePacker("goose")

    def testPacker(self):
        packer = DensePacker("i")
        assert (packer.dense_fstring == "i")
        assert (packer.user_dense_map == [0])
        assert (packer.unpack(packer.pack((32,))) == (32,))

        packer = DensePacker("icdc")
        assert (packer.dense_fstring == "dicc")
        assert (packer.user_dense_map == [2, 0, 3, 1])
        assert (packer.unpack(packer.pack((32, b'f', 8.9, b'p'))) == (32, b'f', 8.9, b'p'))

        packer = DensePacker("dcichd?dci")
        assert (packer.dense_fstring == "dddiihccc?")
        assert (packer.user_dense_map == [7, 5, 0, 9, 2, 4, 8, 3, 1, 6])
        assert (packer.unpack(packer.pack((9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321)))
                == (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))

        packer = DensePacker("dcichd?dci")
        assert (packer.dense_fstring == "dddiihccc?")
        assert (packer.user_dense_map == [7, 5, 0, 9, 2, 4, 8, 3, 1, 6])
        assert (packer.unpack(packer.pack((9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321)))
                == (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))
        assert (packer.unpack(packer.pack((9.1, b'p', 6534, b'p', 0, 1.9, True, 4.5, b'k', 12345)))
                == (9.1, b'p', 6534, b'p', 0, 1.9, True, 4.5, b'k', 12345))
