from pyfakefs import fake_filesystem_unittest

from rexdb import RexDB, DensePacker


class DensePacketTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testFormatter(self):
        db = RexDB("test3.db", 'icfc', lines=1)
        self.assertEqual(db._dense_fstring, "ficc")

        db = RexDB("test3.db", "cifh?c", lines=1)
        self.assertEqual(db._dense_fstring, "fihcc?")

    def testPacker(self):
        packer = DensePacker("icfc")
        assert (packer.dense_fstring == "ficc")
        assert (packer.user_dense_map == [2, 0, 3, 1])
        assert (packer.pack((32, b'f', 8.9, b'p')) == (8.9, 32, b'p', b'f'))
        assert (packer.unpack((8.9, 32, b'p', b'f')) == (32, b'f', 8.9, b'p'))

        packer = DensePacker("fcichf?fci")
        assert (packer.dense_fstring == "fffiihccc?")
        assert (packer.user_dense_map == [7, 5, 0, 9, 2, 4, 8, 3, 1, 6])
        assert (packer.pack((9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))
                == (1.1, 9.1, 9.2, 4321, 1234, 1, b'm', b'p', b'l', True))
        assert (packer.unpack((1.1, 9.1, 9.2, 4321, 1234, 1, b'm', b'p', b'l', True))
                == (9.2, b'l', 1234, b'p', 1, 9.1, True, 1.1, b'm', 4321))
