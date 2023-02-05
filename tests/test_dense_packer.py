from pyfakefs import fake_filesystem_unittest

from rexdb import RexDB


class DensePacketTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def testFormatter(self):
        db = RexDB("test3.db", 'icfc', lines=1)
        self.assertEqual(db._dense_fstring, "ficc")

        db = RexDB("test3.db", "cifh?c", lines=1)
        self.assertEqual(db._dense_fstring, "fihcc?")
