from pyfakefs import fake_filesystem_unittest

from rexdb import FileManager, RexDB


class FileManagerTest(fake_filesystem_unittest.TestCase):
    def setup(self):
        self.setUpPyfakefs()

    def testFileManager(self):
        filemanager = FileManager('icfc', 10, 2, 0)

        assert (filemanager.fstring_size == 10)
        assert (filemanager.lines_per_file == 1)

    def testFileWrite(self):
        db = RexDB("Qcfc", 20, 2)

        db.log((111111111, b'l', 9.8, b'p'))
        db.log((222222222, b'p', 8.7, b'p'))
        db.log((333333333, b'p', 8.7, b'g'))
        db.log((444444444, b'p', 8.7, b'p'))
        db.log((555555555, b'p', 8.9, b'h'))
        db.log((666666666, b'p', 8.9, b'h'))
