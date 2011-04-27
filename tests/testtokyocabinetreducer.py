import unittest
import os
from tempfile import mkstemp
from tokyo.cabinet import HDB, HDBOREADER, BDB, BDBOREADER, BDBOWRITER, BDBOCREAT

from dumbo.lib.tokyocabinetreducer import TokyoCabinetReducer, TokyoCabinetFactory


class TokyoCabinetTestCase(unittest.TestCase):

    def test_default(self):
        proc = TokyoCabinetFactory()
        self.assertEqual(proc('k1', ['v1']), None)
        self.assertEqual(proc('k2', ['v2', 'v3']), None)
        chunks = proc.close()
        fn = mkstemp()[1]
        fo = open(fn, 'wb')
        for chk in chunks:
            self.assertTrue(len(chk) <= proc.chunksize)
            fo.write(chk)
        fo.close()

        db = HDB()
        db.open(fn, HDBOREADER)
        self.assertEqual(list(db.iteritems()), [('k1', 'v1'), ('k2', 'v3')])
        db.close()
        os.remove(fn)

    def test_extended(self):
        class BDBFactory(TokyoCabinetFactory):
            dbcls = BDB
            flags = BDBOWRITER | BDBOCREAT
            methodname = 'addint'
            chunksize = 10 # very small

        proc = BDBFactory()
        self.assertEqual(proc('k1', [2]), None)
        self.assertEqual(proc('k2', [3, 6]), None)
        chunks = proc.close()
        fn = mkstemp()[1]
        fo = open(fn, 'wb')
        for chk in chunks:
            self.assertTrue(len(chk) <= 10)
            fo.write(chk)
        fo.close()

        db = BDB()
        db.open(fn, BDBOWRITER)
        self.assertEqual(len(db), 2)
        self.assertEqual(db.addint('k1', 0), 2)
        self.assertEqual(db.addint('k2', 0), 9)
        db.close()
        os.remove(fn)

    def test_reducer(self):
        red = TokyoCabinetReducer()
        output = red(zip('abcde', '12345'))

        fn = mkstemp()[1]
        fo = open(fn, 'wb')
        fo.writelines(v for k, v in output)
        fo.close()
        db = HDB()
        db.open(fn, HDBOREADER)
        self.assertEqual(list(db.iteritems()),
                [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4'), ('e', '5')])
        db.close()
        os.remove(fn)
