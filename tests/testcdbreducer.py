import os
import unittest
from tempfile import mkstemp
import cdb

from dumbo.lib.cdbreducer import CDBReducer, CDBFactory


class CDBTestCase(unittest.TestCase):

    def test_default(self):
        proc = CDBFactory()
        self.assertEqual(proc('k1', ['v1']), None)
        self.assertEqual(proc('k2', ['v2', 'v3']), None)
        chunks = proc.close()
        fn = mkstemp()[1]
        fo = open(fn, 'wb')
        for chk in chunks:
            self.assertTrue(len(chk) <= proc.chunksize)
            fo.write(chk)
        fo.close()

        db = cdb.init(fn)
        self.assertEqual([(k, db[k]) for k in db.keys()],
                [('k1', 'v1'), ('k2', 'v2')])
        os.remove(fn)

    def test_reducer(self):
        red = CDBReducer()
        output = red(zip('abcde', '12345'))

        fn = mkstemp()[1]
        fo = open(fn, 'wb')
        fo.writelines(v for k, v in output)
        fo.close()

        db = cdb.init(fn)
        self.assertEqual([(k, db[k]) for k in db.keys()],
                [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4'), ('e', '5')])
        os.remove(fn)

