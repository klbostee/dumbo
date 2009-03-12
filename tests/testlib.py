import unittest
from dumbo import lib, core

class TestLib(unittest.TestCase):

    def teststats(self):
        input = [('testkey',i) for i in xrange(10)]
        input = core.itermapred(input, lib.identitymapper, lib.statscombiner)
        output = dict(core.itermapred(input, lib.identitymapper, lib.statsreducer))
        self.assertEqual(output['testkey'][0], 10)
        self.assertEqual(output['testkey'][1], 4.5)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMapReduce)
    unittest.TextTestRunner(verbosity=2).run(suite)
