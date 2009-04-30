import unittest
from dumbo import lib, core

class TestLib(unittest.TestCase):

    def teststats(self):
        input = [('testkey',i) for i in xrange(10)]
        input = core.itermapred(input, lib.identitymapper, lib.statscombiner)
        output = dict(core.itermapred(input, lib.identitymapper, lib.statsreducer))
        self.assertEqual(output['testkey'][0], 10) # n
        self.assertEqual(output['testkey'][1], 4.5) # mean
        self.assertAlmostEqual(output['testkey'][2], 3.02765035409749) # std 

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMapReduce)
    unittest.TextTestRunner(verbosity=2).run(suite)
