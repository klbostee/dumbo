import unittest
from dumbo import lib, core

class TestLib(unittest.TestCase):

    def teststats(self):
        input = [('testkey',i) for i in xrange(3)]
        input = core.itermapred(input, lib.identitymapper, lib.statscombiner)
        output = dict(core.itermapred(input, lib.identitymapper, lib.statsreducer))
        self.assertEqual(output['testkey'][0], 3) # n
        self.assertEqual(output['testkey'][1], 1) # mean
        self.assertEqual(output['testkey'][2], 1.0) # std 

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMapReduce)
    unittest.TextTestRunner(verbosity=2).run(suite)
