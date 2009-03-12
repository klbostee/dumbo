import unittest
from dumbo import core

class TestMapRed(unittest.TestCase):
    def testwordcount(self):
        def mapper(key,value):
            for word in value.split(): yield word,1
        def reducer(key,values):
            yield key,sum(values)
        input = enumerate(['one two','two one two'])
        output = dict(core.itermapred(input,mapper,reducer))
        self.assertEqual(output['one'],2)
        self.assertEqual(output['two'],3)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMapReduce)
    unittest.TextTestRunner(verbosity=2).run(suite)
