#!/usr/bin/env python
"""
(meta-)test the dumbo.mapredtest unit-test framework,
running it on a mock map/reduce job to make sure
validation works as expected.
"""

import unittest

from dumbo.mapredtest import MapDriver, ReduceDriver, MapReduceDriver

# Example mapper / reducers
class mapper(object):
    def __call__(self, key, value):
        self.counters['test_counter'] += 1
        for word in value.split(): yield word,1

def reducer(key,values):
    yield key,sum(values)

class MRTestCase(unittest.TestCase):
    def testmapper(self):
        input = [
            (0, "test me"),
            (1, "hello")
        ]
        output = [('test', 1), ('me', 1), ('hello', 1)]
        MapDriver(mapper).with_input(input).with_output(output).run()

    def testreducer(self):
        input = [('test', 1), ('test', 1), ('me', 1,), ('hello', 1)]
        output = [('test', 2), ('me', 1), ('hello', 1)]
        ReduceDriver(reducer).with_input(input).with_output(output).run()

    def testmapreduce(self):
        input = [
            (0, "test me"),
            (1, "hello"),
            (2, "test")
        ]
        output = [('hello', 1), ('me', 1), ('test', 2)]
        MapReduceDriver(mapper, reducer).with_input(input).with_output(output).run()

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(MRTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
        