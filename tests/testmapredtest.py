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
    
class reducer_with_params(object):
    
    def __call__(self, key, value):
        self.counters['test_counter'] += 1
        yield 'foo', str(self.params['foo'])
        yield 'one', str(self.params['one'])
        self.counters['test_counter'] += 1
    

class MRTestCase(unittest.TestCase):
    def testmapper(self):
        input = [
            (0, "test me"),
            (1, "hello")
        ]
        output = [('test', 1),
                  ('me', 1),
                  ('hello', 1)]
        MapDriver(mapper).with_input(input).with_output(output).run()

    def testreducer_with_params(self):
        input = [
            (0, "test me"),
            (1, "hello")
        ]
        #each 3 map calls will yield with both params 
        output = [('foo', 'bar'), ('one', '1')] * 2
        params = [('foo', 'bar'), ('one', '1')]
        ReduceDriver(reducer_with_params).with_params(params).with_input(input).with_output(output).run()
       
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

    def test_toomany(self):
        input_ = [(0, 'a b c')]
        output = [('a', 1), ('b', 1)]
        self.assertRaises(AssertionError, MapDriver(mapper).with_input(input_).with_output(output).run)
        output = [('a', 1), ('b', 1), ('c', 1), ('d', 1)]
        self.assertRaises(AssertionError, MapDriver(mapper).with_input(input_).with_output(output).run)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(MRTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
        