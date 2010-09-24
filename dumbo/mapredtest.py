#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
dumbo.mapredtest

Provide a simple way of unit-testing MapReduce jobs written
in dumbo locally. This is loosely based on Cloudera's MRUnit design.

See for example discussion on unit-testing MR jobs:
http://www.cloudera.com/blog/2009/07/advice-on-qa-testing-your-mapreduce-jobs/
http://www.cloudera.com/blog/2009/07/debugging-mapreduce-programs-with-mrunit/
"""

import sys
import inspect
from itertools import imap

from dumbo.core import itermap, iterreduce, itermapred
from dumbo.backends.common import MapRedBase

__all__ = ['MapDriver', 'ReduceDriver', 'MapReduceDriver']

class BaseDriver(object):
    """A Generic test driver that passes
    input stream through a callable and
    checks output stream matches specified one.
    Implements some MapReduce/dumbo specific checks
    and verification on parameters."""
    
    def __init__(self, kallable):
        """Initialize instance data"""
        
        # Check if given callable is a function or a class 
        # type that needs instantiation
        if inspect.isclass(kallable):
            # Re-derive class using dumbo's common MapRedBase object.
            kallable = self._instrument_class(kallable)
            self._callable = kallable()
        else:
            self._callable = kallable        
            	
        self._input_source = None
        self._output_source = None
        
    def with_input(self, input_source):
        """Bind input stream"""
        if not hasattr(input_source, "next"):
            # Not an iterator
            self._input_source = iter(input_source)
        else:
            self._input_source = input_source
        return self
    
    def with_output(self, output_source):
        """Bind output stream"""
        if not hasattr(output_source, 'next'):
            # Not an iterator
            self._output_source = iter(output_source)
        else:
            self._output_source = output_source
        return self
        
    def run(self):
        """Run test"""
        for output in imap(self._func, self._input_source):
            exp_out = self._output_source.next()
            assert output == exp_out, \
                   "Output {0} did not match expected output: {1}".format(\
                       output, exp_out)

      
    def _instrument_class(self, cls):
        """Instrument a class for use with dumbo mapreduce tests"""
        newcls = type('InstrumentedClass', (cls, MapRedBase), {})
        return newcls
    
        
class MapDriver(BaseDriver):
    """Driver for Map operations"""    

    @property
    def mapper(self):
        return self._callable
    
    def run(self):
        """Run test"""
        it = itermap(self._input_source, self._callable)
        for output in it:
            exp_out = self._output_source.next()
            assert output == exp_out, \
                   "Output {0} did not match expected output: {1}".format(\
                       output, exp_out)    
    
    
class ReduceDriver(BaseDriver):
    """Stub driver for Reduce operations"""    

    @property
    def reducer(self):
        return self._callable
    
    def run(self):
        """Run test"""
        it = iterreduce(self._input_source, self._callable)
        for output in it:
            exp_out = self._output_source.next()
            assert output == exp_out, \
                   "Output {0} did not match expected output: {1}".format(\
                       output, exp_out)     
    
    
class MapReduceDriver(BaseDriver):
    """Stub driver for Map operations"""
    
    def __init__(self, mapper, reducer):
        BaseDriver.__init__(self, None)
        
        if inspect.isclass(mapper):
            mapper = self._instrument_class(mapper)
            self._mapper = mapper()
        else:
            self._mapper = mapper  
            
        if inspect.isclass(reducer):
            reducer = self._instrument_class(reducer)
            self._reducer = reducer()
        else:
            self._reducer = reducer
            
    @property
    def mapper(self):
        return self._mapper
    
    @property
    def reducer(self):
        return self._reducer
    
    def run(self):
        """Run test"""
        it = itermapred(self._input_source, self._mapper, self._reducer)
        for output in it:
            exp_out = self._output_source.next()
            assert output == exp_out, \
                   "Output {0} did not match expected output: {1}".format(\
                       output, exp_out)     
            
    