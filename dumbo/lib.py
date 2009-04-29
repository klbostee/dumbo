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

import heapq
from itertools import chain, imap
from math import sqrt

from dumbo.util import iizip


def identitymapper(key, value):    
    yield (key, value)


def identityreducer(key, values):
    for value in values:    
        yield (key, value)    


def sumreducer(key, values):
    yield (key, sum(values))         


def sumsreducer(key, values):
    yield (key, tuple(imap(sum, iizip(*values))))       


def nlargestreducer(n, key=None):                      
    def reducer(key_, values):
        yield (key_, heapq.nlargest(n, chain(*values), key=key))
    return reducer                 


def nlargestcombiner(n, key=None):
    def combiner(key_, values):
        yield (key_, heapq.nlargest(n, values, key=key))      
    return combiner    


def nsmallestreducer(n, key=None):
    def reducer(key_, values):
        yield (key_, heapq.nsmallest(n, chain(*values), key=key))
    return reducer

def nsmallestcombiner(n, key=None):
    def combiner(key_, values):                         
        yield (key_, heapq.nsmallest(n, values, key=key))
    return combiner


def statsreducer(key, values):
    columns = iizip(*values)
    s0 = float(sum(columns.next())) # n
    s1 = sum(columns.next()) # sum(x)
    s2 = sum(columns.next()) # sum(x**2)
    minimum = min(columns.next())
    maximum = max(columns.next())
    mean = float(s1) / s0
    std = sqrt((s2-s1**2/s0)/(s0-1)) # sample standard deviation
    yield (key, (s0, mean, std, minimum, maximum))


def statscombiner(key, values):
    columns = iizip(*((1, value, value**2, value, value) for value in values))
    s0 = sum(columns.next())
    s1 = sum(columns.next())
    s2 = sum(columns.next())
    minimum = min(columns.next())
    maximum = max(columns.next())
    yield (key, (s0, s1, s2, minimum, maximum))
