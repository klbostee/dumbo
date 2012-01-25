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
import os
import types
from itertools import chain, imap, izip
from math import sqrt
from copy import copy

from dumbo.util import loadclassname, Options


def identitymapper(key, value):
    yield (key, value)


def identityreducer(key, values):
    for value in values:
        yield (key, value)


def sumreducer(key, values):
    yield (key, sum(values))


def sumsreducer(key, values):
    yield (key, tuple(imap(sum, izip(*values))))


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
    columns = izip(*values)
    s0 = sum(columns.next()) # n
    s1 = sum(columns.next()) # sum(x)
    s2 = sum(columns.next()) # sum(x**2)
    minimum = min(columns.next())
    maximum = max(columns.next())
    mean = float(s1) / s0
    std = 0
    if s0 > 1:
        std = sqrt((s2-s1**2/float(s0))/(s0-1)) # sample standard deviation
    yield (key, (s0, mean, std, minimum, maximum))


def statscombiner(key, values):
    columns = izip(*((1, value, value**2, value, value) for value in values))
    s0 = sum(columns.next())
    s1 = sum(columns.next())
    s2 = sum(columns.next())
    minimum = min(columns.next())
    maximum = max(columns.next())
    yield (key, (s0, s1, s2, minimum, maximum))


class MultiMapper(object):

    def __new__(cls):
        if os.environ.get("dumbo_joinkeys", "no") == "yes":
            cls.__call__ = cls.__call__joinkey
        else:
            cls.__call__ = cls.__call__normalkey
        return object.__new__(cls) 

    def __init__(self):
        self.mappers = []
        self.opts = Options([("addpath", "iter")])

    def configure(self):
        mrbase_class = loadclassname(os.environ['dumbo_mrbase_class'])
        mappers, closefuncs = [], []
        for pattern, mapper in self.mappers:
            if type(mapper) in (types.ClassType, type):
                mappercls = type('DumboMapper', (mapper, mrbase_class), {})
                mapper = mappercls()
                if hasattr(mappercls, 'map'):
                    mapper = mapper.map
            if hasattr(mapper, 'configure'):
                mapper.configure()
            if hasattr(mapper, 'close'):
                closefuncs.append(mapper.close)
            mappers.append((pattern, mapper))
        self.mappers = mappers
        self.closefuncs = closefuncs

    def close(self):
        for closefunc in self.closefuncs:
            closefunc()

    def __call__normalkey(self, data):
        mappers = self.mappers
        for key, value in data:
            path, key = key
            for pattern, mapper in mappers:
                if pattern in path:
                    for output in mapper(key, value):
                        yield output

    def __call__joinkey(self, data):
        mappers = self.mappers
        for key, value in data:
            path = key.body[0]
            key.body = key.body[1]
            for pattern, mapper in mappers:
                if pattern in path:
                    for output in mapper(key, value):
                        yield output

    def add(self, pattern, mapper):
        self.mappers.append((pattern, mapper))
        if hasattr(mapper, 'opts'):
            self.opts += mapper.opts


class JoinMapper(object):

    def __init__(self, mapper, isprimary=False):
        self.mapper = mapper
        self.isprimary = isprimary
        self.opts = Options([('joinkeys', 'yes')])
        if hasattr(mapper, 'opts'):
            self.opts += self.mapper.opts
        self.closefunc = None

    def configure(self):
        mrbase_class = loadclassname(os.environ['dumbo_mrbase_class'])
        mapper = self.mapper
        if type(mapper) in (types.ClassType, type):
            mappercls = type('DumboMapper', (mapper, mrbase_class), {})
            mapper = mappercls()
            if hasattr(mapper, 'map'):
                mapper = mapper.map
        if hasattr(mapper, 'configure'):
            mapper.configure()
        if hasattr(mapper, 'close'):
            self.closefunc = mapper.close
        self.mapper = mapper

    def close(self):
        if self.closefunc:
            self.closefunc()

    def __call__(self, key, value):
        key.isprimary = self.isprimary
        for k, v in self.mapper(key.body, value):
            jk = copy(key)
            jk.body = k
            yield jk, v

            
class PrimaryMapper(JoinMapper):

    def __init__(self, mapper):
        JoinMapper.__init__(self, mapper, True)


class SecondaryMapper(JoinMapper): 

    def __init__(self, mapper):
        JoinMapper.__init__(self, mapper, False)


class JoinCombiner(object):

    opts = Options([("joinkeys", "yes")])

    def __call__(self, key, values):
        if key.isprimary:
            self._key = key.body
            output = self.primary(key.body, values)
            if output:
                for k, v in output:
                    jk = copy(key)
                    jk.body = k
                    yield jk, v
        elif not self.secondary_blocked(key.body):
            for k, v in self.secondary(key.body, values):
                jk = copy(key)
                jk.body = k
                yield jk, v

    def secondary_blocked(self, key_body):
        '''Determines if the secondary method should be blocked or not.'''
        return False

    def primary(self, key, values):
        for value in values:
            yield key, value

    def secondary(self, key, values):
        for value in values:
            yield key, value


class JoinReducer(JoinCombiner):

    def __init__(self):
        self._key = None

    def secondary_blocked(self, body):
        return self._key != body

