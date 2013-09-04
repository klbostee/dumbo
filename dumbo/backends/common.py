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


import os
import re

from dumbo.util import incrcounter, setstatus, configopts

class Params(object):
    """
    >>> os.environ["hi"] = "world"
    >>> p = Params()
    >>> "hi" in p
    True
    >>> p["hi"] == "world"
    True
    >>> p.get("hi") == "world"
    True
    >>> p.get("hello", "dumbo") == "dumbo"
    True
    >>>
    """
    def get(self, name, default=None): 
        try:
            return os.environ[name]
        except KeyError:
            return default

    def __getitem__(self, key):
        return self.get(str(key))

    def __contains__(self, key):
        return self.get(str(key)) is not None


class Counter(object):

    def __init__(self, name, group='Program'):
        self.group = group
        self.name = name

    def incr(self, amount):
        incrcounter(self.group, self.name, amount)
        return self
    __iadd__ = incr


class Counters(object):
    
    def __init__(self):
        self.counters = {}

    def __getitem__(self, key):
        try:
            return self.counters[key]
        except KeyError:
            counter = Counter(str(key))
            self.counters[key] = counter
            return counter

    def __setitem__(self, key, value):
        pass


class MapRedBase(object):
    
    params = Params()
    counters = Counters()

    getparam = params.get
    
    def setstatus(self, msg):
        setstatus(msg)
    status = property(fset=setstatus)


class JoinKey(object):

    def __init__(self, body, isprimary=False):
        self.body = body
        self.isprimary = isprimary
  
    def __cmp__(self, other):
        if isinstance(other, JoinKey):
            # For isprimary, order is switched because we want True to sort before False
            return cmp(self.body, other.body) or cmp(other.isprimary, self.isprimary)
        else:
            return -1     # JoinKeys arbitrarily come before everything else

    @classmethod
    def fromjoinkey(cls, jk):
        return cls(jk.body, jk.isprimary)

    @classmethod
    def fromdump(cls, dump):
        return cls(dump[0], dump[1] == 1)

    def dump(self):
        return (self.body, 2 - int(self.isprimary))

    def __repr__(self):
        return repr(self.dump())


class RunInfo(object):

    def get_input_path(self):
        return 'unknown'


class Iteration(object):

    def __init__(self, prog, opts):
        (self.prog, self.opts) = (prog, opts)

    def run(self):
        opts = self.opts
        attrs = ['fake', 'debug', 'python', 'iteration', 'itercount', 'hadoop', 
            'starter', 'name', 'memlimit', 'param', 'parser', 'record', 
            'joinkeys', 'hadoopconf', 'mapper', 'reducer']
        addedopts = opts.filter(attrs)
        opts.remove(*attrs)

        if 'yes' in addedopts['fake']:
            def dummysystem(*args, **kwargs):
                return 0
            global system
            system = dummysystem  # not very clean, but it works...
        if 'yes' in addedopts['debug']:
            opts.add('cmdenv', 'dumbo_debug=yes')
        if not addedopts['python']:
            python = 'python'
        else:
            python = addedopts['python'][0]
        opts.add('python', python)
        if not addedopts['iteration']:
            iter = 0
        else:
            iter = int(addedopts['iteration'][0])
        if not addedopts['itercount']:
            itercnt = 1
        else:
            itercnt = int(addedopts['itercount'][0])
        if addedopts['name']:
            name = addedopts['name'][0]
        else:
            name = self.prog.split('/')[-1]
        opts.add('name', '%s (%s/%s)' % (name, iter + 1, itercnt))
        if not addedopts['hadoop']:
            pypath = '/'.join(self.prog.split('/')[:-1])
            if pypath:
                opts.add('pypath', pypath)
        else:
            opts.add('hadoop', addedopts['hadoop'][0])
        progmod = self.prog.split('/')[-1]
        progmod = progmod[:-3] if progmod.endswith('.py') else progmod
        memlim = ' 262144000'  # 250MB limit by default
        if addedopts['memlimit']:
            # Limit amount of memory. This supports syntax 
            # of the form '256m', '12g' etc.
            try:
                _memlim = int(addedopts['memlimit'][0][:-1])
                memlim = ' %i' % {
                    'g': 1073741824    * _memlim,
                    'm': 1048576       * _memlim,
                    'k': 1024          * _memlim,
                    'b': 1             * _memlim,
                }[addedopts['memlimit'][0][-1].lower()]
            except KeyError:
                # Assume specified in bytes by default
                memlim = ' ' + addedopts['memlimit'][0]

        if addedopts['mapper']:
            opts.add('mapper', addedopts['mapper'][0])
        else:
            opts.add('mapper', '%s -m %s map %i%s' % (python, progmod, iter, 
                memlim))
        if addedopts['reducer']:
            opts.add('reducer', addedopts['reducer'][0])
        else:
            opts.add('reducer', '%s -m %s red %i%s' % (python, progmod, 
                iter, memlim))
        for param in addedopts['param']:
            opts.add('cmdenv', param)
        if addedopts['parser'] and iter == 0:
            parser = addedopts['parser'][0]
            shortcuts = dict(configopts('parsers', self.prog))
            if parser in shortcuts:
                parser = shortcuts[parser]
            opts.add('cmdenv', 'dumbo_parser=' + parser)
        if addedopts['record'] and iter == 0:
            record = addedopts['record'][0]
            shortcuts = dict(configopts('records', self.prog))
            if record in shortcuts:
                record = shortcuts[record]
            opts.add('cmdenv', 'dumbo_record=' + record)
        if 'yes' in addedopts['joinkeys']:
            opts.add('cmdenv', 'dumbo_joinkeys=yes')
            opts.add('partitioner', 'org.apache.hadoop.mapred.lib.BinaryPartitioner')
            opts.add('jobconf', 'mapred.binary.partitioner.right.offset=-6')
        for hadoopconf in addedopts['hadoopconf']:
            opts.add('jobconf', hadoopconf)
        opts.add('libegg', re.sub('\.egg.*$', '.egg', __file__))
        return 0


class FileSystem(object):
    
    def cat(self, path, opts):
        return 1  # fail by default
    
    def ls(self, path, opts):
        return 1  # fail by default
    
    def exists(self, path, opts):
        return 1  # fail by default
    
    def rm(self, path, opts):
        return 1  # fail by default
    
    def put(self, path1, path2, opts):
        return 1  # fail by default
    
    def get(self, path1, path2, opts):
        return 1  # fail by default


class Backend(object):
    
    def matches(self, opts):
        """ Returns True if the backend matches with the given opts """ 
        return True

    #abstractmethod
    def create_iteration(self, opts):
        """ Creates a suitable Iteration object """
        pass
    
    #abstractmethod
    def create_filesystem(self, opts):
        """ Creates a suitable FileSystem object """
        pass

    def get_mapredbase_class(self, opts):
        """ Returns a suitable MapRedBase class """
        return MapRedBase
    
    def get_joinkey_class(self, opts):
        """ Returns a suitable JoinKey class """
        return JoinKey

    def get_runinfo_class(self, opts):
        """ Returns a suitable RunInfo class """
        return RunInfo
