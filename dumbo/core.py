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

import sys
import os
import re
import types
import resource
import copy
from itertools import groupby
from operator import itemgetter, concat

from dumbo.util import *
from dumbo.cmd import *


class Error(Exception):
    pass


class Job(object):
    
    def __init__(self):
        self.iters = []

    def additer(self, *args, **kwargs):
        self.iters.append((args, kwargs))

    def run(self):
        for (index, (args, kwargs)) in enumerate(self.iters):
            (kwargs['iter'], kwargs['itercnt']) = (index, len(self.iters))
            run(*args, **kwargs)


class Program(object):

    def __init__(self, prog, opts=[]):
        (self.prog, self.opts) = (prog, opts)
        self.started = False

    def addopt(self, key, value):
        self.opts.append((key, value))

    def delopts(self, key):
        return getopts(self.opts, [key], delete=True)[key]

    def delopt(self, key):
        try:
            return self.delopts(key)[0]
        except IndexError:
            return None

    def getopts(self, key):
        return getopts(self.opts, [key], delete=False)[key]

    def getopt(self, key):
        try:
            return self.getopts(key)[0]
        except IndexError:
            return None

    def clone(self):
        return copy.deepcopy(self)

    def start(self):
        if self.started:
            return 0
        self.started = True
        return start(self.prog, self.opts)


class Params(object):

    def get(self, name): 
        try:
            return os.environ[name]
        except KeyError:
            return None

    def __getitem__(self, key):
        return self.get(str(key))

    def __contains__(self, key):
        return self.get(str(key)) != None
   

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
    
    def setstatus(self, msg):
        setstatus(msg)
    status = property(fset=setstatus)


class JoinKey(object):

    def __init__(self, body, isprimary=False):
        self.body = body
        self.isprimary = isprimary
  
    def __cmp__(self, other):
        bodycmp = cmp(self.body, other.body)
        if bodycmp:
           return bodycmp
        else:
           return cmp(self.isprimary, other.isprimary)

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


class Iteration(object):

    def __init__(self, prog, opts):
        (self.prog, self.opts) = (prog, opts)

    def run(self):
        addedopts = getopts(self.opts, ['fake',
                                        'debug',
                                        'python',
                                        'iteration',
                                        'itercount',
                                        'hadoop',
                                        'starter',
                                        'name',
                                        'memlimit',
                                        'param',
                                        'parser',
                                        'record',
                                        'joinkeys',
                                        'hadoopconf',
                                        'mapper',
                                        'reducer'])
        if addedopts['fake'] and addedopts['fake'][0] == 'yes':
            def dummysystem(*args, **kwargs):
                return 0
            global system
            system = dummysystem  # not very clean, but it works...
        if addedopts['debug'] and addedopts['debug'][0] == 'yes':
            self.opts.append(('cmdenv', 'dumbo_debug=yes'))
        if not addedopts['python']:
            python = 'python'
        else:
            python = addedopts['python'][0]
        self.opts.append(('python', python))
        if not addedopts['iteration']:
            iter = 0
        else:
            iter = int(addedopts['iteration'][0])
        if not addedopts['itercount']:
            itercnt = 0
        else:
            itercnt = int(addedopts['itercount'][0])
        if addedopts['name']:
            name = addedopts['name'][0]
        else:
            name = self.prog.split('/')[-1]
        self.opts.append(('name', '%s (%s/%s)' % (name, iter + 1,
                         itercnt)))
        if not addedopts['hadoop']:
            pypath = '/'.join(self.prog.split('/')[:-1])
            if pypath: self.opts.append(('pypath', pypath))
        else:
            self.opts.append(('hadoop', addedopts['hadoop'][0]))
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
            self.opts.append(('mapper', addedopts['mapper'][0]))
        else:
            self.opts.append(('mapper', '%s -m %s map %i%s' % (python,
                             progmod, iter, memlim)))
        if addedopts['reducer']:
            self.opts.append(('reducer', addedopts['reducer'][0]))
        else:
            self.opts.append(('reducer', '%s -m %s red %i%s' % (python,
                             progmod, iter, memlim)))
        for param in addedopts['param']:
            self.opts.append(('cmdenv', param))
        if addedopts['parser'] and iter == 0:
            parser = addedopts['parser'][0]
            shortcuts = dict(configopts('parsers', self.prog))
            if parser in shortcuts:
                parser = shortcuts[parser]
            self.opts.append(('cmdenv', 'dumbo_parser=' + parser))
        if addedopts['record'] and iter == 0:
            record = addedopts['record'][0]
            shortcuts = dict(configopts('records', self.prog))
            if record in shortcuts:
                record = shortcuts[record]
            self.opts.append(('cmdenv', 'dumbo_record=' + record))
        if addedopts['joinkeys'] and addedopts['joinkeys'][0] == 'yes':
            self.opts.append(('cmdenv', 'dumbo_joinkeys=yes'))
            self.opts.append(('partitioner',
                              'org.apache.hadoop.mapred.lib.BinaryPartitioner'))
            self.opts.append(('jobconf',
                              'mapred.binary.partitioner.right.offset=-6'))
        for hadoopconf in addedopts['hadoopconf']:
            self.opts.append(('jobconf', hadoopconf))
        self.opts.append(('libegg', re.sub('\.egg.*$', '.egg', __file__)))
        return 0


class UnixIteration(Iteration):

    def __init__(self, prog, opts):
        Iteration.__init__(self, prog, opts)
        self.opts += configopts('unix', prog, self.opts)

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval
        addedopts = getopts(self.opts, ['input',
                                        'output',
                                        'mapper',
                                        'reducer',
                                        'libegg',
                                        'delinputs',
                                        'cmdenv',
                                        'pv',
                                        'addpath',
                                        'inputformat',
                                        'outputformat',
                                        'numreducetasks',
                                        'python',
                                        'pypath',
                                        'sorttmpdir',
                                        'sortbufsize'])
        (mapper, reducer) = (addedopts['mapper'][0], addedopts['reducer'][0])
        if not addedopts['input'] or not addedopts['output']:
            print >> sys.stderr, 'ERROR: input or output not specified'
            return 1
        inputs = reduce(concat, (input.split(' ') for input in
                        addedopts['input']))
        output = addedopts['output'][0]
        pyenv = envdef('PYTHONPATH', addedopts['libegg'],
                       shortcuts=dict(configopts('eggs', self.prog)),
                       extrapaths=addedopts['pypath'])
        cmdenv = ' '.join("%s='%s'" % tuple(arg.split('=')) for arg in
                          addedopts['cmdenv'])
        if addedopts['pv'] and addedopts['pv'][0] == 'yes':
            mpv = '| pv -s `du -b %s | cut -f 1` -cN map ' % ' '.join(inputs)
            (spv, rpv) = ('| pv -cN sort ', '| pv -cN reduce ')
        else:
            (mpv, spv, rpv) = ('', '', '')

        (sorttmpdir, sortbufsize) = ('', '')
        if addedopts['sorttmpdir']:
            sorttmpdir = "-T %s" % addedopts['sorttmpdir'][0]
        if addedopts['sortbufsize']:
            sortbufsize = "-S %s" % addedopts['sortbufsize'][0]

        python = addedopts['python'][0]
        encodepipe = pyenv + ' ' + python + \
                     ' -m dumbo.cmd encodepipe -file ' + ' -file '.join(inputs)
        if addedopts['inputformat'] and addedopts['inputformat'][0] == 'code':
            encodepipe += ' -alreadycoded yes'
        if addedopts['addpath'] and addedopts['addpath'][0] != 'no':
            encodepipe += ' -addpath yes'
        if addedopts['numreducetasks'] and addedopts['numreducetasks'][0] == '0':
            retval = execute("%s | %s %s %s %s > '%s'" % (encodepipe,
                                                          pyenv,
                                                          cmdenv,
                                                          mapper,
                                                          mpv,
                                                          output))
        else:
            retval = execute("%s | %s %s %s %s| LC_ALL=C sort %s %s %s| %s %s %s %s> '%s'"
                             % (encodepipe,
                                pyenv,
                                cmdenv,
                                mapper,
                                mpv,
                                sorttmpdir,
                                sortbufsize,
                                spv,
                                pyenv,
                                cmdenv,
                                reducer,
                                rpv,
                                output))
        if addedopts['delinputs'] and addedopts['delinputs'][0] == 'yes':
            for file in addedopts['input']:
                execute('rm ' + file)
        return retval


class StreamingIteration(Iteration):

    def __init__(self, prog, opts):
        Iteration.__init__(self, prog, opts)
        self.opts += configopts('streaming', prog, self.opts)
        hadoop = getopt(self.opts, 'hadoop', delete=False)[0]
        self.opts += configopts('streaming_' + hadoop, prog, self.opts)

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval
        if os.path.exists(self.prog):
            self.opts.append(('file', self.prog))
        addedopts = getopts(self.opts, ['hadoop',
                                        'name',
                                        'delinputs',
                                        'libegg',
                                        'libjar',
                                        'inputformat',
                                        'outputformat',
                                        'nummaptasks',
                                        'numreducetasks',
                                        'priority',
                                        'cachefile',
                                        'cachearchive',
                                        'file',
                                        'codewritable',
                                        'addpath',
                                        'getpath',
                                        'python',
                                        'streamoutput',
                                        'pypath'])
        hadoop = findhadoop(addedopts['hadoop'][0])
        streamingjar = findjar(hadoop, 'streaming')
        if not streamingjar:
            print >> sys.stderr, 'ERROR: Streaming jar not found'
            return 1
        try: 
            import typedbytes
        except ImportError:
            print >> sys.stderr, 'ERROR: "typedbytes" module not found'
            return 1
        modpath = re.sub('\.egg.*$', '.egg', typedbytes.__file__)
        if modpath.endswith('.egg'):            
            addedopts['libegg'].append(modpath)    
        else:
            self.opts.append(('file', modpath)) 
        self.opts.append(('jobconf', 'stream.map.input=typedbytes'))
        self.opts.append(('jobconf', 'stream.reduce.input=typedbytes'))
        if addedopts['numreducetasks'] and addedopts['numreducetasks'][0] == '0':
            self.opts.append(('jobconf', 'stream.reduce.output=typedbytes'))
            if addedopts['streamoutput']:
                id_ = addedopts['streamoutput'][0]
                self.opts.append(('jobconf', 'stream.map.output=' + id_))
            else: 
                self.opts.append(('jobconf', 'stream.map.output=typedbytes'))
        else:
            self.opts.append(('jobconf', 'stream.map.output=typedbytes'))
            if addedopts['streamoutput']:
                id_ = addedopts['streamoutput'][0]
                self.opts.append(('jobconf', 'stream.reduce.output=' + id_))
            else:
                self.opts.append(('jobconf', 'stream.reduce.output=typedbytes'))
        if not addedopts['name']:
            self.opts.append(('jobconf', 'mapred.job.name='
                              + self.prog.split('/')[-1]))
        else:
            self.opts.append(('jobconf', 'mapred.job.name=%s'
                              % addedopts['name'][0]))
        if addedopts['nummaptasks']:
            self.opts.append(('jobconf', 'mapred.map.tasks=%s'
                              % addedopts['nummaptasks'][0]))
        if addedopts['numreducetasks']:
            numreducetasks = int(addedopts['numreducetasks'][0])
            self.opts.append(('numReduceTasks', str(numreducetasks)))
        if addedopts['priority']:
            self.opts.append(('jobconf', 'mapred.job.priority=%s'
                              % addedopts['priority'][0]))
        if addedopts['cachefile']:
            for cachefile in addedopts['cachefile']:
                self.opts.append(('cacheFile', cachefile))
        if addedopts['cachearchive']:
            for cachearchive in addedopts['cachearchive']:
                self.opts.append(('cacheArchive', cachearchive))
        if addedopts['file']:
            for file in addedopts['file']:
                if not '://' in file:
                    if not os.path.exists(file):
                        raise ValueError('file "' + file + '" does not exist')
                    file = 'file://' + os.path.abspath(file)
                self.opts.append(('file', file))
        if not addedopts['inputformat']:
            addedopts['inputformat'] = ['auto']
        inputformat_shortcuts = \
            {'code': 'org.apache.hadoop.streaming.AutoInputFormat',
             'text': 'org.apache.hadoop.mapred.TextInputFormat',
             'sequencefile': 'org.apache.hadoop.streaming.AutoInputFormat',
             'auto': 'org.apache.hadoop.streaming.AutoInputFormat'}
        inputformat_shortcuts.update(configopts('inputformats', self.prog))
        inputformat = addedopts['inputformat'][0]
        if inputformat_shortcuts.has_key(inputformat.lower()):
            inputformat = inputformat_shortcuts[inputformat.lower()]
        self.opts.append(('inputformat', inputformat))
        if not addedopts['outputformat']:
            addedopts['outputformat'] = ['sequencefile']
        if addedopts['getpath'] and addedopts['getpath'] != 'no':
            outputformat_shortcuts = \
                {'code': 'fm.last.feathers.output.MultipleSequenceFiles',
                 'text': 'fm.last.feathers.output.MultipleTextFiles',               
                 'sequencefile': 'fm.last.feathers.output.MultipleSequenceFiles'}
        else:
            outputformat_shortcuts = \
                {'code': 'org.apache.hadoop.mapred.SequenceFileOutputFormat',
                 'text': 'org.apache.hadoop.mapred.TextOutputFormat',
                 'sequencefile': 'org.apache.hadoop.mapred.SequenceFileOutputFormat'}
        outputformat_shortcuts.update(configopts('outputformats', self.prog))
        outputformat = addedopts['outputformat'][0]
        if outputformat_shortcuts.has_key(outputformat.lower()):
            outputformat = outputformat_shortcuts[outputformat.lower()]
        self.opts.append(('outputformat', outputformat))
        if addedopts['addpath'] and addedopts['addpath'][0] != 'no':
            self.opts.append(('cmdenv', 'dumbo_addpath=true'))
        pyenv = envdef('PYTHONPATH',
                       addedopts['libegg'],
                       'file',
                       self.opts,
                       shortcuts=dict(configopts('eggs', self.prog)),
                       quote=False,
                       trim=True,
                       extrapaths=addedopts['pypath'])
        if pyenv:
            self.opts.append(('cmdenv', pyenv))
        hadenv = envdef('HADOOP_CLASSPATH', addedopts['libjar'], 'libjar', 
                        self.opts, shortcuts=dict(configopts('jars', self.prog)))
        fileopt = getopt(self.opts, 'file')
        if fileopt:
            tmpfiles = []
            for file in fileopt:
                if file.startswith('file://'):
                    self.opts.append(('file', file[7:]))
                else:
                    tmpfiles.append(file)
            if tmpfiles:
                self.opts.append(('jobconf', 'tmpfiles=' + ','.join(tmpfiles)))
        libjaropt = getopt(self.opts, 'libjar')
        if libjaropt:
            tmpjars = []
            for jar in libjaropt:
                if jar.startswith('file://'):
                    self.opts.append(('file', jar[7:]))
                else:
                    tmpjars.append(jar)
            if tmpjars:
                self.opts.append(('jobconf', 'tmpjars=' + ','.join(tmpjars)))
        cmd = hadoop + '/bin/hadoop jar ' + streamingjar
        retval = execute(cmd, self.opts, hadenv)
        if addedopts['delinputs'] and addedopts['delinputs'][0] == 'yes':
            for (key, value) in self.opts:
                if key == 'input':
                    execute("%s/bin/hadoop dfs -rmr '%s'" % (hadoop, value))
        return retval


def main(runner, starter=None, variator=None):
    opts = parseargs(sys.argv[1:])
    starteropt = getopts(opts, ['starter'])['starter']
    opts.append(('starter', 'no'))
    if starter and not (starteropt and starteropt[0] == 'no') \
    and not (len(sys.argv) > 1 and sys.argv[1][0] != '-'):
        progopt = getopt(opts, 'prog')
        if not progopt:
            program = Program(sys.argv[0], opts)
        else:
            program = Program(progopt[0], opts)
        try:
            if variator:
                programs = variator(program)
                # note the the variator can be a generator, which
                # implies that exceptions might only occur later
            else:
                programs = [program]
            status = 0
            for program in programs:
                try:
                    errormsg = starter(program)
                except Error, e:
                    errormsg = str(e)
                    status = 1
                if errormsg:
                    print >> sys.stderr, "ERROR: " + errormsg
                    status = 1
                retval = program.start()
                if retval != 0:
                    status = retval
            if status != 0:
                sys.exit(status)
        except Error, e:
            print >> sys.stderr, "ERROR: " + str(e)
            sys.exit(1)
    else:
        job = Job()
        errormsg = runner(job)
        if errormsg:
            print >> sys.sdterr, errormsg
            sys.exit(1)
        job.run()


def run(mapper,
        reducer=None,
        combiner=None,
        buffersize=None,
        mapconf=None,
        redconf=None,
        combconf=None,
        mapclose=None,
        redclose=None,
        combclose=None,
        opts=None,
        iter=0,
        itercnt=1):
    if len(sys.argv) > 1 and not sys.argv[1][0] == '-':
        iterarg = 0  # default value
        if len(sys.argv) > 2:
            iterarg = int(sys.argv[2])
        memlim = None  # memory limit
        if len(sys.argv) > 3:
            memlim = int(sys.argv[3])
            resource.setrlimit(resource.RLIMIT_AS, (memlim, memlim))
        if iterarg == iter:
            if sys.argv[1].startswith('map'):
                if type(mapper) in (types.ClassType, type):
                    mappercls = type('DumboMapper', (mapper, MapRedBase), {})
                    mapper = mappercls()
                if hasattr(mapper, 'configure'):
                    mapconf = mapper.configure
                if hasattr(mapper, 'close'):
                    mapclose = mapper.close
                if hasattr(mapper, 'map'):
                    mapper = mapper.map
                if type(combiner) in (types.ClassType, type):
                    combinercls = type('DumboCombiner', (combiner, MapRedBase), {})
                    combiner = combinercls()
                if hasattr(combiner, 'configure'):
                    combconf = combiner.configure
                if hasattr(combiner, 'close'):
                    combclose = combiner.close
                if hasattr(combiner, 'reduce'):
                    combiner = combiner.reduce
                try:
                    print >> sys.stderr, "INFO: consuming %s" % \
                                         os.environ['map_input_file']
                except KeyError:
                    pass
                if os.environ.has_key('stream_map_input') and \
                os.environ['stream_map_input'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: inputting typed bytes"
                    try: import ctypedbytes as typedbytes
                    except ImportError: import typedbytes
                    inputs = typedbytes.PairedInput(sys.stdin).reads()
                else:
                    inputs = loadcode(line[:-1] for line in sys.stdin)
                if mapconf:
                    mapconf()
                if combconf:
                    combconf()
                if os.environ.has_key('dumbo_addpath'):
                    path = os.environ['map_input_file']
                    inputs = (((path, k), v) for (k, v) in inputs)
                if os.environ.has_key('dumbo_joinkeys'):
                    inputs = ((JoinKey(k), v) for (k, v) in inputs)
                if os.environ.has_key('dumbo_parser'):
                    parser = os.environ['dumbo_parser']
                    clsname = parser.split('.')[-1]          
                    modname = '.'.join(parser.split('.')[:-1])            
                    if not modname:
                        raise ImportError(parser)
                    module = __import__(modname, fromlist=[clsname])
                    parse = getattr(module, clsname)().parse
                    outputs = itermap(inputs, mapper, parse)
                elif os.environ.has_key('dumbo_record'):
                    record = os.environ['dumbo_record']
                    clsname = record.split('.')[-1]
                    modname = '.'.join(record.split('.')[:-1])
                    if not modname:
                        raise ImportError(parser)
                    module = __import__(modname, fromlist=[clsname])
                    set = getattr(module, clsname)().set
                    outputs = itermap(inputs, mapper, lambda v: set(*v))
                else:
                    outputs = itermap(inputs, mapper)
                if combiner and type(combiner) != str:
                    if (not buffersize) and memlim:
                        buffersize = int(memlim * 0.33) / 512  # educated guess
                        print >> sys.stderr, 'INFO: buffersize =', buffersize
                    inputs = sorted(outputs, buffersize)
                    if os.environ.has_key('dumbo_joinkeys'):
                        outputs = iterreduce(inputs, combiner,
                                             keyfunc=JoinKey.fromjoinkey)
                    else:
                        outputs = iterreduce(inputs, combiner)
                if os.environ.has_key('dumbo_joinkeys'):
                    outputs = ((jk.dump(), v) for (jk, v) in outputs)
                if os.environ.has_key('stream_map_output') and \
                os.environ['stream_map_output'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: outputting typed bytes"
                    try: import ctypedbytes as typedbytes
                    except ImportError: import typedbytes
                    typedbytes.PairedOutput(sys.stdout).writes(outputs)
                else:
                    for output in dumpcode(outputs):
                        print '\t'.join(output)
                if combclose:
                    combclose()
                if mapclose:
                    mapclose()
            elif reducer:
                if type(reducer) in (types.ClassType, type):
                    reducercls = type('DumboReducer', (reducer, MapRedBase), {})
                    reducer = reducercls()
                if hasattr(reducer, 'configure'):
                    redconf = reducer.configure
                if hasattr(reducer, 'close'):
                    redclose = reducer.close
                if hasattr(reducer, 'reduce'):
                    reducer = reducer.reduce
                if os.environ.has_key('stream_reduce_input') and \
                os.environ['stream_reduce_input'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: inputting typed bytes"
                    try: import ctypedbytes as typedbytes
                    except ImportError: import typedbytes
                    inputs = typedbytes.PairedInput(sys.stdin).reads()
                else:
                    inputs = loadcode(line[:-1] for line in sys.stdin)
                if redconf:
                    redconf()
                if os.environ.has_key('dumbo_joinkeys'):
                    outputs = iterreduce(inputs, reducer,
                                         keyfunc=JoinKey.fromdump)
                    outputs = ((jk.body, v) for (jk, v) in outputs)
                else:
                    outputs = iterreduce(inputs, reducer)
                if os.environ.has_key('stream_reduce_output') and \
                os.environ['stream_reduce_output'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: outputting typed bytes"
                    try: import ctypedbytes as typedbytes
                    except ImportError: import typedbytes
                    typedbytes.PairedOutput(sys.stdout).writes(outputs)
                else:
                    for output in dumpcode(outputs):
                        print '\t'.join(output)
                if redclose:
                    redclose()
            else:
                for output in dumpcode(inputs):
                    print '\t'.join(output)
    else:
        if not opts:
            opts = []
        if type(mapper) == str:
            opts.append(('mapper', mapper))
        elif hasattr(mapper, 'opts'):
            opts += mapper.opts
        if type(reducer) == str:
            opts.append(('reducer', reducer))
        elif hasattr(reducer, 'opts'):
            opts += reducer.opts
        if type(combiner) == str:
            opts.append(('combiner', combiner))
        opts += parseargs(sys.argv[1:])
        outputopt = getopt(opts, 'output', delete=False)
        if not outputopt:
            print >> sys.stderr, 'ERROR: No output path specified'
            sys.exit(1)
        (output, checkoutopt) = (outputopt[0], getopt(opts, 'checkoutput'))
        checkoutput = not (checkoutopt and checkoutopt[0] == 'no')
        if checkoutput and exists(output, opts) == 0:
            print >> sys.stderr, 'ERROR: Output path exists already: %s' % output
            sys.exit(1)
        newopts = {}
        newopts['iteration'] = str(iter)
        newopts['itercount'] = str(itercnt)
        outputopt = getopt(opts, 'output', delete=False)
        if not outputopt:
            print >> sys.stderr, 'ERROR: no output path given'
            sys.exit(1)
        preoutputsopt = getopt(opts, 'preoutputs')
        addpathopt = getopt(opts, 'addpath', delete=False)
        getpathopt = getopt(opts, 'getpath', delete=False)
        if iter != 0:
            newopts['input'] = outputopt[0] + "_pre" + str(iter)
            if not (preoutputsopt and preoutputsopt[0] == 'yes'):
                newopts['delinputs'] = 'yes'
            newopts['inputformat'] = 'code'
            if addpathopt and addpathopt[0] == 'yes':  # not when == 'iter'
                newopts['addpath'] = 'no'
        if iter < itercnt - 1:
            newopts['output'] = outputopt[0] + "_pre" + str(iter + 1)
            newopts['outputformat'] = 'code'
            if getpathopt and getpathopt[0] == 'yes':  # not when == 'iter'
                newopts['getpath'] = 'no'
        if not reducer:
            newopts['numreducetasks'] = '0'
        (key, delindexes) = (None, [])
        for (index, (key, value)) in enumerate(opts):
            if newopts.has_key(key):
                delindexes.append(index)
        for delindex in reversed(delindexes):
            del opts[delindex]
        opts += newopts.iteritems()
        progopt = getopt(opts, 'prog')
        hadoopopt = getopt(opts, 'hadoop', delete=False)
        if hadoopopt:
            retval = StreamingIteration(progopt[0], opts).run()
        else:
            retval = UnixIteration(progopt[0], opts).run()
        if retval == 127:
            print >> sys.stderr, 'ERROR: Are you sure that "python" is on your path?'
        if retval != 0:
            sys.exit(retval)


def setstatus(message):
    print >> sys.stderr, 'reporter:status:%s' % message


def valwrapper(data, valfunc):
    MAX_LOGGED_BADVALUES = 500
    badvalues = 0
    for (key, value) in data:
        try:
            yield (key, valfunc(value))
        except (ValueError, TypeError):
            if badvalues <= MAX_LOGGED_BADVALUES:
                print >> sys.stderr, \
                     'WARNING: skipping bad value (%s)' % str(value)
            if os.environ.has_key('dumbo_debug'):
                raise
            badvalues += 1
            incrcounter('Dumbo', 'Bad inputs', 1)


def mapfunc_iter(data, mapfunc):
    for (key, value) in data:
        for output in mapfunc(key, value):
            yield output


def itermap(data, mapfunc, valfunc=None):
    if valfunc:
        data = valwrapper(data, valfunc)
    try:
        return mapfunc(data)
    except TypeError:
        return mapfunc_iter(data, mapfunc)


def redfunc_iter(data, redfunc):
    for (key, values) in data:
        for output in redfunc(key, values):
            yield output


def iterreduce(data, redfunc, keyfunc=None):
    data = groupby(data, itemgetter(0))
    data = ((key, (v[1] for v in values)) for key, values in data)
    if keyfunc:
        data = ((keyfunc(key), values) for key, values in data)
    try:
        return redfunc(data)
    except TypeError:
        return redfunc_iter(data, redfunc)


def itermapred(data, mapfunc, redfunc):
    return iterreduce(sorted(itermap(data, mapfunc)), redfunc)
