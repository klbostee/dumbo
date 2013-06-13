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
import types
import resource
import copy
from itertools import groupby, chain
from operator import itemgetter

from dumbo.backends import get_backend
from dumbo.util import *
from dumbo.cmd import *


class Error(Exception):
    pass


class Job(object):
    
    def __init__(self):
        self.iters = []
        self.deps = {}  # will contain last dependency for each node
        self.root = -1  # id for the job's root input
        self._argopts = parseargs(sys.argv[1:])
        self.initializer = None
    
    def additer(self, *args, **kwargs):
        kwargs.setdefault('input', len(self.iters)-1)
        input = kwargs['input']
        if type(input) == int:
            input = [input]

        self.iters.append((args, kwargs))
        iter = len(self.iters)-1

        for initer in input:
            self.deps[initer] = iter

        return iter

    def getparam(self, key, default=None):
        if key in os.environ:
            return os.environ.get(key, default)
        elif "param" in self._argopts:
            params = dict(s.split("=", 1) for s in self._argopts["param"])
            return params.get(key, default)
        else:
            return default

    def run(self):
        if len(sys.argv) > 1 and not sys.argv[1][0] == '-':
            iterarg = 0  # default value
            if len(sys.argv) > 2:
                iterarg = int(sys.argv[2])
            # for loop isn't necessary but helps for error reporting apparently
            for args, kwargs in self.iters[iterarg:iterarg+1]:
                kwargs['iter'] = iterarg
                run(*args, **kwargs)
        else:
            for _iter, (args, kwargs) in enumerate(self.iters):
                kwargs['iter'] = _iter
                opts = Options(kwargs.get('opts', []))
                opts += self._argopts
                
                # this has to be done early, while all the opts are still there
                backend = get_backend(opts)
                fs = backend.create_filesystem(opts)

                preoutputsopt = opts.pop('preoutputs')
                delinputsopt = opts.pop('delinputs')

                job_inputs = opts['input']
                if not job_inputs:
                    print >> sys.stderr, 'ERROR: No input path specified'
                    sys.exit(1)

                outputopt = opts['output']
                if not outputopt:
                    print >> sys.stderr, 'ERROR: No output path specified'
                    sys.exit(1)

                job_output = outputopt[0]

                newopts = Options()
                newopts.add('iteration', str(_iter))
                newopts.add('itercount', str(len(self.iters)))

                _input = kwargs['input']
                if type(_input) == int:
                    _input = [_input]
                if _input == [-1]:
                    kwargs['input'] = job_inputs
                    delinputs = 'yes' if 'yes' in delinputsopt and _iter == self.deps[-1] else 'no'
                    newopts.add('delinputs', delinputs)
                else:
                    if -1 in _input:
                        print >> sys.stderr, 'ERROR: Cannot mix job input with intermediate results'
                        sys.exit(1)
                    kwargs['input'] = [job_output + "_pre" + str(initer + 1) for initer in _input]
                    newopts.add('inputformat', 'code')
                    if 'yes' in opts['addpath']:  # not when == 'iter'
                        newopts.add('addpath', 'no')
                    newopts.add('delinputs', 'no')

                if _iter == len(self.iters) - 1:
                    kwargs['output'] = job_output
                else:
                    kwargs['output'] = job_output + "_pre" + str(_iter + 1)
                    newopts.add('outputformat', 'code')
                    if 'yes' in opts['getpath']:  # not when == 'iter'
                        newopts.add('getpath', 'no')

                keys = [k for k, _ in opts if k in newopts]
                opts.remove(*keys)
                opts += newopts

                kwargs['opts'] = opts

                if "initializer" not in kwargs and self.initializer is not None:
                    kwargs["initializer"] = self.initializer

                run(*args, **kwargs)

                if 'yes' not in preoutputsopt and _input != [-1]:
                    for initer in _input:
                        if _iter == self.deps[initer]:
                            fs.rm(job_output + "_pre" + str(initer + 1), opts)


class Program(object):

    def __init__(self, prog, opts=None):
        self.prog, self.opts = prog, opts or Options()
        self.started = False

    def addopt(self, key, value):
        self.opts.add(key, value)

    def delopts(self, key):
        return self.opts.pop(key)

    def delopt(self, key):
        try:
            return self.delopts(key)[0]
        except IndexError:
            return None

    def getopts(self, key):
        return self.opts[key]

    def getopt(self, key):
        try:
            return self.getopts(key)[0]
        except IndexError:
            return None

    def addparam(self, key, value):
        self.addopt("param", "%s=%s" % (key, value))

    def clone(self):
        return copy.deepcopy(self)

    def start(self):
        if self.started:
            return 0
        self.started = True
        return start(self.prog, self.opts)


def main(runner, starter=None, variator=None):
    opts = parseargs(sys.argv[1:])
    starteropt = opts.pop('starter')
    opts.add('starter', 'no')
    if starter and 'no' not in starteropt and \
            not (len(sys.argv) > 1 and sys.argv[1][0] != '-'):
        progopt = opts.pop('prog')
        progname = progopt[0] if progopt else sys.argv[0]
        program = Program(progname, opts)

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
                else:
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
            print >> sys.stderr, errormsg
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
        mapcleanup=None,
        redcleanup=None,
        combcleanup=None,
        opts=None,
        input=None,
        output=None,
        iter=0,
        initializer=None):
    if len(sys.argv) > 1 and not sys.argv[1][0] == '-':
        iterarg = 0  # default value
        if len(sys.argv) > 2:
            iterarg = int(sys.argv[2])
        memlim = None  # memory limit
        if len(sys.argv) > 3:
            memlim = int(sys.argv[3])
            resource.setrlimit(resource.RLIMIT_AS, (memlim, memlim))

        mrbase_class = loadclassname(os.environ['dumbo_mrbase_class'])
        jk_class = loadclassname(os.environ['dumbo_jk_class'])
        runinfo = loadclassname(os.environ['dumbo_runinfo_class'])()

        if iterarg == iter:
            if sys.argv[1].startswith('map'):
                if type(mapper) in (types.ClassType, type):
                    mappercls = type('DumboMapper', (mapper, mrbase_class), {})
                    mapper = mappercls()
                if hasattr(mapper, 'configure'):
                    mapconf = mapper.configure
                if hasattr(mapper, 'close'):
                    mapclose = mapper.close
                if hasattr(mapper, 'map'):
                    mapper = mapper.map
                if hasattr(mapper, 'cleanup'):
                    mapcleanup = mapper.cleanup
                if type(combiner) in (types.ClassType, type):
                    combinercls = type('DumboCombiner', (combiner, mrbase_class), {})
                    combiner = combinercls()
                if hasattr(combiner, 'configure'):
                    combconf = combiner.configure
                if hasattr(combiner, 'close'):
                    combclose = combiner.close
                if hasattr(combiner, 'reduce'):
                    combiner = combiner.reduce
                if hasattr(combiner, 'cleanup'):
                    combcleanup = combiner.cleanup
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
                    path = runinfo.get_input_path()
                    inputs = (((path, k), v) for (k, v) in inputs)
                if os.environ.has_key('dumbo_joinkeys'):
                    inputs = ((jk_class(k), v) for (k, v) in inputs)

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
                if mapcleanup:
                    outputs = chain(outputs, mapcleanup())

                # Combiner
                if combiner and type(combiner) != str:
                    if (not buffersize) and memlim:
                        buffersize = int(memlim * 0.33) / 512  # educated guess
                        print >> sys.stderr, 'INFO: buffersize =', buffersize
                    inputs = sorted(outputs, buffersize)
                    if os.environ.has_key('dumbo_joinkeys'):
                        outputs = iterreduce(inputs, combiner,
                                             keyfunc=jk_class.fromjoinkey)
                    else:
                        outputs = iterreduce(inputs, combiner)
                if os.environ.has_key('dumbo_joinkeys'):
                    outputs = ((jk.dump(), v) for (jk, v) in outputs)
                if combcleanup:
                    outputs = chain(outputs, combcleanup())

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
                # Reducer
                if type(reducer) in (types.ClassType, type):
                    reducercls = type('DumboReducer', (reducer, mrbase_class), {})
                    reducer = reducercls()
                if hasattr(reducer, 'configure'):
                    redconf = reducer.configure
                if hasattr(reducer, 'close'):
                    redclose = reducer.close
                if hasattr(reducer, 'reduce'):
                    reducer = reducer.reduce
                if hasattr(reducer, 'cleanup'):
                    redcleanup = reducer.cleanup
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
                                         keyfunc=jk_class.fromdump)
                    outputs = ((jk.body, v) for (jk, v) in outputs)
                else:
                    outputs = iterreduce(inputs, reducer)
                if redcleanup:
                    outputs = chain(outputs, redcleanup())
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
        opts = Options(opts)
        if type(mapper) == str:
            opts.add('mapper', mapper)
        elif hasattr(mapper, 'opts'):
            opts += mapper.opts
        if type(reducer) == str:
            opts.add('reducer', reducer)
        elif hasattr(reducer, 'opts'):
            opts += reducer.opts
        if type(combiner) == str:
            opts.add('combiner', combiner)
        opts += parseargs(sys.argv[1:])

        if input is not None:
            opts.remove('input')
            for infile in input:
                opts.add('input', infile)

        if output is None:
            outputopt = opts['output']
            if not outputopt:
                print >> sys.stderr, 'ERROR: No output path specified'
                sys.exit(1)
            output = outputopt[0]

        newopts = Options()
        newopts.add('output', output)
        if not reducer:
            newopts.add('numreducetasks', '0')

        keys = [k for k, _ in opts if k in newopts]
        opts.remove(*keys)
        opts += newopts

        if initializer is not None:
            initializer(opts)

        backend = get_backend(opts)

        overwriteopt = opts.pop('overwrite')
        checkoutput = 'no' not in opts.pop('checkoutput')
        fs = backend.create_filesystem(opts)
        if 'yes' in overwriteopt:
            fs.rm(output, opts)
        elif checkoutput and fs.exists(output, opts) == 0:
            print >> sys.stderr, 'ERROR: Output path exists already: %s' % output
            sys.exit(1)
        
        opts.add('cmdenv', 'dumbo_mrbase_class=' + \
                     getclassname(backend.get_mapredbase_class(opts)))
        opts.add('cmdenv', 'dumbo_jk_class=' + \
                     getclassname(backend.get_joinkey_class(opts)))
        opts.add('cmdenv', 'dumbo_runinfo_class=' + \
                     getclassname(backend.get_runinfo_class(opts)))
        retval = backend.create_iteration(opts).run()
        if retval == 127:
            print >> sys.stderr, 'ERROR: Are you sure that "python" is on your path?'
        if retval != 0:
            sys.exit(retval)


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
