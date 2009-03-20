import sys
import os
import re
import types
import resource
from itertools import groupby
from operator import itemgetter, concat

from dumbo.util import *
from dumbo.cmd import *


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

    def start(self):
        return start(self.prog, self.opts)


class Params(object):

    def get(self, name): 
        try:
            return os.environ[name]
        except KeyError:
            return None

    def __getitem__(self,key):
        return self.get(str(key))
   

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
                                        'joinkeys'])
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
            name = sys.argv[0].split('/')[-1]
        self.opts.append(('name', '%s (%s/%s)' % (name, iter + 1,
                         itercnt)))
        if not addedopts['hadoop']:
            progincmd = self.prog
        else:
            self.opts.append(('hadoop', addedopts['hadoop'][0]))
            progincmd = self.prog.split('/')[-1]
        memlim = ' 262144000'  # 250MB limit by default
        if addedopts['memlimit']:
            memlim = ' ' + addedopts['memlimit'][0]
        self.opts.append(('mapper', '%s %s map %i%s' % (python,
                         progincmd, iter, memlim)))
        self.opts.append(('reducer', '%s %s red %i%s' % (python,
                         progincmd, iter, memlim)))
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
                              'mapred.binary.partitioner.right.offset=5'))
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
                                        'python'])
        (mapper, reducer) = (addedopts['mapper'][0], addedopts['reducer'][0])
        if not addedopts['input'] or not addedopts['output']:
            print >> sys.stderr, 'ERROR: input or output not specified'
            return 1
        inputs = reduce(concat, (input.split(' ') for input in
                        addedopts['input']))
        output = addedopts['output'][0]
        pyenv = envdef('PYTHONPATH', addedopts['libegg'],
                       shortcuts=dict(configopts('eggs', self.prog)))
        cmdenv = ' '.join("%s='%s'" % tuple(arg.split('=')) for arg in
                          addedopts['cmdenv'])
        if addedopts['pv'] and addedopts['pv'][0] == 'yes':
            mpv = '| pv -s `du -b %s | cut -f 1` -cN map ' % ' '.join(inputs)
            (spv, rpv) = ('| pv -cN sort ', '| pv -cN reduce ')
        else:
            (mpv, spv, rpv) = ('', '', '')
        python = addedopts['python'][0]
        encodepipe = pyenv + ' ' + python + \
                     ' -m dumbo encodepipe -file ' + ' -file '.join(inputs)
        if addedopts['inputformat'] and addedopts['inputformat'][0] == 'code':
            encodepipe += ' -alreadycoded yes'
        if addedopts['addpath'] and addedopts['addpath'][0] == 'yes':
            encodepipe += ' -addpath yes'
        if addedopts['numreducetasks'] and addedopts['numreducetasks'][0] == '0':
            retval = execute("%s | %s %s %s %s > '%s'" % (encodepipe,
                                                          pyenv,
                                                          cmdenv,
                                                          mapper,
                                                          mpv,
                                                          output))
        else:
            retval = execute("%s | %s %s %s %s| LC_ALL=C sort %s| %s %s %s %s> '%s'"
                             % (encodepipe,
                                pyenv,
                                cmdenv,
                                mapper,
                                mpv,
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

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval
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
                                        'codewritable',
                                        'addpath',
                                        'python'])
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
        self.opts.append(('jobconf', 'stream.map.output=typedbytes'))
        self.opts.append(('jobconf', 'stream.reduce.input=typedbytes'))
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
        outputformat_shortcuts = \
            {'code': 'org.apache.hadoop.mapred.SequenceFileOutputFormat',
             'text': 'org.apache.hadoop.mapred.TextOutputFormat',
             'sequencefile': 'org.apache.hadoop.mapred.SequenceFileOutputFormat'}
        outputformat_shortcuts.update(configopts('outputformats', self.prog))
        outputformat = addedopts['outputformat'][0]
        if outputformat_shortcuts.has_key(outputformat.lower()):
            outputformat = outputformat_shortcuts[outputformat.lower()]
        self.opts.append(('outputformat', outputformat))
        if addedopts['addpath'] and addedopts['addpath'][0] == 'yes':
            self.opts.append(('cmdenv', 'dumbo_addpath=true'))
        pyenv = envdef('PYTHONPATH',
                       addedopts['libegg'],
                       'file',
                       self.opts,
                       shortcuts=dict(configopts('eggs', self.prog)),
                       quote=False,
                       trim=True)
        if pyenv:
            self.opts.append(('cmdenv', pyenv))
        hadenv = envdef('HADOOP_CLASSPATH', addedopts['libjar'], 'file', 
                        self.opts, shortcuts=dict(configopts('jars', self.prog)))
        cmd = hadoop + '/bin/hadoop jar ' + streamingjar
        retval = execute(cmd, self.opts, hadenv)
        if addedopts['delinputs'] and addedopts['delinputs'][0] == 'yes':
            for (key, value) in self.opts:
                if key == 'input':
                    execute("%s/bin/hadoop dfs -rmr '%s'" % (hadoop, value))
        return retval


def main(runner, starter=None):
    opts = parseargs(sys.argv[1:])
    starteropt = getopts(opts, ['starter'])['starter']
    opts.append(('starter', 'no'))
    if starter and not (starteropt and starteropt[0] == 'no') \
    and not (len(sys.argv) > 1 and sys.argv[1][0] != '-'):
        program = Program(sys.argv[0], opts)
        errormsg = starter(program)
        if errormsg:
            print >> sys.stderr, errormsg
            sys.exit(1)
        program.start()
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
        mapclose=None,
        redclose=None,
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
            if type(mapper) == types.ClassType:
                mappercls = type('DumboMapper', (mapper, MapRedBase), {})
                if hasattr(mappercls, 'map'):
                    mapper = mappercls().map
                else:
                    mapper = mappercls()
            if type(reducer) == types.ClassType:
                reducercls = type('DumboReducer', (reducer, MapRedBase), {})
                if hasattr(reducercls, 'reduce'):
                    reducer = reducercls().reduce
                else:
                    reducer = reducercls()
            if type(combiner) == types.ClassType:
                combinercls = type('DumboCombiner', (combiner, MapRedBase), {})
                if hasattr(combinercls, 'reduce'):
                    combiner = combinercls().reduce
                else:
                    combiner = combinercls()
            if sys.argv[1].startswith('map'):
                if os.environ.has_key('stream_map_input') and \
                os.environ['stream_map_input'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: inputting typed bytes"
                    import typedbytes
                    inputs = typedbytes.PairedInput(sys.stdin).reads()
                else:
                    inputs = loadcode(line[:-1] for line in sys.stdin)
                if mapconf:
                    mapconf()
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
                if combiner:
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
                if mapclose:
                    mapclose()
                if os.environ.has_key('stream_map_output') and \
                os.environ['stream_map_output'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: outputting typed bytes"
                    import typedbytes
                    typedbytes.PairedOutput(sys.stdout).writes(outputs)
                else:
                    for output in dumpcode(outputs):
                        print '\t'.join(output)
            elif reducer:
                if os.environ.has_key('stream_reduce_input') and \
                os.environ['stream_reduce_input'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: inputting typed bytes"
                    import typedbytes
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
                if redclose:
                    redclose()
                if os.environ.has_key('stream_reduce_output') and \
                os.environ['stream_reduce_output'].lower() == 'typedbytes':
                    print >> sys.stderr, "INFO: outputting typed bytes"
                    import typedbytes
                    typedbytes.PairedOutput(sys.stdout).writes(outputs)
                else:
                    for output in dumpcode(outputs):
                        print '\t'.join(output)
            else:
                for output in dumpcode(inputs):
                    print '\t'.join(output)
    else:
        opts = parseargs(sys.argv[1:])
        newopts = {}
        newopts['iteration'] = str(iter)
        newopts['itercount'] = str(itercnt)
        outputopt = getopt(opts, 'output', delete=False)
        if not outputopt:
            print >> sys.stderr, 'ERROR: no output path given'
            sys.exit(1)
        preoutputsopt = getopt(opts, 'preoutputs')
        if iter != 0:
            newopts['input'] = outputopt[0] + "_pre" + str(iter)
            if not (preoutputsopt and preoutputsopt[0] == 'yes'):
                newopts['delinputs'] = 'yes'
            newopts['inputformat'] = 'code'
            newopts['addpath'] = 'no'
        if iter < itercnt - 1:
            newopts['output'] = outputopt[0] + "_pre" + str(iter + 1)
            newopts['outputformat'] = 'code'
        if not reducer:
            newopts['numreducetasks'] = '0'
        (key, delindexes) = (None, [])
        for (index, (key, value)) in enumerate(opts):
            if newopts.has_key(key):
                delindexes.append(index)
        for delindex in reversed(delindexes):
            del opts[delindex]
        opts += newopts.iteritems()
        hadoopopt = getopt(opts, 'hadoop', delete=False)
        if hadoopopt:
            retval = StreamingIteration(sys.argv[0], opts).run()
        else:
            retval = UnixIteration(sys.argv[0], opts).run()
        if retval == 127:
            print >> sys.stderr, 'ERROR: Are you sure that "python" is on your path?'
        if retval != 0:
            sys.exit(retval)


def incrcounter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)


def setstatus(message):
    print >> sys.stderr, 'reporter:status:%s' % message


def valwrapper(data, valfunc):
    for (key, value) in data:
        try:
            yield (key, valfunc(value))
        except (ValueError, TypeError):
            print >> sys.stderr, \
                     'WARNING: skipping bad value (%s)' % str(value)
            if os.environ.has_key('dumbo_debug'):
                raise
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
