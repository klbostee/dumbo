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
import sys
import re

from dumbo.backends.common import Backend, Iteration, FileSystem, RunInfo
from dumbo.util import (configopts, envdef, execute, findhadoop, findjar,
        dumpcode, dumptext, Options)


class StreamingBackend(Backend):
    
    def matches(self, opts):
        return bool(opts['hadoop'])
        
    def create_iteration(self, opts):
        return StreamingIteration(opts.pop('prog')[0], opts)

    def create_filesystem(self, opts):
        return StreamingFileSystem(findhadoop(opts['hadoop'][0]))

    def get_runinfo_class(self, opts):
        return StreamingRunInfo


class StreamingIteration(Iteration):

    def __init__(self, prog, opts):
        Iteration.__init__(self, prog, opts)
        self.opts += Options(configopts('streaming', prog, self.opts))
        hadoop_streaming = 'streaming_%s' % self.opts['hadoop'][0]
        self.opts += Options(configopts(hadoop_streaming, prog, self.opts))

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval
        opts = self.opts
        if os.path.exists(self.prog):
            opts.add('file', self.prog)

        keys = ['hadoop', 'name', 'delinputs', 'libegg', 'libjar',
            'inputformat', 'outputformat', 'nummaptasks', 'numreducetasks',
            'priority', 'queue', 'cachefile', 'cachearchive', 'file',
            'codewritable', 'addpath', 'getpath', 'python', 'streamoutput',
            'pypath', 'hadooplib']
        addedopts = opts.filter(keys)
        opts.remove(*keys)

        hadoop = findhadoop(addedopts['hadoop'][0])
        streamingjar = findjar(hadoop, 'streaming', addedopts['hadooplib'])
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
            addedopts.add('libegg', modpath)
        else:
            opts.add('file', 'file://%s' % os.path.abspath(modpath))
        opts.add('jobconf', 'stream.map.input=typedbytes')
        opts.add('jobconf', 'stream.reduce.input=typedbytes')

        if addedopts['numreducetasks'] and addedopts['numreducetasks'][0] == '0':
            opts.add('jobconf', 'stream.reduce.output=typedbytes')
            if addedopts['streamoutput']:
                id_ = addedopts['streamoutput'][0]
                opts.add('jobconf', 'stream.map.output=' + id_)
            else:
                opts.add('jobconf', 'stream.map.output=typedbytes')
        else:
            opts.add('jobconf', 'stream.map.output=typedbytes')
            if addedopts['streamoutput']:
                id_ = addedopts['streamoutput'][0]
                opts.add('jobconf', 'stream.reduce.output=' + id_)
            else:
                opts.add('jobconf', 'stream.reduce.output=typedbytes')

        progname = self.prog.split('/')[-1] if not addedopts['name'] \
                                            else addedopts['name'][0]
        opts.add('jobconf', 'mapred.job.name=%s' % progname)

        nummaptasks = addedopts['nummaptasks']
        numreducetasks = addedopts['numreducetasks']
        if nummaptasks:
            opts.add('jobconf', 'mapred.map.tasks=%s' % nummaptasks[0])
        if numreducetasks:
            opts.add('numReduceTasks', numreducetasks[0])
        if addedopts['priority']:
            opts.add('jobconf', 'mapred.job.priority=%s' % addedopts['priority'][0])
        if addedopts['queue']:
            opts.add('jobconf', 'mapred.job.queue.name=%s' % addedopts['queue'][0])

        for cachefile in addedopts['cachefile']:
            opts.add('cacheFile', cachefile)

        for cachearchive in addedopts['cachearchive']:
            opts.add('cacheArchive', cachearchive)

        for _file in addedopts['file']:
            if not '://' in _file:
                if not os.path.exists(_file):
                    raise ValueError('file "%s" does not exist' % _file)
                _file = 'file://%s' % os.path.abspath(_file)
            opts.add('file', _file)

        if not addedopts['inputformat']:
            addedopts.add('inputformat', 'auto')

        inputformat_shortcuts = {
            'code': 'org.apache.hadoop.streaming.AutoInputFormat',
            'text': 'org.apache.hadoop.mapred.TextInputFormat',
            'sequencefile': 'org.apache.hadoop.streaming.AutoInputFormat',
            'auto': 'org.apache.hadoop.streaming.AutoInputFormat'
        }
        inputformat_shortcuts.update(configopts('inputformats', self.prog))

        inputformat = addedopts['inputformat'][0]
        if inputformat.lower() in inputformat_shortcuts:
            inputformat = inputformat_shortcuts[inputformat.lower()]
        opts.add('inputformat', inputformat)

        if not addedopts['outputformat']:
            addedopts.add('outputformat', 'sequencefile')

        if addedopts['getpath'] and 'no' not in addedopts['getpath']:
            outputformat_shortcuts = {
                'code': 'fm.last.feathers.output.MultipleSequenceFiles',
                'text': 'fm.last.feathers.output.MultipleTextFiles',
                'raw': 'fm.last.feathers.output.MultipleRawFileOutputFormat',
                'sequencefile': 'fm.last.feathers.output.MultipleSequenceFiles'
            }
        else:
            outputformat_shortcuts = {
                'code': 'org.apache.hadoop.mapred.SequenceFileOutputFormat',
                'text': 'org.apache.hadoop.mapred.TextOutputFormat',
                'raw': 'fm.last.feathers.output.RawFileOutputFormat',
                'sequencefile': 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
            }
        outputformat_shortcuts.update(configopts('outputformats', self.prog))

        outputformat = addedopts['outputformat'][0]
        if outputformat.lower() in outputformat_shortcuts:
            outputformat = outputformat_shortcuts[outputformat.lower()]
        opts.add('outputformat', outputformat)

        if addedopts['addpath'] and 'no' not in addedopts['addpath']:
            opts.add('cmdenv', 'dumbo_addpath=true')

        pyenv = envdef('PYTHONPATH', addedopts['libegg'], 'file', self.opts,
            shortcuts=dict(configopts('eggs', self.prog)), quote=False, trim=True,
            extrapaths=addedopts['pypath'])
        if pyenv:
            opts.add('cmdenv', pyenv)

        hadenv = envdef('HADOOP_CLASSPATH', addedopts['libjar'], 'libjar',
            self.opts, shortcuts=dict(configopts('jars', self.prog)))

        tmpfiles = []
        for _file in opts.pop('file'):
            if _file.startswith('file://'):
                opts.add('file', _file[7:])
            else:
                tmpfiles.append(_file)
        if tmpfiles:
            opts.add('jobconf', 'tmpfiles=%s' % ','.join(tmpfiles))

        tmpjars = []
        for jar in opts.pop('libjar'):
            if jar.startswith('file://'):
                opts.add('file', jar[7:])
            else:
                tmpjars.append(jar)
        if tmpjars:
            opts.add('jobconf', 'tmpjars=%s' % ','.join(tmpjars))

        cmd = hadoop + '/bin/hadoop jar ' + streamingjar
        retval = execute(cmd, opts, hadenv)

        if 'yes' in addedopts['delinputs']:
            inputs = opts['input']
            for path in inputs:
                execute("%s/bin/hadoop fs -rmr '%s'" % (hadoop, path))
        return retval

class StreamingFileSystem(FileSystem):
    
    def __init__(self, hadoop):
        self.hadoop = hadoop
        self.hdfs = hadoop + '/bin/hadoop fs'
    
    def cat(self, path, opts):
        streamingjar = findjar(self.hadoop, 'streaming',
                               opts['hadooplib'] if 'hadooplib' in opts else None)
        if not streamingjar:
            print >> sys.stderr, 'ERROR: Streaming jar not found'
            return 1
        hadenv = envdef('HADOOP_CLASSPATH', opts['libjar'],
            shortcuts=dict(configopts('jars')))
        try:
            import typedbytes

            if "combined" in opts and opts["combined"][0] == "yes":
                subpaths = [path]
            else:
                ls = os.popen('%s %s -ls %s' % (hadenv, self.hdfs, path))
                subpaths = [line.split()[-1] for line in ls if ":" in line]
                ls.close()

            for subpath in subpaths:
                if subpath.split("/")[-1].startswith("_"):
                    continue
                dumptb = os.popen('%s %s/bin/hadoop jar %s dumptb %s 2> /dev/null'
                                  % (hadenv, self.hadoop, streamingjar, subpath))

                dump = dumpcode if 'yes' in opts['ascode'] else dumptext
                outputs = dump(typedbytes.PairedInput(dumptb))

                for output in outputs:
                    print '\t'.join(output)
                dumptb.close()
        except IOError:
            pass  # ignore
        return 0
    
    def ls(self, path, opts):
        return execute("%s -ls '%s'" % (self.hdfs, path),
                       printcmd=False)
    
    def exists(self, path, opts):
        shellcmd = "%s -stat '%s' >/dev/null 2>&1"
        return 1 - int(execute(shellcmd % (self.hdfs, path), printcmd=False) == 0)
    
    def rm(self, path, opts):
        return execute("%s -rmr '%s'" % (self.hdfs, path),
                       printcmd=False)
    
    def put(self, path1, path2, opts):
        return execute("%s -put '%s' '%s'" % (self.hdfs, path1,
                       path2), printcmd=False)
    
    def get(self, path1, path2, opts):
        return execute("%s -get '%s' '%s'" % (self.hdfs, path1,
                       path2), printcmd=False)


class StreamingRunInfo(RunInfo):

    def get_input_path(self):
        if os.environ.has_key('mapreduce_map_input_file'):
            return os.environ['mapreduce_map_input_file']
        return os.environ['map_input_file']
