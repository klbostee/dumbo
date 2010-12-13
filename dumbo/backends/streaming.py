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
from dumbo.util import getopt, getopts, configopts, envdef, execute
from dumbo.util import findhadoop, findjar, dumpcode, dumptext


class StreamingBackend(Backend):
    
    def matches(self, opts):
        return bool(getopt(opts, 'hadoop', delete=False))
        
    def create_iteration(self, opts):
        progopt = getopt(opts, 'prog')
        return StreamingIteration(progopt[0], opts)

    def create_filesystem(self, opts):
        hadoopopt = getopt(opts, 'hadoop', delete=False)
        return StreamingFileSystem(findhadoop(hadoopopt[0]))

    def get_runinfo_class(self, opts):
        return StreamingRunInfo


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
                                        'queue',
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
        if addedopts['queue']:
            self.opts.append(('jobconf', 'mapred.job.queue.name=%s'
                              % addedopts['queue'][0]))
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
                    if os.path.exists(hadoop + "/bin/hdfs"):
                        hdfs = hadoop + "/bin/hdfs"
                    else:
                        hdfs = hadoop + "/bin/hadoop"
                    execute("%s dfs -rmr '%s'" % (hdfs, value))
        return retval


class StreamingFileSystem(FileSystem):
    
    def __init__(self, hadoop):
        self.hadoop = hadoop
        if os.path.exists(hadoop + "/bin/hdfs"):
            self.hdfs = hadoop + "/bin/hdfs"
        else:
            self.hdfs = hadoop + "/bin/hadoop"
    
    def cat(self, path, opts):
        addedopts = getopts(opts, ['libjar'], delete=False)
        streamingjar = findjar(self.hadoop, 'streaming')
        if not streamingjar:
            print >> sys.stderr, 'ERROR: Streaming jar not found'
            return 1
        hadenv = envdef('HADOOP_CLASSPATH', addedopts['libjar'],
                        shortcuts=dict(configopts('jars')))
        try:
            import typedbytes
            ls = os.popen('%s %s dfs -ls %s' % (hadenv, self.hdfs, path))
            if sum(c in path for c in ("*", "?", "{")) > 0:
                # cat each file separately when the path contains special chars
                lineparts = (line.split()[-1] for line in ls)
                subpaths = [part for part in lineparts if part.startswith("/")]
            else:
                # we still do the ls even in this case to make sure we print errors 
                subpaths = [path]
            ls.close()
            for subpath in subpaths:
                if subpath.endswith("/_logs"):
                    continue
                dumptb = os.popen('%s %s/bin/hadoop jar %s dumptb %s 2> /dev/null'
                                  % (hadenv, self.hadoop, streamingjar, subpath))
                ascodeopt = getopt(opts, 'ascode')
                if ascodeopt and ascodeopt[0] == 'yes':
                    outputs = dumpcode(typedbytes.PairedInput(dumptb))
                else:
                    outputs = dumptext(typedbytes.PairedInput(dumptb))
                for output in outputs:
                    print '\t'.join(output)
                dumptb.close()
        except IOError:
            pass  # ignore
        return 0
    
    def ls(self, path, opts):
        return execute("%s dfs -ls '%s'" % (self.hdfs, path),
                       printcmd=False)
    
    def exists(self, path, opts):
        shellcmd = "%s dfs -stat '%s' >/dev/null 2>&1"
        return 1 - int(execute(shellcmd % (self.hdfs, path), printcmd=False) == 0)
    
    def rm(self, path, opts):
        return execute("%s dfs -rmr '%s'" % (self.hdfs, path),
                       printcmd=False)
    
    def put(self, path1, path2, opts):
        return execute("%s dfs -put '%s' '%s'" % (self.hdfs, path1,
                       path2), printcmd=False)
    
    def get(self, path1, path2, opts):
        return execute("%s dfs -get '%s' '%s'" % (self.hdfs, path1,
                       path2), printcmd=False)


class StreamingRunInfo(RunInfo):

    def get_input_path(self):
        if os.environ.has_key('mapreduce_map_input_file'):
            return os.environ['mapreduce_map_input_file']
        return os.environ['map_input_file']
