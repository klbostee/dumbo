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

from dumbo.util import *


def dumbo():
    if len(sys.argv) < 2:
        print 'Usages:'
        print '  dumbo start <python program> [<options>]'
        print '  dumbo cat <path> [<options>]'
        print '  dumbo ls <path> [<options>]'
        print '  dumbo exists <path> [<options>]'
        print '  dumbo rm <path> [<options>]'
        print '  dumbo put <path1> <path2> [<options>]'
        print '  dumbo get <path1> <path2> [<options>]'
        print '  dumbo encodepipe [<options>]'
        print '  dumbo decodepipe [<options>]'
        print '  dumbo doctest <python program>'
        return 1
    if sys.argv[1] == 'start':
        retval = start(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'submit':
        retval = submit(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'cat':
        retval = cat(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'ls':
        retval = ls(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'exists':
        retval = exists(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'rm':
        retval = rm(sys.argv[2], parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'put':
        retval = put(sys.argv[2], sys.argv[3], parseargs(sys.argv[3:]))
    elif sys.argv[1] == 'get':
        retval = get(sys.argv[2], sys.argv[3], parseargs(sys.argv[3:]))
    elif sys.argv[1] == 'encodepipe':
        retval = encodepipe(parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'decodepipe':
        retval = decodepipe(parseargs(sys.argv[2:]))
    elif sys.argv[1] == 'doctest':
        retval = doctest(sys.argv[2])
    elif sys.argv[1].endswith('.py'):
        retval = start(sys.argv[1], parseargs(sys.argv[1:]))
    else:
        print >> sys.stderr, 'ERROR: unknown dumbo command:', sys.argv[1]
        retval = 1
    return retval


def start(prog,
          opts,
          stdout=sys.stdout,
          stderr=sys.stderr):
    opts += configopts('common')
    opts += configopts('start')
    addedopts = getopts(opts, ['libegg'], delete=False)
    pyenv = envdef('PYTHONPATH', addedopts['libegg'],
                   shortcuts=dict(configopts('eggs', prog)),
                   extrapaths=sys.path)
    if not getopt(opts, 'prog', delete=False):
        opts.append(('prog', prog))
    if not os.path.exists(prog):
        prog = '-m ' + prog
    return execute("%s %s" % (sys.executable, prog),
                   opts,
                   pyenv,
                   stdout=stdout,
                   stderr=stderr,
                   printcmd=False)


def cat(path, opts):
    opts += configopts('common')
    opts += configopts('cat')
    addedopts = getopts(opts, ['hadoop', 'libjar'], delete=False)
    if not addedopts['hadoop']:
        return decodepipe(opts + [('file', path)])
    hadoop = findhadoop(addedopts['hadoop'][0])
    streamingjar = findjar(hadoop, 'streaming')
    if not streamingjar:
        print >> sys.stderr, 'ERROR: Streaming jar not found'
        return 1
    hadenv = envdef('HADOOP_CLASSPATH', addedopts['libjar'],
                    shortcuts=dict(configopts('jars')))
    try:
        import typedbytes
        process = os.popen('%s %s/bin/hadoop jar %s dumptb %s'
                            % (hadenv, hadoop, streamingjar, path))
        ascodeopt = getopt(opts, 'ascode')
        if ascodeopt and ascodeopt[0] == 'yes':
            outputs = dumpcode(typedbytes.PairedInput(process))
        else:
            outputs = dumptext(typedbytes.PairedInput(process))
        for output in outputs:
            print '\t'.join(output)
        process.close()
    except IOError:
        pass  # ignore
    return 0


def ls(path, opts):
    opts += configopts('common')
    opts += configopts('ls')
    addedopts = getopts(opts, ['hadoop'], delete=False)
    if not addedopts['hadoop']:
        return execute("ls -l '%s'" % path, printcmd=False)
    hadoop = findhadoop(addedopts['hadoop'][0])
    return execute("%s/bin/hadoop dfs -ls '%s'" % (hadoop, path),
                   printcmd=False)


def exists(path, opts):
    opts += configopts('common')
    opts += configopts('exists')
    addedopts = getopts(opts, ['hadoop'], delete=False)
    if not addedopts['hadoop']:
        return execute("test -e '%s'" % path, printcmd=False)
    hadoop = findhadoop(addedopts['hadoop'][0])
    shellcmd = "%s/bin/hadoop dfs -stat '%s' >/dev/null 2>&1"
    return 1 - int(execute(shellcmd % (hadoop, path), printcmd=False) == 0)


def rm(path, opts):
    opts += configopts('common')
    opts += configopts('rm')
    addedopts = getopts(opts, ['hadoop'], delete=False)
    if not addedopts['hadoop']:
        return execute("rm -rf '%s'" % path, printcmd=False)
    hadoop = findhadoop(addedopts['hadoop'][0])
    return execute("%s/bin/hadoop dfs -rmr '%s'" % (hadoop, path),
                   printcmd=False)


def put(path1, path2, opts):
    opts += configopts('common')
    opts += configopts('put')
    addedopts = getopts(opts, ['hadoop'], delete=False)
    if not addedopts['hadoop']:
        return execute("cp '%s' '%s'" % (path1, path2), printcmd=False)
    hadoop = findhadoop(addedopts['hadoop'][0])
    return execute("%s/bin/hadoop dfs -put '%s' '%s'" % (hadoop, path1,
                   path2), printcmd=False)


def get(path1, path2, opts):
    opts += configopts('common')
    opts += configopts('get')
    addedopts = getopts(opts, ['hadoop'], delete=False)
    if not addedopts['hadoop']:
        return execute("cp '%s' '%s'" % (path1, path2), printcmd=False)
    hadoop = findhadoop(addedopts['hadoop'][0])
    return execute("%s/bin/hadoop dfs -get '%s' '%s'" % (hadoop, path1,
                   path2), printcmd=False)


def encodepipe(opts=[]):
    addedopts = getopts(opts, ['addpath', 'file', 'alreadycoded'])
    if addedopts['file']:
        files = (open(f) for f in addedopts['file'])
    else:
        files = [sys.stdin]
    for file in files:
        outputs = (line[:-1] for line in file)
        if addedopts['alreadycoded']:
            outputs = loadcode(outputs)
        else:
            outputs = loadtext(outputs)
        if addedopts['addpath']:
            outputs = (((file.name, key), value) for (key, value) in outputs)
        for output in dumpcode(outputs):
            print '\t'.join(output)
        file.close()
    return 0


def decodepipe(opts=[]):
    addedopts = getopts(opts, ['file'])
    if addedopts['file']:
        files = (open(f) for f in addedopts['file'])
    else:
        files = [sys.stdin]
    for file in files:
        outputs = loadcode(line[:-1] for line in file)
        for output in dumptext(outputs):
            print '\t'.join(output)
        file.close()
        return 0


def doctest(prog):
    import doctest
    sys.path.append(os.getcwd())
    failures = doctest.testmod(__import__(prog[:-3]))
    print '%s failures in %s tests' % failures
    return int(failures > 0)


if __name__ == '__main__':
    sys.exit(dumbo())
