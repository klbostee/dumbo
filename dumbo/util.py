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
import subprocess


def sorted(iterable, piecesize=None, key=None, reverse=False):
    if not piecesize:
        values = list(iterable)
        values.sort(key=key, reverse=reverse)
        for value in values:
            yield value
    else:  # piecewise sorted
        sequence = iter(iterable)
        while True:
            values = list(sequence.next() for i in xrange(piecesize))
            values.sort(key=key, reverse=reverse)
            for value in values:
                yield value
            if len(values) < piecesize:
                break

def incrcounter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)
            
            
def dumpcode(outputs):
    for output in outputs:
        yield map(repr, output)


def loadcode(inputs):
    for input in inputs:
        try:
            yield map(eval, input.split('\t', 1))
        except (ValueError, TypeError):
            print >> sys.stderr, 'WARNING: skipping bad input (%s)' % input
            if os.environ.has_key('dumbo_debug'):
                raise
            incrcounter('Dumbo', 'Bad inputs', 1)


def dumptext(outputs):
    newoutput = []
    for output in outputs:
        for item in output:
            if not hasattr(item, '__iter__'):
                newoutput.append(str(item))
            else:
                newoutput.append('\t'.join(map(str, item)))
        yield newoutput
        del newoutput[:]


def loadtext(inputs):
    offset = 0
    for input in inputs:
        yield (offset, input)
        offset += len(input)


def parseargs(args):
    (opts, key, values) = ([], None, [])
    for arg in args:
        if arg[0] == '-' and len(arg) > 1:
            if key:
                opts.append((key, ' '.join(values)))
            (key, values) = (arg[1:], [])
        else:
            values.append(arg)
    if key:
        opts.append((key, ' '.join(values)))
    return opts


def getopts(opts, keys, delete=True):
    askedopts = dict((key, []) for key in keys)
    (key, delindexes) = (None, [])
    for (index, (key, value)) in enumerate(opts):
        key = key.lower()
        if askedopts.has_key(key):
            askedopts[key].append(value)
            delindexes.append(index)
    if delete:
        for delindex in reversed(delindexes):
            del opts[delindex]
    return askedopts


def getopt(opts, key, delete=True):
    return getopts(opts, [key], delete)[key]


def configopts(section, prog=None, opts=[]):
    from ConfigParser import SafeConfigParser, NoSectionError
    if prog:
        prog = prog.split('/')[-1]
        prog = prog[:-3] if prog.endswith('.py') else prog
        defaults = {'prog': prog}
    else:
        defaults = {}
    try:
        defaults.update([('user', os.environ['USER']), ('pwd',
                        os.environ['PWD'])])
    except KeyError:
        pass
    for (key, value) in opts:
        defaults[key.lower()] = value
    parser = SafeConfigParser(defaults)
    parser.read(['/etc/dumbo.conf', os.environ['HOME'] + '/.dumborc'])
    (results, excludes) = ([], set(defaults.iterkeys()))
    try:
        for (key, value) in parser.items(section):
            if not key.lower() in excludes:
                results.append((key.split('_', 1)[0], value))
    except NoSectionError:
        pass
    return results


def execute(cmd,
            opts=[],
            precmd='',
            printcmd=True,
            stdout=sys.stdout,
            stderr=sys.stderr):
    if precmd:
        cmd = ' '.join((precmd, cmd))
    args = ' '.join("-%s '%s'" % (key, value) for (key, value) in opts)
    if args:
        cmd = ' '.join((cmd, args))
    if printcmd:
        print >> stderr, 'EXEC:', cmd
    return system(cmd, stdout, stderr)


def system(cmd, stdout=sys.stdout, stderr=sys.stderr):
    if sys.version[:3] == '2.4':
        return os.system(cmd) / 256
    proc = subprocess.Popen(cmd, shell=True, stdout=stdout,
                            stderr=stderr)
    return os.waitpid(proc.pid, 0)[1] / 256


def findhadoop(optval):
    (hadoop, hadoop_shortcuts) = (optval, dict(configopts('hadoops')))
    if hadoop_shortcuts.has_key(hadoop.lower()):
        hadoop = hadoop_shortcuts[hadoop.lower()]
    if not os.path.exists(hadoop):
        print >> sys.stderr, 'ERROR: directory %s does not exist' % hadoop
        sys.exit(1)
    return hadoop


def findjar(hadoop, name):
    """Tries to find a JAR file based on given
    hadoop home directory and component base name (e.g 'streaming')"""

    jardir_candidates = filter(os.path.exists, [
        os.path.join(hadoop, 'build', 'contrib', name),
        os.path.join(hadoop, 'contrib', name, 'lib'),
        os.path.join(hadoop, 'contrib', name),
        os.path.join(hadoop, 'contrib')
    ])
    regex = re.compile(r'hadoop.*%s\.jar' % name)

    for jardir in jardir_candidates:
        matches = filter(regex.match, os.listdir(jardir))
        if matches:
            return os.path.join(jardir, matches[-1])

    return None


def envdef(varname,
           files,
           optname=None,
           opts=None,
           commasep=False,
           shortcuts={},
           quote=True,
           trim=False,
           extrapaths=None):
    (pathvals, optvals) = ([], [])
    for file in files:
        if shortcuts.has_key(file.lower()):
            file = shortcuts[file.lower()]
        if file.startswith('path://'):
            pathvals.append(file[7:])
        else:
            if not '://' in file:
                if not os.path.exists(file):
                    raise ValueError('file "' + file + '" does not exist')
                file = 'file://' + os.path.abspath(file)
            if not trim:
                pathvals.append(file.split('://', 1)[1])
            else:
                pathvals.append(file.split('/')[-1])
            optvals.append(file)
    if extrapaths:
        pathvals.extend(extrapaths)
    path = ':'.join(pathvals)
    if optname and optvals:
        if not commasep:
            for optval in optvals:
                opts.append((optname, optval))
        else:
            opts.append((optname, ','.join(optvals)))
    if not quote:
        return '%s=%s' % (varname, path)
    else:
        return '%s="%s"' % (varname, ':'.join((path, '$' + varname)))

