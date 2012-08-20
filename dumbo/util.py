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
import warnings
from collections import defaultdict

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


def setstatus(message):
    print >> sys.stderr, 'reporter:status:%s' % message


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

class Options(object):
    """
    Class that represents a set of options. A key can hold
    more than one value and keys are stored in lowercase.
    The order of the values is preserved per key.
    """

    def __init__(self, seq=None, **kwargs):
        """
        Initialize the option object

        Args:
         - seq: a list of (key, value) pairs 
        """
        self._opts = defaultdict(list)  # not sets since order is important
        options = seq or []
        for k, v in kwargs.iteritems():
            self.add(k, v)
        for k, v in options:
            self.add(k, v)

    def add(self, key, value):
        optlist = self._opts[key]
        try:
            optlist.remove(value)
        except ValueError:
            pass  # ignore "not in list" error
        optlist.append(value)

    def update(self, key, values):
        for value in values:
            self.add(key, value)

    def get(self, key):
        if key not in self._opts:
            return []
        return list(self._opts[key])

    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        return self.remove(key)

    def __iadd__(self, opts):
        if isinstance(opts, Options):
            for k, vs in opts._opts.items():
                self.update(k, vs)
            return self
        elif isinstance(opts, (list, tuple, set)):
            for k, v in opts:
                self.add(k, v)
            return self
        else:
            raise ValueError('Invalid opts type. Must be an iterable of (key, value)')

    def __iter__(self):
        return iter(self.allopts())

    def __contains__(self, key):
        return key in self._opts

    def __len__(self):
        return len(self.allopts())

    def __bool__(self):
        return bool(self._opts)

    def filter(self, keys):
        return Options(seq=[(k, v) for k, v in self.allopts() if k in keys])

    def allopts(self):
        """Return a list with all the options in the form of (key, value)"""
        return [(k, v) for k, vs in self._opts.items() for v in vs]

    def to_dict(self):
        return dict((k, list(vs)) for k, vs in self._opts.items())

    def __str__(self):
        ps = self.allopts()
        return "Options(%s)" % (', '.join('%s="%s"' % (k, v) for k, v in ps))
    __repr__ = __str__

    def remove(self, *keys):
        opts = self._opts
        for k in keys:
            if k in opts:
                del opts[k]

    def pop(self, key, default=None):
        return list(self._opts.pop(key, default or ()))

def parseargs(args):
    (opts, key, values) = (Options(), None, [])
    for arg in args:
        if arg[0] == '-' and len(arg) > 1:
            if key:
                opts.add(key, ' '.join(values))
            (key, values) = (arg[1:], [])
        else:
            values.append(arg)
    if key:
        opts.add(key, ' '.join(values))
    return opts

def getopts(opts, keys, delete=True):
    warnings.warn("getopts will be deprecated. use dumbo.util.Options", 
            DeprecationWarning)
    o = Options(opts)
    result = o.filter(keys).to_dict()
    if delete:
        for k in keys:
            for v in o.get(k):
                opts.remove((k, v))
            if k in o:
                o.remove(k)
    return result

def getopt(opts, key, delete=True):
    warnings.warn("getopts will be deprecated. use dumbo.util.Options", 
            DeprecationWarning)
    o = Options(opts)
    if key not in o:
        return []
    values = o.get(key)
    if delete:
        for val in values:
            opts.remove((key, val))
        o.remove(key)
    return values

def configopts(section, prog=None, opts=None):
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
    for (key, value) in opts or Options():
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
            opts=None,
            precmd='',
            printcmd=True,
            stdout=sys.stdout,
            stderr=sys.stderr):
    if precmd:
        cmd = ' '.join((precmd, cmd))
    opts = opts or Options()
    args = ' '.join("-%s '%s'" % (key, value) for (key, value) in opts)
    if args:
        cmd = ' '.join((cmd, args))
    if printcmd:
        print >> stderr, 'EXEC:', cmd
    return system(cmd, stdout, stderr)


def system(cmd, stdout=sys.stdout, stderr=sys.stderr):
    if sys.version[:3] == '2.4':
        return os.system(cmd)
    proc = subprocess.Popen(cmd, shell=True, stdout=stdout,
                            stderr=stderr)
    return proc.wait()


def findhadoop(optval):
    (hadoop, hadoop_shortcuts) = (optval, dict(configopts('hadoops')))
    if hadoop_shortcuts.has_key(hadoop.lower()):
        hadoop = hadoop_shortcuts[hadoop.lower()]
    if not os.path.exists(hadoop):
        print >> sys.stderr, 'ERROR: directory %s does not exist' % hadoop
        sys.exit(1)
    return hadoop


def findjar(hadoop, name, libdirs=None):
    """Tries to find a JAR file based on given
    hadoop home directory and component base name (e.g 'streaming')"""

    searchdirs = [hadoop]
    if libdirs:
        for libdir in libdirs:
            if os.path.exists(libdir):
                searchdirs.append(libdir)

    jardir_candidates = []
    for searchdir in searchdirs:
        jardir_candidates += filter(os.path.exists, [
            os.path.join(searchdir, 'mapred', 'build', 'contrib', name),
            os.path.join(searchdir, 'build', 'contrib', name),
            os.path.join(searchdir, 'mapred', 'contrib', name, 'lib'),
            os.path.join(searchdir, 'contrib', name, 'lib'),
            os.path.join(searchdir, 'mapred', 'contrib', name),
            os.path.join(searchdir, 'contrib', name),
            os.path.join(searchdir, 'mapred', 'contrib'),
            os.path.join(searchdir, 'contrib'),
            searchdir
        ])

    regex = re.compile(r'hadoop.*%s.*\.jar' % name)

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
        opts = opts or Options()
        if not commasep:
            for optval in optvals:
                opts.add(optname, optval)
        else:
            opts.add(optname, ','.join(optvals))
    if not quote:
        return '%s=%s' % (varname, path)
    else:
        return '%s="%s"' % (varname, ':'.join((path, '$' + varname)))


def getclassname(cls):
    return cls.__module__ + "." + cls.__name__


def loadclassname(name):
    modname, _, clsname = name.rpartition(".")
    mod = __import__(modname, fromlist=[clsname])
    return getattr(mod, clsname)
