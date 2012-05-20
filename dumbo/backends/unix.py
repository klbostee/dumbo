'''
Created on 26 Jul 2010

@author: klaas
'''

import sys
import operator

from dumbo.backends.common import Backend, Iteration, FileSystem
from dumbo.util import configopts, envdef, execute, Options
from dumbo.cmd import decodepipe


class UnixBackend(Backend):

    def matches(self, opts):
        return True  # always matches, but it's last in the list

    def create_iteration(self, opts):
        return UnixIteration(opts['prog'][0], opts)

    def create_filesystem(self, opts):
        return UnixFileSystem()


class UnixIteration(Iteration):

    def __init__(self, prog, opts):
        Iteration.__init__(self, prog, opts)
        self.opts += Options(configopts('unix', prog, self.opts))

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval

        opts = self.opts
        keys = ['input', 'output', 'mapper', 'reducer', 'libegg', 'delinputs',
            'cmdenv', 'pv', 'addpath', 'inputformat', 'outputformat',
            'numreducetasks', 'python', 'pypath', 'sorttmpdir', 'sortbufsize']
        addedopts = opts.filter(keys)
        opts.remove(*keys)

        mapper, reducer = addedopts['mapper'][0], addedopts['reducer'][0]
        if not addedopts['input'] or not addedopts['output']:
            print >> sys.stderr, 'ERROR: input or output not specified'
            return 1

        _inputs = addedopts['input']
        _output = addedopts['output']

        inputs = reduce(operator.concat, (inp.split(' ') for inp in _inputs))
        output = _output[0]

        pyenv = envdef('PYTHONPATH', addedopts['libegg'],
            shortcuts=dict(configopts('eggs', self.prog)), 
            extrapaths=addedopts['pypath'])
        cmdenv = ' '.join("%s='%s'" % tuple(arg.split('=')) for arg in
                          addedopts['cmdenv'])

        if 'yes' in addedopts['pv']:
            mpv = '| pv -s `du -b %s | cut -f 1` -cN map ' % ' '.join(inputs)
            (spv, rpv) = ('| pv -cN sort ', '| pv -cN reduce ')
        else:
            (mpv, spv, rpv) = ('', '', '')

        sorttmpdir, sortbufsize = '', ''
        if addedopts['sorttmpdir']:
            sorttmpdir = "-T %s" % addedopts['sorttmpdir'][0]
        if addedopts['sortbufsize']:
            sortbufsize = "-S %s" % addedopts['sortbufsize'][0]

        python = addedopts['python'][0]
        encodepipe = pyenv + ' ' + python + \
                     ' -m dumbo.cmd encodepipe -file ' + ' -file '.join(inputs)

        if 'code' in addedopts['inputformat']:
            encodepipe += ' -alreadycoded yes'
        if addedopts['addpath'] and 'no' not in addedopts['addpath']:
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

        if 'yes' in addedopts['delinputs']:
            for _file in addedopts['input']:
                execute('rm ' + _file)
        return retval


class UnixFileSystem(FileSystem):

    def cat(self, path, opts):
        opts = Options(opts)
        opts.add('file', path)
        return decodepipe(opts)

    def ls(self, path, opts):
        return execute("ls -l '%s'" % path, printcmd=False)

    def exists(self, path, opts):
        return execute("test -e '%s'" % path, printcmd=False)

    def rm(self, path, opts):
        return execute("rm -rf '%s'" % path, printcmd=False)

    def put(self, path1, path2, opts):
        return execute("cp '%s' '%s'" % (path1, path2), printcmd=False)

    def get(self, path1, path2, opts):
        return execute("cp '%s' '%s'" % (path1, path2), printcmd=False)
