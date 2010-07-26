'''
Created on 26 Jul 2010

@author: klaas
'''

import sys
import operator

from dumbo.backends.common import Backend, Iteration, FileSystem
from dumbo.util import getopt, getopts, configopts, envdef, execute
from dumbo.cmd import decodepipe


class UnixBackend(Backend):
    
    def matches(self, opts):
        return True  # always matches, but it's last in the list
        
    def create_iteration(self, opts):
        progopt = getopt(opts, 'prog')
        return UnixIteration(progopt[0], opts)

    def create_filesystem(self, opts):
        return UnixFileSystem()


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
        inputs = reduce(operator.concat, (input.split(' ') for input in
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
    

class UnixFileSystem(FileSystem):
    
    def cat(self, path, opts):
        return decodepipe(opts + [('file', path)])
    
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
