"""A reducer base class to output one or multiple files in its raw fileformat"""
from itertools import groupby
from dumbo.util import Options

class RawReducer(object):
    """Reducer to generate outputs in raw file format"""

    multipleoutput = False
    singleopts = Options([
        ('outputformat', 'raw'),
    ])
    multipleopts = Options([
        ('getpath', 'yes'),
        ('outputformat', 'raw'),
        ('partitioner', 'fm.last.feathers.partition.Prefix'),
        ('jobconf', 'feathers.output.filename.strippart=true'),
    ])

    def __init__(self, factory=None, multipleoutput=None):
        if factory:
            self.factory = factory
        if multipleoutput is not None:
            self.multipleoutput = multipleoutput
        self.opts = self.multipleopts if self.multipleoutput else self.singleopts

    def __call__(self, data):
        if not self.multipleoutput:
            data = (((None, key), values) for key, values in data)

        proc = self.factory()
        for path, group in groupby(data, lambda x:x[0][0]):
            proc = self.factory()
            for (_, key), values in group:
                for chk in proc(key, values) or ():
                    yield path, chk

            close = getattr(proc, 'close', tuple)
            for chk in close() or ():
                yield path, chk

    def factory(self):
        """Processor factory used to consume reducer input (one per path on multiple outputs)

        Must return a callable (aka processor) that accepts two parameters
        "key" and "values", and returns an iterable of strings or None.

        The processor may have a close() method that returns an iterable of
        strings or None. This method is called when the last key-values pair
        for a path is seen.

        """
        return lambda key, values: values

CHUNKSIZE = 2*1024*1024 # default chunk size to read a file
def chunkedread(filename_or_fileobj, chunksize=CHUNKSIZE):
    """Returns a generator that reads a file in chunks"""
    if hasattr(filename_or_fileobj, 'read'):
        fileobj = filename_or_fileobj
        needclose = False
    else:
        fileobj = open(filename_or_fileobj, 'rb')
        needclose = True

    try:
        content = fileobj.read(chunksize)
        while content:
            yield content
            content = fileobj.read(chunksize)
    finally:
        if needclose:
            fileobj.close()

