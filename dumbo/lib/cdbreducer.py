import os
from tempfile import mkstemp
import cdb

from .rawreducer import RawReducer, chunkedread, CHUNKSIZE


class CDBFactory(object):
    """A RawReducer factory suitable to generate constant dbs from dumbo jobs

    For more info on constant dbs see http://cr.yp.to/cdb.html
    """
    chunksize = CHUNKSIZE

    def __init__(self):
        fd, self.fn = mkstemp('.cdb', dir=os.getcwd())
        os.close(fd)
        self.maker = cdb.cdbmake(self.fn, self.fn + '.tmp')

    def __call__(self, key, values):
        for value in values:
            self.maker.add(key, value)

    def close(self):
        self.maker.finish()
        for chk in chunkedread(self.fn, chunksize=self.chunksize):
            yield chk
        os.unlink(self.fn)


class CDBReducer(RawReducer):
    factory = CDBFactory
