import os
from tempfile import mkstemp
from tokyo.cabinet import HDB, HDBOWRITER, HDBOCREAT

from .rawreducer import RawReducer, chunkedread, CHUNKSIZE


class TokyoCabinetFactory(object):
    """A RawReducer factory suitable to generate tokyocabinets from dumbo jobs"""

    dbcls = HDB
    flags = HDBOWRITER | HDBOCREAT
    methodname = 'putasync'
    chunksize = CHUNKSIZE

    def __init__(self):
        fd, self.fn = mkstemp('.db', 'tc-', os.getcwd())
        os.close(fd)
        self.db = self.dbcls()
        self.db.setxmsiz(0)
        self.db.open(self.fn, self.flags)
        self.add = getattr(self.db, self.methodname)

    def __call__(self, key, values):
        for value in values:
            self.add(key, value)

    def close(self):
        self.db.close()
        for chk in chunkedread(self.fn, chunksize=self.chunksize):
            yield chk
        os.unlink(self.fn)


class TokyoCabinetReducer(RawReducer):
    factory = TokyoCabinetFactory
