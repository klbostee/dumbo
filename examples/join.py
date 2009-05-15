"""
Joins hostnames with logs and counts number of logs per host.
"""

import dumbo
from dumbo.lib import identitymapper, JoinReducer
from dumbo.decor import primary, secondary

class Reducer1(JoinReducer):
    def __init__(self):
        self.hostname = "unknown"
    def primary(self, key, values):
        self.hostname = values.next()
    def secondary(self, key, values):    
        key = self.hostname
        self.hostname = "unknown"
        for value in values:
            yield key, value

def mapper2(key, value):
    yield key, 1

def reducer2(key, values):
    yield key, sum(values)
    
def runner(job):
    mapper1 = dumbo.MultiMapper()
    mapper1.add("hostnames", primary(identitymapper))
    mapper1.add("logs", secondary(identitymapper))
    job.additer(mapper1, Reducer1)
    job.additer(mapper2, reducer2, combiner=reducer2)

if __name__ == "__main__":
    dumbo.main(runner)
