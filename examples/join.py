"""
Joins hostnames with logs and counts number of logs per host.
"""

import dumbo
from dumbo.lib import JoinReducer
from dumbo.decor import primary, secondary

def mapper1(key, value):
    yield value.split("\t", 1) 

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
    multimapper = dumbo.MultiMapper()
    multimapper.add("hostnames", primary(mapper1))
    multimapper.add("logs", secondary(mapper1))
    job.additer(multimapper, Reducer1)
    job.additer(mapper2, reducer2, combiner=reducer2)

if __name__ == "__main__":
    dumbo.main(runner)
