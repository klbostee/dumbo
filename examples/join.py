"""
Joins hostnames with logs and counts number of logs per host.
"""

def mapper1(key, value):
    key.isprimary = "hostnames" in key.body[0]
    key.body = key.body[1]
    yield key, value
    
class Reducer1:
    def __init__(self):
        self.hostname = "unknown"
    def __call__(self, key, values):
        if key.isprimary:
            self.hostname = values.next()
        else:
            key.body = self.hostname
            for value in values:
                yield key, value
            self.hostname = "unknown"

def mapper2(key, value):
    yield key, 1

def reducer2(key, values):
    yield key, sum(values)
    
def runner(job):
    job.additer(mapper1, Reducer1)
    job.additer(mapper2, reducer2, combiner=reducer2)
    
def starter(prog):
    prog.addopt("addpath", "yes")
    prog.addopt("joinkeys", "yes")

if __name__ == "__main__":
    import dumbo
    dumbo.main(runner, starter)
