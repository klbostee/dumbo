"""
Illustrates MultiMapper.
"""

from dumbo import main, MultiMapper, sumreducer

def mapper1(key, value):
    for word in value.split():
        yield ("A", word), 1

class Mapper2:
    def __call__(self, key, value):
        for word in value.split():
            yield ("B", word), 1

def runner(job):
    mapper = MultiMapper()
    mapper.add("brian", mapper1)
    mapper.add("eno", Mapper2)
    job.additer(mapper, sumreducer, combiner=sumreducer)

if __name__ == "__main__":
    main(runner)
