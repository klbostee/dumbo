def mapper1(key,value):
    for word in value.split(): yield word,1

def reducer1(key,values):
    if sum(values) > 1: yield key,

def mapper2(key,value):
    for letter in value: yield letter,1

def reducer2(key,values):
    yield key,sum(values)

if __name__ == "__main__":
    import dumbo
    job = dumbo.Job()
    job.additer(mapper1,reducer1)
    job.additer(mapper2,reducer2,reducer2)
    job.run()
