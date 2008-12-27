"""
Counts how many times each non-excluded word occurs:

>>> import dumbo
>>> opts = [('who','Brian'),('output','counts.txt')]
>>> logfile = open('log.txt','a')
>>> dumbo.start('oowordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(dumbo.loadcode(open('counts.txt')))
>>> int(output['Brian'])
6
"""

class Mapper:
    def __init__(self):
        file = open("excludes.txt","r")
        self.excludes = set(line.strip() for line in file)
        file.close()
    def __call__(self,key,value):
        for word in value.split(): 
            if not (word in self.excludes): yield word,1

def reducer(key,values):
    yield key,sum(values)

def runner(job):
    job.additer(Mapper,reducer,reducer)

def starter(prog):
    who = prog.delopt("who")
    if not who: return "'who' not specified"
    prog.addopt("input",who.lower() + ".txt")

if __name__ == "__main__":
    import dumbo
    dumbo.main(runner,starter)
