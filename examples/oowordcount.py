"""
Counts how many times each non-excluded word occurs:

>>> import dumbo
>>> opts = [('excludes','excludes.txt'),('output','counts.txt')]
>>> logfile = open('log.txt','a')
>>> dumbo.start('oowordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(dumbo.loadcode(open('counts.txt')))
>>> int(output['Brian'])
6
"""

class Mapper:
    def __init__(self):
        file = open(self.params["excludes"],"r")
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
    excludes = prog.delopt("excludes")
    if excludes: prog.addopt("param","excludes="+excludes)
    prog.addopt("input","brian.txt")

if __name__ == "__main__":
    import dumbo
    dumbo.main(runner,starter)
