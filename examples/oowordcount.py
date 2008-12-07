"""
Counts how many times each non-excluded word occurs:

>>> import dumbo
>>> opts = [('input','brian.txt'),('output','counts.txt'),('inputformat','text')]
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

if __name__ == "__main__":
    import dumbo
    dumbo.run(Mapper,reducer,reducer)
