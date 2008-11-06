"""
Counts how many times each non-excluded word occurs:

>>> import dumbo

>>> input = dumbo.loadtext(open('brian.txt'))
>>> output = dict(dumbo.itermapred(input,Mapper(),reducer))
>>> output['Brian']
6

>>> opts = [('input','brian.txt'),('output','counts.txt')]
>>> logfile = open('log.txt','a')
>>> dumbo.submit('oowordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(line[:-1].split('\\t') for line in open('counts.txt'))
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
