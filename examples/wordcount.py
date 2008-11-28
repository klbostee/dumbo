"""
Counts how many times each non-excluded word occurs:

>>> import dumbo
>>> opts = [('input','brian.txt'),('output','counts.txt'),('inputformat','text')]
>>> logfile = open('log.txt','a')
>>> dumbo.submit('wordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(dumbo.loadcode(open('counts.txt')))
>>> int(output['Brian'])
6
"""

excludes = None

def loadexcludes():
    global excludes
    file = open("excludes.txt","r")
    excludes = set(line.strip() for line in file)
    file.close()

def mapper(key,value):
    for word in value.split(): 
        if not (word in excludes): yield word,1

def reducer(key,values):
    yield key,sum(values)

if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper,reducer,reducer,mapconf=loadexcludes)
