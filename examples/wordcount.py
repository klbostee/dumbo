"""
Counts how many times each non-excluded word occurs:

>>> import dumbo
>>> opts = [('input','brian.txt'),('output','counts.txt'),('inputformat','text')]
>>> logfile = open('log.txt','a')
>>> dumbo.start('wordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(dumbo.loadcode(open('counts.txt')))
>>> int(output['Brian'])
6
"""

def mapper(key,value):
    for word in value.split(): yield word,1

def reducer(key,values):
    yield key,sum(values)

if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper,reducer,reducer)
