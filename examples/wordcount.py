"""
Counts how many times each non-excluded word occurs:

>>> import sys
>>> from dumbo import cmd, util
>>> opts = [('python', sys.executable)]
>>> opts += [('input','brian.txt'),('output','counts.txt')]
>>> logfile = open('log.txt','a')
>>> cmd.start('wordcount.py',opts,stdout=logfile,stderr=logfile)
0
>>> output = dict(util.loadcode(open('counts.txt')))
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
