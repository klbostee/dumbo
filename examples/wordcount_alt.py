"""
Counts how many times each word occurs, using the alternative 
(more low-level) interface to mappers/reducers:

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

def mapper(data):
    for key, value in data:
        for word in value.split(): yield word,1

def reducer(data):
    for key, values in data:
        yield key,sum(values)

if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper,reducer,reducer)
