import heapq
from itertools import chain, imap, izip


def identitymapper(key, value):    
    yield (key, value)


def identityreducer(key, values):
    for value in values:    
        yield (key, value)    


def sumreducer(key, values):
    yield (key, sum(values))         


def sumsreducer(key, values):
    yield (key, tuple(imap(sum, izip(*values))))       


def nlargestreducer(n, key=None):                      
    def reducer(key_, values):
        yield (key_, heapq.nlargest(n, chain(*values), key=key))
    return reducer                 


def nlargestcombiner(n, key=None):
    def combiner(key_, values):
        yield (key_, heapq.nlargest(n, values, key=key))      
    return combiner    


def nsmallestreducer(n, key=None):
    def reducer(key_, values):
        yield (key_, heapq.nsmallest(n, chain(*values), key=key))
    return reducer

def nsmallestcombiner(n, key=None):
    def combiner(key_, values):                         
        yield (key_, heapq.nsmallest(n, values, key=key))
    return combiner


def statsreducer(key, values):
    columns = izip(*values)
    s0 = sum(columns.next())
    s1 = sum(columns.next())
    s2 = sum(columns.next())
    minimum = min(columns.next())
    maximum = max(columns.next())
    mean = float(s1) / s0
    std = sqrt(s0 * s2 - s1**2) / s0
    yield (key, (s0, mean, std, minimum, maximum))


def statscombiner(key, values):
    columns = izip(*((1, value, value**2, value, value) for value in values))
    s0 = sum(columns.next())
    s1 = sum(columns.next())
    s2 = sum(columns.next())
    minimum = min(columns.next())
    maximum = max(columns.next())
    yield (key, (s0, s1, s2, minimum, maximum))
