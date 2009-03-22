"""
Counts how many times each word occurs, using the alternative 
(more low-level) interface to mappers/reducers.
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
