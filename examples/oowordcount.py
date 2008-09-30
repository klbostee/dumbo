class Mapper:
    def __init__(self):
        file = open("excludes.txt","r")
        self.excludes = set(line.strip() for line in file)
        file.close()
    def map(self,key,value):
        for word in value.split(): 
            if not (word in self.excludes): yield word,1

def reducer(key,values):
    yield key,sum(values)

if __name__ == "__main__":
    import dumbo
    dumbo.run(Mapper,reducer,reducer)
