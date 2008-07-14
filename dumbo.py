import sys,types,os,random
from itertools import groupby
from operator import itemgetter

def itermap(data,mapper):
    for key,value in data: 
        for output in mapper(key,value): yield output

def iterreduce(data,reducer):
    for key,values in groupby(data,itemgetter(0)):
        for output in reducer(key,(v[1] for v in values)): yield output

def dumpcode(outputs):
    for output in outputs: yield map(repr,output)

def loadcode(inputs):
    for input in inputs: yield map(eval,input.split("\t",1))

def dumptext(outputs):
    newoutput = []
    for output in outputs:
        for item in output:
            if not hasattr(item,"__iter__"): newoutput.append(str(item))
            else: newoutput.append("\t".join(map(str,item)))
        yield newoutput
        del newoutput[:]

def loadtext(inputs):
    for input in inputs: yield (None,input)

def run(mapper,reducer=None,combiner=None,
        mapconf=None,redconf=None,code_in=False,code_out=False,
        iter=0,newopts={}):
    if len(sys.argv) > 1 and not sys.argv[1][0] == "-":
        iterarg = 0 # default value
        if len(sys.argv) > 2: iterarg = int(sys.argv[2])
        if iterarg == iter:
            if sys.argv[1].startswith("map"):
                if mapconf: mapconf()
                if hasattr(mapper,"coded") and (mapper.coded or code_in): 
                    inputs = loadcode(line[:-1] for line in sys.stdin)
                else: inputs = loadtext(line[:-1] for line in sys.stdin)
                outputs = itermap(inputs,mapper)
                if combiner: outputs = iterreduce(sorted(outputs),combiner)
                if reducer or code_out: outputs = dumpcode(outputs)
                else: outputs = dumptext(outputs)
            elif reducer: 
                if redconf: redconf()
                inputs = loadcode(line[:-1] for line in sys.stdin)
                outputs = iterreduce(inputs,reducer)
                if hasattr(reducer,"coded") and (reducer.coded or code_out): 
                    outputs = dumpcode(outputs)
                else: outputs = dumptext(outputs)
            else: outputs = dumptext((line[:-1],) for line in sys.stdin)
            for output in outputs: print "\t".join(output)
    else:
        opts = parseargs(sys.argv[1:]) + [("iteration","%i" % iter)]
        key,delindexes = None,[]
        for index,(key,value) in enumerate(opts):
            if newopts.has_key(key): delindexes.append(index)
        for delindex in reversed(delindexes): del opts[delindex]
        opts += newopts.iteritems()
        retval = submit(sys.argv[0],opts)
        if retval != 0: sys.exit(retval)

class Job:
    def __init__(self): self.iters = []
    def additer(self,*args,**kwargs): self.iters.append((args,kwargs))
    def run(self):
        scratch = "tmp/%s-%i" % (sys.argv[0],random.randint(0,sys.maxint))
        for index,(args,kwargs) in enumerate(self.iters):
            newopts = {}
            if index != 0: 
                newopts["input"] = "%s-%i" % (scratch,index-1)
                newopts["delinputs"] = "yes"
            if index != len(self.iters)-1:
                newopts["output"] = "%s-%i" % (scratch,index)
            kwargs["iter"],kwargs["newopts"] = index,newopts
            run(*args,**kwargs)

def parseargs(args):
    opts,key,values = [],None,[]
    for arg in args:
        if arg[0] == "-" and len(arg) > 1:
            if key: opts.append((key," ".join(values)))
            key,values = arg[1:],[]
        else: values.append(arg)
    if key: opts.append((key," ".join(values)))
    return opts

def submit(prog,opts):
    args = " ".join("-%s '%s'" % (key,value) for key,value in opts)
    cmd = "python -m dumbo '%s' %s" % (prog,args)
    print "Command:",cmd
    return os.system(cmd)

def stream(prog,opts):
    def find_arg(key,description,default):
        matches = map(itemgetter(1),filter(lambda x: x[0]==key,opts))
        if matches: 
            for match in matches: print "%s: %s" % (description,match)
        else:
            if default: 
                opts.append((key,default))
                print "%s: %s" % (description,default)
            else:
                input = None
                while not input: input = raw_input("%s: " % description)
                opts.append((key,input))
    find_arg("hadoop","Hadoop home",None)
    find_arg("input","Input path",None)
    find_arg("output","Output path",None)
    find_arg("name","Job name",prog)
    find_arg("python","Python command","python")
    find_arg("iteration","Iteration","0")
    find_arg("delinputs","Delete inputs","no")

    added_opts_keys = ["hadoop","name","python","iteration","delinputs"]
    added_opts = dict((key,None) for key in added_opts_keys)
    key,delindexes = None,[]
    for index,(key,value) in enumerate(opts):
        if added_opts.has_key(key): 
            added_opts[key] = value
            delindexes.append(index)
    for delindex in reversed(delindexes): del opts[delindex] 

    python,iter = added_opts["python"],int(added_opts["iteration"])
    opts.append(("mapper","%s %s map %i" % (python,prog.split("/")[-1],iter)))
    opts.append(("reducer","%s %s red %i" % (python,prog.split("/")[-1],iter)))
    opts.append(("file",prog))
    opts.append(("file",sys.argv[0]))
    opts.append(("jobconf","mapred.job.name=%s" % added_opts["name"]))

    hadoop = added_opts["hadoop"]
    jardir = hadoop + "/contrib/streaming"
    if not os.path.exists(jardir): jardir = hadoop + "/contrib"
    if not os.path.exists(jardir): jardir = hadoop + "/build/contrib/streaming"
    if not os.path.exists(jardir): jardir = hadoop + "/build/contrib"
    cmd = "%s/bin/hadoop jar %s/hadoop*streaming.jar" % (hadoop,jardir)
    args = " ".join("-%s '%s'" % (key,value) for key,value in opts)
    retval = os.system(" ".join((cmd,args)))
    if added_opts["delinputs"] == "yes":
        for key,value in opts:
            if key == "input": 
                os.system("%s/bin/hadoop dfs -rmr %s" % (hadoop,value))
    sys.exit(retval)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: python -m dumbo <python program> [<options>]"
        sys.exit(1)
    stream(sys.argv[1],parseargs(sys.argv[1:]))
