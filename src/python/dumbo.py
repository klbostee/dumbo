import sys,types,os,random,re,types,subprocess
from itertools import groupby
from operator import itemgetter

def itermap(data,mapfunc):
    for key,value in data: 
        for output in mapfunc(key,value): yield output

def iterreduce(data,redfunc):
    for key,values in groupby(data,itemgetter(0)):
        for output in redfunc(key,(v[1] for v in values)): yield output

def itermapred(data,mapfunc,redfunc):
    return iterreduce(sorted(itermap(data,mapfunc)),redfunc)

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
        mapconf=None,redconf=None,mapclose=None,redclose=None,
        code_in=False,code_out=False,iter=0,newopts={}):
    if len(sys.argv) > 1 and not sys.argv[1][0] == "-":
        try:
            regex = re.compile(".*\.egg")
            for eggfile in filter(regex.match,os.listdir(".")):
                sys.path.append(eggfile)  # add eggs in current dir to path
        except: pass
        if type(mapper) == types.ClassType:
            if hasattr(mapper,'map'): mapper = mapper().map
            else: mapper = mapper()
        if type(reducer) == types.ClassType:
            if hasattr(reducer,'reduce'): reducer = reducer().reduce
            else: reducer = reducer()
        if type(combiner) == types.ClassType:
            if hasattr(combiner,'reduce'): combiner = combiner().reduce
            else: combiner = combiner()
        iterarg = 0  # default value
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
                if mapclose: mapclose()
            elif reducer: 
                if redconf: redconf()
                inputs = loadcode(line[:-1] for line in sys.stdin)
                outputs = iterreduce(inputs,reducer)
                if hasattr(reducer,"coded") and (reducer.coded or code_out): 
                    outputs = dumpcode(outputs)
                else: outputs = dumptext(outputs)
                if redclose: redclose()
            else: outputs = dumptext((line[:-1],) for line in sys.stdin)
            for output in outputs: print "\t".join(output)
    else:
        opts = parseargs(sys.argv[1:]) + [("iteration","%i" % iter)]
        if hasattr(mapper,"coded") and (mapper.coded or code_in):
            opts.append(("codein","yes"))
        if hasattr(reducer,"coded") and (reducer.coded or code_out):
            opts.append(("codeout","yes"))
        if combiner: opts.append(("combine","yes"))
        key,delindexes = None,[]
        for index,(key,value) in enumerate(opts):
            if newopts.has_key(key): delindexes.append(index)
        for delindex in reversed(delindexes): del opts[delindex]
        opts += newopts.iteritems()
        retval = execute("python -m dumbo start '%s'" % sys.argv[0],opts,printcmd=False)
        if retval == 127:
            print >>sys.stderr,'ERROR: Are you sure that "python" is on your path?'
        if retval != 0: sys.exit(retval)

class Job:
    def __init__(self): self.iters = []
    def additer(self,*args,**kwargs): self.iters.append((args,kwargs))
    def run(self):
        scratch = "dumbo-tmp-%i" % random.randint(0,sys.maxint)
        for index,(args,kwargs) in enumerate(self.iters):
            newopts = {}
            if index != 0: 
                newopts["input"] = "%s-%i" % (scratch,index-1)
                newopts["delinputs"] = "yes"
                newopts["inputformat"] = "default"
            if index != len(self.iters)-1:
                newopts["output"] = "%s-%i" % (scratch,index)
            kwargs["iter"],kwargs["newopts"] = index,newopts
            run(*args,**kwargs)

def incrcounter(group,counter,amount):
    print >>sys.stderr,"reporter:counter:%s,%s,%s" % (group,counter,amount)

def setstatus(message):
    print >>sys.stderr,"reporter:status:%s" % message

class Counter:
    def __init__(self,name,group="counters"):
        self.group = group
        self.name = name
    def incr(self,amount):
        incrcounter(self.group,self.name,amount)

def parseargs(args):
    opts,key,values = [],None,[]
    for arg in args:
        if arg[0] == "-" and len(arg) > 1:
            if key: opts.append((key," ".join(values)))
            key,values = arg[1:],[]
        else: values.append(arg)
    if key: opts.append((key," ".join(values)))
    return opts

def getopts(opts,keys,delete=True):
    askedopts = dict((key,[]) for key in keys)
    key,delindexes = None,[]
    for index,(key,value) in enumerate(opts):
        key = key.lower()
        if askedopts.has_key(key):
            askedopts[key].append(value)
            delindexes.append(index)
    if delete:
        for delindex in reversed(delindexes): del opts[delindex]
    return askedopts

def execute(cmd,opts=[],precmd="",printcmd=True,stdout=sys.stdout,stderr=sys.stderr):
    if precmd: cmd = " ".join((precmd,cmd))
    args = " ".join("-%s '%s'" % (key,value) for key,value in opts)
    if args: cmd = " ".join((cmd,args))
    if printcmd: print "EXEC:",cmd
    return system(cmd,stdout,stderr)
    
def system(cmd,stdout=sys.stdout,stderr=sys.stderr):
    if sys.version[:3] == "2.4": return os.system(cmd) / 256
    proc = subprocess.Popen(cmd,shell=True,stdout=stdout,stderr=stderr)
    return os.waitpid(proc.pid,0)[1] / 256

def findjar(hadoop,name):
    jardir = hadoop + "/build/contrib/" + name
    if not os.path.exists(jardir): jardir = hadoop + "/contrib/" + name
    if not os.path.exists(jardir): jardir = hadoop + "/contrib"
    regex = re.compile("hadoop.*" + name + "\.jar")
    try: return jardir + "/" + filter(regex.match,os.listdir(jardir))[-1]
    except: return None

def envdef(varname,files,optname=None,opts=None,commasep=False):
    path,optvals="",[]
    for file in files:
        path += file + ":"
        optvals.append(file)
    if optname and optvals:
        if not commasep: 
            for optval in optvals: opts.append((optname,optval))
        else: opts.append((optname,",".join(optvals)))
    return '%s="%s$%s"' % (varname,path,varname)

def submit(prog,opts,stdout=sys.stdout,stderr=sys.stderr):
    addedopts = getopts(opts,["libegg"],delete=False)
    pyenv = envdef("PYTHONPATH",addedopts["libegg"])
    return execute("python '%s'" % prog,opts,pyenv,stdout=stdout,stderr=stderr)

def start(prog,opts):
    addedopts = getopts(opts,["jumbo","fake"])
    if addedopts["fake"] and addedopts["fake"][0] == "yes":
        def dummysystem(*args,**kwargs): return 0
        global system
        system = dummysystem  # not very clean, but it's easy and it works...
    if not addedopts["jumbo"]:
        addedopts = getopts(opts,["python","iteration","hadoop",
                                  "codein","codeout","combine"])
        if not addedopts["python"]: python = "python"
        else: python = addedopts["python"][0]
        if not addedopts["iteration"]: iter = 0
        else: iter = int(addedopts["iteration"][0])
        if addedopts["hadoop"]: prog = prog.split("/")[-1]
        opts.append(("mapper","%s %s map %i" % (python,prog,iter)))
        opts.append(("reducer","%s %s red %i" % (python,prog,iter)))
        if not addedopts["hadoop"]: return startlocally(prog,opts)
        else: return startonstreaming(prog,opts,addedopts["hadoop"][0])
    else: return startonjumbo(prog,opts)
   
def startlocally(prog,opts):
    addedopts = getopts(opts,["input","output","mapper","reducer","libegg",
        "delinputs","cmdenv"])
    mapper,reducer = addedopts["mapper"][0],addedopts["reducer"][0]
    if (not addedopts["input"]) or (not addedopts["output"]):
        print >>sys.stderr,"ERROR: input or output not specified"
        return 1
    inputs = " ".join("'%s'" % input for input in addedopts["input"])
    output = addedopts["output"][0]
    pyenv = envdef("PYTHONPATH",addedopts["libegg"])
    cmdenv = " ".join("%s='%s'" % tuple(arg.split("=")) \
        for arg in addedopts["cmdenv"])
    retval = execute("cat %s | %s %s %s | LC_ALL=C sort | %s %s %s > '%s'" % \
        (inputs,pyenv,cmdenv,mapper,pyenv,cmdenv,reducer,output))
    if addedopts["delinputs"] and addedopts["delinputs"][0] == "yes":
        for file in addedopts["input"]: execute("rm " + file)
    return retval

def startonstreaming(prog,opts,hadoop):
    addedopts = getopts(opts,["name","delinputs","libegg","libjar","inputformat",
        "nummaptasks","numreducetasks","priority","cachefile","cachearchive"])
    opts.append(("file",prog))
    opts.append(("file",sys.argv[0]))
    if not addedopts["name"]:
        opts.append(("jobconf","mapred.job.name=" + prog.split("/")[-1]))
    else: opts.append(("jobconf","mapred.job.name=%s" % addedopts["name"][0]))
    if addedopts["nummaptasks"]: opts.append(("jobconf",
        "mapred.map.tasks=%s" % addedopts["nummaptasks"][0]))
    if addedopts["numreducetasks"]: opts.append(("numReduceTasks",
        addedopts["numreducetasks"][0]))
    if addedopts["priority"]: opts.append(("jobconf",
        "mapred.job.priority=%s" % addedopts["priority"][0]))
    if addedopts["cachefile"]: opts.append(("cacheFile",
        addedopts["cachefile"][0]))
    if addedopts["cachearchive"]: opts.append(("cacheArchive",
        addedopts["cachearchive"][0]))
    streamingjar,dumbojar = findjar(hadoop,"streaming"),findjar(hadoop,"dumbo")
    if not streamingjar:
        print >>sys.stderr,"ERROR: Streaming jar not found"
        return 1
    inputformat_shortcuts = {
        "textascode": "TextAsCodeInputFormat", 
        "sequencefileascode": "SequenceFileAsCodeInputFormat"}
    if addedopts["inputformat"] and addedopts["inputformat"][0] != 'default':
        inputformat = addedopts["inputformat"][0]
        if inputformat_shortcuts.has_key(inputformat.lower()):
            inputformat = "org.apache.hadoop.dumbo." + \
                inputformat_shortcuts[inputformat.lower()]
            addedopts["libjar"].append(dumbojar)
        opts.append(("inputformat",inputformat))
    envdef("PYTHONPATH",addedopts["libegg"],"file",opts)
    hadenv = envdef("HADOOP_CLASSPATH",addedopts["libjar"],"file",opts) 
    cmd = hadoop + "/bin/hadoop jar " + streamingjar
    retval = execute(cmd,opts,hadenv)
    if addedopts["delinputs"] and addedopts["delinputs"][0] == "yes":
        for key,value in opts:
            if key == "input":
                execute("%s/bin/hadoop dfs -rmr '%s'" % (hadoop,value))
    return retval

def startonjumbo(prog,opts):
    addedopts = getopts(opts,["hadoop","libegg","libjar","file","delinputs",
                              "inputformat","outputformat","codein","codeout"])
    if not addedopts["hadoop"]:
        print >>sys.stderr,"ERROR: Hadoop dir not specified"
        return 1
    hadoop = addedopts["hadoop"][0]
    dumbojar = findjar(hadoop,"dumbo")
    if not dumbojar:
        print >>sys.stderr,"ERROR: Dumbo jar not found"
        return 1
    if addedopts["inputformat"]:
        opts.append(("inputformat",addedopts["inputformat"][0]))
    elif addedopts["codein"]: opts.append(("inputformat","code"))
    if addedopts["outputformat"]:
        opts.append(("outputformat",addedopts["outputformat"][0]))
    elif addedopts["codeout"]: opts.append(("outputformat","code"))
    genopts = []
    envdef("PYTHONPATH",addedopts["libegg"],"files",genopts,True)
    hadenv = envdef("HADOOP_CLASSPATH",addedopts["libjar"],"libjars",genopts,True)
    fileopts = getopts(opts,["files"])["files"]
    for file in addedopts["file"]: fileopts.append(file)
    if fileopts: opts.append(("files",",".join(fileopts)))
    genargs = " ".join("-%s '%s'" % (key,value) for (key,value) in genopts)
    cmd = hadoop + "/bin/hadoop jar " + genargs + " " + dumbojar + " jumbo " + prog
    retval = execute(cmd,opts,hadenv)
    if addedopts["delinputs"] and addedopts["delinputs"][0] == "yes":
        for key,value in opts:
            if key == "input":
                execute("%s/bin/hadoop dfs -rmr '%s'" % (hadoop,value))
    return retval

def cat(path,opts):
    addedopts = getopts(opts,["hadoop","type","libjar"])
    if not addedopts["hadoop"]:
        print >>sys.stderr,"ERROR: Hadoop dir not specified"
        return 1
    hadoop = addedopts["hadoop"][0]
    dumbojar = findjar(hadoop,"dumbo")
    if not dumbojar:
        print >>sys.stderr,"ERROR: Dumbo jar not found"
        return 1
    if not addedopts["type"]: type = "text"
    else: type = addedopts["type"][0]
    hadenv = envdef("HADOOP_CLASSPATH",addedopts["libjar"])
    try:
        if type[:4] == "text": codetype = "textascode"
        else: codetype = "sequencefileascode"
        process = os.popen("%s %s/bin/hadoop jar %s catpath %s %s" % \
            (hadenv,hadoop,dumbojar,codetype,path))    
        if type[-6:] == "ascode": outputs = dumpcode(loadcode(process))
        else: outputs = dumptext(loadcode(process))
        for output in outputs: print "\t".join(output)
        process.close()
    except IOError: pass  # ignore
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usages:"
        print "  python -m dumbo submit <python program> [<options>]"
        print "  python -m dumbo start <python program> [<options>]"
        print "  python -m dumbo cat <path> [<options>]"
        sys.exit(1)
    if sys.argv[1] == "submit": retval = submit(sys.argv[2],parseargs(sys.argv[2:]))
    elif sys.argv[1] == "start": retval = start(sys.argv[2],parseargs(sys.argv[2:]))
    elif sys.argv[1] == "cat": retval = cat(sys.argv[2],parseargs(sys.argv[2:]))
    else: retval = start(sys.argv[1],parseargs(sys.argv[1:]))  # for backwards compat
    sys.exit(retval)
