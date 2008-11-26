import sys,types,os,random,re,types,subprocess,urllib
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
                if (hasattr(mapper,"coded") and mapper.coded) or code_in: 
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
                if (hasattr(reducer,"coded") and reducer.coded) or code_out: 
                    outputs = dumpcode(outputs)
                else: outputs = dumptext(outputs)
                if redclose: redclose()
            else: outputs = dumptext((line[:-1],) for line in sys.stdin)
            for output in outputs: print "\t".join(output)
    else:
        opts = parseargs(sys.argv[1:]) + [("iteration","%i" % iter)]
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
                newopts["inputformat"] = "iter"
            if index != len(self.iters)-1:
                newopts["output"] = "%s-%i" % (scratch,index)
                newopts["outputformat"] = "iter"
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

def configopts(section,prog,opts=[]):
    from ConfigParser import SafeConfigParser,NoSectionError
    defaults = {'prog': prog.split("/")[-1].split(".py",1)[0],
                'user': os.environ["USER"],"pwd": os.environ["PWD"]}
    for key,value in opts: defaults[key] = value
    parser = SafeConfigParser(defaults)
    parser.read(["/etc/dumbo.conf",os.environ["HOME"]+"/.dumborc"])
    results,excludes = [],set(defaults.iterkeys())
    try: 
        for key,value in parser.items(section):
            if not key in excludes: 
                results.append((key.split("_",1)[0],value))
    except NoSectionError: pass
    return results

def execute(cmd,opts=[],precmd="",printcmd=True,stdout=sys.stdout,stderr=sys.stderr):
    if precmd: cmd = " ".join((precmd,cmd))
    args = " ".join("-%s '%s'" % (key,value) for key,value in opts)
    if args: cmd = " ".join((cmd,args))
    if printcmd: print >>stderr,"EXEC:",cmd
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

def envdef(varname,files,optname=None,opts=None,commasep=False,shortcuts={}):
    path,optvals="",[]
    for file in files:
        if shortcuts.has_key(file.lower()): file = shortcuts[file.lower()]
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
    opts += configopts("common",prog,opts)
    addedopts = getopts(opts,["fake"])
    if addedopts["fake"] and addedopts["fake"][0] == "yes":
        def dummysystem(*args,**kwargs): return 0
        global system
        system = dummysystem  # not very clean, but it's easy and it works...
    addedopts = getopts(opts,["python","iteration","hadoop"])
    if not addedopts["python"]: python = "python"
    else: python = addedopts["python"][0]
    if not addedopts["iteration"]: iter = 0
    else: iter = int(addedopts["iteration"][0])
    if not addedopts["hadoop"]: progincmd = prog
    else: progincmd = prog.split("/")[-1]
    opts.append(("mapper","%s %s map %i" % (python,progincmd,iter)))
    opts.append(("reducer","%s %s red %i" % (python,progincmd,iter)))
    if not addedopts["hadoop"]: return startonunix(prog,opts)
    else: return startonstreaming(prog,opts,addedopts["hadoop"][0])
   
def startonunix(prog,opts):
    opts += configopts("unix",prog,opts)
    addedopts = getopts(opts,["input","output","mapper","reducer","libegg",
                              "delinputs","cmdenv","pv"])
    mapper,reducer = addedopts["mapper"][0],addedopts["reducer"][0]
    if (not addedopts["input"]) or (not addedopts["output"]):
        print >>sys.stderr,"ERROR: input or output not specified"
        return 1
    inputs = " ".join(addedopts["input"])
    output = addedopts["output"][0]
    pyenv = envdef("PYTHONPATH",addedopts["libegg"],
                   shortcuts=dict(configopts("eggs",prog)))
    cmdenv = " ".join("%s='%s'" % tuple(arg.split("=")) \
        for arg in addedopts["cmdenv"])
    if addedopts["pv"] and addedopts["pv"][0] == "yes":
        cat = "pv -cN source"
        mpv,spv,rpv = "| pv -cN map ","| pv -cN sort ","| pv -cN reduce "
    else: cat,mpv,spv,rpv = "cat","","",""
    retval = execute("%s %s | %s %s %s %s| LC_ALL=C sort %s| %s %s %s %s> '%s'" % \
        (cat,inputs,pyenv,cmdenv,mapper,mpv,spv,pyenv,cmdenv,reducer,rpv,output))
    if addedopts["delinputs"] and addedopts["delinputs"][0] == "yes":
        for file in addedopts["input"]: execute("rm " + file)
    return retval

def startonstreaming(prog,opts,hadoop):
    opts += configopts("streaming",prog,opts)
    addedopts = getopts(opts,["name","delinputs","libegg","libjar",
        "inputformat","outputformat","nummaptasks","numreducetasks",
        "priority","cachefile","cachearchive","codewritable",
        "inputascode","outputfromcode","namedcode"])
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
    dumbopkg,dumbojar_needed = "org.apache.hadoop.dumbo",False
    inputformat_shortcuts = {
        "textascode": dumbopkg + ".TextAsCodeInputFormat", 
        "sequencefileascode": dumbopkg + ".SequenceFileAsCodeInputFormat"}
    inputformat_shortcuts.update(configopts("inputformats",prog))
    if addedopts["inputformat"] and addedopts["inputformat"][0] != 'iter':
        inputformat = addedopts["inputformat"][0]
        if inputformat_shortcuts.has_key(inputformat.lower()):
            inputformat = inputformat_shortcuts[inputformat.lower()]
            dumbojar_needed = True
        opts.append(("inputformat",inputformat))
    outputformat_shortcuts = {
        "codeastext": dumbopkg + ".CodeAsTextInputFormat",
        "codeassequencefile": dumbopkg + ".CodeAsSequenceFileInputFormat"}
    outputformat_shortcuts.update(configopts("outputformats",prog))
    if addedopts["outputformat"] and addedopts["outputformat"][0] != 'iter':
        outputformat = addedopts["outputformat"][0]
        if outputformat_shortcuts.has_key(outputformat.lower()):
            outputformat = outputformat_shortcuts[outputformat.lower()]
            dumbojar_needed = True
        opts.append(("outputformat",outputformat))
    if addedopts["inputascode"] and addedopts["inputascode"][0] == 'yes':
        opt = getopts(opts,["inputformat"])["inputformat"]
        if opt: opts.append(("jobconf",
                             "dumbo.as.code.input.format.class=" + opt[0]))
        opts.append(("inputformat",dumbopkg + ".AsCodeInputFormat"))
        dumbojar_needed = True
    if addedopts["outputfromcode"] and addedopts["outputfromcode"][0] == 'yes':
        opt = getopts(opts,["outputformat"])["outputformat"]
        if opt: opts.append(("jobconf",
                             "dumbo.as.code.output.format.class=" + opt[0]))
        opts.append(("outputformat",dumbopkg + ".FromCodeOutputFormat"))
        dumbojar_needed = True
    if addedopts["codewritable"] and addedopts["codewritable"][0] == 'yes':
        opts.append(("jobconf",
                     "mapred.mapoutput.key.class=%s.CodeWritable" % dumbopkg))
        opts.append(("jobconf",
                     "mapred.mapoutput.value.class=%s.CodeWritable" % dumbopkg))
        opt = getopts(opts,["mapper"])["mapper"]
        opts.append(("jobconf","stream.map.streamprocessor=" + \
                     urllib.quote_plus(opt[0])))
        opts.append(("mapper",dumbopkg + ".CodeWritableMapper"))
        opts.append(("jobconf","dumbo.code.writable.map.class=" \
                     "org.apache.hadoop.streaming.PipeMapper"))
        dumbojar_needed = True
    if addedopts["namedcode"] and addedopts["namedcode"][0] == 'yes':
        opts.append(("jobconf", "dumbo.as.named.code=true"))
    if dumbojar_needed:
        if not dumbojar:
            print >>sys.stderr,"ERROR: Dumbo jar not found"
            return 1
        addedopts["libjar"].append(dumbojar)
    envdef("PYTHONPATH",addedopts["libegg"],"file",opts,
           shortcuts=dict(configopts("eggs",prog)))
    hadenv = envdef("HADOOP_CLASSPATH",addedopts["libjar"],"file",opts,
                    shortcuts=dict(configopts("jars",prog))) 
    cmd = hadoop + "/bin/hadoop jar " + streamingjar
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
    else:
        print >>sys.stderr,"WARNING: the command 'python -m dumbo <prog>' is " \
                           "deprecated, use 'python <prog>' instead" 
        retval = start(sys.argv[1],parseargs(sys.argv[1:]))  # for backwards compat
    sys.exit(retval)
