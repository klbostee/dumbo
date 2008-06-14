import dumbo

def dotest(data):
    print data
    dumped = dumbo.dumpcode([[data]]).next()[0]
    print dumped
    print dumbo.loadcode([dumped]).next()[0]==data
dotest((1,(2,(3,4)),5))
dotest([(1,2),3,4])
dotest(tuple())
dotest([])
dotest([[],1])
dotest("{\"key\": 1}")

