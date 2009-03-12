import unittest
from dumbo import util

class TestCoding(unittest.TestCase):

    def dotest(self,data):
        dumped = "\t".join(util.dumpcode([("dummy",data)]).next())
        self.assertEqual(util.loadcode([dumped]).next()[1],data)

    def testtuple(self):
        self.dotest(tuple())
        self.dotest((1,(2,(3,4)),5))

    def testlist(self):
        self.dotest([])
        self.dotest([(1,2),3,4])

    def testmap(self):
        self.dotest({})
        self.dotest({'key': 'value'})

    def teststring(self):
        self.dotest("{'key': 1}")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCoding)
    unittest.TextTestRunner(verbosity=2).run(suite)
