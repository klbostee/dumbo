import dumbo,unittest

class TestCoding(unittest.TestCase):

    def dotest(self,data):
        dumped = dumbo.dumpcode([[data]]).next()[0]
        self.assertEqual(dumbo.loadcode([dumped]).next()[0],data)

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
