import os
import sys
import unittest
from dumbo import cmd, util
from dumbo.util import Options

class TestExamples(unittest.TestCase):

    def setUp(self):
        if "directory" in os.environ:
            rootdir = os.environ["directory"]
            self.exdir = rootdir + "/examples/"
            self.tstdir = rootdir + "/tests/"
        elif "/" in __file__:
            self.exdir = __file__.split("tests/")[0] + "examples/"
            self.tstdir = "/".join(__file__.split("/")[:-1]) + "/"
        else:
            self.exdir = "../examples/"
            self.tstdir = "./"
        self.logfile = open(self.tstdir+"log.txt", "w")
        self.outfile = self.tstdir + "output.code"
        self.common_opts = Options([('checkoutput', 'no')])

    def tearDown(self):
        self.logfile.close()
        os.remove(self.outfile)

    def testwordcount(self):
        opts = self.common_opts
        opts += [('input', self.exdir+'brian.txt'), ('output', self.outfile)]
        retval = cmd.start(self.exdir+'wordcount.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEqual(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEqual(6, int(output['Brian']))

    def testoowordcount(self):
        opts = self.common_opts
        opts += [('excludes', self.exdir+'excludes.txt'),
                 ('input', self.exdir+'brian.txt'), ('output', self.outfile)]
        retval = cmd.start(self.exdir+'oowordcount.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEquals(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEquals(6, int(output['Brian']))

    def testaltwordcount(self):
        opts = self.common_opts
        opts += [('input', self.exdir+'brian.txt'), ('output', self.outfile)]
        retval = cmd.start(self.exdir+'altwordcount.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEqual(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEqual(6, int(output['Brian']))

    def testitertwice(self):
        opts = self.common_opts
        opts += [('input', self.exdir+'brian.txt'), ('output', self.outfile)]
        retval = cmd.start(self.exdir+'itertwice.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEqual(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEqual(14, int(output['e']))

    def testjoin(self):
        opts = self.common_opts
        opts += [('input', self.exdir+'hostnames.txt'),
                 ('input', self.exdir+'logs.txt'),
                 ('output', self.outfile)]
        retval = cmd.start(self.exdir+'join.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEqual(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEqual(5, int(output['node1']))

    def testmulticount(self):
        opts = self.common_opts
        opts += [('input', self.exdir+'brian.txt'),
                 ('input', self.exdir+'eno.txt'),
                 ('output', self.outfile)]
        retval = cmd.start(self.exdir+'multicount.py', opts,
                           stdout=self.logfile, stderr=self.logfile)
        self.assertEqual(0, retval)
        output = dict(util.loadcode(open(self.outfile)))
        self.assertEqual(6, int(output[('A', 'Brian')]))
        self.assertEqual(6, int(output[('B', 'Eno')]))


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestExamples)
    unittest.TextTestRunner(verbosity=2).run(suite)
