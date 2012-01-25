import unittest
from dumbo.util import getopt, getopts, Options

class TestUtil(unittest.TestCase):

    def test_getopt(self):
        # Test for backward compatibility
        opts = []
        values = getopt(opts, 'input')
        self.assertEquals(values, [])
        self.assertEquals(opts, [])

        opts = [('param', 'p1'), ('param', 'p2'), ('input', '/dev/path')]
        values = getopt(opts, 'param')
        expected = ['p2', 'p1']
        self.assertEquals(set(values), set(expected))
        self.assertEquals(set(opts), set([('input', '/dev/path')]))

        opts = [('output', '/prod/path')]
        values = getopt(opts, 'output', delete=False)
        self.assertEquals(values, ['/prod/path'])
        self.assertEquals(opts, [('output', '/prod/path')])

        values = getopt(opts, 'output')
        self.assertEquals(values, ['/prod/path'])
        self.assertEquals(opts, [])

    def test_getopts(self):
        # Test for backward compatibility
        opts = []
        values = getopts(opts, ['input'])
        self.assertEquals(values, {})
        self.assertEquals(opts, [])

        opts = [('param', 'p1'), ('param', 'p2'), ('input', '/dev/path'),
                ('output', '/prod/path')]
        values = getopts(opts, ['param', 'input'])
        expected = {'input': ['/dev/path'], 'param': ['p2', 'p1']}
        settize = lambda _dict: set([(k, tuple(sorted(v))) for k, v in _dict.items()])
        self.assertEquals(settize(values), settize(expected))
        self.assertEquals(set(opts), set([('output', '/prod/path')]))

        opts = [('output', '/prod/path')]
        values = getopts(opts, ['output'], delete=False)
        self.assertEquals(values, {'output': ['/prod/path']})
        self.assertEquals(opts, [('output', '/prod/path')])

        values = getopts(opts, ['output'])
        self.assertEquals(values, {'output': ['/prod/path']})
        self.assertEquals(opts, [])

    def test_Options(self):
        o = Options([('param', 'p1')])
        # test add / get
        o.add('param', 'p2')

        # test repeat add same parameter
        o.add('param', 'p2')
        o.add('input', '/dev/path')
        o.add('output', '/dev/out')
        self.assertEquals(set(o.get('param')), set(['p1', 'p2']))
        self.assertEquals(o.get('input'), ['/dev/path'])
        self.assertEquals(o.get('notexist'), [])

        # test __getitem__
        self.assertEquals(set(o['param']), set(['p1', 'p2']))
        self.assertEquals(o['input'], ['/dev/path'])
        self.assertEquals(o['notexist'], [])

        # test __delitem__
        self.assertEquals(o['output'], ['/dev/out'])
        del o['output']
        self.assertEquals(o['output'], [])

        # test __iadd__
        # adding Options objects
        o += Options([('output', '/dev/out2'), ('jar', 'my.jar')])
        self.assertEquals(o['output'], ['/dev/out2'])
        self.assertEquals(o['jar'], ['my.jar'])
        # adding a list & set
        o += [('param', 'p3'), ('egg', 'lib.egg')]
        self.assertEquals(set(o['param']), set(['p1', 'p2', 'p3']))
        self.assertEquals(o['egg'], ['lib.egg'])

        o += set([('cmdenv', 'p=2')])
        self.assertEquals(o['cmdenv'], ['p=2'])

        # testing iter / allopts
        o2 = Options([('param', 'p1')])
        o2.add('param', 'p2')
        o2.add('input', '/dev/path')
        self.assertEquals(set(o2), set([('param', 'p1'), ('param', 'p2'), ('input', '/dev/path')]))
        self.assertEquals(set(o2.allopts()), set([('param', 'p1'), ('param', 'p2'), ('input', '/dev/path')]))


        # testing len
        self.assertEquals(len(o), 8)
        self.assertEquals(len(o2), 3)
        self.assertEquals(len(Options()), 0)

        # testing boolean
        self.assertTrue(o)
        self.assertTrue(o2)
        self.assertFalse(Options())

        # testing filter
        self.assertEquals(set(o2.filter(['param'])['param']), set(['p1', 'p2']))
        self.assertEquals(o2.filter(['input'])['input'], ['/dev/path'])

        nop = o.filter(['param', 'jar', 'egg'])
        self.assertEquals(len(nop), 5)
        self.assertEquals(set(nop['param']), set(['p1', 'p2', 'p3']))
        self.assertEquals(nop['jar'], ['my.jar'])
        self.assertEquals(nop['egg'], ['lib.egg'])

        # testing to_dict
        expected = {
            'param': ['p1', 'p2', 'p3'],
            'egg': ['lib.egg'],
            'jar': ['my.jar']
        }
        self.assertEquals(nop.to_dict(), expected)

        # testing remove
        nop.remove('param', 'jar')
        self.assertEquals(len(nop), 1)
        self.assertEquals(nop['param'], [])
        self.assertEquals(nop['jar'], [])
        self.assertEquals(nop['egg'], ['lib.egg'])

        # testing pop
        self.assertEquals(nop.pop('egg'), ['lib.egg'])
        self.assertEquals(len(nop), 0)
        self.assertEquals(nop['egg'], [])

        self.assertEquals(nop.pop('notexist'), [])







if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUtil)
    unittest.TextTestRunner(verbosity=2).run(suite)
