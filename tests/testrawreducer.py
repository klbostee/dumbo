import os
import unittest
from tempfile import mkstemp
from cStringIO import StringIO
from dumbo.lib.rawreducer import RawReducer, chunkedread


DATA = [('k1', ['v1a', 'v1b']), ('k2', ['v2c']), ('k3', ['v3d', 'v3e', 'v3f'])]
MULTIDATA = sorted(((str(i), k), [v]) for k, vs in DATA for i, v in enumerate(vs))


class RawReducerTestCase(unittest.TestCase):

    def test_default_factory(self):
        red = RawReducer()
        self.assertEqual(list(red(iter(DATA))),
                [(None, 'v1a'), (None, 'v1b'), (None, 'v2c'),
                 (None, 'v3d'), (None, 'v3e'), (None, 'v3f')])

        red = RawReducer(multipleoutput=True)
        self.assertEqual(list(red(iter(MULTIDATA))),
                [('0', 'v1a'), ('0', 'v2c'), ('0', 'v3d'),
                 ('1', 'v1b'), ('1', 'v3e'), ('2', 'v3f')])

    def test_custom_factory(self):
        def first_value_factory():
            return lambda k, v: [v[0]]

        red = RawReducer(first_value_factory)
        self.assertEqual(list(red(iter(DATA))),
                [(None, 'v1a'), (None, 'v2c'), (None, 'v3d')])

        red = RawReducer(first_value_factory, multipleoutput=True)
        self.assertEqual(list(red(iter(MULTIDATA))),
                [('0', 'v1a'), ('0', 'v2c'), ('0', 'v3d'),
                 ('1', 'v1b'), ('1', 'v3e'), ('2', 'v3f')])

    def test_custom_factory_with_close(self):
        class CloseFactory(object):
            def __init__(self):
                self.items = []

            def __call__(self, key, values):
                self.items.extend(values)

            def close(self):
                return self.items

        red = RawReducer(CloseFactory)
        self.assertEqual(list(red(iter(DATA))),
                [(None, 'v1a'), (None, 'v1b'), (None, 'v2c'),
                 (None, 'v3d'), (None, 'v3e'), (None, 'v3f')])

        red = RawReducer(CloseFactory, multipleoutput=True)
        self.assertEqual(list(red(iter(MULTIDATA))),
                [('0', 'v1a'), ('0', 'v2c'), ('0', 'v3d'),
                 ('1', 'v1b'), ('1', 'v3e'), ('2', 'v3f')])

    def test_extending_rawreducer_class(self):
        class DummyFactory(object):
            def __call__(self, key, values):
                yield key

        class DummyReducer(RawReducer):
            factory = DummyFactory

        red = DummyReducer()
        self.assertEqual(list(red(iter(DATA))),
                [(None, 'k1'), (None, 'k2'), (None, 'k3')])

        red = DummyReducer(multipleoutput=True)
        self.assertEqual(list(red(iter(MULTIDATA))),
                [('0', 'k1'), ('0', 'k2'), ('0', 'k3'),
                 ('1', 'k1'), ('1', 'k3'), ('2', 'k3')])

        class MultiDummyReducer(RawReducer):
            factory = DummyFactory
            multipleoutput = True

        red = MultiDummyReducer()
        self.assertEqual(list(red(iter(MULTIDATA))),
                [('0', 'k1'), ('0', 'k2'), ('0', 'k3'),
                 ('1', 'k1'), ('1', 'k3'), ('2', 'k3')])


class ChunkedReadTestCase(unittest.TestCase):

    def test_chunkedread_on_fileobject(self):
        fo = StringIO('one\nbig\nchunk\nof\ndata\n')
        chunks = chunkedread(fo, chunksize=10)
        self.assertEqual(chunks.next(), 'one\nbig\nch')
        self.assertEqual(chunks.next(), 'unk\nof\ndat')
        self.assertEqual(chunks.next(), 'a\n')
        self.assertRaises(StopIteration, chunks.next)
        fo.close()

    def test_chunkedread_on_filename(self):
        fn = mkstemp()[1]
        try:
            fo = open(fn, 'wb')
            fo.write('one\nbig\nchunk\nof\ndata\n')
            fo.close()
            chunks = chunkedread(fn, chunksize=10)
            self.assertEqual(chunks.next(), 'one\nbig\nch')
            self.assertEqual(chunks.next(), 'unk\nof\ndat')
            self.assertEqual(chunks.next(), 'a\n')
            self.assertRaises(StopIteration, chunks.next)
        finally:
            os.unlink(fn)
