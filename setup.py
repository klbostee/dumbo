from distutils.core import setup,Command
from unittest import TextTestRunner,TestLoader
from glob import glob
from os.path import splitext,basename,join as pjoin,walk
import os

class TestCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        testfiles = [ ]
        for t in glob(pjoin(self._dir, 'test', '*.py')):
            if not t.endswith('__init__.py'):
                testfiles.append('.'.join(
                    ['test', splitext(basename(t))[0]])
                )
        tests = TestLoader().loadTestsFromNames(testfiles)
        t = TextTestRunner(verbosity = 1)
        t.run(tests)

setup(name='dumbo',
      version='0.15',
      py_modules=['dumbo'],
      author='Klaas Bosteels',
      author_email='klaas@last.fm',
      license="GNU General Public License (GPL)",
      url='http://www.audioscrobbler.net/development/dumbo/',
      cmdclass = { 'test': TestCommand }
      )
