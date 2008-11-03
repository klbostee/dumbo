import sys
from unittest import TextTestRunner,TestLoader
from glob import glob
from os.path import splitext

modules = [splitext(file)[0] for file in glob("*.py") if file != "main.py"]
tests = TestLoader().loadTestsFromNames(modules)
t = TextTestRunner(verbosity = 2)
sys.exit(len(t.run(tests).errors))
