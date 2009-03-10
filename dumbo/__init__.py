"""
The Dumbo Python module. 

Please refer to http://wiki.github.com/klbostee/dumbo for more info.
"""

from dumbo.core import *
from dumbo.lib import *

if __name__ == '__main__':
    import sys
    from dumbo.cmd import dumbo
    sys.exit(dumbo())
