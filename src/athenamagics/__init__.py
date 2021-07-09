__version__ = '0.0.1'

from .flinkmagics import *
from .alinkmagics import *


def load_ipython_extension(ipython):
    ipython.register_magics(FlinkMagics)
    ipython.register_magics(AlinkMagics)
