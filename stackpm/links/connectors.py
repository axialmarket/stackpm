'''stackpm/links/connectors.py -- connector extensions for stackpm

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''
import importlib

from . import noop, CONNECTORS

def setup():
    '''Setup module-level connectors.'''
    _scope = globals()
    for name,mod in CONNECTORS.iteritems():
        _scope[name] = importlib.import_module(mod)

setup()
del setup
