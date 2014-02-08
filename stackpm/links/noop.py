'''stackpm/links/noop.py -- noop connector and interface definition

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

def _empty_generator():
    '''Yielder used to spoof generator methods in Connectors'''
    for i in xrange(0):
        yield i

class Connector(object):
    '''Define the connector interface for all link types here'''
    def __init__(self, config=None):
        pass

    def __repr__(self):
        return '<NoOpLink>'

    ### API for project_manager connector
    def iterations(self, since=None, limit=None):
        '''Returns an iterable type of up to len ``limit`` iteration
           dictionaries that have been updated since ``since``.

           iterations is typically implemented as a generator.'''
        return _empty_generator()

    def tasks(self, since=None, limit=None):
        '''Returns an iterable type of up to len ``limit`` task dictionaries
           that have been updated since ``since``.

           tasks is typically implemented as a generator.'''
        return _empty_generator()

__all__ = ['Connector']
