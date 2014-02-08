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
           dicts capable of being sent to models.Iteration, that have been
           updated since ``since``.

           iterations is typically implemented as a generator.'''
        return _empty_generator()

    def iteration(self, ext_id):
        '''Return an iteration dict, capable of being sent to
           models.Iteration'''
        raise NotImplementedError

    def tasks(self, since=None, limit=None):
        '''Returns an iterable type of up to len ``limit`` task dicts capable
           of being sent to models.Task, that have been updated since
           ``since``, including events since ``since``.

           tasks is typically implemented as a generator.'''
        return _empty_generator()

    def task(self, ext_id, since=None):
        '''Return an task dict, capable of being sent to models.Task, with
           events since ``since``.'''
        raise NotImplementedError

    def get(self, *args, **kwargs):
        '''Not to be used directly, as interface may vary, but get should be
           exposed for REST API backed Connector's as it is useful in
           debugging.'''
        raise NotImplementedError

__all__ = ['Connector']
