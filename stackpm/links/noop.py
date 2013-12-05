'''stackpm/links/noop.py -- noop connector and interface definition

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

class Connector(object):
    '''Define the connector interface for all link types here'''
    def __init__(self, config=None):
        pass

    def __repr__(self):
        return '<NoOpLink>'
