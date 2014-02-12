'''stackpm_config_link.py -- link to read setup info from a config file.

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''

class Connector(object):
    def __init__(self, config=None):
        self.config = config or {}
