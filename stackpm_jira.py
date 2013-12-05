'''stackpm_jira.py -- jira connection extension for stackpm

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''

class Connector(object):
    '''Jira Connector Ojbect'''
    def __init__(self, config=None):
        self.url = config.get('url')
        pass

    def __repr__(self):
        return '<JiraLink to {}>'.format(self.url)
