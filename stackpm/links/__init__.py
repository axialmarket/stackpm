'''stackpm/links/__init__.py -- package for linking to 3rd party tools

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

from .. import stackpm_app
import noop

### GLOBALS
# each of these ends up being a module level variable
LINK_TYPES = ( 'project_manager', 'scm', 'calendar', )
LINK_CONFIG = stackpm_app.config.get('links', {})
CONNECTORS = LINK_CONFIG.get('connectors', {})

# DEPENDS CONNECTORS, noop
import connectors

# DEPENDS connectors
def setup():
    '''Setup package-level links for all link-types defined in links
       section. Note that if multiple link types specify the same connector,
       a single connector object will be shared across all link types.'''
    conns = {}
    scope = globals()
    for link_type in LINK_TYPES:
        link = LINK_CONFIG.get(link_type) or 'noop'
        config = stackpm_app.config.get(link, {})
        try:
            connector = conns[link]
        except KeyError:
            conns[link] = getattr(connectors, link).Connector(config)
            connector = conns[link]
        scope[link_type] = connector

setup()
del setup

__all__ = [ 'connectors', 'LINK_TYPES', ] + list(LINK_TYPES)
