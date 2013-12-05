'''stackpm/links/__init__.py -- package for linking to 3rd party tools

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

from .. import stackpm_app
import noop

### GLOBALS
# each of these ends up being a module level variable
LINK_TYPES = ( 'project_manager', 'scm', 'calendar', )
CONNECTORS = stackpm_app.config.get('LINKS', {}).get('links', {})

# DEPENDS CONNECTORS, noop
import connectors

# DEPENDS connectors
def setup():
    '''Setup package-level links for all link-types defined in LINKS section'''
    link_conf = stackpm_app.config.get('LINKS', {})
    _scope = globals()
    for link_type in LINK_TYPES:
        link = link_conf.get(link_type) or 'noop'
        config = stackpm_app.config.get(link.upper(), {})
        connector = getattr(connectors, link).Connector(config)
        _scope[link_type] = connector

setup()
del setup

__all__ = [ 'connectors', 'LINK_TYPES', ] + list(LINK_TYPES)
