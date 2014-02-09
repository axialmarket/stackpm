'''stackpm/__init__.py -- wraps stackpm in a bow

   To run stackpm in debug mode:

     python -m stackpm.app

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
   @depends: flask, flask-sqlalchemy, numpy, requests, sqlite3, gunicorn
'''

# ORDER MATTERS HERE -- SOME MODULES ARE DEPENDANT ON OTHERS
from app import stackpm_app, config, db

import fields
import models
import links
import sync

__all__ = ['stackpm_app', 'config', 'db', 'models', 'fields', 'links', 'sync']
