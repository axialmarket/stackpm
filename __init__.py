'''stack/__init__.py -- wraps stack in a bow

   To run stack in debug mode:

     python -m stack.app

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
   @depends: flask, flask-sqlalchemy, numpy, requests, sqlite3, gunicorn
'''

# ORDER MATTERS HERE -- SOME MODULES ARE DEPENDANT ON OTHERS
from app import stack_app, config, db

import models

__all__ = [ 'stack_app', 'config', 'db', 'models', ]
