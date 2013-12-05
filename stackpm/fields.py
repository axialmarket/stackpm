'''stackpm/fields.py -- Custom database fields for stackpm

   classes: JSONField
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''
from . import db
import json

class JSONField(db.TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Shamelessly stolen (and modified) from:
      http://docs.sqlalchemy.org/en/rel_0_9/core/types.html#marshal-json-strings
    """

    impl = db.Text

    def process_bind_param(self, value, dialect):
        return value if value is None else json.dumps(value)

    def process_result_value(self, value, dialect):
        return value if value is None else json.loads(value)
