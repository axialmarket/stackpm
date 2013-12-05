'''stackpm/app.py -- config and flask application setup for stackpm.

   methods: cfg2obj
   objects: stackpm_app, config, db
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

### STANDARD LIBRARY IMPORTS
import os
import ast
from ConfigParser import ConfigParser

### 3RD PARTY IMPORTS
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

### GLOBALS
STACKPM_CONFIG_ENV = 'STACKPM_CONFIG'
#TODO: reset
STACKPM_CONFIG_DFLT = '/home/matt/src/stackpm/src/default.cfg' # '/etc/stackpm.cfg

### EXPOSED METHODS
def cfg2obj(cfg_path):
    '''Convert a ConfigParser cfg file to a python object.

       As Flask seems to lack ConfigParser support, and ConfigParser is much
       easier to read and write, we need to give it a helping hand.

       NB: For ease of typing, and extension, values are interpretted as
           python literals using asl.literal_eval
    '''
    config = {}
    parser = ConfigParser()
    parser.read(cfg_path)
    for section in parser.sections():
        sect_config = config.setdefault(section.upper(), {})
        for name,val in parser.items(section):
            # evaluate value as a python expression and store
            sect_config[name] = ast.literal_eval(val)

    # setup Flask-SQLAlchemy if we need to
    config['SQLALCHEMY_DATABASE_URI'] = config.get('DB', {}).get('url')

    return config

### EXPOSED MODULE LEVEL ATTRIBUTES

# void-scope execution is bad, but it's kind of the point here
config = cfg2obj(os.environ.get(STACKPM_CONFIG_ENV, STACKPM_CONFIG_DFLT))

stackpm_app = Flask('stackpm')
stackpm_app.config.update(config)
if stackpm_app.config['SERVER']['debug']:
    stackpm_app.debug = True

db = SQLAlchemy(stackpm_app)

if __name__ == '__main__':
    stackpm_app.run(host=config['SERVER']['host'],
                  port=config['SERVER']['port'])
