'''stackpm/app.py -- config and flask application setup for stackpm.

   methods: cfg2obj
   objects: stackpm_app, config, db
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
'''

### STANDARD LIBRARY IMPORTS
import os

### 3RD PARTY IMPORTS
import betterconfig
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

### GLOBALS
STACKPM_CONFIG_ENV = 'STACKPM_CONFIG'
STACKPM_CONFIG_DFLT = '/etc/stackpm.cfg'

def setup():
    ### load config
    config = betterconfig.load(os.environ.get(STACKPM_CONFIG_ENV,
                                              STACKPM_CONFIG_DFLT))
    config['SQLALCHEMY_DATABASE_URI'] = config.pop('db', None)
    config.setdefault('debug', False)

    ### configure flask app
    stackpm_app = Flask('stackpm')
    stackpm_app.config.update(config)
    if config['debug']:
        stackpm_app.debug = True

    ### configure database
    db = SQLAlchemy(stackpm_app)

    ### expose globals
    globals().update({
        'config': config,
        'stackpm_app': stackpm_app,
        'db': db,
    })

### DO SETUP
setup()
del setup

if __name__ == '__main__':
    stackpm_app.run(host=config['server']['host'],
                    port=config['server']['port'])
