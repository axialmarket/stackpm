'''stack/setup.py -- packaging for stack

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
   @depends: flask, flask-sqlalchemy, numpy, requests, sqlite3, gunicorn
'''
from distutils.core import setup

setup(
    name='stackpm',
    version='0.1b',
    author='Matthew Story',
    packages=['stackpm', 'stackpm.links'],
    py_modules=['stackpm_jira'],
    data_files=[
        ( '/var/stackpm/', [], ),
        ( '/etc/', [ 'stackpm.cfg', ], ),
    ],
    author_email='matt.story@axial.net',
    url='https://github.com/axialmarket/stackpm',
    license='3-BSD',
    description='Stack Prioritization and Forecasting Utility',
    long_description=open('./README.rst').read(),
    requires=['flask', 'flask.ext.sqlalchemy', 'numpy', 'requests',
              'betterconfig']
)
