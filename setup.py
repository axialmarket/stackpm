'''stack/setup.py -- packaging for stack

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)
   @depends: flask, flask-sqlalchemy, numpy, requests, sqlite3, gunicorn
'''

setup(
    name='stackpm',
    version='0.1b',
    author='Matthew Story',
    author_email='matt.story@axial.net',
    url='https://github.com/axialmarket/stackpm',
    license='3-BSD',
    description='Stack Prioritization and Forecasting Utility',
    long_description=open('./README.rst').read(),
    requires = [ 'flask', 'flask-sqlalchemy', 'numpy', 'requests', ]
)
