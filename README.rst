=======
stackpm
=======

stackpm is a web-appliance designed to provide a set of tools for
prioritization, forecasting and performance management for use with stack
teams.

Installing
==========

stackpm lives on github_, and is available via pip_.

.. _github: https://github.com/axialmarket/stackpm
.. _pip: https://pypi.python.org/

Installing v0.1b From Pip
-------------------------

::

    sudo pip install stackpm==0.1b

Installing v0.1b From Source
----------------------------

::

    curl https://github.com/axialmarket/stackpm/archive/version_0.1b.tar.gz | tar vzxf -
    cd stackpm
    sudo python setup.py install

Configuration
=============

stackpm's configuration is installed to /etc/stackpm.cfg.

Database
========

stackpm currently only supports a sqlite3 database backend, the default
location of the database is /var/stackpm/stackpm.db

Authors
=======

| Matthew Story <matt.story@axial.net>

License
=======

See LICENSE.txt_

.. _LICENSE.txt: ./LICENSE.txt
