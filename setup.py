#!/usr/bin/env python

from distutils.core import setup

setup(name='MysqlTunnelConnector',
      version='1.0',
      description='Utility for connecting to mysql using a tunnel (or not)',
      author='Victor Perez',
      author_email='viperey@gmail.com',
      url='http://www.python.org/sigs/distutils-sig/',
      packages=['MysqlTunnelConnector', 'MysqlTunnelConnector.tunnel_connector'],
      )
