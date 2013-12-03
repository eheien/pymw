#!/usr/bin/env python

from distutils.core import setup

DESC = """PyMW is a Python module for parallel master-worker computing in a variety of environments. With the PyMW module, users can write a single program that scales from multicore machines to global computing platforms."""

setup(name='pymw',
      version='0.4.1',
      author='Eric Heien',
      author_email='pymw@heien.org',
      url='http://pymw.sourceforge.net/',
      description='Python Master-Worker Computing',
      long_description=DESC,
      license="MIT License",
      platforms=["any"],
      packages=['pymw', 'pymw.interfaces'],
      package_dir={'pymw': 'pymw'},
      package_data={'pymw': ['interfaces/pymw_run.exe',] },
     )
