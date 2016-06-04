#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (
    CCompilerError, DistutilsExecError, DistutilsPlatformError
)
from distutils import log
from os.path import join


pkg = 'fastavro'


def version():
    import re
    init_file = open(join(pkg, '__init__.py'), 'r').read()
    match = re.search(r'__version_info__\s*=\s*\((.+?)\)', init_file)
    assert match, 'Failed to find fastavro __version__'
    return '.'.join(match.group(1).replace(' ', '').split(','))


install_requires = []
if sys.version_info[:2] < (2, 7):
    # `argparse` is not in the Python 2.6 standard library
    install_requires.append('argparse')


tests_require = ['nose', 'flake8', 'cython']
if sys.version_info[0] < 3:
    # Some tests require `unittest2` for Python 2.x
    tests_require.append('unittest2')


class maybe_build_ext(build_ext):
    """Allow building Cython extension modules to fail gracefully"""

    build_errors = (
        CCompilerError, DistutilsExecError, DistutilsPlatformError, IOError
    )

    def run(self):
        try:
            build_ext.run(self)
        except self.build_errors:
            log.warn('Failed to build Cython extension modules')

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except self.build_errors:
            log.warn('Failed to build Cython extension modules')


def _extensions(*module_names):
    return [
        Extension(module, sources=['%s.c' % module])
        for module in (
            join(pkg, module_name) for module_name in module_names
        )
    ]


ext_modules = _extensions(
    'c_reader',
    'c_writer',
    'c_utils',
    'c_buffer',
)

if hasattr(sys, 'pypy_version_info'):
    # Don't compile extension under pypy
    # See https://bitbucket.org/pypy/pypy/issue/1770
    ext_modules = []


# ---- Setup -----------------------------------------------------------------#

setup(
    name='fastavro',
    version=version(),
    description='Faster reading and writing Apache Avro files',
    long_description=open('README.md').read(),
    author='Eric Heller',
    author_email='eheller@gmail.com',
    license='MIT',
    url='https://github.com/e-heller/fastavro',

    packages=[pkg],
    zip_safe=False,
    ext_modules=ext_modules,
    cmdclass={'build_ext': maybe_build_ext},

    # Include files from MANIFEST.in
    include_package_data=True,

    # Command-line script
    entry_points={
        'console_scripts': [
            'fastavro = fastavro.__main__:main',
        ]
    },

    # Requirements
    install_requires=install_requires,
    extras_require={
        'snappy': ['python-snappy'],
        'simplejson': ['simplejson'],
    },
    tests_require=tests_require,

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
