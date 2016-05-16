#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (
    CCompilerError, DistutilsExecError, DistutilsPlatformError
)
from os.path import join
import distutils.log as log
import re
import sys


def extension(base):
    cmodule = '_{0}'.format(base)
    cfile = join('fastavro', '{0}.c'.format(cmodule))

    return Extension('fastavro.{0}'.format(cmodule), [cfile])


def version():
    pyfile = 'fastavro/__init__.py'
    with open(pyfile) as fp:
        data = fp.read()

    match = re.search("__version__ = '([^']+)'", data)
    assert match, 'cannot find version in {}'.format(pyfile)
    return match.group(1)


ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
              IOError)


class maybe_build_ext(build_ext):
    """This class allows C extension building to fail."""

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            log.info('cannot bulid C extension, will continue without.')

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            log.info('cannot bulid C extension, will continue without.')


install_requires = []
if sys.version_info[:2] < (2, 7):
    # `argparse` is not in the standard library for Python < 2.7
    install_requires.append('argparse')


tests_require = ['nose', 'flake8']
if sys.version_info[0] < 3:
    # Some tests require 'unittest2' on Python 2.x
    tests_require.append('unittest2')


# Don't compile extension under pypy
# See https://bitbucket.org/pypy/pypy/issue/1770
ext_modules = [
    extension('reader'),
    extension('compat'),
    extension('writer'),
    extension('schema'),
]
if hasattr(sys, 'pypy_version_info'):
    ext_modules = []


setup(
    name='fastavro',
    version=version(),
    description='Faster reading and writing Apache Avro files',
    long_description=open('README.md').read(),
    author='Eric Heller',
    author_email='eheller@gmail.com',
    license='MIT',
    url='https://github.com/e-heller/fastavro',
    packages=['fastavro'],
    ext_modules=ext_modules,
    cmdclass={'build_ext': maybe_build_ext},
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'fastavro = fastavro.__main__:main',
        ]
    },
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
    install_requires=install_requires,
    extras_require={
        'snappy': ['python-snappy'],
        'simplejson': ['simplejson'],
    },
    tests_require=tests_require,
)
