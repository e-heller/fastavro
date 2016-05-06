# -*- coding: utf-8 -*-
# cython: auto_cpdef=True
"""Compatiblity for Python versions"""


# Some of this code is adapted from `CherryPy`:
#   https://bitbucket.org/cherrypy/cherrypy/wiki/Home
#   Licensed under a BSD License:
#   https://bitbucket.org/cherrypy/cherrypy/src/tip/cherrypy/LICENSE.txt

# Some of this code is adapted from `six`:
#   https://bitbucket.org/gutworth/six
#   Licensed under a MIT License:
#   https://bitbucket.org/gutworth/six/src/tip/LICENSE


from __future__ import absolute_import

import sys


# Python version
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    # Python 3 type aliases
    _unicode_type = str
    _bytes_type = bytes
    _string_types = (bytes, str)
    _int_types = (int,)
    _number_types = (int, float)
    _unicode = str

    from io import BytesIO  # flake8: noqa
    xrange = range

    def py3_btou(byte_str):
        return byte_str.decode('utf-8')

    def py3_utob(unicode_str):
        return bytes(unicode_str, 'utf-8')

    def py3_iteritems(obj):
        return iter(obj.items())

else:
    # Python 2 type aliases
    _unicode_type = unicode
    _bytes_type = str
    _string_types = (basestring,)
    _int_types = (int, long)
    _number_types = (int, long, float)
    _unicode = unicode

    from cStringIO import StringIO as BytesIO  # flake8: noqa
    xrange = xrange

    def py2_btou(byte_str):
        return unicode(byte_str, 'utf-8')

    def py2_utob(unicode_str):
        return unicode_str.encode('utf-8')

    def py2_iteritems(obj):
        return obj.iteritems()


# Export an alias for each of the version-specific functions
# We do it this way because Cython does not like redefined functions.
if PY3:
    btou = py3_btou
    utob = py3_utob
    iteritems = py3_iteritems
else:
    btou = py2_btou
    utob = py2_utob
    iteritems = py2_iteritems
