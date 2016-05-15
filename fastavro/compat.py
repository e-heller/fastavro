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

# flake8: noqa

import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    # Python 2 type aliases
    _unicode_type = unicode
    _bytes_type = str
    _string_types = (basestring,)
    _int_types = (int, long)
    _number_types = (int, long, float)

    from cStringIO import StringIO as BytesIO
    xrange = xrange

    def py2_iteritems(obj):
        return obj.iteritems()
else:
    # Python 3 type aliases
    _unicode_type = str
    _bytes_type = bytes
    _string_types = (bytes, str)
    _int_types = (int,)
    _number_types = (int, float)

    from io import BytesIO
    xrange = range

    def py3_iteritems(obj):
        return iter(obj.items())


# Export an alias for each of the version-specific functions
# We do it this way because Cython does not like redefined functions.
if PY2:
    iteritems = py2_iteritems
else:
    iteritems = py3_iteritems


def byte2int(b):
    return ord(b[0]) if PY2 else b[0]
