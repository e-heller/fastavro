# -*- coding: utf-8 -*-
"""Cython Include File: Compatiblity shims for Python 2 and 3"""

from cpython.version cimport PY_MAJOR_VERSION


# Python version
cdef bint PY2 = PY_MAJOR_VERSION == 2
cdef bint PY3 = PY_MAJOR_VERSION == 3


# Python type aliases
cdef tuple _string_types
cdef tuple _int_types
cdef tuple _number_types
cdef byte2int


if PY2:
    # Python 2 type aliases
    _string_types = (basestring,)
    _bytes_type = str
    _int_types = (int, long)
    _number_types = (int, long, float)
    byte2int = lambda b: ord(b[0])
else:
    # Python 3 type aliases
    _string_types = (bytes, str)
    _int_types = (int,)
    _number_types = (int, float)
    byte2int = lambda b: b[0]

cdef inline iteritems(obj):
    return obj.iteritems() if PY2 else iter(obj.items())
