# -*- coding: utf-8 -*-
# cython: c_string_type=bytes, wraparound=False
# cython: optimize.use_switch=True, always_allow_keywords=False
"""Cython utils for fastavro"""

from __future__ import absolute_import

import cython

from libc cimport stdint
from libc.float cimport FLT_MANT_DIG, DBL_MANT_DIG
from libc.stdint cimport uint32_t
from libc.string cimport memcmp


# Define these values as python ints; they will be compared to python values
INT32_MIN = stdint.INT32_MIN
INT32_MAX = stdint.INT32_MAX
INT64_MIN = stdint.INT64_MIN
INT64_MAX = stdint.INT64_MAX


# ---- Determine endian-ness of floats, doubles, and ints on this system -----#

cdef Endianness get_double_format() except error:
    """Determine if a 'double' on this system is big-endian, little-endian,
    or some unknown format"""
    # Copied from the Python library: `_PyFloat_Init()` in floatobject.c
    cdef double x = 9006104071832581.0
    if cython.sizeof(double) != 8:
        return unknown
    elif memcmp(&x, "\x43\x3f\xff\x01\x02\x03\x04\x05", 8) == 0:
        return big
    elif memcmp(&x, "\x05\x04\x03\x02\x01\xff\x3f\x43", 8) == 0:
        return little
    else:
        return unknown


cdef Endianness get_float_format() except error:
    """Determine if a 'float' on this system is big-endian, little-endian,
    or some unknown format"""
    # Copied from the Python library: `_PyFloat_Init()` in floatobject.c
    cdef float y = 16711938.0;
    if cython.sizeof(float) != 4:
        return unknown
    elif memcmp(&y, "\x4b\x7f\x01\x02", 4) == 0:
        return big
    elif memcmp(&y, "\x02\x01\x7f\x4b", 4) == 0:
        return little
    else:
        return unknown


cdef Endianness get_int_format() except error:
    """Determine if an 'int32_t' on this system is big or little-endian"""
    cdef uint32_t y = 1
    cdef char* c = <char*>&y
    if c[0] == 1:
        return little
    else:
        return big
