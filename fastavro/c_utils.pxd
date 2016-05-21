# -*- coding: utf-8 -*-
"""Cython Header File: Cython utils for fastavro"""


cdef enum Endianness:
    little, big, unknown, error


cdef Endianness get_double_format() except error

cdef Endianness get_float_format() except error

cdef Endianness get_int_format() except error
