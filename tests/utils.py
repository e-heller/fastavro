# -*- coding: utf-8 -*-
"""Misc utlities for fastavro tests"""

from __future__ import absolute_import

import random
import struct
import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    _unicode_type = unicode
    _bytes_type = str
    unichr = unichr
    xrange = xrange
else:
    _unicode_type = str
    _bytes_type = bytes
    unichr = chr
    xrange = range


_byte_pack = struct.Struct('>B').pack


def int2byte(n):
    """Equivalent to chr(n) in Python 2. Works for Python 3"""
    return chr(n) if PY2 else _byte_pack(n)


def iteritems(obj):
    """This is compatible for Python 2 or 3"""
    return obj.iteritems() if PY2 else iter(obj.items())


# ---- Generating random strings ---------------------------------------------#

def random_byte_str(count):
    """Create a random byte string (may be non-ascii)"""
    return bytes(b''.join(
        int2byte(random.randint(1, 255)) for _ in xrange(count)
    ))


def random_unicode_str(count):
    """Create a random utf-8 compatible unicode string"""
    # Max UTF-8 code point is 0xFFFF or whatever this Python supports
    max_code_pt = min(0xFFFF, sys.maxunicode)

    def _code_pt():
        p = random.randint(1, max_code_pt)
        # Ordinals in this range are surrogates, invalid for utf-8 encoding
        while 0xD000 <= p <= 0xDFFF:
            p = random.randint(1, max_code_pt)
        return p

    return u''.join(
        unichr(_code_pt()) for _ in xrange(count)
    )
