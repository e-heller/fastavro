# -*- coding: utf-8 -*-
"""Tests for fastavro.c_buffer (ByteBuffer class)"""


# NOTE: The ByteBuffer unit tests must be written in a Cython .pyx file, and
# then compiled. In this module, we create a `unittest.TestCase` class and
# dynamically add each 'test_*' function defined in `_test_byte_buffer`.
# This is very hairy, I know.


from __future__ import absolute_import

import sys

# Import unittest module (requires `unittest2` for Python 2.x)
if sys.version_info[0] == 2:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The 'unittest2' module is required for Python 2.x")
else:
    import unittest

try:
    from tests import _test_byte_buffer
except ImportError:
    _test_byte_buffer = None
    sys.stderr.write(
        'Failed to import compiled test `_test_byte_buffer.pyx`: ' +
        '`test_byte_buffer` tests will not run.\n'
    )


class TestByteBuffer(unittest.TestCase):
    pass


def setup():
    if not _test_byte_buffer:
        return
    module = _test_byte_buffer
    test_funcs = [f for f in dir(module) if f.startswith('test_')]
    for f in test_funcs:
        func = getattr(module, f)

        def wrap(self):
            func(self)

        name = wrap.__name__ = func.__name__
        setattr(TestByteBuffer, name, wrap)

setup()
