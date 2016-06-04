# -*- coding: utf-8 -*-
"""Tests for fastavro.c_buffer (ByteBuffer class)"""


# NOTE: The ByteBuffer unit tests must be written in a Cython .pyx file and
# compiled. The tests in this module are implemented as a `unittest.TestCase`.
#
# Theoretically, it should be as simple as importing this TestCase class into
# the Python test module `test_byte_buffer` -- however, for some unknown reason
# this only seems to work for Python 2, and not in Python 3. (Maybe it has
# something to do with nosetests? I have no idea!!)
#
# So -- here's where things get really hairy. In the Python test module, we
# have to find each 'test_*' method defined in our TestCase, and then
# dynamically add the method to a *different* `unittest.TestCase` which is
# declared in the Python module. Yes, it's ugly! But so far this is the only
# thing that seems to work.


from __future__ import absolute_import

import sys

PY2 = sys.version_info[0] == 2

# Import unittest module (requires `unittest2` for Python 2.x)
if PY2:
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
    if PY2:
        # For some reason unittest2 needs a dummy `methodName` argument
        obj = _test_byte_buffer.TestBase('dummy')
    else:
        obj = _test_byte_buffer.TestBase()
    test_funcs = [f for f in dir(obj) if f.startswith('test_')]
    for f in test_funcs:
        func = getattr(obj, f)
        setattr(TestByteBuffer, func.__name__, func)

setup()


if __name__ == '__main__':
    verbosity = 2
    unittest.main(verbosity=verbosity, catchbreak=True)
