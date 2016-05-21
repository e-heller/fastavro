# -*- coding: utf-8 -*-
"""Tests for fastavro.c_buffer (ByteBuffer class)"""


# NOTE: The ByteBuffer unit tests must be written in a Cython .pyx file,
# and then compiled. In this module, we import the unittest.TestCase classes
# defined in `_test_byte_buffer`, and implement the `load_tests` function so
# nosetests can find and run the tests. Yes, it's a little hairy.


from __future__ import absolute_import

import sys

try:
    from tests._test_byte_buffer import TestByteBuffer
except ImportError:
    TestByteBuffer = None
    sys.stderr.write(
        'Failed to import compiled test `_test_byte_buffer.pyx`: ' +
        '`test_byte_buffer` tests will not run.\n'
    )

# Import unittest module (requires `unittest2` for Python 2.x)
if sys.version_info[0] == 2:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The 'unittest2' module is required for Python 2.x")
else:
    import unittest


_test_cases = (TestByteBuffer,)


def load_tests(*args):
    # If we were using `unittest.main`, then this function would receive
    # 3 arguments `(loader, tests, pattern)`
    #   cf: https://docs.python.org/3/library/unittest.html#load-tests-protocol
    # However, nosetests doesn't seem to implement this properly and passes
    # no arguments to `load_tests`. In this case we just need to create our
    # own `TestLoader` instance, then build and return our TestSuite.
    if args:
        loader, _, _ = args
    else:
        loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for case in filter(None, _test_cases):
        tests = loader.loadTestsFromTestCase(case)
        suite.addTests(tests)
    return suite


if __name__ == '__main__':
    import nose
    config = nose.config.Config(logStream=sys.stdout, stream=sys.stdout)
    nose.runmodule(config=config)
