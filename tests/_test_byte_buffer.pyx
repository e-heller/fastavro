# -*- coding: utf-8 -*-
# cython: c_string_type=bytes
"""Tests for fastavro.c_buffer (ByteBuffer class)"""


# NOTE: The ByteBuffer unit tests must be written in a Cython .pyx file, and
# then compiled. The test functions defined in this module are intended to be
# methods in a `unittest.TestCase` instance. The test file `test_byte_buffer`
# imports each 'test_*' function defined here and dynamically adds them to a
# `unittest.TestCase` for actual testing.

# TODO: Finish writing tests!


from __future__ import absolute_import

import sys

# Import unittest module (requires `unittest2` for Python 2.x)
#if sys.version_info[0] == 2:
#    try:
#        import unittest2 as unittest
#    except ImportError:
#        raise ImportError("The 'unittest2' module is required for Python 2.x")
#else:
#    import unittest

from fastavro.c_buffer cimport (
    Stream, ByteBuffer, cSEEK_SET, cSEEK_CUR, cSEEK_END, SSize_t, uchar,
)


def test_create_and_close_buffer(self):
    cdef ByteBuffer buf
    buf = ByteBuffer(20)
    self.assertIsInstance(buf, Stream)
    self.assertEqual(buf.size(), 20)
    self.assertEqual(len(buf), 0)
    self.assertEqual(buf.tell(), 0)
    self.assertEqual(buf.getvalue(), b'')

    buf.close()
    self.assertTrue(buf.closed)

    # close() may be called multiple times
    buf.close()
    self.assertTrue(buf.closed)

    # These should all be zero
    self.assertEqual(buf.pos, 0)
    self.assertEqual(buf.buf_len, 0)
    self.assertEqual(buf.data_len, 0)

    # The corresponding methods should return zero when closed
    self.assertEqual(buf.tell(), 0)
    self.assertEqual(buf.size(), 0)
    self.assertEqual(len(buf), 0)

    # Other methods should raise an exception
    with self.assertRaises(ValueError):
        buf.getvalue()
    with self.assertRaises(ValueError):
        buf.write(b'foo')
    with self.assertRaises(ValueError):
        buf.read(1)

def test_create_buffer_no_size_arg(self):
    cdef ByteBuffer buf
    buf = ByteBuffer()
    # ByteBuffer with no size argument will start with a default size
    self.assertTrue(buf.size())
    self.assertEqual(len(buf), 0)
    self.assertEqual(buf.tell(), 0)
    self.assertEqual(buf.getvalue(), b'')

    buf.close()

def test_buffer_write(self):
    cdef ByteBuffer buf
    buf = ByteBuffer(16)
    original_size = buf.size()

    # Writing null bytes -
    buf.write(b'')
    self.assertEqual(buf.tell(), 0)
    self.assertEqual(buf.getvalue(), b'')

    # Write 10 bytes
    value1 = b'0123456789'
    buf.write(value1)
    tell1 = buf.tell()
    self.assertEqual(tell1, len(value1))
    self.assertEqual(len(buf), tell1)
    self.assertEqual(buf.getvalue(), value1)
    # Writing 10 bytes should not resize the buffer
    self.assertEqual(buf.size(), original_size)

    # Edge case - write 6 bytes
    value2 = b'012345'
    buf.write(value2)
    tell2 = buf.tell()
    self.assertEqual(tell2, tell1 + len(value2))
    self.assertEqual(len(buf), tell2)
    self.assertEqual(buf.getvalue(), value1 + value2)
    # Writing 6 bytes WILL cause the buffer to resize -- Even though we
    # wrote a total of 16 bytes and the buffer size is also exactly 16
    # bytes -- The buffer will resize whenever buf.data_len >= buf.buf_len
    self.assertNotEqual(buf.size(), original_size)
    # The buffer will be resized to the next power of 2 > 16 * 2 == 32
    self.assertEqual(buf.size(), 32)

    # Write 20 bytes
    value3 = b'x' * 20
    buf.write(value3)
    tell3 = buf.tell()
    self.assertEqual(tell3, tell2 + len(value3))
    self.assertEqual(len(buf), tell3)
    self.assertEqual(buf.getvalue(), value1 + value2 + value3)
    # Writing 20 bytes will cause the buffer to be resized: 16 + 20 = 36
    # The buffer will be resized to next power of 2 > 36 * 2 == 128
    self.assertEqual(buf.size(), 128)

    # Seek to zero, overwrite old data with 100 bytes
    value4 = b'z' * 100
    buf.seek(0)
    buf.write(value4)
    tell4 = buf.tell()
    self.assertEqual(tell4, len(value4))
    self.assertEqual(len(buf), tell4)
    self.assertEqual(buf.getvalue(), value4)
    self.assertEqual(buf.size(), 128)

    buf.close()

def test_buffer_write_chars(self):
    cdef ByteBuffer buf
    buf = ByteBuffer(16)
    original_size = buf.size()

    cdef bytes value1 = b'0123456789'
    cdef uchar* char_value1 = value1
    buf.write_chars(char_value1, len(value1))
    tell1 = buf.tell()
    self.assertEqual(tell1, len(value1))
    self.assertEqual(len(buf), len(value1))
    self.assertEqual(buf.getvalue(), value1)
    # Writing 10 bytes should not resize the buffer
    self.assertEqual(buf.size(), original_size)

    cdef bytes value2 = value1 * 3
    cdef uchar* char_value2 = value2
    buf.write_chars(char_value2, len(value2))
    self.assertEqual(buf.tell(), tell1 + len(value2))
    self.assertEqual(len(buf), len(value1 + value2))
    self.assertEqual(buf.getvalue(), value1 + value2)
    # Writing 30 bytes should resize the buffer, 10 + 30 = 40 > 16
    # The buffer will be resized to nearest power of 2 for 40 * 2 => 128
    self.assertEqual(buf.size(), 128)

    buf.close()

def test_buffer_seek(self):
    cdef ByteBuffer buf
    buf = ByteBuffer(2)

    cdef bytes value = b'0123456789' * 2
    buf.write(value)
    self.assertEqual(buf.tell(), len(value))
    self.assertEqual(buf.getvalue(), value)

    buf.seek(0)
    self.assertEqual(buf.tell(), 0)
    # `buf.getvalue` depends on the seek position
    self.assertEqual(buf.getvalue(), b'')

    buf.seek(10)
    self.assertEqual(buf.tell(), 10)
    # `buf.getvalue` depends on the seek position
    self.assertEqual(buf.getvalue(), value[10:])

    buf.seek(20)
    self.assertEqual(buf.tell(), 20)
    self.assertEqual(buf.getvalue(), value)

    # Should be able to seek to P for 0 <= P <= buf.buf_len:
    for P in range(0, buf.buf_len + 1):
        buf.seek(P)

    # Cannot seek < 0
    with self.assertRaises(ValueError):
        buf.seek(-1)

    # Cannot seek > buf.buf_len
    with self.assertRaises(ValueError):
        buf.seek(buf.buf_len + 1)

    buf.close()
