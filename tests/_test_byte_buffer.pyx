# -*- coding: utf-8 -*-
# cython: c_string_type=bytes
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

# Import unittest module (requires `unittest2` for Python 2.x)
if sys.version_info[0] == 2:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The 'unittest2' module is required for Python 2.x")
else:
    import unittest

from fastavro.c_buffer cimport (
    Stream, ByteBuffer, cSEEK_SET, cSEEK_CUR, cSEEK_END, SSize_t, uchar,
)


ascii_letters = b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


class TestBase(unittest.TestCase):

    def dummy(self):
        pass

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


    def test_buffer_resize(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        # Write 15 bytes
        buf.write(b'x' * 15)
        # Writing 15 bytes should not resize the buffer
        self.assertEqual(buf.size(), original_size)

        # Write 1 byte
        # Writing 1 addition byte WILL cause the buffer to resize -
        # Even though we will have writen a total of 16 bytes and the buffer size
        # is 16 bytes: the buffer will resize whenever buf.data_len >= buf.buf_len
        buf.write(b'y')
        self.assertNotEqual(buf.size(), original_size)
        # The buffer will be resized to the next power of 2 > 16 * 2 == 32
        self.assertEqual(buf.size(), 32)

        # Write 32 bytes
        buf.write(b'z' * 32)
        # The buffer will be resized to the next power of 2 > 64 * 2 == 128
        self.assertEqual(buf.size(), 128)


    def test_buffer_write_empty_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        buf.write(b'')
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        self.assertEqual(buf.size(), original_size)
        buf.close()


    def test_buffer_write_not_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        # Need to write a function to bypass Cython's static type inference
        unicode_val = lambda: u'κόσμε'
        with self.assertRaises(TypeError):
            buf.write(unicode_val())


    def test_buffer_write(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)

        written = b''
        # Write 10 bytes
        value1 = b'x' * 10
        buf.write(value1)
        written += value1
        tell1 = buf.tell()
        self.assertEqual(tell1, len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)

        # Write 20 bytes
        value2 = b'y' * 20
        buf.write(value2)
        written += value2
        tell2 = buf.tell()
        self.assertEqual(tell2, tell1 + len(value2))
        self.assertEqual(len(buf), len(written))
        self.assertEqual(buf.getvalue(), written)

        # Seek to zero, overwrite old data with 28 bytes
        value3 = b'z' * 28
        buf.seek(0)
        buf.write(value3)
        tell3 = buf.tell()
        self.assertEqual(tell3, len(value3))
        self.assertEqual(len(buf), len(value3))
        self.assertEqual(buf.getvalue(), value3)

        # Seek to 10, write 40 bytes
        value4 = b'#' * 40
        buf.seek(10)
        buf.write(value4)
        tell4 = buf.tell()
        self.assertEqual(tell4, 50)
        self.assertEqual(len(buf), 50)
        self.assertEqual(buf.getvalue(), value3[:10] + value4)

        buf.close()


    def test_buffer_write_chars_empty_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        cdef bytes value = b''
        cdef uchar* value_ptr = value
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()


    def test_buffer_write_chars_null_ptr(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        cdef uchar* value_ptr = NULL
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()


    def test_buffer_write_chars_no_length(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)
        original_size = buf.size()

        cdef bytes value = b'test'
        cdef uchar* value_ptr = value
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()


    def test_buffer_write_chars(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)

        cdef bytes written = b''
        # Write 10 bytes
        cdef bytes value1 = b'x' * 10
        cdef uchar* value1_ptr = value1
        buf.write_chars(value1_ptr, len(value1))
        written += value1
        tell1 = buf.tell()
        self.assertEqual(tell1, len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)

        # Write 20 bytes
        cdef bytes value2 = b'y' * 20
        cdef uchar* value2_ptr = value2
        buf.write_chars(value2_ptr, len(value2))
        written += value2
        tell2 = buf.tell()
        self.assertEqual(tell2, tell1 + len(value2))
        self.assertEqual(len(buf), len(written))
        self.assertEqual(buf.getvalue(), written)

        # Seek to zero, overwrite old data with 28 bytes
        cdef bytes value3 = b'z' * 28
        cdef uchar* value3_ptr = value3
        buf.seek(0)
        buf.write_chars(value3_ptr, len(value3))
        tell3 = buf.tell()
        self.assertEqual(tell3, len(value3))
        self.assertEqual(len(buf), len(value3))
        self.assertEqual(buf.getvalue(), value3)

        # Seek to 10, write 40 bytes
        cdef bytes value4 = b'#' * 40
        cdef uchar* value4_ptr = value4
        buf.seek(10)
        buf.write_chars(value4_ptr, len(value4))
        tell4 = buf.tell()
        self.assertEqual(tell4, 50)
        self.assertEqual(len(buf), 50)
        self.assertEqual(buf.getvalue(), value3[:10] + value4)

        buf.close()


    def test_buffer_write(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(16)

        written = b''
        # Write 10 bytes
        value1 = b'x' * 10
        buf.write(value1)
        written += value1
        tell1 = buf.tell()
        self.assertEqual(tell1, len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)


    def test_buffer_read(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        data_len = len(value)
        buf.write(value)

        # Can't read when pos is at EOF
        self.assertEqual(buf.tell(), data_len)
        self.assertEqual(buf.read(1), b'')

        buf.seek(0)
        self.assertEqual(buf.read(1), value[0:1])
        self.assertEqual(buf.tell(), 1)
        self.assertEqual(buf.read(1), value[1:2])
        self.assertEqual(buf.tell(), 2)
        self.assertEqual(buf.read(10), value[2:12])
        self.assertEqual(buf.tell(), 12)

        # Read all
        buf.seek(0)
        self.assertEqual(buf.read(data_len), value)
        self.assertEqual(buf.tell(), data_len)


    def test_buffer_read_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        data_len = len(value)
        buf.write(value)

        # Trying to read past EOF returns whatever is available
        buf.seek(0)
        self.assertEqual(buf.read(999), value)
        self.assertEqual(buf.tell(), data_len)

        buf.seek_to(-5, cSEEK_END)
        self.assertEqual(buf.read(999), value[-5:])
        self.assertEqual(buf.tell(), data_len)


    def test_buffer_read_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Reading length 0 returns b''
        buf.seek(10)
        self.assertEqual(buf.read(0), b'')

        # Reading length < 0 is not an error -- returns b''
        buf.seek(15)
        self.assertEqual(buf.read(-10), b'')


    def test_buffer_read_chars(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        data_len = len(value)
        buf.write(value)

        cdef uchar* char_p
        cdef bytes chars
        cdef SSize_t chars_read

        # Can't read when pos is at EOF
        self.assertEqual(buf.tell(), data_len)
        char_p = buf.read_chars(1, &chars_read)
        self.assertEqual(chars_read, 0)
        self.assertEqual(<long>char_p, 0)

        buf.seek(0)
        char_p = buf.read_chars(1, &chars_read)
        self.assertEqual(chars_read, 1)
        chars = char_p[:chars_read]
        self.assertEqual(chars, value[0:1])
        self.assertEqual(buf.tell(), 1)

        char_p = buf.read_chars(1, &chars_read)
        self.assertEqual(chars_read, 1)
        chars = char_p[:chars_read]
        self.assertEqual(chars, value[1:2])
        self.assertEqual(buf.tell(), 2)

        char_p = buf.read_chars(10, &chars_read)
        self.assertEqual(chars_read, 10)
        chars = char_p[:chars_read]
        self.assertEqual(chars, value[2:12])
        self.assertEqual(buf.tell(), 12)

        # Read all
        buf.seek(0)
        char_p = buf.read_chars(data_len, &chars_read)
        self.assertEqual(chars_read, data_len)
        chars = char_p[:data_len]
        self.assertEqual(chars, value)
        self.assertEqual(buf.tell(), data_len)


    def test_buffer_read_chars_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        data_len = len(value)
        buf.write(value)

        cdef uchar* char_p
        cdef bytes chars
        cdef SSize_t chars_read

        # Trying to read past EOF returns whatever is available
        buf.seek(0)
        char_p = buf.read_chars(999, &chars_read)
        self.assertEqual(chars_read, data_len)
        chars = char_p[:chars_read]
        self.assertEqual(chars, value)
        self.assertEqual(buf.tell(), data_len)

        buf.seek_to(-5, cSEEK_END)
        char_p = buf.read_chars(999, &chars_read)
        self.assertEqual(chars_read, 5)
        chars = char_p[:chars_read]
        self.assertEqual(chars, value[-5:])
        self.assertEqual(buf.tell(), data_len)


    def test_buffer_read_chars_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* char_p
        cdef bytes chars
        cdef SSize_t chars_read

        # Reading length 0 returns b''
        buf.seek(10)
        char_p = buf.read_chars(0, &chars_read)
        self.assertEqual(chars_read, 0)
        self.assertEqual(<long>char_p, 0)

        # Reading length < 0 is not an error -- returns b''
        buf.seek(15)
        char_p = buf.read_chars(-10, &chars_read)
        self.assertEqual(chars_read, 0)
        self.assertEqual(<long>char_p, 0)


    def test_buffer_seek(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters[:20]
        buf.write(value)
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(buf.getvalue(), value)

        buf.seek(0)
        self.assertEqual(buf.tell(), 0)
        # `buf.getvalue` depends on the seek position
        self.assertEqual(buf.getvalue(), b'')

        buf.seek(12)
        self.assertEqual(buf.tell(), 12)
        # `buf.getvalue` depends on the seek position
        self.assertEqual(buf.getvalue(), value[:12])

        buf.seek(20)
        self.assertEqual(buf.tell(), 20)
        self.assertEqual(buf.getvalue(), value)

        # Should be able to seek to pos for 0 <= pos <= buf.data_len:
        for pos in range(0, 21):
            buf.seek(pos)

        # Cannot seek < 0
        with self.assertRaises(ValueError):
            buf.seek(-1)

        # Cannot seek > buf.data_len
        with self.assertRaises(ValueError):
            buf.seek(21)

        buf.close()


    def test_buffer_seek_to(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(2)

        cdef bytes value = ascii_letters[:20]
        buf.write(value)
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(buf.getvalue(), value)
        buf.seek(0)

        buf.seek_to(0, cSEEK_END)
        self.assertEqual(buf.tell(), 20)
        self.assertEqual(buf.getvalue(), value)

        buf.seek_to(-20, cSEEK_END)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')

        buf.seek_to(-5, cSEEK_END)
        self.assertEqual(buf.tell(), 15)
        self.assertEqual(buf.getvalue(), value[:15])

        buf.seek_to(-5, cSEEK_CUR)
        self.assertEqual(buf.tell(), 10)
        self.assertEqual(buf.getvalue(), value[:10])

        buf.seek_to(-10, cSEEK_CUR)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')

        buf.seek_to(15, cSEEK_CUR)
        self.assertEqual(buf.tell(), 15)
        self.assertEqual(buf.getvalue(), value[:15])

        # Cannot seek > buf.data_len
        with self.assertRaises(ValueError):
            buf.seek_to(1, cSEEK_END)
        # Cannot seek < 0
        with self.assertRaises(ValueError):
            buf.seek_to(-21, cSEEK_END)

        buf.seek(10)

        # Cannot seek > buf.data_len
        with self.assertRaises(ValueError):
            buf.seek_to(11, cSEEK_CUR)
        # Cannot seek < 0
        with self.assertRaises(ValueError):
            buf.seek_to(-11, cSEEK_CUR)

        buf.close()
