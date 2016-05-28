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

    def test_buffer_create_and_close(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(100)
        self.assertIsInstance(buf, Stream)
        self.assertEqual(buf.size(), 100)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')

        # close() may be called multiple times, will no ill effects
        for _ in range(3):
            buf.close()

            # `closed` property is set
            self.assertTrue(buf.closed)

            # These methods should all return zero
            self.assertEqual(buf.tell(), 0)
            self.assertEqual(buf.size(), 0)
            self.assertEqual(len(buf), 0)

            # The corresponding attributes should be set to zero
            self.assertEqual(buf.pos, 0)
            self.assertEqual(buf.buf_len, 0)
            self.assertEqual(buf.data_len, 0)

    def test_buffer_create_with_no_size_arg(self):
        cdef ByteBuffer buf
        buf = ByteBuffer()
        # Creating ByteBuffer with no `size` argument does not create an empty
        # buffer -- the buffer will start with some default size
        self.assertTrue(buf.size())
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()

    def test_buffer_when_closed_raises_exceptions(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(100)
        self.assertIsInstance(buf, Stream)
        self.assertEqual(buf.size(), 100)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')

        buf.close()
        self.assertTrue(buf.closed)

        cdef bytes value = b'foobar'
        cdef uchar* value_ptr = value
        cdef uchar* data_p
        cdef SSize_t data_len

        # Calling these methods should raise an exception
        with self.assertRaises(ValueError):
            buf.getvalue()
        with self.assertRaises(ValueError):
            buf.write(value)
        with self.assertRaises(ValueError):
            buf.write_chars(value_ptr, len(value))
        with self.assertRaises(ValueError):
            buf.read(1)
        with self.assertRaises(ValueError):
            data_p = buf.read_chars(1, &data_len)
        with self.assertRaises(ValueError):
            buf.peek(1)
        with self.assertRaises(ValueError):
            data_p = buf.peek_chars(1, &data_len)
        with self.assertRaises(ValueError):
            buf.truncate(0)

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
        buf.close()

    def test_buffer_len(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        self.assertEqual(len(buf), 0)
        self.assertEqual(len(buf), buf.data_len)

        # `size` is not the same as `len`
        self.assertNotEqual(len(buf), buf.size())

        buf.write(b'x')
        self.assertEqual(len(buf), 1)

        buf.write(b'x' * 29)
        self.assertEqual(len(buf), 30)
        self.assertEqual(len(buf), buf.data_len)

        buf.seek(0)
        # Seek does not affect `len`
        self.assertEqual(len(buf), 30)
        self.assertEqual(len(buf), buf.data_len)

        value = buf.getvalue()
        # Calling `getvalue` does not affect `len`
        self.assertEqual(len(buf), 30)
        self.assertEqual(len(buf), buf.data_len)

        buf.seek(0)
        value = buf.read(30)
        # Calling `read` does not affect `len`
        self.assertEqual(len(buf), 30)
        self.assertEqual(len(buf), buf.data_len)

        buf.close()
        # Calling `close` sets `len` to 0
        self.assertEqual(len(buf), 0)
        self.assertEqual(len(buf), buf.data_len)

    def test_buffer_write_empty_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)
        original_size = buf.size()

        buf.write(b'')
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        self.assertEqual(buf.size(), original_size)
        buf.close()

    def test_buffer_write_not_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)
        original_size = buf.size()

        # Need to write a function to bypass Cython's static type inference
        unicode_val = lambda: u'κόσμε'
        with self.assertRaises(TypeError):
            buf.write(unicode_val())
        buf.close()

    def test_buffer_write(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        written = b''
        # Write 10 bytes
        value1 = b'x' * 10
        buf.write(value1)
        written += value1
        pos1 = buf.tell()
        self.assertEqual(pos1, len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)

        # Write 20 bytes
        value2 = b'y' * 20
        buf.write(value2)
        written += value2
        pos2 = buf.tell()
        self.assertEqual(pos2, pos1 + len(value2))
        self.assertEqual(len(buf), len(written))
        self.assertEqual(buf.getvalue(), written)

        # Seek to zero, overwrite old data with 28 bytes
        value3 = b'z' * 28
        buf.seek(0)
        buf.write(value3)
        pos3 = buf.tell()
        self.assertEqual(pos3, len(value3))
        self.assertEqual(len(buf), len(value3))
        self.assertEqual(buf.getvalue(), value3)

        # Seek to 10, write 40 bytes
        value4 = b'#' * 40
        buf.seek(10)
        buf.write(value4)
        self.assertEqual(buf.tell(), 50)
        self.assertEqual(len(buf), 50)
        self.assertEqual(buf.getvalue(), value3[:10] + value4)
        buf.close()

    def test_buffer_write_chars_empty_bytes(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)
        original_size = buf.size()

        cdef bytes value = b''
        cdef uchar* value_ptr = value
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()

    def test_buffer_write_chars_null_ptr(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)
        original_size = buf.size()

        cdef uchar* value_ptr = NULL
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()

    def test_buffer_write_chars_no_length(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)
        original_size = buf.size()

        cdef bytes value = b'test'
        cdef uchar* value_ptr = value
        buf.write_chars(value_ptr, 0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.getvalue(), b'')
        buf.close()

    def test_buffer_write_chars(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes written = b''
        # Write 10 bytes
        cdef bytes value1 = b'x' * 10
        cdef uchar* value1_ptr = value1
        buf.write_chars(value1_ptr, len(value1))
        written += value1
        pos1 = buf.tell()
        self.assertEqual(pos1, len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)

        # Write 20 bytes
        cdef bytes value2 = b'y' * 20
        cdef uchar* value2_ptr = value2
        buf.write_chars(value2_ptr, len(value2))
        written += value2
        pos2 = buf.tell()
        self.assertEqual(pos2, pos1 + len(value2))
        self.assertEqual(len(buf), len(written))
        self.assertEqual(buf.getvalue(), written)

        # Seek to zero, overwrite old data with 28 bytes
        cdef bytes value3 = b'z' * 28
        cdef uchar* value3_ptr = value3
        buf.seek(0)
        buf.write_chars(value3_ptr, len(value3))
        pos3 = buf.tell()
        self.assertEqual(pos3, len(value3))
        self.assertEqual(len(buf), len(value3))
        self.assertEqual(buf.getvalue(), value3)

        # Seek to 10, write 40 bytes
        cdef bytes value4 = b'#' * 40
        cdef uchar* value4_ptr = value4
        buf.seek(10)
        buf.write_chars(value4_ptr, len(value4))
        self.assertEqual(buf.tell(), 50)
        self.assertEqual(len(buf), 50)
        self.assertEqual(buf.getvalue(), value3[:10] + value4)
        buf.close()

    def test_buffer_write(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        written = b''
        # Write 10 bytes
        value1 = b'x' * 10
        buf.write(value1)
        written += value1
        self.assertEqual(buf.tell(), len(value1))
        self.assertEqual(len(buf), len(value1))
        self.assertEqual(buf.getvalue(), written)
        buf.close()

    def test_buffer_read(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Read at EOF returns b''
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(buf.read(1), b'')
        self.assertEqual(buf.tell(), len(value))  # does not advance

        buf.seek(0)
        self.assertEqual(buf.read(1), value[0:1])
        self.assertEqual(buf.tell(), 1)
        self.assertEqual(buf.read(1), value[1:2])
        self.assertEqual(buf.tell(), 2)
        self.assertEqual(buf.read(10), value[2:12])
        self.assertEqual(buf.tell(), 12)

        # Read from last byte
        buf.seek(len(buf)-1)
        self.assertEqual(buf.read(1), value[-1:])
        self.assertEqual(buf.tell(), len(value))

        # Read all
        buf.seek(0)
        self.assertEqual(buf.read(len(value)), value)
        self.assertEqual(buf.tell(), len(value))
        buf.close()

    def test_buffer_read_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Trying to read past EOF returns whatever is available
        buf.seek(0)
        self.assertEqual(buf.read(999), value)
        self.assertEqual(buf.tell(), len(value))

        buf.seek_to(-5, cSEEK_END)
        self.assertEqual(buf.read(999), value[-5:])
        self.assertEqual(buf.tell(), len(value))
        buf.close()

    def test_buffer_read_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Read with length 0 returns b''
        buf.seek(10)
        self.assertEqual(buf.read(0), b'')

        # Read with length < 0 is not an error -- returns b''
        buf.seek(15)
        self.assertEqual(buf.read(-10), b'')
        buf.close()

    def test_buffer_read_chars(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Read at EOF returns no data (NULL ptr)
        self.assertEqual(buf.tell(), len(value))
        data_p = buf.read_chars(1, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)
        self.assertEqual(buf.tell(), len(value))  # does not advance

        buf.seek(0)
        data_p = buf.read_chars(1, &data_len)
        self.assertEqual(data_len, 1)
        data = data_p[:data_len]
        self.assertEqual(data, value[0:1])
        self.assertEqual(buf.tell(), 1)

        data_p = buf.read_chars(1, &data_len)
        self.assertEqual(data_len, 1)
        data = data_p[:data_len]
        self.assertEqual(data, value[1:2])
        self.assertEqual(buf.tell(), 2)

        data_p = buf.read_chars(10, &data_len)
        self.assertEqual(data_len, 10)
        data = data_p[:data_len]
        self.assertEqual(data, value[2:12])
        self.assertEqual(buf.tell(), 12)

        # Read from last byte
        buf.seek(len(buf)-1)
        data_p = buf.read_chars(1, &data_len)
        self.assertEqual(data_len, 1)
        data = data_p[:data_len]
        self.assertEqual(data, value[-1:])
        self.assertEqual(buf.tell(), len(value))

        # Read all
        buf.seek(0)
        data_p = buf.read_chars(len(value), &data_len)
        self.assertEqual(data_len, len(value))
        data = data_p[:data_len]
        self.assertEqual(data, value)
        self.assertEqual(buf.tell(), len(value))
        buf.close()

    def test_buffer_read_chars_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Trying to read past EOF returns whatever is available
        buf.seek(0)
        data_p = buf.read_chars(999, &data_len)
        self.assertEqual(data_len, len(value))
        data = data_p[:data_len]
        self.assertEqual(data, value)
        self.assertEqual(buf.tell(), len(value))

        buf.seek_to(-5, cSEEK_END)
        data_p = buf.read_chars(999, &data_len)
        self.assertEqual(data_len, 5)
        data = data_p[:data_len]
        self.assertEqual(data, value[-5:])
        self.assertEqual(buf.tell(), len(value))
        buf.close()

    def test_buffer_read_chars_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Read with length 0 returns no data (NULL ptr)
        buf.seek(10)
        data_p = buf.read_chars(0, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)

        # Read with length < 0 is not an error - returns no data (NULL ptr)
        buf.seek(15)
        data_p = buf.read_chars(-10, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)
        buf.close()

    def test_buffer_peek(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Peek at EOF returns b''
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(buf.peek(1), b'')
        self.assertEqual(buf.tell(), len(value))  # does not advance

        buf.seek(0)
        self.assertEqual(buf.peek(1), value[0:1])
        self.assertEqual(buf.tell(), 0)  # does not advance

        self.assertEqual(buf.peek(8), value[0:8])
        self.assertEqual(buf.tell(), 0)  # does not advance

        buf.seek(10)
        self.assertEqual(buf.peek(20), value[10:30])
        self.assertEqual(buf.tell(), 10)  # does not advance

        # Peek from last byte
        buf.seek(len(buf)-1)
        pos = buf.tell()
        self.assertEqual(buf.peek(1), value[-1:])
        self.assertEqual(buf.tell(), pos)  # does not advance

        # Peek all
        buf.seek(0)
        self.assertEqual(buf.peek(len(value)), value)
        self.assertEqual(buf.tell(), 0)  # does not advance
        buf.close()

    def test_buffer_peek_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Trying to peek past EOF returns whatever is available
        buf.seek(0)
        self.assertEqual(buf.peek(999), value)
        self.assertEqual(buf.tell(), 0)  # does not advance

        buf.seek_to(-5, cSEEK_END)
        pos = buf.tell()
        self.assertEqual(buf.peek(999), value[-5:])
        self.assertEqual(buf.tell(), pos)  # does not advance
        buf.close()

    def test_buffer_peek_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        # Peek with length 0 returns b''
        buf.seek(10)
        self.assertEqual(buf.peek(0), b'')
        self.assertEqual(buf.tell(), 10)  # does not advance

        # Peek with length < 0 is not an error -- returns b''
        buf.seek(15)
        self.assertEqual(buf.peek(-10), b'')
        self.assertEqual(buf.tell(), 15)  # does not advance
        buf.close()

    def test_buffer_peek_chars(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Peek at EOF returns no data
        self.assertEqual(buf.tell(), len(value))
        data_p = buf.peek_chars(1, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)
        # buf.pos does not advance
        self.assertEqual(buf.tell(), len(value))

        buf.seek(0)
        data_p = buf.peek_chars(1, &data_len)
        self.assertEqual(data_len, 1)
        data = data_p[:data_len]
        self.assertEqual(data, value[0:1])
        self.assertEqual(buf.tell(), 0)  # does not advance

        data_p = buf.peek_chars(8, &data_len)
        self.assertEqual(data_len, 8)
        data = data_p[:data_len]
        self.assertEqual(data, value[0:8])
        self.assertEqual(buf.tell(), 0)  # does not advance

        buf.seek(10)
        data_p = buf.peek_chars(20, &data_len)
        self.assertEqual(data_len, 20)
        data = data_p[:data_len]
        self.assertEqual(data, value[10:30])
        self.assertEqual(buf.tell(), 10)  # does not advance

        # Peek from last byte
        buf.seek(len(buf)-1)
        pos = buf.tell()
        data_p = buf.peek_chars(1, &data_len)
        self.assertEqual(data_len, 1)
        data = data_p[:data_len]
        self.assertEqual(data, value[-1:])
        self.assertEqual(buf.tell(), pos)  # does not advance

        # Peek all
        buf.seek(0)
        data_p = buf.peek_chars(len(value), &data_len)
        self.assertEqual(data_len, len(value))
        data = data_p[:data_len]
        self.assertEqual(data, value)
        self.assertEqual(buf.tell(), 0)  # does not advance
        buf.close()

    def test_buffer_peek_chars_past_eof(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Trying to read past EOF returns whatever is available
        buf.seek(0)
        data_p = buf.peek_chars(999, &data_len)
        self.assertEqual(data_len, len(value))
        data = data_p[:data_len]
        self.assertEqual(data, value)
        self.assertEqual(buf.tell(), 0)  # does not advance

        buf.seek_to(-5, cSEEK_END)
        pos = buf.tell()
        data_p = buf.peek_chars(999, &data_len)
        self.assertEqual(data_len, 5)
        data = data_p[:data_len]
        self.assertEqual(data, value[-5:])
        self.assertEqual(buf.tell(), pos)  # does not advance
        buf.close()

    def test_buffer_peek_chars_length_leq_zero(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters
        buf.write(value)

        cdef uchar* data_p
        cdef bytes data
        cdef SSize_t data_len

        # Peek with length 0 returns no data (NULL ptr)
        buf.seek(10)
        data_p = buf.peek_chars(0, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)
        self.assertEqual(buf.tell(), 10)  # does not advance

        # Peek with length < 0 is not an error - returns no data (NULL ptr)
        buf.seek(15)
        data_p = buf.read_chars(-10, &data_len)
        self.assertEqual(data_len, 0)
        self.assertEqual(<long>data_p, 0)
        self.assertEqual(buf.tell(), 15)  # does not advance
        buf.close()

    def test_buffer_seek(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

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

        # Should be able to seek to pos for 0 <= pos <= len(buf):
        for pos in range(0, 21):
            buf.seek(pos)

        # Cannot seek < 0
        with self.assertRaises(ValueError):
            buf.seek(-1)
        # Cannot seek > len(buf)
        with self.assertRaises(ValueError):
            buf.seek(len(buf) + 1)
        buf.close()

    def test_buffer_seek_to(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        cdef bytes value = ascii_letters[:20]
        buf.write(value)
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(buf.getvalue(), value)
        buf.seek(0)

        buf.seek_to(0, cSEEK_END)
        self.assertEqual(buf.tell(), 20)
        # `buf.getvalue` depends on the seek position
        self.assertEqual(buf.getvalue(), value)

        buf.seek_to(-20, cSEEK_END)
        self.assertEqual(buf.tell(), 0)
        # `buf.getvalue` depends on the seek position
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

        # Cannot seek_to > len(buf) with cSEEK_END
        with self.assertRaises(ValueError):
            buf.seek_to(1, cSEEK_END)
        # Cannot seek_to < 0 with cSEEK_END
        with self.assertRaises(ValueError):
            buf.seek_to(-21, cSEEK_END)

        buf.seek(10)
        # Cannot seek_to > len(buf) with cSEEK_CUR
        one_past_end = len(buf) - buf.tell() + 1
        with self.assertRaises(ValueError):
            buf.seek_to(one_past_end, cSEEK_CUR)
        # Cannot seek_to < 0 with cSEEK_CUR
        negative_one = -buf.tell() -1
        with self.assertRaises(ValueError):
            buf.seek_to(negative_one, cSEEK_CUR)

        buf.close()

    def test_buffer_reset(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        buf.write(b'x' * 30)
        self.assertEqual(buf.tell(), 30)

        buf.reset()
        # `reset` is equivalent to `seek(0)`
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.pos, 0)
        buf.seek(0)
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(buf.pos, 0)
        buf.close()

    def test_buffer_truncate(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(200)

        value = ascii_letters * 3  # 156 bytes
        buf.write(value)
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(len(buf), len(value))
        self.assertEqual(buf.getvalue(), value)
        self.assertEqual(buf.size(), 200)

        buf.truncate(100)
        # buf.pos will be moved to new end of the data
        self.assertEqual(buf.tell(), 100)
        self.assertEqual(len(buf), 100)
        self.assertEqual(buf.getvalue(), value[:100])
        # buf.size() remains unchanged
        self.assertEqual(buf.size(), 200)

        buf.truncate(1)
        # buf.pos will be moved to new end of the data
        self.assertEqual(buf.tell(), 1)
        self.assertEqual(len(buf), 1)
        self.assertEqual(buf.getvalue(), value[:1])
        # buf.size() remains unchanged
        self.assertEqual(buf.size(), 200)

        buf.truncate(0)
        # buf.pos will be moved to new end of the data
        self.assertEqual(buf.tell(), 0)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.getvalue(), b'')
        # buf.size() remains unchanged
        self.assertEqual(buf.size(), 200)
        buf.close()

    def test_buffer_truncate_out_of_bounds(self):
        cdef ByteBuffer buf
        buf = ByteBuffer(4)

        value = ascii_letters
        buf.write(value)
        self.assertEqual(buf.tell(), len(value))
        self.assertEqual(len(buf), len(value))
        self.assertEqual(buf.getvalue(), value)

        # Cannot `truncate` to larger than len(buf)
        with self.assertRaises(ValueError):
            buf.truncate(len(buf) + 1)
        # Cannot `truncate` to size < 0
        with self.assertRaises(ValueError):
            buf.truncate(-1)
        buf.close()
