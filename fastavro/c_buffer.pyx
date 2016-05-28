# -*- coding: utf-8 -*-
# cython: c_string_type=bytes, wraparound=False
# cython: optimize.use_switch=True, always_allow_keywords=False
"""
`ByteBuffer` is a replacement for Python's cStringIO / BytesIO class.

On Python 2.x, benchmarks show this implementation can write data roughly
6-8 times faster than cStringIO.
On Python 3.x, benchmarks show this implementation can write data roughly
3-4 times faster than BytesIO.

This speed boost improves overall read/write speeds in fastavro.

`Stream` is an abstract base class representation of the API implemented by
`ByteBuffer`.

`StreamWrapper` implements a wrapper around a generic Python file-like object
so that it presents the same API as `ByteBuffer`.
This required as some functions in `c_writer` and `c_reader` read/write to both
the in-memory `ByteBuffer` *and* to the file-like object provided by the user.
"""


from __future__ import absolute_import

import cython

from libc.string cimport memcpy
from libc.stdlib cimport malloc, realloc, free


# For reference, here are the function signatures:
#   void* malloc(SSize_t size)
#   void* realloc(void* ptr, SSize_t size)
#   void  free(void* ptr)
#   void* memcpy(void *pto, const void *pfrom, SSize_t size)


# NOTE: SSize_t is defined in c_buffer.pxd
# ctypedef long SSize_t

# NOTE: The SeekType enum is defined in c_buffer.pxd
# ctypedef enum SeekType:
#     cSEEK_SET = 0, cSEEK_CUR = 1,  cSEEK_END = 2


cdef class Stream(object):
    """Abstract base class for `ByteBuffer` API."""

    def __len__(self):
        raise NotImplementedError

    cdef SSize_t size(self) except -1:
        raise NotImplementedError

    cdef SSize_t tell(self) except -1:
        raise NotImplementedError

    cdef int seek(self, SSize_t pos) except -1:
        raise NotImplementedError

    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1:
        raise NotImplementedError

    cdef int reset(self) except -1:
        raise NotImplementedError

    cdef int write(self, bytes bytes_data) except -1:
        raise NotImplementedError

    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1:
        raise NotImplementedError

    cdef bytes read(self, SSize_t num):
        raise NotImplementedError

    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        raise NotImplementedError

    cdef bytes peek(self, SSize_t num):
        raise NotImplementedError

    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        raise NotImplementedError

    cdef bytes getvalue(self):
        raise NotImplementedError

    cdef int truncate(self, SSize_t size) except -1:
        raise NotImplementedError

    cdef int flush(self) except -1:
        raise NotImplementedError

    cdef int close(self) except -1:
        raise NotImplementedError


cdef class StreamWrapper(Stream):
    """
    Wrapper for a generic Python file-like object to implement the same API as
    the `ByteBuffer` class.
    """

    # NOTE: Instance attributes are defined in c_buffer.pxd
    # cdef readonly object stream
    # cdef bytes _read_chars_ref
    # cdef bytes _peek_chars_ref

    def __cinit__(self, stream):
        self.stream = stream

    @property
    def closed(self):
        return self.stream.closed

    def __len__(self):
        self.stream.seek(0, 2)  # 2 = SEEK_END
        pos = self.stream.tell()
        self.stream.seek(0, 0)  # 0 = SEEK_SET
        return pos

    cdef SSize_t size(self) except -1:
        return len(self)

    cdef SSize_t tell(self) except -1:
        self.stream.tell()

    cdef int seek(self, SSize_t pos) except -1:
        self.stream.seek(pos, 0)  # 0 = SEEK_SET

    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1:
        self.stream.seek(pos, whence)

    cdef int reset(self) except -1:
        self.stream.seek(0, 0)  # 0 = SEEK_SET

    cdef int write(self, bytes bytes_data) except -1:
        self.stream.write(bytes_data)

    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1:
        self.stream.write(char_data[:data_len])

    cdef bytes read(self, SSize_t num):
        return self.stream.read(num)

    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        # WARNING: This is bit of a hack.
        # We set `self._read_chars_ref = data` because we MUST keep a reference
        # to the `data` returned by `stream.read` that will persist AFTER this
        # method returns. If we did not keep this reference to the `data`, the
        # object could be garbage collected before the pointer returned by this
        # method is accessed and read by the caller. When the object is garbage
        # collected, it *invalidates the pointer* returned by this method. Any
        # subsequent attempt to access memory with that pointer could result in
        # a segmentation fault.
        # As such, the caller *must* use the pointer to read from memory
        # *before* calling this method again.
        #
        # In other words, the following pattern IS NOT SAFE:
        #   >>> ptr_1 = obj.read_chars(10, &data_len_1)
        #   >>> ptr_2 = obj.read_chars(20, &data_len_2)
        #   >>> byte_data_1 = ptr_1[:data_len_1]
        #   >>> byte_data_2 = ptr_2[:data_len_2]
        #
        # Instead, read the value from memory before calling the method again:
        #   >>> ptr_1 = obj.read_chars(10, &data_len_1)
        #   >>> byte_data_1 = ptr_1[:data_len_1]
        #   >>> ptr_2 = obj.read_chars(10, &data_len_2)
        #   >>> byte_data_2 = ptr_2[:data_len_2]

        self._read_chars_ref = data = self.stream.read(num)
        cdef SSize_t data_len = len(data)
        cdef uchar* buf

        chars_read[0] = data_len
        if not (data and data_len):
            return NULL
        buf = data
        return buf

    cdef bytes peek(self, SSize_t num):
        data = self.stream.read(num)
        self.stream.seek(-len(data), 1)  # 1 = SEEK_CUR
        return data

    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        # WARNING: This is bit of a hack.
        # We set `self._peek_chars_ref = data` because we MUST keep a reference
        # to the `data` returned by `stream.read` that will persist AFTER this
        # method returns. SEE the full explanation above in `read_chars()`

        self._peek_chars_ref = data = self.stream.read(num)
        cdef SSize_t data_len = len(data)
        cdef uchar* buf

        chars_read[0] = data_len
        if not (data and data_len):
            return NULL
        buf = data
        self.stream.seek(-data_len, 1)  # 1 = SEEK_CUR
        return buf

    cdef bytes getvalue(self):
        self.stream.getvalue()

    cdef int truncate(self, SSize_t size) except -1:
        self.stream.truncate(size)

    cdef int flush(self) except -1:
        self.stream.flush()

    cdef int close(self) except -1:
        self.stream.close()


@cython.boundscheck(False)
@cython.initializedcheck(False)
@cython.nonecheck(False)
cdef class ByteBuffer(Stream):
    """
    Replacement for Python's cStringIO / BytesIO class.

    On Python 2.x, benchmarks show this implementation can write data roughly
    6-8 times faster than cStringIO.
    On Python 3.x, benchmarks show this implementation can write data roughly
    3-4 times faster than BytesIO.
    """

    # NOTE: Instance attributes are defined in c_buffer.pxd
    # cdef uchar *buf
    # cdef readonly SSize_t buf_len
    # cdef readonly SSize_t data_len
    # cdef public SSize_t pos
    # cdef readonly bint closed

    def __cinit__(self, SSize_t size=0):
        # If size is zero, start with 8 kB
        size = size or 0x2000
        self.buf = <uchar*>malloc(size)
        if not self.buf:
            raise MemoryError('malloc(%d) failed' % size)
        self.buf_len = size
        self.pos = 0
        self.data_len = 0
        self.closed = False

    def __len__(self):
        return self.data_len

    cdef SSize_t size(self) except -1:
        return self.buf_len

    cdef SSize_t tell(self) except -1:
        return self.pos

    cdef int seek(self, SSize_t pos) except -1:
        if not 0 <= pos <= self.data_len:
            raise ValueError('Cannot seek to %d' % pos)
        self.pos = pos

    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1:
        cdef SSize_t new_pos
        if whence == cSEEK_CUR:
            new_pos = self.pos + pos
        elif whence == cSEEK_SET:
            new_pos = pos
        elif whence == cSEEK_END:
            new_pos = self.data_len + pos
        else:
            raise NotImplementedError

        if not 0 <= new_pos <= self.data_len:
            raise ValueError('Cannot seek to %d' % new_pos)
        self.pos = <SSize_t>new_pos

    cdef int reset(self) except -1:
        # Equivalent to seek(0)
        self.pos = 0

    cdef int write(self, bytes bytes_data) except -1:
        if self.buf == NULL:
            raise ValueError('Buffer is closed')
        if not bytes_data:
            return 0

        cdef uchar* char_data = bytes_data
        cdef SSize_t data_len = len(bytes_data)

        if self.pos + data_len >= self.buf_len:
            self._resize(data_len)

        if not memcpy(self.buf + self.pos, char_data, data_len):
            raise MemoryError('memcpy() failed')

        self.pos += data_len
        self.data_len = self.pos

    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1:
        if self.buf == NULL:
            raise ValueError('Buffer is closed')
        if not (char_data and data_len):
            return 0

        if self.pos + data_len >= self.buf_len:
            self._resize(data_len)

        if not memcpy(self.buf + self.pos, char_data, data_len):
            raise MemoryError('memcpy() failed')

        self.pos += data_len
        self.data_len = self.pos

    cdef bytes read(self, SSize_t num):
        if self.buf == NULL:
            raise ValueError('Buffer is closed')

        cdef SSize_t to_pos = min(self.data_len, self.pos + num)

        if to_pos <= self.pos:
            return b''

        cdef bytes data = self.buf[self.pos:to_pos]
        self.pos = to_pos
        return data

    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        if self.buf == NULL:
            raise ValueError('Buffer is closed')

        cdef SSize_t to_pos = min(self.data_len, self.pos + num)

        if to_pos <= self.pos:
            chars_read[0] = 0
            return NULL

        chars_read[0] = to_pos - self.pos
        cdef uchar* buf_pos = self.buf + self.pos
        self.pos = to_pos
        return buf_pos

    cdef bytes peek(self, SSize_t num):
        if self.buf == NULL:
            raise ValueError('Buffer is closed')

        cdef SSize_t to_pos = min(self.data_len, self.pos + num)

        if to_pos <= self.pos:
            return b''

        cdef bytes data = self.buf[self.pos:to_pos]
        return data

    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1:
        if self.buf == NULL:
            raise ValueError('Buffer is closed')

        cdef SSize_t to_pos = min(self.data_len, self.pos + num)

        if to_pos <= self.pos:
            chars_read[0] = 0
            return NULL

        chars_read[0] = to_pos - self.pos
        return self.buf + self.pos

    cdef bytes getvalue(self):
        if self.buf == NULL:
            raise ValueError('Buffer is closed')
        if not 0 < self.pos <= self.data_len:
            return b''
        cdef bytes data = self.buf[0:self.pos]
        return data

    cdef int truncate(self, SSize_t size) except -1:
        # For simplicity, we don't actually realloc the memory buffer;
        # Just reset `self.pos` and `self.data_len`
        if self.buf == NULL:
            raise ValueError('Buffer is closed')
        if not 0 <= size <= self.data_len:
            raise ValueError('Cannot truncate to %d' % size)

        self.data_len = size
        self.pos = min(size, self.pos)

    cdef int flush(self) except -1:
        # This function is here for completeness
        return 0

    cdef int close(self) except -1:
        self.data_len = 0
        self.buf_len = 0
        self.pos = 0
        self.closed = True
        if self.buf:
            free(self.buf)
        self.buf = NULL

    def __dealloc__(self):
        # Cython documentation states not to call any methods in __dealloc__()
        # So, we'll just repeat the same code from close()
        self.data_len = 0
        self.buf_len = 0
        self.pos = 0
        self.closed = True
        if self.buf:
            free(self.buf)
        self.buf = NULL

    # Private Methods

    cdef int _resize(self, SSize_t data_len) except -1:
        cdef SSize_t min_len, new_len
        min_len = self.pos + data_len
        if min_len > 0x3FFFFFF:
            # If the buffer has grown to over 64 MB, stop increasing the size
            # exponentially. Instead, let's add a chunk of memory about 25% of
            # the current buffer size.
            new_len = min_len + (self.buf_len >> 2)
        else:
            # Let's keep the buffer size to even powers of 2
            new_len = to_pow_2(min_len * 2)
        self.buf = <uchar*>realloc(self.buf, new_len)
        if not self.buf:
            raise MemoryError('realloc(%d) failed' % new_len)
        self.buf_len = new_len


cdef inline int to_pow_2(int v) except -1:
    """Round up `v` to the nearest power of 2. Valid for 32-bit ints"""
    # Shamelessly stolen from:
    #   https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    return v + 1
