# -*- coding: utf-8 -*-
"""Cython Header File: Simple byte buffer interface"""


# Would use 'ssize_t' but it's not in the C standard
ctypedef long SSize_t

ctypedef unsigned char uchar

ctypedef enum SeekType:
    cSEEK_SET = 0, cSEEK_CUR = 1,  cSEEK_END = 2


cdef class Stream(object):

    cdef SSize_t size(self) except -1
    cdef SSize_t tell(self) except -1
    cdef int seek(self, SSize_t pos) except -1
    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1
    cdef int reset(self) except -1
    cdef int write(self, bytes bytes_data) except -1
    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1
    cdef bytes read(self, SSize_t num)
    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes peek(self, SSize_t num)
    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes getvalue(self)
    cdef int truncate(self, SSize_t size) except -1
    cdef int flush(self) except -1
    cdef int close(self) except -1


cdef class StreamWrapper(Stream):

    # NOTE: Instance attributes are defined here, NOT in c_buffer.pyx
    cdef readonly object stream
    cdef bytes _read_chars_ref
    cdef bytes _peek_chars_ref

    # Methods
    cdef SSize_t size(self) except -1
    cdef SSize_t tell(self) except -1
    cdef int seek(self, SSize_t pos) except -1
    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1
    cdef int reset(self) except -1
    cdef int write(self, bytes bytes_data) except -1
    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1
    cdef bytes read(self, SSize_t num)
    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes peek(self, SSize_t num)
    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes getvalue(self)
    cdef int truncate(self, SSize_t pos) except -1
    cdef int flush(self) except -1
    cdef int close(self) except -1


cdef class ByteBuffer(Stream):

    # NOTE: Instance attributes are defined here, NOT in c_buffer.pyx
    cdef uchar *buf
    cdef readonly SSize_t buf_len
    cdef readonly SSize_t data_len
    cdef public SSize_t pos
    cdef readonly bint closed

    # Methods
    cdef SSize_t size(self) except -1
    cdef SSize_t tell(self) except -1
    cdef int seek(self, SSize_t pos) except -1
    cdef int seek_to(self, SSize_t pos, SeekType whence) except -1
    cdef int reset(self) except -1
    cdef int write(self, bytes bytes_data) except -1
    cdef int write_chars(self, uchar* char_data, SSize_t data_len) except -1
    cdef bytes read(self, SSize_t num)
    cdef uchar* read_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes peek(self, SSize_t num)
    cdef uchar* peek_chars(self, SSize_t num, SSize_t* chars_read) except <uchar*>1
    cdef bytes getvalue(self)
    cdef int truncate(self, SSize_t pos) except -1
    cdef int flush(self) except -1
    cdef int close(self) except -1

    # Private
    cdef int _resize(self, SSize_t data_len) except -1
