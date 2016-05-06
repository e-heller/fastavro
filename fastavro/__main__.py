#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Command-line tool to read an Avro file and emit the records in JSON format
to stdout.
"""

from __future__ import absolute_import

import sys

try:
    import simplejson as json
except ImportError:
    import json

import fastavro


def _json_dump(obj, indent, output=None):
    output = output or sys.stdout
    # Try to get encoding from output (eg `sys.stdout.encoding`)
    # Default to 'utf-8' if None
    encoding = getattr(output, 'encoding', None) or 'utf-8'
    json.dump(obj, output, indent=indent, encoding=encoding)


def main(argv=None):
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(
        description='Reads an Avro file, emitting the records as JSON')
    parser.add_argument('file', help='file(s) to parse', nargs='*')
    parser.add_argument('--schema', help='dump schema instead of records',
                        action='store_true', default=False)
    parser.add_argument('--codecs', help='print supported codecs',
                        action='store_true', default=False)
    parser.add_argument('--version', action='version',
                        version='fastavro %s' % fastavro.__version__)
    parser.add_argument('-p', '--pretty', help='pretty print json',
                        action='store_true', default=False)
    args = parser.parse_args(argv[1:])

    if args.codecs:
        print('\n'.join(sorted(fastavro._reader.BLOCK_READERS)))
        raise SystemExit

    files = args.file or ['-']
    for filename in files:
        if filename == '-':
            fo = sys.stdin
        else:
            try:
                fo = open(filename, 'rb')
            except IOError as e:
                raise SystemExit('error: cannot open %s - %s' % (filename, e))

        try:
            reader = fastavro.reader(fo)
        except ValueError as e:
            raise SystemExit('error: %s' % e)

        if args.schema:
            _json_dump(reader.schema, True)
            sys.stdout.write('\n')
            continue

        indent = 4 if args.pretty else None
        try:
            for record in reader:
                _json_dump(record, indent)
                sys.stdout.write('\n')
        except (IOError, KeyboardInterrupt):
            pass


if __name__ == '__main__':
    main()
