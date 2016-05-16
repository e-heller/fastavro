fastavro
========

The Python [`avro`][avro_pypi] package distributed by 
[Apache][apache_avro] is **incredibly slow**. When dealing with very 
large datasets, the performance of [`avro`][avro_pypi] is unacceptable,
more or less unfit for any production use.

**`fastavro`** is less feature complete than [`avro`][avro_pypi], 
however performance reading and writing Avro files is *significantly*
faster.

If you compile the optional [Cython][cython] C-extension modules, then 
**`fastavro`** is even faster, almost on par with the native Java
implementation of Avro.

The focus of this fork ([e-heller/fastavro]) is to improve the 
Cython implementation of `fastavro` to achieve performance that is
even faster. So far I've managed some success.

I hope these improvements will eventually be pulled into the upstream 
repo, but I'm not very optimistic that will happen anytime soon.

This **`fastavro`** fork supports these Python versions:

* Python 2.6
* Python 2.7
* Python 3.4
* Python 3.5


[e-heller/fastavro]: https://github.com/e-heller/fastavro
[avro_pypi]: https://pypi.python.org/pypi/avro
[apache_avro]: http://avro.apache.org
[Cython]: http://cython.org


Basic Usage
===========

Reading from Avro format
------------------------

```python
import fastavro

with open('some_file.avro', 'rb') as input:
    # Create a `Reader` object
    reader = fastavro.Reader(input)

    # Obtain the writer's schema if required
    schema = reader.schema

    # Iteratively read the records:
    for record in reader:
        process_record(record)
        
    # Read the records in one shot:
    records = list(reader)
```


Writing to Avro format
----------------------

```python
import fastavro

# Define an Avro Schema as a dict:
schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}

# Create some records:
records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

# Open a file, and write the records:
with open('some_file.avro', 'wb') as out:
    fastavro.write(out, schema, records)
```


Command line script
-------------------

You can use the `fastavro` script from the command line to dump 
`avro` files to `stdout`

```bash
$ fastavro some_file.avro
```

By default this will print one JSON object per line. You can use the 
`--pretty` flag to change this.

You can also dump the avro schema:

```bash
$ fastavro --schema some_file.avro
```


Limitations
===========

**`fastavro`** is missing many of the features of the Apache
[`avro`][avro_pypi] package. Essentially, **`fastavro`** only supports
reading and writing in Avro format. 

Notably, there is no support for:

* [Avro Protocols][spec_protocol] - no support at all.

* The [Protocol Wire Format][spec_wire_format] or any kind of Wire
  Transmission - no support at all. 


Currently there are also some limitations in reading and writing:

* Incomplete support for *'reader's schemas'* - i.e., reading an Avro
  file written with one schema (the 'writer's schema') and interpreting
  the data with another schema (the 'reader's schema').
  See [Schema Resolution][spec_schema_res] in the Avro specification
  for details.
  
* No support for *Aliases* - i.e., the `aliases` attribute in named 
  Avro types. See [Aliases][spec_aliases] in the Avro specification
  for details.
  
* No support for *Logical Types* - i.e., the `logicalType` attribute
  Avro types. See [Logical Types][spec_logical] in the Avro 
  specification for details.
  > *Note:* the [ViaSat/fastavro] fork currently has support for some
  Logical Types. I hope to merge this code soon.


[spec_wire_format]: https://avro.apache.org/docs/current/spec.html#Protocol+Wire+Format
[spec_protocol]: https://avro.apache.org/docs/current/spec.html#Protocol+Declaration
[spec_schema_res]: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
[spec_aliases]: https://avro.apache.org/docs/current/spec.html#Aliases
[spec_logical]: https://avro.apache.org/docs/current/spec.html#Logical+Types
[ViaSat/fastavro]: https://github.com/ViaSat/fastavro/


Hacking
=======

As recommended by [Cython][cython], the `fastavro` distribution includes
the Cython-generated C files. This has the advantage that the end user 
does not need to have Cython installed. (Although a C compiler is
required.)

If you decide to modify any `fastavro/*.pyx` file, then you will
require:
1. [Cython][cython_pypi] to regenerate the C files
2. Some kind of C compiler like `gcc` on Unix-y platforms

> *Note:* I have tested and successfully compiled with MSVC on Windows.

Rebuilding the C extensions is easy. First, regenerate the C files.
With the provided Makefile, you can just type `make cfiles`, which 
will invoke `cython` to regenerate the C code for each modified file.

Then recompile the C files to shared object extensions (`*.so` files
on Unix-y platforms, `*.pyd` files on Windows).

The easiest way to recompile the C files is to use the included
`setup.py` file, which utilizes Python's [`distutils`][distutils] 
module to automatically configure the build.

On the command line, navigate to the root `fastavro` directory:
```bash
$ make cfiles
$ python setup.py build_ext -i
```

Currently, you can just enter `make` to do all of the above:
```bash
$ make
```

I *definitely* recommend creating a virtual environment with your
target Python version and installing Cython within that virtual 
environment if you plan to modify and rebuild `fastavro`. If you are 
unfamiliar with virtual environments, check out [virtualenv][venv]
or [pew][pew]. I recommend [pew][pew].

[cython]: http://cython.org/
[cython_pypi]: https://pypi.python.org/pypi/Cython
[venv]: http://pypi.python.org/pypi/virtualenv
[pew]: https://pypi.python.org/pypi/pew
[distutils]: https://docs.python.org/2.7/extending/building.html


Changes
=======

See the [ChangeLog]

[ChangeLog]: https://github.com/e-heller/fastavro/blob/master/ChangeLog


Contact
=======

[The e-heller/fastavro fork](https://github.com/e-heller/fastavro/)

[Official fastavro repo](https://github.com/tebeka/fastavro)
