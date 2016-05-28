# Makefile for fastavro

PYTHON ?= python

# Try to find the correct Python Include directory
include py_include.mk

# Try to find the correct compiled extension suffix (e.g., '.so')
include py_ext_suffix.mk


#------ Project Files

pyx_files = $(wildcard fastavro/*.pyx) \
			$(wildcard tests/*.pyx)

c_files  = $(pyx_files:.pyx=.c)
o_files  = $(pyx_files:.pyx=.o)
so_files = $(pyx_files:.pyx=$(PY_SO_SUFFIX))


#------ Cython Arguments

CYTHON    ?= cython
CYTHONIZE ?= cythonize

cython_args    :=
cython_profile := profile=True


#------ GCC compile / link settings

CC ?= gcc

cc_args = -pthread -fPIC -DNDEBUG

cc_debug = -g0

cc_warnings = -Wall -Wformat -Werror=format-security -Wstrict-prototypes \
	-Wno-unused-function -Wsign-compare -Wunreachable-code

cc_opts = -fwrapv -fno-strict-aliasing

cc_include = $(PY_INCLUDE)

cc_optimize = -O3 -ffast-math

ld_args  = -pthread -shared -Wl,-O1

ld_link =


#------ Build Rules

%.c: %.pyx
	$(CYTHON) $(cython_args) $<

%.o: %.c
	$(CC) $(cc_args) $(cc_debug) $(cc_warnings) $(cc_opts) $(cc_optimize) $(cc_include) -c $< -o $@

%$(PY_SO_SUFFIX): %.o
	$(CC) $(ld_args) $(ld_link) $< -o $@

# Build directly with `cythonize`
%$(PY_SO_SUFFIX): %.pyx
	$(CYTHONIZE) -i $<


#------ Build Targets

all: compile


# `cfiles` just calls `cython` to generate '*.pyx' => '*.c'
cfiles: $(c_files)


# `build` calls `cython` to generate '*.pyx' => '*.c', then compiles the
# modules with distutils (setup.py)
build: $(c_files)
	$(PYTHON) setup.py build_ext -i
	rm -rfv build dist *.egg-info


# `compile` calls `cython` to generate '*.pyx' => '*.c', then compiles
# the modules with GCC.
compile: $(c_files) $(o_files) $(so_files)


# `cythonize` calls `cythonize` to build '*.pyx' => '*.so'
cythonize: $(so_files)


# `prof` calls `cython` with "-X profile=True", which allows profiling the
# extension modules with Python's cProfile module.
prof: cython_args := -X $(strip $(filter -X,$(cython_args)))$(cython_profile)
prof: compile


clean:
	rm -rfv */*.c
	rm -rfv */*.o
	rm -rfv */*.s
	rm -rfv */*.so
	rm -rfv */*.pyd
	rm -rfv */__pycache__
	rm -rfv */*.pyc
	rm -rfv build dist *.egg-info

fresh: clean all

# Call `check-manifest` to verify the project manifest
# This requires the `check-manifest` package:
#   https://pypi.python.org/pypi/check-manifest
check:
	check-manifest --ignore *.c,*.o,*.so,*.pyd

publish:
	./publish.sh

test:
	PATH="${PATH}:${HOME}/.local/bin" tox

.PHONY: all cfiles build compile cythonize prof clean fresh check publish test
