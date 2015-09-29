#
# Copyright 2015 David A. Boyuka II
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CXXLD =  $(CXXLD_FOR_TESTS)

cblq_parallel_lib = $(top_builddir)/src/parallel/libpique-parallel.la

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/tests/testutil
AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(RIDCOMPRESS_CPPFLAGS) $(FASTBIT_CPPFLAGS)
AM_LDFLAGS = $(BOOST_LDFLAGS) $(RIDCOMPRESS_LDFLAGS) $(FASTBIT_LDFLAGS)

COMMON_LIBS = $(MPILIBS) $(RIDCOMPRESS_LIBS) $(FASTBIT_LIBS)
PLL_CBLQ_LIBS = $(cblq_parallel_lib) $(COMMON_LIBS)

# Number of processes for parallel tests (default 4 if not set by user)
NPROC ?= 4
MPIRUN ?= mpirun
MPIRUN_NP ?= np

TESTUTIL_HDRS = \
	../testutil/make-index.hpp \
	../testutil/write-and-verify-index.hpp \
	../testutil/write-dataset-metafile.hpp

SCRIPT_TESTS = \
	test-parallel-index-io.sh \
	test-parallel-index-gen.sh

# Removed due it being bugged, and MPI gather stream being a bad idea in the first place	
#	test-mpi-gather-stream.sh

EXE_HELPERS = \
	test-parallel-index-io \
	test-parallel-index-gen

#	test-mpi-gather-stream
	
EXE_TESTS = \
	test-mpi-stream-device \
	test-mpi-stream-buffering

TESTS =
if RUN_PLL_TESTS
TESTS += \
	$(EXE_TESTS) \
	$(SCRIPT_TESTS)
endif

check_PROGRAMS = \
	$(EXE_TESTS) \
	$(EXE_HELPERS)

# Automated tests
testworkdir = test.work.dir
TESTS_ENVIRONMENT = \
	NPROC=$(NPROC) MPIRUN=$(MPIRUN) MPIRUN_NP=$(MPIRUN_NP) \
	top_builddir=$(top_builddir) \
	top_srcdir=$(top_srcdir) \
	testworkdir=$(testworkdir)

$(TESTS): $(testworkdir)/.exists
$(testworkdir)/.exists:
	mkdir -p $(testworkdir)
	touch $(testworkdir)/.exists
clean-local:
	rm -rf $(testworkdir)

test_mpi_stream_device_SOURCES = test-mpi-stream-device.cpp
test_mpi_stream_device_LDADD = $(PLL_CBLQ_LIBS)

test_mpi_stream_buffering_SOURCES = test-mpi-stream-buffering.cpp
test_mpi_stream_buffering_LDADD = $(PLL_CBLQ_LIBS)

#test_mpi_gather_stream_SOURCES = test-mpi-gather-stream.cpp
#test_mpi_gather_stream_LDADD = $(PLL_CBLQ_LIBS)

test_parallel_index_io_SOURCES = test-parallel-index-io.cpp $(TESTUTIL_HDRS)
test_parallel_index_io_LDADD = $(PLL_CBLQ_LIBS)

test_parallel_index_gen_SOURCES = test-parallel-index-gen.cpp $(TESTUTIL_HDRS)
test_parallel_index_gen_LDADD = $(PLL_CBLQ_LIBS)
