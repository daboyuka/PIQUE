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

SUBDIRS = parallel

cblq_lib = $(top_builddir)/src/libpique.la

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/tests/testutil
AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(RIDCOMPRESS_CPPFLAGS) $(FASTBIT_CPPFLAGS)
AM_LDFLAGS = $(BOOST_LDFLAGS) $(RIDCOMPRESS_LDFLAGS) $(FASTBIT_LDFLAGS)

COMMON_LIBS = $(RIDCOMPRESS_LIBS) $(FASTBIT_LIBS)
CBLQ_LIBS = $(cblq_lib) $(COMMON_LIBS)

TESTUTIL_HDRS = \
	testutil/make-index.hpp \
	testutil/write-and-verify-index.hpp \
	testutil/write-dataset-metafile.hpp

TESTS = \
	test-zo-iter \
	test-dilate \
	test-cache-ptr \
	test-quantization \
	test-region-serialize \
	test-region-ridconv \
	test-index-binorder \
	test-index-io \
	test-index-io-cache \
	test-query-io \
	test-query-engines \
	test-query-binnings \
	test-expl-binning \
	test-query-encodings \
	test-cii-setops \
	test-cblq-setops \
	test-setops \
	test-setops-simplify \
	test-cii-decode \
	test-bitmap-conv \
	test-bitmap-bitcount \
	test-subset-datastream \
	test-dataset-index-build \
	test-database-index-build \
	test-database-loadsave

MANUAL_TESTS = \
	test-cblq-semiwords \
	test-cblq-dense \
	test-cblq-dense2 \
	test-cblq-intersect \
	test-cblq-build


check_PROGRAMS = \
	$(TESTS) \
	$(MANUAL_TESTS)

# Automated tests
testworkdir = test.work.dir
TESTS_ENVIRONMENT = \
	top_builddir=$(top_builddir) \
	top_srcdir=$(top_srcdir) \
	testworkdir=$(testworkdir)

$(TESTS): $(testworkdir)/.exists
$(testworkdir)/.exists:
	mkdir -p $(testworkdir)
	touch $(testworkdir)/.exists
clean-local:
	rm -rf $(testworkdir)

# Utility tests (generally for classes in src/util)
test_zo_iter_SOURCES = util/test-zo-iter.cpp
test_dilate_SOURCES = util/test-dilate.cpp
test_cache_ptr_SOURCES = util/test-cache-ptr.cpp

# Region tests (generally on classes in src/indexing and src/convert, does
# not include set operation algorithm tests)
test_region_ridconv_SOURCES = region/test-region-ridconv.cpp
test_region_ridconv_LDADD = $(CBLQ_LIBS)

test_cii_decode_SOURCES = region/test-cii-decode.cpp
test_cii_decode_LDADD = $(CBLQ_LIBS)

test_bitmap_conv_SOURCES = region/test-bitmap-conv.cpp
test_bitmap_conv_LDADD = $(CBLQ_LIBS)

test_bitmap_bitcount_SOURCES = region/test-bitmap-bitcount.cpp
test_bitmap_bitcount_LDADD = $(CBLQ_LIBS)

# Quantization tests (generally on classes in src/indexing)
test_quantization_SOURCES = test-quantization.cpp
test_quantization_LDADD = $(CBLQ_LIBS)

test_index_binorder_SOURCES = test-index-binorder.cpp
test_index_binorder_LDADD = $(CBLQ_LIBS)

# Set operation algorithm tests (generally on classes in src/setops)
test_setops_SOURCES = setops/test-setops.cpp
test_setops_LDADD = $(CBLQ_LIBS)

test_setops_simplify_SOURCES = setops/test-setops-simplify.cpp
test_setops_simplify_LDADD = $(CBLQ_LIBS)

test_cii_setops_SOURCES = setops/test-cii-setops.cpp
test_cii_setops_LDADD = $(CBLQ_LIBS)

test_cblq_setops_SOURCES = setops/test-cblq-setops.cpp
test_cblq_setops_LDADD = $(CBLQ_LIBS)

# Indexing, index I/O, and data I/O tests (generally on classes in src/io)
test_region_serialize_SOURCES = io/test-region-serialize.cpp
test_region_serialize_LDADD = $(CBLQ_LIBS)

test_subset_datastream_SOURCES = io/test-subset-datastream.cpp
test_subset_datastream_LDADD = $(CBLQ_LIBS)

test_dataset_index_build_SOURCES = io/test-dataset-index-build.cpp
test_dataset_index_build_LDADD = $(CBLQ_LIBS)

test_database_index_build_SOURCES = io/test-database-index-build.cpp
test_database_index_build_LDADD = $(CBLQ_LIBS)

test_database_loadsave_SOURCES = io/test-database-loadsave.cpp
test_database_loadsave_LDADD = $(CBLQ_LIBS)

test_index_io_SOURCES = io/test-index-io.cpp $(TESTUTIL_HDRS)
test_index_io_LDADD = $(CBLQ_LIBS)

test_index_io_cache_SOURCES = io/test-index-io-cache.cpp $(TESTUTIL_HDRS)
test_index_io_cache_LDADD = $(CBLQ_LIBS)

# Query tests (generally on classes in src/query)
test_query_io_SOURCES = query/test-query-io.cpp $(TESTUTIL_HDRS)
test_query_io_LDADD = $(CBLQ_LIBS)

test_query_engines_SOURCES = query/test-query-engines.cpp $(TESTUTIL_HDRS)
test_query_engines_LDADD = $(CBLQ_LIBS)

test_query_binnings_SOURCES = query/test-query-binnings.cpp $(TESTUTIL_HDRS)
test_query_binnings_LDADD = $(CBLQ_LIBS)

test_expl_binning_SOURCES = query/test-expl-binning.cpp $(TESTUTIL_HDRS)
test_expl_binning_LDADD = $(CBLQ_LIBS)

test_query_encodings_SOURCES = query/test-query-encodings.cpp $(TESTUTIL_HDRS)
test_query_encodings_LDADD = $(CBLQ_LIBS)

# Manual tests (output to be verified by user; could be
# converted to automated test in the future)
test_cblq_semiwords_SOURCES = manual-tests/test-cblq-semiwords.cpp
test_cblq_semiwords_LDADD = $(CBLQ_LIBS)

test_cblq_dense_SOURCES = manual-tests/test-cblq-dense.cpp
test_cblq_dense_LDADD = $(CBLQ_LIBS)

test_cblq_dense2_SOURCES = manual-tests/test-cblq-dense2.cpp
test_cblq_dense2_LDADD = $(CBLQ_LIBS)

test_cblq_intersect_SOURCES = manual-tests/test-cblq-intersect.cpp
test_cblq_intersect_LDADD = $(CBLQ_LIBS)

test_cblq_build_SOURCES = manual-tests/test-cblq-build.cpp
test_cblq_build_LDADD = $(CBLQ_LIBS)
