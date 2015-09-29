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

# Always link the core library both statically and dynamically
CXXLD =  $(CXXLD_DYNAMIC)

# Build this directory first, then parallel/
SUBDIRS = . parallel

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src
AM_CPPFLAGS += $(HDF5_CPPFLAGS) $(BOOST_CPPFLAGS) $(RIDCOMPRESS_CPPFLAGS) $(FASTBIT_CPPFLAGS)
AM_LDFLAGS = $(HDF5_LDFLAGS) $(BOOST_LDFLAGS) $(RIDCOMPRESS_LDFLAGS) $(FASTBIT_LDFLAGS)

lib_LTLIBRARIES = libpique.la

libpique_la_LIBADD = $(HDF5_LIBS) $(BOOST_LIB) $(RIDCOMPRESS_LIBS) $(FASTBIT_LIBS)

# Core C++ sources    
libpique_la_SOURCES = \
    indexing/binning-spec.cpp \
    indexing/quantization.cpp \
    region/region-encoding.cpp \
    setops/setops.cpp \
    setops/preflist-setops.cpp \
    util/myopts.c \
    util/fixed-archive.cpp \
    util/datatypes.cpp

# C++ dataset sources
if HAVE_HDF5
libpique_la_SOURCES += \
    data/hdf5/dataset-hdf5.cpp
endif

# C++ index IO sources
libpique_la_SOURCES += \
    io/index-io.cpp \
    io/index-io-cache.cpp \
    io/formats/sharedfile/sharedfile-format-index-io.cpp \
    io/posix/posix-index-io.cpp \
    io/database.cpp

# C++ query sources
libpique_la_SOURCES += \
    query/query.cpp \
    query/query-engine.cpp \
    query/simple-query-engine.cpp \
    query/basic-query-engine.cpp \
    query/bitmap-query-engine.cpp

# Index encoding C++ indexing sources
libpique_la_SOURCES += \
	encoding/region-math.cpp \
	encoding/index-encoding.cpp \
	encoding/eq/eq-encoding.cpp \
	encoding/range/range-encoding.cpp \
	encoding/interval/interval-encoding.cpp \
	encoding/binarycomp/binarycomp-encoding.cpp \
	encoding/hier/hier-encoding.cpp

# CBLQ C++ indexing sources
libpique_la_SOURCES += \
    region/cblq/cblq.cpp \
    region/cblq/cblq-semiwords.cpp \
    region/cblq/cblq-ridconv.cpp \
    region/cblq/cblq-encode.cpp \
    setops/cblq/cblq-setops.cpp \
    setops/cblq/cblq-setops-fast.cpp \
    setops/cblq/cblq-setops-nary1.cpp \
    setops/cblq/cblq-setops-nary2-dense.cpp \
    setops/cblq/cblq-setops-nary3-dense.cpp \
    setops/cblq/cblq-setops-nary3-fast.cpp \
    setops/cblq/cblq-setops-actiontables.c setops/cblq/cblq-setops-actiontables.h \
	convert/cblq/cblq-to-bitmap-convert.cpp

# Uncompressed bitmap C++ indexing sources
libpique_la_SOURCES += \
    region/bitmap/bitmap.cpp \
    region/bitmap/bitmap-encode.cpp \
    setops/bitmap/bitmap-setops.cpp

# II C++ indexing sources
libpique_la_SOURCES += \
    region/ii/ii.cpp \
    region/ii/ii-encode.cpp \
    setops/ii/ii-setops.cpp

# CII C++ indexing sources
libpique_la_SOURCES += \
    region/cii/cii.cpp \
    region/cii/cii-encode.cpp \
    region/cii/cii-decoder.cpp \
    setops/cii/cii-setops.cpp
   
# WAH C++ indexing sources
libpique_la_SOURCES += \
    region/wah/wah.cpp \
    region/wah/wah-encode.cpp \
    setops/wah/wah-setops.cpp
