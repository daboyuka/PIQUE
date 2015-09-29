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

CXXLD =  $(CXXLD_FOR_TOOLS)

cblq_lib = $(top_builddir)/src/libpique.la
pll_cblq_lib = $(top_builddir)/src/parallel/libpique-parallel.la

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/tools
AM_CPPFLAGS += $(HDF5_CPPFLAGS) $(BOOST_CPPFLAGS) $(RIDCOMPRESS_CPPFLAGS) $(FASTBIT_CPPFLAGS)
AM_LDFLAGS = $(HDF5_LDFLAGS) $(BOOST_LDFLAGS) $(RIDCOMPRESS_LDFLAGS) $(FASTBIT_LDFLAGS)

COMMON_LIBS = $(HDF5_LIBS) $(BOOST_LIB) $(RIDCOMPRESS_LIBS) $(FASTBIT_LIBS)
CBLQ_LIBS = $(cblq_lib) $(COMMON_LIBS)
PLL_CBLQ_LIBS = $(pll_cblq_lib) $(MPILIBS) $(COMMON_LIBS)

bin_PROGRAMS = build-meta build-index build-index-parallel quantize quantize-equibin characterize-dataset query tableflip index-info

build_meta_SOURCES = build-meta.cpp
build_meta_LDADD = $(CBLQ_LIBS)

build_index_SOURCES = build-index.cpp
build_index_LDADD = $(CBLQ_LIBS)

build_index_parallel_SOURCES = build-index-parallel.cpp
build_index_parallel_LDADD = $(PLL_CBLQ_LIBS)

index_info_SOURCES = index-info.cpp
index_info_LDADD = $(CBLQ_LIBS)

quantize_SOURCES = misc/quantize.cpp
quantize_LDADD = $(CBLQ_LIBS)

quantize_equibin_SOURCES = misc/quantize-equibin.cpp
quantize_equibin_LDADD = $(CBLQ_LIBS)

characterize_dataset_SOURCES = misc/characterize-dataset.cpp
characterize_dataset_LDADD = $(CBLQ_LIBS)

tableflip_SOURCES = misc/tableflip.cpp
tableflip_LDADD = $(CBLQ_LIBS)

query_SOURCES = query-main.cpp query-helper.cpp query-helper.hpp 
query_LDFLAGS = $(AM_LDFLAGS) 
query_LDADD = $(CBLQ_LIBS)
