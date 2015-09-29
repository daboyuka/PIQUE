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

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src 
AM_CPPFLAGS += $(HDF5_CPPFLAGS) $(BOOST_CPPFLAGS) $(RIDCOMPRESS_CPPFLAGS) $(FASTBIT_CPPFLAGS)
AM_LDFLAGS = $(HDF5_LDFLAGS) $(BOOST_LDFLAGS) $(RIDCOMPRESS_LDFLAGS) $(FASTBIT_LDFLAGS)

lib_LTLIBRARIES = libpique-parallel.la

libpique_parallel_la_LIBADD = $(top_builddir)/src/libpique.la $(MPILIBS) $(HDF5_LIBS) $(BOOST_LIB) $(RIDCOMPRESS_LIBS) $(FASTBIT_LIBS)

libpique_parallel_la_SOURCES = \
	io/mpi-index-io.cpp \
	io/mpi-index-io-control.cpp

