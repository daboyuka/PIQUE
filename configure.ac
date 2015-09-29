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

AC_INIT([pique], [0.1.0], [nagiza.samatova@gmail.com])

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([config])
AC_CONFIG_FILES([Makefile
                 include/Makefile
                 src/Makefile
                 src/parallel/Makefile
                 tests/Makefile
                 tests/parallel/Makefile
                 tests/tools/Makefile
                 tests/util/Makefile
                 tools/Makefile])
AC_CONFIG_SRCDIR([tools/build-index.cpp])

# pkg-config generation
AC_CONFIG_FILES([pique.pc
                 pique-parallel.pc]) 

AM_INIT_AUTOMAKE([foreign -Wall -Werror])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES])

AC_PROG_LIBTOOL
LT_INIT

AC_PROG_CC
m4_ifdef([AC_PROG_CC_C99],
         [AC_PROG_CC_C99],
         [AC_MSG_WARN([AC_PROG_CC_C99 macro not supported in your version of autoconf. Attempting to use a workaround, but it may not work.]);
          CPPFLAGS="$CPPFLAGS -std=c99"])

AM_PROG_CC_C_O
m4_pattern_allow([AM_PROG_AR])
AM_PROG_AR

AC_PROG_CXX
m4_include([m4/ax_cxx_compile_stdcxx_11.m4])
AX_CXX_COMPILE_STDCXX_11
m4_include([m4/ax_mpi.m4])
AX_MPI

#AC_CHECK_LIB([stdc++],[main],,[AC_MSG_ERROR(Octree-Experiments requires libstdc++)])

m4_include([m4/ax_boost_base.m4])
m4_include([m4/ax_boost_serialization.m4])
m4_include([m4/ax_boost_timer.m4])
AX_BOOST_BASE([1.48],, [AC_MSG_ERROR([Boost version >= 1.48 required, but it was not found in your system])])
AX_BOOST_SERIALIZATION
AX_BOOST_TIMER

BOOST_LIB="$BOOST_SERIALIZATION_LIB $BOOST_TIMER_LIB"
AC_SUBST([BOOST_LIB])



AC_ARG_ENABLE(static-workaround,
        [AS_HELP_STRING([--enable-static-workaround],[when using a compiler wrapper (e.g., mpic++) and forcing libtool to statically link libraries/executables, try to ensure the compiler wrapper still dynamically links any utility libraries (this fixes compiler issues on some systems, such as NERSC Edison) (default: no)])],
        dnl
        )
if test "$enable_static_workaround" = "yes"; then
  STATIC_LINKER_FLAG="-static -dynamic"
else
  STATIC_LINKER_FLAG="-all-static"
fi

dnl
dnl Goal: for various platform-workaround reasons, we need to change
dnl the library/executable linker on a per-Makefile (but not per-target)
dnl basis.
dnl
dnl The basic issue is that Automake ultimately uses $CXXLD for libtool
dnl linking, and its value is not known until make time. This means there
dnl is no good way to /modify/ its value (though we can totally
dnl overwrite it).
dnl
dnl Failed approach one: in Makefile.am, set CXXLD = $CXXLD plus flags
dnl * Fails because of circular variable definition
dnl Failed approach two: in configure.ac, modify CXXLD
dnl * Fails because CXXLD is not available in configure.ac
dnl Approach three, used: assume $CXXLD == $CXX, then in Makefile.am, set CXXLD = $CXX plus flags
dnl * Not ideal, but appears to work, and it seems the assumption is correct
dnl   based on examining Automake 1.11 source...?
dnl
CXXLD_BASE='$(CXX)'  # Assume $CXXLD == $CXX; not ideal, but cannot find a workaround so far
CXXLD_STATIC='$(CXXLD_BASE) '"$STATIC_LINKER_FLAG"
CXXLD_DYNAMIC='$(CXXLD_BASE)'
AC_SUBST([CXXLD_BASE])
AC_SUBST([CXXLD_STATIC])
AC_SUBST([CXXLD_DYNAMIC])


CXXLD_FOR_TESTS="$CXXLD_DYNAMIC"
CXXLD_FOR_TOOLS="$CXXLD_DYNAMIC"
AC_ARG_ENABLE(all-static,
        [AS_HELP_STRING([--enable-all-static],[force libtool to statically link all executables (tests and tools) (default: no)])],
        [if test "$enableval" = "yes" ; then
        CXXLD_FOR_TESTS="$CXXLD_STATIC"
        CXXLD_FOR_TOOLS="$CXXLD_STATIC"
        fi])

AC_ARG_ENABLE(tests-static,
        [AS_HELP_STRING([--enable-tests-static],[force libtool to statically link tests (default: no)])],
        [if test "$enableval" = "yes" ; then
        CXXLD_FOR_TESTS="$CXXLD_STATIC"
        fi])
AC_ARG_ENABLE(tools-static,
        [AS_HELP_STRING([--enable-tools-static],[force libtool to statically link tools (not including Lua libs) (default: no)])],
        [if test "$enableval" = "yes" ; then
        CXXLD_FOR_TOOLS="$CXXLD_STATIC"
        fi])

#Note: now all Makefiles must manually set CXXLD to CXXLD_FOR_XXX
AC_SUBST([CXXLD_FOR_TESTS])
AC_SUBST([CXXLD_FOR_TOOLS])

#m4_pattern_allow([AM_CXXLD])
#AC_SUBST([CXXLD], ['$(AM_CXXLD)']) dnl Enable per-Automake file CXXLD

AC_ARG_ENABLE([plltests],
        [  --enable-plltests    Turn on parallel "make check" tests],
        [case "${enableval}" in
            yes) plltests=true ;;
            no)  plltests=false ;;
            *) AC_MSG_ERROR([bad value ${enableval} for --enable-plltests]) ;;
        esac],dnl
        [plltests=true])
AM_CONDITIONAL([RUN_PLL_TESTS], [test x$plltests = xtrue])

m4_include([m4/ax_ridcompress.m4])
AX_LIBRIDCOMPRESS

m4_include([m4/ax_fastbit.m4])
AX_LIBFASTBIT

m4_include([m4/ax_hdf5.m4])
AX_HDF5

AC_OUTPUT
