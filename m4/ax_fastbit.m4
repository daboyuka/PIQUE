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

AC_DEFUN([AX_LIBFASTBIT], [

dnl Enable the --with-fastbit=path configure argument
AC_ARG_WITH(
  [fastbit],
  [AS_HELP_STRING(
    [--with-fastbit=DIR],
    [Location of the FastBit library]
  )]dnl
)

dnl If the lib was specified, verify that it exists and can compile
if test "x$with_fastbit" != xno; then
    FASTBIT_CPPFLAGS="-I$with_fastbit/include"
    FASTBIT_LDFLAGS="-L$with_fastbit/lib"
    FASTBIT_LIBS="-lfastbit"

    saveCPPFLAGS="$CPPFLAGS"
    saveLDFLAGS="$LDFLAGS"
    CPPFLAGS="$CPPFLAGS $FASTBIT_CPPFLAGS"
    LDFLAGS="$LDFLAGS $FASTBIT_LDFLAGS $FASTBIT_LIBS"

    AC_LANG_PUSH([C++])

    AC_CHECK_HEADERS(
      [ibis.h],
      [],
      [AC_MSG_FAILURE(
        [Cannot find ibis.h from the FastBit lib. Make sure it has been properly installed at the path specified ($with_fastbit).]dnl
      )]dnl
    )

dnl Removed this test because FastBit is all C++, and autoconf chokes on C++ lib linking tests
dnl    AC_CHECK_LIB(
dnl      [fastbit],
dnl      [ibis::gParameters],
dnl      [AC_DEFINE(
dnl        [HAVE_FASTBIT],
dnl        [1],
dnl        [Define if you have FastBit]
dnl      )],
dnl      [AC_MSG_FAILURE(
dnl        [Cannot successfully link with the FastBit lib. Make sure it has been properly installed at the path specified ($with_fastbit).]dnl
dnl      )],dnl
dnl    )
    
    AC_LANG_POP([C++])

    CPPFLAGS="$saveCPPFLAGS"
    LDFLAGS="$saveLDFLAGS"

    AC_SUBST(FASTBIT_CPPFLAGS)
    AC_SUBST(FASTBIT_LDFLAGS)
    AC_SUBST(FASTBIT_LIBS)

    AM_CONDITIONAL(HAVE_FASTBIT,true)
    AC_DEFINE([HAVE_FASTBIT], [1], [Define if we have libfastbit])

    AC_MSG_RESULT([FastBit library found at $with_fastbit])
else
  AM_CONDITIONAL(HAVE_FASTBIT,false)

  AC_MSG_RESULT([Not building with FastBit library])
fi

]) dnl End of DEFUN
