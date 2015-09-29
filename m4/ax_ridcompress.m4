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

AC_DEFUN([AX_LIBRIDCOMPRESS], [

dnl Enable the --with-ridcompress=path configure argument
AC_ARG_WITH(
  [ridcompress],
  [AS_HELP_STRING(
    [--with-ridcompress=DIR],
    [Location of the RID compression library]
  )]dnl
)

dnl If the ridcompress lib was specified, verify that it exists and can compile
if test "x$with_ridcompress" != xno; then
    RIDCOMPRESS_CPPFLAGS="-I$with_ridcompress -I$with_ridcompress/include"
    RIDCOMPRESS_LDFLAGS="-L$with_ridcompress -L$with_ridcompress/lib"
    RIDCOMPRESS_LIBS="-lridcompress"

    saveCPPFLAGS="$CPPFLAGS"
    saveLDFLAGS="$LDFLAGS"
    CPPFLAGS="$CPPFLAGS $RIDCOMPRESS_CPPFLAGS"
    LDFLAGS="$LDFLAGS $RIDCOMPRESS_LDFLAGS"

    AC_LANG_PUSH([C++])

    AC_CHECK_HEADERS(
      [pfordelta-c-interface.h],
      [],
      [AC_MSG_FAILURE(
        [Cannot find pfordelta-c-interface.h from the RID compression lib. Make sure it has been properly installed at the path specified ($with_ridcompress).]dnl
      )]dnl
    )

    AC_CHECK_LIB(
      [ridcompress],
      [encode_rids],
      [AC_DEFINE(
        [HAVE_LIBRIDCOMPRESS],
        [1],
        [Define if you have libridcompress]
      )],
      [AC_MSG_FAILURE(
        [Cannot successfully link with the RID compression lib. Make sure it has been properly installed at the path specified ($with_ridcompress).]dnl
      )],dnl
    )

    AC_LANG_POP([C++])

    CPPFLAGS="$saveCPPFLAGS"
    LDFLAGS="$saveLDFLAGS"

    AC_SUBST(RIDCOMPRESS_CPPFLAGS)
    AC_SUBST(RIDCOMPRESS_LDFLAGS)
    AC_SUBST(RIDCOMPRESS_LIBS)
else
  AC_MSG_FAILURE(
    [--with-ridcompress is required]dnl
  )
fi

]) dnl End of DEFUN
