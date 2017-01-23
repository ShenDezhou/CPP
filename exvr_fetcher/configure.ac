# -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT(external vertical result fetcher, 1.0.0, liumingzhi@sohu-rd.com)
AC_CONFIG_SRCDIR([common.mk])
AC_CONFIG_AUX_DIR(_aux)
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_HEADER([config-dummy.h])
AC_PREFIX_DEFAULT(/opt/sogou)
#AM_MAINTAINER_MODE

# Checks for system
AC_CANONICAL_HOST
# Checks for programs.
AC_LANG(C++)
#AM_PROG_CC_C_O
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LEX
AC_PROG_YACC
PKG_PROG_PKG_CONFIG
#AC_PROG_RANLIB
#AC_LIBTOOL_WIN32_DLL
#AC_LIBTOOL_DLOPEN
AC_DISABLE_STATIC
AC_PROG_LIBTOOL
#AM_PATH_PYTHON(2.4)

# Checks for libraries.

# Checks for header files.
#AC_HEADER_STDC
AC_CHECK_HEADERS([iostream])

# Checks for typedefs, structures, and compiler characteristics.
#AC_HEADER_STDBOOL
AC_C_CONST
#AC_C_INLINE
#AC_TYPE_SIZE_T
#AC_HEADER_TIME

# Checks for library functions.
#	below removed by kirbyzhou, becauuse underall linux, they work fine.
#AC_FUNC_ALLOCA
#AC_FUNC_MALLOC
#AC_FUNC_REALLOC
#AC_FUNC_SELECT_ARGTYPES
#AC_TYPE_SIGNAL
#AC_FUNC_VPRINTF
#AC_CHECK_FUNCS([gethostbyname inet_ntoa memmove memset select socket sqrt strchr strdup strerror strncasecmp strstr])
#AC_CHECK_FUNCS([mbstowcs wcstombs mbtowc wctomb])

AK_STD_FEATURES

#for large file
CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64 -DSS_DOCID_BITS=128"

##check mysq
#AK_DETECT_LIB2(mysql, [/usr],
#	[mysql.h], [mysqlclient_r], my_init,
#	[optional],
#	[include/mysql], [lib/mysql])

#check ace
AK_DETECT_LIB2(ace, [/usr/local/src/ACE* /usr/local/ACE* /opt/ACE*],
	[ace/ACE.h], [ACE], [ACE::init],
	[optional],
	[], [ace])

##check db4 
#AK_DETECT_LIB2(db4, [],
#	[db.h], [db-4.4 db-4.3 db-4.2 db], db_create,
#	[optional],
#	[], [])

##check sqlite 
#AK_DETECT_LIB2(sqlite3, [],
#	[sqlite3.h], [sqlite3], sqlite3_exec,
#	[optional],
#	[], [])

AC_CACHE_SAVE

# var / condiional

AK_CHECK_MODULE_ROOT(SSPLATFORM_ROOT, [the root of module ssplatform], ssplatform, [_include], [_lib])
AK_CHECK_MODULE_ROOT(ODS_UTILS_ROOT, [the root of module ods utils], ods_utils, [_include], [_lib])

AK_CHECK_BUILD_DEPENDENCIES([yes], [$MODULE_ROOT_VALUES])

#AC_CONFIG_SUBDIRS([sparsehash])

#AC_CONFIG_SUBDIRS([gmock])
#AC_CONFIG_SUBDIRS([gmock/gtest])


AC_CONFIG_FILES([
	Makefile
	ExVrFetcher/Makefile
    ExVrFetcher/tools/Makefile
])

AC_OUTPUT
AK_PATCH_LIBTOOL

