include $(top_srcdir)/common-rules.mk
AM_CPPFLAGS+=-DSS_DOCID_BITS=128

collectlib_DIR=$(top_builddir)/_lib
collectbin_DIR=$(top_builddir)/_bin
collectinclude_DIR=$(top_builddir)/_include
include $(top_srcdir)/collect.mk

