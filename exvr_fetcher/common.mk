INCLUDES = 
#-I$(top_srcdir)/common

MOSTLYCLEANFILES=
BUILT_SOURCES=

CFLAGS+=$(if $(DEBUG), -g -O0)
CXXFLAGS+=$(if $(DEBUG), -g -O0)

CPPFLAGS+=$(if $(OPTIMIZE), -DNDEBUG)
CFLAGS+=$(if $(OPTIMIZE), -O2)
CXXFLAGS+=$(if $(OPTIMIZE), -O2)

CFLAGS+=$(if $(PROFILE), -pg)
CXXFLAGS+=$(if $(PROFILE), -pg)

AM_CPPFLAGS=-D'SVNVERSION="$(SVNVERSION)"' -DENABLE_SSL 
AM_LDFLAGS=-Wl,-no-undefined
AM_CFLAGS=
AM_CXXFLAGS=-Wall -Wno-sign-compare

_SVNVERSION=$(strip $(subst exported, 0, $(shell svnversion -nc $(srcdir) | sed 's/^.*://' | tr -cd [0-9] )))
SVNVERSION=$(if $(_SVNVERSION),$(_SVNVERSION),0)
CONFIGURE_DEPENDENCIES=$(subst @@@,,$(subst CONFIGURE_DEPENDENCIES,@,@CONFIGURE_DEPENDENCIES@))
CONFIGURE_DEPENDENCIES+=$(top_srcdir)/acsite.m4



collectlib_DIR=$(top_builddir)/_lib
collectbin_DIR=$(top_builddir)/_bin
collectinclude_DIR=$(top_builddir)/_include
include $(top_srcdir)/collect.mk
