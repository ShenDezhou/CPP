include $(top_srcdir)/common.mk

bin_PROGRAMS=
lib_LIBRARIES=
lib_LTLIBRARIES=
noinst_LIBRARIES=
AM_CFLAGS+=-Wimplicit-function-declaration

AM_CPPFLAGS += -DSS_LOG_DEFAULT_MODULE=xmlreader_server
common_srcs=Utils.cpp xmlpage_reader.cpp \
			config.cpp xmlpage.cpp Parser.cpp base.cpp \
			NetworkInputAck.cpp string_Util.cpp xmlSendTask.cpp xmlSender.cpp xmlItemMgr.cpp data_handler.cpp send_file_task.cpp send_file_check.cpp

bin_PROGRAMS+=xmlPage_reader
xmlPage_reader_SOURCES=$(common_srcs) main.cpp
xmlPage_reader_LDADD=-lxml2 -lACE -lencoding -loffdb -lssplatform

bin_PROGRAMS+=xmldb_reader
xmldb_reader_SOURCES=$(common_srcs) xmldb_reader.cpp
xmldb_reader_LDADD=-lxml2 -lACE -lencoding -loffdb -lssplatform

bin_PROGRAMS+=genpage
genpage_SOURCES=$(common_srcs) genpage.cpp
genpage_LDADD=-lxml2 -lACE -lencoding -loffdb -lssplatform

