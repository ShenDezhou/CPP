include $(top_srcdir)/common.mk

AM_CPPFLAGS += -D_GNU_SOURCE #-DSS_DOCID_BITS=128
AM_CFLAGS+=-Wimplicit-function-declaration

INCLUDES += -I/usr/include/libxml2/ -DSS_LOG_DEFAULT_MODULE=exvr_fetcher_server -I$(ODS_UTILS_ROOT)

bin_PROGRAMS = length_check 
length_check_SOURCES= length_check.cc
length_check_LDADD= -lxml2 -lencoding -lssplatform

bin_PROGRAMS += delete_item 
delete_item_SOURCES= delete_item.cpp ../Sender.cpp ../QdbWriter.cpp
delete_item_LDADD= -lxml2 -lencoding -lACE -lcurl -loffdb -L/usr/lib64/mysql -lmysqlclient -lssl -lssplatform

bin_PROGRAMS += scan_DB 
scan_DB_SOURCES= scanDB.cc
scan_DB_LDADD=  -loffdb -lssplatform
#exvr_fetcher_LDFLAGS= -DSS_DOCID_BITS=128

bin_PROGRAMS += schemalengthcheck
schemalengthcheck_SOURCES= xml_check_schema_length.cpp
schemalengthcheck_LDADD= -lxml2

bin_PROGRAMS += read_db
read_db_SOURCES= read_db.cpp ../QdbWriter.cpp
read_db_LDADD= -lxml2 -lencoding -lACE -lcurl -loffdb -L/usr/lib64/mysql -lmysqlclient -lssl -lssplatform

bin_PROGRAMS += clean_db
clean_db_SOURCES= clean_db.cpp ../Configuration.cpp ../database.cpp ../Sender.cpp
clean_db_LDADD= -loffdb -lACE -L/usr/lib64/mysql -lmysqlclient -lssplatform

bin_PROGRAMS += delete_doc
delete_doc_SOURCES= delete_item_by_docid.cpp ../Configuration.cpp ../database.cpp ../Sender.cpp ../QdbWriter.cpp
delete_doc_LDADD= -lxml2 -lencoding -lACE -lcurl -loffdb -L/usr/lib64/mysql -lmysqlclient -lssl -lssplatform

bin_PROGRAMS += fake
fake_SOURCES= fake_spider/main.cpp fake_spider/configure.cpp fake_spider/socket_utils.cpp fake_spider/string_Util.cpp  fake_spider/process.cpp fake_spider/sender.cpp ../database.cpp
fake_LDADD= -lxml2 -lencoding -lACE -lcurl -L/usr/lib64/mysql -lmysqlclient -lssl -lssplatform
