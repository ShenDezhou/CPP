include $(top_srcdir)/common.mk

AM_CPPFLAGS += -D_GNU_SOURCE #-DSS_DOCID_BITS=128
AM_CFLAGS+=-Wimplicit-function-declaration

bin_PROGRAMS=
noinst_PROGRAMS=

INCLUDES += -I/usr/include/libxml2/
INCLUDES += -I/usr/include/openssl/ -I$(ODS_UTILS_ROOT)  -I/usr/include/thrift/ -I/usr/include/thrift/fb303 -DSS_LOG_DEFAULT_MODULE=vrspider

bin_PROGRAMS += exvr_fetcher
exvr_fetcher_SOURCES=main.cpp redis_tool.cpp VrCounter.cpp Refresh_Timer.cpp CScanTask.cpp CUrlUpdateTask.cpp CFetchTask.cpp CWriteTask.cpp Crawler.cpp database.cpp Sender.cpp QdbWriter.cpp Configuration.cpp CDeleteTask.cpp COnlineTask.cpp CMysqlHandle.cpp Util.cpp stat_opt_word.cpp ConfigManager.cpp CurlHttp.cpp
#exvr_fetcher_LDADD= -lACE -lcurl -loffdb -lioenvelope -lxml2  -L/usr/lib64/mysql -lmysqlclient -lssplatform -lencoding -lssl 
exvr_fetcher_LDADD= -lACE -lhiredis -lcurl -loffdb -ljson -lxml2  -L/usr/lib64/mysql -lmysqlclient -lssplatform -lencoding -lssl -lsmonitor -lunicode-encoding 
exvr_fetcher_LDFLAGS= -DSS_DOCID_BITS=128 -ggdb

noinst_PROGRAMS+=exvr_test
exvr_test_SOURCES=test_opt_word.cpp stat_opt_word.cpp database.cpp
exvr_test_LDADD= -lACE -lcurl -loffdb -ljson -lxml2  -L/usr/lib64/mysql -lmysqlclient -lssplatform -lencoding -lssl -lsmonitor -lunicode-encoding

noinst_PROGRAMS+=puncnormalize_test
puncnormalize_test_SOURCES=puncnormalize_test.cpp redis_tool.cpp CWriteTask.cpp CMysqlHandle.cpp Configuration.cpp COnlineTask.cpp QdbWriter.cpp database.cpp stat_opt_word.cpp Sender.cpp Util.cpp VrCounter.cpp CurlHttp.cpp CDeleteTask.cpp
puncnormalize_test_LDADD= -lACE -lhiredis -lcurl -loffdb -ljson -lxml2  -L/usr/lib64/mysql -lmysqlclient -lssplatform -lencoding -lssl -lsmonitor -lunicode-encoding

bin_PROGRAMS+=exvr_scheduler
exvr_scheduler_SOURCES=fetcher_scheduler.cpp fetcher_schedule_scan.cpp  redis_tool.cpp VrCounter.cpp  Refresh_Timer.cpp Configuration.cpp  ConfigManager.cpp  Crawler.cpp database.cpp Util.cpp  CurlHttp.cpp  CDeleteTask.cpp  COnlineTask.cpp  Sender.cpp QdbWriter.cpp stat_opt_word.cpp
exvr_scheduler_LDADD= -lACE -lhiredis -lcurl -loffdb -ljson -lxml2  -L/usr/lib64/mysql -lmysqlclient -lssplatform -lencoding -lssl -lsmonitor -lunicode-encoding

