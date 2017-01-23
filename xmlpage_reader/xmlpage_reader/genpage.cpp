#include <stdexcept>
#include <stdio.h>
#include <string>
#include <signal.h>
#include "config.hpp"
#include "reader_log.h"
#include "xmlpage_reader.hpp"
#include <ace/INET_Addr.h> 
#include "NetworkInputAck.h"
#include <Platform/ioenvelope/Inputs.h>
#include <Platform/log.h>
#include <offdb/QuickdbAdapter.h>
#include "base.h"

static int scan_done = 0;
static int kill_sig = 0;
static void sigint_handler(int signo)
{
	if (scan_done)
		stop_signal=1;
	else kill_sig = 1;
        char buf[128];
        int length = snprintf(buf, sizeof(buf), "Receive signal %d: %s, exit.stop_signal:%d\n", signo, strsignal(signo),stop_signal);
        if (length > 0)
                write(STDERR_FILENO, buf, length);
//	_exit(0);
}

static bool register_signals()
{
	if (SIG_ERR == signal(SIGINT, sigint_handler))
        {
                reader_log_error("register sigint_handler on SIGINT failed.\n");
                return false;
        }
        if (SIG_ERR == signal(SIGTERM, sigint_handler))
        {
                reader_log_error("register sigint_handler on SIGTERM failed.\n");
                return false;
        }
        if (SIG_ERR == signal(SIGPIPE, SIG_IGN))
        {
                reader_log_error("ignore signal SIGPIPE failed.\n");
                return false;
        }
        return true;
}

int genpage(const std::string addr){
	QuickdbAdapter xmlDB;
        if (xmlDB.open(addr.c_str())){
                reader_log_error("read_xmldb_sequence::cannot open %s. errno:%d.\n",addr.c_str(),errno);
                stop_signal = 1;
                return -1;
        }
	void *key =  malloc(sizeof(gDocID_t));
	size_t key_len = sizeof(gDocID_t);
	size_t val_len;
	char url[1024];
	char *buf = new char[100*1024*1024];
	FILE *fp = fopen("xml.data","r");
	for(;;){
		url[0]='\0';
		while(fgets(url,1024,fp)!=NULL && strlen(url)==0 && strcmp(url,"\n"));
		if (!strlen(url))
			break;

		gDocID_t docid;
		url[strlen(url)-1]='\0';
		url2DocId(url,&docid);
		fprintf(stdout,"url:%s\ndocid:%016llx-%016llx\n",url,docid.id.value.value_high,docid.id.value.value_low);
		val_len = 0;
		while(fgets(buf+val_len,100*1024*1024,fp)&&strcmp(buf+val_len, "\n")){
			val_len += strlen(buf+val_len);
		}
		memcpy(key,(void *)&docid,key_len);
		buf[val_len]='\0';
		fprintf(stdout,"%s\n\n",buf);
		int ret = xmlDB.put(key,key_len,(void *)buf,val_len,0);
		fprintf(stdout,"put:%016llx-%016llx,ret:%d,errno:%d\n",docid.id.value.value_high,docid.id.value.value_low,ret, errno);
	}
	return 0;
}
int read_xmldb_sequence(const std::string addr, XmlPageReader &xmlPage_reader){
	QuickdbAdapter xmlDB;
	if (xmlDB.openc(addr.c_str())){
		reader_log_error("read_xmldb_sequence::cannot open %s. errno:%d.\n",addr.c_str(),errno);
		stop_signal = 1;
		return -1;
	}
	void *key;
	size_t key_len;
	iovec data;
	while(!kill_sig){
		int ret = xmlDB.next(key, key_len, data.iov_base, data.iov_len);
		if (ret == 1)
			break;
		else if (ret < 0){
			reader_log_error("next error\n");
		}
		else {
			xmlPage_reader.putData(data,true);
			free(data.iov_base);
			free(key);
		}
	}
	if ( kill_sig )
		stop_signal = 1;
	scan_done = 1;
	return 0;
}

SS_LOG_MODULE_DEF(xmlreader_server);

int main(int argc, char *argv[])
{
	stop_signal=0;
	if (!register_signals())
		return -1;
	const char * config_filename = (argc > 1) ? argv[1] : "xmlPage_reader.conf";
        const char * config_keyname = (argc > 2) ? argv[2] : "XML/reader";
	config cfg(config_filename,config_keyname);
	unsigned int listen_port,reader_thread_num,send_task_thread_num;
	bool if_scan_xmldb = false;
	cfg.read_value("SCAN_XMLDB",if_scan_xmldb);
	std::string xmlDB_addr;
	if (!cfg.read_value("LISTEN_PORT",listen_port)){
		reader_log_error("[ERROR]read LISTEN_PORT error.\n");
		return 0;
	}
	try{
		if (if_scan_xmldb && !cfg.read_value("XMLDB_ADDR",xmlDB_addr)){
			reader_log_error("[ERROR]read XMLDB_ADDR error.\n");
			return 0;
		}
		genpage(xmlDB_addr);
		return 0;	
		reader_thread_num = 1;
		cfg.read_value("READER_THREAD_NUM",reader_thread_num);

		Send_Task send_task(config_filename, config_keyname);
		XmlPageReader xmlPage_reader(reader_thread_num,64,cfg);
		xmlPage_reader.setNext(&send_task);
		if (xmlPage_reader.initialize())
			return 0;
		//reader_xmldb_sequence
		if (if_scan_xmldb)
			if (!read_xmldb_sequence(xmlDB_addr,xmlPage_reader))
				reader_log_error("reader xmlDB completed.\n");	
			else {
				send_task.flush();
				send_task.wait();
				xmlPage_reader.clear();
				return 0;
			}
		//return 0;
		//handle network data
		ACE_INET_Addr addr(listen_port);
		NetworkInputAck acceptor(ACE_Reactor::instance(),1,false);
		acceptor.setServer(&xmlPage_reader);
		if (acceptor.open(addr) < 0) {
			reader_log_error("XmlPageReader acceptor open::port already in use,port=%d\n",listen_port);
			return -1;
		}
		reader_log_error("open listen port %d OK!\n",listen_port);	
		while (!stop_signal)
			ACE_Reactor::instance()->handle_events();
		acceptor.close();
		ACE_Reactor::instance()->close_singleton();
		xmlPage_reader.clear();
		send_task.flush();
		send_task.wait();
	}
	catch(const std::exception& e){
		reader_log_error("Initialize reader error:%s\n",e.what());
		return 0;
	}
	return 0;
}
