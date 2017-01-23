#include <stdexcept>
#include <stdio.h>
#include <string>
#include <signal.h>
#include "config.hpp"
#include "reader_log.h"
#include "xmlpage_reader.hpp"
#include <ace/INET_Addr.h> 
#include <ace/SOCK_Acceptor.h>  
#include "NetworkInputAck.h"
#include <Platform/ioenvelope/Inputs.h>
#include <Platform/log.h>
#include <ace/Dev_Poll_Reactor.h>
#include <offdb/QuickdbAdapter.h>
#include "base.h"
#include "PageStore.hpp"
#include "xmlItemMgr.h"
#include "xmlSendTask.h"
#include <sys/stat.h>
#include "data_handler.h"
#include "send_file_task.h"
#include "send_file_check.h"

#define MAX_INDEX_IDX 1024*1024

static void sigint_handler(int signo)
{
	stop_signal=1;
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

int read_xmldb_sequence(const std::string addr, XmlPageReader &xmlPage_reader){
	QuickdbAdapter xmlDB;
	if (xmlDB.openc(addr.c_str())){
		reader_log_error("read_xmldb_sequence::cannot open %s. errno:%d.\n",addr.c_str(),errno);
		return -1;
	}
	void *key;
	size_t key_len;
	iovec data;
//	char* stdr="<?xml version=\"1.0\" encoding=\"gbk\"?><DOCUMENT><item><unique_url><![CDATA[http://www.test.com/]]></unique_url><key index=\"1\"><source>external</source><fetch_time>Mon Mar 15 12:31:17 2010</fetch_time><![CDATA[¼ÒÐ×½]]></key><classno>111</classno><classid>100204</classid><classtag index=\"0\">INTERNAL.VIDEO.MOVIE</classtag><display><content1><![CDATA[ÔÏ¹ۿ´ [Ԥ¸æ/Ƭ¶Ï¼ÒÐ×½ v.ku6.com ÎÒÖҽԺÉÈ°¸ ÐͽÒÓ¾«É²¡ v.ku6.com ¡¾ӰƬ¡¿¡¾¼ÒÐ×½¡¿¾«²ʾçƬDomesticDisturbance¡¾³Ì֡¿ v.youku.com ȡ¿îͻÔϮ»÷Ë³ö·Ðͽ www.letv.com .tv]]></content1>     <date>2010-03-03</date>     <pagesize>36k</pagesize>     <showurl><![CDATA[tv.sogou.com/]]></showurl>     <title><![CDATA[¡¶¼ÒÐ×½¡·-DVD¸ßåÏ¹ۿ´-Ë¹·ÊƵ]]></title>     <url><![CDATA[http://tv.sogou.com/movie/xtjnhugq27g32.html?p=40230600]]></url>   </display>   <key_b><![CDATA[¼ÒÐ×½]]></key_b>   <param1>50000</param1>   <param2>100</param2>   <param3>50000</param3>   <param4>0</param4>   <param5>0</param5>   <tplid>14</tplid> </item></DOCUMENT>";
//	data.iov_base=malloc(strlen(str)*sizeof(char));
//	data.iov_base=str;
//	data.iov_len=strlen(str);
//	xmlPage_reader.putData(data,true);
//	return 0;
	while(!stop_signal){
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
	return 0;
}

void stop_task(XmlPageReader& xmlPage_reader)
{
	stop_signal = 1;
	xmlPage_reader.stop_task();
	for(int i=0; i<g_send_tasks.size(); i++) g_send_tasks[i]->stop_task();
	ACE_Thread_Manager::instance()->wait();
}

SS_LOG_MODULE_DEF(xmlreader_server);


int main(int argc, char *argv[])
{
	setenv("LC_CTYPE","zh_CN.gbk",1);
	SS_INIT_LOCALE();
	stop_signal=0;
	if (!register_signals())
		return -1;
	const char * config_filename = (argc > 1) ? argv[1] : "xmlPage_reader.conf";
        const char * config_keyname = (argc > 2) ? argv[2] : "XML/reader";
	config cfg(config_filename,config_keyname);
	unsigned int listen_port, recv_file_listen_port, reader_thread_num, send_task_thread_num;
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
		reader_thread_num = 1;
		cfg.read_value("READER_THREAD_NUM",reader_thread_num);

		std::string bak_dir;
		int max_linenum = 0;
		cfg.read_value("BAK_FILE_DIR",bak_dir);
		if (!cfg.read_value("MAX_LINENUM",max_linenum))
        {
            max_linenum = 1000;
            reader_log_error("[ERROR]read MAX_LINENUM : set 1000.\n");
        }

		if (bak_dir != "") g_xmlItemMgr.base_dir = bak_dir;
		if (max_linenum != 0) g_xmlItemMgr.MAX_ITEM_COUNT = max_linenum;
        bool is_send_file = false;
        bool is_recv_file = false;
		cfg.read_value("IF_SEND_FIFE",is_send_file);
        std::string xml_reader_address;
        if (is_send_file && (!cfg.read_value("XML_READER_ADDRESS", xml_reader_address) || xml_reader_address.size() < 7))
        {
		    reader_log_error("[ERROR]read XML_READER_ADDRESS error.\n");
            return 0;
        }
		cfg.read_value("IF_RECV_FIFE",is_recv_file);
        // set xmlItemMgr
        g_xmlItemMgr.is_send_file = is_send_file;
        g_xmlItemMgr.is_recv_file = is_recv_file;
        if (is_recv_file && !cfg.read_value("RECV_FILE_LISTEN_PORT",recv_file_listen_port))
        {
		    reader_log_error("[ERROR]read RECV_FILE_LISTEN_PORT error.\n");
		    return 0;
        }
		// read sock_buf_size
        cfg.read_value("SOCK_RECVBUF_SIZE", sock_recvbuf_size);
        cfg.read_value("SOCK_SENDBUF_SIZE", sock_sendbuf_size);
        // init SendFileTask before init XmlItemMgr
        if (is_send_file)
        {
            SendFileTask::instance()->init(&cfg);
            SendFileCheck::instance()->init(&cfg);
        }
		reader_log_error("[BUF] sock_sendbuf_size:%d, sock_recvbuf_size:%d\n", sock_sendbuf_size, sock_recvbuf_size);
        reader_log_error("IF_SEND_FIFE : %s, IF_RECV_FIFE : %s\n", is_send_file == false ? "FALSE" : "TRUE", is_recv_file == false ? "FALSE" : "TRUE");
        // init XmlItemMgr
		if(g_xmlItemMgr.init()!=0)
		{
			reader_log_error("[ERROR] xmlItemMgr.init() fail\n");
			return -1;
		}
        
        if (is_send_file)
        {
            XmlSendFile *xml_send_file = new XmlSendFile(xml_reader_address);
            xml_send_file->connect();
		    reader_log_error("[INIT] send init file_id %d\n" ,g_xmlItemMgr.min_file_id);
            while (xml_send_file->send_file(NULL, SEND_INIT_FILE_ID, g_xmlItemMgr.init_file_id ) != 0)
            {
		        reader_log_error("[INIT] send error init file_id %d\n" ,g_xmlItemMgr.init_file_id );
            }
        }

		bool if_send_index, if_send_summary;
		if_send_index = true;
		if_send_summary = true;
		cfg.read_value("IF_SEND_INDEX",if_send_index);
		cfg.read_value("IF_SEND_SUMMARY",if_send_summary);
		
		if(if_send_index)
		{
			unsigned int nums = 0;
			cfg.read_value("INDEX_NUM",nums);
			for(int i=0; i<nums; i++)
			{
				//Xml_Send_Task *temp = new Xml_Send_Task();
				//temp->init(&cfg, 1, i);
				//g_send_tasks.push_back(temp);
                Xml_Send_Task::init(&cfg, 1, i);
			}
		}
		if(if_send_summary)
		{
			unsigned int nums = 0;
			cfg.read_value("SUMMARY_NUM",nums);
			for(int i=0; i<nums; i++)
			{
				//Xml_Send_Task *temp = new Xml_Send_Task();
				//temp->init(&cfg, 0, i);
				//g_send_tasks.push_back(temp);
                Xml_Send_Task::init(&cfg, 0, i);
			}
		}

		XmlPageReader xmlPage_reader(reader_thread_num,64,cfg);
		if (xmlPage_reader.initialize()){
			return 0;
		}
		//reader_xmldb_sequence
		if (if_scan_xmldb)
		{
			if (!read_xmldb_sequence(xmlDB_addr,xmlPage_reader))
				reader_log_error("reader xmlDB completed.\n");	
			else {
				stop_task(xmlPage_reader);
				return 0;
			}
		}
        // start send task
		for(int i=0; i<g_send_tasks.size(); i++) g_send_tasks[i]->open(NULL);

        if (!is_recv_file)
        {
		    ACE_INET_Addr addr(listen_port);
		    NetworkInputAck acceptor(ACE_Reactor::instance(new ACE_Reactor(new ACE_Dev_Poll_Reactor())),1,false);
		    acceptor.setServer(&xmlPage_reader);
		    if (acceptor.open(addr) < 0) {
		    	reader_log_error("XmlPageReader acceptor open::port already in use,port=%d\n",listen_port);
		    	stop_task(xmlPage_reader);
		    	return -1;
		    }
		    reader_log_error("open listen port %d OK!\n",listen_port);	

		    //handle network data
		    while (!stop_signal)
		    	ACE_Reactor::instance()->handle_events();
		    acceptor.close();
		    ACE_Reactor::instance()->close_singleton();
		    stop_task(xmlPage_reader);
            if (is_send_file)
            {
                SendFileTask::instance()->stop_task();
            }
        }
        else 
        {
            ACE_INET_Addr data_port(recv_file_listen_port);
            DataAcceptor data_acceptor;
            if(data_acceptor.open(data_port) == -1)
            {
                reader_log_error("XmlPageReader acceptor open::port already in use,port=%d\n",recv_file_listen_port);
			    stop_task(xmlPage_reader);
		        data_acceptor.close();
                return -1;
            }
		    reader_log_error("open listen port %d OK!\n", recv_file_listen_port);	
            ACE_Reactor::instance()->owner(ACE_Thread::self());
            ACE_Reactor::instance()->restart(1);
            ACE_Reactor::instance()->run_reactor_event_loop();
		    stop_task(xmlPage_reader);
            ACE_Reactor::instance()->close();
        }
	}
	catch(std::runtime_error& e)
	{
		reader_log_error("Reader runtime error: %s.\n", e.what());
	}
	catch(std::exception& e){
		reader_log_error("Initialize reader error:%s\n",e.what());
		return 0;
	}
	catch(...){
		reader_log_error("Unknown error!\n");
		return 0;
	}
	return 0;
}
