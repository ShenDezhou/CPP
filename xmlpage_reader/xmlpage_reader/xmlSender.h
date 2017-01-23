#ifndef XML_SENDER_H
#define XML_SENDER_H

#include <string>
#include <string.h>
#include <stdexcept>
#include <sstream>
#include <assert.h>

#include "offdb/QuickdbAdapter.h"
#include "Platform/envelope.h"
#include "Platform/docId/docId.h"

#include <ace/SOCK_Stream.h>
#include <ace/Basic_Types.h>
#include <ace/Malloc_Base.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/SOCK_Connector.h>
#include <ace/Malloc_Allocator.h>
#include "reader_log.h"
#include <pthread.h>
#include <sys/time.h>
#include <ace/Time_Value.h>
#include <ace/OS_NS_sys_time.h>
#include <ace/Condition_Thread_Mutex.h>
#include "Utils.hpp"
#include "Platform/encoding/URLInfo.h"
#include "config.hpp"
#include "xmlItemMgr.h"
#include "send_file_task.h"

int hash_url_by_docid(const std::string& objid, int number);

class SUM_CONN
{
	public:
		SUM_CONN(const char *device) 
		{
			qdb_conn.open(device);
			m_connected = false;
		}
		int send(const char* url, gDocID_t &docid, const void *data, size_t size, int status);
	public:
		QuickdbAdapter qdb_conn;
		bool m_connected;
};
class INDEX_CONN
{
	public:
		INDEX_CONN(const char *device) 
		{
			m_address = device;
			m_connected = false;
		}
		int send(const char* url, gDocID_t &docid, const void *data, size_t size, int status);
		int connect();
	public:
		std::string m_address;
		ACE_SOCK_Stream m_stream;
		bool m_connected;
		ACE_New_Allocator m_allocator;
};

class HostData {
	public:
		HostData(std::string host, int port, int type) {}
		~HostData() {}
	public:
		std::string m_host;
		int m_port;
		int m_type;
		
		int file_id;
		int item_index;
		int file_pos;
		int fd;
		ItemData m_data;
		int hash_index;
};

class Xml_Sender
{
	public:
		Xml_Sender(int type, int task_index, int index, int num, std::string address)
		{
			m_address = address;
			send_type = type;
			send_task_index = task_index;
			hash_index = index;
			hash_num = num;
			
			char temp_str[1024];
			snprintf(temp_str, 1023, "%s_%d_%d_%s", 
				g_types[send_type], send_task_index, hash_index, m_address.c_str());
			m_key = temp_str;
			m_file_name = g_xmlItemMgr.base_dir + "/pos_" + m_key + ".dat";
			std::replace(m_file_name.begin(), m_file_name.end(), ':', '_');
			
	        reader_log_error("Xml_Sender::hash_index = %d, hash_num = %d\n", hash_index, hash_num);
			index_conn = NULL;
			sum_conn = NULL;

            if(send_type == 1) index_conn = new INDEX_CONN(m_address.c_str());
			else sum_conn = new SUM_CONN(m_address.c_str());

			file_id = g_xmlItemMgr.now_file_id;
			item_index = 1;
			file_pos = 0;
			fd = -1;
			fd_file_id = -1;
			skip_count = 0;
			error_count = 0;

            // resize xml_index to max size
            xml_index.resize(g_xmlItemMgr.MAX_ITEM_COUNT + 1);
            index_num = 0;
		    fd_index = -1;	
			init();
		}
		
		~Xml_Sender()
		{
			save();
			if(index_conn != NULL) delete index_conn;
			if(sum_conn != NULL) delete sum_conn;
		}
		
		int init();
		int send();
		int save();
		int read();
        int load_index(char *file_name);
		
	public:
		SUM_CONN *sum_conn;
		INDEX_CONN *index_conn;
		std::string m_address;
		std::string m_file_name;
		std::string m_key;
		int send_type;
		int send_task_index;

		int file_id;
		int item_index;
		
		int file_pos;
		int fd;
		int fd_file_id;
		ItemData m_data;
		int hash_index;
		int hash_num;
		
		int skip_count;
		int error_count;

        int fd_index;
        std::vector<std::pair<int, int> > xml_index;
        int index_num;
};

#endif
