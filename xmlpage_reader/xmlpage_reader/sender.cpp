#include <stdexcept>
#include <sstream>
#include <assert.h>

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
#include "sender.hpp"
#include "Utils.hpp"

int hash_url_by_docid(const std::string& objid, int number) 
{
	assert(number > 0);
	gDocID_t docid;
	Utils::objid2docid(objid.c_str(), docid);
	unsigned long long val = docid.id.value.value_high>>32;
	return (val*number)>>32;
}

class Sender::connection
{
	public:
		connection(const std::string& address,unsigned int timeout,
				unsigned int retry_count, bool wait) 
			:m_connected(false),
			m_disconnect(false),
			m_address(address),
			m_retry_timeout(timeout),
			m_retry_lasttime(0),
			m_retry_count(retry_count),
			m_wait(wait),
			m_discardnum(0)
	{
		pthread_mutex_init(&_mutex, NULL);
		_slow._disconnect_times = 0;
		_slow._send_num = 0;
	}

		bool connect()
		{
			if (m_connected)
			{
				reader_log_error("connected..\n");
				return true;
			}

			if (m_disconnect && m_discardnum<m_max_discardnum)
			{
				reader_log_error("disconnected, m_discardnum:%d, m_max_discardnum:%d\n",m_discardnum,m_max_discardnum);
				return false;
			}

			for (;;)
			{
				m_retry_lasttime = time(NULL);
				//while( time(NULL) - m_retry_lasttime < m_retry_timeout)
				{
					for(int i = 0; i< m_retry_count ; i++)
					{
						ACE_SOCK_Connector connector;
						ACE_INET_Addr addr(m_address.c_str());
						m_connected = connector.connect(m_stream, addr) != -1;
						if(m_connected)
						{
							m_disconnect = false;
							return m_connected;
						}
						reader_log_error("Connecting to index %s failure, retrying...\n", m_address.c_str());
					}
				}
				if(!m_wait)
				{
					m_discardnum = 0;
					m_disconnect = true;
					break;
				}
			}
			return m_connected;
		}

		void disconnect()
		{
			if (m_connected)
			{
				m_stream.close();
				m_connected = false;
			}
		}

		int send(const void* data, size_t size, ACE_Allocator& allocator)
		{
			platform::Transceiver r;

			while(connect())
			{
				void *val=NULL;
				size_t val_len;
				int rr;
				ACE_Time_Value tv(5),tv2(5),tv3(5);
				if (r.open(m_stream, allocator) == 0 && r.sendMsg(data, size,0,&tv) > 0 &&r.recvMsg(val,val_len,&tv2,&tv3)>0)
				{
					free(val);
					if (_slow._disconnect_times)	
					{
						if (++_slow._send_num>50)
						{
							_slow._disconnect_times = 0;
							_slow._send_num = 0;
						}
					}
					return 0;
				}
				else
				{
					if(val)
						free(val);
					disconnect();
					reader_log_error(" %s disconnected.\n", m_address.c_str());
					if (!m_wait && (++_slow._disconnect_times>=3))
					{
						m_disconnect = true;
						m_discardnum = 0;

						_slow._disconnect_times = 0;
						_slow._send_num = 0;
						break;
					}
					sleep(1);
				}
			}
			{
				reader_log_error("[Discard] Send error for disconnect %s\n",m_address.c_str());
				++m_discardnum;
			}


			return -1;
		}

		pthread_mutex_t _mutex;

		static int m_max_discardnum;

	private:
		bool m_connected;
		bool m_disconnect;  //主动连接且没有连接上
		unsigned int m_retry_timeout;
		unsigned int m_retry_count;
		unsigned int m_retry_lasttime;
		bool m_wait;
		std::string m_address;
		ACE_SOCK_Stream m_stream;
		int m_discardnum;

		typedef struct
		{
			int _disconnect_times;
			int _send_num;
		}CheckSlow; //发送50条数据内断开超过3次

		CheckSlow _slow;
};

int Sender::connection::m_max_discardnum = 0;


class Sender::pimpl
{
	friend class Sender;
	pimpl(const std::vector<std::string> &address_list,unsigned int timeout,
			unsigned int retry_count, bool wait)
	{
		for(int n=0; n<MIRROR_NUM; n++){
			for (size_t i=0; i<address_list.size(); ++i)
			{
				m_connection[n].push_back(new connection(address_list[i],timeout,retry_count, wait));
			}
		}
	}
	~pimpl()
	{
		for(int n=0; n<MIRROR_NUM; n++){
			for (size_t i=0; i<m_connection[n].size(); ++i)
			{   
				pthread_mutex_destroy(&(m_connection[n][i]->_mutex));
				delete m_connection[n][i];
			}
			m_connection[n].clear();
		}
	}
	private:
	ACE_New_Allocator m_allocator;
	std::vector<Sender::connection*> m_connection[MIRROR_NUM];
};

Sender::Sender(const config& cfg, const std::string des):m_des(des)
{
	bool wait = true;
	unsigned int timeout = 2;
	unsigned int retry_count = 2;

	std::vector<std::string> address_list;

	char conval[128];
	sprintf(conval,"%s_NUM",m_des.c_str());
	unsigned int SendTargetNums = 0;
	if (!cfg.read_value(conval,SendTargetNums)||SendTargetNums==0){
		//reader_log_error("%s_NUM error",m_des.c_str());
		throw std::runtime_error(conval);
	}

	reader_log_error("Configure: %s_NUM %d\n",m_des.c_str(),SendTargetNums);

	for (int i=0;i<SendTargetNums;i++)
	{
		sprintf(conval,"%s_GROUP_%d",m_des.c_str(),i);

		if (!cfg.read_address_list(conval, address_list))
			throw std::runtime_error(conval);

		sprintf(conval,"%s_GROUP_%d_NUM",m_des.c_str(),i);
		size_t sender_number;
		if (cfg.read_value(conval, sender_number) && sender_number != address_list.size())
		{
			sprintf(conval,"SendTarget %d num not consist.",i);
			throw std::runtime_error(conval);
			//      	reader_log_error("SendTarget 's num not consist \n");
			//		exit(0);
		}
		wait = false;
		sprintf(conval,"%s_GROUP_%d_WAIT",m_des.c_str(),i);
		cfg.read_value(conval, wait);
		reader_log_error("Configure: %s_GROUP_%d_WAIT:%d\n",m_des.c_str(),i,wait);
		m_pimpl.push_back(new pimpl(address_list,timeout,retry_count, wait));
	}

	int max_discardnum = 0;
	cfg.read_value("MAX_DISCARD_NUM",max_discardnum);
	//    reader_log_error("Configure: MAX_DISCARD_NUM %d\n",max_discardnum);
	Sender::connection::m_max_discardnum = max_discardnum;
}

int Sender::send(const XmlPage *page)
{

	return send_to(page->url, page->objid, page->doc, page->doc_len);
}

int Sender::send_to(const char *url, const char* objid, const void *value, size_t value_len)
{
	int result = -1,success_num = 0,ret = 0;
	for (int i=0;i<m_pimpl.size();i++)
	{
		int index = hash_url_by_docid(objid, m_pimpl[i]->m_connection[0].size());
		int mirror = hash_url_by_docid(objid, MIRROR_NUM);
		pthread_mutex_lock(&(m_pimpl[i]->m_connection[mirror][index]->_mutex));


		result = m_pimpl[i]->m_connection[mirror][index]->send(
				value, value_len,
				m_pimpl[i]->m_allocator
				);

		if (result >= 0)
		{
			reader_log_error("[IndexSender]sending succeed:%d\n", ret);
			success_num++;
		}
		else
		{
			ret = -1;
			reader_log_error("[IndexSender]ret:%d\n", ret);
		}
		pthread_mutex_unlock(&(m_pimpl[i]->m_connection[mirror][index]->_mutex));


	}
	reader_log_error("[IndexSender]%s, %s\n", objid, url);
	if(success_num < m_pimpl.size())
	{
		ret = -1;
	}

	return ret;
}

bool Sender::check_connect()
{
	for (int i = 0;i < m_pimpl.size();i++)
	{
		for(int n = 0; n < MIRROR_NUM; n++)
		{
			for (size_t j = 0; j < m_pimpl[i]->m_connection[n].size(); ++j)
			{
				pthread_mutex_lock(&(m_pimpl[i]->m_connection[n][j]->_mutex));
				if (!m_pimpl[i]->m_connection[n][j]->connect())
				{
					reader_log_error("trying to connect index failed..\n");
					pthread_mutex_unlock(&(m_pimpl[i]->m_connection[n][j]->_mutex));
					return false;
				}
				pthread_mutex_unlock(&(m_pimpl[i]->m_connection[n][j]->_mutex));
			}
		}
	}

	return true;
}
