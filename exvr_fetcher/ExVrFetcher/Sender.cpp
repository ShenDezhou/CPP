#include <stdexcept>
#include <sstream>

#include "Platform/envelope.h"
#include "Platform/docId/docId.h"
#include "Platform/log.h"

#include <ace/SOCK_Stream.h>
#include <ace/Basic_Types.h>
#include <ace/Malloc_Base.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/SOCK_Connector.h>
#include <ace/Malloc_Allocator.h>
#include <pthread.h>
#include <sys/time.h>
#include <ace/Time_Value.h>
#include <ace/OS_NS_sys_time.h>
#include <ace/Condition_Thread_Mutex.h>
#include "Sender.hpp"
#include <ace/Reactor.h>

class Sender::connection
{
public:
	connection(const std::string& address,unsigned int timeout,
			unsigned int retry_count, bool wait) 
		:m_connected(false),
		m_disconnect(false),
		m_retry_timeout(timeout),
		m_retry_count(retry_count),
		m_retry_lasttime(0),
		m_wait(wait),
		m_address(address),
		m_discardnum(0)
	{
        pthread_mutex_init(&_mutex, NULL);
        _slow._disconnect_times = 0;
        _slow._send_num = 0;
	}

	bool connect()
	{
    	if (m_connected)
            return true;

        if (m_disconnect && m_discardnum<m_max_discardnum)
            return false;

        for (;;)
        {
    		m_retry_lasttime = time(NULL);
    		while( time(NULL) - m_retry_lasttime < m_retry_timeout)
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
					SS_DEBUG((LM_ERROR, "Connecting to %s failure, retrying...\n", m_address.c_str() ));
					sleep(1);
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
			//int rr;
			ACE_Time_Value tv(20),tv2(20),tv3(20);
			if (r.open(m_stream, allocator) == 0 && r.sendMsg(data, size,0,&tv) > 0 && r.recvMsg(val,val_len,&tv2,&tv3)>0)
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
				SS_DEBUG((LM_ERROR, " %s disconnected.\n", m_address.c_str() ));
                if (ACE_Reactor::instance()->reactor_event_loop_done()) {
                    break;
                }
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

        SS_DEBUG((LM_ERROR, "[Discard] Send error for disconnect %s\n",m_address.c_str() ));
        ++m_discardnum;

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

int Sender::connection::m_max_discardnum = 15000;


class Sender::pimpl
{
	friend class Sender;
	pimpl(const std::vector<std::string> &address_list,unsigned int timeout,
		unsigned int retry_count, bool wait)
	{
		for (size_t i=0; i<address_list.size(); ++i)
		{
			m_connection.push_back(new connection(address_list[i],timeout,retry_count, wait));
		}
	}
	~pimpl()
	{
		for (size_t i=0; i<m_connection.size(); ++i)
        {   
            pthread_mutex_destroy(&(m_connection[i]->_mutex));
			delete m_connection[i];
        }
	}
private:
	ACE_New_Allocator m_allocator;
	std::vector<Sender::connection*> m_connection;
};

Sender::Sender(const std::string &des):m_des(des)
{
	bool wait = true;
	unsigned int timeout = 10;
	unsigned int retry_count = 2;

	std::vector<std::string> address_list;
	address_list.push_back(des);

    //char conval[64]; sprintf(conval,"%s_NUM",m_des.c_str());
    unsigned int SendTargetNums = 1;

    for (int i=0;i<SendTargetNums;i++)
    {
        //size_t sender_number = 1;
        m_pimpl.push_back(new pimpl(address_list,timeout,retry_count, wait));
    }

    int max_discardnum = 15000;
	//cfg.read_value("MAX_DISCARD_NUM",max_discardnum); reader_log_error("Configure: MAX_DISCARD_NUM %d\n",max_discardnum);
    Sender::connection::m_max_discardnum = max_discardnum;
}

//int Sender::send(const XmlPage *page) { return send_to(page->url,page->doc, page->doc_len); }

//int Sender::send_to( const char * url,const void *value,size_t value_len)
int Sender::send_to(const char* url,const void *value,size_t value_len)
{
    int result = -1;
    for (int i=0;i<m_pimpl.size();i++)
    {
    	int index = 0;	//hash_url_by_docid(url,m_pimpl[i]->m_connection.size());
        pthread_mutex_lock(&(m_pimpl[i]->m_connection[index]->_mutex));
        
    	result = m_pimpl[i]->m_connection[index]->send(
    			value, value_len,
    			m_pimpl[i]->m_allocator
    			);
        
        pthread_mutex_unlock(&(m_pimpl[i]->m_connection[index]->_mutex));
    }
    //SSLOG_INFO("vh_spider send %s\n",url);

	return result;
}
