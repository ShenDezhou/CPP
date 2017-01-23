#ifndef SENDER_HPP
#define SENDER_HPP

#include <string>
#include <vector>

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
#include <ace/Reactor.h>

class Sender
{
public:
	Sender(const std::string &des);
	//int send(const std::string page);
	int send_to(const char* url,const void *value,size_t value_len);
private:
	class connection;
	class pimpl;
	std::vector<pimpl*> m_pimpl;
	std::string m_des;
};

#endif

