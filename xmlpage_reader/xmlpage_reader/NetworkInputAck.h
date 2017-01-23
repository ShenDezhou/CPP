/** 
 * @file NetworkInput.h
 * @brief the interface for NetworkInput
 * @author fishy@sohu-rd.com
 */

#ifndef __NETWORK_INPUT_ACK_H
#define __NETWORK_INPUT_ACK_H

#include <vector>
#include <ace/Reactor.h>
#include <ace/Svc_Handler.h>
#include <ace/Acceptor.h>
#include <ace/Synch.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/SOCK_Connector.h>
#include <ace/Connector.h>
//#include <ace/OS.h>
#include <ace/Log_Msg.h>
#include <ace/Malloc_Allocator.h>

#include <Platform/envelope.h>

class BaseInputReceiverAck;

/** 
 * @brief the class inherited from ACE_Svc_Handler to handle connections
 */
class NetworkInputAckHandler: public ACE_Svc_Handler <ACE_SOCK_STREAM,ACE_NULL_SYNCH>
{
public:
	int open(void * p);
	int handle_input(ACE_HANDLE handler);
	virtual int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask mask);

	void setServer(BaseInputReceiverAck *value) { server = value; }
	void setAllocator(ACE_Allocator *value) { allocator = value; }
	void setCopy(bool value) { copy = value; }
	void setLog(bool value) { log = value; }

	virtual ~NetworkInputAckHandler() { }

protected:
	//platform::Receiver receiver;
	platform::Transceiver receiver;
	BaseInputReceiverAck *server;
	ACE_Allocator *allocator;
	bool log;
	bool copy;

};

/** 
 * @brief the class inherited from ACE_Acceptor to liston on connections
 */
class NetworkInputAck: public ACE_Acceptor<NetworkInputAckHandler,ACE_SOCK_ACCEPTOR> {
	typedef ACE_Acceptor<NetworkInputAckHandler,ACE_SOCK_ACCEPTOR> PARENT;
public:
	NetworkInputAck(ACE_Reactor* reactor=0, int use_select=1, bool log_receive = true, bool copydata = true)
		: PARENT(reactor, use_select), allocator(ACE_Allocator::instance()), log(log_receive), copy(copydata) {}
public:
	virtual ~NetworkInputAck() { }
	/** 
	 * @brief set the server to handle input data
	 * 
	 * @param value the server
	 */
	void setServer(BaseInputReceiverAck *value) { server = value; }
	/** 
	 * @brief set the allocator used to malloc packet, default is ACE_Allocator::instance()
	 * 
	 * @param value the allocator
	 */
	void setAllocator(ACE_Allocator *value) { allocator = value; }
protected:
	virtual int activate_svc_handler(NetworkInputAckHandler* svc_handler);
	BaseInputReceiverAck *server;
	ACE_Allocator *allocator;
	bool log;
	bool copy;
};


#endif	/* __NETOWKR_INPUT_H */
