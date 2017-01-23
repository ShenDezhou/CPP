#include <Platform/log.h>

#include "NetworkInputAck.h"
#include "BaseInputReceiverAck.h"
#include <sys/time.h>
#include <time.h>

int NetworkInputAck::activate_svc_handler(NetworkInputAckHandler* svc_handler) {
	svc_handler->setServer(server);
	svc_handler->setAllocator(allocator);
	svc_handler->setLog(log);
	svc_handler->setCopy(copy);
	return PARENT::activate_svc_handler(svc_handler);
}

int NetworkInputAckHandler::open(void * p)
{
	ACE_INET_Addr cli_addr;
	peer().get_remote_addr(cli_addr);
	SS_DEBUG((LM_INFO, "NetworkInputAckHandler::open: Client %s:%d connected.\n", cli_addr.get_host_addr(), cli_addr.get_port_number()));

	int ret;
	ret = server->addConnection(this);
	if (ret < 0)
	{
		peer().close();
		SS_DEBUG((LM_INFO, "NetworkInputAckHandler::open: Client %s:%d kicked out.\n", cli_addr.get_host_addr(), cli_addr.get_port_number()));
		return 0;
	}
	
	//Register the service handler with the reactor
	ACE_Reactor::instance()->register_handler(this,	ACE_Event_Handler::READ_MASK);

	return 0;
}

static int timeCost(timeval time1,timeval time2)
{
    return ((time1.tv_sec-time2.tv_sec)*1000000+(time1.tv_usec-time2.tv_usec));
}
static int timeCostMS(timeval time1,timeval time2)
{
    return timeCost(time1,time2)/1000;
}

int NetworkInputAckHandler::handle_input(ACE_HANDLE handler)
{
	int ret;
	static ACE_Time_Value timeout(10);
	static ACE_Time_Value timeout1(10);
	unsigned char *tmp_buf;
	size_t buf_size = 0;
	timeval _beg,_end,_send_beg,_send_end,_put_beg,_put_end;

//	receiver.setStream((ACE_SOCK_Stream&)handler);
//	receiver.setAllocator(*allocator);
   if (receiver.open((ACE_SOCK_Stream&)handler,*allocator) !=0)
	 	return -1;
	while (1)
    {   
		gettimeofday(&_beg,NULL);
    	ret = receiver.recvMsg((void * &)tmp_buf, buf_size,&timeout,&timeout1);
		gettimeofday(&_end,NULL);
    	if (ret <= 0)
    	{
            ACE_INET_Addr cli_addr;
            peer().get_remote_addr(cli_addr);
            SS_DEBUG((LM_INFO, "NetworkInputAckHandler::handle_input: Client %s:%d errno %d.\n", cli_addr.get_host_addr(), cli_addr.get_port_number(),errno));
    		return -1;
    	}
        else
            break;
    }
	char ack=1;
	ACE_Time_Value t1(5);
	gettimeofday(&_send_beg,NULL);
	receiver.sendMsg(&ack,sizeof(char),0,&t1);
	gettimeofday(&_send_end,NULL);
	iovec data;
	data.iov_base = tmp_buf;
	data.iov_len = ret;
	ACE_INET_Addr cli_addr;
	peer().get_remote_addr(cli_addr);
	if(log)
		SS_DEBUG((LM_INFO, "NetworkInputAckHandler::handle_input: Got a packet from %s:%d.\n", cli_addr.get_host_addr(), cli_addr.get_port_number()));
	gettimeofday(&_put_beg,NULL);
	ret = server->putData(data, copy);
	gettimeofday(&_put_end,NULL);
	//SS_DEBUG((LM_INFO,"[NetworkInputAckHandler::handle_input] recv_cost=%d,send_cost=%d,put_cost=%d\n",timeCostMS(_end,_beg),timeCostMS(_send_end,_send_beg),timeCostMS(_put_end,_put_beg)));
	if(copy || ret < 0)
		allocator->free(tmp_buf);
	return ret;
}

int NetworkInputAckHandler::handle_close(ACE_HANDLE handle, ACE_Reactor_Mask mask)
{
	ACE_INET_Addr cli_addr;
	peer().get_remote_addr(cli_addr);

	SS_DEBUG((LM_INFO, "NetworkInputAckHandler::handle_close: Client %s:%d disconnected.\n", cli_addr.get_host_addr(), cli_addr.get_port_number()));

	int ret;

	ret = server->delConnection(this);

	delete this; //commit suicide

	return 0;
}

