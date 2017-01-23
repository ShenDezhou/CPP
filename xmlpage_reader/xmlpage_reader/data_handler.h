#ifndef _DATA_HANDLER_H
#define _DATA_HANDLER_H

#include "ace/INET_Addr.h"
#include "ace/Acceptor.h"
#include "ace/SOCK_Stream.h"
#include "ace/Reactor.h"
#include "ace/SOCK_Acceptor.h"
#include "ace/Svc_Handler.h"
#include <ace/Time_Value.h>
#include <iconv.h>
#include <vector>
#include "xmlItemMgr.h"

#define MAX_XML_DATA_LEN (4*1024*1024)

extern int sock_recvbuf_size;        

struct DataHeader
{
    uint32_t len;
    uint32_t file_id;
    SEND_FILE_TYPE flag;
};


class DataHandler : public ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_NULL_SYNCH>
{
    typedef ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_NULL_SYNCH> parent;

    public:
        virtual int open(void *arg = NULL);
        virtual int handle_input(ACE_HANDLE fd = ACE_INVALID_HANDLE);
        virtual int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask);
        int sendResult(uint32_t file_id);

    DataHandler()
    {
        recv_buf = (char *)malloc(MAX_XML_DATA_LEN);
        is_new_file = false;
        write_len = 0;
        left_index = 0;
        recv_len = 0;
        packet_len = 0;
    }

    ~DataHandler()
    {
        if(recv_buf!=NULL) free(recv_buf);
        recv_buf = NULL;
    }
            
    private:
        int queued_count_;
        int deferred_close_;
    
        char *recv_buf;
        int  recv_len;

        int  left_index;
        bool is_new_file;
        int  write_len;

        int packet_len;        
        unsigned int file_id;
        DataHeader header;
        ACE_INET_Addr client_addr;
};

typedef ACE_Acceptor<DataHandler, ACE_SOCK_ACCEPTOR> DataAcceptor;

#endif
