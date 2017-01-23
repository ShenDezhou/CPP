#ifndef SEND_FILE_TASK_H
#define SEND_FILE_TASK_H

#include <ace/Message_Queue_T.h>
#include <ace/Synch_Traits.h>
#include <ace/Condition_Thread_Mutex.h>
#include <ace/Task_T.h>
#include "ace/SOCK_Stream.h"
#include <ace/Malloc_Allocator.h>
#include <ace/SOCK_Connector.h>
#include <vector>
#include <string>
#include <pthread.h>
#include "config.hpp"
#include "reader_log.h"
#include "xmlItemMgr.h"

#ifndef BUF_SIZE
#define BUF_SIZE 4*1024*1024
#endif

extern int sock_sendbuf_size;

class XmlSendFile{
public:
    XmlSendFile(std::string addr):m_address(addr),m_connected(false) 
    {
        send_buf = (char *)malloc(BUF_SIZE);
    }
    int send_file(char *file_name, SEND_FILE_TYPE type, unsigned int file_id);
    int recv_response();
    int connect();
    int close();
    ~XmlSendFile()
    {
        if (send_buf != NULL)
        {
            free(send_buf);
        }
    }
    
private:
    std::string m_address;
    ACE_SOCK_Stream m_stream;
    bool m_connected;
    ACE_New_Allocator m_allocator;
    char *send_buf;
};

class SendFileTask : public ACE_Task<ACE_SYNCH>{
public:
    static SendFileTask* instance()
    {
        return ACE_Singleton<SendFileTask, ACE_Recursive_Thread_Mutex>::instance();
    }
    virtual int init(config *m_cfg);
	virtual int open(void * = 0);
    virtual int put(ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
	virtual void stop_task();
	virtual int svc(void);
private:
    int save(unsigned int file_id);
    std::string base_dir;
    std::string xml_reader_addr; 
    std::string m_file_name;
    int send_file_thread_num;
};
#endif

