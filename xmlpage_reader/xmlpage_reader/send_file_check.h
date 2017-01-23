#ifndef SEND_FILE_CHECK_H
#define SEND_FILE_CHECK_H

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

class SendFileCheck : public ACE_Task<ACE_SYNCH>{
public:
    static SendFileCheck* instance()
    {
        return ACE_Singleton<SendFileCheck, ACE_Recursive_Thread_Mutex>::instance();
    }
    virtual int init(config *m_cfg);
	virtual int open(void * = 0);
	virtual void stop_task();
	virtual int svc(void);
private:
    int frequency;
    int time_interval;
};
#endif

