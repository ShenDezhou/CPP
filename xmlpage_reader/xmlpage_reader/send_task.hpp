#ifndef SEND_TASK_HPP
#define SEND_TASK_HPP

#include <ace/Message_Queue_T.h>
#include <ace/Synch_Traits.h>
#include <ace/Condition_Thread_Mutex.h>
#include <ace/Task_T.h>

#include <pthread.h>
#include "config.hpp"
#include "reader_log.h"
#include "sender.hpp"
#include "summarySender.hpp"
#include "base.h"
#include "PageStore.hpp"

class Send_Task :public ACE_Task<ACE_MT_SYNCH>{
public:
        Send_Task(const char* file, const char* key, PageStore *summary_pagestore = NULL, PageStore *index_pagestore = NULL, bool bak_source = false);
        ~Send_Task();

        int open(void *arg);
        int close(u_long);
	void stop_task();
        int svc();
private:
        static void * run_svc(void * arg);
protected:
        size_t _thr_count;
        size_t *_stack_size;
        char **_stack;
private:
	//Sender* index_sender;
	//SummarySender* summary_sender;
	bool if_send_index;
	bool if_send_summary;
	PageStore *m_summary_pagestore;
	PageStore *m_index_pagestore;
	bool m_bak_source;
	config *m_cfg;
};
#endif

