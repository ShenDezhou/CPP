#ifndef XML_SEND_TASK_H
#define XML_SEND_TASK_H

#include <ace/Message_Queue_T.h>
#include <ace/Synch_Traits.h>
#include <ace/Condition_Thread_Mutex.h>
#include <ace/Task_T.h>

#include <vector>
#include <string>

#include <pthread.h>
#include "config.hpp"
#include "reader_log.h"
#include "sender.hpp"
#include "xmlSender.h"
#include "base.h"
#include "xmlItemMgr.h"

class Xml_Send_Task :public ACE_Task<ACE_SYNCH>{
	public:
        //Xml_Send_Task(config *m_cfg, int type, int index);
        //~Xml_Send_Task();

        Xml_Send_Task(int send_type, int send_index, int sender_num, Xml_Sender *sender);
		static int init(config *m_cfg, int type, int index);
		int open(void *arg);
		void stop_task();
		int svc();
	public:
		int send_type;
		int send_index;
		int sender_num;
		Xml_Sender  *sender;
};
extern std::vector<Xml_Send_Task *> g_send_tasks;
	
#endif

