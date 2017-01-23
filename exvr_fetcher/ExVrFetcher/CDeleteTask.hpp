#ifndef  __CDELETETASK_H  
#define  __CDELETETASK_H 

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include "database.h"
#include "Sender.hpp"
#include "QdbWriter.hpp"
#include "Util.hpp"

class VrdeleteData
{
public:
	std::vector<std::string> v_docid;
	std::string fetch_time;
	std::string url;
	std::string status;
	int class_id;
	int res_id;
	int data_group;
	int status_id;
	int  timestamp_id;
};

class CDeleteTask: public ACE_Task<ACE_SYNCH> {
	public:
		static CDeleteTask* instance () {
			return ACE_Singleton<CDeleteTask, ACE_Recursive_Thread_Mutex>::instance();
		}

		virtual int open (void * = 0);
		virtual int stopTask ();
		virtual int svc (void);
		int open (const char* configfile);
		int put(ACE_Message_Block *message, ACE_Time_Value *timeout=0);
		int delete_customer_data(CMysql *mysql);
	private:
		int set_delete(CMysql *mysql, std::string &conditions); 
		int set_auto_delete(CMysql *mysql,std::string &sql);
		int delete_data(CMysql *mysql, std::string &conditions);
		int sql_write( CMysql *mysql, const char *sql, int cr = 0 );
		Sender *m_sender;
		Sender *multiHitSender;

		std::string vr_ip;
		std::string vr_user;
		std::string vr_pass;
		std::string vr_db;
		
		std::string m_config;
		int       delete_interval;
		std::string spider_type;
};

#endif

