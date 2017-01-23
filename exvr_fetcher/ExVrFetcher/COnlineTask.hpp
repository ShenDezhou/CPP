#ifndef  __CONLINETASK_H  
#define  __CONLINETASK_H 

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include <vector>
#include "database.h"
#include "Sender.hpp"
#include "QdbWriter.hpp"

class COnlineTask: public ACE_Task<ACE_SYNCH>
{
public:
    static COnlineTask* instance () {
        return ACE_Singleton<COnlineTask, ACE_Recursive_Thread_Mutex>::instance();
    }

    virtual int open (void * = 0);
    virtual int stopTask ();
    virtual int svc (void);
    virtual int put (ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
    int open (const char* configfile);

private:

    int save_timestamp(CMysql *mysql, int timestamp_id);
    int get_data( CMysql *mysql, Sender *sender, Sender *multiHitSender, sub_request_t  *req); 
    int sql_write( CMysql *mysql, const char *sql );

    std::string vr_ip;
    std::string vr_user;
    std::string vr_pass;
    std::string vr_db;
    std::vector<int> filter_tplids;
    std::string m_config;
    std::string res_priority;
    std::string pagereader_addr;
	std::string multiHitXmlreader_addr; //xml reader address for multi-hit vr
	std::string remind_ip;
    std::string remind_user;
    std::string remind_pass;
    std::string remind_db;
	std::string need_changed_vr;
	std::string remind_all;

};

bool needUpdate(CMysql *mysql, int timestamp_id, const std::string &field);

#endif

