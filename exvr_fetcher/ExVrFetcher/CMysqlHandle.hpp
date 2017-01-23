#ifndef  CMYSQLHANLE_H
#define  CMYSQL_HANDLE_H

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include <sys/time.h>
#include "database.h"
#include "ExVrFetcher.h"
#include "Sender.hpp"

#define MAX_SQL_BUF  4096*2
class CMysqlHandle: public ACE_Task<ACE_SYNCH> 
{
 public:
  virtual int open (void * = 0);
  virtual int stopTask ();
  virtual int put (ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
  virtual int svc (void);
public:
  int open (const char* configfile);
private:
  int sql_write( CMysql &mysql, const char *sql, int db_flag = 1);
  int sql_read( CMysqlQuery &query, CMysql &mysql, const char *sql, int db_flag = 1);

  int save_timestamp(CMysql *mysql, int timestamp_id);

  ACE_RW_Thread_Mutex mutex;
  std::string vr_ip;
  std::string vr_user;
  std::string vr_pass;
  std::string vr_db;    
    
  std::string cr_ip;
  std::string cr_user;
  std::string cr_pass;
  std::string cr_db;

  Sender *m_sender;
  Sender *multiHitSender;

  int threadNum;
};
 
#endif
                 
