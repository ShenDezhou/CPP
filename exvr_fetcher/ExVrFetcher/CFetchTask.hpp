#ifndef  CFETCHTASK_H
 #define  CFETCHTASK_H
 
#include <ace/Singleton.h>
 #include <ace/Synch.h>
 #include <ace/Task.h>
 #include <sys/time.h>
 


#define WASTE_TIME_MS(past)\
 ({\
     timeval __now__;\
     gettimeofday(&__now__, NULL);\
     (__now__.tv_sec - past.tv_sec) * 1000 + (__now__.tv_usec - past.tv_usec) / 1000;\
 })
 




class CFetchTask: public ACE_Task<ACE_SYNCH> 
{
 public:
   static CFetchTask* instance ()
   {
      return ACE_Singleton<CFetchTask, ACE_Recursive_Thread_Mutex>::instance();
   }
   
  virtual int open (void * = 0);
   virtual int stopTask ();
   virtual int put (ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
   virtual int svc (void);
 
public:
   static bool speed_control;
   static int speed;     
  int open (const char* configfile);

//private:
  //int save_timestamp(CMysql *mysql, int timestamp_id);
 
private:
    ACE_RW_Thread_Mutex mutex;
    std::map<std::string, ACE_Time_Value> last_fetch_time;

   std::string vr_ip;
   std::string vr_user;
   std::string vr_pass;
   std::string vr_db;

   std::string m_config;
    
 };
 
#endif
 
