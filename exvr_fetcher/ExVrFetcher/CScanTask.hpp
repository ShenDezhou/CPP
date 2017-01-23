#ifndef  __CSCANTASK_H  
 #define  __CSCANTASK_H 

#include <ace/Singleton.h>
 #include <ace/Synch.h>
 #include <ace/Task.h>
 #include "database.h"
 #include "Sender.hpp"
 #include "QdbWriter.hpp"
#include "VrCounter.hpp" 
#include <vector>
#include "redis_tool.h"

#define MAX_FETCH_PER_MiNU 12000

#define LEVEL1_FETCH_FREQUENCY      15*60 //′óóú12000??keyμ??üD??ü?ú×?D??a15·??ó
#define LEVEL2_FETCH_FREQUENCY      60*60
#define LEVEL3_FETCH_FREQUENCY    6*60*60
#define LEVEL4_FETCH_FREQUENCY   24*60*60
#define LEVEL5_FETCH_FREQUENCY 7*24*60*60

struct sched_unit
{
	int data_from;
	int litemNum;
	int frequency;
	long lastfetch_time;	
	int res_id;
	VrCounter *ptr_vrCount;
	std::string time_str;
	std::vector<ACE_Message_Block *> data;
};

class CScanTask: public ACE_Task<ACE_SYNCH> {
 public:
   static CScanTask* instance () {
      return ACE_Singleton<CScanTask, ACE_Recursive_Thread_Mutex>::instance();
   }
   
   virtual int open (void * = 0);
   virtual int stopTask ();
   virtual int svc (void);
   int open (const char* configfile);
 private:
   int get_resource( CMysql *mysql, CMysql *crsql,const std::string &sql,bool is_manual, RedisTool *redis_tool);
   int get_delete(CMysql *mysql, std::string &value);

   sched_unit *createScheUnit(int res_id,int frequency,long lfetchTime,int data_group,CMysqlQuery &k_query,CMysql *mysql);
   int putScheUnitToFetch(sched_unit* req,CMysql *mysql, CMysql *crsql);
   void deleteScheUnit(sched_unit* req);

   int sql_write( CMysql *mysql, const char *sql );

   //save current timestamp to mysql
   int save_timestamp(CMysql *mysql, request_t *req);

   int get_customer_resource( CMysql *mysql, std::string sql, bool is_manual = false );
	
   //get the total item count that last fetched
   int getLastItemCount(CMysqlQuery &k_query, CMysql *mysql, int data_group, int res_id, VrCounter* counter_t, std::map<int, int> &data_source_map);
   int delete_redis_set(int res_id, int vr_id, bool is_manual, int data_group, RedisTool *redis_tool);
   int delete_redis_set(std::string set_value, std::string value, RedisTool *redis_tool);
   std::string update_redis_time(std::string prev_time);
   int AddSomeUniqueName(RedisTool *redis_tool, int start, int num);
   std::string check_add(std::string value, std::string prev_time);
   std::string vr_ip;
   std::string vr_user;
   std::string vr_pass;
   std::string vr_db;

   std::string cr_ip;
   std::string cr_user;
   std::string cr_pass;
   std::string cr_db;

   std::string m_config;
   int       scan_interval;
   std::string res_priority;
   std::string spider_type;
   std::string spider_divide_num; //spider分环数
   std::string spider_divide_index;//spider当前分环index
   std::string redis_address;
   int redis_port;
   //std::string unique_set_name;
   RedisTool *redis_tool;
};

#endif

