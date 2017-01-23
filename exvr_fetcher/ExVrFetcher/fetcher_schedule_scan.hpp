#ifndef  __FetcherScheduleScan_H  
#define  __FetcherScheduleScan_H 

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include "database.h"
#include <vector>
#include "redis_tool.h"
#include <set>
#include <string>

#define MAX_FETCH_PER_MiNU 12000

#define LEVEL1_FETCH_FREQUENCY      15*60
#define LEVEL2_FETCH_FREQUENCY      60*60
#define LEVEL3_FETCH_FREQUENCY    6*60*60
#define LEVEL4_FETCH_FREQUENCY   24*60*60
#define LEVEL5_FETCH_FREQUENCY 7*24*60*60

class FetcherScheduleScan: public ACE_Task<ACE_SYNCH> {
public:
//   static FetcherScheduleScan* instance () {
//      return ACE_Singleton<FetcherScheduleScan, ACE_Recursive_Thread_Mutex>::instance();
//   }
   virtual int open (void * = 0);
   virtual int stopTask ();
   virtual int svc (void);
//   int open (const char* configfile, int spider_type);
   static int init(const char* configfile, std::string spider_type, std::string check_interval, std::string scan_interval);
   FetcherScheduleScan(std::string vr_ip, std::string vr_user, std::string vr_pass, std::string vr_db,
      std::string cr_ip, std::string cr_user, std::string cr_pass, std::string cr_db, std::string m_config,
      int  scan_interval, int spider_type, std::string spider_name, std::string manual_sql, std::string auto_sql, std::string delete_sql,
      std::string redis_address, int redis_port, std::string redis_password, int check_interval):vr_ip(vr_ip), vr_user(vr_user), vr_pass(vr_pass), vr_db(vr_db), cr_ip(cr_ip),
      cr_user(cr_user), cr_pass(cr_pass), cr_db(cr_db), m_config(m_config), scan_interval(scan_interval), spider_type(spider_type),
      spider_name(spider_name), manual_sql(manual_sql), auto_sql(auto_sql), delete_sql(delete_sql), redis_address(redis_address), redis_port(redis_port),
      redis_password(redis_password),check_interval(check_interval)
   {}

private:
   int get_resource( CMysql *mysql, bool is_delete,const std::string &sql, RedisTool *redis_tool, bool is_manual);

   int get_customer_resource( CMysql *mysql, std::string sql, RedisTool *redis_tool, bool is_manual = false);
   int AddSomeUniqueName(RedisTool *redis_tool, int start, int num);
   int CheckSpider(RedisTool *redis_tool);
   std::string vr_ip;
   std::string vr_user;
   std::string vr_pass;
   std::string vr_db;

   std::string cr_ip;
   std::string cr_user;
   std::string cr_pass;
   std::string cr_db;

   std::string m_config;
   int         scan_interval;
   int spider_type;
   std::string spider_name;
   std::string manual_sql;
   std::string auto_sql;
   std::string delete_sql;
   std::string redis_list_name;
   std::string redis_address;
   std::string redis_password;
   int redis_port;
   int check_interval;
};

extern std::vector<FetcherScheduleScan *> fetcher_schedule_scans;
extern int splitStr(const std::string &str, const char separator, std::vector<std::string> &res);

#endif

