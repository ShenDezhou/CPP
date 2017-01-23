#include "stat_opt_word.h"

SS_LOG_MODULE_DEF(vrspider);

std::map<std::string, int> fetch_map;
pthread_mutex_t fetch_map_mutex = PTHREAD_MUTEX_INITIALIZER;

int main(int argc, char **argv)
{
	SS_LOG_UTIL->open();
	
	std::string vr_ip, vr_user, vr_pass, vr_db;
	vr_ip = "10.136.19.111";
	vr_user = "root";
	vr_pass = "";
	vr_db = "open_data";
	CMysql mysql_vr(vr_ip, vr_user, vr_pass, vr_db);
	
	if( !mysql_vr.connect() )
	{
		SS_DEBUG((LM_ERROR, "connect vr fail: %s :%s: %s :%s\n",mysql_vr.error(),vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str()));
		return(-1);
	}
	if( !mysql_vr.open() ) 
	{      
		SS_DEBUG((LM_ERROR, "Mysql: open database fail: %s\n",mysql_vr.error()));
		return(-2);
	}
	CMysqlExecute executor(&mysql_vr);
	executor.execute("set names gbk");
	
	int ret = stat_opt_word(70000700, 0, &mysql_vr);
	printf("stat_opt_word=%d\n", ret);
	
	return(0);
}
