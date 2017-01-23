#include <signal.h>
#include <unistd.h>
#include <malloc.h>
#include <fstream>
#include <ace/Reactor.h>
#include <ace/Dev_Poll_Reactor.h>
#include "Configuration.hpp"
#include <Platform/log.h>
#include <curl/curl.h>
#include "ConfigManager.h"
#include "fetcher_schedule_scan.hpp" 


std::map<std::string, int> fetch_map;
int _mem_check = 0;
ACE_thread_t thr_id_scan = 0;
pthread_mutex_t fetch_map_mutex = PTHREAD_MUTEX_INITIALIZER;


pthread_mutex_t  counter_mutexes[10];

static void sighandler(int signum)
{
    ACE_Reactor::instance()->end_reactor_event_loop();
}
 
static void sig_alarm_handler(int signum)
{
    if( thr_id_scan ){
        ACE_Thread_Manager::instance()->kill(thr_id_scan, SIGCONT);
        SS_DEBUG((LM_ERROR, "SIGCONT to %d\n", thr_id_scan));
    }else{
        SS_DEBUG((LM_ERROR, "SIGCONT to error:%d\n", thr_id_scan));
    }
    signal (SIGALRM, sig_alarm_handler);
}
 
SS_LOG_MODULE_DEF(vrspider);

int main(int argc, char* argv[])
{
    mallopt(M_MMAP_THRESHOLD, 256*1024); 

    signal(SIGALRM, sig_alarm_handler);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, sighandler);
    signal(SIGTERM, sighandler);

    //EncodingConvertor::initializeInstance(); xmlInitParser();

    const char *conf = argc > 1 ? argv[1] : "exvr_fetcher.cfg"; 
    FILE *fp = fopen(conf,"r");
    if( NULL == fp ){
        fprintf(stderr, "%s not found.\n", conf);
        exit(-1);
    }else{
        fclose(fp);
    }

	SS_LOG_UTIL->open();
    u_long priority_mask = SS_LOG_UTIL->process_priority_mask(ACE_Log_Msg::PROCESS);
    u_long log_flags = 0;

    ConfigurationFile config(conf);
    ConfigurationSection section;
    if( !config.GetSection("ExVrFetcher",section) ){
        SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
        exit(-1);
    }
    std::string debug = section.Value<std::string>("DebugLog");
    if( -1 == debug.find("yes") )
        priority_mask &= ~LM_DEBUG;

	SS_LOG_MODULE_DECL(vrspider);
	SS_LOG_MODULE(vrspider).priority_mask(priority_mask);

    //WEBSEARCH_LOG->clr_flags(ACE_Log_Msg::STDERR);
    //WEBSEARCH_LOG->msg_ostream(new ofstream(Config_Manager::instance()->get_log_file().c_str(), ios::trunc|ios::ate));
    //WEBSEARCH_LOG->msg_ostream(new ofstream("err.log", ios::trunc|ios::ate));
    //log_flags |= ACE_Log_Msg::OSTREAM;
    SS_LOG_UTIL->set_flags(log_flags);    



    // if (CScanTask::instance()->init()) return -1;

    // curl不是线程安全的，需要调用该函数提前创建好内存
    curl_global_init(CURL_GLOBAL_ALL);

	// initialize the mutex for vr counter
	for (int i = 0 ; i< 10 ; i++) {
		pthread_mutex_init(&counter_mutexes[i], NULL);
	}

	//if(ConfigManager::instance()->loadOptions(conf) != 0) {
	//	SS_DEBUG((LM_ERROR, "main load configuration failed\n"));
	//	exit(-1);
	//}
	
	std::string spider_type_str = section.Value<std::string>("SpiderType");
    std::vector<std::string> spider_type_vec;
    splitStr(spider_type_str, ';', spider_type_vec);
	std::set<std::string> spider_type_set;
	for (int i = 0; i < spider_type_vec.size(); i++)
	{
		if (spider_type_vec[i].size() > 0)
			spider_type_set.insert(spider_type_vec[i]);
	}
	std::string check_interval_str = section.Value<std::string>("CheckInterval");
    std::vector<std::string> check_interval_vec;
    splitStr(check_interval_str, ';', check_interval_vec);

	std::string scan_interval_str = section.Value<std::string>("ScanInterval");
    std::vector<std::string> scan_interval_vec;
    splitStr(scan_interval_str, ';', scan_interval_vec);
	if (check_interval_str == "" || scan_interval_str == "" || check_interval_vec.size() != scan_interval_vec.size())
    {
        SS_DEBUG((LM_ERROR, "Configuration CheckInterval and CheckInterval error %s %s\n", check_interval_str.c_str(), scan_interval_str.c_str()));
        return -1;
    }
	
    if (check_interval_vec.size() != spider_type_set.size())
    {
        SS_DEBUG((LM_ERROR, "Configuration CheckInterval or SpiderType error %s %s\n", check_interval_str.c_str(), spider_type_str.c_str()));
        return -1;
    }

    for (int i = 0; i < spider_type_vec.size(); i++)
    {
        if (FetcherScheduleScan::init(conf, spider_type_vec[i], check_interval_vec[i], scan_interval_vec[i]) < 0)
        {
            SS_DEBUG((LM_ERROR, "Configuration error\n"));
            return -1;
        }
    }
    // start fetcher_engine_scan threads
	for(int i=0; i< fetcher_schedule_scans.size(); i++) fetcher_schedule_scans[i]->open(NULL);

	ACE_Reactor::instance(new ACE_Reactor(new ACE_Dev_Poll_Reactor()), 1);
	ACE_Reactor::instance()->owner(ACE_Thread::self());

    // begin to loop now...
    ACE_Reactor::instance()->restart(1);
    ACE_Reactor::instance()->run_reactor_event_loop();

	// begin to exit...
    for(int i=0; i< fetcher_schedule_scans.size(); i++) fetcher_schedule_scans[i]->stopTask();
	pthread_mutex_destroy(&fetch_map_mutex);

	// destroy counter_mutexes
    for (int i = 0 ; i< 10 ; i++)
    {
        pthread_mutex_destroy(&counter_mutexes[i]);
    }


    curl_global_cleanup();
    return 0;       
}
 

