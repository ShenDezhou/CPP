#include <signal.h>
#include <unistd.h>
#include <malloc.h>
#include <fstream>
#include <ace/Reactor.h>
#include <ace/Dev_Poll_Reactor.h>
#include "Configuration.hpp"
#include <Platform/log.h>
#include "CWriteTask.hpp"
#include "CFetchTask.hpp"
#include "CScanTask.hpp"
#include "CDeleteTask.hpp"
#include "COnlineTask.hpp"
#include "CUrlUpdateTask.hpp"
#include <pthread.h>
#include <curl/curl.h>
#include "ConfigManager.h"

std::map<std::string, int> fetch_map;
//std::map<int, int> fetch_flag_map;
//std::map<int, int> update_flag_map;
int _mem_check = 0;
ACE_thread_t thr_id_scan = 0;
pthread_mutex_t fetch_map_mutex = PTHREAD_MUTEX_INITIALIZER;
//pthread_mutex_t  fetch_flag_mutex =PTHREAD_MUTEX_INITIALIZER; 
//pthread_mutex_t  update_flag_mutex =PTHREAD_MUTEX_INITIALIZER;

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


	init_suffix();
    //if (CScanTask::instance()->init()) return -1;

    //curl不是线程安全的，需要调用该函数提前创建好内存
    curl_global_init(CURL_GLOBAL_ALL);

	//initialize the mutex for vr counter
	for (int i = 0 ; i< 10 ; i++) {
		pthread_mutex_init(&counter_mutexes[i], NULL);
	}

	if(ConfigManager::instance()->loadOptions(conf) != 0) {
		SS_DEBUG((LM_ERROR, "main load configuration failed\n"));
		exit(-1);
	}

	CWriteTask::instance()->open(conf);
	CUrlUpdateTask::instance()->open(conf);
	CFetchTask::instance()->open(conf);
	CScanTask::instance()->open(conf);
	CDeleteTask::instance()->open(conf);
	COnlineTask::instance()->open(conf);
	ACE_Reactor::instance(new ACE_Reactor(new ACE_Dev_Poll_Reactor()), 1);
	ACE_Reactor::instance()->owner(ACE_Thread::self());

    //begin to loop now...
    ACE_Reactor::instance()->restart(1);
    ACE_Reactor::instance()->run_reactor_event_loop();

	//begin to exit...
	CScanTask::instance()->stopTask();
	CUrlUpdateTask::instance()->stopTask();
	CFetchTask::instance()->stopTask();
	CWriteTask::instance()->stopTask();
	CDeleteTask::instance()->stopTask();
	COnlineTask::instance()->stopTask();
	pthread_mutex_destroy(&fetch_map_mutex);
	//pthread_mutex_destroy(&fetch_flag_mutex);
	//pthread_mutex_destroy(&update_flag_mutex);
	//CCacheDirectory::instance()->close(); xmlCleanupParser(); summary::FreeVrSummaryLib();

	//destroy counter_mutexes
    for (int i = 0 ; i< 10 ; i++)
    {
        pthread_mutex_destroy(&counter_mutexes[i]);
    }
	free_suffix();

    curl_global_cleanup();
    return 0;       
}
 

