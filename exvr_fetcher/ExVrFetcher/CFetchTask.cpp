#include "ace/Message_Block.h"
#include "ace/OS_Memory.h"
#include "ace/Guard_T.h"
#include <ace/Reactor.h>
#include <curl/curl.h>
#include <iostream>
#include <Platform/log.h>
#include "ExVrFetcher.h"
#include "CFetchTask.hpp"
#include "CWriteTask.hpp"
#include "Crawler.h"
#include "scribemonitor.h"
#include "Configuration.hpp"
#include "Util.hpp"
#include <vector>
#include "ConfigManager.h"

bool CFetchTask::speed_control = false;
int CFetchTask::speed = 0; //ÿ�����ץȡ����

int CFetchTask::open (const char* configfile) 
{
	ConfigurationFile config(configfile);
	m_config.assign(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) ){
		SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	vr_ip = section.Value<std::string>("VrResourceIp");
	vr_user = section.Value<std::string>("VrResourceUser");
	vr_pass = section.Value<std::string>("VrResourcePass");
	vr_db = section.Value<std::string>("VrResourceDb");
	return open();  
}
int CFetchTask::open (void*)
{
	int threadNum = ConfigManager::instance()->getFetchTaskNum();
	SS_DEBUG((LM_TRACE, "[CFetchTask::open] thread num:%d\n", threadNum));
	return activate(THR_NEW_LWP, threadNum);  
}

int CFetchTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
	request_t  *req = reinterpret_cast<request_t *>(message->rd_ptr());
	if (!req->error)
	{
		return putq(message, timeout);
	} 
	else 
	{
		SS_DEBUG((LM_ERROR, "CFetchTask::put: skip error request.\n"));
		message->release();
		delete req;
		_mem_check --;
		SS_DEBUG((LM_TRACE, "CFetchTask::svc(%@): memdebug: %d\n",this, _mem_check));
		//CWriteTask::instance()->put(message);
		return -1;
	}
	return 0;
}


int CFetchTask::svc() 
{
	CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	int reconn = 0;

	while( reconn < 5 )
	{//����mysql�����5��
		bool suc = true;
		if( mysql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql.connect() ) {
			SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( !mysql.open() ) {
			SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CFetchTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql.close();
		mysql.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@):Mysql: connect vr fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	SS_DEBUG((LM_TRACE, "CFetchTask::svc(%@): begin to loop\n", this));
	//��ʼ��ץȡcurl 
	struct curl_slist *slist=NULL;
	slist = curl_slist_append(slist, "Connection:keep-alive");
	CURL *curl = curl_easy_init();
	if(NULL == curl)
	{
		SS_DEBUG((LM_ERROR, "CFetchTask::svc: curl init failed\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	} 
	else 
	{
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, Crawler::html_write_callback);//��������ʱ�Ļص�����
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);//������ת
		curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 2);//��ת�Ĳ�� 
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);//���Ӳ���ʱ��Ҫcrash��
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, 90); //ץȡ��ʱ90s
		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10);
		curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 1);//�������ӵĸ���(�ܹ����´ε�ʹ�ñ��������)
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, "Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)");
	}

	//std::vector<std::string> urls;
	std::string html;
	ACE_Time_Value tmpt1;
	time_t interval = 1;
	int ret;
	request_t  *req = NULL;
	request_t  *dreq = NULL;
	vr_info		*curr_vi;		//��ǰҪץȡ��url�Ķ���
	ACE_Message_Block   *msg = NULL;

	for(ACE_Message_Block *msg_blk; getq(msg_blk) != -1; ) 
	{
		//��CWriteTask��ˮ�Ķ��д�С������30��(�߳�����5��)�����ץȡ��url̫��ʱ�������ڴ�̫������⡣
		int limit = ConfigManager::instance()->getWriteTaskNum() * 5;
		while(CWriteTask::instance()->msg_queue()->message_count() > limit) {
			sleep(1);
		}

		tmpt1=ACE_OS::gettimeofday();
		req = reinterpret_cast<request_t *>(msg_blk->rd_ptr());
		SS_DEBUG((LM_TRACE, "CFetchTask::svc class_id:%d queue_len:%d\n", req->class_id, msg_queue()->message_count()));

		//���󱣻�
		if(req->vr_info_list.empty()) {
			msg_blk->release();
			delete req->counter_t;
			delete req;
			req = NULL;
			continue;
		}

		curr_vi = req->vr_info_list[req->current_index];
		if( 0 == curr_vi->retries )
		{
			SS_DEBUG((LM_TRACE, "CFetchTask::svc(%lu): recv req %s, res_id=%d, done %d, total %d, que_num=%d\n", 
						pthread_self(), curr_vi->url.c_str(), req->res_id, req->current_index, req->vr_info_list.size(), 
						msg_queue()->message_count()));
			//��¼ץȡ��ʼʱ��
			if(req->current_index == 0)
			{
				Util::save_timestamp(&mysql, req->timestamp_id, "fetch_start_time");
			}
		}

		if(curr_vi->retries == 0 || curr_vi->request_tick + interval <= time(NULL)) 
		{//5s����ץһ��
			curr_vi->retries++;
			curr_vi->request_tick = time(NULL);
			timeval begin_fetch;
			gettimeofday(&begin_fetch, NULL);

			if(speed_control) 
			{
				unsigned long t = ACE_OS::gettimeofday().msec();
				{
					ACE_Read_Guard<ACE_RW_Thread_Mutex> guard_(mutex);
					if(last_fetch_time.find(curr_vi->url) != last_fetch_time.end()) 
						t -= last_fetch_time[curr_vi->url].msec();  
				}
				unsigned long mint = 1000 * thr_count() / speed; 
				if(t < mint && !ACE_Reactor::instance()->reactor_event_loop_done()) 
				{
					SS_DEBUG((LM_DEBUG, "CFetchTask::svc: speed control, will pause %u ms.\n", mint-t));
					usleep((mint - t)*1000);
				}
			}

			html.clear();
			ret = Crawler::instance()->get_html(curr_vi->url, html, curl);
			if(speed_control) 
			{
				{
					ACE_Write_Guard<ACE_RW_Thread_Mutex> guard_(mutex);
					last_fetch_time[curr_vi->url] = ACE_OS::gettimeofday();
				}
				SS_DEBUG((LM_DEBUG, "CFetchTask::svc: speed control, crawled %s.\n", curr_vi->url.c_str()));
			}
			int fetch_time = (int)(WASTE_TIME_MS(begin_fetch));//�õ�һ��url��ץȡʱ��(ms)

			//ץȡ�ɹ����߳���3��
			if(ret==0 || curr_vi->retries>3) 
			{
				dreq = new request_t(*req, html, *curr_vi);
				if(ret == 0)
				{
					SS_DEBUG((LM_DEBUG, "CFetchTask::svc: crawled (%s) cost(%d) html is:\n---html---\n",
							curr_vi->url.c_str(), fetch_time));
				}
			   	else 
				{
					dreq->error = true;

					//����ץȡʧ�ܵ�url��fetch_map��ȥ��
					pthread_mutex_lock(&fetch_map_mutex);//lock
					if( fetch_map.find(curr_vi->unqi_key) != fetch_map.end() && fetch_map[curr_vi->unqi_key] > 0 )
					{ 
						fetch_map[curr_vi->unqi_key] =0;
					}
					else
					{
						SS_DEBUG(( LM_ERROR, "CFetchTask::svc(%@): map error: %d %s\n",this, fetch_map[curr_vi->unqi_key], curr_vi->url.c_str() ));
					}    
					pthread_mutex_unlock(&fetch_map_mutex);//unlock
					SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@): crawled (%s) of res_id=%d fail, retry over 3 times.\n", this, curr_vi->url.c_str(), req->res_id));
				}
				//put to CWriteTask
				dreq->fetchTime=(ACE_OS::gettimeofday()-tmpt1).usec();
				_mem_check++;
				SS_DEBUG(( LM_TRACE, "CFetchTask::svc(%@): memdebug: %d, res_id=%d\n",this, _mem_check, req->res_id));
				msg = new ACE_Message_Block(reinterpret_cast<char *>(dreq));
				CWriteTask::instance()->put(msg);

				req->current_index++;
				if(req->current_index >= req->vr_info_list.size()) {
					//�������һ��urlץȡ������ʱ��
					Util::save_timestamp(&mysql, req->timestamp_id, "fetch_end_time");
					msg_blk->release();
					delete req;
					req = NULL;
					_mem_check--;
					SS_DEBUG(( LM_TRACE, "CFetchTask::svc(%@): memdebug: %d\n",this, _mem_check));
				} else { //����urlûץȡ���Żض���
					putq(msg_blk);
				}
				continue;
			}
			//ץȡʧ��
			SS_DEBUG((LM_ERROR, "CFetchTask::svc(%@): crawled (%s) of res_id=%d cost(%d) fail, retry %d already.\n", this, curr_vi->url.c_str(), req->res_id, fetch_time, curr_vi->retries));
		} 

		sleep(1);

		//�����������
		putq(msg_blk);

	}
	curl_easy_cleanup(curl);
	curl_slist_free_all(slist);
	SS_DEBUG((LM_TRACE, "CFetchTask::svc(%@): exit from loop\n", this));
	return 0;
}

int CFetchTask::stopTask()
{	
	flush();
	wait();
	return 0;
}



