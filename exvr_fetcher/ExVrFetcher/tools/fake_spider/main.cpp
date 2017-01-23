#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <fstream>
#include <streambuf>
#include <string>
#include <malloc.h>
#include <ace/Reactor.h>
#include <ace/INET_Addr.h>   
#include <ace/Acceptor.h>   
#include <ace/Dev_Poll_Reactor.h>   
#include <ace/Svc_Handler.h>   
#include <ace/SOCK_Stream.h>  
#include <ace/SOCK_Acceptor.h>  
#include "Platform/log.h"
#include "Platform/signal/sig.h"
#include "Platform/bchar_cxx.h"
#include <Platform/encoding.h>
#include "configure.hpp"
#include "process.hpp"
#include <curl/curl.h>
#include <dirent.h>
#include "../../database.h"

size_t write_function(void *ptr, size_t size, size_t nmemb, void *stream);
int get_xml(std::string& xml,const std::string& url);
int get_xpath_value(std::map<std::string,std::string>& xmap,const std::string& tmp);

SS_LOG_MODULE_DEF(exvr_fetcher_server);//设置log的模块..

configuration g_configure;

#define CONFIG_KEY "VR/Fake"

static void sighandler(int signum)
{
    ACE_Reactor::instance()->end_reactor_event_loop();
}


int getOpenData(request_t *req)
{
	int ret = 0;
	if(req->class_id >= 70000000)
	{
		CMysql mysql(g_configure.getStrConf("vr_ip"), g_configure.getStrConf("vr_user"), g_configure.getStrConf("vr_pass"), g_configure.getStrConf("vr_db"), "set names gbk");
		int reconn = 0;


		while( reconn < 5 )
		{//重连mysql，最多5次
			bool suc = true;
			if( mysql.isClosed() ) {
				SS_DEBUG((LM_ERROR, "CScanTask::svc:Mysql: init fail\n"));
				suc = false;
			}
			if( !mysql.connect() ) {
				SS_DEBUG((LM_ERROR, "CScanTask::svc:Mysql: connect server fail: %s\n", mysql.error()));
				suc = false;
			}
			if( !mysql.open() ) {
				SS_DEBUG((LM_ERROR, "CScanTask::svc:Mysql: open database fail: %s\n", mysql.error()));
				suc = false;
			}
			if( suc ){
				CMysqlExecute executor(&mysql);
				executor.execute("set names gbk");
				SS_DEBUG((LM_TRACE, "CScanTask::svc:Mysql vr success\n"));
				break;
			}
			reconn ++;
			mysql.close();
			mysql.init();
		}
		if( reconn >=5 ) 
		{
			SS_DEBUG((LM_ERROR, "CScanTask::svc:Mysql: connect vr fail\n"));
			//ACE_Reactor::instance()->end_reactor_event_loop();
			return -1;
		}
		CMysqlQuery format_query(&mysql);
		
		//execute query and get result use func 
		char sql_buf[512];
		snprintf(sql_buf,512,"select label_name, parent_label,index_type, is_mark, is_fullhit, \
				short_label, is_index, second_query, group_type from open_xml_format where vr_id=%d",req->class_id);
		SS_DEBUG((LM_DEBUG,"format sql: %s\n", sql_buf));
		alarm(30);
		ret = format_query.execute(sql_buf);
		alarm(0);
		while(ret == -1)
		{
			if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource:Mysql[]failed, reason:[%s],errorno[%d]\n",
							mysql.error(), mysql.errorno()));
				return -1;
			} 
			alarm(30);
			ret = format_query.execute(sql_buf);
			alarm(0);
		}

		int format_count = format_query.rows();
		if(format_count > 0)
		{
			std::string xpath, parent;
			bool is_annotation=false, is_hit=false;
			bool is_index=false, is_second_query=false;
			int index=0, sort=0, group_type=0,full_text = 0;
			for(int format_irow=0; format_irow<format_count; ++format_irow)
			{
				CMysqlRow *f_mysqlrow = format_query.fetch();
				//get the xpath info
				if(f_mysqlrow->fieldValue("parent_label") != NULL)
					parent.assign(f_mysqlrow->fieldValue("parent_label"));
				if(parent.size()>0 && parent[0]=='/')
					xpath.assign("/");
				else
					xpath.assign("//");
				xpath.append(parent);
				xpath.append("/");
				xpath.append(f_mysqlrow->fieldValue("label_name"));

				//get the annotation info
				is_annotation = atoi(f_mysqlrow->fieldValue("is_mark"))>0 ? true : false;
				if(is_annotation)
					req->annotation_map.insert(std::make_pair(xpath, "1"));

				//get the index info
				is_index = atoi(f_mysqlrow->fieldValue("is_index"))==0 ? false : true;
				if(is_index)
				{
					index = atoi(f_mysqlrow->fieldValue("is_fullhit"));
					if(index == 1) //no segment
						req->index_map.insert(std::make_pair(xpath, "1"));
					else if(index == 2) //need segment
						req->index_map.insert(std::make_pair(xpath, "2"));
				}

				//get the sort info
				sort = atoi(f_mysqlrow->fieldValue("short_label"));
				if(sort != 0)
					req->sort_map.insert(std::make_pair(xpath, f_mysqlrow->fieldValue("short_label")));
				
				full_text = atoi(f_mysqlrow->fieldValue("index_type"));
				if(full_text == 1)
					req->full_text_map.insert(std::make_pair(xpath, f_mysqlrow->fieldValue("index_type")));
				//get the hit field info
				is_hit = atoi(f_mysqlrow->fieldValue("is_fullhit"))==0 ? false : true;
				if(is_hit)
					req->hit_fields.insert(std::make_pair(f_mysqlrow->fieldValue("label_name"), xpath));

				//get the second query info
				is_second_query = atoi(f_mysqlrow->fieldValue("second_query"))>0 ? true : false;
				if(is_second_query)
					req->second_query_map.insert(std::make_pair(xpath, "1"));

				//get the group info
				group_type = atoi(f_mysqlrow->fieldValue("group_type"));
				if(group_type>=1 && group_type<=3)
					req->group_map.insert(std::make_pair(xpath, f_mysqlrow->fieldValue("group_type")));
			}
		}
		else
		{
			SS_DEBUG((LM_ERROR,"get_resource:format error, no format config info, vr_id:%d\n",
						req->class_id));
			ret = -1;
		}
	}
	else
	{
		SS_DEBUG((LM_ERROR,"get_resource:format error,vr_id not mutilhit class  vr_id=%d\n",req->class_id));
		ret = -1;
	}
	return ret;
}

int main(int argc,char* argv[])
{
    mallopt(M_MMAP_THRESHOLD, 256*1024);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, sighandler);
    signal(SIGTERM, sighandler);
	
	setenv("LC_CTYPE","zh_CN.gbk",1);//设置环境语言编码，gbk
    SS_INIT_LOCALE();
	
	SS_LOG_UTIL->open();//打开log，如果不打开，线程号，时间等信息都无法打印...
    std::string tmp_config_path = "fake_spider/fake_spider.cfg";
    if(argc > 1)
        tmp_config_path = argv[1];

	if(g_configure.open(tmp_config_path.c_str(),CONFIG_KEY))
		SS_ERROR_RETURN((LM_ERROR,"[main] open configure error\n"),-1);
     
	std::string tmp,xmlurl,xmldata;


	request_t *req = new request_t();
	req->class_id = g_configure.getIntConf("ClassId"); 
	req->tag = g_configure.getStrConf("Tag");
	req->reader_addr=g_configure.getStrConf("XmlPageReaderAddress");
	req->multihit_reader_addr=g_configure.getStrConf("MultiHitXmlReaderAddress");
	/*
	tmp = g_configure.getStrConf("Annotation");
	get_xpath_value(req->annotation_map,tmp);
    tmp = g_configure.getStrConf("Index");
	get_xpath_value(req->index_map,tmp);
	tmp = g_configure.getStrConf("Sort");
	get_xpath_value(req->sort_map,tmp);
    tmp = g_configure.getStrConf("SecondQuery");
	get_xpath_value(req->second_query_map,tmp);
    tmp = g_configure.getStrConf("Group");
	get_xpath_value(req->group_map,tmp);
	*/
	getOpenData(req);
	
	int isgetonline=0;
    isgetonline = g_configure.getIntConf("IsGetOnline");
	if (isgetonline) {
		xmlurl = g_configure.getStrConf("XmlUrl");
		if (get_xml(xmldata,xmlurl))
			SS_ERROR_RETURN((LM_ERROR,"[main] get online xml error\n"),-1);
	} else {
		std::string filelist_path = g_configure.getStrConf("XmlDir");
		std::string::size_type pos = filelist_path.find_last_not_of('/'); 
		if(pos != std::string::npos)
		{
			filelist_path.erase(pos + 1); 
		}
		

		DIR *dp;
		struct dirent *dirp;
		if((dp = opendir(filelist_path.c_str()))== NULL)
		{
			SS_DEBUG((LM_DEBUG,"open dir %s  failed.\n",filelist_path.c_str()));
			return -1;
		} 
		
		while((dirp = readdir(dp)) != NULL)
		{
			if(strcmp(dirp->d_name, ".")==0 || strcmp(dirp->d_name, "..")==0) continue;
				
			std::string file_name = filelist_path + '/' + dirp->d_name;
			std::ifstream xmlread(file_name);
			xmldata.assign((std::istreambuf_iterator<char>(xmlread)),std::istreambuf_iterator<char>());
			xmlread.close();
			process(req,xmldata);
		}
		closedir(dp);
	
	}
	

    SS_DEBUG((LM_DEBUG,"[main] stoped ok\n"));	
    delete req;
    //SS_DEBUG((LM_DEBUG,"index:%s\n",g_configure.index_clients[0].ip.c_str()));

	return 0;
}

size_t write_function(void *ptr, size_t size, size_t nmemb, void *stream)
{
  ((std::string*)stream)->append((const char*)ptr, size * nmemb);
  return nmemb * size;
}
//download xml from url
int get_xml(std::string& xml,const std::string& url)
{
  CURL *curl_handle;
  CURLcode res;
  struct curl_slist *slist=NULL;
  slist = curl_slist_append(slist, "Connection:keep-alive");
  
  curl_handle = curl_easy_init();

  if (curl_handle) {
    curl_easy_setopt(curl_handle,CURLOPT_WRITEFUNCTION,write_function);
	curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 2);
	curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 90);
	curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 10);
	curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1);
	curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, slist);
	curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)");
	curl_easy_setopt(curl_handle,CURLOPT_URL,url.c_str());
    curl_easy_setopt(curl_handle,CURLOPT_WRITEDATA,&xml);
	
	res = curl_easy_perform(curl_handle);
    
	if (res != CURLE_OK) {
      curl_easy_cleanup(curl_handle);
	  curl_slist_free_all(slist);
	  return -1;
	}
  } else {
    return -1;
  }
  curl_easy_cleanup(curl_handle);
  curl_slist_free_all(slist);
  return 0;
}

//get xpath and it's value
int get_xpath_value(std::map<std::string,std::string>& xmap,const std::string& tmp)
{
  size_t lastfound=0,found=0;
  std::string xpath,value;
  
  while (lastfound<tmp.size()&&(found=tmp.find(":",lastfound))!=std::string::npos)
  {
    xpath=tmp.substr(lastfound,found-lastfound);
	lastfound=found+1;
	found=tmp.find(";",lastfound);
	if (found==std::string::npos) {
	  value=tmp.substr(lastfound,tmp.size()-lastfound);
	  xmap[xpath]=value;
	  break;
	}
	else 
      value=tmp.substr(lastfound,found-lastfound);
	lastfound=found+1;
	xmap[xpath]=value;
  }
  return 0;
}	
