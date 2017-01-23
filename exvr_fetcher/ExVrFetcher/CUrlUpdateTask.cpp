#include "ace/Message_Block.h"
#include "ace/OS_Memory.h"
#include "ace/Guard_T.h"
#include <ace/Reactor.h>
#include <curl/curl.h>
#include <iostream>
#include <Platform/log.h>
#include <openssl/md5.h>
#include "ExVrFetcher.h"
#include "CUrlUpdateTask.hpp"
#include "CFetchTask.hpp"
#include "Crawler.h"
#include "scribemonitor.h"
#include "Configuration.hpp"
#include "Util.hpp"
#include <vector>
#include "ConfigManager.h"

int CUrlUpdateTask::open (const char* configfile) 
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
int CUrlUpdateTask::open (void*)
{
	int threadNum = ConfigManager::instance()->getUrlupdateTaskNum();
	SS_DEBUG((LM_TRACE, "[CUrlUpdateTask::open] thread num:%d\n", threadNum));
	return activate(THR_NEW_LWP, threadNum);
}

int CUrlUpdateTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
	request_t  *req = reinterpret_cast<request_t *>(message->rd_ptr());
	if (!req->error)
	{
		//has no site map info
		if(req->sitemap_info_map.empty())
		{
			CFetchTask::instance()->put(message);
		}
		else
		{
			return putq(message, timeout);
		}
	}
	else 
	{
		SS_DEBUG((LM_ERROR, "CUrlUpdateTask::put: skip error request.\n"));
		message->release();
		delete req;
		_mem_check --;
		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@): memdebug: %d\n",this, _mem_check));
		//CWriteTask::instance()->put(message);
		return -1;
	}
	return 0;
}

int CUrlUpdateTask::svc() 
{
	CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	int reconn = 0;

	while( reconn < 5 )
	{//????mysql??×??à5??
		bool suc = true;
		if( mysql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql.connect() ) {
			SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( !mysql.open() ) {
			SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql.close();
		mysql.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@):Mysql: connect vr fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@): begin to loop\n", this));
	//??????×???curl 
	struct curl_slist *slist=NULL;
	slist = curl_slist_append(slist, "Connection:keep-alive");
	CURL *curl = curl_easy_init();
	if(NULL == curl)
	{
		SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc: curl init failed\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	} 
	else 
	{
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, Crawler::html_write_callback);//?????????±?????÷????
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);//???í??×?
		curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 2);//??×??????? 
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);//?????????±????crash??
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, 90); //×??????±90s
		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10);
		curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 1);//±?????????????(????????????????±?????????)
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, "Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)");
	}

	std::string		site_map;
	request_t		*req = NULL;
	for(ACE_Message_Block *msg_blk; getq(msg_blk) != -1; ) 
	{
		req = reinterpret_cast<request_t *>(msg_blk->rd_ptr());

		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc class_id:%d queue_len:%d\n", req->class_id, msg_queue()->message_count()));

		std::map<std::string, int> url_dsid_map;
		std::set<int>	ignored_ds;
		std::string		fetch_detail;
		int				fetch_status;

		//fetch sitemap
		fetch_status = fetchSiteMap(req, curl, mysql, site_map, url_dsid_map, ignored_ds, fetch_detail);

		//update url
		if(updateUrl(req, mysql, url_dsid_map, ignored_ds))
		{
			//reload all the urls of a vr which url has changed
			reloadUrl(req, mysql);
		}

		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), vr_info_list size:%d\n", this, req->vr_info_list.size()));
		//update fetch status
		if(fetch_status != 2)
		{
			Util::update_fetchstatus(&mysql, req->res_id, fetch_status, fetch_detail);
			req->counter_t->fetch_detail.append(fetch_detail);
			req->counter_t->error_status = fetch_status;
		}
		if(req->is_manual && req->vr_info_list.empty())
		{
			char sql[1024];
			snprintf(sql, 1024, "update vr_resource set manual_update = 0 where id = %d", req->res_id);
			CMysqlExecute exe(&mysql);
			int retmp;
			while((retmp=exe.execute(sql)) < 1)
			{
				if( 0 == retmp )
					break;

				SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@) fail to update vr_resource.manual_update, "
							"vr_id:%d, error code: %d!\n", this, req->class_id, retmp));
				if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
					break;
				}
			}
		}
		CFetchTask::instance()->put(msg_blk);
	}
	curl_easy_cleanup(curl);
	curl_slist_free_all(slist);
	SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@): exit from loop\n", this));
	return 0;
}

int CUrlUpdateTask::fetchSiteMap(request_t *req, CURL *curl, CMysql &mysql, std::string &site_map,
		std::map<std::string, int> &url_dsid_map, std::set<int> &ignored_ds, std::string &fetch_detail)
{
	time_t	interval = 5;
	int		ret;
	time_t	request_tick = time(NULL);
	unsigned char	original_md5[MD5_DIGEST_LENGTH];  //md5 sum of current sitemap
	char			new_md5[33];  //hex md5 sum of current sitemap
	char			sql[512];
	std::string		msg_prefix;
	std::string		msg;
	msg_prefix.assign(req->res_name);
	msg_prefix.append(":");
	msg_prefix.append(req->tag);
	msg_prefix.append(":");
	//fetch each site map
	sitemap_info	*curr_sm;		//?±?°??×?????sitemap???ó
	std::map<int, sitemap_info *>::iterator itr;
	int fetch_status = 2;  //success by default
	std::string		err_url;
	for(itr=req->sitemap_info_map.begin(); itr!=req->sitemap_info_map.end(); ++itr)
	{
		curr_sm = itr->second;
		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::fetchSiteMap(%@):recv a vr, vr_id:%d, fetch:%s, total:%d\n", this,
					req->class_id, curr_sm->url_.c_str(), req->sitemap_info_map.size()));

		//fetch a site map 
		while(1)
		{
			if(curr_sm->retries==0 || request_tick+interval<=time(NULL))
			{
				curr_sm->retries++;
				request_tick = time(NULL);
				timeval begin_fetch;
				gettimeofday(&begin_fetch, NULL);

				site_map.clear();
				ret = Crawler::instance()->get_html(curr_sm->url_, site_map, curl);
				//×????????ò??????3??
				if(ret==0 || curr_sm->retries>3) 
				{
					if(ret == 0) //fetch success
					{
						SS_DEBUG((LM_TRACE, "CUrlUpdateTask::fetchSiteMap(%@): sitemap fetch success, vr_id:%d, url:%s\n", 
									this, req->class_id, curr_sm->url_.c_str()));
						//process the new urls
						MD5((const unsigned char*)site_map.c_str(), site_map.size(), original_md5);
						Util::hexstr(original_md5, new_md5, MD5_DIGEST_LENGTH);
						if(strlen(curr_sm->md5_)==0 || strncmp(curr_sm->md5_, new_md5, 32) 
								|| req->is_manual)
						{
							//site map has changed or manual udpate
							int ret = urlRetrieve(site_map, url_dsid_map, itr->first, err_url);
							if(ret==0 || ret==-1)
							{
								ignored_ds.insert(itr->first);
								msg.assign(msg_prefix);
								msg.append(curr_sm->url_.c_str());
								msg.append(Util::gbk_to_utf8("，[",", ["));
								msg.append(curr_sm->ds_name_);
								if(ret == 0)
									msg.append(Util::gbk_to_utf8(":sitemap中有效的url数为0]",":sitemap url num is zero]"));
								else
									msg.append(Util::gbk_to_utf8(":sitemap中含有无效的url地址]",":sitemap include useless url addr]"));
								fetch_detail.append(msg);
								fetch_detail.append("<br>");
								if(fetch_status == 2)
									fetch_status = 4;
								SS_DEBUG((LM_TRACE, "CUrlUpdateTask::fetchSiteMap(%@), site map is empty:%s\n", 
											this, msg.c_str()));
								break;
							}

							if(!err_url.empty())
							{
								msg.assign(msg_prefix);
								msg.append(err_url);
								msg.append(Util::gbk_to_utf8("，[",", ["));
								msg.append(curr_sm->ds_name_);
								msg.append(Util::gbk_to_utf8(":url长度超过255]",":url length over 255 byte]"));
								fetch_detail.append(msg);
								fetch_detail.append("<br>");
								if(fetch_status == 2)
									fetch_status = 4;
								SS_DEBUG((LM_TRACE, "CUrlUpdateTask::fetchSiteMap(%@), site map(%s) contains the url"
										   " which length is over 255.\n", this, msg.c_str()));
							}

							//update the md5 sum
							CMysqlExecute exe(&mysql);
							sprintf(sql, "update vr_open_datasource set sitemap_md5='%s' where id=%d", new_md5, itr->first);
							int retmp;
							while((retmp=exe.execute(sql)) < 1)
							{
								if( 0 == retmp )
									break;
								SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@)fail to Update vr_open_datasource, sql:%s, error code: %d!\n", this, sql, retmp));
								if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
									break;
								}
							}
						}
						else //md5 is unchanged
						{
							ignored_ds.insert(itr->first);
							SS_DEBUG((LM_TRACE, "CUrlUpdateTask::fetchSiteMap(%@), site map is unchanged:%s\n", this, curr_sm->url_.c_str()));
						}
					}
					else
					{
						ignored_ds.insert(itr->first);
						msg.assign(msg_prefix);
						msg.append(curr_sm->url_.c_str());
						msg.append(Util::gbk_to_utf8("，[",", ["));
						msg.append(curr_sm->ds_name_);
						msg.append(Util::gbk_to_utf8(":sitemap无法访问]",":sitemap can not acess]"));
						fetch_detail.append(msg);
						fetch_detail.append("<br>");
						fetch_status = 3;
						SS_DEBUG((LM_ERROR, "CUrlUpdateTask::fetchSiteMap(%@): sitemap fetch fail over 3 times, vr_id:%d, info:%s\n", 
									this, req->class_id, msg.c_str()));
					}
					break;
				}
				SS_DEBUG((LM_ERROR, "CUrlUpdateTask::fetchSiteMap(%@): sitemap fetch fail, vr_id:%d, url:%s\n", this,
							req->class_id, curr_sm->url_.c_str()));
			}
			sleep(1);
		}
	}
	return fetch_status;
}

bool CUrlUpdateTask::updateUrl(request_t *req, CMysql &mysql, std::map<std::string, int> &url_dsid_map,
		std::set<int> &ignored_ds)
{
	//update url, ignore the sitemap which fetch failed
	bool is_url_changed = false;
	char sql[1024];
	std::map<std::string, vr_info *> old_urls;
	std::vector<vr_info *>::const_iterator vi_itr;
	for(vi_itr=req->vr_info_list.begin(); vi_itr!=req->vr_info_list.end(); ++vi_itr)
	{
		old_urls.insert(std::make_pair((*vi_itr)->url, *vi_itr));
		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), old url:%s\n", this, (*vi_itr)->url.c_str()));
	}

	//insert a new url into mysql
	std::map<std::string, int>::const_iterator url_itr;
	std::map<std::string, vr_info *>::iterator old_itr;
	for(url_itr=url_dsid_map.begin(); url_itr!=url_dsid_map.end(); ++url_itr)
	{
		//get a new url
		old_itr = old_urls.find(url_itr->first);
		if(old_itr == old_urls.end())
		{
			CMysqlExecute exe(&mysql);
			snprintf(sql, 1024, "insert into resource_status(res_id, url, res_status, online_date, modify_date," 
					" md5_sum, sub_date, data_source_id) values(%d, '%s', 2, '0000-00-00 00:00:00', now(), " 
					"'', '0000-00-00 00:00:00', %d)", req->res_id, url_itr->first.c_str(), url_itr->second);
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), sql:%s\n", this, sql));
			int retmp;
			while((retmp=exe.execute(sql)) < 1)
			{
				if( 0 == retmp )
					break;
				SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@) fail to 	"
							"insert a new url failed, vr_id:%d, url:%s, error code: %d!\n", this, req->class_id, url_itr->first.c_str(), retmp));
				if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
					break;
				}
			}
			is_url_changed = true;
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), inserted a new url:%s\n", this, url_itr->first.c_str()));
		}
		//datasource id has been changed
		else if(url_itr->second != old_itr->second->ds_id)
		{
			CMysqlExecute exe(&mysql);
			snprintf(sql, 1024, "update resource_status set data_source_id=%d where id=%d", url_itr->second,
					old_itr->second->status_id);
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), sql:%s\n", this, sql));
			int retmp;
			while((retmp=exe.execute(sql)) < 1)
			{
				if( 0 == retmp )
					break;
				SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@) fail to update datasource id, sql:%s, vr_id:%d,"
						   " url:%s, error code: %d!\n", this, sql, req->class_id, url_itr->first.c_str(), retmp));
				if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
					break;
				}
			}
			is_url_changed = true;
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), updated a datasource id, new:%d, old:%d, status_id:%d\n", 
						this, url_itr->second, old_itr->second->ds_id, old_itr->second->status_id));
			old_itr->second->ds_id = url_itr->second;
		}
	}

	//remove the old url that don't exist in the site map
	std::map<std::string, vr_info *>::const_iterator old_url_itr;
	for(old_url_itr=old_urls.begin(); old_url_itr!=old_urls.end(); ++old_url_itr)
	{
		if(req->sitemap_info_map.find(old_url_itr->second->ds_id) != req->sitemap_info_map.end()
				&& ignored_ds.find(old_url_itr->second->ds_id) == ignored_ds.end()
				&& url_dsid_map.find(old_url_itr->first) == url_dsid_map.end())
		{
			CMysqlExecute exe(&mysql);
			snprintf(sql, 1024, "delete from resource_status where id=%d", old_url_itr->second->status_id);
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), sql:%s\n", this, sql));
			int retmp;
			while((retmp=exe.execute(sql)) < 1)
			{
				if( 0 == retmp )
					break;
				SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@)fail to failed to delete an old url, "
							"id:%d, url:%s, error code: %d!\n", this, old_url_itr->second->status_id, old_url_itr->first.c_str(), retmp));
				if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
					break;
				}
			}
			is_url_changed = true;
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), removed an old url:%s\n", this, old_url_itr->first.c_str()));
		}
	}

	return is_url_changed;
}

int CUrlUpdateTask::reloadUrl(request_t *req, CMysql &mysql)
{
	char sql[512];
	//clear old url list
	std::vector<vr_info *>::iterator vi_itr;
	pthread_mutex_lock(&fetch_map_mutex);
	for(vi_itr=req->vr_info_list.begin(); vi_itr!=req->vr_info_list.end(); ++vi_itr)
	{
		if(fetch_map.find((*vi_itr)->unqi_key)!=fetch_map.end() && fetch_map[(*vi_itr)->unqi_key]>0)
			fetch_map[(*vi_itr)->unqi_key] = 0;
		else
			SS_DEBUG(( LM_ERROR, "CUrlUpdateTask::svc(%@): map error: %d %s\n",this, fetch_map[(*vi_itr)->url], (*vi_itr)->url.c_str() ));
		//delete vr info
		delete *vi_itr;
	}
	req->url_uniq_set.clear();
	pthread_mutex_unlock(&fetch_map_mutex);
	req->vr_info_list.clear();
	SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), after clear vr_info_list, size:%d\n", this, req->vr_info_list.size()));

	//reload url list
	CMysqlQuery mysql_query(&mysql);
	snprintf(sql, 512, "select t1.id, t1.url, t1.data_source_id, t2.priority, t2.data_source from resource_status t1 inner join vr_open_datasource t2 on t1.data_source_id=t2.id where t1.res_id=%d  and t1.res_status>1", req->res_id);
	alarm(30);
	int ret = -1;
	ret = mysql_query.execute(sql);
	alarm(0);
	while(ret == -1)
	{
		if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CUrlUpdateTask::svc(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",
						this, mysql.error(), mysql.errorno()));
			break;
		} 
		alarm(30);
		ret = mysql_query.execute(sql);
		alarm(0);
	}
	int url_count = mysql_query.rows();
	int ds_id;
	std::string url;
	vr_info *vi;
	//initialize data source counter
	DataSourceCounter *ds_counter;
	std::map<int, DataSourceCounter*>::iterator ds_counter_itr;
	for(ds_counter_itr=req->counter_t->ds_counter.begin(); 
			ds_counter_itr!=req->counter_t->ds_counter.end(); ++ds_counter_itr)
	{
		ds_counter = ds_counter_itr->second;
		ds_counter->total_count = 0;
	}
	//get url info
	for(int irow=0; irow<url_count; ++irow)
	{
		CMysqlRow * row = mysql_query.fetch();
		if(!row)
		{
			SS_DEBUG((LM_ERROR, "CUrlUpdateTask::svc(%@): get vr row error:%d %s [%s]\n",
						this, irow, mysql.error(), mysql.errorno()));
			continue;
		}

		url.assign(row->fieldValue("url"));
		std::set<std::string>::iterator it_set;
		ds_id = atoi(row->fieldValue("data_source_id"));
		pthread_mutex_lock(&fetch_map_mutex);
		it_set = req->url_uniq_set.find(url);
		if(it_set != req->url_uniq_set.end())
		{
			SS_DEBUG((LM_ERROR,"CUrlUpdateTask::svc class_id = %d already have in url set\n",req->class_id));
			pthread_mutex_unlock(&fetch_map_mutex);//unlock
			continue;
		}
		req->url_uniq_set.insert(url);
		std::string unqi_key = req->classid_str + "#" + url;
		if( (fetch_map.find(unqi_key)!=fetch_map.end()) && fetch_map[unqi_key]>0 )
		{
			SS_DEBUG(( LM_ERROR, "CUrlUpdateTask::svc(%@): already fetching: %d %s, the value in fetch_map is %d\n",
						this,fetch_map[unqi_key], url.c_str() ,fetch_map[unqi_key]));
			pthread_mutex_unlock(&fetch_map_mutex);
			continue;
		}
		else
		{
			fetch_map[unqi_key] = 1;
		}
		pthread_mutex_unlock(&fetch_map_mutex);

		//generate vr info
		vi = new vr_info(url, atoi(row->fieldValue("id")), ds_id);
		vi->ds_priority = atoi(row->fieldValue("priority"));
		vi->ds_name.assign(row->fieldValue("data_source"));
		vi->unqi_key = unqi_key;
		//put a url into url list
		req->vr_info_list.push_back(vi);
		SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), reload a url:%s\n", this, url.c_str()));

		//update data source counter
		ds_counter_itr = req->counter_t->ds_counter.find(ds_id);
		if(ds_counter_itr == req->counter_t->ds_counter.end())
		{
			ds_counter = new DataSourceCounter();
			ds_counter->total_count += 1;
			ds_counter->ds_name = vi->ds_name;
			req->counter_t->ds_counter.insert(std::make_pair(ds_id, ds_counter));
		}
		else
		{
			ds_counter_itr->second->total_count += 1;
		}
	}
	//update counter
	req->counter_t->setCount(req->vr_info_list.size() - 1);
	SS_DEBUG((LM_TRACE, "CUrlUpdateTask::svc(%@), after reload urls, vr_info_list size:%d\n", this, req->vr_info_list.size()));
	return 0;
}


int CUrlUpdateTask::urlRetrieve(std::string &site_map, std::map<std::string, int> &urls_map, 
		int ds_id, std::string &err_url)
{
	int start = 0;
	int end;
	std::string url;
	int idx;
	int count = 0;
	int len;
	err_url.clear();
	bool has_format_error = false;
	for(idx=0; idx<site_map.size(); ++idx)
	{
		if((site_map[idx]=='\r' && idx+1<site_map.size() && site_map[idx+1]=='\n') ||
				site_map[idx] == '\n')
		{
			//trim left
			while(site_map[start]==' ' || site_map[start]=='\t')
				++start;
			//trim right
			end = idx - 1;
			while(end>-1 && (site_map[end]==' ' || site_map[end]=='\t'))
				--end;
			if(end >= start)
			{
				len = end - start + 1;
				url.assign(site_map.substr(start, len));
				if(len<4 || url[0]!='h' || url[1]!='t' || url[2]!='t' || url[3]!='p')
				{
					has_format_error = true;
				}
				else if(len <= 255)
				{
					urls_map[url] = ds_id;
					count++;
					SS_DEBUG((LM_TRACE, "CUrlUpdateTask::urlRetrieve(%@): get a url: %s\n", this, url.c_str()));
				}
				else
				{
					err_url.append(url);
					err_url.append("<br>");
					SS_DEBUG((LM_ERROR, "CUrlUpdateTask::urlRetrieve(%@): the length of %s is over 255", this, url.c_str()));
				}
			}

			if(site_map[idx]=='\r')
			{
				start = idx + 2;
				idx += 1;
			}
			else
			{
				start = idx + 1;
			}
		}
	}
	//trim left
	while(start<idx && (site_map[start]==' ' || site_map[start]=='\t'))
		++start;
	//trim right
	end = idx - 1;
	while(end>-1 && (site_map[end]==' ' || site_map[end]=='\t'))
		--end;
	if(end >= start)
	{
		len = end - start + 1;
		url.assign(site_map.substr(start, len));
		if(len<4 || url[0]!='h' || url[1]!='t' || url[2]!='t' || url[3]!='p')
		{
			has_format_error = true;
		}
		else if(len <= 255)
		{
			urls_map[url] = ds_id;
			count++;
			SS_DEBUG((LM_TRACE, "CUrlUpdateTask::urlRetrieve(%@): get a url: %s\n", this, url.c_str()));
		}
		else
		{
			err_url.append(url);
			err_url.append("<br>");
			SS_DEBUG((LM_ERROR, "CUrlUpdateTask::urlRetrieve(%@): the length of %s is over 255", this, url.c_str()));
		}
	}

	if(has_format_error)
		return -1;

	return count;
}

int CUrlUpdateTask::stopTask()
{
	flush();
	wait();
	return 0;
}
