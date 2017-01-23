#pragma once

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include <sys/time.h>
#include <set>
#include <curl/curl.h>
#include "database.h"

#define WASTE_TIME_MS(past)\
	({\
	 timeval __now__;\
	 gettimeofday(&__now__, NULL);\
	 (__now__.tv_sec - past.tv_sec) * 1000 + (__now__.tv_usec - past.tv_usec) / 1000;\
	 })

/**
  *fetch the sitemap and update url
  *
  */
class CUrlUpdateTask: public ACE_Task<ACE_SYNCH> 
{
	public:
		static CUrlUpdateTask* instance ()
		{
			return ACE_Singleton<CUrlUpdateTask, ACE_Recursive_Thread_Mutex>::instance();
		}

		virtual int open (void * = 0);
		virtual int stopTask ();
		virtual int put (ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
		virtual int svc (void);
		int open (const char* configfile);
	
	private:
		/**
		  *get the urls from sitemap, and save in a map
		  *in: sitemap, the content of sitemap
		  *in: ds_id, datasource id
		  *out: urls_map
		  *out: err_url contains the url which length is over 255
		  *return: the amount of url
		  */
		int urlRetrieve(std::string &site_map, std::map<std::string, int> &urls_map, 
				int ds_id, std::string &err_url);
		/**
		  *fetch all the sitemap
		  *in: req, curl, mysql, site_map
		  *out: url_dsid_map, key is url, value is datasource_id
		  *out: ignored_ds, contains datasource_id which fetch fail, stiemap is unchanged, and the amount of url is 0
		  */
		int fetchSiteMap(request_t *req, CURL *curl, CMysql &mysql, std::string &site_map,
				std::map<std::string, int> &url_dsid_map, std::set<int> &ignored_ds, 
				std::string &fetch_detail);
		/**
		  *update the urls that are in the mysql
		  *in: req, mysql, url_dsid_map, ignored_ds
		  *return true:url are changed, false:url are unchanged
		  */
		bool updateUrl(request_t *req, CMysql &mysql, std::map<std::string, int> &url_dsid_map,
				std::set<int> &ignored_ds);
		/**
		  *reload url from mysql
		  *in: req, mysql
		  */
		int reloadUrl(request_t *req, CMysql &mysql);


	private:
		std::string vr_ip;
		std::string vr_user;
		std::string vr_pass;
		std::string vr_db;
		std::string m_config;

};
