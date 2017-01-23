#include "ExVrFetcher.h"
#include <curl/curl.h>
#include <string>
#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <vector>

class Crawler{
public:
  	static Crawler* instance()
  	{
    		return ACE_Singleton<Crawler, ACE_Recursive_Thread_Mutex>::instance();
  	}

public:
	int get_url(request_t *req, std::vector<std::string>& urls);

	int get_html(const std::vector<std::string>& url, std::string& html, CURL *curl, bool lasttime=false); 

    // just fetch one url
	int get_html(const std::string& url, std::string& html, CURL *curl);

	//int parse_html(request_t *req, std::string& summary);

public:
	static size_t html_write_callback(void *ptr, size_t size, size_t nmemb, void *stream); 

};
