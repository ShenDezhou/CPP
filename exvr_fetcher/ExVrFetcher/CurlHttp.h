#pragma once

#include <curl/curl.h>
#include <string>

const static int DEFAULT_TIMEOUT = 3000;
const static int DEFAULT_CONNECT_TIMEOUT = 1000;
const static int CURL_MAX_RETRY_TIMES = 2;

class CurlHttp{

public:
	CurlHttp():curl_(0), m_down_timeout_ms(DEFAULT_TIMEOUT), m_connect_timeout_ms(DEFAULT_CONNECT_TIMEOUT), 
		m_retry_times(CURL_MAX_RETRY_TIMES){}
	~CurlHttp(){
		if(curl_)
			curl_easy_cleanup(curl_);
	}

	int setParams(int down_timeout_ms, int connect_timeout_ms, int retry_num);
	// easy send http request and recieve response
	int curlHttpGet(const std::string & url, std::string & response);	
	
	int curlHttpPost(const std::string & url, const std::string & param, std::string & response);
	int init();
	int init(int down_timeout_ms, int connect_timeout_ms, int retry_num_ms);

	void setTimeout(int timeout){  
		if(curl_ && timeout>0)
			curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS, timeout);
	}

	void setConnectTimeout(int timeout){  
		if(curl_ && timeout>0)
			curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT_MS, timeout);
	}

private:
	// call back function to write response
	static size_t htmlWriteCallback(void *ptr, size_t size, size_t nmemb, void *stream);
	
	// send and recv
	int sendAndRecv(const std::string& url, const std::string& param, std::string & response, int max_retry);

private:
	CURL *curl_;
	int m_down_timeout_ms;
	int m_connect_timeout_ms;
	int m_retry_times;

}; // end of class
