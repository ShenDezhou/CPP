#include <string>
#include <string.h>
#include <Platform/log.h>

#include "CurlHttp.h"

using namespace std;


// usePOST: false:GET, true:POST
int CurlHttp::init(int down_timeout_ms, int connect_timeout_ms, int retry_num){
    curl_ = curl_easy_init();
    m_down_timeout_ms = down_timeout_ms;
    m_connect_timeout_ms = connect_timeout_ms;
    m_retry_times = retry_num;
	struct curl_slist *slist  =  NULL;
    slist  =  curl_slist_append(slist, "Expect:");

    if(NULL == curl_)
    {   
	SS_ERROR((LM_ERROR, "CurlHttp::init: failed to init curl_.\n")); 
	return -1; 
    }   
    else 
    {   
	curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, CurlHttp::htmlWriteCallback);//接收数据时的回调函数
	curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1);//允许跳转
	curl_easy_setopt(curl_, CURLOPT_MAXREDIRS, 2);//跳转的层次 
	curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);//
	curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS, m_down_timeout_ms); //read response timeout
	curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT_MS, m_connect_timeout_ms); //connect timeout
	curl_easy_setopt(curl_, CURLOPT_FRESH_CONNECT, 0);// not using fresh connection
	curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, slist);
    }
    return 0;
}

int CurlHttp::init(){
    curl_ = curl_easy_init();
    struct curl_slist *slist  =  NULL;
    slist  =  curl_slist_append(slist, "Expect:");
    
    if(NULL == curl_)
    {   
        SS_ERROR((LM_ERROR, "CurlHttp::init: failed to init curl_.\n")); 
		return -1; 
    }   
    else 
    {   
        curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, CurlHttp::htmlWriteCallback);//接收数据时的回调函数
        curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1);//允许跳转
        curl_easy_setopt(curl_, CURLOPT_MAXREDIRS, 2);//跳转的层次 
        curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);//
	curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS, m_down_timeout_ms); //read response timeout
	curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT_MS, m_connect_timeout_ms); //connect timeout
        curl_easy_setopt(curl_, CURLOPT_FRESH_CONNECT, 0);// not using fresh connection
	curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, slist);
    }
	return 0;
}

int CurlHttp::setParams(int down_timeout_ms, int connect_timeout_ms, int retry_num){
    m_down_timeout_ms = down_timeout_ms;
    m_connect_timeout_ms = connect_timeout_ms;
    m_retry_times = retry_num;

	return 0;
}

// url:		url=host+?+params
int CurlHttp::curlHttpGet(const string & url, string & response){
    curl_easy_setopt(curl_, CURLOPT_POST, 0 );//设置请求格式为 GET
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);//指定接收数据的buffer
	curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
	std::string param;
	if(sendAndRecv(url, param, response, m_retry_times) < 0){
   		//fprintf(stderr, "CurlHttp::curlHttpGet: sendAndRecv() failed!\n");
		return -1;
	}
	return 0;
}

int CurlHttp::curlHttpPost(const string & url, const string & param, string & response){
	
    curl_easy_setopt(curl_, CURLOPT_POST, 1 );//设置请求格式为 POST
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);//指定接收数据的buffer
	curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, param.c_str());
	curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
	if(sendAndRecv(url, param, response, m_retry_times) < 0){
   		//fprintf(stderr, "CurlHttp::curlHttpPost: sendAndRecv() failed!\n");
		return -1;
	}
	return 0;
}

int CurlHttp::sendAndRecv(const std::string& url, const std::string& param, string & response, int max_retry){
	CURLcode ret;

	int retry = 0;
	response.clear();
    int cur_down_timeout_ms = m_down_timeout_ms;
	while(retry++<max_retry){
	    curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS, cur_down_timeout_ms); //read response timeout
	    ret = curl_easy_perform(curl_);
		double time = 0;
		curl_easy_getinfo(curl_, CURLINFO_TOTAL_TIME, &time);
		CURLcode res1;
        long code = -1;
        res1 = curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &code);
	    if(ret != CURLE_OK) // failed
        {
            SS_ERROR((LM_ERROR, "HttpRequest %s FAIL [%s] reason=%s, retry=%d\n, time=%f ms\n", 
                url.c_str(), param.c_str(), curl_easy_strerror(ret), retry, time*1000));
            response.clear();
        }
        else // success
        {
            SS_ERROR((LM_ERROR, "HttpRequest %s SUCCESS, code=%u, time=%fms\n", url.c_str(), code, time*1000));
            break;
        }
	}

	if(retry > max_retry){
		SS_ERROR((LM_ERROR, "HttpRequest %s ERROR [%s]\n", url.c_str(), param.c_str()));
        	return -1;
	}
    return 0;
}


size_t CurlHttp::htmlWriteCallback(void *ptr, size_t size, size_t nmemb, void *stream){
	/*
	char *buff = new char[size*nmemb + 1];
    std::string* html = (std::string*)stream;
    memcpy(buff, ptr, size * nmemb);
    buff[size * nmemb] = 0;
    html->append(buff);
    delete[] buff;
	*/
	std::string *html = (std::string *)stream;
	html->append((const char *)ptr, size * nmemb);
    return nmemb * size;	
}
