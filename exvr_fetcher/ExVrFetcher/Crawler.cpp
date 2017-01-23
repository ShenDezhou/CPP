//#include "Base_Class.h" #include "Config_Manager.h" #include "Parser.h"
#include <iostream>
#include "Crawler.h"
#include <Platform/encoding.h>
#include <Platform/log.h>
//#include "CReCrawlTask.h"

int Crawler::get_url(request_t *req, std::vector<std::string>& urls) {
    urls.push_back(req->url);	
    return 0;
}

size_t Crawler::html_write_callback(void *ptr, size_t size, size_t nmemb, void *stream) 
{
    //char *buff = new char[size*nmemb + 1];
    //std::string* html = (std::string*)stream;
    //memcpy(buff, ptr, size * nmemb);
    //buff[size * nmemb] = 0;
    //html->append(buff);
    //delete[] buff;

    ((std::string*)stream)->append((const char*)ptr, size * nmemb);
    return nmemb * size;
}


int Crawler::get_html(const std::vector<std::string>& urls, std::string& html, CURL *curl, bool lasttime)
{
    if (curl==NULL)
        return -1;
    //char urltype[64];
    for (int i=0;i<urls.size();i++)
    {
        //sprintf(urltype,"%s%d",prd.c_str(),i);
        //if (CReCrawlTask::instance()->is_urlerror(urltype)) continue;

        CURLcode res;
        curl_easy_setopt(curl, CURLOPT_URL, urls[i].c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &html);

        res = curl_easy_perform(curl);
        bool failed = false;
        if(res != CURLE_OK) 
        {
            SS_DEBUG((LM_ERROR, "Crawler::get_html: requesting url: [%s] failed: %s\n", urls[i].c_str(),curl_easy_strerror(res)));
            failed = true;
        } 
        else 
        {
            long code;
            CURLcode res1,res2;
            res1 = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
            double time;
            res2 = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &time);

            char log[2048];
            std::string logstr;
            sprintf(log,"requesting url: [%s] success ",urls[i].c_str());
            logstr = log;
            if (res1 == CURLE_OK)
            {
                if(code!=200)
                    failed = true;
                sprintf(log,"success code: %ld ",code);
                logstr += log;
            }
            if (res2 == CURLE_OK)
            {
                sprintf(log,"cost time: %.2gms",time*1000);
                logstr += log;
            }
            SS_DEBUG((LM_TRACE, "Crawler::get_html:%s\n", logstr.c_str()));
        }

        /* always cleanup */
        //curl_easy_cleanup(curl);
        if (failed)
        {
            html.clear();
            //if (lasttime) CReCrawlTask::instance()->put_errorurl(urltype, urls[i]);
        } 
        else 
        {
            return 0;
        }
    }

    return -1;
}

int Crawler::get_html(const std::string& url, std::string& html, CURL *curl)
{
    if (curl==NULL) {
        return -1;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &html);

    CURLcode res = curl_easy_perform(curl);
    if(res != CURLE_OK) 
    {
        SS_DEBUG((LM_ERROR, "Crawler::get_html: requesting url: [%s] failed: %s\n", url.c_str(),curl_easy_strerror(res)));
        return -1;
    } 
    else 
    {
        double time = 0;
        long code = 0;
        CURLcode res1, res2;
        res1 = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        res2 = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &time);

        if (res1 != CURLE_OK || res2 != CURLE_OK || code != 200) {
            SS_DEBUG((LM_TRACE, "Crawler::get_html code and time failed[url:%s][code:%ld]\n", url.c_str(), code));
            return -1;
        }

        SS_DEBUG((LM_TRACE, "Crawler::get_html:%s, success code: %ld, cost time: %.2gms\n", url.c_str(), code, time*1000));
    }
    return 0;
}

