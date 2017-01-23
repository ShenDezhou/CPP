#ifndef QUERY_CORE_CONFIGURATION_HPP
#define QUERY_CORE_CONFIGURATION_HPP

#include <vector>
#include <netinet/in.h>
#include <string>
#include <Platform/log.h>
#include <map>
#include "socket_utils.h"

struct HostInfo
{
	HostInfo():ip(""),port(0)
	{}
	std::string ip;
	int port;
};

class configuration {
	public:
		int open(const char *filename, const char *key);
		static std::string readString(char const * filename, char const * path, char const * name);
		std::string getStrConf(std::string key,std::string defaults = "")
		{
			std::map<std::string,std::string>::iterator iter ;
			iter = para_map.find(key);
			if(iter != para_map.end())
				return iter->second;
			SS_DEBUG((LM_ERROR,"[getStrConf] read key=%s error\n",key.c_str()));
			return defaults;
		}
		int getIntConf(std::string key,int defaults = 0)
		{
			std::map<std::string,std::string>::iterator iter ;
			iter = para_map.find(key);
			if(iter != para_map.end())
				return atoi(iter->second.c_str());
			SS_DEBUG((LM_ERROR,"[getIntConf] read key=%s error\n",key.c_str()));
			return defaults;
		}
	protected:
		int readParameter(const char *filename, const char * key);
		int readThreadNumber(const char *filename, const char * key);
		int readIndexClient(const char *filename, const char * key);
		int readMemCache(const char *filename, const char * key);	
		int readSummary(const char *filename, const char * key);				

private:
		std::map<std::string,std::string> para_map;
	public:
		int thread_size;
		std::vector<HostInfo> index_clients;
		std::vector<HostInfo> summarys;

};

#endif /* QUERY_CORE_SERVER_CONFIGURATION_HPP */

