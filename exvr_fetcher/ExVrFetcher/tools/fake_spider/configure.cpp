#include "configure.hpp"
#include <string.h>
#include <netdb.h>
#include "config_map.hpp"

int configuration::open(const char *filename, const char *key)
{
	
	para_map.clear();
	summarys.clear();
	index_clients.clear();
	thread_size = 0;
	if (readParameter(filename, key))
		SS_ERROR_RETURN((LM_ERROR, "[readParameter] error\n"),-1);
	//if (readReaderClient(filename, key))
	//	SS_ERROR_RETURN((LM_ERROR, "[readIndexClient] error\n"),-1);
    //delete by ys

	return 0;
}

int configuration::readParameter(const char *filename, const char * key)
{
	config_map config;
	if (config.import(filename))
		SS_ERROR_RETURN((LM_ERROR, "[config_map::import(%s)]", filename),-1);

	std::string full_key(std::string(key) + "/Parameter");
	config.set_section(full_key.c_str());
	const char *value;
	int tmp_value;
	std::string tmp_param[] = {"ClassId","Tag","vr_ip",
	  "vr_user","vr_pass","vr_db","XmlDir","XmlPageReaderAddress",
	  "MultiHitXmlReaderAddress","IsGetOnline","XmlUrl"};
	
	int tmp_size = sizeof(tmp_param)/sizeof(tmp_param[0]);

	fprintf(stderr,"fmm_test size=%d\n",tmp_size);
	for(int i = 0 ; i < tmp_size;i++)
	{   
		if (config.get_value(tmp_param[i].c_str(), value))
			return -1;
		fprintf(stderr,"fmm_test %s=%s\n",tmp_param[i].c_str(),value);
		para_map[tmp_param[i]] = value;
	}

	return 0;
}

int configuration::readThreadNumber(const char *filename, const char * key)
{
	config_map config;
	if (config.import(filename))
		SS_ERROR_RETURN((LM_ERROR, "[config_map::import(%s)]", filename),-1);

	std::string full_key(std::string(key) + "/ThreadNumber");
	config.set_section(full_key.c_str());
	const char *value;
	
	std::string tmp_param[] = {"ServerReceNum","IQThreadNum","SumThreadNum","ServerReplyNum","MaxQueryConnNum","MaxSumConnNum","MaxQueryItemPerIP","MaxSumItemPerIP"};
	
	int tmp_size = sizeof(tmp_param)/sizeof(tmp_param[0]);
	
	for(int i = 0 ; i < tmp_size;i++)
	{
		if (config.get_value(tmp_param[i].c_str(), value))
			return -1;

		para_map[tmp_param[i]] = value;
	}

	std::map<std::string,std::string>::iterator iter;
	for(iter = para_map.begin();iter!=para_map.end();iter++)
	{
		fprintf(stderr,"fmm_test para %s=%s\n",iter->first.c_str(),iter->second.c_str());
	}
	return 0;
}

int configuration::readMemCache(const char *filename, const char * key)
{
	config_map config;
	if (config.import(filename))
		SS_ERROR_RETURN((LM_ERROR, "[config_map::import(%s)]", filename),-1);

	std::string full_key(std::string(key) + "/MemCache");
	config.set_section(full_key.c_str());
	const char *value;
	
	std::string tmp_param[] = {"MemCacheAddr","TimeOut"};
	
	int tmp_size = sizeof(tmp_param)/sizeof(tmp_param[0]);
	
	for(int i = 0 ; i < tmp_size;i++)
	{
		if (config.get_value(tmp_param[i].c_str(), value))
			return -1;

		para_map[tmp_param[i]] = value;
	}

	std::map<std::string,std::string>::iterator iter;
	for(iter = para_map.begin();iter!=para_map.end();iter++)
	{
		fprintf(stderr,"fmm_test para %s=%s\n",iter->first.c_str(),iter->second.c_str());
	}
	return 0;
}

int configuration::readIndexClient(const char *filename, const char * key)
{
	config_map config;
	if (config.import(filename))
		SS_ERROR_RETURN((LM_ERROR, "[config_map::import(%s)]", filename),-1);

	std::string full_key(std::string(key) + "/IndexClient");
	config.set_section(full_key.c_str());
	const char *enum_key, *enum_value;

	while (config.enum_value(enum_key, enum_value) == 0) {
		HostInfo tmp_info;
		
        fprintf(stderr,"fmm_test ip=%s\n",enum_key);
     
        if (strncmp(enum_key, "Server", sizeof "Server" - 1)) 
            continue;
		std::string tmp_ip;
		get_host_port(enum_value,tmp_ip,tmp_info.port);
		
		tmp_info.ip = hostnameToIp(tmp_ip);
		fprintf(stderr,"fmm_test ip=%s,port=%d\n",tmp_info.ip.c_str(),tmp_info.port);
		//index_clients.push_back(tmp_info);
        	index_clients.insert(index_clients.begin(),tmp_info);
	}

	if (index_clients.size() == 0)
	{
		SS_DEBUG((LM_INFO,"[configuration::readIndexClient] index size==0\n"));
		return 0;
	}
	return 0;
}


int configuration::readSummary(const char *filename, const char * key)
{
    config_map config;
    if (config.import(filename))
        SS_ERROR_RETURN((LM_ERROR, "[config_map::import(%s)]", filename),-1);

    std::string full_key(std::string(key) + "/summary");
    config.set_section(full_key.c_str());
    const char *enum_key;
    const char *enum_value;

    while (config.enum_value(enum_key, enum_value) == 0) {
		HostInfo tmp_info;
		
        fprintf(stderr,"fmm_test ip=%s\n",enum_key);
     
        if (strncmp(enum_key, "Server", sizeof "Server" - 1)) 
            continue;
		std::string tmp_ip;
		get_host_port(enum_value,tmp_ip,tmp_info.port);
		
		tmp_info.ip = hostnameToIp(tmp_ip);
     
        summarys.insert(summarys.begin(),tmp_info);
    }   

    for(int i = 0;i<summarys.size();i++)

    {   
        fprintf(stderr,"fmm_test summary %s=%d\n",summarys[i].ip.c_str(),summarys[i].port);
    }   
    if (summarys.size() == 0)
        SS_ERROR_RETURN((LM_ERROR,"[configuration::readSummary] summary size=0\n"),-1);
    
    return 0;
}

std::string configuration::readString(char const * filename, char const * path, char const * name)
{
	config_map config;
	std::string ret = "";
	const char * value;

	if (config.import(filename))
		return ret;

	config.set_section(path);
	if (config.get_value(name, value))
		return ret;

	ret = value;
	return ret;
}

