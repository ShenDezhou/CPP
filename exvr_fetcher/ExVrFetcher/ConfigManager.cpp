#include <boost/algorithm/string.hpp>
#include "ConfigManager.h"
#include "Configuration.hpp"
#include "Platform/log.h"
#include <map>


int ConfigManager::loadOptions(const std::string &configFile)
{
	m_configFile = configFile;
	ConfigurationFile config(configFile.c_str());
	ConfigurationSection section;
	if(!config.GetSection("ExVrFetcher", section)) {
		SS_DEBUG((LM_ERROR, "[ConfigManager::loadOptions] Cannot find ExVrFetcher config section\n"));
		return -1;
	}

	std::map<std::string, int*>s_conf2para_int;
	s_conf2para_int.insert(std::make_pair("OnlineTaskNum", &m_options.onlineTaskNum));	
	s_conf2para_int.insert(std::make_pair("WriteTaskNum", &m_options.writeTaskNum));	
	s_conf2para_int.insert(std::make_pair("FetchTaskNum", &m_options.fetchTaskNum));	
	s_conf2para_int.insert(std::make_pair("UrlupdateTaskNum", &m_options.urlupdateTaskNum));	
	s_conf2para_int.insert(std::make_pair("MysqlHandleNum", &m_options.mysqlHandleNum));	
	s_conf2para_int.insert(std::make_pair("updateDeleteNum", &m_options.updateDeleteNum));	

	std::map<std::string, int*>::iterator it;
	for(it=s_conf2para_int.begin(); it!=s_conf2para_int.end(); it++)
	{
		int value = section.Value<int>(it->first.c_str());
		if(value<1 || value>20)
		{
			SS_DEBUG((LM_ERROR, "[ConfigManager::loadOptions] invalid %s: %d\n", it->first.c_str(), value));
			return -1;
		}
		*(it->second) = value;
		SS_DEBUG((LM_TRACE, "[ConfigManager::loadOptions] %s: %d\n", it->first.c_str(), value));
	}

	std::string servers = section.Value<std::string>("UrlTransformServers");
	if(servers.empty()) {
		SS_DEBUG((LM_ERROR, "[ConfigManager::loadOptions] config item UrlTransformServers is missing\n"));
		return -1;
	}
	boost::split(m_options.urlTransformServers, servers, boost::is_any_of(",;"), boost::token_compress_on);
	if(m_options.urlTransformServers.size() < 1) {
		SS_DEBUG((LM_ERROR, "[ConfigManager::loadOptions] UrlTransformServers is invalid: %s\n", servers.c_str()));
		return -1;
	}
	SS_DEBUG((LM_TRACE, "[ConfigManager::loadOptions] load UrlTransformServers is ok: %s\n", servers.c_str()));
	return 0;
}
