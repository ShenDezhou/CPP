#pragma once
#include <string>
#include <ace/Singleton.h>
#include <ace/Thread.h>
#include <vector>


//spider配置项
struct Options {
	int onlineTaskNum;
	int writeTaskNum;
	int fetchTaskNum;
	int urlupdateTaskNum;
	int mysqlHandleNum;
	int updateDeleteNum;
	std::vector<std::string> urlTransformServers;
};

//配置文件管理器
class ConfigManager {
	friend class ACE_Singleton<ConfigManager, ACE_Recursive_Thread_Mutex>;

public:
	static ConfigManager * instance()
	{
		return ACE_Singleton<ConfigManager, ACE_Recursive_Thread_Mutex>::instance();
	}

	int loadOptions(const std::string &configFile);

	int getOnlineTaskNum()
	{
		return m_options.onlineTaskNum;
	}

	int getWriteTaskNum()
	{
		return m_options.writeTaskNum;
	}

	int getFetchTaskNum()
	{
		return m_options.fetchTaskNum;
	}

	int getUrlupdateTaskNum()
	{
		return m_options.urlupdateTaskNum;
	}

	int getmysqlHandleNum()
	{
		return m_options.mysqlHandleNum;
	}

	int getupdateDeleteNum()
	{
		return m_options.updateDeleteNum;
	}

	std::string getUrlTransformServer()
	{
		urlTransformMutex.acquire();
		int index = currentIndex;
		currentIndex = (currentIndex+1) % m_options.urlTransformServers.size();
		urlTransformMutex.release();
		return m_options.urlTransformServers[index];
	}
protected:
	ConfigManager() : currentIndex(0) {}
	ConfigManager(const ConfigManager &) {}

private:
	std::string m_configFile;
	Options m_options;
	int currentIndex;
	ACE_Thread_Mutex urlTransformMutex;
};
