#ifndef _SOGOU_SCRIBEMONITOR_H_
#define _SOGOU_SCRIBEMONITOR_H_

#include <utils/monitor/monitor.h>

class ScribeMonitor: public TaskMonitor
{
public:
	static ScribeMonitor* instance(std::string category, std::string ip, unsigned int port)
	{
		if (NULL == _instance)
		{
			int pos = category.find(":");
			if (pos != std::string::npos)
			{
				category_class = category.substr(0,pos);
				category_port = category.substr(pos+1);
			}
			else
			{
				category_class = category;
				category_port = "0";
			}
			_instance = new ScribeMonitor(category_class, ip, port);
		}
		return _instance;
	}
	static ScribeMonitor* instance()
	{
		return _instance;
	}

	virtual void CheckState();

	virtual ~ScribeMonitor()
	{
		delete _instance;
	}
	

private:
	ScribeMonitor(std::string category, std::string ip, unsigned int port)
		:TaskMonitor(category, ip, port)
	{
		SetInterval(60);
		Start();
		AddType("fetchAvgCostTime",TaskMonitor::AVG);
		AddType("fetchSucc",TaskMonitor::SUM);
		AddType("fetchFail",TaskMonitor::SUM);
		AddType("docsOnline",TaskMonitor::SUM);
		AddType("docsOffline",TaskMonitor::SUM);
		AddType("valLengthFail",TaskMonitor::SUM);
		AddType("valSchemaFail",TaskMonitor::SUM);
		AddType("urlException",TaskMonitor::SUM);
	}
	
	static ScribeMonitor* _instance;	
	static std::string category_class;
	static std::string category_port;

};




#endif
