#include "scribemonitor.h"
ScribeMonitor* ScribeMonitor::_instance = NULL;

std::string ScribeMonitor::category_class = "";
std::string ScribeMonitor::category_port = "";

void ScribeMonitor::CheckState()
{
    std::vector<std::string> list;
    list.push_back(category_);
    std::string logstr = Map2String();
    int pos = logstr.find("\t");
    logstr.insert(pos,":" + category_port);
    
    client_.Send(list,logstr);
}

