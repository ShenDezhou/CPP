#include <VrCounter.hpp>


VrCounter::VrCounter()
    : count(0),res_id(0),total_count(0)
{
}

VrCounter::VrCounter(int id,int cnt)
    : error_status(1),count(cnt),res_id(id),total_count(cnt+1)
{
}
VrCounter::~VrCounter()
{
	std::map<int, DataSourceCounter*>::iterator itr;
	for(itr=ds_counter.begin(); itr!=ds_counter.end(); ++itr)
	{
		delete itr->second;
	}
}

int VrCounter::minus(bool lflag,int write_count,std::string &_debugInfo, int ds_id)
{
	count--;
	
	debugInfo.append(_debugInfo);
	ds_counter[ds_id]->current_item_num += write_count;
	if(lflag)
		ds_counter[ds_id]->success_count += 1;

    return 0;
}

int VrCounter::getCount()
{
	return count;
}

int VrCounter::setCount(int _count)
{
	count = _count;
    total_count = _count + 1;
	return 0;
}

int VrCounter::getTotalCount()
{
    return total_count;
}
