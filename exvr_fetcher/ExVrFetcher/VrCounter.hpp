#ifndef    VR_COUNTER_H
#define    VR_COUNTER_H
#include <pthread.h>
#include <map>
#include <vector>
#include "database.h"
#include "ExVrFetcher.h"

struct DataSourceCounter
{
	DataSourceCounter():
		last_item_num(0), current_item_num(0), success_count(0), total_count(0) {}
	int last_item_num; //item number of one data source
	int current_item_num;
	int success_count;
	int total_count; //number of url
	std::string ds_name;
};

class VrCounter
{
	public:
		VrCounter();
		VrCounter(int id, int cnt);
		~VrCounter();
		int minus(bool lflag,int write_count,std::string &debugInfo, int ds_id);
		int getCount();
		int setCount(int total_count);
        int getTotalCount();
        std::map<std::string, std::string> m_docid; //key:docid  value:md5
        std::map<int, std::vector<std::string> > m_UrlDocid;  //key:url id  value:docid
	public:
		std::map<int, DataSourceCounter*> ds_counter; //key:数据源id
		int last_item_num; //total item number of all data source
		int valid_num;
		std::string debugInfo;
		std::string fetch_detail;  //记录抓取状态的详细内容
		int error_status;  //抓取状态码，只记录出错时的状态，可能的取值是1、3、4、5，默认为1
	private:
		int count;
		int res_id;
        int total_count;  //总数，即count的初始值
};


#endif
