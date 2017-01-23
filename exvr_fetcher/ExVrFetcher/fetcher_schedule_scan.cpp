#include <ace/Reactor.h>
#include <Platform/log.h>
#include "ExVrFetcher.h"
#include "fetcher_schedule_scan.hpp"
#include "Refresh_Timer.h"
#include "Configuration.hpp"
#include "Util.hpp"
#include "redis_tool.h"
#include "CDeleteTask.hpp"

std::vector<FetcherScheduleScan *> fetcher_schedule_scans;

int splitStr(const std::string &str, const char separator, std::vector<std::string> &res)
{
    //fprintf(stderr, "enter splitStr\n");
	size_t startPos = 0;

	size_t curPos = 0;

	while(curPos < str.size())
	{
		//fprintf(stderr, "splitStr %s curPos is %d\n",str.c_str(),curPos);
		if(str[curPos] != separator)
		{
			curPos++;
		}
		else
		{
            //fprintf(stderr, "splitStr %s curPos is %d add str %s\n",str.c_str(),curPos,str.substr(startPos, curPos - startPos).c_str());
			res.push_back(str.substr(startPos, curPos - startPos));
			curPos++;
			startPos = curPos;
		}
	}
    res.push_back(str.substr(startPos,str.size() - startPos));
	return 0; 
}  


static time_t time_str2int(const char* str_time)
{
	if(str_time==NULL)
		return 0;
	std::string time_str(str_time);
	fprintf(stderr,"lastFetch:%s\n",time_str.c_str());
	if(time_str.empty())
		return 0;
	fprintf(stderr,"TEST:%s\n",time_str.c_str());
	//Mon Jan 17 15:55:25 2011"
	struct tm tm; 
	if (strptime(time_str.c_str(),"%F %T",&tm)!=NULL)
	{   
		time_t t = mktime(&tm);
		fprintf(stderr,"[timeProcess]%s=%ld\n",time_str.c_str(),t);
		return t;
	}else
		return 0;
}

int FetcherScheduleScan::init(const char* configfile, std::string type, std::string spidercheck_interval, std::string spiderscan_interval)
{
	ConfigurationFile config(configfile);
	std::string m_config;
	m_config.assign(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) ){
		SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	std::string vr_ip = section.Value<std::string>("VrResourceIp");
	std::string vr_user = section.Value<std::string>("VrResourceUser");
	std::string vr_pass = section.Value<std::string>("VrResourcePass");
	std::string vr_db = section.Value<std::string>("VrResourceDb");
	
	int spider_type = atoi(type.c_str());

	std::string cr_ip = section.Value<std::string>("CrResourceIp");
	std::string cr_user = section.Value<std::string>("CrResourceUser");
	std::string cr_pass = section.Value<std::string>("CrResourcePass");
	std::string cr_db = section.Value<std::string>("CrResourceDb");

	SS_DEBUG((LM_INFO, "load vr_resource configiguration: %s %s %s %s\n",
				vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));   

	SS_DEBUG((LM_INFO, "load vr_resource configiguration: %s %s %s %s\n",
				cr_ip.c_str(), cr_user.c_str(), cr_pass.c_str(), cr_db.c_str() )); 

    std::string redis_address = section.Value<std::string>("RedisAddress");
    int redis_port = section.Value<int>("RedisPort");
	std::string redis_password = section.Value<std::string>("RedisPassword");

    int check_interval = atoi(spidercheck_interval.c_str());

	std::string SqlPrefix = section.Value<std::string>("SqlPrefix");
	std::string DeleteSqlPrefix = section.Value<std::string>("DeleteSqlPrefix");
	std::string sql = section.Value<std::string>(std::string(SqlPrefix + "_" + type).c_str());
	std::string manual_sql = sql + " and manual_update = 1";
	std::string auto_sql = sql + " and (unix_timestamp(now()) - unix_timestamp(fetch_time) > frequency) and manual_update != 1";
	
	std::string delete_sql = section.Value<std::string>(std::string(DeleteSqlPrefix + "_" + type).c_str());
	if (spider_type == 0)
	{
		auto_sql = "select * from customer_resource where auto_update = 1 and (unix_timestamp(now())- unix_timestamp(fetch_time) >= frequency) and (unix_timestamp(now()) >= unix_timestamp(start_time)) and manual_update = 0 and type = 1";
	}
	if (sql == "" || (delete_sql == "" && spider_type != 0))
	{
		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc manual_sql or auto_sql or delete_sql is NULL spider_type(%d) error! \n", spider_type));
		return -1;
	}

	int scan_interval = atoi(spiderscan_interval.c_str());

	/*
	// spider_type 1：normal，2：inst，3：多命中
    std::string manual_sql = "select * from vr_resource as t1 where cast(t1.vr_flag as unsigned)>0";
    std::string auto_sql = "select * from  vr_resource as t1 where (unix_timestamp(now()) - unix_timestamp(t1.fetch_time) > t1.frequency) and cast(t1.vr_flag as unsigned)>0";
    std::string temp;
    std::string delete_sql = "select * from vr_resource as t1 where cast(t1.vr_flag as unsigned)=0 and t1.auto_del=1 and ";
    int scan_interval = 1;
	std::string spider_name;
    if(spider_type == 0)
    {
    	manual_sql = "select * from customer_resource where auto_update = 1 and (unix_timestamp(now())- unix_timestamp(fetch_time) >= frequency) and (unix_timestamp(now()) >= unix_timestamp(start_time)) and manual_update = 0 and type = 1";		
    	auto_sql = "select * from customer_resource where manual_update = 1 and type = 1";
    	scan_interval = section.Value<int>("Customer_ScanInterval");
    	spider_name = "Customer_Spider";
    }
    //1:normal
    else if(spider_type == 1) {
        temp.assign(" and ((t1.vr_id >= 10000000 and t1.vr_id < 20000000 and t1.crawlInter = 1) or ((t1.vr_id >= 20000000 and t1.vr_id < 30000000) or (t1.vr_id >= 40000000))) and t1.priority=0 and t1.vr_id<70000000 and t1.is_quick=0");
    	scan_interval = section.Value<int>("Normal_ScanInterval");
    	spider_name = "Normal_Spider";
    	delete_sql += " t1.priority=0 and t1.vr_id<70000000 and t1.is_quick=0";
    //2: inst
    } else if(spider_type == 2) {
        temp.assign(" and (((t1.vr_id >= 10000000 and t1.vr_id < 20000000 and t1.crawlInter = 1)) or (t1.vr_id >= 20000000 and t1.vr_id < 30000000) or (t1.vr_id >= 40000000)) and t1.priority=1 and t1.is_quick=0");
    	scan_interval = section.Value<int>("Inst_ScanInterval");
    	spider_name = "Inst_Spider";
    	delete_sql += "t1.priority=1 and t1.is_quick=0";
    //3: 多命中
    } else if(spider_type == 3) {
        temp.assign(" and t1.priority=0 and t1.vr_id>=70000000 and t1.vr_id<80000000 and t1.is_quick=0");
        scan_interval = section.Value<int>("MH_ScanInterval");
        spider_name = "MH_Spider";
        delete_sql += "t1.priority=0 and t1.vr_id>=70000000 and t1.vr_id<80000000 and t1.is_quick=0";
    //4: 大vr
    } else if(spider_type == 4) {
        temp.assign(" and t1.priority=2 and t1.is_quick=0");
        scan_interval = section.Value<int>("Large_ScanInterval");
        spider_name = "Large_Spider";
        delete_sql += "t1.priority=2 and t1.is_quick=0";
    //5: 快速抓取
    } else if(spider_type == 5)
    {
        temp.assign(" and t1.is_quick=1");
        scan_interval = section.Value<int>("Quick_ScanInterval");
        spider_name = "Quick_Spider";
        delete_sql += "t1.is_quick=1";
    // 其他情况
    } else {
        SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc spider_type(%d) error! \n", spider_type));
        ACE_Reactor::instance()->end_reactor_event_loop();
        return -1;
    }

    if (spider_type != 0)
    {
    	manual_sql.append(temp);
	    auto_sql.append(temp);
	    manual_sql += " and t1.manual_update = 1";
	    auto_sql += " and t1.manual_update != 1";
    }
	*/
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc: spider_type : %d, scan sql is %s\n", spider_type, auto_sql.c_str()));
    SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc: spider_type : %d, manual sql is %s\n", spider_type, manual_sql.c_str()));

    FetcherScheduleScan *fes = new FetcherScheduleScan(vr_ip, vr_user, vr_pass, vr_db, cr_ip, cr_user, cr_pass, cr_db, m_config,
      scan_interval, spider_type, "", manual_sql, auto_sql, delete_sql, redis_address, redis_port, redis_password, check_interval);
	fetcher_schedule_scans.push_back(fes);

	return 0; 
}

int FetcherScheduleScan::open (void*) {
	return activate(THR_NEW_LWP, 1); //can use only one thread 
}

//得到vr_resource中需要抓取的vr资源，每个资源可能对应多个url
int FetcherScheduleScan::get_resource( CMysql *mysql, bool is_delete, const std::string &sql, RedisTool *redis_tool, bool is_manual)
{

	struct tm tm;
	CMysqlQuery query(mysql);
	CMysqlQuery c_query(mysql);
	CMysqlQuery format_query(mysql);
	CMysqlQuery data_source_query(mysql);
	CMysqlQuery k_query(mysql);
	

	int ret = -1;
	alarm(30);
	ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) 
	{
		if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"FetcherScheduleScan::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}
	int record_count = query.rows();
	
	SS_DEBUG((LM_TRACE,"FetcherScheduleScan::get_resource(%@): [exstat] sql[%s]. found record_count=%d\n",
				this,sql.c_str(), record_count ));
	int doc_num = 0;

	//遍历当前抓取轮中需要进行抓取的vr类别(res_id) 
	for(int irow = 0; irow < record_count; irow++)
	{
		//当前vr类别的所有url的fetchtime统一为vr类别的抓取时间
		time_t now = time(NULL);//get current time str can make a func
		localtime_r(&now, &tm);
		char time_str[64];
        strftime(time_str, sizeof(time_str), "%F %T", &tm);

		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow)
		{
			SS_DEBUG((LM_ERROR,"FetcherScheduleScan::get_resource(%@): [exstat] get vr row error:%d %s [%s]\n",
						this, irow, mysql->error(), mysql->errorno() ));
			break;
		}
        
		//field check push back everytime can make a static variable
		//put into a func
        std::vector<std::string> field_vec;
		field_vec.push_back("vr_type");
		field_vec.push_back("id");
		field_vec.push_back("data_group");
		field_vec.push_back("vr_id");
		field_vec.push_back("vr_attr1");
		field_vec.push_back("vr_attr2");
		field_vec.push_back("vr_attr3");
		field_vec.push_back("vr_attr5");
		field_vec.push_back("vr_rank");
		field_vec.push_back("template");
		field_vec.push_back("data_from");
		std::vector<std::string>::iterator itr;
		bool has_err = false;
		for(itr = field_vec.begin(); itr != field_vec.end(); ++itr)
		{
			if(!mysqlrow->fieldValue(*itr) || strlen(mysqlrow->fieldValue(*itr))<1)
			{
				SS_DEBUG(( LM_CRITICAL, "FetcherScheduleScan::get_resource(%@): field error: %s!\n", this, itr->c_str() ));
				has_err = true;
				break;
			}
		}
		if(has_err)
			continue;
 
		std::string res_id_str;
		res_id_str.assign(mysqlrow->fieldValue("id"));
		int res_id = atoi(res_id_str.c_str());
		int frequency = atoi(mysqlrow->fieldValue("frequency"));
		int data_group        = atoi(mysqlrow->fieldValue("data_group"));  
		long lfetchTime        = time_str2int(mysqlrow->fieldValue("fetch_time"));//上一次抓取时间              
		int data_from   = atoi(mysqlrow->fieldValue("data_from"));
		std::string vr_id_str, group_str;
		vr_id_str.assign(mysqlrow->fieldValue("vr_id"));
		group_str.assign(mysqlrow->fieldValue("data_group"));
        std::string item_str = res_id_str + "#" + vr_id_str;

        // update fetch_time and manual_update
        //char buf[1024];
		//sprintf(buf, "update vr_resource set fetch_time = '%s', manual_update = 0 where id = %d",
		//		time_str,res_id);
		//CMysqlExecute exe(mysql);
		//int retmp;
		//while((retmp=exe.execute(buf)) < 1)
		//{
		//	if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
		//		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::get resource(%@)fail to "
		//					"Update vr_resource %d update_date error code: %d!\n", this, res_id, retmp));
		//		break;
		//	}
		//	if( 0 == retmp )
		//		break;
		//}

		//SS_DEBUG((LM_ERROR, "FetcherScheduleScan::get resource(%@) Update vr_resource id:%d  vr_id:%s sql:%s!\n", this, res_id, vr_id_str.c_str(), buf));
        

        // put into redis
        if (is_manual)
        {
            item_str += "#1#" + group_str;
        }
        else
        {
            item_str += "#0#" + group_str;
        }
        
        int redis_ret = -1;
        while (redis_ret <= 0)
        {
            if (!redis_tool->GetConnected())
            {
                redis_tool->Connect();
            }
			//if (is_delete)
			//	redis_ret = redis_tool->ListAdd(item_str);
			//else
			if (is_delete)
	        {
	        	redis_ret = redis_tool->AddDel(item_str);
	        }
	        else
	        {
	        	redis_ret = redis_tool->Add(item_str);
	        }
            if (redis_ret < 0)
            {
                SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_resource(%@): put to redis error, spider_type : %d, id : %s, vr_id %s\n",this, spider_type, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_id") ));
                redis_tool->SetConnected(false);
                redis_tool->Close();
            }
            else if (redis_ret == 1)
            {
                SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_resource(%@): put to redis %s, spider_type : %d, id : %s, vr_id %s\n",this, item_str.c_str() , 
                	spider_type, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_id") ));
            }
			else if (redis_ret == 2)
			{
				 SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_resource(%@): already in redis set  %s, spider_type : %d, id : %s, vr_id %s\n",this, item_str.c_str() , 
                	spider_type, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_id") ));
			}
			else
			{
				SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_resource(%@): put to redis not success %s, spider_type : %d, id : %s, vr_id %s\n",this, item_str.c_str() , 
                	spider_type, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_id") ));
			}
        } 
        
		//SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_resource(%@): spider_type : %d, id : %s, vr_id %s\n",this, spider_type, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_id") ));
	}
	
	return 0;
}


int FetcherScheduleScan::get_customer_resource( CMysql *mysql, std::string sql, RedisTool *redis_tool, bool is_manual)
{
	CMysqlQuery query(mysql);
	int ret = -1;
	alarm(30);
	ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"FetcherScheduleScan::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}

	int record_count = query.rows();
	SS_DEBUG((LM_TRACE,"FetcherScheduleScan::get_customer_resource(%@): [exstat] found record_count=%d\n",this, record_count ));
	int doc_num = 0;

	for(int irow = 0; irow < record_count; irow++)
	{
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"FetcherScheduleScan::get_customer_resource(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		//field check
		if(!mysqlrow->fieldValue("url") || strlen(mysqlrow->fieldValue("url"))<1)
		{
			SS_DEBUG(( LM_CRITICAL, "FetcherScheduleScan::get_customer_resource: field error: url!\n" ));
			continue;
		}

        std::string res_id_str;
		res_id_str.assign(mysqlrow->fieldValue("id"));
        std::string vr_id_str;
		vr_id_str.assign(mysqlrow->fieldValue("vr_id"));

		// update customer_resource
		char buf[1024];
		sprintf(buf, "update customer_resource set fetch_time = NOW(), manual_update = 0 where id = %s", res_id_str.c_str());
		CMysqlExecute exe(mysql);
		int retmp;
		while((retmp=exe.execute(buf)) < 1)
		{
			if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
				SS_DEBUG((LM_ERROR, "FetcherScheduleScan::get_customer_resource(%@)fail to "
							"Update customer_resource %s update_date error code: %d!\n", this, res_id_str.c_str(), retmp));
				break;
			}
			if( 0 == retmp )
				break;
		}

		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::(%@) Update customer_resource id:%s  vr_id:%s sql:%s!\n", this, res_id_str.c_str(), vr_id_str.c_str(), buf));

        std::string item_str = res_id_str + "#" + vr_id_str;
        if (is_manual)
        {
            item_str += "#1";
        }
        else
        {
            item_str += "#0";
        }
        // put into redis
        int redis_ret = -1;
        //redis_tool->SetListSetName(ACTION_FETCH);
        while (redis_ret <= 0)
        {
            if (!redis_tool->GetConnected())
            {
                redis_tool->Connect();
            }
            //if (is_delete)
			//	redis_ret = redis_tool->ListAdd(item_str);
			//else
            redis_ret = redis_tool->Add(item_str);
            if (redis_ret < 0)
            {
                SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_customer_resource(%@): put customer_resource to redis error, spider_type : %d, id : %s\n",this, spider_type, mysqlrow->fieldValue("id") ));
                redis_tool->SetConnected(false);
                redis_tool->Close();
            }
            else if (redis_ret == 1)
            {
                SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_customer_resource(%@): put customer_resource to redis, spider_type : %d, id : %s\n",this, spider_type, mysqlrow->fieldValue("id") ));
            }
			else if (redis_ret == 2)
			{
				SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_customer_resource(%@): customer_resource already in redis set, spider_type : %d, id : %s\n",this, spider_type, mysqlrow->fieldValue("id") ));
			}
			else
			{
				SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_customer_resource(%@): put customer_resource to redis not success, spider_type : %d, id : %s\n",this, spider_type, mysqlrow->fieldValue("id") ));
			}
        } 
		doc_num++;
		//SS_DEBUG((LM_TRACE, "FetcherScheduleScan::get_customer_resource(%@): memdebug: %d, put %s\n",this, _mem_check, req->url.c_str()));
	}
	SS_DEBUG((LM_TRACE,"FetcherScheduleScan::get_customer_resource(%@): [exstat] to fetch doc_num=%d\n",this, doc_num ));
	return 0;
}


int FetcherScheduleScan::AddSomeUniqueName(RedisTool *redis_tool, int start, int num)
{
	
	std::string base_set_name = "UniqueName_";
	for (int i = start; i < start + num; i++)
	{
		std::stringstream ss;
    	ss << spider_type << "_" << i;
		std::string value = base_set_name + ss.str();
		int redis_ret = -1;
		while (redis_ret < 0)
	    {
	        if (!redis_tool->GetConnected())
	        {
	            redis_tool->Connect();
	        }
	        redis_ret = redis_tool->AddUniqueName(value);
	        if (redis_ret < 0)
	        {
	            SS_DEBUG((LM_TRACE, "FetcherScheduleScan::AddSomeUniqueName(%@): add error,  value : %s %d\n",this, value.c_str(), redis_ret ));
	            redis_tool->SetConnected(false);
	            redis_tool->Close();
	        }
	        else
	        {
	            SS_DEBUG((LM_TRACE, "FetcherScheduleScan::AddSomeUniqueName(%@): add success value : %s %d\n", this,value.c_str(), redis_ret ));
	        }
	    } 
	}
	return 0;
}

int FetcherScheduleScan::CheckSpider(RedisTool *redis_tool)
{
	std::map<std::string, std::string> spider_members;
	int redis_ret = -1;
	while (redis_ret < 0)
    {
        if (!redis_tool->GetConnected())
        {
            redis_tool->Connect();
        }
        redis_ret = redis_tool->GetAllHashMembers(spider_members);
        if (redis_ret < 0)
        {
            SS_DEBUG((LM_TRACE, "FetcherScheduleScan::CheckSpider_%d(%@): redis error %d\n",spider_type, this, redis_ret ));
            redis_tool->SetConnected(false);
            redis_tool->Close();
        }
        else
        {
            SS_DEBUG((LM_TRACE, "FetcherScheduleScan::CheckSpider_%d(%@): check %d %d\n", spider_type, this, redis_ret, check_interval));
        }
    } 

	if (redis_ret > 0)
	{
		std::map<std::string, std::string>::iterator it;
		for (it = spider_members.begin(); it != spider_members.end(); it++)
		{
			std::string set_name = it->first;
			std::string set_value = it->second;
			std::vector<std::string> time_vec;
			splitStr(set_value, '#', time_vec);
			if (time_vec.size() < 1)
				continue;
			long set_seconds = atol(time_vec[0].c_str());
			time_t now_seconds;  
			now_seconds = time((time_t *)NULL); 
			SS_DEBUG((LM_TRACE, "FetcherScheduleScan::CheckSpider_%d(%@): check %s %s %lld\n", spider_type, this, set_name.c_str(),	set_value .c_str(), now_seconds));

			if (now_seconds - set_seconds >= check_interval)
			{
				
				std::vector<std::string> vec;
				//redis_tool->SetListSetName(ACTION_FETCH);
				int set_num = redis_tool->GetSetMembers(set_name, vec);
				
				if (set_num > 0)
				{
					for (int i = 0; i < vec.size(); i++)
					{
						int ret = -1;
						while(ret != 1)
						{
							ret = redis_tool->ListAdd(vec[i]);
							if (ret != 1)
								SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: repush unique_set item to redis list fail %s\n", spider_type, this, vec[i].c_str()));
							else
								SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: repush unique_set item to redis list success%s\n", spider_type, this, vec[i].c_str()));
						}

						ret = -1;
						while(ret != 1)
						{
							ret = redis_tool->DeleteSet(set_name, vec[i]);
							if (ret != 1)
								SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: remove unique_set item fail %s\n", spider_type, this, vec[i].c_str()));
							else
								SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: remove unique_set item success%s\n", spider_type, this, vec[i].c_str()));
						}
					}

					int ret = redis_tool->DeleteHash(set_name);
					SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: DeleteHash %s %d\n", spider_type, this, set_name.c_str(), ret));
					//ret = redis_tool->AddUniqueName(set_name);
					//SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider(%@):redis: AddUniqueName %s %d\n", this, set_name.c_str(), ret));
					ret = redis_tool->DeleteKey(set_name);
					SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: DeleteKey %s %d\n", spider_type, this, set_name.c_str(), ret));
					
				}
				else
				{
					int ret = redis_tool->DeleteHash(set_name);
					SS_DEBUG((LM_ERROR, "FetcherScheduleScan::CheckSpider_%d(%@):redis: DeleteHash %s %d\n", spider_type, this, set_name.c_str(), ret));

				}
	
			}
		}
	}
	
	return redis_ret;
}

int FetcherScheduleScan::svc(){ 
    RedisTool *redis_tool = new RedisTool(redis_address, redis_port, spider_type, redis_password);
	CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	CMysql crsql(cr_ip, cr_user, cr_pass, cr_db, "set names gbk");
	int reconn = 0;
    while (reconn < 5)
    {
       if (!redis_tool->Connect())
       {
           break;
       }
       else
       {
           reconn++;
       }
    }
    if (reconn >= 5)
    {
		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):redis: connect vr fail\n", this));
        return -1;
    }
	// add 100 UniqueName for every spider type
	//if (redis_tool->UniqueSetLen() == 0)
	//{
	//	int num = redis_tool->HashLen();
	//	AddSomeUniqueName(redis_tool, num, 100);
	//}
	
    reconn = 0;

	while( reconn < 5 )
	{//重连mysql，最多5次
		bool suc = true;
		if( mysql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql.connect() ) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( !mysql.open() ) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql.close();
		mysql.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: connect vr fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	/**************数据库连接  **********/
	reconn = 0;
	while( reconn < 5 ){
		bool suc = true;
		if( crsql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !crsql.connect()) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: connect server fail: %s\n",this, crsql.error()));
			suc = false;
		}
		if( !crsql.open() ) {
			SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: open database fail: %s\n",this, crsql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&crsql);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		crsql.close();
		crsql.init();
	}
	if( reconn >=5 ) {
		SS_DEBUG((LM_ERROR, "FetcherScheduleScan::svc(%@):Mysql: connect cr mysql server fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): print_sql_%d manual_sql %s\n",this, spider_type, manual_sql.c_str()));
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): print_sql_%d auto_sql %s\n",this, spider_type, auto_sql.c_str()));
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): print_sql_%d delete_sql %s\n",this, spider_type, delete_sql.c_str()));
	thr_id_scan = thr_mgr()->thr_self();
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): begin to loop %d\n",this, thr_id_scan));
	Refresh_Timer *timer = new Refresh_Timer;

	while(!ACE_Reactor::instance()->reactor_event_loop_done())
	{
		//Refresh_Timer::instance()->Update(m_config.c_str()); //look at clock, remember when we begin to refresh
		timer->Update(scan_interval);
		time_t rest = timer->getRestTime();
		if(rest > 0)  //time up or not?
		{//rest为还剩的秒数
			SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): [exstat] time to rest: %d\n", this, rest));
			while(rest-- > 0 && !ACE_Reactor::instance()->reactor_event_loop_done())
				sleep(1);//sleep一秒,是一分钟扫描一次库
			//sleep rest时间,去抓取扫描数据库 
			if(ACE_Reactor::instance()->reactor_event_loop_done())
				break;

			timer->Update(scan_interval); //remember when we begin to refresh
		}

		SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): scan resource and data %d\n",this, spider_type));

        if (spider_type != 0)
        {

		    get_resource( &mysql, false, manual_sql, redis_tool, true);//得到手的设置抓取的
		    get_resource( &mysql, false, auto_sql, redis_tool, false);//得带自动需要抓取的
		    get_resource(&mysql, true, delete_sql, redis_tool, false);
        }
        else
        {
            
		    get_customer_resource(&crsql, manual_sql, redis_tool, true);
		    get_customer_resource(&crsql, auto_sql, redis_tool);
		    CDeleteTask::instance()->delete_customer_data(&crsql);
        }
        CheckSpider(redis_tool);
		SS_DEBUG((LM_TRACE, "FetcherScheduleScan::svc(%@): spider_type : %d,  scan complete\n",this, spider_type));
	}
	delete timer;
    delete redis_tool;
	SS_DEBUG((LM_TRACE, "FetcherScheduleScan::sl_update = 1 manual_update = 1 svc(%@): exit from loop\n",this));
	return 0;
}


int FetcherScheduleScan::stopTask() {
	flush();
	thr_mgr()->kill_grp(grp_id(), SIGINT);
	wait();
	return 0;
}

