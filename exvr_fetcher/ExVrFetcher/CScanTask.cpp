#include <ace/Reactor.h>
#include <Platform/log.h>
#include <openssl/md5.h>
#include "ExVrFetcher.h"
#include "CScanTask.hpp"
#include "CFetchTask.hpp"
#include "CUrlUpdateTask.hpp"
#include "Refresh_Timer.h"
#include "Configuration.hpp"
#include "ConfigManager.h"
#include "CWriteTask.hpp"
#include "COnlineTask.hpp"
#include "Util.hpp"
#include "redis_tool.h"
#include "CDeleteTask.hpp"

extern std::string unique_set_name;
extern pthread_mutex_t unique_set_name_mutex;

extern char *hexstr(unsigned char *buf,char *des,int len);

using namespace std;

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

extern int splitStr(const string &str, const char separator, vector<string> &res);

int CScanTask::save_timestamp(CMysql *mysql, request_t *req)
{
	char buf[1024];
	int flag = 0;
	snprintf(buf, 1024, "insert into process_timestamp (res_id, scan_time) values (%d, %ld)",
			req->res_id, time(NULL));
	CMysqlExecute exe(mysql);
	int ret = exe.execute2(buf);//execute2() 会返回插入时产生的id
	if (ret < 1) {
		flag = -1;
		SS_DEBUG((LM_ERROR, "CScanTask::save_timestamp execute failed sql[%s]\n", buf));
		req->timestamp_id = -1;
	} else {
		req->timestamp_id = ret;
	}

	return flag;
}

int CScanTask::open (const char* configfile)
{
	ConfigurationFile config(configfile);
	m_config.assign(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) ){
		SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	vr_ip = section.Value<std::string>("VrResourceIp");
	vr_user = section.Value<std::string>("VrResourceUser");
	vr_pass = section.Value<std::string>("VrResourcePass");
	vr_db = section.Value<std::string>("VrResourceDb");
	scan_interval = section.Value<int>("ScanInterval");
	spider_type = section.Value<std::string>("SpiderType");
	SS_DEBUG((LM_INFO, "load  vr_resource configiguration: %s %s %s %s\n",
				vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));

	cr_ip = section.Value<std::string>("CrResourceIp");
	cr_user = section.Value<std::string>("CrResourceUser");
	cr_pass = section.Value<std::string>("CrResourcePass");
	cr_db = section.Value<std::string>("CrResourceDb");

	SS_DEBUG((LM_INFO, "load vr_resource configiguration: %s %s %s %s\n",
				cr_ip.c_str(), cr_user.c_str(), cr_pass.c_str(), cr_db.c_str() ));    
	redis_address = section.Value<std::string>("RedisAddress");
	redis_port = section.Value<int>("RedisPort");
	std::string redis_password = section.Value<std::string>("RedisPassword");
	//unique_set_name = section.Value<std::string>("UniqueSetName");
	int type = atoi(spider_type.c_str());
	redis_tool = new RedisTool(redis_address, redis_port, type, redis_password);
	
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
		SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):redis: connect redis fail\n", this));
        return -1;
    }

	// get unique_set_name from redis 
	int ret = -1;
	while(ret != 1)
	{
		ret = redis_tool->GetIncrUniqueName(unique_set_name);
		if (ret != 1)
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):redis: get random unique_set_name fail %s\n", this, unique_set_name.c_str()));
		else
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):redis: get random unique_set_name success%s\n", this, unique_set_name.c_str()));
	}
	// set rand seed
	srand((unsigned)time(NULL));
	return open();  
}

int CScanTask::open (void*) {
	return activate(THR_NEW_LWP, 1); //can use only one thread 
}


bool msgGreator(sched_unit* reqA,sched_unit*  reqB)
{
	return reqA->litemNum < reqB->litemNum;
}

bool msgGreatorB(sched_unit* reqA,sched_unit* reqB)
{
	time_t tm=time(NULL);

	int frequencyA=reqA->frequency;
	int frequencyB=reqB->frequency;

	int mul=reqA->litemNum/MAX_FETCH_PER_MiNU;		
	//调整reqA和reqB的frequency值
	if(mul<=0)	
		frequencyA=reqA->frequency;
	else if( mul>0 && mul<=5 )
		frequencyA=frequencyA > LEVEL1_FETCH_FREQUENCY ?frequencyA:LEVEL1_FETCH_FREQUENCY;
	else if(mul>5 &&mul<=10)
		frequencyA=frequencyA > LEVEL2_FETCH_FREQUENCY ?frequencyA:LEVEL2_FETCH_FREQUENCY;
	else if(mul>10&&mul<=50)
		frequencyA=frequencyA > LEVEL3_FETCH_FREQUENCY ?frequencyA:LEVEL3_FETCH_FREQUENCY;
	else if(mul>50&&mul<=100)
		frequencyA=frequencyA > LEVEL4_FETCH_FREQUENCY ?frequencyA:LEVEL4_FETCH_FREQUENCY;
	else if(mul>100)
		frequencyA=frequencyA > LEVEL5_FETCH_FREQUENCY ?frequencyA:LEVEL5_FETCH_FREQUENCY;

	mul=reqB->litemNum/MAX_FETCH_PER_MiNU;
	if(mul <=0)
		frequencyB=reqB->frequency;
	else if( mul>0 && mul<=5 )
		frequencyB=frequencyB > LEVEL1_FETCH_FREQUENCY ?frequencyB:LEVEL1_FETCH_FREQUENCY;
	else if(mul>5 &&mul<=10)
		frequencyB=frequencyB > LEVEL2_FETCH_FREQUENCY ?frequencyB:LEVEL2_FETCH_FREQUENCY;
	else if(mul>10&&mul<=50)
		frequencyB=frequencyB > LEVEL3_FETCH_FREQUENCY ?frequencyB:LEVEL3_FETCH_FREQUENCY;
	else if(mul>50&&mul<=100)      
		frequencyB=frequencyB > LEVEL4_FETCH_FREQUENCY ?frequencyB:LEVEL4_FETCH_FREQUENCY;
	else if(mul>100)
		frequencyB=frequencyB > LEVEL5_FETCH_FREQUENCY ?frequencyB:LEVEL5_FETCH_FREQUENCY;


	int critiA=tm-reqA->lastfetch_time-frequencyA;
	int critiB=tm-reqB->lastfetch_time-frequencyB;

	reqA->frequency=frequencyA;
	reqB->frequency=frequencyB;

	return critiA > critiB ;
}

int CScanTask::get_delete(CMysql *mysql, std::string &value)
{
	std::vector<std::string> vec;
	splitStr(value, '#', vec);

	if (vec.size() < 4 )
		return -1;

	CMysqlQuery query(mysql);
	struct tm tm;
	std::string doc_sql = "";
	char time_str[64];
	
	int ret = -1;
	int record_count = 0;
	int vr_id = atoi(vec[1].c_str());
	int res_id = atoi(vec[0].c_str());
	int data_group = atoi(vec[3].c_str());
	
	doc_sql = "select doc_id from vr_data_";
	doc_sql.append(vec[3]);
	doc_sql.append(" where res_id=");
	doc_sql.append(vec[0]);

	SS_DEBUG((LM_ERROR,"CScanTask::get_delete_docid(%@):Mysql[%s]\n",this, doc_sql.c_str()));
	alarm(30);
	ret = query.execute(doc_sql.c_str());
	alarm(0);
	while( ret == -1 ) 
	{
		if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CScanTask::get_delete_docid(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,doc_sql.c_str(),
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(doc_sql.c_str());
		alarm(0);
	}
	record_count = query.rows();
	if(0 == record_count)
	{
		return 0;
	}

	VrdeleteData *deleteData = new VrdeleteData();
	time_t now = time(NULL);//get current time str can make a func
	localtime_r(&now, &tm);
    strftime(time_str, sizeof(time_str), "%c", &tm);
	deleteData->fetch_time = time_str;

	for(int irow=0; irow<record_count; irow++)
	{
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow)
		{
			SS_DEBUG((LM_ERROR,"CScanTask::get_delete(%@): [exstat] get vr row error:%d %s [%s]\n",
						this, irow, mysql->error(), mysql->errorno() ));
			break;
		}
		std::string doc_id = mysqlrow->fieldValue("doc_id");
		deleteData->v_docid.push_back(doc_id);
	}

	deleteData->class_id = vr_id;
	deleteData->res_id = res_id;
	deleteData->data_group = data_group;
	deleteData->status = "DELETE";
	deleteData->timestamp_id = -1;

	ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(deleteData) );
	CDeleteTask::instance()->put(msg);

	return 0;
}


//得到vr_resource中需要抓取的vr资源，每个资源可能对应多个url
int CScanTask::get_resource( CMysql *mysql, CMysql *crsql, const std::string &sql,bool is_manual, RedisTool *redis_tool)
{

	struct tm tm;
	std::vector<sched_unit*>req_vector;
	req_vector.clear();

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
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}
	int record_count = query.rows();
	
SS_DEBUG((LM_TRACE,"CScanTask::get_resource(%@): [exstat] sql[%s]. found record_count=%d\n",
				this,sql.c_str(), record_count ));
	int doc_num = 0;

	//遍历当前抓取轮中需要进行抓取的vr类别(res_id) 
	for(int irow=0; irow<record_count; irow++)
	{
		//当前vr类别的所有url的fetchtime统一为vr类别的抓取时间
		time_t now = time(NULL);//get current time str can make a func
		localtime_r(&now, &tm);
		char time_str[64];
        strftime(time_str, sizeof(time_str), "%F %T", &tm);

		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow)
		{
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@): [exstat] get vr row error:%d %s [%s]\n",
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
		for(itr=field_vec.begin(); itr!=field_vec.end(); ++itr)
		{
			if(!mysqlrow->fieldValue(*itr) || strlen(mysqlrow->fieldValue(*itr))<1)
			{
				SS_DEBUG(( LM_CRITICAL, "CScanTask::get_resource(%@): field error: %s!\n", this, itr->c_str() ));
				has_err = true;
				break;
			}
		}
		if(has_err)
			continue;
    
        //vr resource real processor begin
		//init vr counter
        //following three statemen can be combined into one
		std::string res_id_str;
        //atoi part can be written as inline func
		res_id_str.assign(mysqlrow->fieldValue("id"));
		int res_id = atoi(res_id_str.c_str());
		int vr_id = atoi(mysqlrow->fieldValue("vr_id"));
		int frequency = atoi(mysqlrow->fieldValue("frequency"));
		int data_group        = atoi(mysqlrow->fieldValue("data_group"));  
		long lfetchTime        = time_str2int(mysqlrow->fieldValue("fetch_time"));//上一次抓取时间              
		int data_from   = atoi(mysqlrow->fieldValue("data_from"));
		//int res_status   = atoi(mysqlrow->fieldValue(CR_RES_STATUS));
		//std::string index_field;
		//index_field.assign(mysqlrow->fieldValue("index_fields"));                    // vr_res
		//int isNewFrame = atoi(mysqlrow->fieldValue("new_framework"));    

		//产生一个resource的调度单元
		sched_unit *ptr_unit=NULL;

        //class inheritance to solve if
		if(is_manual)
		{//如果是人工更新，那么直接设置更新时间
			char buf[1024];
			sprintf(buf, "update vr_resource set fetch_time = '%s' where id = %d",
					time_str,res_id);
			CMysqlExecute exe(mysql);
			int retmp;
			while((retmp=exe.execute(buf)) < 1)
			{
				if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
					SS_DEBUG((LM_ERROR, "CScanTask::get resource(%@)fail to "
								"Update vr_resource %d update_date error code: %d!\n", this, res_id, retmp));
					break;
				}
				if( 0 == retmp )
					break;
			}
			SS_DEBUG((LM_INFO,"ScanTask is_manual id = %d and data_from = %d\n",res_id , data_from));
			if(data_from == 4)
			{
				sprintf(buf, "update customer_resource set fetch_time = '%s' "
						"where online_id = %d",time_str, res_id);
				CMysqlExecute cr_exe(crsql);
				while((retmp=cr_exe.execute(buf)) < 1)
				{
					if(crsql->errorno() != CR_SERVER_GONE_ERROR) {
						SS_DEBUG((LM_ERROR, "CScanTask::get resource(%@)fail to Update customer_resource %d "
								"update_date error code: %d!\n", this, res_id, retmp));
						break;
					}
					if( 0 == retmp )
						break;
				}
			}

		}
		else//否则则需要调度
		{
			ptr_unit=createScheUnit(res_id,frequency,lfetchTime,data_group,k_query,mysql);
			ptr_unit->time_str=time_str;
			ptr_unit->data_from = data_from;
		}


        //schedule and request are separated?
		request_t *req	= new request_t();
		req->class_id	= atoi(mysqlrow->fieldValue("vr_id"));			//vr id
		//SS_DEBUG((LM_TRACE,"CScanTask::DEBUGYY:get_resource(%@) scan vrid is %d\n", this, req->class_id));
        req->classid_str.assign(mysqlrow->fieldValue("vr_id"));
		//get the data source info
		bool config_error = false;
		std::map<int, data_source> ds_info_map;
		int ds_id;
		string data_source_sql("select id, priority, data_source, sitemap_url, sitemap_md5 from vr_open_datasource where vr_id=");
		data_source_sql.append(mysqlrow->fieldValue("vr_id"));
		alarm(30);
		ret = data_source_query.execute(data_source_sql.c_str());
		alarm(0);
		while(ret == -1)
		{
			if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",
							this, data_source_sql.c_str(), mysql->error(), mysql->errorno()));
				break;
			}
			alarm(30);
			ret = data_source_query.execute(data_source_sql.c_str());
			alarm(0);
		}
		int data_source_count = data_source_query.rows();
		if(data_source_count > 0)
		{
			sitemap_info *si;
			for(int ds_irow=0; ds_irow<data_source_count; ++ds_irow)
			{
				CMysqlRow *data_source_mysqlrow = data_source_query.fetch();
				ds_id = atoi(data_source_mysqlrow->fieldValue("id"));
				data_source ds(atoi(data_source_mysqlrow->fieldValue("priority")),
								data_source_mysqlrow->fieldValue("data_source"));
				ds_info_map.insert(make_pair(ds_id, ds));

				//get the sitemap info
				if(data_source_mysqlrow->fieldValue("sitemap_url")!=NULL &&
						strlen(data_source_mysqlrow->fieldValue("sitemap_url"))>0)
				{
					si = new sitemap_info(ds_id, atoi(data_source_mysqlrow->fieldValue("priority")),
							data_source_mysqlrow->fieldValue("data_source"),
							data_source_mysqlrow->fieldValue("sitemap_url"),
							data_source_mysqlrow->fieldValue("sitemap_md5"));
					req->sitemap_info_map.insert(make_pair(ds_id, si));
				}
			}
		}
		else
		{
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):data source error, no data source config info, vr_id:%d\n",
						this, req->class_id));
			config_error = true;
		}

		//得到vr_resouce中res_id对应的url信息
		std::string c_sql("select id, url, data_source_id from resource_status where res_id = " );
		c_sql = c_sql + res_id_str +" and res_status > 1";
		int ret = -1;
		alarm(30);
		ret = c_query.execute(c_sql.c_str());
		alarm(0);
		while(ret == -1)
		{
			if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
				if(ptr_unit!=NULL)
					delete ptr_unit;
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",
							this, mysql->error(), mysql->errorno()));
				return -1;
			}
			alarm(30);
			ret = c_query.execute(c_sql.c_str());
			alarm(0);
		}
		//得到res_id对应的url的条数 
		int c_record_count = c_query.rows();
		SS_DEBUG((LM_TRACE,"CScanTask::get_resource(%@): [exstat] new %s found record_count=%d\n",
					this,c_sql.c_str(), c_record_count ));
		VrCounter* counter = NULL;
        std::map<int, int> data_source_map;
        data_source_map.clear();
		//如果site_map或者resource_data不为空，则需要创建计数器
		if(c_record_count!=0 || !req->sitemap_info_map.empty())
		{
			//counter的初始值为总记录数-1，后续再释放counter对象时，不能先对计数器减1，再判断是否等于0.
			int temp_c_record_count = 0;
			if(c_record_count > 0)
				temp_c_record_count = c_record_count -1;
			counter = new VrCounter(res_id, temp_c_record_count);
			//VrCounter记录了当前resource有多少个url需要被抓取

            counter->m_docid.clear();
			counter->m_UrlDocid.clear();
            counter->last_item_num = getLastItemCount(k_query, mysql, data_group, res_id, counter, data_source_map);

			if(counter->valid_num != counter->last_item_num || counter->valid_num == 0)
			{
				req->is_db_error = true;
				SS_DEBUG((LM_TRACE,"CScanTask::get_resource(%@): vr_id %d, item num:%d, valid num %d, change to manual \n",
					this, req->class_id,counter->last_item_num, counter->valid_num));
			}

			if(ptr_unit!=NULL)
			{
				ptr_unit->ptr_vrCount=counter;
                ptr_unit->litemNum = counter->last_item_num;
			}
		}
		else
		{
			if(ptr_unit != NULL) 
			{
				delete ptr_unit;
				ptr_unit = NULL;
			}
			delete req;
			// delete redis set
			delete_redis_set(res_id, vr_id , is_manual, data_group, redis_tool);
			
			continue;
		}

		//先用一个对象记录下vr的公共信息
		//better to use setter func
        
        req->tag.assign(mysqlrow->fieldValue("vr_type"));
		req->frequency			= frequency;	//update frequency
	//	req->over24				= ( atoi(mysqlrow->fieldValue("frequency")) > 86400 );    //is over 24 hours
		req->over24 			= false;
		req->res_id				= res_id;		//id in vr_resource table
		req->data_group			= data_group;
		req->class_no			= atoi(mysqlrow->fieldValue("class_no"));		//class number
		req->attr1				= atoi(mysqlrow->fieldValue("vr_attr1"));
		req->attr2				= atoi(mysqlrow->fieldValue("vr_attr2"));
		req->attr3				= atoi(mysqlrow->fieldValue("vr_attr3"));
		req->rank				= atoi(mysqlrow->fieldValue("vr_rank"));
		req->attr5				= atoi(mysqlrow->fieldValue("vr_attr5"));
		req->tplid				= atoi(mysqlrow->fieldValue("template"));		//template id
      req->data_format.assign(mysqlrow->fieldValue("data_format"));
        
//	fprintf(stderr, "request data format is %s\n",req->data_format.c_str());
	req->vr_flag = atoi(mysqlrow->fieldValue("vr_flag")) > 0? true: false;

        req->index_field.assign(mysqlrow->fieldValue("index_fields")); 			//index field
		req->isNewFrame 		= atoi(mysqlrow->fieldValue("new_framework"));
		SS_DEBUG((LM_DEBUG,"resource id :%d is new Frame %d\n", req->res_id,req->isNewFrame));
		strftime(req->fetch_time_sum,sizeof(req->fetch_time_sum), "%c", &tm);
		strftime(req->fetch_time, sizeof(req->fetch_time), "%F %T", &tm);
		req->lastfetch_time    = lfetchTime;	//上一次抓取时间
		req->is_manual         = is_manual;
		req->data_from			= data_from;
		req->res_status = atoi(mysqlrow->fieldValue("status"));
		req->counter_t         = counter;
		ACE_Time_Value tmpt1;
		tmpt1=ACE_OS::gettimeofday();
		req->startTime=tmpt1;
		req->scanTime=(ACE_OS::gettimeofday()-tmpt1).usec();
		req->res_name.assign(mysqlrow->fieldValue("res_name"));
		req->extend_content.assign(mysqlrow->fieldValue("extend_content"));

		std::string xml_format_str;
		std::string field_str;
		
		// get the xml format info for multi-hit vr
		if(req->class_id >= 70000000)
		{
            //execute query and get result use func 
			std::string xml_format_sql("select label_name, index_type,parent_label, is_mark, is_fullhit, \
					short_label, is_index, second_query, group_type, need_transform from open_xml_format where vr_id=");
			xml_format_sql.append(mysqlrow->fieldValue("vr_id"));
			SS_DEBUG((LM_DEBUG,"format sql: %s\n", xml_format_sql.c_str()));
			alarm(30);
			ret = format_query.execute(xml_format_sql.c_str());
			alarm(0);
			while(ret == -1)
			{
				if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
					if(ptr_unit!=NULL)
						delete ptr_unit;
					SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",
								this, mysql->error(), mysql->errorno()));
					return -1;
				} 
				alarm(30);
				ret = format_query.execute(xml_format_sql.c_str());
				alarm(0);
			}

			int format_count = format_query.rows();
			if(format_count > 0)
			{
				string xpath, parent;
				bool is_annotation=false, is_hit=false;
				bool is_index=false, is_second_query=false, need_transform=false;
				int index=0, sort=0, group_type=0,full_text = 0;
				for(int format_irow=0; format_irow<format_count; ++format_irow)
				{
					CMysqlRow *f_mysqlrow = format_query.fetch();
					//get the xpath info
					if(f_mysqlrow->fieldValue("parent_label") != NULL)
						parent.assign(f_mysqlrow->fieldValue("parent_label"));
					if(parent.size()>0 && parent[0]=='/')
						xpath.assign("/");
					else
						xpath.assign("//");
					xpath.append(parent);
					xpath.append("/");
					xpath.append(f_mysqlrow->fieldValue("label_name"));

					field_str.assign(f_mysqlrow->fieldValue("label_name"));
					xml_format_str += field_str;

					//get the annotation info
					is_annotation = atoi(f_mysqlrow->fieldValue("is_mark"))>0 ? true : false;
					if(is_annotation)
						req->annotation_map.insert(make_pair(xpath, "1"));

					field_str.assign(f_mysqlrow->fieldValue("is_mark"));
					xml_format_str += field_str;

					//get the index info
					is_index = atoi(f_mysqlrow->fieldValue("is_index"))==0 ? false : true;
					if(is_index)
					{
						index = atoi(f_mysqlrow->fieldValue("is_fullhit"));
						if(index == 1) //no segment
							req->index_map.insert(make_pair(xpath, "1"));
						else if(index == 2) //need segment
							req->index_map.insert(make_pair(xpath, "2"));
					}
					full_text = atoi(f_mysqlrow->fieldValue("index_type"));
					if(full_text == 1)
						req->full_text_map.insert(std::make_pair(xpath, f_mysqlrow->fieldValue("index_type")));

					field_str.assign(f_mysqlrow->fieldValue("is_index"));
					xml_format_str += field_str;
					field_str.assign(f_mysqlrow->fieldValue("is_fullhit"));
					xml_format_str += field_str;
					field_str.assign(f_mysqlrow->fieldValue("index_type"));
					xml_format_str += field_str;

					//get the sort info
					sort = atoi(f_mysqlrow->fieldValue("short_label"));
					if(sort != 0)
						req->sort_map.insert(make_pair(xpath, f_mysqlrow->fieldValue("short_label")));

					field_str.assign(f_mysqlrow->fieldValue("short_label"));
					xml_format_str += field_str;

					//get the hit field info
					is_hit = atoi(f_mysqlrow->fieldValue("is_fullhit"))==0 ? false : true;
					if(is_hit)
						req->hit_fields.insert(make_pair(f_mysqlrow->fieldValue("label_name"), xpath));

					field_str.assign(f_mysqlrow->fieldValue("is_fullhit"));
					xml_format_str += field_str;

					//get the second query info
					is_second_query = atoi(f_mysqlrow->fieldValue("second_query"))>0 ? true : false;
					if(is_second_query)
						req->second_query_map.insert(make_pair(xpath, "1"));

					field_str.assign(f_mysqlrow->fieldValue("second_query"));
					xml_format_str += field_str;

					//get the group info
					group_type = atoi(f_mysqlrow->fieldValue("group_type"));
					if(group_type>=1 && group_type<=3)
						req->group_map.insert(make_pair(xpath, f_mysqlrow->fieldValue("group_type")));
					need_transform = atoi(f_mysqlrow->fieldValue("need_transform"))>0 ? true : false;
					if(need_transform)
						req->url_transform_map.insert(make_pair(xpath, "1"));

					field_str.assign(f_mysqlrow->fieldValue("group_type"));
					xml_format_str += field_str;
					field_str.assign(f_mysqlrow->fieldValue("need_transform"));
					xml_format_str += field_str;
				}
			}
			else
			{
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):format error, no format config info, vr_id:%d\n",
							this, req->class_id));
				config_error = true;
			}
		}

		// save xml_format md5 to redis and update xml_format status
		unsigned char *  xml_format_md5;
		xml_format_md5 = MD5((const unsigned char*)xml_format_str.c_str(), xml_format_str.length(), NULL);
		char xml_format_md5_hex[33];
		hexstr(xml_format_md5, xml_format_md5_hex, MD5_DIGEST_LENGTH);
		std::string xml_format_md5_redis;
		std::string xml_format_md5_str(xml_format_md5_hex);
		int ret_redis = redis_tool->GetFormat(req->classid_str, xml_format_md5_redis);
		SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@): xml_format %s, xml_format_md5 %u , xml_format_md5_hex %s, xml_format_md5_redis %s\n",\
						this, xml_format_str.c_str(), xml_format_md5, xml_format_md5_hex, xml_format_md5_redis.c_str()));
		
		if (ret_redis > 0)
		{
			if (xml_format_md5_redis == xml_format_md5_str)
			{
				req->xml_format_status = false;
			}
			else
			{
				redis_tool->SetFormat(req->classid_str, xml_format_md5_str);
			}
		}
		else if (ret_redis == 0)
		{
			redis_tool->SetFormat(req->classid_str, xml_format_md5_str);
		}

		SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@): xml_format_status %d vr_id %s", this, req->xml_format_status, req->classid_str.c_str()));

		//遍历resource的每一条url
		int c_doc_num = 0;
		vr_info *vi = NULL;
		DataSourceCounter *ds_counter;
		bool is_fetching = false;
		for(int c_irow = 0; c_irow < c_record_count; c_irow++)
		{
			ACE_Time_Value tmpt1;
			tmpt1=ACE_OS::gettimeofday();
			CMysqlRow * c_mysqlrow = c_query.fetch();
			if(!c_mysqlrow)
			{
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@): "
							"[exstat] get vr row error:%d %s [%s]\n",this,
							c_irow, mysql->error(), mysql->errorno() ));
				break;
			}

			//field check
			if(!c_mysqlrow->fieldValue("url") || strlen(c_mysqlrow->fieldValue("url"))<1)
			{
				SS_DEBUG(( LM_CRITICAL, "CScanTask::get_resource_status(%@): field error: url!\n",this ));
				continue;
			}

			//check data source
			ds_id = atoi(c_mysqlrow->fieldValue("data_source_id"));
			std::map<int, data_source>::const_iterator ds_itr;
			ds_itr = ds_info_map.find(ds_id);
			if(ds_itr == ds_info_map.end())
			{
				SS_DEBUG((LM_ERROR, "CScanTask::get_resource(%@): data source id error:%d, vr_id:%d\n", 
							this, ds_id, req->class_id));
				continue;
			}

			//check url
			std::string url(c_mysqlrow->fieldValue("url"));
			std::set<std::string>::iterator it_set;
			pthread_mutex_lock(&fetch_map_mutex);//lock
			it_set = req->url_uniq_set.find(url);
			if(it_set != req->url_uniq_set.end())
			{
				SS_DEBUG((LM_ERROR,"CScanTask::get_resource class_id = %d already have in url set\n",req->class_id));
				pthread_mutex_unlock(&fetch_map_mutex);//unlock
				continue;
			}
			req->url_uniq_set.insert(url);
			std::string unqi_key = req->classid_str+"#" + url;
			if( (fetch_map.find(unqi_key) != fetch_map.end()) && fetch_map[unqi_key] > 0 )
			{
				is_fetching = true;
				SS_DEBUG(( LM_ERROR, "CScanTask::get_resource class_id:%d already fetching:%s, the value in fetch_map is %d\n",
							req->class_id, url.c_str() ,fetch_map[unqi_key]));
				//reset url
				std::vector<vr_info *>::iterator itr;
				for(itr=req->vr_info_list.begin(); itr!=req->vr_info_list.end(); ++itr)
				{
					
					SS_DEBUG(( LM_ERROR, "CScanTask::get_resource class_id:%d already fetching:%s, unqi_key %s \n",(*itr)->unqi_key.c_str()));
					fetch_map[(*itr)->unqi_key] = 0;
				}
				pthread_mutex_unlock(&fetch_map_mutex);//unlock
				break;
			}
			else
			{
				fetch_map[unqi_key] = 1;//记录已经抓取的url
			}
			pthread_mutex_unlock(&fetch_map_mutex);//unlock


			//generate vr info
			vi = new vr_info(url, atoi(c_mysqlrow->fieldValue("id")), ds_id);
			vi->ds_priority = ds_itr->second.ds_priority;
			vi->ds_name.assign(ds_itr->second.ds_name);
			vi->unqi_key.assign(unqi_key);

			_mem_check ++;

			//initialize the counter of each data source
			std::map<int, DataSourceCounter*>::iterator ds_counter_itr = req->counter_t->ds_counter.find(ds_id);
            std::map<int, int>::iterator ds_it;
			if(ds_counter_itr == req->counter_t->ds_counter.end())
			{
				ds_counter = new DataSourceCounter();
				ds_counter->total_count += 1;
                ds_it = data_source_map.find(ds_id);
				//get the total number of this data source
                if( ds_it != data_source_map.end())
                {
                    ds_counter->last_item_num = ds_it->second;
                }
                else
                {
                    ds_counter->last_item_num = 0;
                }

				ds_counter->ds_name = vi->ds_name;

				req->counter_t->ds_counter.insert(make_pair(ds_id, ds_counter));
				SS_DEBUG(( LM_TRACE, "CScanTask::get_resource(%@): a new data source counter\n", this));
			}
			else
			{
				ds_counter_itr->second->total_count += 1;
			}

			//将url和表id push到vr info列表中
			req->vr_info_list.push_back(vi);

			c_doc_num++;
			SS_DEBUG(( LM_TRACE, "CScanTask::get_resource(%@): memdebug: %d, "
						"put %d %s %s\n",this, _mem_check, req->tplid, req->tag.c_str(), url.c_str() ));
		}//end for(int c_irow = 0; c_irow < c_record_count; c_irow++)

		req->counter_t->setCount(req->vr_info_list.size() - 1);
		//遍历完了一个resource的所有url
		if(!config_error && !is_fetching && (!req->vr_info_list.empty()||!req->sitemap_info_map.empty()))
		{
			ACE_Message_Block *msg = NULL;
			msg = new ACE_Message_Block( reinterpret_cast<char *>(req) );
			if(is_manual)
			{
				ret = save_timestamp(mysql, req);//save timestamp
				if (ret < 0) {
					SS_DEBUG((LM_ERROR, "CScanTask::get_resource(%@) get timestamp_id failed res_id:%s\n", this, req->res_id ));
				}
				//update fetch status 1:fetching
				Util::update_fetchstatus(mysql, req->res_id, 1, "");
				//人工更新，直接put到下级流水
				CUrlUpdateTask::instance()->put(msg);	
				SS_DEBUG(( LM_TRACE, "CScanTask::get_resource class_id=%d is_manual, total url number %d, begin fetch\n", req->class_id, req->vr_info_list.size() ));
				continue;
			}
			else
			{
				ptr_unit->data.push_back(msg);
				fprintf(stderr,"[TEST]put to schedule [input req_vector]res_id=%d,url_num=%lu,keynum=%d\n",
						ptr_unit->res_id,ptr_unit->data.size(),ptr_unit->litemNum);
				req_vector.push_back(ptr_unit);//将调度单元放入调度容器中 
			}
		}
		else 
		{
			SS_DEBUG((LM_ERROR, "[CScanTask::get_resource(%@)] destroy no url schedule.\n", this));
			if(is_manual)
			{
				if(req->counter_t != NULL)
					delete req->counter_t;
				delete req;
				req = NULL;
			} 
			else 
			{
				deleteScheUnit(ptr_unit);
			}
			
			// delete redis set
			delete_redis_set(res_id, vr_id , is_manual, data_group, redis_tool);
		}
		doc_num++;//这里表示一个resource
	}
	/**        END      **/
	//排序后，把msg放入抓队列中
	std::stable_sort(req_vector.begin(),req_vector.end(),msgGreator); 
	std::vector<sched_unit*>::iterator itr;
	int FetchedKey=0;	
	/**TEST***/
	/*
	   for(itr=req_vector.begin();itr!=req_vector.end();itr++)
	   {
	//request_t  *req = ACE_reinterpret_cast(request_t*, (*itr)->rd_ptr());
	sched_unit* req=(*itr);
	fprintf(stderr,"[TEST]put to fetch [Task sort]key_num=%d,res_id=%d,url_num=%lu\n",req->litemNum,req->res_id,req->data.size());
	}		
	 */
	/**TEST***/
	for(itr=req_vector.begin();itr!=req_vector.end();)
	{
		//request_t  *req = ACE_reinterpret_cast(request_t*, (*itr)->rd_ptr());
		sched_unit* req=(*itr);
		//如果可抓取，那么直接放入抓取队列
		if( (FetchedKey+req->litemNum)< MAX_FETCH_PER_MiNU )
		{
			FetchedKey+=putScheUnitToFetch(req,mysql,crsql);

			//update fetch status 1:fetching
			Util::update_fetchstatus(mysql, req->res_id, 1, "");

			delete req;//同时删除res
			itr=req_vector.erase(itr);
		}
		else//留出机会给最需要的大数据更新
		{
			fprintf(stderr,"[TEST]put to fetch large key res\n");
			break;	
		}
	}

	//如果当前writeTask中的堆积小于线程数，那么放入一个大数据
	if( (!req_vector.empty()) && (CWriteTask::instance()->msg_queue()->message_count()<8) ) 
	{
		std::stable_sort(req_vector.begin(),req_vector.end(),msgGreatorB);
		sched_unit* req=req_vector[0];

		fprintf(stderr,"[TEST]put to fetch [last Task]res_id=%d,key_num=%d,url_num=%ld,lfetch=%ld,frequency=%d\n\n",
				req->res_id,req->litemNum,req->data.size(),req->lastfetch_time,req->frequency);
		putScheUnitToFetch(req,mysql,crsql);
		
		//update fetch status 1:fetching
		Util::update_fetchstatus(mysql, req->res_id, 1, "");

		delete req;	
		req_vector.erase(req_vector.begin());//找出最需要抓取的url，进行抓取		
	}
	//其他的vr类别当前不再进行抓取
	for(itr=req_vector.begin();itr!=req_vector.end();)
	{//其他的url丢掉
		sched_unit* _req=(*itr);
		fprintf(stderr,"[TEST]put to fetch [Discard] key_num=%d,res_id=%d,lfetch=%ld,frequency=%d\n\n",
				_req->litemNum,_req->res_id,_req->lastfetch_time,_req->frequency);
		deleteScheUnit(_req);
		itr=req_vector.erase(itr);  
		// delete redis set
		//std::stringstream ss;
        //ss << _req->res_id;
        //std::string value = ss.str();
		//delete_redis_set(_req->res_id, _req->vr_id, _req->, redis_tool);
	}    

	SS_DEBUG((LM_TRACE,"CScanTask::get_resource(%@): [exstat] to fetch c_doc_num=%d\n",this, doc_num ));
	return 0;
}

sched_unit *CScanTask::createScheUnit(int res_id,int frequency,long lfetchTime,int data_group,CMysqlQuery &k_query,CMysql *mysql)
{
	fprintf(stderr,"[TEST]put to fetch[need to fetch res_id=%d]\n",res_id);
	sched_unit *ptr_unit=new sched_unit;
	if(ptr_unit==NULL)
		return NULL;
	ptr_unit->res_id=res_id;
	ptr_unit->frequency=frequency;
	ptr_unit->lastfetch_time=lfetchTime;
	ptr_unit->ptr_vrCount=NULL;

	return ptr_unit;
}


int CScanTask::getLastItemCount(CMysqlQuery &k_query, CMysql *mysql, int data_group, int res_id, VrCounter* counter_t, std::map<int, int> &data_source_map)
{
	char keybuf[256];//select * from vr_data_%d where res_id = %d
	std::map<std::string, std::string> *docid_map = &(counter_t->m_docid);
	std::map<int, std::vector<std::string> > *urlDocid_map = &(counter_t->m_UrlDocid);
	counter_t->valid_num = 0;
	snprintf(keybuf,256,"select md5_sum, doc_id, data_source_id, status_id, data_status from vr_data_%d WHERE res_id =%d",data_group,res_id);
	alarm(30);
	int k_ret = k_query.execute(keybuf);
	alarm(0);
	while(k_ret == -1)
	{
		if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CScanTask::get vr_data (%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",
						this, keybuf, mysql->error(), mysql->errorno()));
			break;
		}
		alarm(30);
		k_ret = k_query.execute(keybuf);
		alarm(0);
	}
	int k_record_count = 0;
	k_record_count = k_query.rows();
    std::map<int, int>::iterator it;
    std::string md5sum;
    std::string docid;
	int status_id;
    int data_sourceid; 
    CMysqlRow * k_mysqlrow;

	for(int i=0; i<k_record_count; i++)
	{
		k_mysqlrow = k_query.fetch();
		if(!k_mysqlrow)
		{
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@): [exstat] get vr row error: %s [%s]\n",this, mysql->error(), mysql->errorno() ));
            break;
		}

        md5sum = k_mysqlrow->fieldValue("md5_sum");
        docid = k_mysqlrow->fieldValue("doc_id");
        data_sourceid = atoi(k_mysqlrow->fieldValue("data_source_id")); 
		status_id = atoi(k_mysqlrow->fieldValue("status_id")); 
		counter_t->valid_num += atoi(k_mysqlrow->fieldValue("data_status"));

        (*docid_map).insert(make_pair(docid, md5sum));
		(*urlDocid_map)[status_id].push_back(docid);

        it = data_source_map.find(data_sourceid);
        if(it != data_source_map.end())
        {
            it->second += 1;
        }
        else
        {
            data_source_map.insert(make_pair(data_sourceid, 1));
        }
	}
	
	return k_record_count;
}


int CScanTask::putScheUnitToFetch(sched_unit* req,CMysql *mysql,CMysql *crsql)
{
	int ret=req->litemNum;
	int res_id=req->res_id;
	char buf[1024];//更新resource的更新时间
	sprintf(buf, "update vr_resource set fetch_time = '%s' where id = %d",req->time_str.c_str(),req->res_id);
	CMysqlExecute exe(mysql);
	int retmp;
	while((retmp=exe.execute(buf)) < 1)
	{
		if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR, "CScanTask::get resource(%@)fail to Update vr_resource %d update_date error code: %d!\n",
					this, res_id, retmp));
			break;
		}
		if( 0 == retmp )
			break;
	}

	if(req->data_from == 4)
	{
		sprintf(buf, "update customer_resource set fetch_time = '%s' where online_id = %d",req->time_str.c_str(),req->res_id);
		CMysqlExecute cr_exe(crsql);
		//int retmp;
		while((retmp=cr_exe.execute(buf)) < 1)
		{
			if(crsql->errorno() != CR_SERVER_GONE_ERROR) {
				SS_DEBUG((LM_ERROR, "CScanTask::get resource(%@)fail to Update customer_resource %d update_date error code: %d!\n",
						this, res_id, retmp));
				break;
			}
			if( 0 == retmp )
				break;
		}
	}
	//将res_id对应的所有的url都加入到抓取队列中
	fprintf(stderr,"[TEST]put to fetch [put to FetchTask][%s]res_id=%d key_num=%d,lfetch=%ld,frequency=%d all url\n",
			buf,req->res_id,req->litemNum,req->lastfetch_time,req->frequency);
	std::vector<ACE_Message_Block*>::iterator reqItr;
	for(reqItr=req->data.begin();reqItr!=req->data.end();reqItr++)
	{
		ACE_Message_Block* msg = *reqItr;
		request_t*         pReq= reinterpret_cast<request_t *>(msg->rd_ptr());
		int ret = save_timestamp(mysql, pReq);//save timestamp
		if (ret < 0) {
			SS_DEBUG((LM_ERROR, "get timestamp_id failed\n"));
		}
		SS_DEBUG(( LM_TRACE, "CScanTask::putScheUnitToFetch class_id=%d is auto update, total url number is %d, begin fetch\n", pReq->class_id, pReq->vr_info_list.size() ));
		CUrlUpdateTask::instance()->put(msg);
	}
	return ret;
}

//删除一个sched_uint 
void CScanTask::deleteScheUnit(sched_unit* req)
{
	if(req==NULL)
		return ;
	if(req->ptr_vrCount!=NULL)
		delete req->ptr_vrCount;
	std::vector<ACE_Message_Block*>::iterator reqItr;
	for(reqItr=req->data.begin();reqItr!=req->data.end();reqItr++)
	{	
		ACE_Message_Block* msg=(*reqItr);
		request_t*         pReq= reinterpret_cast<request_t *>(msg->rd_ptr());
        
        // delete_redis_set          
        delete_redis_set(pReq->res_id, pReq->class_id, pReq->is_manual, pReq->data_group, redis_tool);
		pthread_mutex_lock(&fetch_map_mutex);//lock
		std::vector<vr_info *>::iterator itr;
		for(itr=pReq->vr_info_list.begin(); itr!=pReq->vr_info_list.end(); ++itr) {
			if( fetch_map.find((*itr)->unqi_key) != fetch_map.end() && fetch_map[(*itr)->unqi_key] > 0 ) {
				fetch_map[(*itr)->unqi_key]  = 0;
				fprintf(stderr,"[deleteScheUnit]Discard url=%s\n", (*itr)->url.c_str());
			}
		}
		pthread_mutex_unlock(&fetch_map_mutex);//unlock
		if(msg!=NULL)
			msg->release();
		if(pReq!=NULL)
			delete pReq;
	}
	delete req;
}

int CScanTask::delete_redis_set(int res_id, int vr_id, bool is_manual, int data_group, RedisTool *redis_tool)
{
	std::stringstream ss;
    ss << res_id << "#" << vr_id << "#" << is_manual << "#" << data_group;
    std::string value = ss.str();
	std::stringstream ss_set;
	ss_set << res_id;
	std::string set_value = ss_set.str();
	return delete_redis_set(set_value, value, redis_tool);
}

int CScanTask::delete_redis_set(std::string set_value, std::string value, RedisTool *redis_tool)
{
	int redis_ret = -1;
    //redis_tool->SetListSetName(ACTION_FETCH);
    while (redis_ret < 0)
    {
        if (!redis_tool->GetConnected())
        {
            redis_tool->Connect();
        }
        redis_ret = redis_tool->DeleteSet(set_value);
        if (redis_ret < 0)
        {
            SS_DEBUG((LM_TRACE, "CScanTask::delete_redis_set(%@): [ignore_vr]delete_redis_set redis error, spider_type : %s, value : %s %d\n",this, spider_type.c_str(), value.c_str(), redis_ret ));
            redis_tool->SetConnected(false);
            redis_tool->Close();
        }
        else
        {
            SS_DEBUG((LM_TRACE, "CScanTask::delete_redis_set(%@): [ignore_vr]delete_redis_set from redis, spider_type : %s, value : %s %d\n",this, spider_type.c_str(), value.c_str(), redis_ret ));
        }
    } 
	redis_ret = -1;
	while (redis_ret < 0)
    {
        if (!redis_tool->GetConnected())
        {
            redis_tool->Connect();
        }
        redis_ret = redis_tool->DeleteSet(unique_set_name, value);
        if (redis_ret < 0)
        {
            SS_DEBUG((LM_TRACE, "CScanTask::delete_redis_set(%@): [ignore_vr]delete_redis_unique_set redis error, spider_type : %s, value : %s %d\n",this, spider_type.c_str(), value.c_str(),redis_ret ));
            redis_tool->SetConnected(false);
            redis_tool->Close();
        }
        else
        {
            SS_DEBUG((LM_TRACE, "CScanTask::delete_redis_set(%@): [ignore_vr]delete_redis_unique_set from redis, spider_type : %s, value : %s %d\n",this, spider_type.c_str(), value.c_str(),redis_ret ));
        }
		
    } 
	return 0;
}

std::string CScanTask::update_redis_time(std::string prev_time)
{
	time_t now_seconds;  
	now_seconds = time((time_t *)NULL);
	int random_num = rand();
	std::stringstream ss;
	ss << now_seconds << "#" << random_num;
	std::string value = ss.str();

	if (prev_time == "")
	{
		int redis_ret = -1;
		while (redis_ret < 0)
		{
			if (!redis_tool->GetConnected())
			{
				redis_tool->Connect();
			}
			redis_ret = redis_tool->SetHash(unique_set_name, value);
			if (redis_ret < 0)
			{
				SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time error, unique_set_name : %s, value : %s %d\n",this, unique_set_name.c_str(), value.c_str(), redis_ret ));
				redis_tool->SetConnected(false);
				redis_tool->Close();
			}
			else
			{
				//SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time success, unique_set_name : %s, value : %s %d\n",this, unique_set_name.c_str(), value.c_str(), redis_ret ));
			}
		} 
		
	}
	else
	{
		std::string redis_time;
		int redis_ret = -1;
		while (redis_ret < 0)
		{
			if (!redis_tool->GetConnected())
			{
				redis_tool->Connect();
			}
			redis_ret = redis_tool->GetHash(unique_set_name, redis_time);
			if (redis_ret < 0)
			{
				SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time error, unique_set_name : %s, value : %s %d\n",this, unique_set_name.c_str(), value.c_str(), redis_ret ));
				redis_tool->SetConnected(false);
				redis_tool->Close();
			}
			else if (redis_ret == 0 || redis_time != prev_time)
			{
				SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time not valid, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), redis_ret ));
				int ret = redis_tool->GetIncrUniqueName(unique_set_name);
				SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): GetIncrUniqueName success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
				ret = redis_tool->SetHash(unique_set_name, value);
				SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
			}
			else
			{
				int ret = redis_tool->SetHash(unique_set_name, value);
				//SS_DEBUG((LM_TRACE, "CScanTask::update_redis_time(%@): update_redis_time success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
			}
		} 
	}
	return value;
}

int CScanTask::AddSomeUniqueName(RedisTool *redis_tool, int start, int num)
{
	
	std::string base_set_name = "UniqueName_";
	for (int i = start; i < start + num; i++)
	{
		std::stringstream ss;
    	ss << spider_type << "_" << i ;
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
	            SS_DEBUG((LM_TRACE, "CScanTask::AddSomeUniqueName(%@): add error,  value : %s %d\n",this, value.c_str(), redis_ret ));
	            redis_tool->SetConnected(false);
	            redis_tool->Close();
	        }
	        else
	        {
	            SS_DEBUG((LM_TRACE, "CScanTask::AddSomeUniqueName(%@): add success value : %s %d\n", this,value.c_str(), redis_ret ));
	        }
	    } 
	}
	return 0;
}

std::string CScanTask::check_add(std::string value, std::string prev_time)
{
	std::string redis_time;
	int redis_ret = -1;
	while (redis_ret < 0)
	{
		if (!redis_tool->GetConnected())
		{
			redis_tool->Connect();
		}
		redis_ret = redis_tool->GetHash(unique_set_name, redis_time);
		if (redis_ret < 0)
		{
			SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): check_add error, unique_set_name : %s, value : %s %d\n",this, unique_set_name.c_str(), value.c_str(), redis_ret ));
			redis_tool->SetConnected(false);
			redis_tool->Close();
		}
		else if (redis_ret == 0 || redis_time != prev_time)
		{
			SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): check_add not valid, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), redis_ret ));
			int ret = redis_tool->GetIncrUniqueName(unique_set_name);
			SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): GetIncrUniqueName success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
			ret = redis_tool->SetAdd(unique_set_name, value);
			SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): check_add success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
			// reset prev_time
			prev_time = "";
		}
		else
		{
			int ret = redis_tool->SetAdd(unique_set_name, value);
			SS_DEBUG((LM_TRACE, "CScanTask::check_add(%@): check_add success, unique_set_name : %s, value : %s, prev_time %s %d\n",this, unique_set_name.c_str(), value.c_str(), prev_time.c_str(), ret ));
		}
	} 
	return prev_time;
}



int CScanTask::svc(){ 
	//bool SelfUpdate = true;  //can change it if you need
	//fetch_flag_map.clear();
	//update_flag_map.clear();
	
	CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	CMysql crsql(cr_ip, cr_user, cr_pass, cr_db, "set names gbk");

    int reconn = 0;
	while( reconn < 5 )
	{//重连mysql，最多5次
		bool suc = true;
		if( mysql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql.connect() ) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( !mysql.open() ) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CScanTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql.close();
		mysql.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: connect vr fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	/**************数据库连接  **********/
	reconn = 0;
	while( reconn < 5 ){
		bool suc = true;
		if( crsql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !crsql.connect()) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: connect server fail: %s\n",this, crsql.error()));
			suc = false;
		}
		if( !crsql.open() ) {
			SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: open database fail: %s\n",this, crsql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&crsql);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "CScanTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		crsql.close();
		crsql.init();
	}
	if( reconn >=5 ) {
		SS_DEBUG((LM_ERROR, "CScanTask::svc(%@):Mysql: connect cr mysql server fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}

    // spider_type 1：normal，2：inst，3：多命中
    std::string vr_sql("select * from vr_resource where ");
	std::string cr_sql("select * from customer_resource where ");	

	//delete
	std::string del_vr_sql("select * from vr_resource where vr_id=");
  
	thr_id_scan = thr_mgr()->thr_self();
	SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): begin to loop %d\n",this, thr_id_scan));

	std::string sql;

	// set redis_time and save in prev_time
	std::string prev_time = update_redis_time("");
	
	while(!ACE_Reactor::instance()->reactor_event_loop_done())
	{
		int write_task_num = ConfigManager::instance()->getWriteTaskNum();
		int fetch_task_num = ConfigManager::instance()->getFetchTaskNum();
		int updatedelete_task_num = ConfigManager::instance()->getupdateDeleteNum();

		int ret = -1;
		std::string value;
        SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@):write_task_queue : %d, fetch_task_queue : %d, online_task_queue : %d, update/delete_task_queue : %d\n",this, \
							CWriteTask::instance()->msg_queue()->message_count(), \
							CFetchTask::instance()->msg_queue()->message_count(), \
							COnlineTask::instance()->msg_queue()->message_count(), \
							CDeleteTask::instance()->msg_queue()->message_count()));

		//删除数据
		while(ret < 0)
		{
			if (!redis_tool->GetConnected())
			{
				redis_tool->Connect();
			}
			ret = redis_tool->GetDel(value);
			if (ret < 0)
			{
				//SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): get del redis error\n",this ));
				redis_tool->SetConnected(false);
				redis_tool->Close();
			}
			else if( ret > 0 )
			{
				//SS_DEBUG((LM_TRACE, "CScanTask::get_from_redis del %s\n", value.c_str()));
				get_delete(&mysql, value);

				// sleep time for process
                sleep(1);
			}
		}

		ret = -1;

		//抓取数据
		if (CWriteTask::instance()->msg_queue()->message_count() < write_task_num &&
			CFetchTask::instance()->msg_queue()->message_count() < fetch_task_num )
		{
			while(ret < 0)
			{
				//SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@): begin get from redis\n",this ));
				if (!redis_tool->GetConnected())
				{
					redis_tool->Connect();
				}

				//获取抓取的数据
				//redis_tool->SetListSetName(ACTION_FETCH);
				ret = redis_tool->Get(value);
				if (ret < 0)
				{
					SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@): get redis error\n",this ));
					redis_tool->SetConnected(false);
					redis_tool->Close();
				}
				else if (ret > 0)
				{
					std::vector<std::string> vec;
	                SS_DEBUG((LM_TRACE, "CScanTask::get_from_redis %s\n", value.c_str()));
					splitStr(value, '#', vec);
					
					// put to unique_set
					//int redis_ret = redis_tool->SetAdd(unique_set_name, value);
					
					prev_time = check_add(value, prev_time);
					
					SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): put to unique_set %s, prev_time %s\n",this, value.c_str(), prev_time.c_str() ));
					
					if (vec.size() > 2 && spider_type == "0")
					{
                        sql = cr_sql + " id = " + vec[0];
		                SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): get res_id %s, sql %s\n",this, vec[0].c_str(), sql.c_str() ));
						if (vec[2] == "1")
						{
							get_customer_resource(&crsql, sql, true);
						}
						else if (vec[2] == "0")
						{
							get_customer_resource(&crsql, sql, false);
						}
					}
					else if (vec.size() > 3)
					{
                        sql = vr_sql + " id = " + vec[0];
		                SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): get res_id %s, sql %s\n",this, vec[0].c_str(), sql.c_str() ));
						if (vec[2] == "1")
						{
							get_resource( &mysql, &crsql, sql, true, redis_tool);
						}
						else if (vec[2] == "0")
						{
							get_resource( &mysql, &crsql, sql, false, redis_tool);
						}
					}
		            SS_DEBUG((LM_TRACE, "CScanTask::svc(%@): get res_id %s, sql %s\n",this, vec[0].c_str(), sql.c_str()));
                    // sleep time for process
                    sleep(1);
				}
                else
                {
	                //SS_DEBUG((LM_TRACE, "CScanTask::get_redis no item\n",this));
                }
				//SS_DEBUG((LM_TRACE, "CScanTask::get_resource(%@): end get from redis\n",this ));

			}
		}
		
		prev_time = update_redis_time(prev_time);
		sleep(1);
	}
	
	SS_DEBUG((LM_TRACE, "CScanTask::sl_update = 1 manual_update = 1 svc(%@): exit from loop\n",this));
	return 0;
}

int CScanTask::get_customer_resource( CMysql *mysql, std::string sql, bool is_manual)
{
	CMysqlQuery query(mysql);
	int ret = -1;
	alarm(30);
	ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}

	int record_count = query.rows();
	SS_DEBUG((LM_TRACE,"CScanTask::get_customer_resource(%@): [exstat] found record_count=%d\n",this, record_count ));
	int doc_num = 0;

	for(int irow=0; irow<record_count; irow++)
	{
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"CScanTask::get_customer_resource(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		//field check
		if(!mysqlrow->fieldValue("url") || strlen(mysqlrow->fieldValue("url"))<1)
		{
			SS_DEBUG(( LM_CRITICAL, "CScanTask::get_customer_resource: field error: url!\n" ));
			continue;
		}

		/*
		int field_num = mysqlrow->fieldNum();
		int i = 0;
		for(;i<field_num;i++){
			if(mysqlrow->fieldValue(i))
				SS_DEBUG(( LM_DEBUG, "field %d %s\n", i, mysqlrow->fieldValue(i) ));
		}
		if( i != field_num )
		{
			SS_DEBUG(( LM_CRITICAL, "CScanTask::get_resource: field num error: %d!\n", i ));
			continue;
		}
		*/

		std::string url(mysqlrow->fieldValue("url"));
		std::string unqi_key = "customer#"+url;
		pthread_mutex_lock(&fetch_map_mutex);//lock
		if( (fetch_map.find(unqi_key) != fetch_map.end()) && fetch_map[unqi_key] > 0 ){
			SS_DEBUG(( LM_ERROR, "CScanTask::get_resource(%@): already fetching: %d %s\n",this,
						fetch_map[url], url.c_str() ));
			pthread_mutex_unlock(&fetch_map_mutex);//unlock
			continue;
		}else{
			fetch_map[unqi_key] = 1;
		}
		pthread_mutex_unlock(&fetch_map_mutex);//unlock

		ACE_Message_Block *msg = NULL;
		request_t *        req = new request_t();
		_mem_check ++;
		req->tag.assign("customer"); // marked as external customer resource
		req->frequency         = atoi(mysqlrow->fieldValue("frequency"));                // res_status
		req->over24            = ( atoi(mysqlrow->fieldValue("frequency")) > 86400 );    // res_status
		req->res_id            = atoi(mysqlrow->fieldValue("id"));                       // vr_res
		req->status_id         = 0; //atoi(mysqlrow->fieldValue(RS_ID));                       // res_status
		req->data_group        = -1; //atoi(mysqlrow->fieldValue(RS_DATA_GROUP));               // res_status
		req->class_id          = 0;//atoi(mysqlrow->fieldValue(VR_CLASS_ID));                 // vr_res
		req->class_no          = 0;//atoi(mysqlrow->fieldValue(VR_CLASS_NO));                 // vr_res
		req->attr1             = 0;//atoi(mysqlrow->fieldValue(VR_ATTR1));                    // vr_res
		req->attr2             = 0;//atoi(mysqlrow->fieldValue(VR_ATTR2));
		req->attr3             = 0;//atoi(mysqlrow->fieldValue(VR_ATTR3));
		req->rank              = 0;//atoi(mysqlrow->fieldValue(VR_RANK));                     // vr_res
		req->attr5             = 0;//atoi(mysqlrow->fieldValue(VR_ATTR5));                    // vr_res
		req->tplid             = 0;//atoi(mysqlrow->fieldValue(VR_TEMPLATE));                 // vr_res
		req->index_field.assign("");//mysqlrow->fieldValue(VR_INDEX_FIELD));                    // vr_res
		req->isNewFrame = 0;//atoi(mysqlrow->fieldValue(NEW_FRAMEWORK));                      // vr_res
		req->is_manual = is_manual;

		vr_info *vi = new vr_info(url, 0);
		req->vr_info_list.push_back(vi);

		msg = new ACE_Message_Block( reinterpret_cast<char *>(req) );
		CFetchTask::instance()->put(msg);
		doc_num++;
		SS_DEBUG((LM_TRACE, "CScanTask::get_customer_resource(%@): memdebug: %d, put %s\n",this, _mem_check, req->url.c_str()));
	}
	SS_DEBUG((LM_TRACE,"CScanTask::get_customer_resource(%@): [exstat] to fetch doc_num=%d\n",this, doc_num ));
	return 0;
}

int CScanTask::sql_write( CMysql *mysql, const char *sql ){
	CMysqlExecute exe(mysql);
	alarm(30);
	int ret = exe.execute(sql);
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CScanTask::sql_write(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql, mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = exe.execute(sql);
		alarm(0);
	}
	SS_DEBUG((LM_DEBUG, "CScanTask::sql_write(%@):Mysql[%s] success\n",this, sql ));
	return ret;
}

int CScanTask::stopTask() {
	flush();
	thr_mgr()->kill_grp(grp_id(), SIGINT);
	wait();
	return 0;
}

