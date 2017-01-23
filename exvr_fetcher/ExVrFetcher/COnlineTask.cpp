#include <ace/Reactor.h>
#include <Platform/log.h>
#include "ExVrFetcher.h"
#include "COnlineTask.hpp"
#include "Refresh_Timer.h"
#include "Configuration.hpp"
#include <ace/Time_Value.h>
#include "scribemonitor.h"
#include "Util.hpp"
#include "stat_opt_word.h"
#include "ConfigManager.h"
#include "Platform/bchar.h"
#include "Platform/bchar_cxx.h"
#include "Platform/bchar_utils.h"
const char TPLID_SEP = ';';
const unsigned int MAX_PARAMED_SQL_LEN = 1024; 

int COnlineTask::open(const char* configfile)
{
    ConfigurationFile config(configfile);
    m_config.assign(configfile);
    ConfigurationSection section;
    if( !config.GetSection("ExVrFetcher",section) ){
        SS_DEBUG((LM_ERROR, "[COnlineTask::open]Cannot find ExVrFetcher config section\n"));
        //exit(-1);
        ACE_Reactor::instance()->end_reactor_event_loop();
        return -1;
    }
    vr_ip           = section.Value<std::string>("VrResourceIp");
    vr_user         = section.Value<std::string>("VrResourceUser");
    vr_pass         = section.Value<std::string>("VrResourcePass");
    vr_db           = section.Value<std::string>("VrResourceDb");
    filter_tplids.reserve(256);
    std::string filter_tplid_str;
    filter_tplid_str = section.Value<std::string>("FilterTplids");
    fprintf(stderr, "[TEST] the FileterTplids is %s\n",filter_tplid_str.c_str());
    int sep_pos = 0;
    int last_sep_pos = 0;
    while((sep_pos = filter_tplid_str.find(TPLID_SEP,sep_pos)) != std::string::npos)
    {
        //fprintf(stderr, "sep_pos %d, last_pos %d, len %d\n",sep_pos,last_sep_pos,sep_pos-last_sep_pos);	
        fprintf(stderr, "[TEST] tplidstr %s, tplidint %d\n",filter_tplid_str.substr(last_sep_pos,sep_pos-last_sep_pos).c_str(),atoi(filter_tplid_str.substr(last_sep_pos,sep_pos-last_sep_pos).c_str()));
        filter_tplids.push_back(atoi(filter_tplid_str.substr(last_sep_pos,sep_pos-last_sep_pos).c_str()));
        last_sep_pos = ++sep_pos;
    }	
    filter_tplids.push_back(atoi(filter_tplid_str.substr(last_sep_pos,filter_tplid_str.length()-last_sep_pos).c_str()));

    //online_interval = section.Value<int>("OnlineInterval");
    //res_priority = section.Value<std::string>("ResourcePriority");

    SS_DEBUG((LM_INFO, "[COnlineTask::open]load vr_resource configiguration: %s %s %s %s\n",
                vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));
	remind_ip = section.Value<std::string>("RemindResourceIp");
	remind_user = section.Value<std::string>("RemindResourceUser"); 
	remind_pass = section.Value<std::string>("RemindResourcePass");
	remind_db = section.Value<std::string>("RemindResourceDb");
	need_changed_vr = section.Value<std::string>("NeedUpdremind");

    pagereader_addr = section.Value<std::string>("XmlPageReaderAddress");
	multiHitXmlreader_addr = section.Value<std::string>("MultiHitXmlReaderAddress");

    return open();  
}

int COnlineTask::open (void*) {
	int threadNum = ConfigManager::instance()->getOnlineTaskNum();
	SS_DEBUG((LM_TRACE, "[COnlineTask::open] thread num: %d\n", threadNum));
    return activate(THR_NEW_LWP, threadNum);
}

int COnlineTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{ 
    return putq(message, timeout);
}


static inline std::string& replace_all_distinct(std::string& str,const std::string& old_value,const std::string& new_value){
    if( 1 == old_value.length() && '\\' == old_value[0] ){
        for(std::string::size_type   pos(0);   pos!=std::string::npos;   pos+=new_value.length())   {
            if(   (pos=str.find(old_value,pos))!=std::string::npos   ){
                if( pos > 0 && (unsigned char)str[pos-1] < 0x80 )
                    str.replace(pos,old_value.length(),new_value);
                else pos ++;
            }else   break;
        }
    }else{
        for(std::string::size_type   pos(0);   pos!=std::string::npos;   pos+=new_value.length())   {
            if(   (pos=str.find(old_value,pos))!=std::string::npos   )
                str.replace(pos,old_value.length(),new_value);
            else   break;
        }
    }
    return   str;
}

int COnlineTask::get_data(CMysql *mysql, Sender *sender, Sender *multiHitSender, sub_request_t  *req)
{
    unsigned int len;
	std::list<online_data_t *>::iterator it;
    
    len = req->uurl_item_list.len;
    for(std::vector<int>::iterator iter = filter_tplids.begin(); iter != filter_tplids.end(); iter++)
    {
    	if(req->tplid == *iter)
    	{
			SS_DEBUG((LM_TRACE,"COnlineTask::get_data(%@): hit filter_tplids %d\n",this, req->tplid));
            req->uurl_item_list.free_list(); 
                        
			return 0;
		}
    }

	Sender *this_sender;
	if(req->class_id > 70000000)
		this_sender = multiHitSender;
	else
		this_sender = sender;

	int doc_num = 0;
    int data_num = 0;
    int content_num = 0;
    int succ_num = 0;
	int row_succ = 0;
	int row_data = 0;
    //int row_cont = 0;
	std::string docid;
	//char docidBuf[32] = {0};
    char buf[1024] = {0};
	int ret;
	ACE_Time_Value t1;
    t1 = ACE_OS::gettimeofday();

    //online_data_t *data = req->uurl_item_list.head;

/*
    while(data != NULL)
    {
        std::string url = data->key;
        char *value = data->value;
        int data_len = data->len;
	
	ret = this_sender->send_to(url.c_str(), value, data_len);
	//SS_DEBUG((LM_TRACE, "COnlineTask::get_data sender send %d %s %d,%s,len=%d,bslen=%d\n", ret, Util::utf8_to_gbk(url).c_str(),req->tplid ,Util::utf8_to_gbk(utf8_val).c_str(),data_len,bcslen((bchar_t*)value)));

	gDocID_t docid;
	url2DocId(url.c_str(),&(docid.id));

	sprintf(docidBuf, "%llx-%llx", docid.id.value.value_high, docid.id.value.value_low);
	SS_DEBUG((LM_TRACE, "COnlineTask::get_data docID=%s docid=%llx-%llx %d\n",
				docidBuf, docid.id.value.value_high, docid.id.value.value_low, req->class_id));

	if( 0 == ret ){
	        sprintf(buf, "update vr_data_%d set data_status = 1 where doc_id = '%s' ", req->data_group, docidBuf);
            SS_DEBUG((LM_DEBUG,"COnlineTask::get_data SQL is %s\n",buf));
            SS_DEBUG((LM_TRACE,"COnlineTask::get_data send item succeed, time cost: %d\n",( ACE_OS::gettimeofday()-t1).usec()));
            if( sql_write( mysql, buf ) ){
                succ_num ++;
                row_succ ++;
            }
	}

	content_num ++;
        row_cont ++;
        data_num ++;
        row_data ++;

        data = data->next;
    }
    req->uurl_item_list.free_list();
    */
   
    if(ACE_Reactor::instance()->reactor_event_loop_done())
    {
        SS_DEBUG((LM_INFO, "COnlineTask::get_data(%@): reactor_event_loop_done\n", this));
        return 1;
    }

	CMysqlQuery data_query(mysql);

	sprintf(buf, "select count(*) as count from vr_data_%d where data_status = 1 and res_id=%d ", req->data_group, req->res_id);
	SS_DEBUG((LM_DEBUG,"COnlineTask::get_data SQL is %s\n",buf));
	alarm(30);
	ret = data_query.execute(buf);
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
            SS_DEBUG(( LM_ERROR,"COnlineTask::get_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",
                        this, buf, mysql->error(), mysql->errorno()));
            return -1;
        }
        alarm(30);
        ret = data_query.execute(buf);
        alarm(0);
	}
	int record_count = data_query.rows();
	for(int irow=0; irow<record_count; irow++)
	{
		CMysqlRow * mysqlrow = data_query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"COnlineTask::get_data(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		if( !mysqlrow->fieldValue("count"))
			row_succ = 0;
		else
			row_succ = atoi(mysqlrow->fieldValue("count"));
	}

	if( row_succ > 0 ){
        time_t now = time(NULL);
        struct tm tm;
        char time_str[64];
        localtime_r(&now, &tm);
        strftime(time_str, sizeof(time_str), "%F %T", &tm);
        sprintf(buf, "update resource_status set res_status = 3, online_date = '%s' where id = %d", time_str, req->status_id);
		SS_DEBUG((LM_TRACE,"COnlineTask::get_data SQL is 1 %s\n",buf));
        sql_write( mysql, buf );
		
		goto out;
	}

	sprintf(buf, "select count(*) as count from vr_data_%d where res_id=%d ", req->data_group, req->res_id);
	SS_DEBUG((LM_DEBUG,"COnlineTask::get_data SQL is %s\n",buf));
	alarm(30);
	ret = data_query.execute(buf);
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
            SS_DEBUG(( LM_ERROR,"COnlineTask::get_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",
                        this, buf, mysql->error(), mysql->errorno()));
            return -1;
        }
        alarm(30);
        ret = data_query.execute(buf);
        alarm(0);
	}
	record_count = data_query.rows();
	for(int irow=0; irow<record_count; irow++)
	{
		CMysqlRow * mysqlrow = data_query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"COnlineTask::get_data(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		if( !mysqlrow->fieldValue("count"))
			row_data = 0;
		else
			row_data = atoi(mysqlrow->fieldValue("count"));
	}

    if(row_data > 0) {
    	sprintf(buf, "select * from vr_data_%d where status_id=%d and res_id=%d and data_status=1 limit 1", req->data_group, req->status_id, req->res_id);
        
		SS_DEBUG((LM_TRACE,"COnlineTask::get_data SQL is 2 %s\n",buf));
        //CMysqlQuery data_query(mysql);
        alarm(30);
        ret = data_query.execute(buf);
        alarm(0);
        while( ret == -1 ) {
            if(mysql->errorno() != CR_SERVER_GONE_ERROR){
                SS_DEBUG(( LM_ERROR,"COnlineTask::get_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",
                            this, buf, mysql->error(), mysql->errorno()));
                return -1;
            }
            alarm(30);
            ret = data_query.execute(buf);
            alarm(0);
        }
        if( 0 == data_query.rows() ){
            sprintf(buf, "update resource_status set res_status = 4 where id = %d", req->status_id);
            sql_write( mysql, buf );
            SS_DEBUG((LM_ERROR, "COnlineTask::process(%@): error: RS[%d], set res_status = 4 \n", this, req->status_id));
        }
    }
out:
    doc_num++;
    
    SS_DEBUG((LM_TRACE,"COnlineTask::get_data(%@): [exstat] vr_resource %d/%d/%d, put xml %d, vr %d\n",
                this, doc_num, data_num, content_num, succ_num, req->class_id));
    return 0;
}


int COnlineTask::svc(){ 
    //bool SelfUpdate = true;  //can change it if you need
    Sender *sender = new Sender(pagereader_addr.c_str());
	Sender *multiHitSender = new Sender(multiHitXmlreader_addr.c_str());

    CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
    int reconn = 0;
    while( reconn < 5 ){
        bool suc = true;
        if( mysql.isClosed() ) {
            SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):Mysql: init fail\n", this));
            suc = false;
        }
        if( !mysql.connect() ) {
            SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
            suc = false;
        }
        if( !mysql.open() ) {
            SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
            suc = false;
        }
        if( suc ){
            CMysqlExecute executor(&mysql);
            executor.execute("set names utf8");
            SS_DEBUG((LM_TRACE, "COnlineTask::svc(%@):Mysql vr success\n",this));
            break;
        }
        reconn ++;
        mysql.close();
        mysql.init();
    }
    if( reconn >=5 ) {
        SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):Mysql: connect vr fail\n", this));
        ACE_Reactor::instance()->end_reactor_event_loop();
        return -1;
    }
	CMysql mysql_remind(remind_ip, remind_user, remind_pass, remind_db, "set names gbk");
	reconn = 0;
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_remind.isClosed() ) {
			SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):mysql_remind: init fail\n", this));
			suc = false;
		}
		if( !mysql_remind.connect() ){
			SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):mysql_remind: connect cr fail: %s\n",this, mysql_remind.error()));
			suc = false;
		}
		if( !mysql_remind.open() ) {
			SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):mysql_remind: open database fail: %s\n",this, mysql_remind.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql_remind);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "COnlineTask::svc(%@):Mysql mysql_remind success\n",this));
			break;
		}
		reconn ++;
		mysql_remind.close();
		mysql_remind.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "COnlineTask::svc(%@):Mysql: connect mysql_remind fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
    SS_DEBUG((LM_TRACE, "COnlineTask::svc(%@): begin to loop %d\n",this, thr_mgr()->thr_self()));
    //Refresh_Timer *timer = new Refresh_Timer;

    for (ACE_Message_Block *msg_blk; getq(msg_blk) != -1; )
    {
        sub_request_t  *req = reinterpret_cast<sub_request_t *>(msg_blk->rd_ptr());

        SS_DEBUG((LM_TRACE, "COnlineTask::svc(%@): online vr class_id:%d status_id(%d) queue_len:%d \n", this, req->class_id, req->status_id, msg_queue()->message_count()));
		//记录第一条url上线的开始时间
		bool is_toupdate = false;
		is_toupdate = needUpdate(&mysql, req->timestamp_id, "online_start_time");
		if(is_toupdate)
			Util::save_timestamp(&mysql, req->timestamp_id, "online_start_time");

		//数据上线
        //get_data(&mysql, sender, multiHitSender, req);
        //SS_DEBUG((LM_TRACE, "COnlineTask::DEBUGYY::svc(%@): online vr class_id:%d status_id(%d) queue_len:%d \n", this, req->class_id, req->status_id, msg_queue()    ->message_count()));

		//update fetch status to fetch end
		Util::update_fetchstatus(&mysql, req->res_id);

		//更新上线结束时间
		Util::save_timestamp(&mysql, req->timestamp_id, "online_end_time");

		//print end log
		ACE_Time_Value end = ACE_OS::gettimeofday();
		time_t cost = end.sec() - req->startTime.sec();
		SS_DEBUG((LM_TRACE, "COnlineTask::svc class_id:%d,cost:%d,url:%s end fetch\n", req->class_id, cost, req->url.c_str()));

		std::string unqi_key;

		char tmp_classid[32];
		snprintf(tmp_classid,32,"%d",req->class_id);
		unqi_key = tmp_classid;
		unqi_key +=  "#" + req->url;
        pthread_mutex_lock(&fetch_map_mutex);//lock
        if( fetch_map.find(unqi_key) != fetch_map.end() && fetch_map[unqi_key] > 0 ){
            fetch_map[unqi_key] = 0;
			SS_DEBUG((LM_TRACE, "COnlineTask::svc class_id:%d, url:%s, process end.\n", req->class_id, req->url.c_str()));
        }else{
            SS_DEBUG(( LM_ERROR, "COnlineTask::svc(%@): map error: %d %s\n",this, fetch_map[unqi_key], req->url.c_str() ));
        }
        pthread_mutex_unlock(&fetch_map_mutex);//unlock
		if(req->is_changed)
		{
			char buf[20] = {0},sqlbuf[512] = {0};
			snprintf(buf,20,"%d",req->class_id);
			if(need_changed_vr.find(buf) !=std::string::npos/* || remind_all.find(buf) != std::string::npos*/)
			{
				{
					if(req->location.length()>0)
					{
						snprintf(sqlbuf,512,"update vr_info set changed=1,res_id=%d,fetch_time=NOW(),remind_time=NOW() where vr_id=%d and location_str='%s';",req->ds_id,req->class_id,req->location.c_str());
					}
					else
					{
						snprintf(sqlbuf,512,"update vr_info set changed=1,res_id=%d,fetch_time=NOW(),remind_time=NOW() where vr_id=%d;",req->ds_id,req->class_id);
					}
				}
                CMysqlExecute executor(&mysql_remind);
                int ret = executor.execute(Util::utf8_to_gbk(sqlbuf).c_str());
                SS_DEBUG((LM_INFO,"COnlineTask::svc execsql=%s,classid=%d,need_changed_vr=%s,ret=%d\n", Util::utf8_to_gbk(sqlbuf).c_str(),req->class_id,need_changed_vr.c_str(), ret ));
			}
		}

		//add by fengxiaoyong for stat_opt_word
		//if(req->class_id >= 70000000) //for multi-hit vr
		//{
			timeval begin_stat_opt, end_stat_opt;
			gettimeofday(&begin_stat_opt, NULL);
			int ret = stat_opt_word(0, req->res_id, &mysql);
			gettimeofday(&end_stat_opt, NULL);
			int stat_opt_time = (end_stat_opt.tv_sec - begin_stat_opt.tv_sec) * 1000 + (end_stat_opt.tv_usec - begin_stat_opt.tv_usec) / 1000;
			SS_DEBUG(( LM_TRACE, "COnlineTask::svc(%@): stat_opt_word(%d)=%d cost=%dms\n",this, req->class_id, ret, stat_opt_time ));
		//}
		
        delete req;
        msg_blk->release();
    }
    if( sender ){
        delete sender;
        sender = NULL;
		delete multiHitSender;
		multiHitSender = NULL;
    }
    SS_DEBUG((LM_TRACE, "COnlineTask::svc(%@): exit from loop\n",this));
    return 0;
}

/*
 *判断online开始时间是否为0，若为0，则返回true，否则返回false
 */
bool needUpdate(CMysql *mysql, int timestamp_id, const std::string &field)
{
	char sql[1024];
	snprintf(sql, 1024, "select %s from process_timestamp where id=%d", field.c_str(), timestamp_id);
	CMysqlQuery query(mysql);
	alarm(30);
	int ret = -1;
	ret = query.execute(sql);
	alarm(0);
	while( ret == -1 ) 
	{
		if(mysql->errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource():Mysql[]failed, reason:[%s],errorno[%d]\n",
				mysql->error(), mysql->errorno()));
			return false;
		}
		alarm(30);
		ret = query.execute(sql);
		alarm(0);
	}
	int record_count = query.rows();
	if(record_count != 1)
		return false;
	CMysqlRow * mysqlrow = query.fetch();
	unsigned long start_time = atol(mysqlrow->fieldValue(0));
	if(start_time == 0)
		return true;
	return false;
}

int COnlineTask::sql_write( CMysql *mysql, const char *sql ){
    CMysqlExecute exe(mysql);
    alarm(30);
    int ret = exe.execute(sql);
    alarm(0);
    while( ret == -1 ) {
        if(mysql->errorno() != CR_SERVER_GONE_ERROR){
            SS_DEBUG((LM_ERROR,"COnlineTask::sql_write(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
                        Util::utf8_to_gbk(sql).c_str(), mysql->error(), mysql->errorno()));
            return -1;
        }
        alarm(30);
        ret = exe.execute(sql);
        alarm(0);
    }
    SS_DEBUG((LM_DEBUG, "COnlineTask::sql_write(%@):Mysql[%s] success\n",this, Util::utf8_to_gbk(sql).c_str() ));
    return ret;
}

int COnlineTask::stopTask() {
    flush();
    thr_mgr()->kill_grp(grp_id(), SIGINT);
    wait();
    return 0;
}



