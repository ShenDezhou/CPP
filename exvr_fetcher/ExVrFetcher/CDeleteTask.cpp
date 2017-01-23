#include <ace/Reactor.h>
#include "ace/Message_Block.h"
#include <Platform/log.h>
#include "ExVrFetcher.h"
#include "CDeleteTask.hpp"
#include "Refresh_Timer.h"
#include "Configuration.hpp"
#include <ace/Time_Value.h>
#include "scribemonitor.h"
#include "CMysqlHandle.hpp"
#include "stat_opt_word.h"
#include "COnlineTask.hpp"
#include "ConfigManager.h"

int CDeleteTask::open (const char* configfile) {
	ConfigurationFile config(configfile);
	m_config.assign(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) ){
		SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	vr_ip           = section.Value<std::string>("VrResourceIp");
	vr_user         = section.Value<std::string>("VrResourceUser");
	vr_pass         = section.Value<std::string>("VrResourcePass");
	vr_db           = section.Value<std::string>("VrResourceDb");
	spider_type		= section.Value<std::string>("SpiderType");
	SS_DEBUG((LM_INFO, "CDeleteTask load vr_resource configiguration: %s %s %s %s\n",
				vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));

	m_sender = new Sender(section.Value<std::string>("XmlPageReaderAddress").c_str());
	multiHitSender = new Sender(section.Value<std::string>("MultiHitXmlReaderAddress").c_str());

	return open();  
}

int CDeleteTask::open (void*) {
	int threadNum = ConfigManager::instance()->getupdateDeleteNum();
	return activate(THR_NEW_LWP, 1); //can use only one thread 
}

int CDeleteTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
	return putq(message, timeout);
}


int CDeleteTask::set_delete(CMysql *mysql, std::string &conditions)
{
	CMysqlQuery query(mysql);
	std::string sql("select id, vr_type, data_group, update_date from vr_resource as t1 where ");
	sql.append(conditions);

	int ret = -1;
	alarm(30);
	ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CDeleteTask::set_delete(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql.c_str(), mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}
	SS_DEBUG(( LM_TRACE,"CDeleteTask::set_delete(%@):Mysql[%s] success\n",this, sql.c_str() ));

	int record_count = query.rows();
	int doc_num = 0;
	int data_num = 0;
	int del_num = 0;
	char buf[1024];

	SS_DEBUG((LM_TRACE,"CDeleteTask::set_delete(%@): [exstat] found record_count=%d\n",this, record_count ));
	//±é?ú?ù?????ù????vr_resource,
	for(int irow=0; irow<record_count; irow++) {
		int row_data = 0;
		int row_del = 0;
		if(ACE_Reactor::instance()->reactor_event_loop_done()){
			SS_DEBUG((LM_INFO, "CDeleteTask::set_delete(%@): reactor_event_loop_done\n", this));
			return 1;
		}
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"CDeleteTask::set_delete(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		//±é?ú?±?°res_id???????ù????key 
		sql.assign( "select sql_no_cache id, keyword, update_date from vr_data_" );
		sql = sql + mysqlrow->fieldValue("data_group") + " where res_id = " + mysqlrow->fieldValue("id") +" and status = 1";
		CMysqlQuery data_query(mysql);
		alarm(30);
		ret = data_query.execute(sql.c_str());
		alarm(0);
		while( ret == -1 ) {
			if(mysql->errorno() != CR_SERVER_GONE_ERROR){
				SS_DEBUG(( LM_ERROR,"CDeleteTask::set_delete(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
							sql.c_str(), mysql->error(), mysql->errorno() ));
				return -1;
			}
			alarm(30);
			ret = data_query.execute(sql.c_str());
			alarm(0);
		}
		int data_count = data_query.rows();
		SS_DEBUG((LM_DEBUG,"CDeleteTask::set_delete find(%@): %d in %s %s %s\n",this, data_count, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("data_group"), mysqlrow->fieldValue("vr_type") ));
		//±é?ú?±?°res_id???ù??key
		for( int j = 0; j < data_count; j ++ ){
			CMysqlRow * data_row = data_query.fetch();
			if(!data_row){
				SS_DEBUG((LM_ERROR,"CDeleteTask::set_delete(%@): [exstat] get vd row error:%d %s [%s]\n",this,
							j, mysql->error(), mysql->errorno() ));
				break;
			}

			// set delete
			struct tm tm_r, tm_d;
			time_t time_r, time_d;
			//±???resource??update_date??vr_data??update_date???±??
			sscanf(mysqlrow->fieldValue("update_date"), "%d-%d-%d %d:%d:%d", &tm_r.tm_year, &tm_r.tm_mon, &tm_r.tm_mday, 
					&tm_r.tm_hour, &tm_r.tm_min, &tm_r.tm_sec );
			tm_r.tm_year -= 1900;
			tm_r.tm_mon -= 1;
			sscanf(data_row->fieldValue("update_date"), "%d-%d-%d %d:%d:%d", &tm_d.tm_year, &tm_d.tm_mon, &tm_d.tm_mday, 
					&tm_d.tm_hour, &tm_d.tm_min, &tm_d.tm_sec );
			tm_d.tm_year -= 1900;
			tm_d.tm_mon -= 1;
			time_r = mktime(&tm_r);
			time_d = mktime(&tm_d);
			SS_DEBUG(( LM_DEBUG,"CDeleteTask::set_delete(%@):delete[%s %d]:[%s %d]\n",this,
						mysqlrow->fieldValue("update_date"), time_r, data_row->fieldValue("update_date"), time_d ));
			if( time_r > time_d ){
				sprintf(buf, "update vr_data_%s set status = 0 where id = %s", mysqlrow->fieldValue("data_group"), data_row->fieldValue("id") );
				sql_write( mysql, buf );
				SS_DEBUG(( LM_TRACE,"CDeleteTask::set_delete(%@):delete %s.%s.%s[%s|%s] for [%s %d]>[%s %d]\n",this, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("data_group"), data_row->fieldValue("id"), Util::utf8_to_gbk(data_row->fieldValue("keyword")).c_str(), mysqlrow->fieldValue("vr_type"), mysqlrow->fieldValue("update_date"), time_r, data_row->fieldValue("update_date"), time_d ));
				del_num ++;
				row_del ++;
			}
			data_num ++;
			row_data ++;
		}
		if( row_del > 0 )
			SS_DEBUG((LM_TRACE,"CDeleteTask::set_delete(%@): [exstat] vr_resource %s-%s: delete %d []\n",this, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_type"), row_del ));
		doc_num++;
	}
	if(del_num > 0)
		SS_DEBUG((LM_TRACE,"CDeleteTask::set_delete(%@): [exstat] vr_resource %d/%d, delete %d\n",this, doc_num, data_num, del_num ));
	return 0;
}

int CDeleteTask::set_auto_delete(CMysql *mysql, std::string &sql)
{
	CMysqlQuery query(mysql);
	int ret = -1;
	alarm(30);
	ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CDeleteTask::set_auto_delete(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
						mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}

	int record_count = query.rows();
	SS_DEBUG((LM_TRACE,"CDeleteTask::set_auto_delete(%@): [exstat] sql:%s, found record_count=%d\n",this, 
				sql.c_str(), record_count ));
	int doc_num = 0;
	int data_num = 0;
	int del_num = 0;
	char buf[1024];
	for(int irow=0; irow<record_count; irow++)
	{
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"CDeleteTask::set_auto_delete(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}

		int row_data = 0;
		int row_del = 0;
		if(ACE_Reactor::instance()->reactor_event_loop_done()){
			SS_DEBUG((LM_INFO, "CDeleteTask::set_auto_delete(%@): reactor_event_loop_done\n", this));
			return 1;
		}

		sql.assign( "select id, keyword from vr_data_" );
		sql = sql + mysqlrow->fieldValue("data_group") + " where res_id = " + mysqlrow->fieldValue("id") +
			" and status = 1";
		CMysqlQuery data_query(mysql);
		alarm(30);
		ret = data_query.execute(sql.c_str());
		alarm(0);
		while( ret == -1 ) {
			if(mysql->errorno() != CR_SERVER_GONE_ERROR){
				SS_DEBUG(( LM_ERROR,"CDeleteTask::set_auto_delete(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
							sql.c_str(), mysql->error(), mysql->errorno() ));
				return -1;
			}
			alarm(30);
			ret = data_query.execute(sql.c_str());
			alarm(0);
		}
		int data_count = data_query.rows();
		if(data_count > 0){
			SS_DEBUG((LM_DEBUG,"CDeleteTask::set_auto_delete find(%@): %d in %s %s %s\n",this, data_count, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("data_group"), mysqlrow->fieldValue("vr_type") ));
		}	

		for( int j = 0; j < data_count; j ++ ){
			CMysqlRow * data_row = data_query.fetch();
			if(!data_row){
				SS_DEBUG((LM_ERROR,"CDeleteTask::set_auto_delete(%@): [exstat] get vd row error:%d %s [%s]\n",this,
							j, mysql->error(), mysql->errorno() ));
				break;
			}
			sprintf(buf, "update vr_data_%s set status = 0 where id = %s", mysqlrow->fieldValue("data_group"), data_row->fieldValue("id") );
			sql_write( mysql, buf );
			SS_DEBUG(( LM_TRACE,"CDeleteTask::set_auto_delete(%@):set data of offline resource # %s.%s.%s[%s|%s] to be deleted \n",this, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("data_group"), data_row->fieldValue("id"), data_row->fieldValue("keyword"), mysqlrow->fieldValue("vr_type")));
			del_num ++;
			row_del ++;
			data_num ++;
			row_data ++;
		}	
		if( row_del > 0 )
		{

			SS_DEBUG((LM_TRACE,"CDeleteTask::set_auto_delete(%@): [exstat] set vr_resource %s-%s: to be deleted %d []\n",this, mysqlrow->fieldValue("id"), mysqlrow->fieldValue("vr_type"), row_del ));
		}
		doc_num++;
	}
	SS_DEBUG((LM_TRACE,"CDeleteTask::set_auto_delete(%@): [exstat] to delete doc_num=%d\n",this, doc_num ));
	return 0;
}

int CDeleteTask::delete_customer_data(CMysql *mysql)
{
	CMysqlQuery query(mysql);
	std::string sql("select t2.id item_id, t1.update_time, t2.modify_time, t1.id res_id, t2.keyword from customer_resource as t1, customer_item as t2 where t1.id = t2.res_id");

	alarm(30);
	int ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CDeleteTask::delete_customer_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql.c_str(), mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}
	SS_DEBUG(( LM_TRACE,"CDeleteTask::delete_customer_data(%@):Mysql[%s] success\n",this, sql.c_str() ));

	int record_count = query.rows();
	int doc_num = 0;
	int del_num = 0;
	char buf[1024];

	SS_DEBUG((LM_TRACE,"CDeleteTask::delete_customer_data(%@): [exstat] found record_count=%d\n",this, record_count ));
	for(int irow=0; irow<record_count; irow++) {
		//int row_data = 0;
		if(ACE_Reactor::instance()->reactor_event_loop_done()){
			SS_DEBUG((LM_INFO, "CDeleteTask::delete_customer_data(%@): reactor_event_loop_done\n", this));
			return 1;
		}
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"CDeleteTask::delete_customer_data(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}

		char* item_id = mysqlrow->fieldValue("item_id");

		// set delete
		struct tm tm_r, tm_d;
		time_t time_r, time_d;
		sscanf(mysqlrow->fieldValue("update_time"), "%d-%d-%d %d:%d:%d", &tm_r.tm_year, &tm_r.tm_mon, &tm_r.tm_mday, 
				&tm_r.tm_hour, &tm_r.tm_min, &tm_r.tm_sec );
		tm_r.tm_year -= 1900;
		tm_r.tm_mon -= 1;
		sscanf(mysqlrow->fieldValue("modify_time"), "%d-%d-%d %d:%d:%d", &tm_d.tm_year, &tm_d.tm_mon, &tm_d.tm_mday, 
				&tm_d.tm_hour, &tm_d.tm_min, &tm_d.tm_sec );
		tm_d.tm_year -= 1900;
		tm_d.tm_mon -= 1;
		time_r = mktime(&tm_r);
		time_d = mktime(&tm_d);
		//SS_DEBUG(( LM_DEBUG,"CDeleteTask::delete_customer_data(%@):delete[%s %d]:[%s %d]\n",this,
		//			mysqlrow->fieldValue( RS_UPDATE_DATE ), time_r, data_row->fieldValue( VD_UPDATE_DATE ), time_d ));
		if( time_r > time_d ){
			sprintf(buf, "delete from customer_item where id = %s", item_id);
			sql_write( mysql, buf, 1 );
			SS_DEBUG(( LM_TRACE,"CDeleteTask::delete_customer_data(%@):delete res_id:%s, item_id:%s, keyword:%s\n",this, mysqlrow->fieldValue("res_id"), item_id,  Util::utf8_to_gbk(mysqlrow->fieldValue("keyword")).c_str()));
			del_num ++;
		}

		doc_num++;
	}
	if(del_num > 0)
		SS_DEBUG((LM_TRACE,"CDeleteTask::delete_customer_data(%@): [exstat] customer_resource docs:%d delete %d\n",this,  doc_num, del_num ));
	return 0;
}


static char *hexstr(unsigned char *buf,char *des,int len)
{

	const char *set = "0123456789abcdef";
	//static char str[65],*tmp;
	unsigned char *end;
	char *tmp;

	if (len > 32)
		len = 32;

	end = buf + len;
	//tmp = &str[0];
	tmp = des;

	while (buf < end)
	{
		*tmp++ = set[ (*buf) >> 4 ];
		*tmp++ = set[ (*buf) & 0xF ];
		buf ++;
	}

	*tmp = '\0';
	return des;
}

static std::string compute_cksum_value(const std::string& str)
{
	const char* buf = str.c_str();
	std::string ret_str;
	unsigned int a(0),b(0),sum(0);
	const char* pStr = "sogou-search-ods";
	int i = 0;
	while (*buf != '\0')
	{
		if ((*buf >= '0')&&(*buf <= '9'))
			a = (*buf) - '0';
		else if ((*buf >= 'a')&&(*buf <= 'f'))
			a = 10 + (*buf) - 'a';
		else
			return "";
		buf++;
		if (*buf == '\0') return "";
		if ((*buf >= '0')&&(*buf <= '9'))
			b = (*buf) - '0';
		else if ((*buf >= 'a')&&(*buf <= 'f'))
			b = 10 + (*buf) - 'a';
		else
			return "";
		sum = 16*a + b;
		ret_str.append(1,str[(sum+pStr[i++]) % str.size()]);        
		buf++;
	}
	return ret_str;
}

std::string fakeDelXML(std::string vrid,std::string doc_id)
{
	std::string objid = doc_id;
	objid.append(compute_cksum_value(doc_id));
	std::string fxml = "<?xml version=\"1.0\" encoding=\"utf-16\"?>\n<DOCUMENT>\n";
	fxml += "<item><classid>"+vrid+"</classid>\n";
	fxml.append("<unique_url><![CDATA[]]></unique_url>\n");
	fxml += "<objid>"+objid+"</objid>\n";
	fxml.append("<status>DELETE</status>\n");
	fxml.append("</item></DOCUMENT>\n<!--STATUS VR OK-->\n");
	return fxml;
}

std::string fakeXML(std::string vrid,std::string doc_id, std::string fetch_time, std::string status)
{
	std::string objid = doc_id;
	objid.append(compute_cksum_value(doc_id));
	std::string fxml = "<?xml version=\"1.0\" encoding=\"utf-16\"?>\n<DOCUMENT>\n";
	fxml += "<item><classid>"+vrid+"</classid>\n";
	fxml += "<fetch_time>"+fetch_time+"</fetch_time>\n";
	fxml.append("<unique_url><![CDATA[]]></unique_url>\n");
	fxml += "<objid>"+objid+"</objid>\n";
	fxml += "<status>"+status+"</status>\n";
	fxml.append("</item></DOCUMENT>\n<!--STATUS VR OK-->\n");
	return fxml;
}


int CDeleteTask::delete_data(CMysql *mysql, std::string &conditions){
	bool isInter = false;
	CMysqlQuery query(mysql);
	std::string sql("select id, vr_id, vr_type, data_group from vr_resource as t1 where t1.auto_del=1 and ");
	sql.append(conditions);
	alarm(30);
	int ret = query.execute(sql.c_str());
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CDeleteTask::delete_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql.c_str(), mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = query.execute(sql.c_str());
		alarm(0);
	}
	SS_DEBUG(( LM_TRACE,"CDeleteTask::delete_data(%@):Mysql[%s] success, found %d resources to delete\n",this, sql.c_str() ,query.rows())); 

	int record_count = query.rows();
	int doc_num      = 0;
	int data_num     = 0;
	int content_num  = 0;
	int succ_num     = 0;
	const char *docidBuf;

	SS_DEBUG((LM_TRACE,"CDeleteTask::delete_data(%@): [exstat] found record_count=%d\n",this, record_count ));
	for(int irow=0; irow<record_count; irow++) {
		if(ACE_Reactor::instance()->reactor_event_loop_done()){
			SS_DEBUG((LM_INFO, "CDeleteTask::delete_data(%@): reactor_event_loop_done\n", this));
			return 1;
		}

		int ifail_num= 0;
		int row_succ = 0;
		int row_data = 0;
		int row_cont = 0;
		CMysqlRow * mysqlrow = query.fetch();
		if(!mysqlrow){
			SS_DEBUG((LM_ERROR,"CDeleteTask::delete_data(%@): [exstat] get vr row error:%d %s [%s]\n",this,
						irow, mysql->error(), mysql->errorno() ));
			break;
		}
		std::string vr_id_str;
		vr_id_str.assign(mysqlrow->fieldValue("vr_id"));
		int vr_id = atoi(vr_id_str.c_str());
		if(vr_id >= 10000000 and vr_id < 20000000)
			isInter = true;
		else
			isInter = false;
		sql.assign( "select id, res_id, data_status, doc_id from vr_data_" );
		sql = sql + mysqlrow->fieldValue("data_group") + " where res_id = " + mysqlrow->fieldValue("id") + " and status = 0";
		CMysqlQuery data_query(mysql);
		alarm(30);
		ret = data_query.execute(sql.c_str());
		alarm(0);
		while( ret == -1 ) {
			if(mysql->errorno() != CR_SERVER_GONE_ERROR){
				SS_DEBUG(( LM_ERROR,"CDeleteTask::delete_data(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
							sql.c_str(), mysql->error(), mysql->errorno() ));
				//return -1;
				ifail_num=1;
				goto NEXT;
			}
			alarm(30);
			ret = data_query.execute(sql.c_str());
			alarm(0);
		}

NEXT:
		if(ifail_num>0)
			continue;		 

		int data_count = data_query.rows();
		if(data_count > 0){
			SS_DEBUG((LM_DEBUG,"CDeleteTask::delete_data(%@): [exstat] found data_count=%d group=%s res=%s\n",this,
						data_count, mysqlrow->fieldValue("data_group"), mysqlrow->fieldValue("vr_type") ));
		}

		for( int j = 0; j < data_count; j ++ ){
			CMysqlRow * data_row = data_query.fetch();
			if(!data_row){
				SS_DEBUG((LM_ERROR,"CDeleteTask::delete_data(%@): [exstat] get vd row error:%d %s [%s]\n",this,
							j, mysql->error(), mysql->errorno() ));
				break;
			}

			docidBuf = data_row->fieldValue("doc_id");
			if (NULL == strchr(docidBuf,'-'))
				continue;
			std::string xml;//( content_row->fieldValue(VDC_CONTENT) );
			/* xml 自己组装*/
			int gRet = -1;
			SS_DEBUG((LM_DEBUG,"delete docid %s ID %d res %d\n",docidBuf,data_row->fieldValue("id"),data_row->fieldValue("res_id")));

			int ret2 = 0;
			std::string url = "";
			//remove the if condition for feixiao test
			if ((!isInter && gRet > 0) || isInter)
			{
				std::string del_data_sql("delete from vr_data_");
				del_data_sql = del_data_sql + mysqlrow->fieldValue("data_group") + " where id = " + data_row->fieldValue("id");
				if( sql_write( mysql, del_data_sql.c_str() ))
				{	                    
					SS_DEBUG((LM_TRACE,"CDeleteTask::delete_data(%@): [exstat] no data found in offsumdb, just delete vr data and content group=%s res=%s data_id=%d \n",this,
								mysqlrow->fieldValue("data_group"), mysqlrow->fieldValue("vr_type"),data_row->fieldValue("id")));
				}
				if(!isInter && gRet > 0)
				{
					gDocID_t docid;
					char tmpDocIdbuf[32] = {0};
					sscanf(docidBuf,"%llx-%llx",(unsigned long long*)&(docid.id.value.value_high), (unsigned long long*)&(docid.id.value.value_low));
					hexstr((unsigned char*)(&docid.id),tmpDocIdbuf, 16);
					xml = fakeDelXML(vr_id_str,tmpDocIdbuf);
					Util::UTF82UTF16(xml,xml);
					if(vr_id >= 70000000) //for muti-hit vr
						ret2 = multiHitSender->send_to(url.c_str(), xml.c_str(), xml.length() );
					else
						ret2 = m_sender->send_to(url.c_str(), xml.c_str(), xml.length() );
						
					SS_DEBUG((LM_INFO,"CDeleteTask::delete_data(%@) delete fake xml vrid=%s,docid=%s,ret=%d\n",this,vr_id_str.c_str(),docidBuf,ret2));
				}
				continue;
			}
			Util::UTF162UTF8(xml,xml);

			int pos = 0, pos_end = 0;
			
			if( -1 != (pos=xml.find("<unique_url><![CDATA["))  && -1 != (pos_end=xml.find("]]></unique_url>")) ){
				url = xml.substr(pos+strlen("<unique_url><![CDATA["), pos_end-(pos+strlen("<unique_url><![CDATA[")) );
				//SS_DEBUG((LM_ERROR, "unique_url: %s\n", url.c_str() ));
			}else{
				SS_DEBUG((LM_ERROR, "unique_url find fail: %s\n", mysqlrow->fieldValue("vr_type") ));
				continue;
			}
			std::string source;
			if( -1 != (pos=xml.find("<source>"))  && -1 != (pos_end=xml.find("</source>")) ){
				source = xml.substr(pos+strlen("<source>"), pos_end-(pos+strlen("<source>")) );
			}else{
				SS_DEBUG((LM_ERROR, "source find fail: %s\n", mysqlrow->fieldValue("vr_type") ));
				continue;
			}
			if(vr_id < 70000000)
			{
				std::string key;
				if( -1 != (pos=xml.find("<key index_level=\"0\"><![CDATA["))  && -1 != (pos_end=xml.find("]]></key>")) ){
					key = xml.substr(pos+strlen("<key index_level=\"0\"><![CDATA["), pos_end-(pos+strlen("<key index_level=\"0\"><![CDATA[")) );
				}else if( -1 != (pos=xml.find("<key index_level=\"0\">"))  && -1 != (pos_end=xml.find("</key>")) ){
					key = xml.substr(pos+strlen("<key index_level=\"0\">"), pos_end-(pos+strlen("<key index_level=\"0\">")) );
				}else{
					SS_DEBUG((LM_ERROR, "key find fail: %s\n", mysqlrow->fieldValue("vr_type") ));
					continue;
				}
			}
			if((pos=xml.rfind("<status>")) != -1)
			{
				xml.replace(pos+strlen("<status>"),6,std::string("DELETE"));
			}
			else
			{
				if((pos=xml.find("</item>")) != -1)
				{
					xml.insert(pos,"<status>DELETE</status>");
				}
				else
				{
					SS_DEBUG((LM_ERROR,"status find fail for %d : %s\n",data_row->fieldValue("id"),mysqlrow->fieldValue("vr_type")));
					continue;
				}
			}

			if(ACE_Reactor::instance()->reactor_event_loop_done()){
				SS_DEBUG((LM_INFO, "CDeleteTask::delete_onlinedata(%@): reactor_event_loop_done\n", this));
				return 1;
			}
			int ret = 0;
			
			if(data_row->fieldValue("data_status"))  // update to page reader
			{											
				SS_DEBUG((LM_TRACE, "m_writer delete %d %s %s\n", ret, mysqlrow->fieldValue("vr_type"), url.c_str() ));
				Util::UTF82UTF16(xml,xml);

				if(vr_id >= 70000000) //for muti-hit vr
					ret2 = multiHitSender->send_to(url.c_str(), xml.c_str(), xml.length() );
				else
					ret2 = m_sender->send_to(url.c_str(), xml.c_str(), xml.length() );

				SS_DEBUG((LM_TRACE, "m_sender send %d %s %s to be deleted\n", ret2, mysqlrow->fieldValue("vr_type"), url.c_str() ));
			}

			if( (0 == ret || 1 == ret) && 0 == ret2 ){

				std::string del_data_sql("delete from vr_data_");
				del_data_sql = del_data_sql + mysqlrow->fieldValue("data_group") + " where id = " + data_row->fieldValue("id");
				if( sql_write( mysql, del_data_sql.c_str() ))/* && sql_write(mysql,del_content_sql.c_str()))*/
				{
					succ_num ++;
					row_succ ++;
					SS_DEBUG((LM_TRACE,"CDeleteTask::delete_data(%@): [exstat] delete vr data and content group=%s res=%s data_id=%d uniq_url=%s\n",this,
								mysqlrow->fieldValue("data_group"), mysqlrow->fieldValue("vr_type"),data_row->fieldValue("id") ,url.c_str()));
				}
			}
			else
			{
				SS_DEBUG((LM_TRACE,"CDeleteTask::delete_data send delete signal failed "));
			}

			content_num ++;
			row_cont ++;
			//}
			data_num ++;
			row_data ++;
	}
	doc_num++;
}

SS_DEBUG((LM_TRACE,"CDeleteTask::delete_data(%@): [exstat] vr_resource %d/%d/%d, delete xml %d\n",this, doc_num, data_num, content_num, succ_num ));
return 0;
}

int CDeleteTask::svc(){ 
	//bool SelfUpdate = true;  //can change it if you need
	CMysql mysql(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	int reconn = 0;
	while( reconn < 5 ){
		bool suc = true;
		if( mysql.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CDeleteTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql.connect() ) {
			SS_DEBUG((LM_ERROR, "CDeleteTask::svc(%@):Mysql: connect server fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( !mysql.open() ) {
			SS_DEBUG((LM_ERROR, "CDeleteTask::svc(%@):Mysql: open database fail: %s\n",this, mysql.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CDeleteTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql.close();
		mysql.init();
	}
	if( reconn >=5 ) {
		SS_DEBUG((LM_ERROR, "CDeleteTask::svc(%@):Mysql: connect vr fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	
	SS_DEBUG((LM_TRACE, "CDeleteTask::svc(%@): begin to loop\n",this));

	std::string item;
	std::string tmp_item;
	std::string doc_id;
	char vr_id[9] = {0};
	char fetch_time[64];
	char tmpDocIdbuf[32] = {0};
	struct tm tm;
	int ret;
	Sender *this_sender;

	for(ACE_Message_Block *msg_blk; getq(msg_blk) != -1;)
	{
        SS_DEBUG((LM_TRACE, "CDeleteTask::get_resource(%@):delete_task_queue : %d\n",this, CDeleteTask::instance()->msg_queue()->message_count()));
		char csql[MAX_SQL_BUF] = {0};
		VrdeleteData  *req = reinterpret_cast<VrdeleteData *>(msg_blk->rd_ptr());

		bool is_toupdate = false;
		is_toupdate = needUpdate(&mysql, req->timestamp_id, "online_start_time");
		if(is_toupdate)
			Util::save_timestamp(&mysql, req->timestamp_id, "online_start_time");
		
		time_t now = time(NULL);
		localtime_r(&now, &tm);
		snprintf(vr_id, sizeof(vr_id), "%d", req->class_id);
		strftime(fetch_time, sizeof(fetch_time), "%c", &tm);

		if(req->class_id > 70000000)
			this_sender = multiHitSender;
		else
			this_sender = m_sender;
		SS_DEBUG((LM_TRACE, "CDeleteTask::svc in class id %d, url : %s\n", req->class_id, req->url.c_str()));

		if (req->status == "UPDATE")
		{
			SS_DEBUG((LM_TRACE, "CUpdateTask::svc class id %d, vr_data size %d, url : %s\n", req->class_id, req->v_docid.size(), req->url.c_str()));
			for(int i=0; i<req->v_docid.size(); i++)
			{
				item = "";
				tmp_item = "";
				doc_id = req->v_docid[i];

				//拼接xmlc
				gDocID_t docid;
				sscanf(doc_id.c_str(),"%llx-%llx",(unsigned long long*)&(docid.id.value.value_high), (unsigned long long*)&(docid.id.value.value_low));
				hexstr((unsigned char*)(&docid.id),tmpDocIdbuf, 16);
				item = fakeXML(vr_id, tmpDocIdbuf, fetch_time, req->status);
				//SS_DEBUG((LM_TRACE, "CDeleteTask::svc delete xml %s, doc_id %s\n", item.c_str(), doc_id.c_str()));

				Util::UTF82UTF16(item,tmp_item);
				ret = this_sender->send_to(doc_id.c_str(), tmp_item.c_str(), tmp_item.length());
				
				SS_DEBUG((LM_TRACE, "CUpdateTask::svc update docID=%s , class id %d, sent to reader %d\n", doc_id.c_str(), req->class_id, ret));
				if(ret !=0 )
				{
					snprintf(csql, MAX_SQL_BUF, "update vr_data_%d set data_status = 0 where doc_id = '%s' ", req->data_group,  req->v_docid[i].c_str() );
					
					while((ret=sql_write( &mysql, csql )) < 1) 
					{
						SS_DEBUG((LM_ERROR, "CUpdateTask::svc sql[%s] (%@)fail to Update vr_update_info update_date error code: %d!\n", csql, this, ret));
						if( 0 == ret )
							break;
					}
				}

			}
			
			Util::update_fetchstatus(&mysql, req->res_id);
			char tmp_classid[32];
			snprintf(tmp_classid,32,"%d",req->class_id);
			std::string unqi_key;
			unqi_key = tmp_classid;
			unqi_key +=  "#" + req->url;
			pthread_mutex_lock(&fetch_map_mutex);//lock
			if( fetch_map.find(unqi_key) != fetch_map.end() && fetch_map[unqi_key] > 0 ){
			    fetch_map[unqi_key] = 0;
				SS_DEBUG((LM_TRACE, "CUpdateTask::svc class_id:%d, url:%s, process end.\n", req->class_id, req->url.c_str()));
			}else{
			     SS_DEBUG(( LM_ERROR, "CUpdateTask::svc(%@): map error: %d , class_id %d, %s\n",this, fetch_map[unqi_key], req->class_id, req->url.c_str() ));
			}
			pthread_mutex_unlock(&fetch_map_mutex);//unlock			
		}
		else if (req->status == "DELETE")
		{
			SS_DEBUG((LM_TRACE, "CDeleteTask::svc class id %d, vr_data size %d\n", req->class_id, req->v_docid.size()));
			for(int i=0; i<req->v_docid.size(); i++)
			{
				item = "";
				tmp_item = "";
				doc_id = req->v_docid[i];

				//拼接xmlc
				gDocID_t docid;
				sscanf(doc_id.c_str(),"%llx-%llx",(unsigned long long*)&(docid.id.value.value_high), (unsigned long long*)&(docid.id.value.value_low));
				hexstr((unsigned char*)(&docid.id),tmpDocIdbuf, 16);
				item = fakeXML(vr_id, tmpDocIdbuf, fetch_time, req->status);
				//SS_DEBUG((LM_TRACE, "CDeleteTask::svc delete xml %s, doc_id %s\n", item.c_str(), doc_id.c_str()));

				Util::UTF82UTF16(item,tmp_item);
				ret = this_sender->send_to(doc_id.c_str(), tmp_item.c_str(), tmp_item.length());
				
				SS_DEBUG((LM_TRACE, "CDeleteTask::svc Task::svc delete docID=%s , class id %d, sent to reader %d\n", doc_id.c_str(), req->class_id, ret));

				if(ret ==0 )
				{
					snprintf(csql, MAX_SQL_BUF, "delete from vr_data_%d where doc_id='%s'", req->data_group, doc_id.c_str());
				}
				else
				{
					snprintf(csql, MAX_SQL_BUF, "update vr_data_%d set data_status = 0 where doc_id = '%s' ", req->data_group,  req->v_docid[i].c_str() );
				}

				while((ret=sql_write( &mysql, csql )) < 1) 
		        {
		            SS_DEBUG((LM_ERROR, "CDeleteTask::svc sql[%s] (%@)fail to Update vr_update_info update_date error code: %d!\n", csql, this, ret));
		            if( 0 == ret )
						break;
		        }
			}
			SS_DEBUG((LM_TRACE, "CDeleteTask::svc delete docID=%s , class id %d\n", doc_id.c_str(), req->class_id ));

		}
		else
		{
			SS_DEBUG((LM_ERROR, "CDeleteTask::svc  vr_id %s, req status error, status %s\n", req->class_id, req->status.c_str() ));
		}

		std::string sql = "delete from vr_update_info where update_time < date_sub(curdate() ,INTERVAL 2 DAY);";
		while((ret=sql_write( &mysql, sql.c_str() )) < 1) 
		{
			SS_DEBUG((LM_ERROR, "CDelete sql[%s] (%@)fail to Update vr_update_info update_date error code: %d!\n", sql.c_str(), this, ret));
			if( 0 == ret )
				break;
		}
		
		Util::save_timestamp(&mysql, req->timestamp_id, "online_end_time");

		timeval begin_stat_opt, end_stat_opt;
		gettimeofday(&begin_stat_opt, NULL);
		int ret = stat_opt_word(0, req->res_id, &mysql);
		gettimeofday(&end_stat_opt, NULL);
		int stat_opt_time = (end_stat_opt.tv_sec - begin_stat_opt.tv_sec) * 1000 + (end_stat_opt.tv_usec - begin_stat_opt.tv_usec) / 1000;
		SS_DEBUG(( LM_TRACE, "CDeleteTask::svc(%@): stat_opt_word(%d)=%d cost=%dms, vr_id %d\n",this, req->class_id, ret, stat_opt_time, req->class_id ));

		msg_blk->release();
		delete req;
	}

	SS_DEBUG((LM_TRACE, "CDeleteTask::svc(%@): exit from loop\n",this));
	return 0;
}

int CDeleteTask::sql_write( CMysql *mysql, const char *sql, int cr ){
	CMysqlExecute exe(mysql);
	alarm(30);
	int ret = exe.execute(sql);
	alarm(0);
	while( ret == -1 ) {
		if(mysql->errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CDeleteTask::sql_write(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql, mysql->error(), mysql->errorno()));
			return -1;
		}
		alarm(30);
		ret = exe.execute(sql);
		alarm(0);
	}
	SS_DEBUG((LM_DEBUG, "CDeleteTask::sql_write(%@):Mysql[%s] success\n",this, sql ));
	return ret;
}

int CDeleteTask::stopTask() {

	flush();
	thr_mgr()->kill_grp(grp_id(), SIGINT);
	wait();
	delete m_sender;
	delete multiHitSender;
	return 0;
}

