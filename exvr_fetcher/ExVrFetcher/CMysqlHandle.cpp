#include <algorithm>
#include "ace/Message_Block.h"
#include "ace/OS_Memory.h"
#include "ace/Guard_T.h"
#include <ace/Reactor.h>
#include <Platform/log.h>
#include <Platform/encoding/URLEncoder.h>
#include <ace/Time_Value.h>
#include "Configuration.hpp"
#define _USE_TYPE_STR_
#include "CMysqlHandle.hpp"
#include <sstream>
#include "scribemonitor.h"
#include <Platform/log.h>
#include <Platform/encoding/URLEncoder.h> 
#include "COnlineTask.hpp"
#include "Util.hpp"
#include "ConfigManager.h"


int CMysqlHandle::open (void*)
{
	threadNum = ConfigManager::instance()->getmysqlHandleNum();
	return activate(THR_NEW_LWP, threadNum);
}

int CMysqlHandle::open (const char* configfile) 
{
	ConfigurationFile config(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) )
	{
		SS_DEBUG((LM_ERROR, "Cannot find ExVrFetcher config section\n"));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	vr_ip = section.Value<std::string>("VrResourceIp");
	vr_user = section.Value<std::string>("VrResourceUser"); 
	vr_pass = section.Value<std::string>("VrResourcePass");
	vr_db = section.Value<std::string>("VrResourceDb"); 
	SS_DEBUG((LM_INFO, "load vr_resource configiguration: %s %s %s %s\n",
				vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));

	cr_ip = section.Value<std::string>("CrResourceIp");
	cr_user = section.Value<std::string>("CrResourceUser");
	cr_pass = section.Value<std::string>("CrResourcePass");
	cr_db = section.Value<std::string>("CrResourceDb");
	SS_DEBUG((LM_INFO, "load cr_info configiguration: %s %s %s %s\n",
				cr_ip.c_str(), cr_user.c_str(), cr_pass.c_str(), cr_db.c_str() )); 

	m_sender = new Sender(section.Value<std::string>("XmlPageReaderAddress").c_str());
	multiHitSender = new Sender(section.Value<std::string>("MultiHitXmlReaderAddress").c_str());

	return open();  
}

int CMysqlHandle::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
	while(msg_queue()->message_count() > threadNum*100)
	{
		//SS_DEBUG((LM_ERROR,"CMysqlHandle::put sleep queue len %d\n", msg_queue()->message_count()));
		usleep(10000);
	}
	return putq(message, timeout);
}


int CMysqlHandle::sql_read( CMysqlQuery &query, CMysql &mysql, const char *sql, int db_flag)
{
	int ret = query.execute(sql);
	while( ret == -1 ) 
	{
		if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CMysqlHandle::sql_read(%@):Mysql[%s]failed,reason:[%s],errorno[%d]\n",
						this, Util::utf8_to_gbk(sql).c_str(), mysql.error(), mysql.errorno()));
			return -1;
		}
		ret = query.execute(sql);
	}
	SS_DEBUG((LM_DEBUG, "CMysqlHandle::sql_read(%@):Mysql[%s] success %d\n",
				this, Util::utf8_to_gbk(sql).c_str(), query.rows() ));
	return query.rows();
}
int CMysqlHandle::sql_write( CMysql &mysql, const char *sql, int db_flag)
{
	CMysqlExecute exe(&mysql);
	int ret = exe.execute(sql);
	while( ret == -1 ) 
	{
		if(mysql.errorno() != CR_SERVER_GONE_ERROR) {
			SS_DEBUG((LM_ERROR,"CMysqlHandle::sql_write(%@):Mysql[%s]failed,"
						"reason:[%s],errorno[%d]\n",this,
						Util::utf8_to_gbk(sql).c_str(), mysql.error(), mysql.errorno()));
			return -1;
		}
		ret =  exe.execute(sql);
	}
	SS_DEBUG((LM_TRACE, "CMysqlHandle::sql_write(%@):Mysql[%s] success %d\n",this, Util::utf8_to_gbk(sql).c_str(), ret ));
	return ret;
}


int CMysqlHandle::svc() 
{//写qdb以及mysql库
	CMysql mysql_vr(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	int reconn = 0;
	//mysql连接
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_vr.isClosed() ) 
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql_vr.connect() )
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql:"
						"connect vr fail: %s :%s: %s :%s\n",
						this, mysql_vr.error(),vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str()));
			suc = false;
		}
		if( !mysql_vr.open() ) 
		{      
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: open database fail: %s\n",
						this, mysql_vr.error()));
			suc = false;
		}
		if( suc )
		{
			CMysqlExecute executor(&mysql_vr);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CMysqlHandle::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql_vr.close();
		mysql_vr.init();
	}
	if( reconn >=5 ) 
	{//5次连接不上就退出了
		SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: connect vr fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}//vr_data的连接

	CMysql mysql_cr(cr_ip, cr_user, cr_pass, cr_db, "set names gbk");
	reconn = 0;
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_cr.isClosed() ) 
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql_cr.connect() )
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: connect cr fail: %s\n",this, mysql_cr.error()));
			suc = false;
		}
		if( !mysql_cr.open() ) 
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: open database fail: %s\n",this, mysql_cr.error()));
			suc = false;
		}
		if( suc )
		{
			CMysqlExecute executor(&mysql_cr);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "CMysqlHandle::svc(%@):Mysql cr success\n",this));
			break;
		}
		reconn ++;
		mysql_cr.close();
		mysql_cr.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CMysqlHandle::svc(%@):Mysql: connect cr fail\n", this));
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}


	SS_DEBUG((LM_TRACE, "CMysqlhandle::svc(%@): begin to loop\n",this));
	//bool to_update = false;
	int write_num = 0;
	char buf[MAX_SQL_BUF];
    int retmp = 0;
	for(ACE_Message_Block *msg_blk; getq(msg_blk) != -1;) 
	{
		sub_request_t  *req = reinterpret_cast<sub_request_t *>(msg_blk->rd_ptr());
		SS_DEBUG((LM_TRACE, "CMysqlHandle::svc class_id:%d queue_len:%d\n", req->class_id, msg_queue()->message_count()));
		SS_DEBUG((LM_TRACE,"enter CMysqlHandle,res_id=%d\n",req->res_id));
		if(req->is_footer)
		{

			SS_DEBUG((LM_INFO, "CMysqlHandle is footer res_id %d status_id %d\n", req->res_id,req->status_id));
			//if(to_update)
			//{
				int retmp = 0;
				snprintf(buf, MAX_SQL_BUF, "update vr_resource set manual_update = 0 where id = %d", req->res_id);
				if((retmp=sql_write( mysql_vr, buf )) < 0)
				{
					//fprintf(stderr, "fail to update vr_resource\n");
					fprintf(stderr,"CMysqlHandle::fail to update vr_resource"
							"update_statement: %s\n", buf);

				}
				snprintf(buf, MAX_SQL_BUF, "update  resource_status set  md5_sum = '%s'"
						"where id = %d", req->md5_hex.c_str(),req->status_id );
				if((retmp=sql_write( mysql_vr, buf )) < 0)
				{
					SS_DEBUG((LM_ERROR, "CMysqlHandle::process:"
								"Update  resource_status error: %d! "
								"the update_statement is: %s\n", retmp, buf));
				}
				snprintf(buf, MAX_SQL_BUF, "update  vr_resource set query_total = %d,"
						"query_success = %d where id = %d",
						req->item_num, write_num, req->res_id );
				sql_write( mysql_vr, buf );
				snprintf(buf, MAX_SQL_BUF, "update  resource_status set err_info  = '%s'"
						"where id = %d", req->err_info.c_str(), req->status_id);
				sql_write(mysql_vr, buf);
			//}

			if(0 == write_num && 0 == req->item_num)
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_resource set manual_update = 0 where id = %d" , req->res_id);
				sql_write( mysql_vr, buf );
				snprintf(buf, MAX_SQL_BUF, "update resource_status set res_status = 4 where id = %d", req->status_id );
				sql_write( mysql_vr, buf );
				SS_DEBUG((LM_ERROR, "CMysqlHandle::process(%@): error: %d-%d, set res_status = 4 \n", this,req->res_id, req->status_id));
				int retmp;
				if(req->data_from == 4)
				{
					snprintf(buf, MAX_SQL_BUF, "update customer_resource set res_status = 5,check_info = 'no item in xml' where online_id = %d and type  = 2", req->res_id);
					while((retmp = sql_write(mysql_cr, buf, 2)) < 1)
					{

						SS_DEBUG((LM_ERROR, "CMysqlHandle::process(%@)fail to Update "
									"customer_resource resource_status %d update_date error code: %d!\n", this, req->res_id, retmp));
						if( 0 == retmp )
							break;
					}

				}
			}
			if( req->item_num >0 && (float)req->schema_fail_cnt/req->item_num > 0.05f)
			{
				SS_DEBUG((LM_ERROR,"CMysqlHandle::process(%@):ID:%d schema validation:%d of %d failed, higher than 5\%.\n",
							this,req->res_id,req->schema_fail_cnt,req->item_num));
			}

			//save_timestamp(&mysql_vr, req->timestamp_id);
			Util::save_timestamp(&mysql_vr, req->timestamp_id, "write_end_time");
			//send msg to Online
			SS_DEBUG((LM_TRACE,"CMysqlHandle::process begin send to online thread[time:%d.%ds]\n",
						ACE_OS::gettimeofday().sec(), ACE_OS::gettimeofday().usec()));
			ACE_Message_Block *msg=new ACE_Message_Block(reinterpret_cast<char *>(req));
			COnlineTask::instance()->put(msg);

			//to_update = false;
			write_num = 0;
			timeval end_time;
			gettimeofday(&end_time, NULL);
			time_t end_time_usec = end_time.tv_sec * 1000000 + end_time.tv_usec;	
			SS_DEBUG((LM_ERROR,"[CMysqlHandle::Time]process end: res_id: %d,"
						"status_id: %d time: %lld\n", req->res_id,req->status_id, end_time_usec));

			msg_blk->release();
			continue;
		}
		int ret;
		if(req->item != "")
		{
			if(req->class_id >= 70000000)
				ret = multiHitSender->send_to(req->uurl.c_str(), req->item.c_str(), req->item.size());
			else
				ret = m_sender->send_to(req->uurl.c_str(), req->item.c_str(), req->item.size());

			SS_DEBUG((LM_TRACE, "CMysqlHandle::svc send uurl:%s docId:%s vr_id:%d ret:%d\n",
						req->uurl.c_str(), req->docIdbuf.c_str(), req->class_id, ret));

			if( 0 == ret )
			{
				req->data_status = 1;
				snprintf(buf, MAX_SQL_BUF, "update resource_status set res_status = 3 where id = %d", req->status_id );
				sql_write( mysql_vr, buf );
			}
		}

		if(req->updateType == INSERT)
		{
			fprintf(stderr, "CMysqlHandle insert\n");
			if(req->class_id >= 70000000) //for multi-hit vr
			{
				snprintf(buf, MAX_SQL_BUF, "insert into vr_data_%d" 
						"(res_id, status_id, keyword, optword, synonym,data_status,"
						"check_status, create_date, update_date, status, update_type,"
						"valid_status, valid_info,md5_sum,doc_id,new_frame,loc_id, "
						"location, data_source_id, hit_field)"
						"values (%d,%d,'%s','%s','%s',%d,%d,'%s','%s', 1, 1, %d,'%s','%s','%s',%d,%d,'%s', %d, '%s') ",
						req->data_group, req->res_id, req->status_id, req->key.c_str(), 
						req->optword.c_str(),req->synonym.c_str(), req->data_status , req->check_status,
						req->fetch_time.c_str(),req->fetch_time.c_str(),req->valid_status, 
						req->valid_info.c_str(), req->data_md5_hex.c_str(), req->docIdbuf.c_str(),
						req->isNewFrame,req->locID, req->location.c_str(), req->ds_id,
						req->key_values.c_str());
			}
			else
			{
				snprintf(buf, MAX_SQL_BUF, "insert into vr_data_%d" 
						"(res_id, status_id, keyword, optword, synonym,data_status,"
						"check_status, create_date, update_date, status, update_type,"
						"valid_status, valid_info,md5_sum,doc_id,new_frame,loc_id, "
						"location, data_source_id)"
						"values (%d,%d,'%s','%s','%s',%d,%d,'%s','%s', 1, 1, %d,'%s','%s','%s',%d,%d,'%s', %d) ",
						req->data_group, req->res_id, req->status_id, req->key.c_str(), 
						req->optword.c_str(),req->synonym.c_str(), req->data_status , req->check_status,
						req->fetch_time.c_str(),req->fetch_time.c_str(),req->valid_status, 
						req->valid_info.c_str(), req->data_md5_hex.c_str(), req->docIdbuf.c_str(),
						req->isNewFrame,req->locID, req->location.c_str(), req->ds_id);
			}
			//fprintf(stderr, "location %s,TESTTEST %s\n",req->location.c_str(),buf);
		}
		else if(req->updateType == UPDATE1)
		{
			fprintf(stderr, "CMysqlHandle update1\n");
			if(req->class_id >= 70000000)
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"check_status = %d, update_date = '%s', status = 1, update_type=2,"
						"valid_status=%d, valid_info='%s', md5_sum='%s', new_frame = %d, loc_id = %d, location = '%s', "
						"status_id = %d, data_source_id=%d, hit_field='%s', data_status=%d where res_id=%d and doc_id = '%s'", 
						req->data_group, req->optword.c_str(), req->synonym.c_str(), req->key.c_str(), req->check_status, 
						req->fetch_time.c_str(), req->valid_status, req->valid_info.c_str(),
						req->data_md5_hex.c_str(),req->isNewFrame,req->locID, req->location.c_str(),req->status_id, 
						req->ds_id, req->key_values.c_str(),req->data_status, req->res_id, req->docIdbuf.c_str() );
			}
			else
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"check_status = %d, update_date = '%s', status = 1, update_type=2,"
						"valid_status=%d, valid_info='%s', md5_sum='%s', new_frame = %d, loc_id = %d, location = '%s', "
						"status_id = %d, data_source_id=%d , data_status=%d where res_id=%d and doc_id = '%s'", 
						req->data_group, req->optword.c_str(), req->synonym.c_str(), req->key.c_str(), req->check_status, 
						req->fetch_time.c_str(), req->valid_status, req->valid_info.c_str(),
						req->data_md5_hex.c_str(),req->isNewFrame,req->locID, req->location.c_str(),req->status_id, 
						req->ds_id, req->data_status, req->res_id, req->docIdbuf.c_str() );
			}
		}
		else if(req->updateType == UPDATE2)
		{
			fprintf(stderr, "CMysqlHandle update2\n"); 
			if(req->class_id >= 70000000)
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"update_date = '%s', status = 1, update_type=0, valid_status=%d,"
						"valid_info='%s',new_frame = %d, status_id = %d , location = '%s',data_source_id=%d,"
						"hit_field='%s', data_status=%d where res_id=%d and doc_id = '%s'",
						req->data_group, req->optword.c_str(), req->synonym.c_str(), req->key.c_str(),
						req->fetch_time.c_str(), req->valid_status, req->valid_info.c_str(),
						req->isNewFrame, req->status_id, req->location.c_str(),req->ds_id,
						req->key_values.c_str(), req->data_status, req->res_id, req->docIdbuf.c_str() );
			}
			else
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"update_date = '%s', status = 1, update_type=0, valid_status=%d, check_status = %d,"
						"valid_info='%s',new_frame = %d, status_id = %d , location = '%s',data_source_id=%d, data_status=%d"
						" where res_id=%d and doc_id = '%s'",
						req->data_group, req->optword.c_str(), req->synonym.c_str(), req->key.c_str(),
						req->fetch_time.c_str(), req->valid_status,req->check_status,req->valid_info.c_str(),						
						req->isNewFrame, req->status_id, req->location.c_str(),req->ds_id, req->data_status,
						req->res_id, req->docIdbuf.c_str() );
			}
		}
		else if(req->updateType == UPDATE3)
		{
			fprintf(stderr, "CMysqlHandle update3\n");
			if(req->class_id >= 70000000)
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"update_date = '%s', status = 1, update_type=0, valid_status=%d,"
						"valid_info='%s', new_frame = %d, loc_id = %d, location = '%s', status_id = %d,"
						"data_source_id=%d, hit_field='%s', data_status=%d where res_id=%d and doc_id = '%s'"
						, req->data_group, req->optword.c_str(), req->synonym.c_str(), 
						req->key.c_str(), req->fetch_time.c_str(), req->valid_status, req->valid_info.c_str(), 
						req->isNewFrame,req->locID, req->location.c_str(),req->status_id, 
						req->ds_id, req->key_values.c_str(), req->data_status, req->res_id, req->docIdbuf.c_str() );
			}
			else
			{
				snprintf(buf, MAX_SQL_BUF, "update vr_data_%d set optword = '%s', synonym = '%s', keyword='%s',"
						"update_date = '%s', status = 1, update_type=0, valid_status=%d,"
						"valid_info='%s', new_frame = %d, loc_id = %d, location = '%s', status_id = %d, "
						"data_source_id=%d, data_status=%d where res_id=%d and doc_id = '%s'", 
						req->data_group, req->optword.c_str(), req->synonym.c_str(), req->key.c_str(),
						req->fetch_time.c_str(), req->valid_status, req->valid_info.c_str(), 
						req->isNewFrame,req->locID, req->location.c_str(),req->status_id, 
						req->ds_id, req->data_status, req->res_id, req->docIdbuf.c_str() );
			}
		}
        else if(req->updateType == OTHER)
        {
            //大vr 只是用于更新数据库
            if(req->is_large_vr)
            {
                snprintf(buf, MAX_SQL_BUF, "update vr_resource set priority=2 where id=%d", req->res_id);
            }
            else
            {
                goto continue_l;
            }
        }
        else
        {
            goto continue_l; 
        }

		if((retmp=sql_write( mysql_vr, buf )) < 0)
		{
			SS_DEBUG((LM_ERROR, "CMysqlHandle::process(%@):update sql[%s] error: %d!\n", this, Util::utf8_to_gbk(buf).c_str(), retmp));
		}else
		{
			write_num ++;
			//to_update = true;
		}

		
continue_l:
		msg_blk->release();
		delete req;	

	}
	return 0;
}

int CMysqlHandle::stopTask()
{
	flush();
	wait();
	return 0;
}


