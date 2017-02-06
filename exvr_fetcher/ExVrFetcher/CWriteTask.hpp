/*
Copyright (c) 2017-2018 Dezhou Shen, Sogou Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef  __CWRITETASK 
#define  __CWRITETASK 

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <vector>
#include <map>

#include <ace/Singleton.h>
#include <ace/Synch.h>
#include <ace/Task.h>
#include <libxml/xmlschemas.h>
#include <libxml/xpath.h>
#include <Platform/bchar.h>
#include <Platform/bchar_cxx.h>
#include <Platform/bchar_utils.h>
#include <Platform/unicode_encoding.h>

#include "database.h"
#include "ExVrFetcher.h"
#include "QdbWriter.hpp"
#include "Sender.hpp"
#include "VrCounter.hpp"
#include "CMysqlHandle.hpp"
#include "CurlHttp.h"
#include "redis_tool.h"

#define MAX_ITEM_LEN   (1024*10)

struct mysql_cost
{
	time_t read_cost;
	time_t write_cost;
	time_t total_cost;
};

struct libxml_cost
{
	time_t readDoc_cost;
	time_t keySearch_cost;
	time_t validate_cost;
	time_t parseXml_cost;
	time_t addProp_cost;
	time_t checkLen_cost;
	time_t createNew_cost;
	time_t total_cost;
};

enum ProcessFlag
{
	ERR,
	UNCHANGED,
	SUCCESS
};
void init_suffix();
void free_suffix();

class CWriteTask: public ACE_Task<ACE_SYNCH> 
{
public:
    static CWriteTask* instance ()
    {
        return ACE_Singleton<CWriteTask, ACE_Recursive_Thread_Mutex>::instance();
    }

    virtual int open (void * = 0);
    virtual int stopTask ();
    virtual int put (ACE_Message_Block *mblk, ACE_Time_Value *timeout = 0);
    virtual int svc (void);
    int open (const char* configfile);

private:
    void addProp(xmlNodePtr root,std::string &index_level);
    bool UTF82GBK(xmlChar * utf8, char ** gbk);
    int get_length( xmlNodePtr node );
    std::string compute_cksum_value(const std::string& str);

    int reload_length_file(int type);
    // return 0 lengths all right.
    // >0 if found length error.
    // <0 if fuction error.
    int check_length(xmlDocPtr doc, int type,std::string &keyword,request_t *req, std::string &errinfo);

    //以下三个函数是对一种特殊的情况做验证处理：
    //<form>
    //    <col content="院校" position="left"/> 
    //    <col content="专业名称" position="left" >
    //        <col2 content="专业名称fuck" position="left" />
    //    </col>
    //    <col content="生源地" position="left"/> 
    //</form> 
    //对form/col/@content的验证是把所有col/@content的长度加起来，加和过程中
    //如果某col有子col2，就取max(col/@content, col/col2/@content)长度求和.
    //最后对求得的和做验证。
    int form_col_content_check_max(xmlNodePtr xml_node);
    int form_col_content_check_sum(xmlNodePtr xml_node);
    int form_col_content_check(const char *one_xpath, xmlXPathContextPtr xpathCtx, int low, int high, std::string &errinfo);

    // return 0 for not found in blacklist.
    int check_blacklist(int class_id, std::string key, CMysql &mysql_vi);                  

    // Cut into ITEMs, write who pass blacklist and schema in mysql.
    int process(request_t *req, std::string &xml, CMysql &mysql_vr, CMysql &mysql_vi, CMysql &mysql_cr, char* sbuf, time_t &preprocess_cost,unsigned int & update_statistic, unsigned int& insertion_statistic, struct mysql_cost &sql_cost, struct libxml_cost &xml_cost, time_t& xml_md5_cost,time_t&insert_update_cost, time_t&qdb_cost ,time_t &schema_check_cost, CMysqlHandle *thisHandler, CurlHttp &curl);

    int process_customer_res(request_t *req, std::string &xml, CMysql &mysql_cr);
    int sql_write( CMysql &mysql, const char *sql, int vi = 0 );
    int sql_read( CMysqlQuery &query, CMysql &mysql, const char *sql, int vi = 0 );
    int load_locationlist(char const * filename,std::map<std::string,int>& loc_map);
    int get_locationID(std::string locName);
    void req_done(request_t *req,CMysql &mysql_vr,int fetch_status, CMysqlHandle *thisHandler, ProcessFlag lflag=SUCCESS,int write_count=0);
	bool needUpdate(CMysql *mysql, int timestamp_id, const std::string &field);
	int delete_redis_set(int res_id, int vr_id, bool is_manual, int data_group, RedisTool *redis_tool);
	int delete_redis_set(std::string set_value, std::string value, RedisTool *redis_tool);
    int m_type_num;
    std::string m_schema_prefix;
    std::string m_schema_suffix;
    std::string m_length_prefix;
    std::string m_length_suffix;
    std::string m_schema_string[TYPE_NUM];
    std::map<int,std::vector<std::string> > m_xpath_map;
    std::map<int,std::vector<int> > m_len_min_map;
    std::map<int,std::vector<int> > m_len_max_map;
    std::map<int,std::string> m_schema_string_map;
    std::map<std::string,int> m_loc_map;
    std::string m_loc_file;

    std::string vr_ip;
    std::string vr_user;
    std::string vr_pass;
    std::string vr_db;
    std::string vi_ip;
    std::string vi_user;
    std::string vi_pass;
    std::string vi_db;

    std::string cr_ip;
    std::string cr_user;
    std::string cr_pass;
    std::string cr_db;
    
    //std::vector<int> m_len_loaded_vec;
    //xmlSchemaValidCtxt* validityContext[TYPE_NUM];
    std::map<int,xmlSchemaValidCtxt*> validityContextMap;
    //xmlSchemaParserCtxt* schemaParser[TYPE_NUM];
    std::map<int,xmlSchemaParserCtxt*> schemaParserMap;
    //xmlSchema* schema[TYPE_NUM];
    std::map<int,xmlSchema*> schemaMap;
    pthread_rwlock_t m_length_rwlock;
    pthread_mutex_t m_validCtxt_mutex;
    std::map<int,time_t> m_len_time_map;
    std::map<int,time_t> m_schema_time_map;
    std::string qdb_addr;
	std::string m_need_urltran;
    const char * configName;	
	RedisTool *redis_tool;
	//std::string unique_set_name;
};

#endif

