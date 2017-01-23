#ifndef PROCESS_HPP
#define PROCESS_HPP

#include <Platform/encoding/URLEncoder.h>
#include <openssl/md5.h>
#include <ace/Time_Value.h>
#include <Platform/encoding/EncodingConvertor.h>
#include <libxml/xmlschemas.h>
#include <libxml/schemasInternals.h>
#include <libxml/xmlschemastypes.h>
#include <libxml/xmlautomata.h>
#include <libxml/xmlregexp.h>
#include <libxml/dict.h>
#include <libxml/encoding.h>
#include <libxml/xmlIO.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/schemasInternals.h>
#include <map>
#include <Platform/log.h>
#include <stdlib.h>
#include <vector>
#include <unordered_set>
#include <sstream>
#include <string.h>
#include <libxml/xpath.h>
#include <vector>
 #include <map>
 #include <ace/Singleton.h>
 #include <ace/Synch.h>
 #include <ace/Task.h>
#include <Platform/docId/docId.h>
#include "sender.hpp"
const static int MAXBUFCOUNT = 1000;
const static std::string CONTENTA = "content_A";
const static std::string CONTENTB = "content_B";
const static char *RULEAB = "AxB";
const static char *RULEABP = "AxBxP";

//item �������
const static int MAXIMEMNUNBER = 1000000;
//urls �������
const static int MAXURLSNUMBER = 50;
const static int MAXURLSITEMNUMBER = 10000;

struct request_t
{
	bool        error;
	bool        over24;
	int         retries;
	int         res_id;
	int         status_id;
	int         data_group;
	int         class_id;
	int         class_no;
	int         attr1, attr2, attr3, rank, attr5, tplid;
	time_t      request_tick;
	std::string tag;
	std::string url;
	std::string xml;
	std::string index_field;
	bool        is_manual;
	int	  		frequency;
	long 	    lastfetch_time;
	int         isNewFrame;
	char        fetch_time[64];
	char        fetch_time_sum[64];
	class       VrCounter* counter_t;
	int 	    litemNum;
	int			data_from;
	int			res_status;
	int			citemNum;//��ǰ����������item�ĸ���(��û���)
	ACE_Time_Value startTime;
	int 		scanTime;
	int			fetchTime;
	int			writeTime;
	int			timestamp_id;//��Ӧprocess_timestamp���е�id
	
    //RULETYPE        ruleType;
    std::string  debugInfo;
	std::string  res_name;
    std::string  classid_str;
	std::string  failedKeyWord; //δͨ��У���key�����ڱ���
	int			failedKeyCount;   //δͨ��У���key�ĸ���
	//std::vector<vr_info *>		vr_info_list;	//vr��url��resource_status���id����Ϣ���б�
	int							current_index;  //��ǰҪץȡ��url���±�
	std::string					extend_content; //��չ�ֶ�
	std::map<std::string, std::string>	annotation_map; //����ֶ�, key:xpath, value: attribute value
	std::map<std::string, std::string>  index_map;      //�����ֶ�, key:xpath, value: attribute value
	std::map<std::string, std::string>	sort_map;		//�����ֶ�, key:xpath, value: attribute value
	std::map<std::string, std::string>	hit_fields;		//�����ֶ�, key:field name, value:xpath
	std::map<std::string, std::string>	second_query_map;	//�����ֶ�, key:xpath, value: attribute value
	std::map<std::string, std::string>	group_map;		//�����У������ֶ�, key:xpath, value: group id
	std::map<std::string,std::string> full_text_map;
	int			ds_id;			//����Դid
	int			ds_priority;	//����Դ���ȼ������ڶ�����Դ����
	std::string	ds_name;		//����Դ����
	//std::map<int, sitemap_info *> sitemap_info_map;		//sitemap list  

    std::string data_format;
    bool vr_flag;

    //reader address
	std::string reader_addr;
	std::string multihit_reader_addr;

	request_t() : error(false), retries(0),request_tick(0),is_manual(false),
	isNewFrame(0),citemNum(0),timestamp_id(-1),debugInfo(),
	failedKeyCount(0),current_index(0)
	{ }
};
int process(request_t *req, std::string &xml);

#endif
