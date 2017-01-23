#ifndef _EXVRFETCHER_H_
#define _EXVRFETCHER_H_
#include <string>
#include <map>
#include <ace/Thread.h>
#include "VrCounter.hpp"
#include <vector>
#include <set>

extern std::map<std::string, int> fetch_map;
extern int _mem_check;
extern ACE_thread_t thr_id_scan;
//extern std::map<int, int> fetch_flag_map;
//extern std::map<int,int> update_flag_map;
extern pthread_mutex_t  fetch_map_mutex;
//extern pthread_mutex_t  fetch_flag_mutex;
//extern pthread_mutex_t  update_flag_mutex;
extern pthread_mutex_t  counter_mutexes[10];

#define VrSpider_Monitor "cacti_vr_spider_monitor"

/*
vr_resource: t1,  resource_status: t2
 */

enum RULETYPE
{
    PLAIN = 0,
    AB = 1,
    MHIT = 2
};

enum UpdateType
{
	INSERT  = 0,
	UPDATE1  = 1,
	UPDATE2 = 2,
	UPDATE3 = 3,
    OTHER     //其他情况 e.g.大vr
};

enum SResourceStatusField{
	SRS_ID ,
	SRS_RES_ID,
	SRS_URL,
	SRS_RES_STATUS,
	SRS_ONLINE_DATE,
	SRS_MODIFY_DATE,
	SRS_START_TIME,
	SRS_ERR_INFO,
	SRS_MD5_SUM,
	SMANUAL_UPDATE,
	SRS_SUB_DATE
};

enum VrResourceField{
	VR_ID ,              
	VR_USER_ID,         
	VR_RES_NAME,        
	VR_TEMPLATE, 
	VR_CLASS_NO, 
	VR_CLASS_ID, 
	VR_TYPE, 
	VR_ATTR1,
	VR_ATTR2,
	VR_ATTR3,
	VR_ATTR4,
	VR_ATTR5,
	VR_DATA_GROUP,
	VR_UPDATE_DATE,
	VR_FETCH_TIME,
	VR_FREQUENCY,
	MANUAL_UPDATE,
	VR_RANK, 
	DATA_FORMAT,            
	VR_STATUS,      
	AUTO_CHECK,     
	NEW_FRAMEWORK, 
	REGION, 
	DUPFILT_TYPE,
	AUTO_DEL,
	VR_PRIORITY, 
	VR_INFO,
	VR_QUERY_TOTAL, 
	VR_QUERY_SUCCESS, 
	VR_PV, 
	VR_CLICK, 
	VR_INDEX_FIELD,    
	VR_CHECKER,
	VR_DATA_FROM,
	VR_USEQO,
	VR_CRWAL_INTER,
	VR_FLAG,
	VR_END
};

enum ResourceStatusField{
	RS_ID = VR_END,             
	RS_RES_ID,
	RS_URL,
	RS_RES_STATUS,
	RS_ONLINE_DATE,
	RS_MODIFY_DATE,
	RS_START_TIME,
	RS_ERR_INFO,
	RS_MD5_SUM,
	//         MANUAL_UPDATE,
	RS_SUB_DATE,
	RS_END  
};

enum VrDataField{
	VD_ID,
	VD_RES_ID,
	VD_STATUS_ID,
	VD_KEYWORD,
	VD_OPTWORD,
	VD_SYNONYM,
	VD_DATA_STATUS,
	VD_CHECK_STATUS,
	VD_CREATE_DATE,
	VD_MODIFY_TIME,
	VD_UPDATE_DATE,
	VD_STATUS,
	VD_UPDATE_TYPE,
	VD_VALID_STATUS,
	VD_VALID_INFO,
	VD_MD5_SUM,
	VD_DOC_ID,
	VD_NEW_FRAME,
	VD_LOC_ID,   
	VD_LOC,
	VD_END
};
enum CustomerResourceField
{
	CR_ID,
	CR_RES_NAME,
	CR_USER_NAME,
	CR_PRIORITY,
	CR_TMPLATE_ID,
	CR_TYPE,
	CR_ALWAYS_VALID,
	CR_INVALID_TIME,
	CR_URL,
	CR_AUTO_UPDATE,
	CR_START_TIME,
	CR_FREQUENCY,
	CR_CREATE_TIME,
	CR_MODIFY_TIME,
	CR_FETCH_TIME,
	CR_UPDATE_TIME,
	CR_MD5_SUM,
	CR_RES_STATUS,
	CR_MANUAL_UPDATE,
	CR_INFO,
	CR_CHECK_INFO,
	CR_CHECK_TIME,
	CR_ONLINE_ID,
	CR_CHECKER,
	CR_END
};

enum CustomerItemField
{
	CD_ID,
	CD_RES_ID,
	CD_KEYWORD,
	CD_XML,
	CD_CHECK_STATUS,
	//CD_CHECKER,
	CD_CREATE_TIME,
	CD_MODIFY_TIME,
	CD_MD5_SUM,
	CD_END
};
enum VrDataContentField{
	VDC_ID,
	VDC_DATA_ID,
	VDC_CONTENT,
	VDC_END
};
enum VrInfoField{
	VI_TYPE,
	VI_TEMPLATE,
	VI_END
};

enum ClassTag{
	INTERNAL_IMAGE_ALL,
	INTERNAL_IMAGE_STAR,
	INTERNAL_VIDEO_ALL,
	INTERNAL_VIDEO_SERIES,
	INTERNAL_VIDEO_SEASON,
	INTERNAL_MUSIC_ALL,
	INTERNAL_MUSIC_SINGER,
	INTERNAL_MUSIC_SONG,
	INTERNAL_MUSIC_ALBUM,
	INTERNAL_NEWS_ALL,
	INTERNAL_MAP_ALL,
	INTERNAL_BBS_NOVEL,
	EXTERNAL_HAODAIFU_HOSPITAL,
	EXTERNAL_HAODAIFU_FACULTY,
	EXTERNAL_HAODAIFU_DOCTOR,
	EXTERNAL_HUDONG_ALL,
	INTERNAL_VIDEO_MOVIE,
	EXTERNAL_SOHU_STAR,
	EXTERNAL_DOUBAN_BOOK,
	EXTERNAL_TVMAO_PROGRAM,
	EXTERNAL_XUNLEI_DOWNLOAD,
	EXTERNAL_MTIME_THEATER,
	EXTERNAL_TVMAO_STATION,
	EXTERNAL_LETU_SCENIC,
	EXTERNAL_AIBANG_ALL,
	EXTERNAL_IMOBILE_ALL,
	EXTERNAL_TVMAO_CHANNEL,
	EXTERNAL_HUDONG_HOT,
	EXTERNAL_MTIME_PLAYING,
	EXTERNAL_MTIME_COMING,
	EXTERNAL_DAODAO_ALL,
	EXTERNAL_GOUMIN_ALL,
	EXTERNAL_TRAIN_CHECI,
	EXTERNAL_TRAIN_ZHONGZHUAN,
	EXTERNAL_KUXUN_PLANE,
	EXTERNAL_TRAIN_ZHIDA,
	EXTERNAL_CAIPIAO_ALL,
	EXTERNAL_YAOLAN_ALL,
	EXTERNAL_YINGJIESHENG_ALL,
	EXTERNAL_TAOBAO_ALL,
	EXTERNAL_AD_ALL,
	EXTERNAL_XUNLEI_CARTOON,
	EXTERNAL_LILV_ALL,
	EXTERNAL_KUXUN_FLIGHT,
	EXTERNAL_KUXUN_HOTEL,
	EXTERNAL_58TONGCHENG_TRAIN,
	EXTERNAL_XUNLEI_MANU,
	EXTERNAL_GANJI_TRAIN,
	EXTERNAL_DOUBAN_MOVIE,
	EXTERNAL_SHIBO_TICKET,
	EXTERNAL_SOHU_CAREXHIBITION,
	EXTERNAL_CAIPIAO_EXP,
	INTERNAL_MAPMUL_EXP,
	INTERNAL_MAPSIN_EXP,
	INTERNAL_MAPCITY_EXP,
	EXTERNAL_MARKS_EXP,
	EXTERNAL_17173_EXP,
	EXTERNAL_KUFU_NET,
	EXTERNAL_SOHU_XINGZUOEXP,
	FIXRANK_WEB_ALL,
	//EXTERNAL_NBASAICHENG_EXP,
	//EXTERNAL_NBAQIUDUI_EXP,
	EXTERNAL_SHIBO_TICKET2,
	EXTERNAL_XUANXUEXIAO_CLASS,
	EXTERNAL_SOHU_AUTOTYPE,
	EXTERNAL_TTMEISHI_COOKBOOK,
	EXTERNAL_HAICI_CIZU,
	EXTERNAL_HAICI_CHANGCI,
	EXTERNAL_HAICI_DUANCI,
	EXTERNAL_SOHU_AUTOPRICE,
	EXTERNAL_SHIBO_CHANGGUAN,
	INTERNAL_VIDEO_TEST2,
	INTERNAL_VIDEO_TEST1,
	EXTERNAL_BAIKE_TEST,
	EXTERNAL_BOKE_TEST,
	INTERNAL_MUSIC_QUFENG1,
	INTERNAL_MUSIC_QUFENG2,
	INTERNAL_MUSIC_QUFENG3,
	INTERNAL_MAP_PLACE,
	INTERNAL_MAP_CITY,

	TYPE_NUM = 1024 
};   

/*#ifdef _USE_TYPE_STR_
  static const char* type_str[] = {
  "INTERNAL.IMAGE.ALL",
  "INTERNAL.IMAGE.STAR",
  "INTERNAL.VIDEO.ALL",
  "INTERNAL.VIDEO.SERIES",
  "INTERNAL.VIDEO.SEASON",
  "INTERNAL.MUSIC.ALL",
  "INTERNAL.MUSIC.SINGER",
  "INTERNAL.MUSIC.SONG",
  "INTERNAL.MUSIC.ALBUM",
  "INTERNAL.NEWS.ALL",
  "INTERNAL.MAP.ALL",
  "INTERNAL.BBS.NOVEL",
  "EXTERNAL.HAODAIFU.HOSPITAL",
  "EXTERNAL.HAODAIFU.FACULTY",
  "EXTERNAL.HAODAIFU.DOCTOR",
  "EXTERNAL.HUDONG.ALL",
  "INTERNAL.VIDEO.MOVIE",
  "EXTERNAL.SOHU.STAR",
  "EXTERNAL.DOUBAN.BOOK",
  "EXTERNAL.TVMAO.PROGRAM",
  "EXTERNAL.XUNLEI.DOWNLOAD",
  "EXTERNAL.MTIME.THEATER",
  "EXTERNAL.TVMAO.STATION",
  "EXTERNAL.LETU.SCENIC",
  "EXTERNAL.AIBANG.ALL",
  "EXTERNAL.IMOBILE.ALL",
  "EXTERNAL.TVMAO.CHANNEL",
  "EXTERNAL.HUDONG.HOT",
  "EXTERNAL.MTIME.PLAYING",
  "EXTERNAL.MTIME.COMING",
  "EXTERNAL.DAODAO.ALL",
  "EXTERNAL.GOUMIN.ALL",
  "EXTERNAL.TRAIN.CHECI",
  "EXTERNAL.TRAIN.ZHONGZHUAN",
  "EXTERNAL.KUXUN.PLANE",
  "EXTERNAL.TRAIN.ZHIDA",
  "EXTERNAL.CAIPIAO.ALL",
  "EXTERNAL.YAOLAN.ALL",
  "EXTERNAL.YINGJIESHENG.ALL",
  "EXTERNAL.TAOBAO.ALL",
  "EXTERNAL.AD.ALL",
  "EXTERNAL.XUNLEI.CARTOON",
  "EXTERNAL.LILV.ALL",
  "EXTERNAL.KUXUN.FLIGHT",
  "EXTERNAL.KUXUN.HOTEL",
  "EXTERNAL.58TONGCHENG.TRAIN",
  "EXTERNAL.XUNLEI.MANU",
  "EXTERNAL.GANJI.TRAIN",
  "EXTERNAL.DOUBAN.MOVIE",
  "EXTERNAL.SHIBO.TICKET",
  "EXTERNAL.SOHU.CAREXHIBITION",
  "EXTERNAL.CAIPIAO.EXP",
  "INTERNAL.MAPMUL.EXP",
  "INTERNAL.MAPSIN.EXP",
  "INTERNAL.MAPCITY.EXP",
  "EXTERNAL.MARKS.EXP",
  "EXTERNAL.17173.EXP",
  "EXTERNAL.KUFU.NET",
  "EXTERNAL.SOHU.XINGZUOEXP",
  "FIXRANK.WEB.ALL",
//"EXTERNAL.NBASAICHENG.EXP",
//"EXTERNAL.NBAQIUDUI.EXP",
"EXTERNAL.SHIBO.TICKET2",
"EXTERNAL.XUANXUEXIAO.CLASS",
"EXTERNAL.SOHU.AUTOTYPE",
"EXTERNAL.TTMEISHI.COOKBOOK",
"EXTERNAL.HAICI.CIZU",
"EXTERNAL.HAICI.CHANGCI",
"EXTERNAL.HAICI.DUANCI",
"EXTERNAL.SOHU.AUTOPRICE",
	"EXTERNAL.SHIBO.CHANGGUAN",
	"INTERNAL.VIDEO.TEST2",
	"INTERNAL.VIDEO.TEST1",
	"EXTERNAL.BAIKE.TEST",
	"EXTERNAL.BOKE.TEST",
	"INTERNAL.MUSIC.QUFENG1",
	"INTERNAL.MUSIC.QUFENG2",
	"INTERNAL.MUSIC.QUFENG3",
	"INTERNAL.MAP.PLACE",
	"INTERNAL.MAP.CITY"
	};
#endif

enum Tags{
	KEY,
	UNIQUE_URL,
	CLASSTAG,
	DISPLAY,
	URL,
	TITLE,
	SHOWURL,
	PAGESIZE,
	CONTENT1,
	TAGS_END
};
#ifdef _USE_TAG_STR_
static const char* tag_str[] = { "key", "unique_url", "classtag", "display", "url", "title", "showurl", "pagesize", "content1" };
#endif
*/

/**
 *site map info
 */
struct sitemap_info {
	sitemap_info(int ds_id, int ds_priority, const std::string &name, const std::string &url, char *md5) : 
			ds_id_(ds_id), ds_priority_(ds_priority), ds_name_(name), url_(url), retries(0) {
		if(md5 == NULL)
		{
			md5_[0] = '\0';
		}
		else
		{
			strncpy(md5_, md5, 33);
		}
	}

	int				ds_id_;			//数据源id
	int				ds_priority_;	//数据源优先级，用于多数据源排序 
	std::string		ds_name_;
	std::string		url_;			//sitemap地址
	char			md5_[33];		//sitemap内容签名
	int				retries;		//抓取重试次数
};

/**
 * data source info
 **/
struct data_source {
	data_source(int priority, const std::string &name) :
			ds_priority(priority), ds_name(name) {}
	int				ds_priority;	//数据源优先级，用于多数据源排序
	std::string		ds_name;		//数据源名称
};

/**
 *vr的url等其它信息
 **/
struct vr_info {
	std::string		url;			//url
	int				status_id;		//resource_status表的id
	int				retries;		//抓取重试次数
	time_t      	request_tick;	//抓取时间,用于抓取时间间隔控制
	int				ds_id;			//数据源id
	int				ds_priority;	//数据源优先级，用于多数据源排序 
	std::string		ds_name;		//数据源名称
	std::string     unqi_key;

	vr_info(std::string &u, int si, int datasource_id=0) : url(u), status_id(si), retries(0),
			ds_id(datasource_id), ds_priority(-1)
	{}
};

struct request_t{
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
	bool is_db_error;
	int	  		frequency;
	long 	    lastfetch_time;
	int         isNewFrame;
	char        fetch_time[64];
	char        fetch_time_sum[64];
	class       VrCounter* counter_t;
	int 	    litemNum;
	int			data_from;
	int			res_status;
	int			citemNum;//当前解析出来的item的个数(还没入库)
	ACE_Time_Value startTime;
	int 		scanTime;
	int			fetchTime;
	int			writeTime;
	int			timestamp_id;//对应process_timestamp表中的id
	
    RULETYPE        ruleType;
    std::string  debugInfo;
	std::string  res_name;
    std::string  classid_str;
	std::string  failedKeyWord; //未通过校验的key，用于报警
	int			failedKeyCount;   //未通过校验的key的个数
	std::vector<vr_info *>		vr_info_list;	//vr的url，resource_status表的id等信息的列表
	int							current_index;  //当前要抓取的url的下标
	std::string					extend_content; //扩展字段
	std::map<std::string, std::string>	annotation_map; //标红字段, key:xpath, value: attribute value
	std::map<std::string, std::string>  index_map;      //索引字段, key:xpath, value: attribute value
	std::map<std::string, std::string>	sort_map;		//排序字段, key:xpath, value: attribute value
	std::map<std::string, std::string>	hit_fields;		//命中字段, key:field name, value:xpath
	std::map<std::string, std::string>	second_query_map;	//命中字段, key:xpath, value: attribute value
	std::map<std::string, std::string>	group_map;		//多命中，分组字段, key:xpath, value: group id
	std::map<std::string, std::string>	url_transform_map;		//多命中，url transform, key:xpath, value: attribute value
	std::map<std::string, std::string> full_text_map;		//用于全文检索打标记
	int			ds_id;			//数据源id
	int			ds_priority;	//数据源优先级，用于多数据源排序
	std::string	ds_name;		//数据源名称
	std::map<int, sitemap_info *> sitemap_info_map;		//sitemap list  

    std::string data_format;
    bool vr_flag;
	bool is_changed;
	std::set<std::string> url_uniq_set;    //用于滤重同一类VR中重复的url
    
    // determine xml_format status
    bool xml_format_status;
    
	request_t() : error(false), retries(0),request_tick(0),is_manual(false),
	isNewFrame(0),citemNum(0),timestamp_id(-1),debugInfo(),
	failedKeyCount(0),current_index(0), xml_format_status(true),is_db_error(false)
	{ 
		full_text_map.clear();
		url_uniq_set.clear();
	}

	//自定义构造函数, 基类部分调用基类的复制构造函数初始化
	request_t(const request_t &req, const std::string &xml, vr_info &vi) : 
		error(req.error), over24(req.over24), res_id(req.res_id), status_id(vi.status_id), data_group(req.data_group),
		class_id(req.class_id), class_no(req.class_no), attr1(req.attr1), attr2(req.attr2),
		attr3(req.attr3), rank(req.rank), attr5(req.attr5), tplid(req.tplid), tag(req.tag),
		url(vi.url), xml(xml),index_field(req.index_field), is_manual(req.is_manual), frequency(req.frequency),
		lastfetch_time(req.lastfetch_time), isNewFrame(req.isNewFrame), counter_t(req.counter_t),
		litemNum(req.litemNum), data_from(req.data_from), res_status(req.res_status),
		citemNum(req.citemNum), startTime(req.startTime), scanTime(req.scanTime),
		timestamp_id(req.timestamp_id), debugInfo(), res_name(req.res_name), classid_str(req.classid_str),
		failedKeyCount(0), current_index(0), extend_content(req.extend_content),
		annotation_map(req.annotation_map), index_map(req.index_map), sort_map(req.sort_map),
		hit_fields(req.hit_fields), second_query_map(req.second_query_map), group_map(req.group_map), 
		url_transform_map(req.url_transform_map),full_text_map(req.full_text_map),ds_id(vi.ds_id), ds_priority(vi.ds_priority), 
		ds_name(vi.ds_name), data_format(req.data_format),is_changed(req.is_changed),url_uniq_set(req.url_uniq_set), xml_format_status(req.xml_format_status),
		is_db_error(req.is_db_error)
	{
		strncpy(fetch_time, req.fetch_time, 64);
		strncpy(fetch_time_sum, req.fetch_time_sum, 64);
		debugInfo.append(req.tag);
		debugInfo.append(":");
		debugInfo.append(vi.url);
		debugInfo.append(" ");
	}

	~request_t()
	{
		//if(counter_t != NULL)
		//	counter_t->minus();
		std::vector<vr_info *>::iterator itr;
		for(itr=vr_info_list.begin(); itr!=vr_info_list.end(); ++itr)
			delete *itr;
		vr_info_list.clear();

		std::map<int, sitemap_info *>::iterator s_itr;
		for(s_itr=sitemap_info_map.begin(); s_itr!=sitemap_info_map.end(); ++s_itr)
			delete s_itr->second;
		sitemap_info_map.clear();
	}
};

typedef struct online_data_t
{
public:
    online_data_t(std::string _key, std::string _value) : key(_key)
    {
        len = _value.size();
        value = (char *)malloc((len)*sizeof(char));
        memcpy(value, _value.c_str(),len);
    }

    ~online_data_t()
    {
        free(value);
        value = NULL;
    }

    std::string key;
    int len;
    char *value;
    struct online_data_t *next;
}online_data_t;

typedef struct list_head
{
    online_data_t *head;
    unsigned int len;

    list_head():head(NULL), len(0)
    {}

    void free_list()
    {
        online_data_t *current, *next;
        current = head;
        while(len--)
        {
            next = current->next;
            if(current)
                delete current;
            current = next;
        }

        head = NULL;
    }

    void add_node(std::string _key, std::string _value)
    {
        online_data_t *node = new online_data_t(_key, _value);
        if(0 ==len)
        {
            node->next = NULL;
            head = node;
        }
        else
        {
            node->next = head->next;
            head->next = node;
        }

        len++;
    }
}list_head;

struct sub_request_t
{
	int res_id;
	int status_id;
	int data_group;
	int check_status;
	int loc_id;
	int data_status; 
	int item_num;       
	int data_from; 
	int schema_fail_cnt;
	int tplid;

	std::string uurl;
	std::string key;
	std::string optword;
	std::string synonym;
	std::string valid_info;	
	std::string md5_hex;
	std::string location;
	std::string url;
	std::string item;

	std::string err_info; 
	std::string data_md5_hex;
	std::string docIdbuf; 
	int valid_status;
	bool is_manual;
	bool is_footer;
    bool is_large_vr;
	int  isNewFrame;
	bool is_changed;
	int locID; 
	std::string  fetch_time;
	int  res_status;
	int  timestamp_id;//对应process_timestamp表中的id
	UpdateType updateType; 
	int			ds_id;			//数据源id
	std::string	key_values;		//key and values, key1::value1||key2::value2
	int class_id;
    struct list_head uurl_item_list;
	ACE_Time_Value startTime;

	sub_request_t() :is_manual(false),is_large_vr(false),isNewFrame(0),is_changed(false), timestamp_id(-1) 
    {
    }

	~sub_request_t()
	{
	}
};

#endif

