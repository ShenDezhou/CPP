#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <libxml/parser.h>
#include <libxml/xmlreader.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include <map>
#include "database.h"
#include "CurlHttp.h"
#include "JsonObject.h"

/**
  * util class, functions are static
  */
class Util {
public:
	static xmlCharEncodingHandlerPtr xml_encoding_handler;
/**
  *in: timestamp_id is the id in process_timestamp table
  *in: field is the field to be updated
  *return: rows that have been affected, -1:error
  */
	static int save_timestamp(CMysql *mysql, int timestamp_id, const std::string &field);
/**
  *in: res_id is the id in vr_resource table
  *in: status_code, 1:fetching, 2:fetch success, 3:fetch failed, 4&5:warning
  *in: detail is the fetch detail info
  *return 0:success, -1:error
  */
	static int update_fetchstatus(CMysql *mysql, int res_id, int status_code, const std::string &detail);
/**
  *in: res_id is the id in vr_resource table
  *return 0:success, -1:error
  */
	static int update_fetchstatus(CMysql *mysql, int res_id);
/**
  *description: add attribute to the tag specified by xpath
  *in: xpathe_value_map key is xpath, value is the attribute value, 
  *in: name is the attribute name 
  *return 0::success, -1,-2:error
  */
	static int addAttribute(xmlXPathContextPtr xpathCtx, const std::map<std::string, std::string> &xpath_value_map, const std::string &name);
/*
 *description: get the vlaue specified by xpath
 *in: xpath
 *out: value
 */
	static int getValueByXpath(xmlXPathContextPtr xpathCtx, const std::string &xpath, std::string &value);
	static int urlTransform(xmlDocPtr doc, xmlXPathContextPtr xpathCtx, const std::string &xpath, CurlHttp &curl,std::string &pkg_name);
/**
  *convert utf8 to gbk
  */
	static bool UTF82GBK(xmlChar * utf8, char ** gbk);
/**
  *convert utf8 to gbk
  */
	static int UTF82GBK(const std::string &src, std::string &result);
/**
  * convert gbk to utf
  */
	static int GBK2UTF8(const std::string &src, std::string &result);

/**
 *desc: 
 *in:     str
 *out:    str
 *return: void
 */
	static void trim(char *str);
/**
 * convert md5 output to hex string
 * in: buf
 * out: des
 */
	static char * hexstr(unsigned char *buf, char *des,int len);

	/***
	  * compute the check sum value of docid
	  * in: str
	  */
	static std::string compute_cksum_value(const std::string& str);

	static std::string docid2Objid(const std::string docid);
	static std::string gbk_to_utf8(const  std::string & src,std::string defaults = "");
	static std::string utf8_to_gbk(const  std::string & src,std::string defaults = "");
	static int UTF82UTF16(const  std::string & src,std::string &result);
	static int UTF162UTF8(const  std::string & src,std::string &result);
	static int GBK2UTF16(const  std::string & src,std::string &result);
    	static int ChangeEncodeDeclaration(std::string in,std::string &out,std::string src,std::string des);
	static int SaveToStringWithEncodeFormat(xmlDocPtr doc,std::string &str);
	//修复有问题的GBK文字，将有问题的编码替换成指定符号
	static std::string recoverGBKErr(std::string & src,char defaults=' ');

private:
	static int XmlEncodingInput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen);
	static int XmlEncodingOutput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen);
	static int s_iconv(const std::string &src, std::string &result,std::string in,std::string out);
	static int getTransformedUrlList(CurlHttp &curl, JsonObject &urlList, std::string &postContent);
};

#endif
