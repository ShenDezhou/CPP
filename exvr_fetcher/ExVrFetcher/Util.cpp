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
#include <Platform/docId/docId.h>
#include <Platform/log.h>
#include <assert.h>

#include "Util.hpp"
#include "ConfigManager.h"
#include "JsonObject.h"

using namespace std;

xmlCharEncodingHandlerPtr Util::xml_encoding_handler = xmlNewCharEncodingHandler("gbk", XmlEncodingInput, XmlEncodingOutput);
/**
 *in: timestamp_id is the id in process_timestamp table
 *in: field is the field to be updated
 *return: rows that have been affected, -1:error
 */
int Util::save_timestamp(CMysql *mysql, int timestamp_id, const string &field)
{
	if(timestamp_id > 0)
	{
		char sql[1024];
		snprintf(sql, 1024, "update process_timestamp set %s = %ld where id = %d", field.c_str(), time(NULL), timestamp_id);
		SS_DEBUG((LM_ERROR, "Util::save_timestamp sql[%s]\n", sql));
		CMysqlExecute exe(mysql);
		int ret = exe.execute(sql);
		if (ret < 0)  //ret=0表示影响的行数为0，不视为错误
		{
			SS_DEBUG((LM_ERROR, "Util::save_timestamp execute failed ret %d sql[%s]\n", ret, sql));
		}
		return ret;
	}
	else
	{
		SS_DEBUG((LM_ERROR, "Util::save_timestamp timestamp id error: %d\n", timestamp_id));
		return -1;
	}
}

/**
 *in: res_id is the id in vr_resource table
 *in: status_code, 1:fetching, 2:fetch success, 3:fetch failed, 4&5:warning
 *in: detail is the fetch detail info
 *return 0:success, -1:error
 */
int Util::update_fetchstatus(CMysql *mysql, int res_id, int status_code, const string &detail)
{
	//generate the update sql
	string sql("update vr_resource set fetch_status=");
	char temp[10];
	snprintf(temp, 10, "%d", status_code);
	sql.append(temp);
	if(detail.size() == 0)
	{
		sql.append(" , fetch_detail=NULL");
	}
	else
	{
		sql.append(" , fetch_detail='");
		sql.append(detail);
		sql.append("'");
	}
	sql.append(" where id=");
	snprintf(temp, 10, "%d", res_id);
	sql.append(temp);

	//execute the sql
	//SS_DEBUG((LM_ERROR, "Util::update_fetchstatus sql[%s]\n", sql.c_str()));
	CMysqlExecute exe(mysql);
	int ret = exe.execute(sql.c_str());
	if (ret < 0)  //ret=0表示影响的行数为0，不视为错误
	{
		SS_DEBUG((LM_ERROR, "Util::update_fetchstatus execute failed ret %d sql[%s]\n", ret, sql.c_str()));
		return -1;
	}
	return 0;
}

/**
 *in: res_id is the id in vr_resource table
 *return 0:success, -1:error
 */
int Util::update_fetchstatus(CMysql *mysql, int res_id)
{
	//generate the update sql
	string sql("update vr_resource set fetch_status=2, fetch_detail=NULL where fetch_status=1 and id=");
	char temp[10];
	snprintf(temp, 10, "%d", res_id);
	sql.append(temp);

	//execute the sql
	SS_DEBUG((LM_ERROR, "Util::update_fetchstatus sql[%s]\n", sql.c_str()));
	CMysqlExecute exe(mysql);
	int ret = exe.execute(sql.c_str());
	if (ret < 0)  //ret=0表示影响的行数为0，不视为错误
	{
		SS_DEBUG((LM_ERROR, "Util::update_fetchstatus execute failed ret %d sql[%s]\n", ret, sql.c_str()));
		return -1;
	}
	return 0;
}

/**
 *description: add an attribute to the tag specified by xpath
 *in: xpathe_value_map key is xpath, value is the attribute value, 
 *in: name is the attribute name 
 *return 0::success, -1,-2:error
 */
int Util::addAttribute(xmlXPathContextPtr xpathCtx, const map<string, string> &xpath_value_map, const string &name)
{
	int ret = 0;
	map<string, string>::const_iterator itr;
	for(itr=xpath_value_map.begin(); itr!=xpath_value_map.end(); ++itr)
	{
		xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(itr->first.c_str()), xpathCtx);
		if(xpathObj == NULL)
		{
			SS_DEBUG((LM_ERROR, "Util::addAttribute Error: unable to evaluate xpath expression %s\n", itr->first.c_str()));
			xmlXPathFreeObject(xpathObj);
			ret = -1;
			continue;
		}

		//get the selected node
		xmlNodeSetPtr nodeset=xpathObj->nodesetval;
		if(xmlXPathNodeSetIsEmpty(nodeset)) 
		{
			SS_DEBUG((LM_ERROR, "Util::addAttribute No such nodes %s\n", itr->first.c_str()));
			xmlXPathFreeObject(xpathObj);
			ret = -1;
			continue;
		}

		//add an attribute to the selected node
		int size = (nodeset) ? nodeset->nodeNr : 0;
		for(int i=0; i<size; ++i)
		{
			//check wether the attribute is exist
			xmlChar *value_str = xmlGetProp(nodeset->nodeTab[i], (const xmlChar*)(name.c_str()));
			if(value_str)
			{
				SS_DEBUG((LM_ERROR, "Util::addAttribute attribute is already exist xpath expression %s, attribute %s=%s\n", itr->first.c_str(), name.c_str(), itr->second.c_str()));
				xmlFree(value_str);
				continue;
			}

			xmlAttrPtr newattr;
			newattr = xmlNewProp(nodeset->nodeTab[i], (const xmlChar*)(name.c_str()), (const xmlChar*)(itr->second.c_str()));
			if(!newattr)
			{
				SS_DEBUG((LM_ERROR, "Util::addAttribute add attribute error xpath expression %s, attribute %s=%s\n", itr->first.c_str(), name.c_str(), itr->second.c_str()));
				ret = -2;
				continue;
			}
		}
		xmlXPathFreeObject(xpathObj);
	}
	return ret;
}

/*
 *description: get the vlaue specified by xpath
 *in: xpath
 *out: value
 *return: 0:success, -1:error
 */
int Util::getValueByXpath(xmlXPathContextPtr xpathCtx, const std::string &xpath, std::string &value)
{
	int ret = 0;
	xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(xpath.c_str()), xpathCtx);
	if(xpathObj == NULL)
	{
		SS_DEBUG((LM_ERROR, "Util::getValueByXpath Error: unable to evaluate xpath expression %s\n", xpath.c_str()));
		xmlXPathFreeObject(xpathObj);
		return -1;
	}

	//get the selected node
	xmlNodeSetPtr nodeset = xpathObj->nodesetval;
	if(xmlXPathNodeSetIsEmpty(nodeset)) 
	{
		SS_DEBUG((LM_ERROR, "Util::getValueByXpath No such nodes %s\n", xpath.c_str()));
		ret = -1;
	}
	else
	{
		//get the value
		xmlChar *utf8 = xmlNodeGetContent(nodeset->nodeTab[0]);
		//UTF82GBK(xmlNodeGetContent(nodeset->nodeTab[0]), &gbk);
		//if(gbk)
		{
			value.assign((char*)utf8);
			xmlFree(utf8);
			ret = 0;
		}
		//else
		//	ret = -1;
	}
	xmlXPathFreeObject(xpathObj);

	return ret;
}
int Util::urlTransform(xmlDocPtr doc, xmlXPathContextPtr xpathCtx, const std::string &xpath, CurlHttp &curl,std::string &pkg_name)
{
	xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(xpath.c_str()), xpathCtx);
	if(xpathObj == NULL) {
		SS_DEBUG((LM_ERROR, "[Util::urlTransform] unable to evaluate xpath expression %s\n", xpath.c_str()));
		xmlXPathFreeObject(xpathObj);
		return -1;
	}
	xmlNodeSetPtr nodeset = xpathObj->nodesetval;
	if(xmlXPathNodeSetIsEmpty(nodeset)) {
		SS_DEBUG((LM_ERROR, "[Util::urlTransform] No such nodes %s\n", xpath.c_str()));
		xmlXPathFreeObject(xpathObj);
		return -1;
	}
	std::string postContent;
	postContent.append("vrspider\n");
	xmlChar *url;
	std::map<std::string, int> urlMap;
	int size = (nodeset) ? nodeset->nodeNr : 0;
	for(int i=0; i<size; ++i) {
		url = xmlNodeGetContent(nodeset->nodeTab[i]);
		postContent.append((char *) url).append("\n");
		urlMap.insert(make_pair((char *)url, i));
		xmlFree(url);
	}
	xmlXPathFreeObject(xpathObj);
	if(size > 0) {
		JsonObject urlList;
		if(getTransformedUrlList(curl, urlList, postContent) != 0) {
			SS_DEBUG((LM_ERROR, "[Util::urlTransform] transform url failed\n"));
			return -1;
		}
		std::string::size_type pos = xpath.find_last_of("/");
		if(pos != std::string::npos) {
			std::string fieldName = xpath.substr(pos+1);
			fieldName.append("_transform");
			std::string parentXpath = xpath.substr(0, pos);
			xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(parentXpath.c_str()), xpathCtx);
			if(xpathObj == NULL) {
				SS_DEBUG((LM_ERROR, "[Util::urlTransform] unable to evaluate parentXpath expression %s\n", parentXpath.c_str()));
				xmlXPathFreeObject(xpathObj);
				return -1;
			}
			xmlNodeSetPtr nodeset = xpathObj->nodesetval;
			if(xmlXPathNodeSetIsEmpty(nodeset)) {
				SS_DEBUG((LM_ERROR, "[Util::urlTransform] No such nodes %s\n", parentXpath.c_str()));
				xmlXPathFreeObject(xpathObj);
				return -1;
			}
			int size = (nodeset) ? nodeset->nodeNr : 0;
			for(int i=0; i<urlList.size(); ++i) {
				JsonObject item = urlList[i];
				if(item.get("code").ToString() == "ok") {
					std::string orignalUrl = item.get("url").ToString();
					std::string transfomredUrl = item.get("trans").ToString();
					std::string transfomredUrl_gbk;
					UTF82GBK(transfomredUrl, transfomredUrl_gbk);
					std::string packageName = item.get("apppkg").ToString();
					pkg_name = packageName;
					std::map<std::string, int>::iterator itr = urlMap.find(orignalUrl);
					if(itr!=urlMap.end() && itr->second<size) {
						xmlNodePtr parentNode = nodeset->nodeTab[itr->second];
						xmlNodePtr node = xmlNewNode(NULL, (const xmlChar *)(fieldName.c_str()));
						xmlNodePtr nodeValue = xmlNewCDataBlock(doc, (const xmlChar *)transfomredUrl.c_str(), transfomredUrl.size());
						xmlAddChild(node, nodeValue);
						xmlAddChild(parentNode, node);
						node = xmlNewNode(NULL, (const xmlChar *)"pkg");
						nodeValue = xmlNewCDataBlock(doc, (const xmlChar *)packageName.c_str(), packageName.size());
						xmlAddChild(node, nodeValue);
						xmlAddChild(parentNode, node);
					}
				}
			}
			xmlXPathFreeObject(xpathObj);
		}
	}
	return 0;
}
int Util::getTransformedUrlList(CurlHttp &curl, JsonObject &urlList, std::string &postContent)
{
	std::string addr("http://");
	addr.append(ConfigManager::instance()->getUrlTransformServer());
	addr.append("/urlToTrans");
	std::string result;
	if(curl.curlHttpPost(addr, postContent, result) == 0) {
		JsonObject js = result.c_str();
		if(!js.empty()) {
			urlList = js.get("transform");
			if(urlList.is_array())
				return 0;
		}
	}
	return -1;
}

/**
 *convert utf8 to gbk
 */
bool Util::UTF82GBK(xmlChar * utf8, char ** gbk) {
	xmlBufferPtr xmlbufin=xmlBufferCreate();
	xmlBufferPtr xmlbufout=xmlBufferCreate();
	if( NULL == xmlbufin || NULL == xmlbufout )
		return false;
	xmlBufferEmpty(xmlbufout);
	xmlBufferCat(xmlbufin,utf8);
	xmlCharEncOutFunc(xml_encoding_handler,xmlbufout,xmlbufin);
	*gbk = (char *)malloc((size_t)xmlBufferLength(xmlbufout) + 1);
	memcpy(*gbk,xmlBufferContent(xmlbufout),((size_t)xmlBufferLength(xmlbufout)+1) );
	//SS_DEBUG((LM_DEBUG, "keyword: |%s|%s|%d|\n", xmlBufferContent(xmlbufout), *gbk, strlen(*gbk) )); 
	//xmlChar* tmp = (xmlChar*)utf8;
	xmlFree(utf8);
	xmlBufferFree(xmlbufout);
	xmlBufferFree(xmlbufin);
	return true;
}

int Util::UTF82GBK(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF8", "GB18030//IGNORE");
}

int Util::GBK2UTF8(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"GB18030","UTF8//IGNORE");
}

int Util::UTF82UTF16(const  std::string & src,std::string &result)
{
	return s_iconv(src,result,"UTF8","UTF-16LE//IGNORE");
}
	
int Util::UTF162UTF8(const  std::string & src,std::string &result)
{
	return s_iconv(src,result,"UTF-16LE","UTF8//IGNORE");
}

int Util::GBK2UTF16(const  std::string & src,std::string &result)
{
	return s_iconv(src,result,"GBK","UTF-16LE//IGNORE");
}
std::string Util::recoverGBKErr(std::string & src,char defaults)
{
    unsigned one_byte = 0X00; //binary 00000000
    unsigned char c=0,low = 0;
    int len = src.length();
    for (int i=0; i<len;)
    {
	c = src[i];
	//如果是ascii码，直接过掉	
	if (c>>7 == one_byte) 
	{
	    i++;
	    continue;
	}
	//判断是gbk的高字符
	else if(c>=0x81 && c<=0xFE)
	{
	    //判断是否有第二个字符，如果没有，肯定出错
	    if(i+1 < len)
	    {
		low = src[i+1];
		//判断是否是合法的gbk编码
		if(low >=0x40 && low <= 0xFE)
		{
		    i+=2;
		    continue;
		}
		//不是合法的GBK编码的，直接用默认字符替换..
		else
		{
		    //SS_DEBUG((LM_INFO,"char=%x\n",low));
		    src[i] = defaults;
		    src[i+1] = defaults;
		    i+=2;
		    continue;
		}
	    }
	    else
	    {
		src[i] = defaults;
	    }
	}
	else
	{
	    src[i] = defaults;
	    i +=2;
	}
    }

    return src;
}


std::string Util::gbk_to_utf8(const std::string & src,std::string defaults )
{
	std::string result;
	if(GBK2UTF8(src,result) == -1)
	{
		result = defaults;
	}
	//result = src;
	return result;
}

std::string Util::utf8_to_gbk(const  std::string & src,std::string defaults)
{
	std::string result;
	if(UTF82GBK(src,result) == -1)
	{
		result = defaults;
	}
	//result = src;
	return result;
}


int Util::s_iconv(const std::string &src, std::string &result,std::string in,std::string out)
{
	int max_len = src.length()*2+1;
	char *buf = new char[max_len];
	memset(buf,0,max_len);
	if (buf == NULL)
		return -1; 
	iconv_t cd = iconv_open(out.c_str(),in.c_str());
	char *inbuff = const_cast<char *>(src.c_str());
	size_t inbytesleft = src.size();
	char *outbuff = buf;
	size_t outbytesleft = max_len;
	size_t ret = iconv(cd, &inbuff, &inbytesleft, &outbuff, &outbytesleft);
	if(ret == size_t(-1))
	{   
		printf("iconv failed: %s %s\n", strerror(errno), src.c_str());
	}   
	else
	{   
		size_t content_len = max_len - outbytesleft;
		result.assign(buf,content_len);
	} 
	iconv_close(cd);
	delete []buf;
	return ret;
}

/**
 *desc:   去掉str中首尾的空格和tab
 *in:     str
 *out:    str
 *return: void
 */
void Util::trim(char *str)
{
	if(str != NULL) {
		//trim left
		char *pos = str;
		char *start = str;
		while(*pos && (*pos==' ' || *pos=='\t')) {
			++pos;
		}
		while(*pos) {
			*start = *pos;
			++start;
			++pos;
		}

		//trim right
		while(start-1>=str && (*(start-1)==' ' || *(start-1)=='\t')) {
			--start;
		}
		*start = '\0';
	}
}

/**
 * convert md5 output to hex string
 * in: buf
 * out: des
 */
char * Util::hexstr(unsigned char *buf, char *des,int len)
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

int Util::XmlEncodingInput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen)
{
	iconv_t gbk2utf8 = iconv_open("utf-8//IGNORE", "gbk//IGNORE");
	char * outbuf = (char *)out;
	char * inbuf = (char *)in;
	size_t outlenbuf = *outlen;
	size_t inlenbuf = *inlen;

	size_t ret = iconv(gbk2utf8, &inbuf, &inlenbuf, &outbuf, &outlenbuf);
	iconv_close(gbk2utf8);

	if(ret == size_t(-1) && errno == EILSEQ){               
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;
		return -2;
	} else {
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;

		if(ret == size_t(-1) && errno == E2BIG)
			return -1;
		else
			return *outlen;
	}
}

int Util::XmlEncodingOutput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen)
{
	if ((out == NULL) || (outlen == NULL) || (inlen == NULL)) 
		return -1;
	if (in == NULL) {
		*outlen = 0; 
		*inlen = 0; 
		return 0;
	}    

	assert(*inlen >= 0 && *outlen>=0 );
	iconv_t utf82gbk = iconv_open("gbk//IGNORE", "utf-8//IGNORE");

	char * outbuf = (char *)out;
	char * inbuf = (char *)in;
	size_t outlenbuf = *outlen;
	size_t inlenbuf = *inlen;
	size_t ret = iconv(utf82gbk, &inbuf, &inlenbuf, &outbuf, &outlenbuf);
	iconv_close(utf82gbk);

	if(ret == size_t(-1) && errno == EILSEQ){
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;
		return -2;
	} else {
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;
		if(ret == size_t(-1) && errno == E2BIG)
			return -1;
		else
			return *outlen;
	}
}


std::string Util::compute_cksum_value(const std::string& str)
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

std::string Util::docid2Objid(const std::string docidStr)
{
	std::string objid;
	gDocID_t docid;
	sscanf(docidStr.c_str(), "%llx-%llx", &docid.id.value.value_high, &docid.id.value.value_low);
	char buf[32];
	hexstr((unsigned char*)(&docid.id), buf, 16);
	objid.append(buf);
	objid.append(compute_cksum_value(buf));

	return objid;
}

static void toLower(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =tolower(*p);
	}
}
static void toUpper(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =toupper(*p);
	}
}


int Util::SaveToStringWithEncodeFormat(xmlDocPtr doc,std::string &str)
{	
	xmlChar *xmlbuff;
	int buffersize;
	std::string output="";

	if (!doc)
		return -1;
	xmlDocDumpFormatMemory(doc, &xmlbuff, &buffersize, 1);
	char * buf = (char *)xmlbuff;
	//for (int i=0;i<buffersize;i++)
	//	output += buf[i];	
	output.assign(buf,buffersize);
	
	str = output;
	xmlFree(xmlbuff);
	
	return 1;
}

int Util::ChangeEncodeDeclaration(std::string in,std::string &out,std::string src,std::string des)
{
	//xml version="1.0" encoding="gbk"?>
	int pos = in.find("<?xml");
	int pos2= in.find("?>");
	if ((pos<0 ) ||(pos2<0))
		return -1;
	int pos_encoding = in.find(src,pos);
	if (pos_encoding > pos2)
		return -1;
	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::ChangeEncodeDeclaration:str_in  %s\n",in.c_str()));
	if (pos_encoding < 0) {
		toUpper(src);
		pos_encoding = in.find(src,pos);
		if ((pos_encoding > pos2)||(pos_encoding <0))
			return -1;
	}
	in.replace(pos_encoding,src.size(),des);
	out = in;
	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::ChangeEncodeDeclaration:out  %s\n",out.c_str()));
	return 1;
}
