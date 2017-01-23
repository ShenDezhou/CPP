#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
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
#include <libxml/xpath.h>
#include <libxml/schemasInternals.h>
#include <iconv.h>
#include <errno.h>
#include <assert.h>
#include <string>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <utils/utils/auto_buf.hpp>

using namespace std;

struct StructuredErrorInfo {
	StructuredErrorInfo(int t) : type(t) {}

	int type;
	string keyWord;
	map<int, vector<string> > detail;
};

std::string m_length_prefix = "lengthFile";
std::string m_length_suffix = ".txt";
std::string m_schema_prefix = "schemaFile";
std::string m_schema_suffix = ".xsd";
//std::map<int,std::vector<std::string> > m_xpath_map;
//std::map<int,std::vector<int> > m_len_min_map;
//std::map<int,std::vector<int> > m_len_max_map;
std::map<int,time_t> m_len_time_map;

std::vector<std::string> m_xpath_vector;
std::vector<int> m_len_min_vector;
std::vector<int> m_len_max_vector;

#define ERRMSG_BUF_LEN 204

static int XmlEncodingInput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen)
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
static int XmlEncodingOutput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen)
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
	//iconv_t utf82gbk = iconv_open("gbk", "utf-8");

	char * outbuf = (char *)out;
	char * inbuf = (char *)in;
	size_t outlenbuf = *outlen;
	size_t inlenbuf = *inlen;
	//fprintf(stderr, "cycy:%s, len:%d\n", inbuf, *inlen);
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
static xmlCharEncodingHandlerPtr xml_encoding_handler = xmlNewCharEncodingHandler("gbk", XmlEncodingInput, XmlEncodingOutput);

bool UTF82GBK(xmlChar * utf8, char ** gbk)
{
	xmlBufferPtr xmlbufin=xmlBufferCreate();
	xmlBufferPtr xmlbufout=xmlBufferCreate();
	if( NULL == xmlbufin || NULL == xmlbufout )
		return false;
	xmlBufferEmpty(xmlbufout);
	xmlBufferCat(xmlbufin,utf8);
	xmlCharEncOutFunc(xml_encoding_handler,xmlbufout,xmlbufin);
	*gbk = (char *)malloc((size_t)xmlBufferLength(xmlbufout) + 1);
	memcpy(*gbk,xmlBufferContent(xmlbufout),((size_t)xmlBufferLength(xmlbufout)+1) );
	xmlFree(utf8);
	xmlBufferFree(xmlbufout);
	xmlBufferFree(xmlbufin);
	return true;
}

bool UTF82GBK(xmlChar * utf8, string &err_info)
{
	xmlBufferPtr xmlbufin=xmlBufferCreate();
	xmlBufferPtr xmlbufout=xmlBufferCreate();
	if( NULL == xmlbufin || NULL == xmlbufout )
		return false;
	xmlBufferEmpty(xmlbufout);
	xmlBufferCat(xmlbufin,utf8);
	xmlCharEncOutFunc(xml_encoding_handler,xmlbufout,xmlbufin);
	err_info.assign((const char *)xmlBufferContent(xmlbufout));
	//*gbk = (char *)malloc((size_t)xmlBufferLength(xmlbufout) + 1);
	//memcpy(*gbk,xmlBufferContent(xmlbufout),((size_t)xmlBufferLength(xmlbufout)+1) );
	xmlBufferFree(xmlbufout);
	xmlBufferFree(xmlbufin);
	return true;
}

int reload_length_file(int type, const char *path, std::string &errMsg)
{
	errMsg.clear();
	struct stat sbuf;
	char length_file_name[256];

	sprintf(length_file_name, "%s%s%02d%s", path, m_length_prefix.c_str(), type, m_length_suffix.c_str());
	if(access(length_file_name,R_OK))
	{
		errMsg.append("ERROR: 读取长度文件失败：");
		errMsg.append(length_file_name);
		errMsg.append(".\n");
		return -1;
	}

	stat(length_file_name,&sbuf);

	//if modify time changed, reload length file
	if(m_len_time_map.find(type) == m_len_time_map.end() || m_len_time_map[type] != sbuf.st_ctime )
	{
		m_len_min_vector.clear();
		m_len_max_vector.clear();
		m_xpath_vector.clear();

		FILE* fp = NULL;
		fp = fopen(length_file_name, "ro");
		if( NULL == fp )
		{
			errMsg.append("ERROR: 读取长度文件失败：");
			errMsg.append(length_file_name);
			errMsg.append(".\n");
			return -1;
		}
		char line[1024], tag[512];
		int a, b;
		while( fgets(line, sizeof(line)-1, fp) )
		{
			//trim right
			char *tab_pos = line + strlen(line) -1;
			while (tab_pos >= line
					&& (*tab_pos == '\t' || *tab_pos == '\n' || *tab_pos == '\r')) {
				*tab_pos-- = '\0';
			}
			*(tab_pos+1) = '\n';
			*(tab_pos+2) = '\0';
			//ignore the empty line
			if(strlen(line) <= 1)
				continue;
			//count tab
			int tab = 0;
			tab_pos = line;
			while (*tab_pos) {
				if (*tab_pos == '\t') {
					++tab;
				}
				++tab_pos;
			}
			//parse element
			std::string line_str(line);
			if( 2 == tab )
			{
				sscanf(line_str.c_str(), "%s\t%d\t%d\n", tag, &a, &b);
				m_xpath_vector.push_back(tag);
				m_len_min_vector.push_back(a);
				m_len_max_vector.push_back(b);
			}
			else if (2 < tab)
			{
				// add attributes' length check
				int pos = line_str.find("\t");
				std::string base = line_str.substr(0, pos);
				while(-1!=pos)
				{
					line_str = line_str.substr(pos+1);
					pos = line_str.find("\t");
					m_xpath_vector.push_back(base+"/@"+line_str.substr(0, pos));
					string xpath(base+"/@"+line_str.substr(0, pos));
					fprintf(stderr, "xpath:%s\n", xpath.c_str());
					line_str = line_str.substr(pos+1);
					pos = line_str.find("\t");
					m_len_min_vector.push_back(atoi(line_str.substr(0,pos).c_str()));
					line_str = line_str.substr(pos+1);
					pos = line_str.find_first_of("\t\n");
					m_len_max_vector.push_back(atoi(line_str.substr(0,pos).c_str()));
					pos = line_str.find("\t");
				}
			}
			else
			{
				errMsg.append("ERROR: 长度文件格式有误。\n");
				fclose(fp);
				return -1;
			}
			//printf("DEBUG: config length %d %d %s\n", m_len_min_map[type][m_len_min_map[type].size()-1],
			//m_len_max_map[type][m_len_max_map[type].size()-1], m_xpath_map[type][m_xpath_map[type].size()-1].c_str());

			if( (0 > m_len_min_vector[m_len_min_vector.size()-1] &&
						0 > m_len_max_vector[m_len_max_vector.size()-1]) ||
					m_len_min_vector[m_len_min_vector.size()-1] > m_len_max_vector[m_len_max_vector.size()-1] )
			{
				snprintf(line, 1024, "ERROR:长度文件配置有误，最小值:%d，最大值:%d，xpath路径：%s\n",
						m_len_min_vector[m_len_min_vector.size()-1], m_len_max_vector[m_len_max_vector.size()-1],
						m_xpath_vector[m_xpath_vector.size()-1].c_str());
				errMsg.append(line);
				fclose(fp);
				return -1;
			}
		}
		m_len_time_map[type] = sbuf.st_ctime;
		//printf("NOTICE: load length file %d success.\n",type);
		fclose(fp);
	}

	return 0;
}

int form_col_content_check_max(xmlNodePtr xml_node)
{
	int tlen2 = 0;
	xmlChar *szKey;
	char *gbk = NULL;
	while (xml_node) {
		if (!xmlStrcmp(xml_node->name, BAD_CAST"col2")) {
			szKey = xmlGetProp(xml_node, BAD_CAST"content");
			if (UTF82GBK(szKey, &gbk)) {
				//printf("%s\n", gbk);
				tlen2 = std::max(tlen2, (int)strlen(gbk));
				free(gbk);
				gbk = NULL;
			}
		}
		xml_node = xml_node->next;
	}
	return tlen2;
}

int form_col_content_check_sum(xmlNodePtr xml_node)
{
	int total_len = 0;
	xmlChar *szKey;
	char *gbk = NULL;
	while (xml_node) {
		if (!xmlStrcmp(xml_node->name, BAD_CAST"col")) {
			int tlen1 = 0;
			szKey = xmlGetProp(xml_node, BAD_CAST"content");
			if (UTF82GBK(szKey, &gbk)) {
				//printf("%s\n", gbk);
				tlen1 = strlen(gbk);
				free(gbk);
				gbk = NULL;
			}
			//xmlFree(szKey);
			xmlNodePtr sub_xml_node = xml_node->xmlChildrenNode;
			int tlen2 = form_col_content_check_max(sub_xml_node);
			total_len += std::max(tlen1, tlen2);
		}
		xml_node = xml_node->next;
	}
	return total_len;
}

int form_col_content_check(const char *one_xpath, xmlXPathContextPtr xpathCtx, 
		int low, int high, vector<string> &lenErrors, const string &key)
{
	string errMsg;
	char buf[1024];
	const char *pos = strstr(one_xpath, "/col/@content");
	if (pos == NULL) {
		return -1;
	}
	strncpy(buf, one_xpath, pos - one_xpath);
	buf[pos - one_xpath] = '\0';

	xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)buf, xpathCtx);
	xmlNodeSetPtr nodeset = xpathObj->nodesetval;
	if(xmlXPathNodeSetIsEmpty(nodeset)) {
		xmlXPathFreeObject(xpathObj);
		return -1;
	}

	int size = (nodeset) ? nodeset->nodeNr : 0;
	xmlNodePtr xml_node = NULL;

	int total_len;
	for (int i=0; i<size; ++i) {
		total_len = 0;
		xml_node = nodeset->nodeTab[i]->xmlChildrenNode;
		total_len = form_col_content_check_sum(xml_node);
		//printf("total_len:%d\n", total_len);
		if (total_len < low || total_len > high) {
			xmlXPathFreeObject(xpathObj);
			snprintf(buf, 1024, "key为：%s, %s 表格中的字段长度有误，%d<=%d<=%d\n", key.c_str(), one_xpath, low, total_len, high);
			lenErrors.push_back(buf);
			return 1;
		}
	}
	xmlXPathFreeObject(xpathObj);

	return 0;
}

int check_length(xmlDocPtr doc, int type, const char *path, vector<string> &lenErrors, const string &key)
{
	string errMsg;
	bool in_form = false;
	int form_len_max = 0;
	int form_len_min = 0;
	std::string form_xpath;
	std::map<int,std::vector<int> > formMap;
	char buf[1024];

	/* Create xpath evaluation context */
	xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
	if(xpathCtx == NULL)
	{
		errMsg.assign("key为：");
		errMsg.append(key);
		errMsg.append(", unable to create new XPath context.\n");
		lenErrors.push_back(errMsg);
		xmlXPathFreeContext(xpathCtx);
		return -2;
	}

	int size = m_xpath_vector.size();
	for( int i = 0; i < size; i ++ )
	{
		const std::string &one_xpath = m_xpath_vector[i];
		if (one_xpath.find("form/col/@content") != -1) {
			if (1 == form_col_content_check(one_xpath.c_str(), xpathCtx, m_len_min_vector[i], 
						m_len_max_vector[i], lenErrors, key)) {
				return 1;
			}
			continue;
		}
		/* Evaluate xpath expression */
		xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(one_xpath.c_str()), xpathCtx);
		if(xpathObj == NULL)
		{
			continue;
		}
		if(in_form == false && one_xpath.find("form") != -1)
		{
			std::string::size_type pos = one_xpath.find("col");
			if(pos != -1)
			{
				pos += 3;
				while(pos != std::string::npos)
				{
					if(isdigit(one_xpath[pos]))
					{
						pos++;
					}
					else
						break;
				}
			}
			if(pos == one_xpath.length())
			{
				in_form = true;
				//printf("DEBUG: form exist in file %s %d.\n", one_xpath.c_str(),i);
				form_xpath.assign(one_xpath.c_str(), one_xpath.find("/@col"));
			}

		}
		else if(in_form == true)
		{
			if(one_xpath.find("form") == -1)
				in_form = false;
			else
			{
				std::string::size_type pos = one_xpath.find("col");
				if(pos == -1)
					in_form = false;
				else
				{
					pos += 3;
					while(pos != std::string::npos)
					{
						if(isdigit(one_xpath[pos]))
							pos++;
						else
							break;
					}
					if(pos != one_xpath.length())
						in_form = false;
				}
			}
		}
		if(in_form)
		{
			form_len_max += m_len_max_vector[i];
			form_len_min += m_len_min_vector[i];
		}

		/* get values of the selected nodes */
		xmlNodeSetPtr nodeset=xpathObj->nodesetval;
		if(xmlXPathNodeSetIsEmpty(nodeset))
		{
			xmlXPathFreeObject(xpathObj);
			continue;
		}

		//get the value
		int size = (nodeset) ? nodeset->nodeNr : 0;
		for(int j=0; j <size; j++)
		{
			char *gbk = NULL;
			int len = 0;
			if( UTF82GBK( xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1), &gbk ) )
			{
				len = strlen(gbk);
				if(len>0 && one_xpath.find("display/")!=-1)
				{
					if(gbk[0]=='\t' || gbk[0]=='\n' || gbk[0]==' ' || 
						gbk[len-1]=='\t' || gbk[len-1]=='\n' || gbk[len-1]==' ')
					{
						snprintf(buf, 1024, "key为：%s, %s两侧包含特殊字符：制表符、换行符或空格\n",
								key.c_str(), one_xpath.c_str());
						lenErrors.push_back(buf);
					}
				}
			}
			if( gbk )
				free(gbk);

			if(in_form)
			{
				formMap[j].push_back(len);
				continue;
			}
			if( len < m_len_min_vector[i] || len > m_len_max_vector[i] )
			{
				snprintf(buf, 1024, "key为：%s, 长度校验失败，%d<=%d<=%d xpath:%s\n",
						key.c_str(), m_len_min_vector[i], len, m_len_max_vector[i], one_xpath.c_str());
				lenErrors.push_back(buf);
				xmlXPathFreeObject(xpathObj);
				xmlXPathFreeContext(xpathCtx);
				return 1;
			}
		}
		xmlXPathFreeObject(xpathObj);
	}

	for(std::map<int,std::vector<int> >::iterator it = formMap.begin();it != formMap.end();it++)
	{
		int form_len =0;

		for(std::vector<int>::iterator vit = it->second.begin();vit != it->second.end();vit++)
		{
			form_len += (*vit);
		}
		//printf("DEBUG: check length in a form: %d<=%d<=%d %s\n", form_len_min,form_len,form_len_max,form_xpath.c_str());
		if(form_len < form_len_min || form_len > form_len_max)
		{
			snprintf(buf, 1024, "key为：%s, 表格长度校验失败，%d<=%d<=%d xpath:%s\n",
					key.c_str(), form_len_min, form_len, form_len_max, form_xpath.c_str());
			lenErrors.push_back(buf);
			xmlXPathFreeContext(xpathCtx);
			return 1;
		}
	}

	//Cleanup of XPath data
	xmlXPathFreeContext(xpathCtx);

	//printf("NOTICE: check_length success.\n");
	return 0;
}

// libxml error handler
void schemaErrorCallback(void*, const char* message, ...) {
	printf("ERROR: ");
	va_list varArgs;
	va_start(varArgs, message);
	vprintf(message, varArgs);
	fflush(stderr);
	va_end(varArgs);
}

// libxml warning handler
void schemaWarningCallback(void* callbackData, const char* message, ...) {
	printf("WARNING: ");
	va_list varArgs;
	va_start(varArgs, message);
	vprintf(message, varArgs);
	fflush(stdout);
	va_end(varArgs);
}

xmlSchemaValidCtxt* load_schema(int type, const char *path)
{
	char schema_name[256];
	sprintf(schema_name, "%s%s%02d%s", path, m_schema_prefix.c_str(), type, m_schema_suffix.c_str());

	if(access(schema_name, R_OK)) {
		printf("ERROR: %sschema文件不存在.\n", schema_name);
		return NULL;
	}

	xmlSchemaValidCtxt* validityContext = NULL;
	xmlSchemaParserCtxt* schemaParser = NULL;
	xmlSchema* schema = NULL;
	if( ( schemaParser = xmlSchemaNewParserCtxt(schema_name)) != NULL )
	{
		if ( (schema = xmlSchemaParse(schemaParser)) != NULL )
		{
			if( (validityContext = xmlSchemaNewValidCtxt(schema)) != NULL )
			{
				xmlSchemaSetValidErrors(validityContext, schemaErrorCallback, schemaWarningCallback, 0);
				//printf("NOTICE: load schema file %s success.\n", schema_name);
			}
			else
			{
				printf("ERROR: create validity Context from schema fail: %s\n", schema_name);
			}
		}
		else
		{
			printf("ERROR: create xmlschema parser fail: %s \n", schema_name);
		}
	}
	else
	{
		printf("ERROR: create schema fail: %s\n", schema_name);
	}

	/*
	   if(schemaParser)
	   xmlSchemaFreeParserCtxt(schemaParser);
	   if(schema)
	   xmlSchemaFree(schema);
	 */
	return validityContext;
}

void parseErrorFunc(StructuredErrorInfo *errorInfo, xmlErrorPtr error)
{
	char errMsg_buf[ERRMSG_BUF_LEN];
	string temp1, temp2;
	/*
	   if(errorInfo->detail.find(error->code) == errorInfo->detail.end())
	   {
	   errorInfo->detail.insert(make_pair(error->code, vector<string>()));
	   }
	 */

	switch (error->code)
	{
		case XML_ERR_TAG_NAME_MISMATCH:	// 76
			UTF82GBK((xmlChar *)error->str1, temp1);
			UTF82GBK((xmlChar *)error->str2, temp2);
			snprintf(errMsg_buf, ERRMSG_BUF_LEN, "第%d行第%d列: 开闭标签不匹配 <%s>@行%d 和 </%s>\n",
					error->line, error->int2, temp1.c_str(), error->int1, temp2.c_str());
			break;
		case XML_SCHEMAV_ELEMENT_CONTENT:	// 1871
			UTF82GBK((xmlChar *)error->message, temp1);
			snprintf(errMsg_buf, ERRMSG_BUF_LEN, "第%d行第%d列: 不允许的元素 %s",
					error->line, error->int2, temp1.c_str());
			break;
		default:
			UTF82GBK((xmlChar *)error->message, temp1);
			snprintf(errMsg_buf, ERRMSG_BUF_LEN, "{error code:%d}第%d行第%d列: %s",
					error->code, error->line, error->int2, temp1.c_str());
			break;
	}
	string finalError;
	if(errorInfo->type == 0)
	{
		errorInfo->detail[error->code].push_back(errMsg_buf);
	}
	else if(errorInfo->type==1 || errorInfo->type==2)
	{
		finalError.append("key为：");
		finalError.append(errorInfo->keyWord);
		finalError.append("，");
		finalError.append(errMsg_buf);
		errorInfo->detail[error->code].push_back(finalError);
	}

	// trim the trailing '\n' in error->message
	/*
	   size_t pend = strlen(errMsg[0]);
	   if(pend > 0 && errMsg[0][pend - 1] == '\n')
	   errMsg[0][pend - 1] = '\0';
	 */
	// format the dbg msg
	/*
	   snprintf(errMsg[1], ERRMSG_BUF_LEN,
	   "code(%d), message(\"%s\"), line(%d), str1(\"%s\"), str2(\"%s\"), str3(\"%s\"), int1(%d), int2/col(%d)",
	   error->code, error->message, error->line, error->str1, error->str2, error->str3, error->int1, error->int2);
	 */
}

int check_schema(xmlDocPtr doc, xmlSchemaValidCtxt* validityContext, StructuredErrorInfo *errorInfo)
{
	xmlSetStructuredErrorFunc(errorInfo, (xmlStructuredErrorFunc)parseErrorFunc);
	int ret = xmlSchemaValidateDoc(validityContext, doc);
	xmlSetStructuredErrorFunc(NULL, NULL);	// restore default
	return ret;
}




int main(int argc, const char* argv[])
{
	if (argc != 4) {
		printf("Usage: %s xml_to_validate template_id path\n", argv[0]);
		printf("Example: %s data331.xml 331 /search/\n", argv[0]);
		exit(-1);
	}

	const char *xml_to_validate = argv[1];
	int type = atoi(argv[2]);
	const char *path = argv[3]; //文件绝对路径


	//判断xml文件是否存在
	int fd = ::open(xml_to_validate, O_RDONLY);
	if (fd < 3) {
		printf("ERROR: %s not found\n", xml_to_validate);
		exit(-1);
	}

	//读取xml文件
	struct stat st;
	fstat(fd, &st);
	char *buf = new char[st.st_size];
	if (::read(fd, buf, st.st_size) < 0) {
		printf("ERROR: read %s failed(errno: %d)\n", xml_to_validate, errno);
		exit(-1);
	}
	//printf("%s\n", buf);

	//加载schema文件
	xmlSchemaValidCtxt* validityContext;
	validityContext = load_schema(type, path);
	if(validityContext == NULL) {
		exit(-1);
	}

	std::string errMsgs;
	//加载长度文件
	if(reload_length_file(type, path, errMsgs))
	{
		printf("ERROR:长度文件有误，错误信息如下：\n%s", errMsgs.c_str());
		exit(-1);
	}

	xmlDocPtr doc = NULL;
	bool isSuccess = true;

    std::string encoding;
    const char* utf = strstr(buf, "encoding=\"utf-8\"");
    if( utf )
    {
        encoding = "utf-8";
    }
    else
    {
        encoding = "gbk";
    }

	//xml语法校验
	StructuredErrorInfo *errorInfo = new StructuredErrorInfo(0);
	xmlSetStructuredErrorFunc(errorInfo, (xmlStructuredErrorFunc)parseErrorFunc);
	doc = xmlReadMemory(buf, st.st_size, NULL, encoding.c_str(), 0);
	xmlSetStructuredErrorFunc(NULL, NULL);	// restore default
	if(doc == NULL)
	{
		isSuccess = false;
		/*
		   printf("ERROR: xml解析失败，错误信息如下: \n");
		   map<int, vector<string> >::iterator map_itr;
		   vector<string>::iterator err_itr;
		   for(map_itr=errorInfo->detail.begin(); map_itr!=errorInfo->detail.end(); ++map_itr)
		   {
		   vector<string> &errs = map_itr->second;
		   for(err_itr=errs.begin(); err_itr!=errs.end(); ++err_itr)
		   printf("ERROR: [errorCode:%d][sum:%lu]%s", map_itr->first, errs.size(), err_itr->c_str());
		   }
		 */
	}
	else {
		xmlFreeDoc(doc);
		doc = NULL;
	}

	StructuredErrorInfo *schemaErrorInfo = new StructuredErrorInfo(1);
	vector<string> lenErrors;
	vector<string> urlErrors;
	string keyAbsense;
	if(isSuccess) {
		//循环校验xml中的每个item是否合法
		char *pos = buf;
		char *pos_end = buf + st.st_size;
		char *item_beg = NULL;
		char *item_end = NULL;
		string one_record;
		string key;
		xmlNodePtr root, node;
		bool has_key;
		bool has_url;
		while (pos < pos_end) {
			item_beg = strstr(pos, "<item>");
			if(!item_beg) {
				break;
			}
			item_end = strstr(item_beg, "</item>");
			if(!item_end) {
				isSuccess = false;
				break;
			}
			pos = item_end + strlen("</item>");
			one_record.assign("<?xml version=\"1.0\" encoding=\""+encoding+"\"?><DOCUMENT>");
			one_record.append(item_beg, item_end - item_beg + strlen("</item>"));
			one_record.append("</DOCUMENT>");
			int pos1 = one_record.find("<key><![CDATA[");
			int pos2 = one_record.find("]]></key>");
			key = one_record.substr(pos1+strlen("<key><![CDATA["), pos2-pos1-strlen("<key><![CDATA["));
			errorInfo->type = 2;
			errorInfo->keyWord.assign(key);
			has_key = false;
			has_url = false;
			xmlSetStructuredErrorFunc(errorInfo, (xmlStructuredErrorFunc)parseErrorFunc);
			doc = xmlReadMemory(one_record.c_str(), one_record.size(), NULL, encoding.c_str(), 0);
			xmlSetStructuredErrorFunc(NULL, NULL);	// restore default
			if(doc) {
				root = xmlDocGetRootElement(doc);
				root = root->xmlChildrenNode; //item
				node = root->xmlChildrenNode;
				//get key
				while(node) {
					if(0 == xmlStrcmp(node->name, BAD_CAST"key")) {
						has_key = true;
						char *gbk = NULL;
						UTF82GBK(xmlNodeGetContent(node), &gbk);
						if(gbk) {
							key.assign(gbk);
							free(gbk);
						}
					}
					else if(0 == xmlStrcmp(node->name, BAD_CAST"display")) {
						xmlNodePtr tmp_node = node->xmlChildrenNode;
						while(tmp_node) {
							if(0 == xmlStrcmp(tmp_node->name, BAD_CAST"url")) {
								char *gbk = NULL;
								UTF82GBK(xmlNodeGetContent(tmp_node), &gbk);
								if(gbk && strlen(gbk)>0) {
									has_url = true;
									free(gbk);
								}
								break;
							}
							tmp_node = tmp_node->next;
						}
					}
					node = node->next;
				}

				if(has_key && !has_url) {
					urlErrors.push_back(key);
				}

				if(!has_key || key.empty()) {
					keyAbsense.append(one_record).append("<br/><br/>\n");
					//printf("ERROR: xml格式有误，key不存在。\n");
					isSuccess = false;
					continue;
				}

				schemaErrorInfo->keyWord.assign(key);
				//check schema
				if (0 != check_schema(doc, validityContext, schemaErrorInfo)) {
					isSuccess = false;
				}
				//check length
				if (0 != check_length(doc, type, path, lenErrors, key)) {
					isSuccess = false;
				}


				string final;
				xmlBufferPtr buffer = xmlBufferCreate();
				xmlOutputBufferPtr out_buffer = xmlOutputBufferCreateBuffer(buffer, xml_encoding_handler);
				if(out_buffer && xmlSaveFileTo(out_buffer, doc, "gbk")>0)
				{
					//final.assign((const char *)xmlBufferContent(buffer),(size_t)xmlBufferLength(buffer));
					//printf("doc output succed, key:%s\n", key.c_str());
				}
				//else
				//{
				//	printf("doc output failed, key:%s\n%s\n", key.c_str(), one_record.c_str());
				//}

				if(buffer)
					xmlBufferFree(buffer);

				//free doc
				xmlFreeDoc(doc);
				doc = NULL;
			}
			//else {
			//	printf("%s\n", one_record.c_str());
			//}
		}

	}

	//print error info
	if(!errorInfo->detail.empty()) {
		isSuccess = false;
		printf("ERROR: xml解析失败，错误信息如下: \n");
		map<int, vector<string> >::iterator map_itr;
		vector<string>::iterator err_itr;
		for(map_itr=errorInfo->detail.begin(); map_itr!=errorInfo->detail.end(); ++map_itr) {
			vector<string> &errs = map_itr->second;
			for(err_itr=errs.begin(); err_itr!=errs.end(); ++err_itr)
				printf("ERROR: [errorCode:%d][sum:%lu]%s", map_itr->first, errs.size(), err_itr->c_str());
		}
	}

	vector<string>::iterator err_itr;
	if(!schemaErrorInfo->detail.empty()) {
		isSuccess = false;
		printf("ERROR: schema验证失败，错误信息如下: \n");
		map<int, vector<string> >::iterator map_itr;
		for(map_itr=schemaErrorInfo->detail.begin(); map_itr!=schemaErrorInfo->detail.end(); ++map_itr) {
			vector<string> &errs = map_itr->second;
			for(err_itr=errs.begin(); err_itr!=errs.end(); ++err_itr)
				printf("ERROR: [errorCode:%d][sum:%lu]%s", map_itr->first, errs.size(), err_itr->c_str());
		}
	}

	if(!lenErrors.empty()) {
		isSuccess = false;
		printf("ERROR: 长度验证失败，错误信息如下：\n");
		for(err_itr=lenErrors.begin(); err_itr!=lenErrors.end(); ++err_itr)
			printf("ERROR: [errorCode:lenError][sum:%lu]%s", lenErrors.size(), err_itr->c_str());
	}

	if(!urlErrors.empty()) {
		isSuccess = false;
		printf("ERROR: display下缺少url的key如下：\n");
		for(auto itr=urlErrors.begin(); itr!=urlErrors.end(); ++itr)
			printf("ERROR: [errorCode:urlError][sum:%lu]%s\n", urlErrors.size(), itr->c_str());
	}

	if(!keyAbsense.empty()) {
		isSuccess = false;
		printf("ERROR: key不存在：\n%s\n", keyAbsense.c_str());
	}

	if(isSuccess) {
		printf("SUCCESS: xml data is valid.\n");
	}

	// garbage collect
	if (fd) {
		::close(fd);
		fd = -1;
	}
	if (buf) {
		delete[] buf;
		buf = NULL;
	}

	if(validityContext)
		xmlSchemaFreeValidCtxt(validityContext);

	if(errorInfo)
		delete errorInfo;


	return 0;
}
