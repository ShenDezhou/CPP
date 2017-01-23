#include "process.hpp"

using namespace std;

char *hexstr(unsigned char *buf,char *des,int len)
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
std::string compute_cksum_value(const std::string& str)
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
int splitStr(const string &str, const char separator, vector<string> &res)
{
    //fprintf(stderr, "enter splitStr\n");
	size_t startPos = 0;

	size_t curPos = 0;

	while(curPos < str.size())
	{
		//fprintf(stderr, "splitStr %s curPos is %d\n",str.c_str(),curPos);
		if(str[curPos] != separator)
		{
			curPos++;
		}
		else
		{
            //fprintf(stderr, "splitStr %s curPos is %d add str %s\n",str.c_str(),curPos,str.substr(startPos, curPos - startPos).c_str());
			res.push_back(str.substr(startPos, curPos - startPos));
			curPos++;
			startPos = curPos;
		}
	}
    res.push_back(str.substr(startPos,str.size() - startPos));
	return 0; 
}       


/**
 *desc:   将str中连续出现的在nullspace中指定的字符浓缩成一个ch
 *in:     str,nullspace
 *out:    str
 *return: void
 */
void multichar_to_onechar(char *str, const string &nullspace, const char ch)
{
	if(str != NULL) 
	{
		char *curr_end=str, *pos=str;
		bool flag = false;
		size_t nullspace_count = 0;
		nullspace_count = nullspace.size();
		while(*pos)
		{
			//find the first char that in nullspace
			for(int i=0; i<nullspace_count; ++i)
			{
				if(*pos == nullspace[i])
				{
					flag = true;
					++pos;
					break;
				}
			}

			if(flag)
			{
				//replace the first char by tab
				*curr_end = ch;
				flag = false;
				//ignore the rest char that in nullspace
				while(*pos)
				{
					for(int j=0; j<nullspace_count; ++j)
					{
						if(*pos == nullspace[j])
						{
							++pos;
							flag = true;
							break;
						}
						else
						{
							flag = false;
						}
					}
					if(!flag)
						break;
				}
				flag = false;
			}
			else
			{
				*curr_end = *pos;
				++pos;
			}
			++curr_end;
		}
		*curr_end= '\0';
	}
}

/**
 *desc:   将str中连续出现的在nullspace中指定的字符浓缩成一个ch
 *in:     str,nullspace
 *out:    str
 *return: void
 */
inline void multichar_to_onechar(string &str, const string &nullspace, const char ch)
{
	if(str.size() > 0)
	{
		char temp[256];
		*temp = '\0';
		strncpy(temp, str.c_str(), 256);
		multichar_to_onechar(temp, nullspace, ch);
		str.assign(temp);
	}
}

int prune_suffix(std::string &location)
{
	static const int subCount = 26;                                                                                                          
	static const char* subList[] = {"省", "市", "自治区", "自治县", "自治州", "直辖市", "维吾尔族", "壮族", "苗族", "彝族",
		"土家族", "藏族", "地区", "朝鲜族", "羌族", "布依族", "侗族", "哈尼族", "傣族", "白族", 
		"景颇族", "傈僳族", "回族", "蒙古族", "维吾尔", "哈萨克"};
	static const int subLength[] = {2, 2, 6, 6, 6, 6, 8, 4, 4, 4,
		6, 4, 4, 6, 4, 6, 4, 6, 4, 4,
		6, 6, 4, 6, 6, 6}; 

	bool isHit = true;
	while (isHit) {
		isHit = false;
		const char* pStr = location.c_str();
		int strLen = location.length();
		for (int i = 0; i < subCount; i ++) {
			int subLen = subLength[i];
			if (subLen > location.length()) continue;
			int index = 0; 
			while ((index < subLen) && (pStr[strLen - index - 1] == subList[i][subLen - index - 1])) index ++;

			if (index >= subLen) {
				isHit = true;
				location = location.substr(0, strLen-subLen);
				break;
			}       
		}       
	}       
	return 0;
}

int prune_suffix_tab(std::string &location)
{
	std::string tmp_str;
	std::string res_location;
	size_t prepos = 0;
	size_t pos = location.find('\t');
	while (pos != std::string::npos) {
		tmp_str = location.substr(prepos, pos);
		prune_suffix(tmp_str);
		res_location += tmp_str;
		prepos = pos;
		pos = location.find('\t', pos+1);
	}
	tmp_str = location.substr(prepos);
	prune_suffix(tmp_str);
	res_location += tmp_str;

	location = res_location;
	return 0;
}
static xmlCharEncodingHandlerPtr xml_encoding_handler = xmlNewCharEncodingHandler("gbk", XmlEncodingInput, XmlEncodingOutput);
bool UTF82GBK(xmlChar * utf8, char ** gbk) {
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
static inline bool ispunct_u16(int ch)
{
	return (ch < 128 && ispunct(ch)) ||	// 半角标点
		(ch > 0xff00 && ch < 0xff5f && ispunct(ch - 0xfee0)) ||	// 可转换为半角的全角标点
		(ch >= 0x3001 && ch <= 0x3004) || (ch >= 0x3008 && ch <= 0x301f) ||	// 、。〈〉《》「」『』【】等
		(ch >= 0x2010 && ch <= 0x203a);	// —…等
}
static inline bool isalpha_u16(int ch)
{
	return (ch < 128 && isalpha(ch)) ||	// 半角英文、数字
		(ch > 0xff00 && ch < 0xff5f && isalpha(ch - 0xfee0));	// 全角英文、数字
}
static inline bool isdigit_u16(int ch)
{
	return (ch < 128 && isdigit(ch)) ||	// 半角英文、数字
		(ch > 0xff00 && ch < 0xff5f && isdigit(ch - 0xfee0));	// 全角英文、数字
}
static inline bool isspace_u16(int ch)
{
	return ((ch < 128 && isspace(ch)) || ch == 0x3000);
}

// 删除标点，除非标点前后都是数字字符
// 如果 delSpace==true，则也删除空格，除非空格前后都是英文字符
// delNumberSign: 设为false时，保留'#'，用于vrqo的逐条可有可无词归一化 
char *PuncNormalize_gbk(char *str, bool delSpace = false, bool delNumberSign = true)
{
	iconv_t gbk2u16 = iconv_open("utf-16le//IGNORE", "gbk//IGNORE");	// "le" makes iconv to omit BOM
	iconv_t u162gbk = iconv_open("gbk//IGNORE", "utf-16le//IGNORE");
	size_t slen = strlen(str) + 1;
	size_t glen = slen;
	size_t ulen = glen * 4;
	char *gp = str;
	uint16_t *ubuf = new uint16_t[glen * 2];
	uint16_t *up = ubuf;
	if((size_t)-1 == iconv(gbk2u16, &gp, &glen, (char **)&up, &ulen))
	{
		SS_DEBUG((LM_ERROR, "PuncNormalize_gbk() Error iconv %s\n", str));
		iconv_close(gbk2u16);
		iconv_close(u162gbk);
		delete[] ubuf;
		return str;
	}

	// process
	enum {AD_NORM, AD_ALPHA, AD_DIGIT} adstate = AD_NORM;
	//bool inPun = false;	// in the middle of puncuation
	uint16_t *out = ubuf;
	for(uint16_t *in = ubuf; *in; ++in)
	{
		if(isdigit_u16(*in))
		{
			adstate = AD_DIGIT;
			*out++ = *in;
		}
		else if(isalpha_u16(*in) || (!delNumberSign && *in == '#'))
		{
			adstate = AD_ALPHA;
			*out++ = *in;
		}
		else if(ispunct_u16(*in))
		{
			if(adstate == AD_DIGIT)
			{
				// we should look forward beyond all puncts, if also isdigit, keep those puncts
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in)
				{
					if(ispunct_u16(*in))
						*out++ = *in++;
					else if(delSpace && isspace_u16(*in))
						++in;	// if(delSpace), then "1. .2" => "1..2", else "1 2"
					else
						break;
				}
				// now *in is the first char beyond all puncts
				if(!isdigit_u16(*in))
					out = outt;	// still remove the puncts
				--in;	// because the outer "for" will ++in;
			}
			// if not after a digit, the digit can be safely removed, so nothing needs to be done
		}
		else if(delSpace && isspace_u16(*in))
		{
			if(adstate == AD_ALPHA)
			{
				// we should look forward beyond all spaces, if also isalpha, keep those spaces
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in && (isspace_u16(*in) || ispunct_u16(*in)))
					++in;	// first space is already kept before this "while", so later spaces can be removed
				// now *in is the first char beyond all puncts
				if(!isalpha_u16(*in))
					out = outt;	// still remove the space
				--in;	// because the outer "for" will ++in;
			}
			// if not after an alpha, the space can be safely removed, so nothing needs to be done
		}
		else	// non-alphanum and non-punct
		{
			*out++ = *in;
			adstate = AD_NORM;
		}
	}
	*out++ = 0;

	if(ubuf[0])	// is not empty after processing
	{
		gp = str;
		up = ubuf;
		glen = slen;
		ulen = (out - ubuf) * 2;
		if((size_t)-1 == iconv(u162gbk, (char **)&up, &ulen, &gp, &glen))
		{
			SS_DEBUG((LM_ERROR, "PuncNormalize_gbk() Error iconv back %s\n", str));
			// if iconv back failed, str is possibly damaged, but there's little we can do, 
			// so just make sure the caller will not get an out-of-bound
			if(glen == slen || glen == 0)
				str[slen - 1] = 0;
			else
				*gp = 0;
		}
	}

	iconv_close(gbk2u16);
	iconv_close(u162gbk);
	delete[] ubuf;
	return str;
}

/**
  *description: add an attribute to the tag specified by xpath
  *in: xpathe_value_map key is xpath, value is the attribute value, 
  *in: name is the attribute name 
  *return 0::success, -1,-2:error
  */
int addAttribute(xmlXPathContextPtr xpathCtx, const map<string, string> &xpath_value_map, const string &name)
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

/**
 *desc:   去掉str中首尾的空格和tab
 *in:     str
 *out:    str
 *return: void
 */
void trim(char *str)
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

/*
 *description: get the vlaue specified by xpath
 *in: xpath
 *out: value
 *return: 0:success, -1:error
 */
int getValueByXpath(xmlXPathContextPtr xpathCtx, const std::string xpath, std::string &value)
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
		char *gbk = NULL;
		UTF82GBK(xmlNodeGetContent(nodeset->nodeTab[0]), &gbk);
		if(gbk)
		{
			value.assign(gbk);
			free(gbk);
			ret = 0;
		}
		else
			ret = -1;
	}
	xmlXPathFreeObject(xpathObj);

	return ret;
}


//replace character
inline void replace_char(char *str, char old, char new_char)
{
	if(str != NULL) {
		while(*str) {
			if(*str == old)
				*str = new_char;
			++str;
		}
	}
}

static inline std::string& replace_all_distinct(std::string& str,const std::string& old_value,const std::string& new_value)
{
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

void addProp(xmlNodePtr root,std::string &index_level)
{
	if(!root)
		return;
	std::string level;
	bool has_level=false;
	int bpos=0;
	int epos=-1;
	int sep_pos=-1;
	std::string record;
	std::string name;

	while((epos=index_level.find(";",bpos))!=std::string::npos)
	{
		record=index_level.substr(bpos,epos-bpos);
		bpos=epos+1;
		if((sep_pos=record.find(":"))!=std::string::npos)
		{
			name=record.substr(0,sep_pos);
			if(0 == xmlStrcmp(root->name,BAD_CAST name.c_str()))
			{
				has_level=true;
				level=record.substr(sep_pos+1);
			}
		}       
	}
	record=index_level.substr(bpos);
	bpos=epos+1;
	if((sep_pos=record.find(":"))!=std::string::npos)
	{
		name=record.substr(0,sep_pos);
		if(0 == xmlStrcmp(root->name,BAD_CAST name.c_str() ))
		{       
			has_level=true;
			level=record.substr(sep_pos+1).c_str();
		}
	}
	if(has_level && level.size()>0)
	{
		xmlNewProp(root, BAD_CAST "index_level", BAD_CAST level.c_str());
		//SS_DEBUG((LM_DEBUG, "add prop: %s %s\n",root->name,level.c_str()));
	}
	xmlNodePtr node = root->xmlChildrenNode;        
	while(node)
	{
		addProp(node,index_level);
		node=node->next;
	}
}
//处理每个req(即url)抓取回来的结果xml
int process(request_t *req, std::string &xml)
{
    Sender *sender = new Sender((req->reader_addr).c_str());
	Sender *multiHitSender = new Sender((req->multihit_reader_addr).c_str());
	int sendret=0;

	std::string errinfo;
	//1.验证抓取的正确性

	ACE_Time_Value testTime1,testTime2,testTime3,testTime4,testTime5;

	//1.验证xml数据
	std::string::size_type pos = xml.find("<item>");//一个item是一个key

	std::string::size_type next_pos = 0;
	int item_num = 0;
	int write_num = 0;
	time_t now = time(NULL);
	struct tm tm;
	char time_str[64];
	char buf[1024];
	//strftime(time_str, sizeof(time_str)-1, "%F_%T", localtime(&now));
	localtime_r(&now, &tm);
	strftime(time_str, sizeof(time_str), "%F %T", &tm);
	// ys int auto_check ;

	int ds_id = 0;
	//int valid_cnt = 0;

	//int send_succ   = 0;
	//验证当前classid的正确性
	std::string      err_info="";
	//ys unsigned char *  md5;
	//ys char            *md5_hex_old;
	//ys char             md5_hex[33];
	//int data_status = 0;
	char             time_tmp[64];
	std::string      location;
	location = "";
	char docIdbuf[33];
	char tmpDocIdbuf[32];
	gDocID_t docid;
	// ys int schema_fail_cnt = 0;
	ACE_Time_Value t1,t2;
	//ys std::string::size_type tpos;
	int class_id_convert = req->class_id;

	strftime(time_tmp, sizeof(time_tmp), "%F %T", &tm);
	SS_DEBUG((LM_DEBUG,"CWriteTask: resource url:%s\n",req->url.c_str()));

	//ys timeval  tplid_check_beg;

	int count = 0; 
	pos = xml.find("<item>");
	while(pos != std::string::npos)
	{
		count++;
		pos = xml.find("<item>", pos+strlen("<item>"));
	}
	//需要解析文件的处理
	//SS_DEBUG((LM_TRACE,"[CWriteTask::TEST]process3:%d\n",(testTime4-testTime3).usec()));


	req->citemNum=0;
	bool parseFailed = false;

	map<string, vector<string> > fieldBufs;
	if(req->class_id < 70000000) //not multi-hit vr
	{
		//fprintf(stderr, "test glp data format is %s\n", req->data_format.c_str());
		vector<string> emptyV;
		emptyV.reserve(MAXBUFCOUNT);
		if(strcmp(req->data_format.c_str(), RULEAB) == 0
				|| strcmp(req->data_format.c_str(), RULEABP) == 0)
		{
			//fileName = classidStr+ CONTENTA;
			fieldBufs.insert(make_pair(CONTENTA,emptyV));
			//fileName = classidStr + CONTENTB;
			fieldBufs.insert(make_pair(CONTENTB,emptyV));
		}
		else
		{
			//fileName = classidStr+ CONTENTA; 
			fieldBufs.insert(make_pair(CONTENTA,emptyV));    
		}
	}
	else
	{
		map<string, string>::iterator itr;
		for(itr=req->hit_fields.begin(); itr!=req->hit_fields.end(); ++itr)
		{   
			//fileName = classidStr + itr->first; 
			vector<string> emptyV;
			emptyV.reserve(MAXBUFCOUNT);
			fieldBufs.insert(make_pair(itr->first, emptyV));            
		}
	}

	// puncuation normalization settings
	//ys bool puncNorm = req->class_id >= 20000000 && req->class_id < 50000000;	// only for EXTERNAL and WIRELESS
	static const std::unordered_set<std::string> dSpWhiteList({"xAx", "AxP", "AxR", "A", "AP", "xP"});
	bool puncNormDelSpace = dSpWhiteList.find(req->data_format) != dSpWhiteList.end();	// do not delSpace for AB, A1A2...

	//process each item
	pos = xml.find("<item>");
	while( std::string::npos != pos )
	{
		ds_id ++;
		item_num ++;
		//ys int ret = 0, type = -1;
		std::string::size_type end_pos = xml.find("</item>", next_pos);
		if( std::string::npos == end_pos )
		{
			SS_DEBUG((LM_ERROR, "</item> not found error: %s\n", req->url.c_str()));
			break;
		}

		char time_tmp[64];
		//extract a single item begin with<item> and end with </item>
		std::string item = xml.substr(pos, (end_pos+strlen("</item>")-pos));

		if(req->class_id >= 20000000)
		{
			if( -1 == item.find("<title><![CDATA[") )
			{
				replace_all_distinct( item, "<title>", "<title><![CDATA[" );
				replace_all_distinct( item, "</title>", "]]></title>" );
			}
			if( -1 == item.find("<content1><![CDATA[") )
			{
				replace_all_distinct( item, "<content1>", "<content1><![CDATA[" );
				replace_all_distinct( item, "</content1>", "]]></content1>" );
			}
			if( -1 == item.find("<content2><![CDATA[") )
			{
				replace_all_distinct( item, "<content2>", "<content2><![CDATA[" );
				replace_all_distinct( item, "</content2>", "]]></content2>" );
			}
			if( -1 == item.find("<content3><![CDATA[") )
			{
				replace_all_distinct( item, "<content3>", "<content3><![CDATA[" );
				replace_all_distinct( item, "</content3>", "]]></content3>" );
			}
			if( -1 == item.find("<location><![CDATA[") && item.find("<location>"))
			{
				replace_all_distinct( item, "<location>", "<location><![CDATA[" );
				replace_all_distinct( item, "</location>", "]]></location>" );
			}
			if( -1 == item.find("<optword><![CDATA[") && item.find("<optword>"))
			{
				replace_all_distinct( item, "<optword>", "<optword><![CDATA[" );
				replace_all_distinct( item, "</optword>", "]]></optword>" );
			}
			if( -1 == item.find("<synonym><![CDATA[") && item.find("<synonym>"))
			{
				replace_all_distinct( item, "<synonym>", "<synonym><![CDATA[" );
				replace_all_distinct( item, "</synonym>", "]]></synonym>" );
			}

			//if find extract the location info from item
			//and store it to location
			if(-1 != item.find("<location><![CDATA["))
			{
				int pos,pos_e;
				pos = item.find("<location><![CDATA[");
				pos_e = item.find("]]></location>");
				location = item.substr(pos+strlen("<location><![CDATA["),
						pos_e-(pos+strlen("<location><![CDATA[")));
				SS_DEBUG((LM_DEBUG,"location is: %s\n",location.c_str()));
			}
			else
			{
				location = "";
			}
		}
		//add xml head and xml tail to item
		item = "<DOCUMENT>" + item + "</DOCUMENT><!--STATUS VR OK-->";

		std::string key, vrdata_key,optword, synonym, sql, url, uurl, keytmp;

		xmlNodePtr node = NULL, root = NULL;
		xmlBufferPtr buffer = NULL;
		//ys xmlOutputBufferPtr temp_buffer = NULL;
		//read item into a xmldoc and store it into doc
		xmlDocPtr doc = xmlReadMemory(item.c_str(), item.length(), NULL, "gbk", 0); 

		int valid_status = 1;
		//ys int check_status = 0;
		char valid_info[128] = {0};
		//ys unsigned char * data_md5;
		//ys char *data_md5_hex_old;
		//ys char data_md5_hex[33];
		//ys int locID = -1;
		std::string::size_type key_start = item.find("<key><!CDATA[");
		std::string::size_type key_end = item.find("]]></key>");
		char classid[16];
		std::stringstream ss;
		std::string objid;

		//if fail to create libxml doc, continue to next item 
		//move this directly after the doc creation statement
        if( NULL == doc )
		{
			if(err_info.length() == 0)
				err_info = "xmlReadMemory fail to parse item:\n " + item.substr(0,200);
			parseFailed = true;
			
            SS_DEBUG((LM_DEBUG,"cannot create doc\n"));	
			goto contin;
		}

		//extract key from item if not find, contiune to next item
		if(key_start == -1 || key_end == -1)//获取key字段
		{
			key_start = item.find("<key>");
			key_end = item.find("</key>");
			if(key_start == -1 || key_end == -1)
			{
				SS_DEBUG((LM_ERROR,"CWriteTask::process(): key find failed in item : %s\n",
							item.substr(0,255).c_str()));
				goto contin;
			}
			key_start += strlen("<key>");
			key_end = key_end - key_start;
		}
		else
		{
			key_start += strlen("<key><!CDATA[");
			key_end = key_end - key_start;
		} 
		keytmp = item.substr(key_start,key_end);	// keytmp 只在打 log 用了，后面实际使用的是 libxml 提取的 key
		SS_DEBUG((LM_DEBUG,"keyword splited is %s\n",keytmp.c_str()));


		//begin libxml parse
		root = xmlDocGetRootElement(doc);
		root = root->xmlChildrenNode;
		//if not find item , continue to next item
		if( 0 != xmlStrcmp(root->name,BAD_CAST"item") )
		{
			SS_DEBUG((LM_ERROR , "xml error: root is not <item> but %s\n",
						(char*)root->name ));
			goto contin;
		}
		// get vr_type(class tag), check schema
		//type = req->tplid;
		//if( type < 1 )
		//{
		//	goto contin;
		//}


		// add prop for index
		node = root->xmlChildrenNode;
		SS_DEBUG((LM_DEBUG, "add index_field prop: %s\n",req->index_field.c_str()));
		if(req->class_id < 70000000) //not multi-hit vr
			addProp(root,req->index_field);

		//check necessary properities 
		while( node )
		{
			if( 0 == xmlStrcmp(node->name,BAD_CAST"key") )
			{
				char *gbk = NULL;
				UTF82GBK( xmlNodeGetContent(node), &gbk );
				if( gbk )
				{
					key.assign(gbk);
					replace_all_distinct(key, "\\","\\\\");
					replace_all_distinct(key, "\'","\\\'");
					free(gbk);
				}
				else
				{

                    SS_DEBUG((LM_DEBUG,"gbk error\n"));	
					goto contin;
				}
			}
			if(req->class_id >= 20000000)
			{	
				if( 0 == xmlStrcmp(node->name,BAD_CAST"display") )
				{
					xmlNodePtr tmp_node = node->xmlChildrenNode;
					while( tmp_node )
					{
						if( 0 == xmlStrcmp(tmp_node->name,BAD_CAST"url") )
						{
							char *gbk = NULL;
							UTF82GBK( xmlNodeGetContent(tmp_node), &gbk );
							if( gbk )
							{
								url.assign(gbk);
								free(gbk);
							}
							else
							{
								SS_DEBUG((LM_ERROR, "CWriteTask::"
											"process(): url UTF82GBK fail: %d\n",
											 req->res_id ));
								goto contin;
							}
							break;
						}
						tmp_node = tmp_node->next;
					}
				}
				
				else if (0 ==  xmlStrcmp(node->name,BAD_CAST"synonym"))
				{
					char* gbk = NULL;
					UTF82GBK( xmlNodeGetContent(node), &gbk );
					if( gbk )
					{
						synonym.assign(gbk);
						replace_all_distinct(synonym, "\\","\\\\");
						replace_all_distinct(synonym, "\'","\\\'");
						free(gbk);
					}
					else
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process(): "
									"synonym UTF82GBK fail: %d\n",
									 req->res_id ));
						goto contin;
					}
					if (synonym.size() > 760)
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process():"
									"synonym size bigger than %d\n",
									 req->res_id, 760 ));
						goto contin;
					}                           
				}
			}
			node = node->next;
		}//end while(node)


		//generate docid according to classid, key and location
		//based on docid generate objid,which is used just int log
		char ubuff[2048];	// ubuff 只用于计算 unique_url/docid/objid
		char dbc_buf[2048];
		//ys ss << class_id_convert;
		//ys classid.clear();
		// ys classid = ss.str();
		sprintf(classid,"%d",class_id_convert);
		//确保都是半角小写
		if (EncodingConvertor::sbc2dbc(key.c_str(), ubuff, sizeof(ubuff), true) < 0) {
			SS_DEBUG((LM_ERROR, "CWriteTask::process(): EncodingConvertor::sbc2dbc [key:%s] failed.\n",
						 key.c_str()));
			strncpy(ubuff, key.c_str(), sizeof(ubuff));
		}

	    vrdata_key.assign(ubuff);

		trim(ubuff);

		//将查询词中的连续空格或tab替换成一个tab
		multichar_to_onechar(ubuff, " \t", '\t');

		// 标点、空格归一化
		PuncNormalize_gbk(ubuff, puncNormDelSpace);

		if(location.length() > 0) {
			//地域信息格式归一化
			multichar_to_onechar(location, " \t", '\t');

			//去掉省、市等词
			prune_suffix_tab(location);
			//省略...   fmm
			//locID = get_locationID(location);
			//SS_DEBUG((LM_DEBUG,"location %s, ID :%d\n",locID, location.c_str()));
			if(req->class_id >= 70000000) //add data source id for multi-hit vr
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%s#%d", classid, ubuff, location.c_str(), ds_id);
			else
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%s", classid, ubuff, location.c_str());
		} else {
			if(req->class_id >= 70000000)
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%d", classid, ubuff, ds_id);
			else
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s", classid, ubuff);
		}

		URLEncoder::encoding(dbc_buf, ubuff, sizeof(ubuff));
		url.assign(ubuff);//item对应的文档的url构成(classid#key#location)
		uurl = url;
		url2DocId(url.c_str(),&(docid.id));
		replace_all_distinct(url, "&amp;","&");
		sprintf(docIdbuf,"%llx-%llx",(unsigned long long)docid.id.value.value_high, (unsigned long long)docid.id.value.value_low);
		hexstr((unsigned char*)(&docid.id),tmpDocIdbuf, 16); 
		objid = tmpDocIdbuf;
		objid.append(compute_cksum_value(objid));
        /* ys 
		//compute the md5 of the current item		
		data_md5 = MD5((const unsigned char *)item.c_str(),item.length(),NULL);
		hexstr(data_md5,data_md5_hex, MD5_DIGEST_LENGTH);
        */

		if(req->class_id >= 20000000)
		{
			if( -1 == item.find("<unique_url>") )
				xmlNewTextChild(root, NULL, (const xmlChar *)"unique_url", 
						(const xmlChar *)url.c_str());
			xmlNewTextChild(root, NULL, (const xmlChar *)"source", 
					(const xmlChar *)(req->over24?"":"external") );
			strftime(time_tmp, sizeof(time_tmp), "%c", &tm);
			/* delete by ys
			 xmlNewTextChild(root, NULL, (const xmlChar *)"fetch_time", 
					(const xmlChar *)req->fetch_time_sum);//构造fetchtime
			*/
			xmlNewTextChild(root,NULL,(const xmlChar *)"fetch_time",(const xmlChar *)&time_tmp);
			//struct the expire time which is the current time plus the frequency
			time_t    expire_tm;
			memset(&expire_tm,0,sizeof(time_t));
			expire_tm  = mktime(&tm);
			expire_tm += req->frequency;
			struct tm *expire_tmp;
			expire_tmp = localtime(&expire_tm);
			strftime(time_tmp, sizeof(time_tmp), "%c", expire_tmp);
			xmlNewTextChild(root, NULL, (const xmlChar *)"expire_time", (const xmlChar *)&time_tmp);
			sprintf( buf, "%d", class_id_convert );
			xmlNewTextChild(root, NULL, (const xmlChar *)"classid", (const xmlChar *)buf);
			xmlNewTextChild(root, NULL, (const xmlChar *)"classtag", (const xmlChar *)req->tag.c_str());
			sprintf( buf, "%d", req->attr1 );
			xmlNewTextChild(root, NULL, (const xmlChar *)"param1", (const xmlChar *)buf);
			sprintf( buf, "%d", req->attr2 );
			xmlNewTextChild(root, NULL, (const xmlChar *)"param2", (const xmlChar *)buf);
			sprintf( buf, "%d", req->attr3 );
			xmlNewTextChild(root, NULL, (const xmlChar *)"param3", (const xmlChar *)buf);
			sprintf( buf, "%d", req->rank );
			xmlNewTextChild(root, NULL, (const xmlChar *)"param4", (const xmlChar *)buf);
			sprintf( buf, "%d", req->isNewFrame );
			xmlNewTextChild(root, NULL, (const xmlChar *)"param5", (const xmlChar *)buf);
			sprintf( buf, "%d", req->tplid );
			xmlNewTextChild(root, NULL, (const xmlChar *)"tplid", (const xmlChar *)buf);
			xmlNewTextChild(root, NULL, (const xmlChar *)"status", (const xmlChar *)"APPEND");
			xmlNewTextChild(root, NULL, (const xmlChar *)"update_type", (const xmlChar *)"no_update" );
			xmlNewTextChild(root, NULL, (const xmlChar *)"objid", (const xmlChar *)objid.c_str());
			SS_DEBUG((LM_TRACE,"Add objid:%s\n",(const xmlChar *)objid.c_str()));
			//add extend content tag
			xmlNodePtr extend_node = xmlNewNode(NULL, (const xmlChar *)"extend_content");
			xmlNodePtr extend_value = xmlNewCDataBlock(doc, (const xmlChar *)req->extend_content.c_str(),
					req->extend_content.size());
			xmlAddChild(extend_node, extend_value);
			xmlAddChild(root, extend_node);
		}

		//add some attribute like annotaion, index_level, sort for multi-hit vr
		if(req->class_id >= 70000000)
		{
			xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
			if(xpathCtx != NULL)
			{
				//add some properties
				addAttribute(xpathCtx, req->annotation_map, "annotation");
				addAttribute(xpathCtx, req->index_map, "index_level");
				addAttribute(xpathCtx, req->sort_map, "sort");
				addAttribute(xpathCtx, req->second_query_map, "second_query");
				addAttribute(xpathCtx, req->group_map, "group");
				addAttribute(xpathCtx, req->full_text_map, "fulltext");

				//get the hit field value
				map<string, string>::const_iterator itr;
				string value;
				int ret = 0;
				for(itr=req->hit_fields.begin(); itr!=req->hit_fields.end(); ++itr)
				{
					ret = getValueByXpath(xpathCtx, itr->second, value);
					if(ret == 0)
					{

						char tmpbuf[2048];

						if (EncodingConvertor::sbc2dbc(value.c_str(), tmpbuf, sizeof(tmpbuf), true) < 0)                        { 
							
							SS_DEBUG((LM_DEBUG,"EncodingError\n"));	
							goto contin;
						}

						vector<string> hit_keys;
						splitStr(string(tmpbuf),';',hit_keys);

						for(int i = 0; i< hit_keys.size(); i++)
						{
							fieldBufs[itr->first].push_back(hit_keys[i]);
						}
						//SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): get xpath value success, xpath:%s, value:%s\n", , itr->second.c_str(), value.c_str()));
					}
					else
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process(): get xpath value error, xpath:%s\n",  itr->second.c_str()));
					}
				}

			}
			else
			{
				SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): create xpath context error, vr_id:%d\n",
							req->class_id));
			}
			//free the xpath context
			xmlXPathFreeContext(xpathCtx);


			//add data_source_priority tag and sort attribute which value is ss
			char temp_priority[10];
			snprintf(temp_priority, 10, "%d", 1);
			xmlNodePtr data_source_node = xmlNewTextChild(root, NULL, 
					(const xmlChar *)"data_source_priority", (const xmlChar *)temp_priority);
			xmlNewProp(data_source_node, (const xmlChar *)"sort", (const xmlChar *)"ss");
		}
		else
		{
			//change fmm
			if(1)
			{
				fieldBufs[CONTENTA].push_back(vrdata_key);   
			}
			else
			{
				vector<string> keys;
				splitStr(vrdata_key, '\t', keys);
                if(keys.size() ==2)
                {
               
				    fieldBufs[CONTENTA].push_back(keys[0]);
				    fieldBufs[CONTENTB].push_back(keys[1]);
                }
                else
                    fprintf(stderr, "test glp rule AB key error\n");
			}
		}

		//ys timeval  add_prop;

		// validate length after add prop
		errinfo = "";
		if(req->class_id >= 20000000  )
		{
			valid_status = 0;
			if(strlen(valid_info) > 0)
				strncat(valid_info,"|", 127);
			strncat(valid_info, "LENGTH_ERROR:", 127);
			strncat(valid_info, errinfo.c_str(), 127);
		}


		//libxml doc to string, store it into item
		//added by ys show xml
		xmlChar *xmlbuffer;
		int buffersize;
        //xmlDocDumpFormatMemory(doc, &xmlbuffer, &buffersize, 1);
		xmlDocDumpMemoryEnc(doc,&xmlbuffer,&buffersize,"gbk");
		//SS_DEBUG((LM_ERROR, "xmlbuffer %s\n",(char *)xmlbuffer));
		item.assign((const char *)xmlbuffer,xmlStrlen(xmlbuffer));
        /* delete by ys
		buffer = xmlBufferCreate();
		if( NULL == buffer )
		{
			SS_DEBUG((LM_ERROR, "xmlBufferCreate fail: %d %s\n", 
						req->res_id, key.c_str() ));
			goto contin;
		}
		temp_buffer = xmlOutputBufferCreateBuffer(buffer, xml_encoding_handler);
		if(temp_buffer && (ret=xmlSaveFileTo(temp_buffer, doc, "gbk")) > 0) 
		{
			item.assign((const char *)xmlBufferContent(buffer),(size_t)xmlBufferLength(buffer));
		}
		else
		{
			SS_DEBUG((LM_ERROR, "xmlSaveFileTo fail: %d %s %x %d\n", 
						req->res_id, key.c_str(), temp_buffer, ret));
		//	SS_DEBUG((LM_ERROR, "xmlSaveFileTo fail: %s %s\n",item.c_str(),temp_buffer));
			goto contin;
		}
		*/
		replace_all_distinct(item, "\\","\\\\");
		//replace_all_distinct(item, "\'","\\\'");
		replace_all_distinct(item, "\'","&#39;");
		if( -1 == item.find("<unique_url><![CDATA[") )
		{
			replace_all_distinct( item, "<unique_url>", "<unique_url><![CDATA[" );
			replace_all_distinct( item, "</unique_url>", "]]></unique_url>" );
		}
		SS_DEBUG((LM_ERROR,"send result is %s\n",item.c_str()));
		//send to reader
        if (req->class_id >= 70000000)  //muti hit vr
		  sendret=multiHitSender->send_to(url.c_str(),item.c_str(),item.length());
		else
		  sendret=sender->send_to(url.c_str(),item.c_str(),item.length());

		if (sendret==0) 
		  SS_DEBUG((LM_TRACE,"[Process] send to reader uurl:%s\n",url.c_str()));
		else
		  SS_DEBUG((LM_ERROR,"[Process] send to reader failed uurl:%s\n",url.c_str()));	       

		write_num ++;
contin:
		
        //SS_DEBUG((LM_DEBUG,"res::\n%s\n",item.c_str()));
		if ( buffer )
			xmlBufferFree(buffer);
		if ( doc )
			xmlFreeDoc(doc);
		if ( xmlbuffer )
			xmlFree(xmlbuffer);

		next_pos = end_pos + strlen("</item>");
		pos = xml.find("<item>", next_pos);//处理下一个item
	}//end  while( std::string::npos != pos &&... 处理完了url结果xml中所有item

	if ( sender ){
		delete sender;
		sender = NULL;
		delete multiHitSender;
		multiHitSender = NULL;
    }	
	SS_DEBUG((LM_TRACE, "[CWriteTask::TEST]process(): [exstat] write items %d of %d in %s time cost: %d\n",
				 write_num, item_num, req->url.c_str(),req->writeTime));
	return 0;
}
