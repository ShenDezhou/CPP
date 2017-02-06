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

#define _USE_TYPE_STR_
//#define _USE_TAG_STR_
#include "CWriteTask.hpp"

#include <stdlib.h>

#include <vector>
#include <unordered_set>
#include <sstream>
#include <algorithm>

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
#include "ace/Message_Block.h"
#include "ace/OS_Memory.h"
#include "ace/Guard_T.h"
#include <ace/Reactor.h>
#include <Platform/log.h>
#include <Platform/encoding/URLEncoder.h>
#include <openssl/md5.h>
#include <ace/Time_Value.h>
#include <Platform/encoding/EncodingConvertor.h>
#include <Platform/encoding.h>

#include "Configuration.hpp"
#include "scribemonitor.h"
#include "Util.hpp"
#include "ConfigManager.h"
#include "COnlineTask.hpp"
#include "CDeleteTask.hpp"
#include "redis_tool.h"

std::string unique_set_name;
pthread_mutex_t unique_set_name_mutex = PTHREAD_MUTEX_INITIALIZER;

using namespace std;

const static int MAXBUFCOUNT = 1000;
const static string CONTENTA = "content_A";
const static string CONTENTB = "content_B";
const static char *RULEAB = "AxB";
const static char *RULEABP = "AxBxP";

//item 最大数量
const static int MAXIMEMNUNBER = 1000000;
//urls 最大数量
const static int MAXURLSNUMBER = 50;
const static int MAXURLSITEMNUMBER = 10000;


static void toLower(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =tolower(*p);
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

//多个空格转换为一个
int multi_space_to_one(char *str)
{
	char *pos = str;
	char *over_pos = str;
	while (*pos) {
		if (*pos == ' ') {
			*over_pos++ = *pos++;
			while (*pos == ' ') ++pos;
		} else {
			*over_pos++ = *pos++;
		}   
	}   
	*over_pos = '\0';
	return 0;
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

std::string urlEncode(const std::string input)
{
    std::string conv_words = "";
    conv_words.reserve(input.length() * 3);
    const unsigned char* c = (const unsigned char*)input.c_str();
    for(size_t j = 0; j < input.size(); ++j) 
    {
	char s[8] = {0};
	snprintf(s, 8, "%%%.2X", c[j]);
	s[7] = 0;
	conv_words += s;
    }  
    return conv_words;
}

int g_subCount = 26;
int *g_subLength = NULL;
bchar_t (*g_subList)[16] = NULL;

void init_suffix()
{
        static const char* subList[] = {"省", "市", "自治区", "自治县", "自治州", "直辖市", "维吾尔族", "壮族", "苗族", "彝族",
                "土家族", "藏族", "地区", "朝鲜族", "羌族", "布依族", "侗族", "哈尼族", "傣族", "白族",
                "景颇族", "傈僳族", "回族", "蒙古族", "维吾尔", "哈萨克"};
        g_subCount = 26;
        g_subLength = new int[g_subCount];
        g_subList = new bchar_t[g_subCount][16];
	std::string result;
        for(int i=0; i<g_subCount; i++)
        {
		
		Util::GBK2UTF16(subList[i],result);
                bcscpy(g_subList[i], (const bchar_t *)result.c_str());
                g_subLength[i] = bcslen(g_subList[i]);
        }
}
void free_suffix()
{
        delete g_subLength;
        delete g_subList;
}
int prune_suffix_u16(std::bstring &location)
{
        bool isHit = true;
        while (isHit) {
                isHit = false;
                const bchar_t* pStr = location.c_str();
                int strLen = bcslen(location.c_str());
                for (int i = 0; i < g_subCount; i ++) {
                        int subLen = g_subLength[i];
                        if (subLen > location.length()) continue;
                        int index = 0;
                        while ((index < subLen) && (pStr[strLen - index - 1] == g_subList[i][subLen - index - 1])) index ++;

                        if (index >= subLen) {
                                isHit = true;
                                location = location.substr(0, strLen-subLen);
                                break;
                        }  
                }  
        }
        return 0;
}

int prune_suffix_tab_u16(std::bstring &location)
{
        std::bstring tmp_str;
        std::bstring res_location;
        size_t prepos = 0;
        size_t pos = location.find(0x0009);
        while (pos != std::bstring::npos) {
                tmp_str = location.substr(prepos, pos-prepos);
                prune_suffix_u16(tmp_str);
                res_location += tmp_str;
                prepos = pos;
                pos = location.find(0x0009, pos+1);
        }
        tmp_str = location.substr(prepos);
        prune_suffix_u16(tmp_str);
        res_location += tmp_str;

        location = res_location;
        return 0;
}


int prune_suffix_tab_u8(std::string src,std::string &result)
{
	Util::UTF82UTF16(src,src);
	std::bstring tmp_src;
	tmp_src.assign((const bchar_t*)src.c_str(),src.length()/2);
	
	prune_suffix_tab_u16(tmp_src);
	result.assign((const char*)tmp_src.c_str(),tmp_src.length()*2);
	Util::UTF162UTF8(result,result);
	return 0;
	
}



static inline bool ispunct_u16(const int ch)
{
	return (ch < 128 && ispunct(ch)) && ch != '@' && ch != '&' && ch!='%' && ch!='*' ||	// 半角标点。"@&%" 暂时不算标点
		(ch > 0xff00 && ch < 0xff5f && ispunct(ch - 0xfee0)) ||	// 可转换为半角的全角标点
		(ch >= 0x3001 && ch <= 0x3004) || (ch >= 0x3008 && ch <= 0x301f) ||	// 、。〈〉《》「」『』【】等
		(ch >= 0x2010 && ch <= 0x203a);	// —…等
}
static inline bool isalpha_u16(const int ch)
{
	return (ch < 128 && isalpha(ch)) ||	// 半角英文
		(ch > 0xff00 && ch < 0xff5f && isalpha(ch - 0xfee0));	// 全角英文
}
static inline bool isdigit_u16(const int ch)
{
	return (ch < 128 && isdigit(ch)) ||	// 半角数字
		(ch > 0xff00 && ch < 0xff5f && isdigit(ch - 0xfee0));	// 全角数字
}
static inline bool isspace_u16(const int ch)
{
	return ((ch < 128 && isspace(ch)) || ch == 0x3000);
}

// 删除标点，除非标点前后都是数字字符
// 如果 delSpace==true，则也删除空格，除非空格前后都是英文字符或都是数字字符
// delNumberSign: 设为false时，保留'#'，用于vrqo的逐条可有可无词归一化 
char *PuncNormalize_gbk(char *str,std::string encoding="gbk", bool delSpace = false, bool delNumberSign = true)
{
	iconv_t gbk2u16 = iconv_open("utf-16le//IGNORE", encoding.c_str());	// "le" makes iconv to omit BOM
	iconv_t u162gbk = iconv_open(encoding.c_str(), "utf-16le//IGNORE");
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
				while(*in && (ispunct_u16(*in) || (delSpace && isspace_u16(*in))))
					*out++ = *in++;
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
			else if(adstate == AD_DIGIT)
			{
				// we should look forward beyond all puncts, if also isdigit, keep those puncts
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in && (ispunct_u16(*in) || isspace_u16(*in)))
					*out++ = *in++;
				// now *in is the first char beyond all puncts
				if(!isdigit_u16(*in))
					out = outt;	// still remove the puncts
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

static inline bool ispunct_gchar(const gchar_t ch)
{
	const static std::set<gchar_t> puncts({44195 /*，*/, 41889 /*。*/, 41379 /*！*/, 49059 /*？*/,
		 48035 /*；*/, 47779 /*：*/, 42915 /*＇*/, 41635 /*＂*/, 41633 /*、*/, 43171 /*（*/,
		 43427 /*）*/, 44451 /*－*/, 48291 /*＜*/, 48803 /*＞*/, 46753 /*《*/, 47009 /*》*/,
		 44707 /*．*/, 64419 /*｛*/, 64931 /*｝*/, 56227 /*［*/, 56739 /*］*/, 48801 /*【*/,
		 49057 /*】*/, 43937 /*～*/});
	return puncts.find(ch) != puncts.end();
}
static inline bool isalpha_gchar(const gchar_t ch)
{
	return ((ch & 0xff) == 0xa3 &&
			( ((ch & 0xff00) >= 0xe100 && (ch & 0xff00) <= 0xfa00) ||	// ａ-ｚ
			  ((ch & 0xff00) >= 0xc100 && (ch & 0xff00) <= 0xda00) ));	// Ａ-Ｚ
}
static inline bool isdigit_gchar(const gchar_t ch)
{
	return ((ch & 0xff) == 0xa3 && (ch & 0xff00) >= 0xb000 && (ch & 0xff00) <= 0xb900);
}
static inline bool isspace_gchar(const gchar_t ch)
{
	return (ch == 0xa1a1);
}

// 删除标点，除非标点前后都是数字字符。输入编码是gchar
// 如果 delSpace==true，则也删除空格，除非空格前后都是英文字符或都是数字字符
// delNumberSign: 设为false时，保留'#'，用于vrqo的逐条可有可无词归一化 
// 返回结果字符串的长度，单位是gchar，不包含结尾的0
size_t PuncNormalize_gchar(gchar_t *str, bool delSpace = false, bool delNumberSign = true)
{
	enum {AD_NORM, AD_ALPHA, AD_DIGIT} adstate = AD_NORM;
	gchar_t *out = str, *in = str;
	for( ; *in; ++in)
	{
		if(isdigit_gchar(*in))
		{
			adstate = AD_DIGIT;
			*out++ = *in;
		}
		else if(isalpha_gchar(*in) || (!delNumberSign && *in == '#'))
		{
			adstate = AD_ALPHA;
			*out++ = *in;
		}
		else if(ispunct_gchar(*in))
		{
			if(adstate == AD_DIGIT)
			{
				// we should look forward beyond all puncts, if also isdigit, keep those puncts
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in && (ispunct_gchar(*in) || (delSpace && isspace_gchar(*in))))
					*out++ = *in++;
				// now *in is the first char beyond all puncts
				if(!isdigit_gchar(*in))
					out = outt;	// still remove the puncts
				--in;	// because the outer "for" will ++in;
			}
			// if not after a digit, the digit can be safely removed, so nothing needs to be done
		}
		else if(delSpace && isspace_gchar(*in))
		{
			if(adstate == AD_ALPHA)
			{
				// we should look forward beyond all spaces, if also isalpha, keep those spaces
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in && (isspace_gchar(*in) || ispunct_gchar(*in)))
					++in;	// first space is already kept before this "while", so later spaces can be removed
				// now *in is the first char beyond all puncts
				if(!isalpha_gchar(*in))
					out = outt;	// still remove the space
				--in;	// because the outer "for" will ++in;
			}
			else if(adstate == AD_DIGIT)
			{
				// we should look forward beyond all puncts, if also isdigit, keep those puncts
				uint16_t *outt = out;	// keep start pos so that we may revert
				*out++ = *in++;
				while(*in && (ispunct_gchar(*in) || isspace_gchar(*in)))
					*out++ = *in++;
				// now *in is the first char beyond all puncts
				if(!isdigit_gchar(*in))
					out = outt;	// still remove the puncts
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
	if(out != str)
		*out = 0;
	else
		out = in;	// if out == str, then the whole string was deleted. revert to the original string in this case

	return out - str;
}

int CWriteTask::open (void*)
{
	//EncodingConvertor::initializeInstance();
	int threadNum = ConfigManager::instance()->getWriteTaskNum();
	SS_DEBUG((LM_TRACE, "[CWriteTask::open] thread num:%d\n", threadNum));
	return activate(THR_NEW_LWP, threadNum);
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
static xmlCharEncodingHandlerPtr xml_encoding_handler = xmlNewCharEncodingHandler("gbk", XmlEncodingInput, XmlEncodingOutput);

bool CWriteTask::UTF82GBK(xmlChar * utf8, char ** gbk) {
	xmlBufferPtr xmlbufin=xmlBufferCreate();
	xmlBufferPtr xmlbufout=xmlBufferCreate();
	if( NULL == xmlbufin || NULL == xmlbufout )
	{
		SS_DEBUG((LM_INFO,"CWriteTask::UTF82GBK addr=%p,%p\n",xmlbufin,xmlbufout));
		return false;
	}
	xmlBufferEmpty(xmlbufout);
	xmlBufferCat(xmlbufin,utf8);
	xmlCharEncOutFunc(xml_encoding_handler,xmlbufout,xmlbufin);
	*gbk = (char *)malloc((size_t)xmlBufferLength(xmlbufout) + 1);
	memcpy(*gbk,xmlBufferContent(xmlbufout),((size_t)xmlBufferLength(xmlbufout)+1) );
	SS_DEBUG((LM_DEBUG, "[CWriteTask::UTF82GBK]]keyword: |%s|%s|%d|\n", xmlBufferContent(xmlbufout), *gbk, strlen(*gbk) )); 
	//xmlChar* tmp = (xmlChar*)utf8;
	xmlFree(utf8);
	xmlBufferFree(xmlbufout);
	xmlBufferFree(xmlbufin);
	return true;
}
static std::string getNodeContent(xmlNodePtr node)
{
	std::string tmp;
	xmlChar* content = xmlNodeGetContent(node);
	tmp.assign((char*)content);
	xmlFree(content);
	return tmp;
}

// libxml error handler
void schemaErrorCallback(void*, const char* message, ...) {
	va_list varArgs;
	va_start(varArgs, message);
	vfprintf(stderr, message, varArgs);
	fflush(stderr);
	va_end(varArgs);
}

// libxml warning handler
void schemaWarningCallback(void* callbackData, const char* message, ...) {
}

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

int CWriteTask::load_locationlist(char const * filename,std::map<std::string,int>& loc_map)
{
	if(filename == NULL)
		return -1;

	SS_DEBUG((LM_TRACE,"[load_location] list %s begin\n ",filename));

	FILE * fp = fopen(filename,"r");
	char buf[1024];
	std::string m_province;
	std::string m_city;
	bool isMuni = true;

	loc_map.clear();

	if(fp == NULL)
	{
		SS_DEBUG((LM_ERROR,"Can not open location list file %s\n",filename));
		return -1;
	}
	int id;
	char *pos;
	std::string location;
	int count = 0;

	while((fgets(buf,1024,fp)) != NULL)
	{
		if( strlen(buf) <6 )
		{
			continue;
		}
		pos = strchr(buf,'\n');
		if(pos != NULL)
			*pos = '\0';

		pos = strchr(buf,'\t');
		if(pos == NULL)
		{
			continue;
		}

		*pos = '\0';
		pos++;
		id = atoi(buf);

		if (id%10000 == 0)
		{
			m_province.clear();
			m_province.assign(pos);
			//_INFO("before");
			prune_suffix(m_province);
			isMuni = true;
			std::string location = m_province;
			loc_map.insert(std::pair<std::string,int>(location,id));
			count++;
			SS_DEBUG((LM_TRACE,"location %s %d",location.c_str(),id));
		}
		else if(id%100 == 0)
		{
			m_city.clear();
			m_city.assign(pos);
			prune_suffix(m_city);
			isMuni = false;
		}

		if(!isMuni){
			std::string location = m_province+"\t"+m_city;
			loc_map.insert(std::pair<std::string,int>(location,id));
			count++;
			SS_DEBUG((LM_TRACE,"location %s %d\n",location.c_str(),id));
		}
	}
	fclose(fp);
	SS_DEBUG((LM_TRACE,"reload location list %s finished, %d items have been loaded\n",filename,count));
	return 0;
}

int CWriteTask::get_locationID(std::string locName)
{
	std::map<std::string,int>::iterator it;
	it = m_loc_map.find(locName);
	if(it == m_loc_map.end())
		return -1;
	else
		return it->second;
}

int CWriteTask::open (const char* configfile) 
{
	configName = configfile;	
	ConfigurationFile config(configfile);
	ConfigurationSection section;
	if( !config.GetSection("ExVrFetcher",section) )
	{
		SS_DEBUG((LM_ERROR, "[CWriteTask::open]Cannot find ExVrFetcher config section\n"));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}
	vr_ip = section.Value<std::string>("VrResourceIp");
	vr_user = section.Value<std::string>("VrResourceUser"); 
	vr_pass = section.Value<std::string>("VrResourcePass");
	vr_db = section.Value<std::string>("VrResourceDb"); 
	SS_DEBUG((LM_INFO, "[CWriteTask::open ]load vr_resource configiguration: %s %s %s %s\n",
				vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str(), vr_db.c_str() ));
	vi_ip = section.Value<std::string>("VrInfoIp");
	vi_user = section.Value<std::string>("VrInfoUser"); 
	vi_pass = section.Value<std::string>("VrInfoPass");
	vi_db = section.Value<std::string>("VrInfoDb"); 
	SS_DEBUG((LM_INFO, "[CWriteTask::open]load vr_info configiguration: %s %s %s %s\n",
				vi_ip.c_str(), vi_user.c_str(), vi_pass.c_str(), vi_db.c_str() ));
	cr_ip = section.Value<std::string>("CrResourceIp");
	cr_user = section.Value<std::string>("CrResourceUser"); 
	cr_pass = section.Value<std::string>("CrResourcePass");
	cr_db = section.Value<std::string>("CrResourceDb");
	m_need_urltran = section.Value<std::string>("NeedUrlTransform");

	SS_DEBUG((LM_INFO, "[CWriteTask::open]load cr_info configiguration: %s %s %s %s\n",
				cr_ip.c_str(), cr_user.c_str(), cr_pass.c_str(), cr_db.c_str() ));

	m_schema_prefix = section.Value<std::string>("SchemaPrefix"); 
	m_schema_suffix = section.Value<std::string>("SchemaSuffix"); 
	m_length_prefix = section.Value<std::string>("LengthPrefix"); 
	m_length_suffix = section.Value<std::string>("LengthSuffix"); 

	std::string res_priority("0");
	std::string spider_type = section.Value<std::string>("SpiderType");
	if(spider_type == "2")
		res_priority.assign("1");

	SS_DEBUG((LM_INFO, "cycy spider_type:%s res_priority:%s\n", spider_type.c_str(), res_priority.c_str()));

	SS_DEBUG((LM_INFO, "[CWriteTask::open ]load ValidateCount configiguration: %sXX%s %sXX%s\n",  
				m_schema_prefix.c_str(), m_schema_suffix.c_str(), m_length_prefix.c_str(), m_length_suffix.c_str() ));
	pthread_rwlock_init(&m_length_rwlock,NULL);
	pthread_mutex_init(&m_validCtxt_mutex,NULL);
	m_loc_file = section.Value<std::string>("LocationList");
	if(load_locationlist(m_loc_file.c_str(),m_loc_map))
	{
		SS_DEBUG((LM_ERROR,"load location list %s failed\n",m_loc_file.c_str()));
		exit(-1);
	}
	
	std::string redis_address = section.Value<std::string>("RedisAddress");
	std::string redis_password = section.Value<std::string>("RedisPassword");
	int redis_port = section.Value<int>("RedisPort");
	//unique_set_name = section.Value<std::string>("UniqueSetName");
	int type = atoi(spider_type.c_str());
	redis_tool = new RedisTool(redis_address, redis_port, type, redis_password);
	//redis_tool->SetListSetName(ACTION_FETCH);
	int reconn = 0;
	while (reconn < 5)
	{
	   if (!redis_tool->Connect())
	   {
		   break;
	   }
	   else
	   {
		   reconn++;
	   }
	}
	if (reconn >= 5)
	{
		SS_DEBUG((LM_ERROR, "CWriteTask::open(%@):redis: connect vr fail\n", this));
		return -1;
	}
	
	SS_DEBUG((LM_TRACE, "CWriteTask::open(%@):load length success\n",this));
	return open();  
}

int CWriteTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
	return putq(message, timeout);
}

int CWriteTask::get_length( xmlNodePtr node )
{
	char *gbk = NULL;
	int len = 0;
	if( UTF82GBK( xmlNodeGetContent(node), &gbk ) )
	{
		len = strlen(gbk);
	}
	if( gbk )
		free(gbk);
	return len;
}

int CWriteTask::reload_length_file(int type)
{
	struct stat sbuf;
	char length_file_name[256];

	sprintf(length_file_name, "%s%02d%s", m_length_prefix.c_str(), type, m_length_suffix.c_str());
	pthread_rwlock_wrlock(&m_length_rwlock);
	if(access(length_file_name,R_OK))
	{
		SS_DEBUG((LM_ERROR,"fail to open length file %s in read mode.\n",length_file_name));
		pthread_rwlock_unlock(&m_length_rwlock);
		return -1;
	}

	stat(length_file_name,&sbuf);

	//if modify time changed, reload length file
	if(m_len_time_map.find(type) == m_len_time_map.end() || m_len_time_map[type] != sbuf.st_ctime )
	{
		//std::vector<int>::iterator it = std::find(m_len_loaded_vec.begin(),m_len_loaded_vec.end(),type);

		//if( it != m_len_loaded_vec.end())
		//{
		//m_len_loaded_vec.erase(it);
		m_len_min_map.erase(type);
		m_len_max_map.erase(type);
		m_xpath_map.erase(type);
		//}

		FILE* fp = NULL;
		fp = fopen(length_file_name, "ro");
		if( NULL == fp )
		{
			SS_DEBUG((LM_ERROR, "config length file error: %s\n", length_file_name ));
			pthread_rwlock_unlock(&m_length_rwlock);
			return -1;
		}
		char line[1024], tag[512];
		int a, b;
		while( fgets(line, sizeof(line) -1, fp) )
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
				//m_xpath[type].push_back(tag);
				m_xpath_map[type].push_back(tag);
				//m_len_min[type].push_back(a);
				m_len_min_map[type].push_back(a);
				//m_len_max[type].push_back(b);
				m_len_max_map[type].push_back(b);
			}
			else if (2 < tab)
			{  // add attributes' length check
				int pos = line_str.find("\t");
				std::string base = line_str.substr(0, pos);
				while(-1!=pos)
				{
					line_str = line_str.substr(pos+1);
					pos = line_str.find("\t");
					//m_xpath[type].push_back(base+"/@"+line_str.substr(0, pos));
					m_xpath_map[type].push_back(base+"/@"+line_str.substr(0, pos));
					line_str = line_str.substr(pos+1);
					pos = line_str.find("\t");
					//m_len_min[type].push_back(atoi(line_str.substr(0,pos).c_str()));
					m_len_min_map[type].push_back(atoi(line_str.substr(0,pos).c_str()));
					line_str = line_str.substr(pos+1);
					pos = line_str.find_first_of("\t\n");
					//m_len_max[type].push_back(atoi(line_str.substr(0,pos).c_str()));
					m_len_max_map[type].push_back(atoi(line_str.substr(0,pos).c_str()));
					pos = line_str.find("\t");
				}
			}
			else
			{
				SS_DEBUG((LM_ERROR, "CWriteTask::reload_length_file(%@) length file error, template id:%d\n", this, type));
				pthread_rwlock_unlock(&m_length_rwlock);
				fclose(fp);
				return -1;
			}
			SS_DEBUG((LM_DEBUG, "[CWriteTask::svc]config length %d %d %s\n", m_len_min_map[type][m_len_min_map[type].size()-1], 
						m_len_max_map[type][m_len_max_map[type].size()-1], m_xpath_map[type][m_xpath_map[type].size()-1].c_str() ));

			if( 0 > m_len_min_map[type][m_len_min_map[type].size()-1] || 
					0 > m_len_max_map[type][m_len_max_map[type].size()-1] ||
					m_len_min_map[type][m_len_min_map[type].size()-1] > m_len_max_map[type][m_len_max_map[type].size()-1] )
			{
				SS_DEBUG((LM_ERROR, "[CWriteTask::svc]config length error: %d %d %s\n", 
							m_len_min_map[type][m_len_min_map[type].size()-1], 
							m_len_max_map[type][m_len_max_map[type].size()-1], 
							m_xpath_map[type][m_xpath_map[type].size()-1].c_str() ));
				pthread_rwlock_unlock(&m_length_rwlock);
				fclose(fp);
				return -1;
			}
		}
		//m_length_loaded[type] = true;
		//m_len_loaded_vec.push_back(type);
		if(m_xpath_map.find(type) == m_xpath_map.end()) {
			SS_DEBUG((LM_ERROR, "[CWriteTask::svc]load length file %d error, need retry\n", type));
			pthread_rwlock_unlock(&m_length_rwlock);
			fclose(fp);
			return 1;
		} else {
			m_len_time_map[type] = sbuf.st_ctime;
			SS_DEBUG((LM_TRACE,"[CWriteTask::svc]load length file %d succeed.\n",type));
		}
		fclose(fp);
	}
	pthread_rwlock_unlock(&m_length_rwlock);

	return 0;
}

int CWriteTask::form_col_content_check_max(xmlNodePtr xml_node)
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

int CWriteTask::form_col_content_check_sum(xmlNodePtr xml_node)
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
			//可能存在的col2
			xmlNodePtr sub_xml_node = xml_node->xmlChildrenNode;
			int tlen2 = form_col_content_check_max(sub_xml_node);
			total_len += std::max(tlen1, tlen2);
		}
		xml_node = xml_node->next;
	}
	return total_len;
}

int CWriteTask::form_col_content_check(const char *one_xpath, xmlXPathContextPtr xpathCtx, int low, int high, std::string &errinfo)
{
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
		printf("No such nodes %s\n", buf);
		xmlXPathFreeObject(xpathObj);
		return -1;
	}

	int size = (nodeset) ? nodeset->nodeNr : 0;
	xmlNodePtr xml_node = NULL;

	int total_len;
	for (int i=0; i<size; ++i) {
		total_len = 0;
		//计算每个form下的col@content长度和
		xml_node = nodeset->nodeTab[i]->xmlChildrenNode;
		total_len = form_col_content_check_sum(xml_node);
		//printf("total_len:%d\n", total_len);
		if (total_len < low || total_len > high) {
			xmlXPathFreeObject(xpathObj);
			printf("check form/col length error: %d<=%d<=%d %s\n", low, total_len, high, one_xpath);
			errinfo = one_xpath;
			return 1;
		}
	}
	xmlXPathFreeObject(xpathObj);

	return 0;
}

int CWriteTask::check_length(xmlDocPtr doc, int type,std::string &keyword,request_t *req, std::string &errinfo)
{
	bool in_form = false;
	int form_len_max = 0;
	int form_len_min = 0;
	std::string form_xpath;
	std::map<int,std::vector<int> > formMap;

	//load length file
	int ret, retryCount = 0;
	while((ret=reload_length_file(type))==1 && retryCount<3) {
		SS_DEBUG((LM_ERROR, "retry to load length file %d.\n", type));
		sleep(1);
		++retryCount;
	}
	if (0 != ret) {
		SS_DEBUG((LM_ERROR, "Error: reload length file[type:%d] failed.\n", type));
		errinfo = "load lengthFile failed";
		return 1;
	}

	/* Create xpath evaluation context */
	xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
	if(xpathCtx == NULL) 
	{
		SS_DEBUG((LM_ERROR, "Error: unable to create new XPath context\n"));
		xmlXPathFreeContext(xpathCtx);
		return -2;
	}

	pthread_rwlock_rdlock(&m_length_rwlock);
	int size = m_xpath_map[type].size();
	for( int i = 0; i < size; i++ )
	{
		const std::string &one_xpath = m_xpath_map[type][i];
		if (one_xpath.find("form/col/@content") != -1) {
			if (1 == form_col_content_check(one_xpath.c_str(), xpathCtx, m_len_min_map[type][i], m_len_max_map[type][i], errinfo)) {
				pthread_rwlock_unlock(&m_length_rwlock);
				return 1;
			}
			continue;
		}
		/* Evaluate xpath expression */
		xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(one_xpath.c_str()), xpathCtx);
		if(xpathObj == NULL)
		{
			SS_DEBUG((LM_ERROR, "Error: unable to evaluate xpath expression %s\n", one_xpath.c_str()));
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
				printf("form exist in file %s %d.\n", one_xpath.c_str(),i);
				form_xpath.assign(one_xpath.c_str(), one_xpath.find("form") -1);
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
			form_len_max += m_len_max_map[type][i];
			form_len_min += m_len_min_map[type][i];
		}

		/* get values of the selected nodes */
		xmlNodeSetPtr nodeset=xpathObj->nodesetval;
		if(xmlXPathNodeSetIsEmpty(nodeset)) 
		{
			SS_DEBUG((LM_DEBUG, "No such nodes %s\n", one_xpath.c_str()));
			xmlXPathFreeObject(xpathObj);
			continue;
		}

		//get the value    
		int size = (nodeset) ? nodeset->nodeNr : 0;
		for(int j = 0; j <size; j++) 
		{
			//val=(char*)xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1);
			//cout<<"the results are: "<<val<<endl;
			char *gbk = NULL;
			int len = 0;
			xmlChar* tmp = xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1);
			if( UTF82GBK( tmp, &gbk ) )
			{
				len = strlen(gbk);
			}
			if( gbk )
			{
				free(gbk);
			}

			if(in_form)
			{
				formMap[j].push_back(len);
				continue;
			}
			if( len < m_len_min_map[type][i] || len > m_len_max_map[type][i] )
			{
				SS_DEBUG((LM_ERROR, "check length error: %d<=%d<=%d %s keyword:%s resID:%d resTag:%s\n",
							m_len_min_map[type][i], len, m_len_max_map[type][i], one_xpath.c_str(),
							Util::utf8_to_gbk(keyword).c_str(),req->res_id,req->tag.c_str()));
				xmlXPathFreeObject(xpathObj);
				xmlXPathFreeContext(xpathCtx);
				errinfo = one_xpath;
				pthread_rwlock_unlock(&m_length_rwlock);
				return 1;
			}
			//xmlFree(val);
		}
		xmlXPathFreeObject(xpathObj);
	}
	pthread_rwlock_unlock(&m_length_rwlock);

	if(formMap.size() != 0) {
		printf("form size %d\n",(int) formMap.size());
	}
	for(std::map<int,std::vector<int> >::iterator it = formMap.begin();it != formMap.end();it++)
	{
		int form_len =0;

		for(std::vector<int>::iterator vit = it->second.begin();vit != it->second.end();vit++)
		{
			//printf("%d\t",(*vit));
			form_len += (*vit); 
		}
		if(form_len < form_len_min || form_len > form_len_max)
		{
			SS_DEBUG((LM_ERROR,"check length error in a form: %d<=%d<=%d %s\n",
						form_len_min,form_len,form_len_max,Util::utf8_to_gbk(form_xpath).c_str()));
			xmlXPathFreeContext(xpathCtx);
			errinfo = form_xpath;
			return 1;
		}
	}

	//Cleanup of XPath data 
	xmlXPathFreeContext(xpathCtx);

	fprintf(stderr, "check_length done.\n");
	return 0;
}

int CWriteTask::check_blacklist(int class_id, std::string key, CMysql &mysql_vi)
{
	CMysqlQuery query(&mysql_vi);
	char sql[2048];
    if (key.size() > 1800) {
        SS_DEBUG((LM_ERROR, "CWriteTask::check_blacklist(%@):key too long, classid:%d, key word:%s\n",this,class_id,key.c_str()));
        return -1;
    }
	sprintf(sql, "select * from vr_blacklist where classid = %d and keyword = '%s'", class_id, key.c_str());
	int ret = query.execute(sql);
	while( ret == -1 ) {
		if(mysql_vi.errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CWriteTask::check_blacklist(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql, mysql_vi.error(), mysql_vi.errorno()));
			return -1;
		}
		ret = query.execute(sql);
	}
	SS_DEBUG((LM_DEBUG, "CWriteTask::check_blacklist(%@):Mysql[%s] success\n",this, sql ));

	return query.rows();
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

void CWriteTask::addProp(xmlNodePtr root,std::string &index_level)
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

std::string CWriteTask::compute_cksum_value(const std::string& str)
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

int CWriteTask::process_customer_res(request_t *req, std::string &xml, CMysql &mysql_cr)
{
	//return 0;
	std::string::size_type pos = xml.find("<item>");
	std::string::size_type next_pos = 0;
	int item_num = 0;
	int write_num = 0;
	int data_num = 0;
	time_t now = time(NULL);
	struct tm tm;
	char time_str[64];
	char buf[MAX_ITEM_LEN];

	char *data_md5_hex_old;
	unsigned char * data_md5;
	localtime_r(&now, &tm);
	strftime(time_str, sizeof(time_str), "%F %T", &tm);

	std::string      err_info;
	unsigned char *  md5;
	char            *md5_hex_old;
	char             md5_hex[33];
	std::string key_word;

	char data_md5_hex[33];
	int data_status = 0;
	char             time_tmp[64];
	strftime(time_tmp, sizeof(time_tmp), "%F %T", &tm);

	md5 = MD5((const unsigned char*)xml.c_str(), xml.length(), NULL);
	hexstr(md5, md5_hex, MD5_DIGEST_LENGTH);
	SS_DEBUG((LM_TRACE,"customer resource: MD5 sum is %us MD5_hex sum is %s for %s\n",md5,md5_hex,req->url.c_str()));

	CMysqlQuery query_vrtmp(&mysql_cr);
	sprintf(buf,"select md5_sum from customer_resource where id = %d",req->res_id);
	if( sql_read( query_vrtmp, mysql_cr, buf, 2 ) < 1)
	{
		SS_DEBUG((LM_ERROR,"failed to get auto_check field of resource %d.\n",req->res_id));
		return -1;
	}
	else
	{
		CMysqlRow * cr_row = query_vrtmp.fetch();
		md5_hex_old = cr_row->fieldValue("md5_sum");
		SS_DEBUG((LM_DEBUG,"MD5 sum of resource %d url %s is %s\n",req->res_id, req->url.c_str(),md5_hex_old));
	}
	if(md5_hex_old && !strncmp(md5_hex,md5_hex_old,32) && req->is_manual == false)
	{ //md5 not changed and won't be deleted automaticlly       
		sprintf(buf, "update customer_resource set fetch_time = '%s',manual_update = 0 where id = %d ",time_tmp,req->res_id);
		int retmp;
		while((retmp=sql_write( mysql_cr, buf, 2 )) < 1){
			SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@)fail to Update customer_resource %d update_date error code: %d!\n", this, req->res_id, retmp));
			if( 0 == retmp )
				break;
		}

		SS_DEBUG((LM_TRACE,"CWriteTask::process_customer_res(%@)Resource %d remain unchanged, finish processing. %s\n",this, req->res_id, buf));
		return 2;
	}

	while( std::string::npos != pos && !ACE_Reactor::instance()->reactor_event_loop_done() ){
		//item_num ++;
		int ret = 0;
		std::string::size_type end_pos = xml.find("</item>", next_pos);
		if( std::string::npos == end_pos ){
			SS_DEBUG((LM_ERROR, "</item> not found error: %s\n", req->url.c_str()));
			break;
		}
		item_num ++;
		std::string item = xml.substr( pos, (end_pos+strlen("</item>")-pos) );
		data_md5 = MD5((const unsigned char *)item.c_str(),item.length(),NULL);
		hexstr(data_md5,data_md5_hex, MD5_DIGEST_LENGTH);

		// extract key
		std::string::size_type key_start = item.find("<key><!CDATA[");
		std::string::size_type key_end = item.find("]]></key>");
		std::stringstream ss;
		std::string keytmp;

		if(key_start == -1 || key_end == -1)
		{
			key_start = item.find("<key>");
			key_end = item.find("</key>");
			if(key_start == -1 || key_end == -1)
			{
				SS_DEBUG((LM_ERROR,"CWriteTask::process_customer_res(%@): key find failed in item : %s\n",this,item.substr(0,255).c_str()));
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

		keytmp = item.substr(key_start,key_end);
		SS_DEBUG((LM_DEBUG,"keyword splited is %s\n",keytmp.c_str()));

		key_word = keytmp;
		replace_all_distinct(key_word, "\\","\\\\");
		replace_all_distinct(key_word, "\'","\\\'");

		sprintf(buf, "select * from customer_item where res_id = %d and keyword = '%s'", req->res_id, key_word.c_str());
		fprintf(stdout, "sql:%s\n",buf);
		ret = sql_read( query_vrtmp, mysql_cr, buf, 2 );
		data_status = 0;
		int cur_item_len ;
		cur_item_len= item.length();
		if (cur_item_len >= MAX_ITEM_LEN)
			goto contin;
		if( 0 == ret)
		{// insert
			int check_status = 0;
			//return 0;
			sprintf(buf, "insert into customer_item (res_id, keyword, xml, check_status, create_time, modify_time, md5_sum) values (%d, '%s', '%s', %d, '%s', '%s', '%s')", 
					req->res_id, key_word.c_str(), item.c_str(), check_status,time_str, time_str, data_md5_hex);

			int retmp = 0;
			if((retmp=sql_write( mysql_cr, buf, 2 )) < 1){
				SS_DEBUG((LM_ERROR, "Insert into customer_item error: %d resource ID: %d!\n", retmp,req->res_id));
				goto contin;
			}
			write_num ++;
			data_num ++;

		}
		else
		{
			sprintf(buf, "select md5_sum from customer_item where res_id = %d and keyword ='%s'",
					req->res_id, key_word.c_str());
			CMysqlQuery query_tmp(&mysql_cr);
			if( sql_read( query_tmp, mysql_cr, buf, 2 ) < 1 ){
				SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@): select query_tmp error!\n", this ));
				goto contin;
			}
			//int         data_id;
			CMysqlRow * data_row = query_tmp.fetch();
			data_md5_hex_old = data_row->fieldValue("md5_sum");

			bool no_change(true);
			if(data_md5_hex_old && (strncmp(data_md5_hex, data_md5_hex_old, 32) || strlen(data_md5_hex_old) == 0))
			{

				int check_status = 0;       

				sprintf(buf, "update customer_item set check_status = %d, modify_time = '%s', md5_sum='%s', xml = '%s' where res_id = %d and keyword = '%s'",
						check_status, time_str,data_md5_hex,item.c_str(),req->res_id, key_word.c_str() );
				int retmp = 0;
				if((retmp=sql_write( mysql_cr, buf, 2 )) < 0){
					SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@): update customer_res id:%d key:%s error: %d!\n",
								this, req->res_id, key_word.c_str(), retmp));
					goto contin;
				}
				write_num ++;
				no_change = false; 
			}
			else
			{
				sprintf(buf, "update customer_item set modify_time = '%s' where res_id = %d and keyword = '%s'",
						time_str,req->res_id, key_word.c_str() );
				int retmp = 0;
				if((retmp=sql_write( mysql_cr, buf, 2 )) < 0){
					SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@): update customer_res id:%d key:%s error: %d!\n",
								this, req->res_id, key_word.c_str(), retmp));
					goto contin;
				}
			}  
			data_num ++;
		}
contin:

		next_pos = end_pos + strlen("</item>");
		pos = xml.find("<item>", next_pos);

	}
	if (data_num > 0 )
	{
		sprintf(buf, "update customer_resource set fetch_time = '%s', update_time = '%s', md5_sum = '%s', manual_update = 0 where id = %d", time_str, time_str, md5_hex,req->res_id );
		int retmp = 0;
		if((retmp=sql_write( mysql_cr, buf,2 )) < 1){
			SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@): Update customer_resource error: %d!\n", this, retmp));
		}
	}

	if (item_num == 0 )
	{
		sprintf(buf, "update customer_resource set res_status = 5, check_info = 'no item in xml', manual_update = 0, fetch_time = '%s' where id = %d", time_str, req->res_id );
		sql_write( mysql_cr, buf, 2 );
		SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@): error: %d-%s-%s, set res_status = 0 \n", this,
					req->res_id, req->tag.c_str(), req->url.c_str() ));
	}
	SS_DEBUG((LM_ERROR, "CWriteTask::process_customer_res(%@):item_num:%d write_num:%d!\n", this, item_num, write_num));

	//remove the url from the fetch_map for return value 0
	std::string unqi_key = "customer#"+req->url;
	pthread_mutex_lock(&fetch_map_mutex);//lock
	if( fetch_map.find(unqi_key) != fetch_map.end() && fetch_map[unqi_key] > 0 ){
		fetch_map[unqi_key] = 0;
	}else{
		SS_DEBUG(( LM_ERROR, "CWriteTask::svc(%@): map error: %d %s\n",this, fetch_map[unqi_key], req->url.c_str() ));
	}
	pthread_mutex_unlock(&fetch_map_mutex);//unlock
	return 0;

}

//lflag表示当前req是否正确处理
void CWriteTask::req_done(request_t *req,CMysql &mysql_vr, int fetch_status, CMysqlHandle *thisHandler, ProcessFlag lflag,int write_count)
{
	bool parseOk = true;
	std::string err_info(req->res_name + ":" + req->debugInfo);
	std::string log_msg;
	std::string detail;
	std::string temp_msg;
	CMysqlQuery query(&mysql_vr); 
	if(lflag == ERR)
	{
		detail.append(err_info);
		detail.append("<br>");
		parseOk = false;
	}
	//处理跳首页的url，item数为0，需要监控
	if(lflag==SUCCESS && req->citemNum==0)
	{
		detail.append(err_info);
		detail.append(", [");
		detail.append(req->ds_name);
		detail.append(Util::gbk_to_utf8(":item数为0]<br>",":item num is zero]<br>"));
		if(fetch_status>4 || fetch_status==1)
			fetch_status = 4;
		parseOk = false;
	}
	//如果解出来的key的个数不为0，但是入库比例小于20%,那么报警
	if(req->citemNum!=0 && lflag==SUCCESS)
	{
		float failRate = 0.0f;
		failRate = static_cast<float>(req->failedKeyCount)/req->citemNum;
		if(failRate > 0.8f)
		{
			char temp[256];
			snprintf(temp, 255, "%0.2f%%", failRate*100);
			log_msg.assign(err_info);
			log_msg.append(Util::gbk_to_utf8(":<校验失败的item>",":<checked fail item>"));
			string truncKeys_msg(log_msg);
			if(req->failedKeyCount <= 20)
			{
				truncKeys_msg.append(req->failedKeyWord);
			}
			else
			{
				string::size_type start = 0;
				string::size_type pos = 0;
				for(int i=0; i<20; ++i)
				{
					pos = req->failedKeyWord.find(Util::gbk_to_utf8("、"), start);
					if(pos == string::npos)
						break;
					else
					{
						start = pos + 2;
					}

				}
				truncKeys_msg.append(req->failedKeyWord.substr(0, pos));
			}
			log_msg.append(req->failedKeyWord);
			log_msg.append(Util::gbk_to_utf8("</校验失败的item>:rate:","</check fail item>:rate:"));
			log_msg.append(temp);
			temp_msg.assign(" [");
			temp_msg.append(req->ds_name);
			temp_msg.append(Util::gbk_to_utf8(":有效item个数低于20%]",":effective item num less than 20%]"));
			log_msg.append(temp_msg);
			truncKeys_msg.append(Util::gbk_to_utf8("</校验失败的item>:rate:","</check fail item>:rate:"));
			truncKeys_msg.append(temp);
			truncKeys_msg.append(temp_msg);
			SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@)failed keys %s\n", this, Util::utf8_to_gbk(truncKeys_msg).c_str()));
			detail.append(log_msg);
			detail.append("<br>");
			if(fetch_status>4 || fetch_status==1)
				fetch_status = 4;
			parseOk = false;
		}
	}

	if(parseOk)
	{
		req->debugInfo.append(", [");
		req->debugInfo.append(req->ds_name);
		req->debugInfo.append(Util::gbk_to_utf8(":xml解析成功]",":xml parse OK]"));
	}

	pthread_mutex_lock(&counter_mutexes[req->res_id % 10]);

	req->counter_t->fetch_detail.append(detail);

    //如果发生变化，插入一条新数据..
    if(req->is_changed || req->is_manual)
    {
		char buf[512];
        snprintf(buf,512,"select id from vr_update_info where fetch_time = '%s' and vr_id = %d",req->fetch_time,req->class_id);
        
        int ret = -1;
        alarm(30);
        ret = query.execute(buf);
        alarm(0);
        while( ret == -1 ) 
        {
            if(mysql_vr.errorno() != CR_SERVER_GONE_ERROR) {
                SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
                        mysql_vr.error(), mysql_vr.errorno()));
	            pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);
                return ;
            }
            alarm(30);
            ret = query.execute(buf);
            alarm(0);
        }
        int record_count = query.rows();
		//对于不存在的数据，插入一条新数据，用于表示该VR有数据更新..
        if(record_count == 0)
        {
            snprintf(buf,512,"insert into vr_update_info(vr_id,status,fetch_time,create_time) values(%d,1,'%s',NOW())",req->class_id,req->fetch_time);

            int retmp;
            while((retmp=sql_write( mysql_vr, buf )) < 1) 
            {
                SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@)fail to Update vr_resource %d update_date error code: %d!\n",
                        this, req->res_id, retmp));
                if( 0 == retmp )
                    break;
            }
        }
    }

	if( req->counter_t->getCount() == 0)//已经处理完所有的url了
	{
		char buf[512];
		//resource处理完所有的url以后更新update_date时间
		//可以在这里判断是否为所有的url都抓取失败，如果是，那么可以直接更新key的update_date
		snprintf(buf, 512, "update vr_resource set update_date = '%s',manual_update=0 where id = %d",req->fetch_time, req->res_id);
		int retmp;
		while((retmp=sql_write( mysql_vr, buf )) < 1) 
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@)fail to Update vr_resource %d update_date error code: %d!\n",
						this, req->res_id, retmp));
			if( 0 == retmp )
				break;
		}
		//对于已经处理完成的VR，更新一下状态，并且更新一下时间..
		snprintf(buf,512,"update vr_update_info set status = 2 where fetch_time = '%s' and vr_id = %d and status=1",req->fetch_time,req->class_id);
		while((retmp=sql_write( mysql_vr, buf )) < 1) 
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@)fail to Update vr_update info %d update_date error code: %d!\n",
						this, req->res_id, retmp));
			if( 0 == retmp )
				break;
		}

		//一类vr或者一个数据源有80%的url抓取或者处理失败了，需要监控
		if(lflag != ERR)
		{
			req->counter_t->ds_counter[req->ds_id]->success_count += 1;
		}

		float succ_count;
		std::map<int, DataSourceCounter*>::iterator itr;
		std::string fail_url_msg;
		for(itr=req->counter_t->ds_counter.begin(); itr!=req->counter_t->ds_counter.end(); ++itr)
		{
			succ_count = itr->second->success_count;
			if(itr->second->total_count!=0 && succ_count/itr->second->total_count<0.2f)
			{
				fail_url_msg.append("[");
				fail_url_msg.append(itr->second->ds_name);
				fail_url_msg.append(Util::gbk_to_utf8(":有效url个数低于20%]",":effective url num less 20%]"));
			}
			//如果没有一个url处理成功，则更新key的时间
			if(itr->second->success_count == 0)
			{
				snprintf(buf, 512, "update vr_data_%d set update_date = '%s' where res_id = %d and data_source_id=%d",
						req->data_group, req->fetch_time, req->res_id, itr->first);
				int retmp = 0;
				while((retmp=sql_write( mysql_vr, buf )) < 1)
				{
					SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@): update Update vr_data_%d error: %d!\n",
								this, req->data_group, retmp));
					if(0 == retmp)
						break;
				}
			}
		}
		if(fail_url_msg.size() > 0)
		{
			log_msg.assign(req->res_name);
			log_msg.append(":");
			log_msg.append(req->counter_t->debugInfo);
			log_msg.append(req->debugInfo);
			log_msg.append(fail_url_msg);
			req->counter_t->fetch_detail.append(log_msg);
			req->counter_t->fetch_detail.append("<br>");
			if(fetch_status>4 || fetch_status==1)
				fetch_status = 4;

		}
		//一类vr或者一个数据源item数减少超过80%
		int last_item_num=0, current_item_num=0;
		req->counter_t->ds_counter[req->ds_id]->current_item_num += write_count;
		float curr_num;
		std::string failed_item_msg;
		for(itr=req->counter_t->ds_counter.begin(); itr!=req->counter_t->ds_counter.end(); ++itr)
		{
			curr_num = itr->second->current_item_num;
			current_item_num	+= curr_num;
			last_item_num		+= itr->second->last_item_num;
			if(!req->is_manual)
			{
				if(itr->second->last_item_num != 0 && curr_num/itr->second->last_item_num<0.2f)
				{
					failed_item_msg.append("[");
					failed_item_msg.append(itr->second->ds_name);
					failed_item_msg.append(Util::gbk_to_utf8(":item数较前次减少幅度大于80%]",":item num less than 80%]"));

					//update the update_date
					snprintf(buf, 512, "update vr_data_%d set update_date = '%s' where res_id = %d and data_source_id=%d",
							req->data_group, req->fetch_time, req->res_id, itr->first);
					int retmp = 0;
					while((retmp=sql_write( mysql_vr, buf )) < 1)
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@): update Update vr_data_%d error: %d!\n",
									this, req->data_group, retmp));
						if(0 == retmp)
							break;
					}
				}
			}
		}
		if(last_item_num != current_item_num)
		{
			char buf[512];
			snprintf(buf,512,"select id from vr_update_info where fetch_time = '%s' and vr_id = %d",req->fetch_time,req->class_id);
			
			int ret = -1;
			alarm(30);
			ret = query.execute(buf);
			alarm(0);
			while( ret == -1 ) 
			{
				if(mysql_vr.errorno() != CR_SERVER_GONE_ERROR) {
					SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
							mysql_vr.error(), mysql_vr.errorno()));
					return ;
				}
				alarm(30);
				ret = query.execute(buf);
				alarm(0);
			}
			int record_count = query.rows();
			//对于不存在的数据，插入一条新数据，用于表示该VR有数据更新..
			if(record_count == 0)
			{
				snprintf(buf,512,"insert into vr_update_info(vr_id,status,fetch_time,create_time) values(%d,2,'%s',NOW())",req->class_id,req->fetch_time);

				int retmp;
				while((retmp=sql_write( mysql_vr, buf )) < 1) 
				{
					SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@)fail to Update vr_resource %d update_date error code: %d!\n",
							this, req->res_id, retmp));
					if( 0 == retmp )
						break;
				}
			}
		}
		
		if(failed_item_msg.size() > 0)
		{
			log_msg.assign(req->res_name);
			log_msg.append(":");
			log_msg.append(req->counter_t->debugInfo);
			log_msg.append(req->debugInfo);
			log_msg.append(failed_item_msg);
			req->counter_t->fetch_detail.append(log_msg);
			req->counter_t->fetch_detail.append("<br>");
			if(fetch_status>4 || fetch_status==1)
				fetch_status = 4;
		}

        //item数量过多 或 urls过多 修改 vr_resource的priority字段
        if( current_item_num>MAXIMEMNUNBER || (req->counter_t->getTotalCount()>MAXURLSNUMBER && current_item_num>MAXURLSITEMNUMBER) )
        {
            SS_DEBUG((LM_INFO, "CWriteTask::req_done(%@): large vr, items[%d] > %d, or (urls[%d] > %d and items[%d] > %d, vr_id %d\n", this, 
			current_item_num, MAXIMEMNUNBER, req->counter_t->getTotalCount(), MAXURLSNUMBER, current_item_num, MAXURLSITEMNUMBER, req->class_id));
            sub_request_t *large_vr_sub_req = new sub_request_t();
            large_vr_sub_req->res_id = req->res_id;
            large_vr_sub_req->status_id = req->status_id;
            large_vr_sub_req->timestamp_id = req->timestamp_id;
            large_vr_sub_req->is_large_vr = true;
            large_vr_sub_req->is_footer = false;
            large_vr_sub_req->updateType = OTHER;

            ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(large_vr_sub_req) );
            thisHandler->put(msg);
            SS_DEBUG((LM_TRACE, "large vr, put msg res_id %d, staus_id %d large vr%d\n", large_vr_sub_req->res_id, large_vr_sub_req->status_id,  large_vr_sub_req->is_large_vr));
        }

		//记录item数量
		float rate;
		if(last_item_num > 0)
		{
			rate = (float)(current_item_num - last_item_num) / last_item_num;
			rate *= 100;
			snprintf(buf, 512, "update process_timestamp set current_item_num=%d, last_item_num=%d, rate=%f where id=%d", current_item_num, last_item_num, rate, req->timestamp_id);
		}
		else
		{
			snprintf(buf, 512, "update process_timestamp set current_item_num=%d, last_item_num=%d where id=%d", current_item_num, last_item_num, req->timestamp_id);
		}
		while((retmp=sql_write(mysql_vr, buf)) < 1)
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::req_done(%@): execute sql:%s , error: %d!\n", this, buf, retmp));
			if(0 == retmp)
				break;
		}
		


		//update fetch status
		if(fetch_status!=1 && (fetch_status<=req->counter_t->error_status || req->counter_t->error_status==1))
		{
			req->counter_t->error_status = fetch_status;
		}
		if(fetch_status != 1)
		{
			Util::update_fetchstatus(&mysql_vr, req->res_id, req->counter_t->error_status, req->counter_t->fetch_detail);
			//SS_DEBUG((LM_TRACE, "CWriteTask::req_done(%@): %s\n", this, req->counter_t->fetch_detail.c_str()));
		}
 		
		//删除没有被更新的doc_id
		std::map<std::string, std::string>::iterator map_it;
		VrdeleteData *deleteData = new VrdeleteData();
		for(map_it=req->counter_t->m_docid.begin(); map_it!=req->counter_t->m_docid.end(); map_it++)
		{
			deleteData->v_docid.push_back(map_it->first);
		}
		deleteData->class_id = req->class_id;
		deleteData->data_group = req->data_group;
		deleteData->res_id = req->res_id;
		deleteData->fetch_time = req->fetch_time_sum;
		deleteData->status = "DELETE";
		deleteData->timestamp_id = req->timestamp_id;
		if (deleteData->v_docid.size() > 0)
		{
			ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(deleteData) );
			CDeleteTask::instance()->put(msg);
		}
		else
		{
			delete deleteData;
		}
		
		delete req->counter_t;
		req->counter_t = NULL; 


		//todo:move file from tmp to final

		// delete item from redis
		
		pthread_mutex_lock(&unique_set_name_mutex);
		int redis_ret = delete_redis_set(req->res_id, req->class_id, req->is_manual, req->data_group, redis_tool);
		SS_DEBUG((LM_TRACE, "CWriteTask::req_done(%@): delete_redis_set id :%d, ret: %d\n", this, req->res_id, redis_ret));
		pthread_mutex_unlock(&unique_set_name_mutex);
	}
	else
	{
		//update fetch status in the counter, fetch_status=1 means fetching and no error
		if(fetch_status!=1 && (fetch_status<=req->counter_t->error_status || req->counter_t->error_status==1))
		{
			req->counter_t->error_status = fetch_status;
		}
		if(fetch_status != 1)
		{
			Util::update_fetchstatus(&mysql_vr, req->res_id, req->counter_t->error_status, req->counter_t->fetch_detail);
			//SS_DEBUG((LM_TRACE, "CWriteTask::req_done(%@): %s\n", this, req->counter_t->fetch_detail.c_str()));
		}
		bool flag = true;
		if(lflag == ERR)
			flag = false;
		req->counter_t->minus(flag,write_count,req->debugInfo, req->ds_id);
	}
	pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);
}

int CWriteTask::delete_redis_set(int res_id, int vr_id, bool is_manual, int data_group, RedisTool *redis_tool)
{
	std::stringstream ss;
    ss << res_id << "#" << vr_id << "#" << is_manual << "#" << data_group;
    std::string value = ss.str();
	std::stringstream ss_set;
	ss_set << res_id;
	std::string set_value = ss_set.str();
	return delete_redis_set(set_value, value, redis_tool);
}

int CWriteTask::delete_redis_set(std::string set_value, std::string value, RedisTool *redis_tool)
{
	int redis_ret = -1;
    //redis_tool->SetListSetName(ACTION_FETCH);
    while (redis_ret < 0)
    {
        if (!redis_tool->GetConnected())
        {
            redis_tool->Connect();
        }
        redis_ret = redis_tool->DeleteSet(set_value);
        if (redis_ret < 0)
        {
            SS_DEBUG((LM_TRACE, "CWriteTask::delete_redis_set(%@): [ignore_vr]delete_redis_set redis error, value : %s %d\n",this, value.c_str(), redis_ret ));
            redis_tool->SetConnected(false);
            redis_tool->Close();
        }
        else
        {
            SS_DEBUG((LM_TRACE, "CWriteTask::delete_redis_set(%@): [ignore_vr]delete_redis_set from redis, value : %s %d\n",this,  value.c_str(), redis_ret ));
        }
    } 
	redis_ret = -1;
	while (redis_ret < 0)
    {
        if (!redis_tool->GetConnected())
        {
            redis_tool->Connect();
        }
        redis_ret = redis_tool->DeleteSet(unique_set_name, value);
        if (redis_ret < 0)
        {
            SS_DEBUG((LM_TRACE, "CWriteTask::delete_redis_set(%@): [ignore_vr]delete_redis_unique_set redis error, value : %s %d\n",this, value.c_str(),redis_ret ));
            redis_tool->SetConnected(false);
            redis_tool->Close();
        }
        else
        {
            SS_DEBUG((LM_TRACE, "CWriteTask::delete_redis_set(%@): [ignore_vr]delete_redis_unique_set from redis, value : %s %d\n",this, value.c_str(),redis_ret ));
        }
		
    } 
	return 0;
}

/*
 *判断write开始时间是否为0，若为0，则返回true，否则返回false
 */
bool CWriteTask::needUpdate(CMysql *mysql, int timestamp_id, const std::string &field)
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
			SS_DEBUG((LM_ERROR,"CScanTask::get_resource(%@):Mysql[]failed, reason:[%s],errorno[%d]\n",this,
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

static void utf8_to_lower(std::string &key)
{
	std::bstring tmp_key;
	Util::UTF82UTF16(key,key);
	tmp_key.assign((bchar_t*)key.c_str(),key.length()/2);
	int temp_len = tmp_key.length();
	bchar_t tempCh;
	bchar_t* tempStr = new bchar_t[temp_len+1];
	for(int i=0; i<temp_len; i++)
	{
		tempCh = unicode_toLower(unicode_toHalf(tmp_key.c_str()[i]));
		tempStr[i] = tempCh;
	}
	tempStr[temp_len] = 0;
	key.assign((char*)tempStr,bcslen(tempStr)*2);
	Util::UTF162UTF8(key,key);
	delete []tempStr;
}

std::string comments_begin = "<!--";
std::string comments_end = "-->";

//处理每个req(即url)抓取回来的结果xml
int CWriteTask::process(request_t *req, std::string &xml, 
		CMysql &mysql_vr, CMysql &mysql_vi,CMysql &mysql_cr,
		//std::map <int,xmlSchemaValidCtxt*> validityContextMap, 
		char* sbuf, time_t &preprocess_cost,
		unsigned int & update_statistic, unsigned int& insertion_statistic , 
		struct mysql_cost &sql_cost, struct libxml_cost &xml_cost, time_t& xml_md5_cost,
		time_t&insert_update_cost, time_t& qdb_cost, time_t &schema_check_cost, 
		CMysqlHandle *thisHandler, CurlHttp &curl)
{
	//记录第一条url的开始处理时间
	bool is_toupdate = false;
	is_toupdate = needUpdate(&mysql_vr, req->timestamp_id, "write_start_time");
	if(is_toupdate)
		Util::save_timestamp(&mysql_vr, req->timestamp_id, "write_start_time");

	std::string errinfo;
	//1.验证抓取的正确性

	timeval  preprocess_beg;
	gettimeofday(&preprocess_beg,NULL);
	if(req->error)
	{
		SS_DEBUG((LM_ERROR,"[CWriteTask::process][%s]fetch ERROR\n",req->url.c_str()));
		if(req->ds_id == 0)
			req->debugInfo.append(Util::gbk_to_utf8(",[xml无法访问]",",[xml file can not access]"));
		else
		{
			req->debugInfo.append(",[");
			req->debugInfo.append(req->ds_name);
			req->debugInfo.append(Util::gbk_to_utf8(":xml无法访问]",":xml file can not access]"));
		}

		//保留上一次的数据
		VrdeleteData *  vr_data = new VrdeleteData();
		vr_data->class_id = req->class_id;
		vr_data->res_id = req->res_id;
		vr_data->data_group = req->data_group;
		vr_data->fetch_time = req->fetch_time;
		vr_data->status = "UPDATE";
		vr_data->url = req->url;
		vr_data->status_id = req->status_id;
		vr_data->timestamp_id = req->timestamp_id;

		pthread_mutex_lock(&counter_mutexes[req->res_id % 10]);
		req->citemNum = req->counter_t->m_UrlDocid[req->status_id].size();

		//只处理7开头的vr
		if (req->class_id >= 70000000)
		{
			for(int i=0; i<req->citemNum ; i++)
			{
				std::string doc_id = req->counter_t->m_UrlDocid[req->status_id][i];
				vr_data->v_docid.push_back(doc_id);
			}
		}	
		for(int i=0; i<req->citemNum ; i++)
		{
			std::string doc_id = req->counter_t->m_UrlDocid[req->status_id][i];
			if (req->counter_t->m_docid.find(doc_id) != req->counter_t->m_docid.end())
			{
				req->counter_t->m_docid.erase(doc_id);
			}
		}
		pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);
		
		SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)url error, vr_id %d, record_count : %d, size : %d , begin processing.\n", this, req->class_id, req->citemNum , vr_data->v_docid.size()));

		ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(vr_data) );
		CDeleteTask::instance()->put(msg);

		req_done(req,mysql_vr, 3, thisHandler, ERR);
		return 0;
	}

	ACE_Time_Value testTime1,testTime2,testTime3,testTime4,testTime5;

	testTime1=ACE_OS::gettimeofday();
	//1.验证xml数据

	std::string::size_type next_pos = 0;
	int item_num = 0;
	int write_num = 0;
	time_t now = time(NULL);
	struct tm tm;
	char time_str[64];
	char buf[1024];
	localtime_r(&now, &tm);
	strftime(time_str, sizeof(time_str), "%F %T", &tm);
	int auto_check ;

	std::string      err_info="";
	unsigned char *  md5;
	char            *md5_hex_old;
	char             md5_hex[33];
	//int data_status = 0;
	char             time_tmp[64];
	std::string      location;
	bool			is_changed = false;
	location = "";
	char docIdbuf[33];
	char tmpDocIdbuf[32];
	gDocID_t docid;
	int schema_fail_cnt = 0;
	ACE_Time_Value t1,t2;
	t1 = ACE_OS::gettimeofday();
	std::string::size_type tpos;
	int class_id_convert = req->class_id;
	if(req->class_id < 1000000)
	{
		int remain = req->class_id % 100000;
		int high = req->class_id / 100000;
		class_id_convert = (high * 10000000) + remain;
	}
	strftime(time_tmp, sizeof(time_tmp), "%F %T", &tm);
	SS_DEBUG((LM_DEBUG,"CWriteTask: resource url:%s\n",req->url.c_str()));

	timeval  tplid_check_beg;
	gettimeofday(&tplid_check_beg,NULL);

	if(req->tplid < 1)
	{
		req->debugInfo.append(",tplid<1");
		SS_DEBUG((LM_ERROR,"resource url:%s template id %d  is less than 1.\n",req->url.c_str(),req->tplid));
		int retmp;
		timeval  sql_beg1;
		gettimeofday(&sql_beg1,NULL);

		sprintf(buf, "update vr_resource set manual_update = 0 where id = %d", req->res_id);
		while((retmp=sql_write( mysql_vr, buf )) < 1)
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update vr_resource %d "
						"reason:[%s] update_date error code: %d! %s\n", this, req->res_id,mysql_vr.error(), retmp,buf));
			if( 0 == retmp )
				break;
		}
		timeval sql_end1;
		gettimeofday(&sql_end1, NULL);
		time_t sql_cost1;
		sql_cost1 = 1000000*(sql_end1.tv_sec - sql_beg1.tv_sec) + sql_end1.tv_usec - sql_beg1.tv_usec;
		//fprintf(stderr, "Time Detector scan SQL2 get vr_resource %lld \n", sql_cost2);
		sql_cost.write_cost += sql_cost1;	
		sql_cost.total_cost += sql_cost1;

		sprintf(buf,"update  resource_status set  res_status = 4 where id = %d", req->status_id);
		timeval  sql_beg2;
		gettimeofday(&sql_beg2,NULL);
		while((retmp=sql_write( mysql_vr, buf )) < 1)
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update resource_status %d update_date error code: %d!\n",
						this, req->res_id, retmp));
			if( 0 == retmp )
				break;
		}
		timeval sql_end2;
		gettimeofday(&sql_end2, NULL);
		time_t sql_cost2;
		sql_cost2 = 1000000*(sql_end2.tv_sec - sql_beg2.tv_sec) + sql_end2.tv_usec - sql_beg2.tv_usec;
		//fprintf(stderr, "Time Detector Write SQL1 %lld \n", sql_cost1);
		sql_cost.write_cost += sql_cost2;
		sql_cost.total_cost += sql_cost2;

		if(req->data_from == 4)
		{
			sprintf(buf, "update customer_resource set res_status = 5 , check_info = 'tplid lower than 1' where online_id = %d", req->res_id);
			timeval  sql_beg3;
			gettimeofday(&sql_beg3,NULL);
			while((retmp = sql_write(mysql_cr, buf, 2)) < 1)
			{

				SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update customer_resource resource_status %d "
							"update_date error code: %d!\n", this, req->res_id, retmp));
				if( 0 == retmp )
					break;
			}
			timeval sql_end3;
			gettimeofday(&sql_end3, NULL);
			time_t sql_cost3;
			sql_cost3 = 1000000*(sql_end3.tv_sec - sql_beg3.tv_sec) + sql_end3.tv_usec - sql_beg3.tv_usec;
			//fprintf(stderr, "Time Detector scan SQL2 get vr_resource %lld \n", sql_cost3);
			sql_cost.write_cost += sql_cost3;
			sql_cost.total_cost += sql_cost3;
		} 
		req_done(req, mysql_vr, 4, thisHandler, ERR);
		SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)Resource %d template id %d is invalid, set res_status = 4, "
					"finish processing.\n",this, req->res_id,req->tplid));
		timeval tplid_check_end;
		gettimeofday(&tplid_check_end, NULL);
		time_t tplid_check_cost;
		tplid_check_cost = 1000000*(tplid_check_end.tv_sec - tplid_check_beg.tv_sec) + tplid_check_end.tv_usec - tplid_check_beg.tv_usec;
		fprintf(stderr, "Record statistic tplid_check:	%ld",tplid_check_cost);
		return -1;
	}//end if(req->tplid < 1)
	else
	{
		timeval tplid_check_end;
		gettimeofday(&tplid_check_end, NULL);
		time_t tplid_check_cost;
		tplid_check_cost = 1000000*(tplid_check_end.tv_sec - tplid_check_beg.tv_sec) + tplid_check_end.tv_usec - tplid_check_beg.tv_usec;
		fprintf(stderr, "Record statistic tplid_check:  %ld",tplid_check_cost);
	}


	timeval  xml_md5_check_beg;
	gettimeofday(&xml_md5_check_beg,NULL);
	md5 = MD5((const unsigned char*)xml.c_str(), xml.length(), NULL);
	hexstr(md5, md5_hex, MD5_DIGEST_LENGTH);
	SS_DEBUG((LM_TRACE,"MD5 sum is %us MD5_hex sum is %s for %s\n",md5,md5_hex,req->url.c_str()));

	std::string url_id_str;

	CMysqlQuery query_vrtmp(&mysql_vr);
	sprintf(buf,"select auto_check, md5_sum from  vr_resource as t1 inner join  resource_status as t2 on t1.id = t2.res_id where t1.id = %d and t2.id = %d",req->res_id,req->status_id);
	timeval  sql_beg7;
	gettimeofday(&sql_beg7,NULL);
	if( sql_read( query_vrtmp, mysql_vr, buf ) < 1)
	{
		timeval sql_end7;
		gettimeofday(&sql_end7, NULL);
		time_t sql_cost7;
		sql_cost7 = 1000000*(sql_end7.tv_sec - sql_beg7.tv_sec) + sql_end7.tv_usec - sql_beg7.tv_usec;
		//fprintf(stderr, "Time Detector Write SQL5 %lld \n", sql_cost5);
		sql_cost.read_cost += sql_cost7;
		sql_cost.total_cost += sql_cost7;
		SS_DEBUG((LM_ERROR,"[CWriteTask::process]failed to get auto_check field of resource %d.\n",req->res_id));
		req->debugInfo.append(", [get auto_check field failed]");
		req_done(req,mysql_vr,5, thisHandler, ERR);//处理失败的req
		return -1;
	}
	else
	{
		timeval sql_end7;
		gettimeofday(&sql_end7, NULL);
		time_t sql_cost7;
		sql_cost7 = 1000000*(sql_end7.tv_sec - sql_beg7.tv_sec) + sql_end7.tv_usec - sql_beg7.tv_usec;
		//fprintf(stderr, "Time Detector Write SQL2 %lld \n", sql_cost5);
		sql_cost.read_cost += sql_cost7;
		sql_cost.total_cost += sql_cost7;
		CMysqlRow * vr_row = query_vrtmp.fetch();
		auto_check = atoi(vr_row->fieldValue("auto_check"));
		md5_hex_old = vr_row->fieldValue("md5_sum");
		SS_DEBUG((LM_DEBUG,"MD5 sum of resource %d url %s is %s\n",req->res_id, req->url.c_str(),md5_hex_old));
	}


	if((md5_hex_old && strncmp(md5_hex,md5_hex_old,32)) || req->is_manual || req->is_db_error)
	{

		SS_DEBUG((LM_INFO,"need update vr info table cause old_md5=%s,new_md5=%s\n",md5_hex_old,md5_hex));
		is_changed = true;
	}
	req->is_changed = is_changed;

	//if(md5_hex_old && !strncmp(md5_hex,md5_hex_old,32) && req->is_manual == false && ((req->class_id >= 20000000  && req->over24) or (req->class_id < 20000000)))
	//{ //md5 not changed and won't be deleted automaticlly,虽然不重新入库，但是必须更新当前url的所有key的update_date
	//	//防止由于更新时间问题被删除,(OnlineTask只会把status为0的发到线上，但是iq只会删除过期小于24小时且2天没有更新的东西)
	//	char * max_update_date;	
	//	sprintf(buf,"select max(update_date) update_date from vr_data_%d where res_id = %d and status_id = %d",req->data_group,req->res_id,req->status_id);
	//	if( sql_read( query_vrtmp, mysql_vr, buf ) < 1)
	//	{
	//		SS_DEBUG((LM_ERROR,"[CWriteTask::process]failed to get max update_date field of resource %d.\n",req->res_id));
	//		req->debugInfo.append(", [get max update_date field failed]");
	//		req_done(req,mysql_vr,5, thisHandler, ERR);//处理失败的req
	//		return -1;
	//	}
	//	else
	//	{
	//		CMysqlRow * vr_row = query_vrtmp.fetch();
	//		max_update_date = vr_row->fieldValue("update_date");

	//		SS_ERROR((LM_DEBUG,"[CWriteTask::process]MD5 sum of resource %d url %s is %s\n",req->res_id, req->url.c_str(),md5_hex_old));	
	//		//fprintf(stderr, "self test md5_hex %s, md5_hex_old %s\n",md5_hex,md5_hex_old);
	//		sprintf(buf, "update vr_data_%d set update_date = '%s' where res_id = %d and status_id = %d and update_date >= '%s'", 
	//				req->data_group,req->fetch_time,req->res_id, req->status_id ,max_update_date);

	//		//fprintf(stderr, "self test %s\n",buf);
	//		int retmp = 0;
	//		timeval  sql_beg8;
	//		gettimeofday(&sql_beg8,NULL);
	//		while((retmp=sql_write( mysql_vr, buf )) < 1)
	//		{
	//			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): update Update vr_data_%d error: %d!\n",														this, req->data_group, retmp));
	//			if(0 == retmp)
	//				break;
	//		}
	//		timeval sql_end8;
	//		gettimeofday(&sql_end8, NULL);
	//		time_t sql_cost8;
	//		sql_cost8 = 1000000*(sql_end8.tv_sec - sql_beg8.tv_sec) + sql_end8.tv_usec - sql_beg8.tv_usec;
	//		//fprintf(stderr, "Time Detector Write SQL2 %lld \n", sql_cost6);
	//		sql_cost.write_cost += sql_cost8;
	//		sql_cost.total_cost += sql_cost8;
	//		//int retmp;  
	//		sprintf(buf, "update  vr_resource set manual_update = 0 where id = %d", req->res_id);
	//		timeval  sql_beg9;
	//		gettimeofday(&sql_beg9,NULL);
	//		while((retmp=sql_write( mysql_vr, buf )) < 1)
	//		{
	//			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update   vr_resource %d reason:[%s]  "
	//						"update_date error code: %d! %s\n", this, req->res_id,mysql_vr.error(),retmp,buf));
	//			if( 0 == retmp )
	//				break;
	//		}
	//		timeval sql_end9;
	//		gettimeofday(&sql_end9, NULL);
	//		time_t sql_cost9;
	//		sql_cost9= 1000000*(sql_end9.tv_sec - sql_beg9.tv_sec) + sql_end9.tv_usec - sql_beg9.tv_usec;
	//		//fprintf(stderr, "Time Detector Write SQL7 %lld \n", sql_cost7);
	//		sql_cost.write_cost += sql_cost9;
	//		sql_cost.total_cost += sql_cost9;
	//		req->debugInfo.append(Util::gbk_to_utf8(",[xml文件未变化]",",[xml is not changed]"));
	//		//count the items
	//		int count = 0;
	//		std::string::size_type pos = xml.find("<item>");
	//		while(pos != std::string::npos)
	//		{
	//			count++;
	//			pos = xml.find("<item>", pos+strlen("<item>"));
	//		}
	//		req_done(req,mysql_vr,1, thisHandler, UNCHANGED, count);//处理失败的req

	//		SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)Resource %d remain unchanged, finish processing.\n",
	//					this, req->res_id));
	//		timeval xml_md5_check_end;
	//		gettimeofday(&xml_md5_check_end, NULL);
	//		xml_md5_cost += 1000000*(xml_md5_check_end.tv_sec - xml_md5_check_beg.tv_sec) + xml_md5_check_end.tv_usec - xml_md5_check_beg.tv_usec;
	//		//update fetch status to fetch end
	//		Util::update_fetchstatus(&mysql_vr, req->res_id);
	//		return 2;
	//	}
	//}
	//else
	//{
	//	timeval xml_md5_check_end;
	//	gettimeofday(&xml_md5_check_end, NULL);
	//	xml_md5_cost += 1000000*(xml_md5_check_end.tv_sec - xml_md5_check_beg.tv_sec) + xml_md5_check_end.tv_usec - xml_md5_check_beg.tv_usec;
	//}

    SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)url content unchanged is_manual : %d, vr_id : %d, url: %s, status : %d remain unchanged, begin processing.\n", this, req->is_manual, req->class_id, req->url.c_str(), req->xml_format_status));

	// url content unchanged
	if (md5_hex_old && !strncmp(md5_hex,md5_hex_old,32) && req->is_manual == false && req->is_db_error==false)
	{
		VrdeleteData *  vr_data = new VrdeleteData();
		vr_data->class_id = req->class_id;
		vr_data->res_id = req->res_id;
		vr_data->data_group = req->data_group;
		vr_data->fetch_time = req->fetch_time;
		vr_data->status = "UPDATE";
		vr_data->url = req->url;
		vr_data->status_id = req->status_id;
		vr_data->timestamp_id = req->timestamp_id;

		if (req->class_id >= 70000000 && !req->xml_format_status)
		{
			pthread_mutex_lock(&counter_mutexes[req->res_id % 10]);

			req->citemNum = req->counter_t->m_UrlDocid[req->status_id].size();
			for(int i=0; i<req->citemNum ; i++)
			{
				std::string doc_id = req->counter_t->m_UrlDocid[req->status_id][i];
				vr_data->v_docid.push_back(doc_id);

				// remove unchanged doc_id from req->counter_t->m_docid
				// Todo ... : req->counter_t->m_docid can store by url, so don't need reload doc_id from mysql
				
				if (req->counter_t->m_docid.find(doc_id) != req->counter_t->m_docid.end())
				{
					req->counter_t->m_docid.erase(doc_id);
				}
				
			}
			pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);
			
            SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)url_content_unchanged record_count : %d, size : %d, vr_id : %d, url : %s remain unchanged, begin processing.\n", this, req->citemNum, vr_data->v_docid.size(), req->class_id, req->url.c_str()));

			ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(vr_data) );
			CDeleteTask::instance()->put(msg);
			req_done(req, mysql_vr, 1, thisHandler, SUCCESS, req->citemNum);
			return 0;
		}
		else if(req->class_id < 70000000)
		{
			pthread_mutex_lock(&counter_mutexes[req->res_id % 10]);
			req->citemNum = req->counter_t->m_UrlDocid[req->status_id].size();
			for(int i=0; i<req->citemNum ; i++)
			{
				std::string doc_id = req->counter_t->m_UrlDocid[req->status_id][i];
				if (req->counter_t->m_docid.find(doc_id) != req->counter_t->m_docid.end())
				{
					req->counter_t->m_docid.erase(doc_id);
				}
			}
			pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);

			ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(vr_data) );
			CDeleteTask::instance()->put(msg);
			req_done(req, mysql_vr, 1, thisHandler, SUCCESS, req->citemNum);
			return 0;
		}
		else
		{
			delete vr_data;
		}
	}

	//check wether schema file and length file exist exclude inner vr
	xmlSchemaValidCtxt* validityContext = NULL;
	xmlSchemaParserCtxt* schemaParser = NULL;
	xmlSchema* schema = NULL;
	if(req->class_id >= 20000000)
	{
		timeval  schema_check_beg;
		gettimeofday(&schema_check_beg,NULL);
		char schema_name[1024];
		char length_name[1024];
		//struct stat stat_buf;
		sprintf(schema_name, "%s%02d%s", m_schema_prefix.c_str(), req->tplid, m_schema_suffix.c_str()); 
		sprintf(length_name, "%s%02d%s", m_length_prefix.c_str(), req->tplid, m_length_suffix.c_str());
		int schema_exist = 0, length_exist = 0;
		schema_exist = access(schema_name,R_OK);
		length_exist = access(length_name,R_OK);
		if(schema_exist || length_exist)
		{
			SS_DEBUG((LM_ERROR,"[CWriteTask::process]resource url:%s template id %d  is less than 1.\n",req->url.c_str(),req->tplid));

			int retmp;
			SS_DEBUG((LM_ERROR,"[CWriteTask::process]schema file %s can not be read.\n ",schema_name));
			sprintf(buf, "update vr_resource set manual_update = 0 where id = %d", req->res_id);
			timeval  sql_beg4;
			gettimeofday(&sql_beg4,NULL);
			while((retmp=sql_write( mysql_vr, buf )) < 1)
			{
				SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update   vr_resource %d reason:[%s] "
							"update_date error code: %d! %s\n", this, req->res_id, mysql_vr.error(),retmp, buf));
				if( 0 == retmp )
					break;
			}
			timeval sql_end4;
			gettimeofday(&sql_end4, NULL);
			time_t sql_cost4;
			sql_cost4 = 1000000*(sql_end4.tv_sec - sql_beg4.tv_sec) + sql_end4.tv_usec - sql_beg4.tv_usec;
			sql_cost.write_cost += sql_cost4;
			sql_cost.total_cost += sql_cost4;
			sprintf(buf,"update  resource_status set  res_status = 4, err_info = 'schema file notfound' where id = %d", req->status_id);
			timeval  sql_beg5;
			gettimeofday(&sql_beg5,NULL);
			while((retmp=sql_write( mysql_vr, buf )) < 1)
			{
				SS_DEBUG((LM_ERROR,"CWriteTask::process(%@)fail to Update resource_status %d"
							"update_date error code: %d!\n", this, req->res_id, retmp));
				if( 0 == retmp )
					break;
			}
			timeval sql_end5;
			gettimeofday(&sql_end5, NULL);
			time_t sql_cost5;
			sql_cost5 = 1000000*(sql_end5.tv_sec - sql_beg5.tv_sec) + sql_end5.tv_usec - sql_beg5.tv_usec;
			//fprintf(stderr, "Time Detector Write SQL3 %lld \n", sql_cost3);
			sql_cost.write_cost += sql_cost5;
			sql_cost.total_cost += sql_cost5;
			if(req->data_from == 4)
			{
				sprintf(buf, "update customer_resource set res_status = 5 ,check_info = 'access schema error' where online_id = %d and type = 2" , req->res_id);
				timeval  sql_beg6;
				gettimeofday(&sql_beg6,NULL);
				while((retmp = sql_write(mysql_cr, buf, 2)) < 1)
				{

					SS_DEBUG((LM_ERROR, "CWriteTask::process(%@)fail to Update " 
								"customer_resource resource_status %d update_date error code: %d!\n", 
								this, req->res_id, retmp));
					if( 0 == retmp )
						break;
				}
				timeval sql_end6;
				gettimeofday(&sql_end6, NULL);
				time_t sql_cost6;
				sql_cost6 = 1000000*(sql_end6.tv_sec - sql_beg6.tv_sec) + sql_end6.tv_usec - sql_beg6.tv_usec;
				sql_cost.write_cost += sql_cost6;
				sql_cost.total_cost += sql_cost6;
				//fprintf(stderr, "Time Detector Write SQL4 %lld \n", sql_cost4);
			}
			if(schema_exist)
				req->debugInfo.append(Util::gbk_to_utf8("[无Schema验证文件]","[no schema file]"));
			if(length_exist)
				req->debugInfo.append(Util::gbk_to_utf8("[无长度验证文件]","[no length file]"));
			req_done(req,mysql_vr,3, thisHandler, ERR);//处理失败的req

			SS_DEBUG((LM_TRACE,"CWriteTask::process(%@)Resource %d status %d schemaFile %s does not exist, "
						"set res_status = 4, finish processing.\n",this, req->res_id, req->status_id, schema_name));
			return -1;
		}
		testTime2=ACE_OS::gettimeofday();
		//SS_DEBUG((LM_TRACE,"[CWriteTask::TEST]process1: %d \n",(testTime2-testTime1).usec()));

		//create validity context
		pthread_mutex_lock(&m_validCtxt_mutex);
		if( ( schemaParser = xmlSchemaNewParserCtxt(schema_name)) )
		{
			if ( (schema = xmlSchemaParse(schemaParser)) )
			{
				if( (validityContext = xmlSchemaNewValidCtxt(schema)) )
				{
					xmlSchemaSetValidErrors(validityContext,
							schemaErrorCallback,
							schemaWarningCallback,
							/*callback data*/  0);
					SS_DEBUG((LM_TRACE, "schema %d:%s loaded\n", req->tplid, schema_name));
					// Returns 0 if validation succeeded
					//result = xmlSchemaValidateFile(validityContext, uri.c_str(), 0) == 0;
				}
				else
				{
					SS_DEBUG((LM_ERROR,"create validity Context from schema fail: %s \n", schema_name));
					//exit(-1);
					//ACE_Reactor::instance()->end_reactor_event_loop();
					pthread_mutex_unlock(&m_validCtxt_mutex);
					//req->debugInfo.append(", [schema文件有误]");
					req->debugInfo.append(Util::gbk_to_utf8(", [schema文件有误]",",[schema file error]"));
					req_done(req,mysql_vr,5, thisHandler, ERR);
					return -1;
				}//end else
			}//end if ( (schema = xmlSchemaParse(schemaParser)) )
			else
			{
				SS_DEBUG((LM_ERROR,"create xmlschema parser fail: %s \n", schema_name));
				//exit(-1);
				//ACE_Reactor::instance()->end_reactor_event_loop();
				pthread_mutex_unlock(&m_validCtxt_mutex);

				//req->debugInfo.append(", [schema文件有误]");
				req->debugInfo.append(Util::gbk_to_utf8(", [schema文件有误]",",[create schema file error]"));
				req_done(req,mysql_vr,5, thisHandler, ERR);
				return -1;
			}//解析xml schemal
		}//end if( schemaParser = xmlSchemaNewParserCtxt(schema_name))
		else
		{
			SS_DEBUG((LM_ERROR,"create schema fail: %s \n", schema_name));
			//exit(-1);
			//ACE_Reactor::instance()->end_reactor_event_loop();
			pthread_mutex_unlock(&m_validCtxt_mutex);

			req->debugInfo.append(Util::gbk_to_utf8(", [schema文件有误]",",[schema file error]"));
			req_done(req,mysql_vr,5, thisHandler, ERR);
			return -1;
		}
		pthread_mutex_unlock(&m_validCtxt_mutex);

		//解析schema文件正确 cut into items
		testTime3=ACE_OS::gettimeofday();
		//	SS_DEBUG((LM_TRACE,"[CWriteTask::TEST]process2:%d\n",(testTime3-testTime2).usec()));

		timeval schema_check_end;
		gettimeofday(&schema_check_end, NULL);
		schema_check_cost = 1000000*(schema_check_end.tv_sec - schema_check_beg.tv_sec) + schema_check_end.tv_usec - schema_check_beg.tv_usec;
	}


	//需要解析文件的处理
	testTime4=ACE_OS::gettimeofday();
	//SS_DEBUG((LM_TRACE,"[CWriteTask::TEST]process3:%d\n",(testTime4-testTime3).usec()));


	timeval begin_time;
	gettimeofday(&begin_time, NULL);
	preprocess_cost = 1000000*(begin_time.tv_sec - preprocess_beg.tv_sec) + begin_time.tv_usec - preprocess_beg.tv_usec;
	time_t begin_time_usec = begin_time.tv_sec * 1000000 + begin_time.tv_usec;
	SS_DEBUG((LM_ERROR,"[CWriteTask::Time]process begin: res_id: %d, status_id: %d time: %lld\n",
				req->res_id,req->status_id,begin_time_usec));	
	req->citemNum=0;
	bool parseFailed = false;

	std::string tmp_item;
	map<string, vector<string> > fieldBufs;
	if(req->class_id < 70000000) //not multi-hit vr
	{
		//fprintf(stderr, "test glp data format is %s\n", req->data_format.c_str());
		vector<string> emptyV;
		emptyV.reserve(MAXBUFCOUNT);
		if(strcmp(req->data_format.c_str(), RULEAB) == 0
				|| strcmp(req->data_format.c_str(), RULEABP) == 0)
		{
			req->ruleType = AB;
			//fileName = classidStr+ CONTENTA;
			fieldBufs.insert(make_pair(CONTENTA,emptyV));
			//fileName = classidStr + CONTENTB;
			fieldBufs.insert(make_pair(CONTENTB,emptyV));
		}
		else
		{
			req->ruleType = PLAIN;
			//fileName = classidStr+ CONTENTA; 
			fieldBufs.insert(make_pair(CONTENTA,emptyV));    
		}
	}
	else
	{
		req->ruleType = MHIT;
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
	bool puncNorm = req->class_id >= 20000000 && req->class_id < 50000000;	// only for EXTERNAL and WIRELESS
	static const std::unordered_set<std::string> dSpWhiteList({"xAx", "AxP", "AxR", "A", "AP", "xP"});
	bool puncNormDelSpace = dSpWhiteList.find(req->data_format) != dSpWhiteList.end();	// do not delSpace for AB, A1A2...

	std::map<std::string, std::string>::iterator map_it;
    std::map<std::string, std::string> *docid_map = &(req->counter_t->m_docid);

    std::string encoding = "gbk";

	size_t beg_pos = xml.find("<?xml");
	if(beg_pos != std::string::npos)
	{
		size_t end_pos = xml.find("?>");
		if(end_pos != std::string::npos && end_pos > beg_pos)
		{
			std::string head = xml.substr(beg_pos,end_pos);
			toLower(head);
			if(head.find("utf-8")!= std::string::npos || head.find("utf8") != std::string::npos)
				encoding="utf-8";
		}
	}

	SS_DEBUG((LM_INFO,"CWriteTask::process xml encoding is: %s\n", encoding.c_str()));

	if(encoding=="gbk")
	{
		//如果转码失败，剔除有问题的字符，然后再进行转码
		if(Util::GBK2UTF8(xml,xml))
		{
			if(Util::GBK2UTF8(Util::recoverGBKErr(xml),xml))
			{
			    SS_DEBUG((LM_ERROR,"[CWriteTask::process] xml GBK2UTF8 failed,classid=%d,url=%s\n",req->class_id,req->url.c_str()));
				req->debugInfo.append(",[");
				req->debugInfo.append(req->ds_name);
				req->debugInfo.append(Util::gbk_to_utf8(", xml GBK转utf8失败]",",xml GBK2UTF8 failed]"));
			}
			else
			    SS_DEBUG((LM_INFO,"[CWriteTask::process] xml GBK2UTF8 OK,after recoverGBKErr,classid=%d,url=%s\n",req->class_id,req->url.c_str()));
		}
		else
		{
			SS_DEBUG((LM_INFO,"[CWriteTask::process] xml GBK2UTF8 OK,classid=%d,url=%s\n",req->class_id,req->url.c_str()));
		}
	}

	sub_request_t *footer_sub_req = new sub_request_t();
	
	std::string::size_type pos = xml.find("<item>");//一个item是一个key
	//process each item
	while( std::string::npos != pos && !ACE_Reactor::instance()->reactor_event_loop_done() )
	{
		sub_request_t *  sub_req = new sub_request_t();
		req->citemNum++;
		item_num ++;
		int ret = 0, type = -1;
		std::string::size_type end_pos = xml.find("</item>", next_pos);
		if( std::string::npos == end_pos )
		{
			SS_DEBUG((LM_ERROR, "[CWriteTask::process]</item> not found error: %s\n", req->url.c_str()));
			delete sub_req;
			break;
		}
		timeval  item_valid_beg;
		gettimeofday(&item_valid_beg,NULL);

		char time_tmp[64];
		//extract a single item begin with<item> and end with </item>
		std::string item = xml.substr(pos, (end_pos+strlen("</item>")-pos));
		//SS_DEBUG((LM_INFO,"cut item is %s,gbk=%s\n",item.c_str(),Util::utf8_to_gbk(item).c_str()));
		//alter item from xml format to libxml compatible format
		//add <![CDATA[...]]> between tags
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
                //if(encoding == "utf-8")
                //{
                //    Util::UTF82GBK(location, location);
                //}
				SS_DEBUG((LM_DEBUG,"[CWriteTask::process]location is: %s\n",Util::gbk_to_utf8(location).c_str()));
			}
			else
			{
				location = "";
			}
		}

		//去除注释信息
		int begin_p = 0;
		int end_p = 0;
		
		while(true)
		{
			begin_p = item.find(comments_begin, 0);
			end_p = item.find(comments_end, begin_p);

			if( begin_p != std::string::npos && end_p != std::string::npos )
			{
				item.replace(begin_p, end_p+comments_end.size()-begin_p, "");
				continue;
			}
			break;
		}

		//add xml head and xml tail to item
		item = "<DOCUMENT>" + item + "</DOCUMENT><!--STATUS VR OK-->";
		
        //get a complete <document>
        timeval item_valid_end;
		gettimeofday(&item_valid_end, NULL);
		time_t item_valid_cost;
		item_valid_cost = 1000000*(item_valid_end.tv_sec - item_valid_beg.tv_sec) +
			item_valid_end.tv_usec - item_valid_beg.tv_usec;

		timeval  item_libxml_check_beg;
		gettimeofday(&item_libxml_check_beg,NULL);

		std::string key, vrdata_key,optword, synonym, sql, url, uurl, keytmp;

		xmlNodePtr node = NULL, root = NULL;
		xmlNodePtr display_node = NULL;
		xmlBufferPtr buffer = NULL;
		//xmlOutputBufferPtr temp_buffer = NULL;
		//read item into a xmldoc and store it into doc
		//SS_DEBUG((LM_INFO,"fmm_test item=%s\n",Util::utf8_to_gbk(item).c_str()));
		xmlDocPtr doc = xmlReadMemory(item.c_str(), item.length(), NULL, "utf8", 0); 

		//time statistics better to extract to func
		timeval  read_doc;
		gettimeofday(&read_doc,NULL);
		xml_cost.readDoc_cost+= 1000000*(read_doc.tv_sec - item_libxml_check_beg.tv_sec) + 
			read_doc.tv_usec - item_libxml_check_beg.tv_usec; 

		//prepare for following mysql operation
		CMysqlQuery query_vi(&mysql_vi);
		CMysqlQuery query_vr(&mysql_vr);


		int valid_status = 1;
		int check_status = 0;
		char valid_info[128] = {0};
		unsigned char * data_md5;
		char *data_md5_hex_old;
		char data_md5_hex[33];
		int locID = -1;
		std::string::size_type key_start = item.find("<key><!CDATA[");
		std::string::size_type key_end = item.find("]]></key>");
		std::string classid;
		std::stringstream ss;
		std::string objid;

		//if fail to create libxml doc, continue to next item 
		//move this directly after the doc creation statement
        if( NULL == doc )
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@):Parse xml item failed, xmlReadMemory fail: %d %d %d %s [%s]\n",
						this,req->res_id, pos, end_pos, req->url.c_str(), Util::utf8_to_gbk(item.substr(0,255)).c_str() ));
			if(err_info.length() == 0)
				err_info = "xmlReadMemory fail to parse item:\n " + item.substr(0,200);
			delete sub_req;
			parseFailed = true;
			goto contin;
		}

		//extract key from item if not find, contiune to next item
		if(key_start == -1 || key_end == -1)//获取key字段
		{
			key_start = item.find("<key>");
			key_end = item.find("</key>");
			if(key_start == -1 || key_end == -1)
			{
				SS_DEBUG((LM_ERROR,"CWriteTask::process(%@): key find failed in item : %s\n",
							this,item.substr(0,255).c_str()));
				delete sub_req;
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


		timeval  key_search;
		gettimeofday(&key_search,NULL);
		xml_cost.keySearch_cost += 1000000*(key_search.tv_sec - read_doc.tv_sec) + 
			key_search.tv_usec - read_doc.tv_usec;
		//begin libxml parse
		root = xmlDocGetRootElement(doc);
		root = root->xmlChildrenNode;
		//if not find item , continue to next item
		if( 0 != xmlStrcmp(root->name,BAD_CAST"item") )
		{
			SS_DEBUG((LM_ERROR , "xml error: root is not <item> but %s\n",
						(char*)root->name ));
			delete sub_req;	
			goto contin;
		}
		// get vr_type(class tag), check schema
		type = req->tplid;
		if( type < 1 )
		{
			SS_DEBUG((LM_ERROR, "template id error: %d %s %s\n",
						type, req->tag.c_str(), req->url.c_str() ));
			delete sub_req;
			goto contin;
		}

		//validate the libxml doc by schema
		if(req->class_id>=20000000 && (ret=xmlSchemaValidateDoc(validityContext, doc))!=0)
		{
			//if failed validation, record schema error
			SS_DEBUG((LM_ERROR, "xmlSchemaValidateDoc fail: %d %s %s template id :%d keyword: %s\n",
						ret, req->tag.c_str(), req->url.c_str(),req->tplid,Util::utf8_to_gbk(keytmp).c_str() ));
			valid_status = 0;
			schema_fail_cnt++;
			strncat(valid_info, "SCHEMA_ERROR", 127);
		}
		timeval  schema_validate;
		gettimeofday(&schema_validate,NULL);
		xml_cost.validate_cost+= 1000000*(schema_validate.tv_sec - key_search.tv_sec) + 
			schema_validate.tv_usec - key_search.tv_usec;

		// add prop for index
		node = root->xmlChildrenNode;
		SS_DEBUG((LM_DEBUG, "add index_field prop: %s\n",req->index_field.c_str()));
		//if(req->class_id < 70000000) //not multi-hit vr
			//addProp(root,req->index_field);

		
		//check necessary properities 
		while( node )
		{
			if( 0 == xmlStrcmp(node->name,BAD_CAST"key") )
			{
				key = getNodeContent(node);

				replace_all_distinct(key, "\\","\\\\");
				replace_all_distinct(key, "\'","\\\'");

			}
			if(req->class_id >= 20000000)
			{	
				if( 0 == xmlStrcmp(node->name,BAD_CAST"display") )
				{
					display_node = node;
					xmlNodePtr tmp_node = node->xmlChildrenNode;
					while( tmp_node )
					{
						if( 0 == xmlStrcmp(tmp_node->name,BAD_CAST"url") )
						{
							url = getNodeContent(tmp_node);
							break;
						}
						tmp_node = tmp_node->next;
					}
				}
				else if (0 ==  xmlStrcmp(node->name,BAD_CAST"optword"))
				{

					optword = getNodeContent(node);
					replace_all_distinct(optword, "\\","\\\\");
					replace_all_distinct(optword, "\'","\\\'");

					if (optword.size() > 760)
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process(%@):"
									"optword size bigger than %d\n",
									this, req->res_id, 760 ));
						delete sub_req;
						goto contin;
					}                           
				}
				else if (0 ==  xmlStrcmp(node->name,BAD_CAST"synonym"))
				{
					synonym = getNodeContent(node);

					replace_all_distinct(synonym, "\\","\\\\");
					replace_all_distinct(synonym, "\'","\\\'");

					if (synonym.size() > 760)
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process(%@):"
									"synonym size bigger than %d\n",
									this, req->res_id, 760 ));
						delete sub_req;
						goto contin;
					}                           
				}
			}
			node = node->next;
		}//end while(node)
		if( 0 == key.length() || (0 == url.length() && req->class_id >= 20000000 ))
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): find <key> or <url> fail:"
						"%d %d [%s]\n",this,
						req->res_id, req->class_id, Util::utf8_to_gbk(key).c_str() ));
			delete sub_req;
			goto contin;
		}

		//check hit black list or not, if hit record valid info
		if( 0 != check_blacklist(req->class_id, Util::utf8_to_gbk(key), mysql_vi) )
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): check blacklist fail:%d %d %s\n",
						this, req->res_id, req->class_id, Util::utf8_to_gbk(key).c_str() ));
			//valid_status = 0;
			if(strlen(valid_info) > 0)
				strncat(valid_info,"|", 127);
			strncat(valid_info, "BLACKLIST", 127);
		}

		//generate docid according to classid, key and location
		//based on docid generate objid,which is used just int log
		char ubuff[2048];	// ubuff 只用于计算 unique_url/docid/objid
		char dbc_buf[2048];
		ss << class_id_convert;
		classid.clear();
		classid = ss.str();


		//确保都是半角小写
		utf8_to_lower(key);

		strncpy(ubuff, key.c_str(), sizeof(ubuff));
		

	    vrdata_key.assign(ubuff);
		//将查询词中的多空格转换成一个
		//multi_space_to_one(ubuff);
		//trim
		trim(ubuff);
		//replace space character by tab
		//replace_char(ubuff, ' ', '\t');

		//将查询词中的连续空格或tab替换成一个tab
		multichar_to_onechar(ubuff, " \t", '\t');

		// 标点、空格归一化
		if(puncNorm)
			PuncNormalize_gbk(ubuff, "utf8",puncNormDelSpace);

		if(location.length() > 0) {
			//地域信息格式归一化
			multichar_to_onechar(location, " \t", '\t');

			//去掉省、市等词
			prune_suffix_tab_u8(location,location);
			locID = get_locationID(Util::utf8_to_gbk(location));
			//SS_DEBUG((LM_DEBUG,"location %s, ID :%d\n",locID, location.c_str()));
			if(req->class_id >= 70000000) //add data source id for multi-hit vr
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%s#%d", classid.c_str(), ubuff, location.c_str(), req->ds_id);
			else
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%s", classid.c_str(), ubuff, location.c_str());
		} else {
			if(req->class_id >= 70000000)
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s#%d", classid.c_str(), ubuff, req->ds_id);
			else
				snprintf(dbc_buf, sizeof(dbc_buf), "%s#%s", classid.c_str(), ubuff);
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
		SS_DEBUG((LM_TRACE, "CWriteTask:: DocID=%s docid=%llx-%llx, uurl=%s,classid=%s,key=%s,location=%s,objid=%s\n", 
					docIdbuf, docid.id.value.value_high,docid.id.value.value_low,
					url.c_str(),classid.c_str(),Util::utf8_to_gbk(key).c_str(),Util::utf8_to_gbk(location).c_str(),objid.c_str()));

		//compute the md5 of the current item		
		data_md5 = MD5((const unsigned char *)item.c_str(),item.length(),NULL);
		hexstr(data_md5,data_md5_hex, MD5_DIGEST_LENGTH);

		timeval  parse_xml;
		gettimeofday(&parse_xml,NULL);
		xml_cost.parseXml_cost+= 1000000*(parse_xml.tv_sec - schema_validate.tv_sec) + 
			parse_xml.tv_usec - schema_validate.tv_usec;


		if(req->class_id >= 20000000)
		{
			if( -1 == item.find("<unique_url>") )
				xmlNewTextChild(root, NULL, (const xmlChar *)"unique_url", 
						(const xmlChar *)url.c_str());
			xmlNewTextChild(root, NULL, (const xmlChar *)"source", 
					(const xmlChar *)(req->over24?"":"external") );
			strftime(time_tmp, sizeof(time_tmp), "%c", &tm);
			xmlNewTextChild(root, NULL, (const xmlChar *)"fetch_time", 
					(const xmlChar *)req->fetch_time_sum);//构造fetchtime
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
			//SS_DEBUG((LM_TRACE,"Add objid:%s\n",(const xmlChar *)objid.c_str()));
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
			std::string pkg_name = "";
			if(xpathCtx != NULL)
			{
				//add some properties
				Util::addAttribute(xpathCtx, req->annotation_map, "annotation");
				Util::addAttribute(xpathCtx, req->index_map, "index_level");
				Util::addAttribute(xpathCtx, req->sort_map, "sort");
				Util::addAttribute(xpathCtx, req->second_query_map, "second_query");
				Util::addAttribute(xpathCtx, req->group_map, "group");
				Util::addAttribute(xpathCtx, req->full_text_map, "fulltext");
				if(m_need_urltran == "1")
				{
					std::string tmp_pkg = "";
					for(auto itr=req->url_transform_map.begin(); itr!=req->url_transform_map.end(); ++itr) {
						Util::urlTransform(doc, xpathCtx, itr->first, curl,tmp_pkg);
						//防止有一部分url没有相关信息，该条item失去pkg的信息
						if(tmp_pkg != "")
						{
							pkg_name = tmp_pkg;
						}
					}
				}

				//get the hit field value
				map<string, string>::const_iterator itr;
				string value;
				int ret = 0;
				for(itr=req->hit_fields.begin(); itr!=req->hit_fields.end(); ++itr)
				{
					ret = Util::getValueByXpath(xpathCtx, itr->second, value);
					if(ret == 0)
					{
						if(sub_req->key_values.size() > 0)
						{
							sub_req->key_values.append("||");
						}
						sub_req->key_values.append(itr->first); //key
						sub_req->key_values.append("::");
						sub_req->key_values.append(value); //value

						utf8_to_lower(value);
						/*char tmpbuf[2048];

						if (EncodingConvertor::sbc2dbc(value.c_str(), tmpbuf, sizeof(tmpbuf), true) < 0)                        { 
							SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): EncodingConvertor::sbc2dbc [key:%s] failed.\n",this, sub_req->key_values.c_str()));
							continue;
						}*/

						vector<string> hit_keys;
						splitStr(value,';',hit_keys);

						for(int i = 0; i< hit_keys.size(); i++)
						{
							fieldBufs[itr->first].push_back(hit_keys[i]);
						}
						//SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): get xpath value success, xpath:%s, value:%s\n", this, itr->second.c_str(), value.c_str()));
					}
					else
					{
						SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): get xpath value error, xpath:%s\n", this, itr->second.c_str()));
					}
				}
				replace_all_distinct(sub_req->key_values, "\\", "\\\\");
				replace_all_distinct(sub_req->key_values, "\'", "\\\'");
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
			snprintf(temp_priority, 10, "%d", req->ds_priority);
			
			xmlNodePtr data_source_node = xmlNewTextChild(root, NULL, 
					(const xmlChar *)"data_source_priority", (const xmlChar *)temp_priority);
			xmlNewProp(data_source_node, (const xmlChar *)"sort", (const xmlChar *)"ss");
			if(display_node)
				xmlNewTextChild(display_node, NULL, (const xmlChar *)"pkg_name", (const xmlChar *)pkg_name.c_str());
		}
		else
		{
			if(req->ruleType == PLAIN)
			{
				fieldBufs[CONTENTA].push_back(vrdata_key);   
			}
			else
			{
				vector<string> keys;
				splitStr(vrdata_key, '\t', keys);
                if(keys.size() ==2)
                {
                    //fprintf(stderr, "test glp rule Type AB key %s\n", vrdata_key.c_str());
                    //fprintf(stderr, "test glp rule Type AB key %s, key1 %s \n", vrdata_key.c_str(), keys[0].c_str());
				    //fprintf(stderr, "test glp rule Type AB key %s, key1 %s key2 %s\n", vrdata_key.c_str(), keys[0].c_str(), keys[1].c_str());
                
				    fieldBufs[CONTENTA].push_back(keys[0]);
				    fieldBufs[CONTENTB].push_back(keys[1]);
                }
                else
                    fprintf(stderr, "test glp rule AB key error\n");
			}
		}

		timeval  add_prop;
		gettimeofday(&add_prop,NULL);
		xml_cost.addProp_cost+= 1000000*(add_prop.tv_sec - parse_xml.tv_sec) + 
			add_prop.tv_usec - parse_xml.tv_usec;	

		SS_DEBUG((LM_TRACE,"vr id %d, Add objid:%s res_id is %d status_id is %d\n",
					req->class_id, (const xmlChar *)objid.c_str(),req->res_id, req->status_id));
		// validate length after add prop
		errinfo = "";
		if(req->class_id >= 20000000 && check_length( doc, type,key,req, errinfo) > 0 )
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::process(%@): check length fail: resource ID:%d classTag:%s key:%s\n",
						this,req->res_id, req->tag.c_str(), Util::utf8_to_gbk(key).c_str()));
			valid_status = 0;
			if(strlen(valid_info) > 0)
				strncat(valid_info,"|", 127);
			strncat(valid_info, "LENGTH_ERROR:", 127);
			strncat(valid_info, errinfo.c_str(), 127);
		}


		timeval  check_length;
		gettimeofday(&check_length,NULL);
		xml_cost.checkLen_cost+= 1000000*(check_length.tv_sec - add_prop.tv_sec) + 
			check_length.tv_usec - add_prop.tv_usec;  
		//libxml doc to string, store it into item
		buffer = xmlBufferCreate();
		if( NULL == buffer )
		{
			SS_DEBUG((LM_ERROR, "xmlBufferCreate fail: %d %s\n", 
						req->res_id, key.c_str() ));

			delete sub_req;
			goto contin;
		}
		//temp_buffer = xmlOutputBufferCreateBuffer(buffer, xml_encoding_handler);
		Util::SaveToStringWithEncodeFormat(doc,item);
		//SS_DEBUG((LM_INFO,"[SaveToStringWithEncodeFormat] run here\n"));
		Util::ChangeEncodeDeclaration(item,item,"utf8","utf-16");
		/*if(temp_buffer && (ret=xmlSaveFileTo(temp_buffer, doc, "utf-16")) > 0) 
		{
			item.assign((const char *)xmlBufferContent(buffer),(size_t)xmlBufferLength(buffer));
		}
		else
		{
			SS_DEBUG((LM_ERROR, "xmlSaveFileTo fail: %d %s %x %d\n", 
						req->res_id, key.c_str(), temp_buffer, ret));
			delete sub_req;
			goto contin;
		}*/
		replace_all_distinct(item, "\\","\\\\");
		//replace_all_distinct(item, "\'","\\\'");
		replace_all_distinct(item, "\'","&#39;");
		if( -1 == item.find("<unique_url><![CDATA[") )
		{
			replace_all_distinct( item, "<unique_url>", "<unique_url><![CDATA[" );
			replace_all_distinct( item, "</unique_url>", "]]></unique_url>" );
		}


		timeval  create_new;
		gettimeofday(&create_new,NULL);
		xml_cost.createNew_cost+= 1000000*(create_new.tv_sec - check_length.tv_sec) + 
			create_new.tv_usec - check_length.tv_usec;

		//create new doc
		timeval item_libxml_check_end;
		gettimeofday(&item_libxml_check_end, NULL);
		xml_cost.total_cost += 1000000*(item_libxml_check_end.tv_sec - item_libxml_check_beg.tv_sec) +
			item_libxml_check_end.tv_usec - item_libxml_check_beg.tv_usec;

		timeval  insert_update_beg;
		gettimeofday(&insert_update_beg,NULL);
		if(!valid_status)
		{
			req->failedKeyWord.append(key+Util::gbk_to_utf8("、"));
			req->failedKeyCount++;
		}

		//initialize sub_req
		sub_req->res_id = req->res_id;
		sub_req->status_id = req->status_id;
		sub_req->data_group = req->data_group;
		sub_req->data_status = 0;
		sub_req->uurl = uurl;
		sub_req->key = key;
		sub_req->optword = optword;
		sub_req->synonym = synonym;
		sub_req->valid_info = valid_info;
		sub_req->data_md5_hex.assign((char*)data_md5_hex);
		sub_req->docIdbuf.assign((char*)docIdbuf);
		sub_req->valid_status = valid_status;
		sub_req->is_manual = req->is_manual;
		sub_req->is_footer = false;
		sub_req->isNewFrame = req->isNewFrame;
		sub_req->fetch_time.assign(req->fetch_time);
		sub_req->locID = locID;
		sub_req->timestamp_id = req->timestamp_id;
		sub_req->url = req->url;
		sub_req->tplid= req->tplid;
		sub_req->item = "";
		if(location.length() > 0)
		{
			sub_req->location = location;
		}else
			sub_req->location = "";
		sub_req->err_info = err_info;	
		sub_req->ds_id	= req->ds_id;
		sub_req->class_id = req->class_id;

		// lock 
		pthread_mutex_lock(&counter_mutexes[req->res_id % 10]);
        map_it = docid_map->find(docIdbuf);
		item.append(1,'\0');
        if( map_it != docid_map->end() )
        {
            ret = 1;
            data_md5_hex_old = (char *)map_it->second.c_str();
			docid_map->erase(map_it);
        }
        else
        {
            ret = 0;
        }
		pthread_mutex_unlock(&counter_mutexes[req->res_id % 10]);
		
		if( 0 == ret )
		{// insert,如果mysql库中没有当前数据，则写入
			//insert_routine:
			sub_req->updateType = INSERT;
			if(auto_check == 1 || auto_check > 2)
				check_status = 1;
			else
				check_status = 0;
			sub_req->check_status = check_status;
			if(req->class_id >= 20000000)
			{
				if((tpos = item.find("<update_type>")) == -1)
				{
					delete sub_req;
					goto contin;
				}
				else
					item.replace(tpos+13,9,"key_update",10);
			}
			if(req->class_id >= 20000000)
			{
				//only send valid item to online db
				Util::UTF82UTF16(item,tmp_item);
				if(sub_req->check_status==1 && sub_req->valid_status==1) {
					sub_req->item = tmp_item;
                	//footer_sub_req->uurl_item_list.add_node(uurl, tmp_item);
				}
			}

			ACE_Message_Block *msg=new ACE_Message_Block(reinterpret_cast<char *>(sub_req));	
			thisHandler->put(msg);
			fprintf(stderr, "after put msg\n");				
		}//end if(0==ret)
		else if( 1 == ret )
		{
			if((data_md5_hex_old && (strncmp(data_md5_hex, data_md5_hex_old, 32) 
						|| strlen(data_md5_hex_old) == 0)) || is_changed)
			{   
				sub_req->updateType = UPDATE1;
				is_changed = false;
				if(auto_check >= 2)
					check_status = 1;
				else
					check_status = 0;
				sub_req->check_status = check_status;
				if(req->class_id >= 20000000)
				{
					if((tpos = item.find("<update_type>")) == -1)
					{
						SS_DEBUG((LM_TRACE,"failed to find <update_type>.\n"));
						delete sub_req;
						continue;
					}
					else
						item.replace(tpos+13,9,"content_update",14);
				}
				if(req->class_id >= 20000000)
				{
					Util::UTF82UTF16(item,tmp_item);
					if(sub_req->check_status==1 && sub_req->valid_status==1) {
						sub_req->item = tmp_item;
                    	//footer_sub_req->uurl_item_list.add_node(uurl, tmp_item);
					}
				}
				ACE_Message_Block *msg=new ACE_Message_Block(reinterpret_cast<char *>(sub_req)); 
				thisHandler->put(msg);
			}//end if(data_md5_hex_old && (strncmp..
			else//item没有改变了
			{                 
				if(auto_check >= 2)
					sub_req->check_status = 1;
				else
					sub_req->check_status = 0;
				Util::UTF82UTF16(item,tmp_item);
				if(req->class_id>=20000000 && sub_req->check_status==1 && sub_req->valid_status==1) {
					sub_req->item = tmp_item;
					//footer_sub_req->uurl_item_list.add_node(uurl, tmp_item); //need online
				}

				if(req->is_manual ||(req->class_id >= 20000000 && !req->over24))
				{
					sub_req->updateType = UPDATE2;
				}//end if(req->is_manual || !req->over24)
				else
				{//会更新地域信息
					sub_req->updateType = UPDATE3;
				}
				ACE_Message_Block *msg = new ACE_Message_Block(reinterpret_cast<char *>(sub_req));
				thisHandler->put(msg);
			}//end else
		}//end else if(1==ret)
		else
		{//query mysql error
			SS_DEBUG((LM_ERROR, "vr_data count error: %d %d %s\n", ret, req->res_id, key.c_str() ));
			delete sub_req;
			goto contin;
		}
		timeval insert_update_end;
		gettimeofday(&insert_update_end, NULL);
		insert_update_cost += 1000000*(insert_update_end.tv_sec - insert_update_beg.tv_sec) + 
			insert_update_end.tv_usec - insert_update_beg.tv_usec;
		write_num ++;
		testTime5=ACE_OS::gettimeofday();
contin:
		if( buffer )
			xmlBufferFree(buffer);
		if( doc )
			xmlFreeDoc(doc);
		next_pos = end_pos + strlen("</item>");
		pos = xml.find("<item>", next_pos);//处理下一个item
	}//end  while( std::string::npos != pos &&... 处理完了url结果xml中所有item

	if( ACE_Reactor::instance()->reactor_event_loop_done() )
	{
		if(parseFailed)
		{
			req->debugInfo.append(Util::gbk_to_utf8(", [xml错误]",", [xml error]"));
			req_done(req,mysql_vr,3, thisHandler, ERR,write_num);//处理完抓取成功的req
		}
		else
		{
			req_done(req,mysql_vr,1, thisHandler, SUCCESS,write_num);//处理完抓取成功的req
		}

		SS_DEBUG((LM_INFO, "CWriteTask::process(%@): reactor_event_loop_done %d\n", 
					this, req->res_id));
		return 1;
	}

	if(parseFailed)
	{
		req->debugInfo.append(Util::gbk_to_utf8(", [xml错误]",", [xml error]"));
		req_done(req,mysql_vr,3, thisHandler, ERR,write_num);//处理完抓取成功的req
	}
	else
	{
		req_done(req,mysql_vr,1, thisHandler, SUCCESS,write_num);//处理完抓取成功的req
	}

	footer_sub_req->res_id = req->res_id;
	footer_sub_req->class_id = req->class_id;
	footer_sub_req->status_id = req->status_id;
	footer_sub_req->data_from = req->data_from;
	footer_sub_req->data_group = req->data_group;
	footer_sub_req->is_footer = true;
	footer_sub_req->err_info = err_info;
	footer_sub_req->item_num = item_num;
	footer_sub_req->schema_fail_cnt = schema_fail_cnt;	
	footer_sub_req->md5_hex.assign(md5_hex);	
	footer_sub_req->timestamp_id = req->timestamp_id;
	footer_sub_req->url = req->url;
	footer_sub_req->is_changed = req->is_changed;
	footer_sub_req->ds_id	= req->ds_id;
	if(location.length() > 0)
	{
		footer_sub_req->location = location;
	}else
		footer_sub_req->location = "";
	footer_sub_req->tplid = req->tplid;
	footer_sub_req->class_id = req->class_id;
	footer_sub_req->startTime = req->startTime;

	ACE_Message_Block *msg = new ACE_Message_Block( reinterpret_cast<char *>(footer_sub_req) );
	thisHandler->put(msg);
	SS_DEBUG((LM_TRACE, "after put msg res_id %d, staus_id %d footer %d\n", footer_sub_req->res_id, footer_sub_req->status_id,  footer_sub_req->is_footer));

	if(validityContext)
		xmlSchemaFreeValidCtxt(validityContext);
	if(schemaParser)
		xmlSchemaFreeParserCtxt(schemaParser);
	if(schema)
		xmlSchemaFree(schema);

	sprintf(buf, "update  resource_status set err_info  = '%s' where id = %d", err_info.c_str(), req->status_id);
	timeval sql_beg17;
	gettimeofday(&sql_beg17, NULL);
	sql_write(mysql_vr, buf);
	timeval sql_end17;
	gettimeofday(&sql_end17, NULL);
	time_t sql_cost17;
	sql_cost17 = 1000000*(sql_end17.tv_sec - sql_beg17.tv_sec) + sql_end17.tv_usec - sql_beg17.tv_usec;
	sql_cost.write_cost += sql_cost17;
	sql_cost.total_cost += sql_cost17;
	req->writeTime=(ACE_OS::gettimeofday()-testTime1).usec();
	SS_DEBUG((LM_TRACE, "[CWriteTask::TEST]process(%@): [exstat] write items %d of %d in %s time cost: %d\n",
				this, write_num, item_num, req->url.c_str(),req->writeTime));
	return 0;
}

int CWriteTask::sql_read( CMysqlQuery &query, CMysql &mysql, const char *sql, int vi ){
	int ret = query.execute(sql);
	while( ret == -1 ) {
		if(mysql.errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CWriteTask::sql_read(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql, mysql.error(), mysql.errorno()));
			return -1;
		}
		ret = query.execute(sql);
	}
	SS_DEBUG((LM_DEBUG, "CWriteTask::sql_read(%@):Mysql[%s] success %d\n",this, sql, query.rows() ));
	return query.rows();
}

int CWriteTask::sql_write( CMysql &mysql, const char *sql, int vi )
{
	CMysqlExecute exe(&mysql);
	int ret = exe.execute(sql);
	while( ret == -1 ) {
		if(mysql.errorno() != CR_SERVER_GONE_ERROR){
			SS_DEBUG((LM_ERROR,"CWriteTask::sql_write(%@):Mysql[%s]failed, reason:[%s],errorno[%d]\n",this,
						sql, mysql.error(), mysql.errorno()));
			return -1;
		}
		ret =  exe.execute(sql);
	}
	SS_DEBUG((LM_TRACE, "CWriteTask::sql_write(%@):Mysql[%s] success %d\n",this, Util::utf8_to_gbk(sql).c_str(), ret ));
	return ret;
}


int CWriteTask::svc() 
{
	//写qdb以及mysql库
	CMysql mysql_vr(vr_ip, vr_user, vr_pass, vr_db, "set names utf8");
	int reconn = 0;
	//mysql连接
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_vr.isClosed() ) 
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql_vr.connect() )
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect vr fail: %s :%s: %s :%s\n",
						this, mysql_vr.error(),vr_ip.c_str(), vr_user.c_str(), vr_pass.c_str()));
			suc = false;
		}
		if( !mysql_vr.open() ) 
		{
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: open database fail: %s\n",this, mysql_vr.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql_vr);
			executor.execute("set names utf8");
			SS_DEBUG((LM_TRACE, "CWriteTask::svc(%@):Mysql vr success\n",this));
			break;
		}
		reconn ++;
		mysql_vr.close();
		mysql_vr.init();
	}
	if( reconn >=5 ) 
	{//5次连接不上就退出了
		SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect vr fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}//vr_data的连接
	CMysql mysql_vi(vi_ip, vi_user, vi_pass, vi_db, "set names gbk");
	reconn = 0;
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_vi.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql_vi.connect() ){
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect vi fail: %s\n",this, mysql_vi.error()));
			suc = false;
		}
		if( !mysql_vi.open() ) {
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: open database fail: %s\n",this, mysql_vi.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql_vi);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "CWriteTask::svc(%@):Mysql vi success\n",this));
			break;
		}
		reconn ++;
		mysql_vi.close();
		mysql_vi.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect vi fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}//vr_info(blacklist)库的连接

	CMysql mysql_cr(cr_ip, cr_user, cr_pass, cr_db, "set names gbk");
	reconn = 0;
	while( reconn < 5 )
	{
		bool suc = true;
		if( mysql_cr.isClosed() ) {
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: init fail\n", this));
			suc = false;
		}
		if( !mysql_cr.connect() ){
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect cr fail: %s\n",this, mysql_cr.error()));
			suc = false;
		}
		if( !mysql_cr.open() ) {
			SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: open database fail: %s\n",this, mysql_cr.error()));
			suc = false;
		}
		if( suc ){
			CMysqlExecute executor(&mysql_cr);
			executor.execute("set names gbk");
			SS_DEBUG((LM_TRACE, "CWriteTask::svc(%@):Mysql cr success\n",this));
			break;
		}
		reconn ++;
		mysql_cr.close();
		mysql_cr.init();
	}
	if( reconn >=5 ) 
	{
		SS_DEBUG((LM_ERROR, "CWriteTask::svc(%@):Mysql: connect cr fail\n", this));
		//exit(-1);
		ACE_Reactor::instance()->end_reactor_event_loop();
		return -1;
	}

	CMysqlHandle thisHandler;
	thisHandler.open(configName);

	CurlHttp curl;
	curl.init(200, 10, 1);
	char * simple_buf = (char*) malloc(1024*1024);
	SS_DEBUG((LM_TRACE, "CWriteTask::svc(%@): begin to loop\n",this));
	int ret = 0;
	for(ACE_Message_Block *msg_blk; getq(msg_blk) != -1; ) 
	{
		request_t  *req = reinterpret_cast<request_t *>(msg_blk->rd_ptr());
		SS_DEBUG(( LM_TRACE, "CWriteTask::svc now processing %s, class_id:%d, queue_len:%d\n", req->url.c_str(), req->class_id, msg_queue()->message_count()));

		unsigned int update_statistic = 0;
		unsigned int insertion_statistic = 0;
		time_t qdb_cost = 0;
		time_t xml_md5_cost = 0;
		time_t insert_update_cost = 0;
		time_t preprocess_cost = 0;

		struct mysql_cost sql_cost;
		sql_cost.read_cost = 0;
		sql_cost.write_cost = 0;
		sql_cost.total_cost = 0;

		struct libxml_cost xml_cost;

		xml_cost.readDoc_cost = 0;
		xml_cost.keySearch_cost = 0;
		xml_cost.validate_cost = 0;
		xml_cost.parseXml_cost = 0;
		xml_cost.addProp_cost = 0;
		xml_cost.checkLen_cost = 0;
		xml_cost.createNew_cost = 0;
		xml_cost.total_cost = 0;

		time_t schema_check_cost = 0;
		timeval tm_beg;
		gettimeofday(&tm_beg, NULL);

		if (req->tag.compare("customer") != 0)
		{
		//fprintf(stderr, "[TEST] insertion_statistic beg %s",req->xml.c_str());
			ret = process(req, req->xml, mysql_vr, mysql_vi, mysql_cr, simple_buf,
					preprocess_cost,update_statistic,insertion_statistic,sql_cost, xml_cost, xml_md5_cost,
					insert_update_cost,qdb_cost,schema_check_cost, &thisHandler, curl);
			fprintf(stderr, "[Record Detector Write statistic] update_statistic: %u, insertion_statistic: %u, ",update_statistic,insertion_statistic);
			timeval tm_end;
			gettimeofday(&tm_end, NULL);
			time_t  tm_cost ;
			tm_cost = 1000000*(tm_end.tv_sec - tm_beg.tv_sec) + tm_end.tv_usec - tm_beg.tv_usec;
			fprintf(stderr, "req_id: %d, status_id: %d, total_cost: %ld, preprocess_cost: %ld\nmysql_totalcost: %ld, msyql_readcost: %ld, mysql_writecost: %ld\nlibxml_totalcost: %ld, readdoc: %ld, keysearch: %ld, validate: %ld, parse: %ld, addProp: %ld, checklen: %ld, newDoc: %ld\nxml_md5_cost: %ld, schema_check_cost: %ld, qdb_cost: %lu\n",
					req->res_id, req->status_id, tm_cost, preprocess_cost,sql_cost.total_cost,sql_cost.read_cost,sql_cost.write_cost,
					xml_cost.total_cost,xml_cost.readDoc_cost,xml_cost.keySearch_cost,xml_cost.validate_cost,xml_cost.parseXml_cost,
					xml_cost.addProp_cost, xml_cost.checkLen_cost,xml_cost.createNew_cost,
					xml_md5_cost,schema_check_cost,qdb_cost);
		}
		else
		{
			ret = process_customer_res(req, req->xml, mysql_cr);
		}

		//如果没有放到下一级流水，就需要置空fetch_map，否则该项就再也不会再次被抓取
		if(ret != 0) {
			std::string unqi_key;
			if(req->tag.compare("customer") != 0)
				unqi_key= req->classid_str+"#" + req->url;
			else
				unqi_key = "customer#" + req->url;
			pthread_mutex_lock(&fetch_map_mutex);//lock
			if( fetch_map.find(unqi_key) != fetch_map.end() && fetch_map[unqi_key] > 0 ){
				fetch_map[unqi_key] = 0;
				SS_DEBUG((LM_TRACE, "CWriteTask::svc class_id:%d, url:%s, process end.\n", req->class_id, req->url.c_str()));
			}else{
				SS_DEBUG(( LM_ERROR, "CWriteTask::svc(%@): map error: %d %s\n",this, fetch_map[unqi_key], req->url.c_str() ));
			}
			pthread_mutex_unlock(&fetch_map_mutex);//unlock
		}

		SS_DEBUG((LM_TRACE,"[CWriteTask::svc]process:scan:%d,fetch:%d,write:%d,total:%d\n",
					req->scanTime,req->fetchTime,req->writeTime,(ACE_OS::gettimeofday()-req->startTime).usec()));
		msg_blk->release();
		delete req;
		_mem_check --;
		SS_DEBUG(( LM_TRACE, "CWriteTask::svc(%@): memdebug: %d\n",this, _mem_check ));
	}
	thisHandler.stopTask();
	free(simple_buf);
	SS_DEBUG((LM_TRACE, "CWriteTask::svc(%@): exit from loop\n", this));
	return 0;
}

int CWriteTask::stopTask()
{
	//vrdb->clear(); delete vrdb;
	pthread_rwlock_destroy(&m_length_rwlock);
	pthread_mutex_destroy(&m_validCtxt_mutex);
	//pthread_mutex_destroy(&m_wrt_mtx);

	flush();
	wait();
	return 0;
}


