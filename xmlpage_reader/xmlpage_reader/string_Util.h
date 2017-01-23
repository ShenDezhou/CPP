#ifndef STRING_UTIL_H
#define STRING_UTIL_H
#include <string.h>
#include <string>
#include <vector>
#include "Platform/log.h"
#include <iconv.h>

//LOG
//WEBSEARCH_ERROR((LM_TRACE, "[main] exit received\n"));
//WEBSEARCH_ERROR((LM_ERROR, "[main] [read config error]\n"));
//WEBSEARCH_DEBUG((LM_TRACE, "Handler::dataHandle: xml Parse: item discarded!\n"));
//WEBSEARCH_DEBUG((LM_DEBUG, "Handler::dataHandle: xml Parse: item discarded!\n"));
//
int utf16_to_utf8(const std::string &src, std::string &result);
int utf8_to_utf16(const std::string &src, std::string &result);
int utf16_to_gbk(const std::string &src, std::string &result);
int gbk_to_utf16(const std::string &src, std::string &result);
int utf8_to_gbk(const std::string &src, std::string &result);
int gbk_to_utf8(const std::string &src, std::string &result);


//string strim
std::string trim_left(std::string &s,const std::string &drop);
std::string trim(std::string &s,const std::string &drop);
std::string trimall(std::string &src);

std::string cut_strLen(std::string &src,int len);


void toUpper(std::basic_string<char>& s);
void toLower(std::basic_string<char>& s);
std::string ToDBS(std::string str);

char * cltrim ( char * sLine );
char * crtrim ( char * sLine );
char * ctrim ( char * sLine );
char * cfind ( char * src,  char * des );
char*  cfind(char* src,char des);

int strtoks(char *src,char *sep,std::vector<std::string> &ret);

//url process
int getkeyofUrl(char* url,char* ukey,int &nextPos);
int getHostofUrl( char* srcUrl,char* dhost,int hostLen);
#endif
