#include "string_Util.h"
#include <errno.h>
#include <cctype>

#define STRHTTP  "http://"
#define STRHTTP_LEN  7

static int s_iconv(const std::string &src, std::string &result,std::string in,std::string out)
{
	
	int max_len = src.length()*2+1;
	
	iconv_t cd = iconv_open(out.c_str(),in.c_str());
	if (cd<0){
		return -1;	
	}

	char *buf = new char[max_len];
	if (buf == NULL)
		return -1;

	char *inbuff = const_cast<char *>(src.c_str());

	size_t inbytesleft = src.length();
	char *outbuff = buf;
	size_t outbytesleft = max_len;
	size_t ret = iconv(cd, &inbuff, &inbytesleft, &outbuff, &outbytesleft);

	if(ret == size_t(-1))
	{
		printf("iconv failed: %s\n", strerror(errno));
	}else {
		size_t content_len = max_len - outbytesleft;
		//printf("content_len: %d\n", content_len);
		//printf("out len : %s\n",strlen(buf));
		result.assign(buf,content_len);
	}
	iconv_close(cd);
	delete []buf;
	return ret;
}


int utf16_to_utf8(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF16LE","UTF8");
}
int utf8_to_utf16(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF8","UTF16LE");
}
int utf16_to_gbk(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF16LE","GB18030");
}
int gbk_to_utf16(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"GB18030","UTF16LE");
}
int utf8_to_gbk(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF8","GB18030");
}
int gbk_to_utf8(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"GB18030","UTF8");
}


std::string trim_left(std::string &s,const std::string &drop=" ")
{
	return s.erase(0,s.find_first_not_of(drop));
}

std::string trim(std::string &s,const std::string &drop=" ")
{
	s.erase(s.find_last_not_of(drop)+1);
	return s.erase(0,s.find_first_not_of(drop));
}

std::string  trimall(std::string &src)
{
	int len=src.length();
	if(len==0)
		return "";
	int i=0;
	while(i<len&&isspace(src[i])){
		char temp[2];
		temp[0]=src[i];
		temp[1]='\0';
		trim(src,temp);
		++i;
	}
	len=src.length()-1;
	while(len>=0&&isspace(src[len])){
		char temp[2];
		temp[0]=src[len];
		temp[1]='\0';
		trim(src,temp);		
		--len;
	}
	return src;
}

//处理gbk的截长问题
std::string cut_strLen(std::string &src,int len){
	unsigned char tmp;
	int realLen=0;
	
	if (src.length()<=len)
	{
		return src;
	}else{
		for (int i=0;i<src.length();++i)
		{
			tmp=src[i];
			if (tmp>=128)
			{
				if ((i+1)>(len-1))
				{
					break;
				}
				realLen=i+1;
				++i;
			}else{
				if(i>(len-1))
					break;
				realLen=i;
			}
		}
		src=src.substr(0,realLen);
		//src=src.substr(0,realLen+2);
		//src[realLen+1]='\0';
	}
	return src;
}

//全角转半角
std::string ToDBS(std::string str) {
	std::string result = "";
	unsigned char tmp;
	unsigned char tmp1;
	for (unsigned int i = 0; i < str.length(); i++) {
		tmp = str[i];
		tmp1 = str[i + 1];
		if (tmp == 163) {
			result += (unsigned char) str[i + 1] - 128;
			i++;
			continue;
		} else if (tmp > 163) {
			result += str.substr(i, 2);
			i++;
			continue;
		} else if (tmp >=128) {
			if(tmp==161&&tmp1==161)
				result += " ";
			i++;
			continue;
		} else {
			result += str.substr(i, 1);
		}
	}
	return result;
}

void toUpper(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =toupper(*p);
	}
}

void toLower(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =tolower(*p);
	}
}

char * cltrim ( char * sLine )
{
	if(sLine==NULL)
		return sLine;
	char* pos=sLine;
	while ( *pos && isspace(*pos) )
		pos++;
	strcpy(sLine,pos);
	return sLine;
}
char * crtrim ( char * sLine )
{
	if (sLine==NULL)
		return sLine;
	char * p = sLine + strlen(sLine) - 1;
	while ( p>=sLine && isspace(*p) )
		p--;
	p[1] = '\0';
	return sLine;
}
char * ctrim ( char * sLine )
{
	if (sLine==NULL)
	{
		return sLine;
	}
	return cltrim ( crtrim ( sLine ) );
}

char* cfind(char* src,char des){
	if(src==NULL||des=='\0'){
		fprintf(stderr,"[cfind]src==NULL or des=\0\n");
		return NULL;
	}
	while((*src)&&(*src)!=des) ++src;
	if((*src)==des)
		return src;
	return NULL;
}

char * cfind ( char * src,  char * des )
{
	if(src==NULL||des==NULL)
		return NULL;
	while ((*src)&&(strncmp(src,des,strlen(des))!=0))
	{
		++src;
	}
	if ((*src)=='\0')
	{
		return NULL;
	}
	return src;
}

int strtoks(char *src,char *sep,std::vector<std::string> &ret){
	ret.clear();
	
	fprintf(stderr,"[strtoks]%s:%s\n",src,sep);
	char *pos = NULL;
	
	int len;
	char des[1024];
	des[1023]='\0';	
	while(pos=cfind(src,sep)){
		len=pos-src;
		strncpy(des,src,len);
		des[len]='\0';
		fprintf(stderr,"key=%s\n",des);
		ret.push_back(des);
		src=pos+1;
	}
	strncpy(des,src,1024);
	fprintf(stderr,"key=%s\n",des);
    ret.push_back(des);
	return 0;
}



int getkeyofUrl( char* url,char* ukey,int &nextPos){
	const char* curpos;
	if (url==NULL||ukey==NULL)
	{
		SS_DEBUG((LM_DEBUG, "getkeyofUrl:url ,ukey is NULL!\n"));
		return -1;
	}


	if (nextPos==0)
	{
		//cut blank char
		while ((*url)==' '||(*url)=='\t')
		{
			++nextPos;
		}
		if (strncmp(url,STRHTTP,STRHTTP_LEN)==0)
		{
			nextPos+=STRHTTP_LEN;
		}
	}
	curpos=url+nextPos;
	if((*curpos)=='\0'||(*curpos)==' '||(*curpos)=='\t')
		return -1;
	const char* keyBeg=curpos;

	while ((*curpos)!='\0'&&(*curpos)!='.'&&(*curpos)!='\\'
		&&(*curpos)!='/'&&(*curpos)!='\t'&&(*curpos)!=' ')
	{
		++curpos;
	}
	strncpy(ukey,keyBeg,curpos-keyBeg);
	nextPos=curpos-url+1;
	if((*curpos)=='\0'||(*curpos)==' '||(*curpos)=='\t')
		nextPos=curpos-url;
	ukey[curpos-keyBeg]='\0';
	return 0;
}

int getHostofUrl( char* srcUrl,char* dhost,int hostLen){
	if (srcUrl==NULL||dhost==NULL)
	{
		SS_DEBUG((LM_DEBUG, "getHostofUrl:desString or srcUrl is NULL!\n"));
		return -1;
	}
	ctrim(srcUrl);
	if(strlen(srcUrl)==0)
		return -1;
	 char *pos=srcUrl;
	 char *urlBeg;
	 char *urlEnd;

	/*while((*pos)&&((*pos)!='h'||strncmp(pos,STRHTTP,STRHTTP_LEN)!=0)){
		++pos;
	}*/
	char* pfind=cfind(pos,STRHTTP);
	if (pfind==NULL)
	{
		pfind=pos;
	}else
	{
		pos=pfind+STRHTTP_LEN;
	}
	urlBeg=pfind;
	while((*pos)&&(*pos)!='/'){
		++pos;
	}
	urlEnd=pos-1;
	if((urlEnd-urlBeg+1)<=0)
		return -1;
	strncpy(dhost,urlBeg,urlEnd-urlBeg+1);
	if((urlEnd-urlBeg+1)>=hostLen)
		return -1;
	dhost[urlEnd-urlBeg+1]='\0';
	return 0;
}
