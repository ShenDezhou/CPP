#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include "reader_log.h"
#include "PageStore.hpp"



const char SEPRATOR[] = "@|";

static std::vector<std::string> split(std::string str,std::string pattern)
{
	std::string::size_type pos;
	std::vector<std::string> result;
	str += pattern;   
	int size = str.size();

	for(int i = 0; i<size; i++)
	{   
		pos = str.find(pattern,i);
		if(pos<size)
		{   
			std::string s = str.substr(i,pos-i);
			result.push_back(s);
			i = pos + pattern.size()-1;
		}   
	}   

	return result;
}

static std::vector<std::bstring> bstr_split(std::bstring str,std::bstring pattern)
{
	std::bstring::size_type pos;
	std::vector<std::bstring> result;
	str += pattern;   
	int size = str.size();

	for(int i = 0; i<size; i++)
	{   
		pos = str.find(pattern,i);
		if(pos<size)
		{   
			std::bstring s = str.substr(i,pos-i);
			result.push_back(s);
			i = pos + pattern.size()-1;
		}   
	}   

	return result;
}
string& replace_all_distinct(string str, string old_value, string new_value) 
{ 
	string &ret = str;
	for(string::size_type pos(0); pos != string::npos; pos += new_value.length())
	{ 
		if( (pos=ret.find(old_value,pos))!=string::npos ) 
			ret.replace(pos,old_value.length(),new_value); 
		else break; 
	} 
	return ret; 
}
/*
std::bstring& replace_all_distinct(std::bstring str, std::bstring old_value, std::bstring new_value) 
{ 
	std::bstring &ret = str;
	for(std::bstring::size_type pos(0); pos != std::bstring::npos; pos += new_value.length())
	{   
		if( (pos=ret.find(old_value,pos))!=std::bstring::npos ) 
			ret.replace(pos,old_value.length(),new_value); 
		else break; 
	}   
	return ret; 
}
*/
PageStore::PageStore(std::string name, int index, int max_linenum)
{
	m_linenum = 0;
	m_filename = name;
	m_index = index;
	m_max_linenum = max_linenum;
	char index_str[16] = {'\0'};
	snprintf(index_str, sizeof(index_str) - 1,"%d", m_index);
	string file_name = m_filename + index_str;
	m_file.open(file_name.c_str(), ios::app);
	pthread_mutex_init(&m_lock, NULL);
}

PageStore::~PageStore()
{
	m_file.close();
	pthread_mutex_destroy(&m_lock);
}

bool PageStore::WritePage(const XmlPage &page)
{
	pthread_mutex_lock(&m_lock);
	std::string doc;
	//reader_log_error("start doc:%s\n",SS_TO_ASCIIX((bchar_t*)page.doc));
	//string end_utf16,null_utf16;
	//gbk_to_utf16("\n", end_utf16);
	//gbk_to_utf16("", null_utf16);
	//reader_log_error("end doc:%s\n",SS_TO_ASCIIX((bchar_t*)doc.c_str()));

	if (!m_file)
	{
		char index_str[16] = {'\0'};
		snprintf(index_str, sizeof(index_str) - 1,"%d", m_index);
		string file_name = m_filename + index_str;
		m_file.open(file_name.c_str(), ios::app);
	}
	
	string url_utf8,doc_utf8,objid_utf8;
	string src;
	int max_len = page.doc_len * 2 + 1;
	iconv_t cd = iconv_open("UTF8","UTF16LE");
	if (cd<0){
		return -1;	
	}

	char *buf = new char[max_len];
	if (buf == NULL)
		return -1;

	char *inbuff = (char *)page.doc;
	//reader_log_error("inbuff:%s\n",SS_TO_ASCIIX((bchar_t*)inbuff));

	size_t inbytesleft = page.doc_len;
	char *outbuff = buf;
	size_t outbytesleft = max_len;
	size_t ret = iconv(cd, &inbuff, &inbytesleft, &outbuff, &outbytesleft);

	if(ret == size_t(-1))
	{
		fprintf(stderr,"iconv failed: %s\n", strerror(errno));
	}else {
		size_t content_len = max_len - outbytesleft;
		//fprintf(stderr,"content_len: %d\n", content_len);
		//fprintf(stderr,"out len : %d\n",strlen(buf));
		doc_utf8.assign(buf,content_len);
	}
	iconv_close(cd);
	delete []buf;

	doc = replace_all_distinct(doc_utf8, "\n", "");
	gbk_to_utf8(page.url, url_utf8);
	gbk_to_utf8(page.objid, url_utf8);
	//reader_log_error("writing doc:%s\n",doc_utf8.c_str());	

	string seprator_utf8;
	gbk_to_utf8(SEPRATOR, seprator_utf8);
	m_file<<url_utf8<<seprator_utf8<<objid_utf8<<seprator_utf8<<doc<<seprator_utf8<<page.doc_len<<seprator_utf8<<page.status<<seprator_utf8<<page.update_type<<endl;

	//m_file<<SS_TO_UTF16(page.url)<<SS_TO_UTF16(SEPRATOR)<<SS_TO_UTF16(page.objid)<<SS_TO_UTF16(SEPRATOR)<<doc<<SS_TO_UTF16(SEPRATOR)<<page.doc_len<<SS_TO_UTF16(SEPRATOR)<<page.status<<SS_TO_UTF16(SEPRATOR)<<page.update_type<<endl;
	m_linenum++;
	//reader_log_error("m_linenum:%d, url:%s\n",m_linenum, page.url);	
	if (m_linenum >= m_max_linenum)
	{
		m_file.close();
		m_index++;
		char index_str[16] = {'\0'};
		snprintf(index_str, sizeof(index_str) - 1,"%d", m_index);
		string file_name = m_filename + index_str;
		m_file.open(file_name.c_str(), ios::app);
		m_linenum = 0;
	}
	pthread_mutex_unlock(&m_lock);

	return true;
}

bool PageStore::SendPage(SummarySender *summary_sender)
{
	bool readed = false;

	for (int i = 0; i < m_index + 1; i++)
	{
		char index_str[16] = {'\0'};
		snprintf(index_str, sizeof(index_str) - 1,"%d", i);
		string file_name = m_filename + index_str;
		std::ifstream fin(file_name);
		std::string line;

		if(!fin)
		{  
			cerr<<"no such file:"<<file_name<<endl;
			continue;
		}

		while(getline(fin,line))
		{
			XmlPage page;
			string seprator_utf8;
			gbk_to_utf8(SEPRATOR, seprator_utf8);
			vector<std::string> content = split(line, seprator_utf8);
			if(content.size() < 6)
			{
				cerr<<"invalid content:"<<line<<endl;
				continue;
			}
			page.url = (char *)content[0].c_str();
			page.objid = (char *)content[1].c_str();
			
			string doc_utf16;
			utf8_to_utf16(content[2].c_str(), doc_utf16);
			page.doc = (char *)doc_utf16.c_str();
			page.doc_len = atoi(content[3].c_str());
			page.status = atoi(content[4].c_str());
			page.update_type = atoi(content[5].c_str());
			cerr<<"page url:"<<page.url<<endl;
			//cerr<<"page doc:"<<SS_TO_ASCIIX((bchar_t*)(page.doc))<<endl;
			cerr<<"page status:"<<page.status<<endl;
			cerr<<"doc_len:"<<page.doc_len<<endl;
			readed = true;

			string doc_gbk;
			utf8_to_gbk(content[2], doc_gbk);
			if (ValidDoc(doc_gbk) && page.doc_len > 0)
			{
				reader_log_error("sending bak summary file......\n");	
				if(summary_sender != NULL && summary_sender->send(&page) == -1)
				{
					reader_log_error("sending bak summary file failed,writing file......\n");	
					WritePage(page);
					return false;
				}
			}
		}

		if (readed)
		{
			reader_log_error("bak summary file finished:%s\n",file_name.c_str());	
			pthread_mutex_lock(&m_lock);
			if (i == m_index)
			{
				m_file.close();
				m_index = 0;
				m_linenum = 0;
				remove(file_name.c_str());
				pthread_mutex_unlock(&m_lock);
				readed = false;

				char index_str[16] = {'\0'};
				snprintf(index_str, sizeof(index_str) - 1,"%d", m_index);
				string file_name = m_filename + index_str;
				m_file.open(file_name.c_str(), ios::app);
				return true;
			}
			remove(file_name.c_str());
			pthread_mutex_unlock(&m_lock);
			readed = false;
		}
	}

	return true;
}

bool PageStore::SendPage(Sender *index_sender)
{
	bool readed = false;

	for (int i = 0; i < m_index + 1; i++)
	{
		char index_str[16] = {'\0'};
		snprintf(index_str, sizeof(index_str) - 1,"%d", i);
		string file_name = m_filename + index_str;
		std::ifstream fin(file_name);
		std::string line;

		if(!fin)
		{  
			cerr<<"no such file:"<<file_name<<endl;
			continue;
		}

		while(getline(fin,line))
		{
			XmlPage page;
			string seprator_utf8;
			gbk_to_utf8(SEPRATOR, seprator_utf8);
			vector<std::string> content = split(line, seprator_utf8);
			if(content.size() < 6)
			{
				cerr<<"invalid content:"<<line<<endl;
				continue;
			}
			page.url = (char *)content[0].c_str();
			page.objid = (char *)content[1].c_str();
			
			string doc_utf16;
			utf8_to_utf16(content[2].c_str(), doc_utf16);
			page.doc = (char *)doc_utf16.c_str();
			page.doc_len = atoi(content[3].c_str());
			page.status = atoi(content[4].c_str());
			page.update_type = atoi(content[5].c_str());
			cerr<<"page url:"<<page.url<<endl;
			//cerr<<"page doc:"<<SS_TO_ASCIIX((bchar_t*)(page.doc))<<endl;
			cerr<<"page status:"<<page.status<<endl;
			cerr<<"doc_len:"<<page.doc_len<<endl;
			readed = true;

			string doc_gbk;
			utf8_to_gbk(content[2], doc_gbk);
			if (ValidDoc(doc_gbk) && page.doc_len > 0)
			{
				reader_log_error("sending bak index file......\n");	
				if(index_sender != NULL && index_sender->send(&page) == -1)
				{
					reader_log_error("sending bak index file failed,writing file......\n");	
					WritePage(page);
					return false;
				}
			}
		}

		if (readed)
		{
			reader_log_error("bak index file finished:%s\n",file_name.c_str());	
			pthread_mutex_lock(&m_lock);
			if (i == m_index)
			{
				m_file.close();
				m_index = 0;
				m_linenum = 0;
				remove(file_name.c_str());
				pthread_mutex_unlock(&m_lock);
				readed = false;

				char index_str[16] = {'\0'};
				snprintf(index_str, sizeof(index_str) - 1,"%d", m_index);
				string file_name = m_filename + index_str;
				m_file.open(file_name.c_str(), ios::app);
				return true;
			}
			remove(file_name.c_str());
			pthread_mutex_unlock(&m_lock);
			readed = false;
		}
	}

	return true;
}

bool PageStore::ValidDoc(string doc)
{
	int pos_start = -1,pos_end = -1,followers_count = 0;
	string start_str = "<expire_time>", url = "";
	pos_start = doc.find(start_str);
	if(pos_start == -1)
	{
		return true;
	}

	pos_start += start_str.length();
	pos_end = doc.find("</expire_time>", pos_start);
	if(pos_end == -1)
	{
		return true;
	}
	string expiration = doc.substr(pos_start, pos_end - pos_start);

	start_str = "<status>", url = "";
	pos_start = doc.find(start_str);
	if(pos_start == -1)
	{
		return true;
	}

	pos_start += start_str.length();
	pos_end = doc.find("</status>", pos_start);
	if(pos_end == -1)
	{
		return true;
	}
	string status = doc.substr(pos_start, pos_end - pos_start);

	if (status == "DELETED")
	{
		return true;
	}
	else
	{
		vector<string> content = split(expiration, " ");
		time_t timep;
		struct tm tm1;

		if(strptime(expiration.c_str() + 4, "%b %d %H:%M:%S %Y", &tm1))
		{
			timep = mktime(&tm1);
			if (time(NULL) > timep)
			{
				reader_log_error("xml data expired......\n");	
				return false;
			}
		}

		return true;
	}
}
