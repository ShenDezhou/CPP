#include "../Sender.hpp"
#include "../QdbWriter.hpp"
#include <Platform/log.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <fstream>
#include <iterator>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

using namespace std;

void USAGE(const char* bin_name)
{
    printf("Usage:\n");
    printf("        %s [options]\n\n", bin_name);
    printf("Options:\n");
    printf("        -h:          Show help messages.\n");
    printf("        -f fileName:          XML Item File Name.\n");
    printf("        -q qdbAddr:           QDB Address(host@port).\n");
    printf("        -r readerAddr:        XML Reader Address(host@port) (optional).\n");
}

static inline std::string& replace_all_distinct(std::string& str,const std::string& old_value,const std::string& new_value){
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

int delete_item(string &xml,QdbWriter *writer,Sender *sender)
{
    int pos = 0, pos_end = 0;
    //printf("%s\n",xml.c_str());
    
    std::string url,uurl;
    if( -1 != (pos=xml.find("<unique_url><![CDATA["))  && -1 != (pos_end=xml.find("]]></unique_url>")) ){
        url = xml.substr(pos+strlen("<unique_url><![CDATA["), pos_end-(pos+strlen("<unique_url><![CDATA[")) );
        SS_DEBUG((LM_TRACE, "unique_url: %s\n", url.c_str() ));
    }else{
        SS_DEBUG((LM_ERROR, "unique_url find failed.\n"));
        return -1;
    }
    uurl = url;
    replace_all_distinct(uurl,"&amp;","&");
    SS_DEBUG((LM_TRACE, "new unique_url: %s\n", uurl.c_str() ));
    std::string source;
    if( -1 != (pos=xml.find("<source>"))  && -1 != (pos_end=xml.find("</source>")) ){
        source = xml.substr(pos+strlen("<source>"), pos_end-(pos+strlen("<source>")) );
    }else{
        SS_DEBUG((LM_ERROR, "source find failed.\n"));
        return -1;
    }
    std::string key;
    if( -1 != (pos=xml.find("<key index_level=\"0\"><![CDATA["))  && -1 != (pos_end=xml.find("]]></key>")) ){
        key = xml.substr(pos+strlen("<key index_level=\"0\"><![CDATA["), pos_end-(pos+strlen("<key index_level=\"0\"><![CDATA[")) );
    }else if( -1 != (pos=xml.find("<key index_level=\"0\">"))  && -1 != (pos_end=xml.find("</key>")) ){
        key = xml.substr(pos+strlen("<key index_level=\"0\">"), pos_end-(pos+strlen("<key index_level=\"0\">")) );
    }else{
        SS_DEBUG((LM_ERROR, "key find failed.\n"));
        return -1;
    }
    SS_DEBUG((LM_TRACE,"key is %s\n",key.c_str()));
    if((pos=xml.find("<status>")) != -1)
    {
        xml.replace(pos+strlen("<status>"),6,std::string("DELETE"));
    }
    else
    {
        if((pos=xml.find("</item>")) != -1)
        {
            xml.insert(pos,"<status>DELETE</status>");
        }
        else
        {
            SS_DEBUG((LM_ERROR,"status find failed.\n"));
            return -1;
        }
    }
    int ret = -1;
    int ret2 = -1;
        
    while((ret = writer->del(url.c_str())) < 0 && (writer->del(uurl.c_str())) < 0)
    {
        SS_DEBUG((LM_TRACE,"delete %s failed.\n",url.c_str()));
    }
    
    SS_DEBUG((LM_TRACE, "qdb writer delete %d  %s\n", ret, url.c_str() ));
    
    if(sender != NULL){
        ret2 = sender->send_to(url.c_str(), xml.c_str(), xml.length() );
        SS_DEBUG((LM_TRACE, "pagereader sender send %d %s to be deleted\n", ret2, url.c_str() ));
    }
    
    if(!sender || (ret >= 0 && ret2 >= 0))
        printf("Delete xml item succeed.\n");
    
    return 0;
}

SS_LOG_MODULE_DEF(exvr_fetcher_server);

int main(int argc, char** argv)
{
    const char *reader_addr = NULL;
    const char *qdb_addr = NULL;
    const char *item_file = NULL;
    char optchar;
    Sender *m_sender;
    QdbWriter *m_writer;
    
    while((optchar = getopt(argc,argv,"f:q:r:h")) != -1)
    {
        switch(optchar)
        {
        case 'f':
            item_file   = optarg;
            break;
        case 'q':
            qdb_addr    = optarg;
            break;
        case 'r':
            reader_addr = optarg;
            break;
        case 'h':
            USAGE(argv[0]);
            return 0;
        default:
            break;
        }
    }
    
    if(!item_file || !qdb_addr)
    {
        USAGE(argv[0]);
        return -1;
    }
    setenv("LC_ALL","zh_CN.GBK",1);
    
    m_writer = new QdbWriter(qdb_addr);
    if(reader_addr != NULL)
        m_sender = new Sender(reader_addr);
    else
        m_sender = NULL;

    //ifstream inFile(item_file);
    std::string xml;

    /*istream_iterator<std::string> DataBegin(inFile);
    istream_iterator<std::string> DataEnd;

    while(DataBegin != DataEnd){
        xml += *DataBegin;
        DataBegin++;
        }*/
    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    fp = fopen(item_file, "r");
        
    if (fp == NULL)
        exit(EXIT_FAILURE);
    int pos = 0, pos_end = 0;
    
    while ((read = getline(&line, &len, fp)) != -1) {
        
        //printf("Retrieved line of length %zu: %s", read,line);
        
        xml += line;
        if((pos = xml.find("<?xml version=\"1.0\" encoding=\"gbk\"?>") )== -1 || (pos_end = xml.find("<!--STATUS VR OK-->") )== -1)
        {
            continue;
        }
        
        string item = xml.substr(pos,pos_end-pos+strlen("<!--STATUS VR OK-->"));
        //printf("%s\n",item.c_str());
        
        xml.clear();
        
        delete_item(item,m_writer,m_sender);
    }
    
    if (line)
    {
        free(line);
    }
    
    
    delete m_writer;
    delete m_sender;

    return 0;
}
