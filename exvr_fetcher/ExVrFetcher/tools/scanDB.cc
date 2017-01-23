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
#include <unistd.h>
#include "offdb/QuickdbAdapter.h"
#include <vector>
#include <string.h>
#include <time.h>
using namespace std;

void USAGE(const char* bin_name)
{
    printf("Usage:\n");
    printf("        %s [options]\n\n", bin_name);
    printf("Options:\n");
    printf("        -h:                   Show help messages.\n");
    printf("        -q qdbAddr:           QDB Address(host@port).\n");
    printf("        -o outputFile:        Output file name.\n");
    printf("        -t ClassTagFile:      Resource ClassTag file name.\n");
    printf("        -e:                   Scan item not in the ClassTagFile(By Default).\n");
    printf("        -i:                   Scan item in the ClassTagFile.\n");
}

int main(int argc,char **argv)
{
    const char *qdb_addr = NULL;
    const char *output_file = NULL;
    const char *class_tag_file = NULL;
    int fd;
    int to_del = 0,scan_include = 0,scan_exclude = 0;
    FILE *fp;
    void *key,*value;
    size_t key_len,value_len;
    char optchar;
    QuickdbAdapter m_qdb;
    char line[1024];
    vector<string> tagVec;
        
    setenv("LC_ALL","zh_CN.GBK",1);
    tagVec.clear();
    
    while((optchar = getopt(argc,argv,"t:q:o:hdie")) != -1)
    {
        switch(optchar)
        {
        case 'o':
            output_file   = optarg;
            break;
        case 'q':
            qdb_addr    = optarg;
            break;
        case 't':
            class_tag_file = optarg;
            fprintf(stderr,"class tag file name is %s\n",class_tag_file);
            break;
        case 'h':
            USAGE(argv[0]);
            return 0;
        case 'd':
            to_del = 1;
            fprintf(stderr,"delete item directly\n");
            break;
        case 'i':
            scan_include = 1;
            break;
        case 'e':
            scan_exclude = 1;
            break;
        default:
            break;
        }
    }
    
    if(scan_include & scan_exclude)
    {
        fprintf(stderr,"-d and -e option cannot be set simultaneously\n");
        USAGE(argv[1]);
        return -4;
    }
    if (!scan_include )
        scan_exclude = 1;
    
    if (scan_exclude)
        fprintf(stderr,"scan in exclude mode.\n");
    if (scan_include)
        fprintf(stderr,"scan in include mode.\n");
    
    if((class_tag_file == NULL)||(qdb_addr == NULL))
    {
        USAGE(argv[1]);
        return 0;
    }
    
    if((fp = fopen(class_tag_file,"r") )== NULL)
    {
        fprintf(stderr,"classTag file %s open failed.\n",class_tag_file);
        return -3;
    }
    
    while( fgets(line,1024,fp) != NULL)
    {
        if(line[0] == '\0' || line[0] == '\r' || line[0] == '\n')
        {
            continue;
        }
        
        if(strncmp(line,"EXTERNAL",strlen("EXTERNAL")))
        {
            continue;
        }
        
        line[strlen(line) - 1] = '\0';
        string tag(line);
        tagVec.push_back(tag);
        fprintf(stderr,"class tag %s loaded.\n",tag.c_str());
    }
        
    if(output_file != NULL)
    {
        fd = open(output_file,O_CREAT|O_WRONLY|O_TRUNC);
    }
    else
    {
        fd = dup2(STDOUT_FILENO,23);
    }
    
    fp = fdopen(fd,"w");
    if(fp == NULL)
    {
        fprintf(stderr,"open output file failed.\n");
        return -1;
    }

    if(m_qdb.open(qdb_addr))
    {
        fprintf(stderr,"open qdb:%s failed.\n",qdb_addr);
        fclose(fp);
        return -2;
    }
    if(m_qdb.openc(qdb_addr,double_buffer_cursor))
    {
        fprintf(stderr,"open qdb cursor failed.\n");
        fclose(fp);
        return -2;
    }
    
    m_qdb.seek(0);
    
    while(1)
    {
        if( m_qdb.next(key,key_len,value,value_len,NULL) > 0)
            break;
        else
        {
            int found = 0;
            string item((const char*)value,value_len);
            string keyword((const char*)key,key_len);
            bool old =false;
            
            //fprintf(stderr,"key find, key is %s\n",keyword.c_str());
            //fprintf(fp,"%s\n",item.c_str());
            for(vector<string>::iterator it = tagVec.begin();it != tagVec.end();it++)
            {
                if( item.find(*it) != -1)
                    found = 1;
                if(scan_include == 1 && found == 1)
                {
                    string::size_type pos,poe;
                    /*pos = item.find("<fetch_time>");
                    poe = item.find("</fetch_time>");

                    string dateStr = item.substr(pos+strlen("<fetch_time>"),poe-pos-string("<fetch_time>").length());
                    pos = 0;
                    while(1)
                    {
                        static const string delim = "\n";
                        pos = dateStr.find_first_of(delim,pos);
                        if(pos == string::npos)
                            break;
                        dateStr = dateStr.erase(pos,1);
                    }
                    
                    struct tm tm;
                    if(strptime(dateStr.c_str(),"%a %b %d %H:%M:%S %Y",&tm) !=NULL)
                    {
                        time_t t = mktime(&tm);
                        
                        if(t <= 1298908800 && t>=1296489600)
                        {
                            fprintf(stderr,"time val :%d %s\n",t,dateStr.c_str());
                            old =true;
                        }
                    }
                    else
                    {
                        fprintf(stderr,"date cannot parse: %s\n",dateStr.c_str());
                        return -1;
                        }*/
                    
                    //fwrite(item.c_str(),sizeof(char),item.length(),fp);
                    if(fprintf(fp,"%s\n",item.c_str()) < 0 )
                    {
                        fprintf(stderr,"item %s write failed.\n",item.c_str());
                    
                    }
                
                    /*else
                      fprintf(stderr,"item write succeed.\n");*/
                    break;
                }
            }

            //scan old frame data            
            if(found == 0 && item.find("<param5>0") != string::npos)
                found = 0;
            else
                found = 1;

            if(found == 0 && scan_exclude == 1)
                if(fprintf(fp,"%s\n",item.c_str()) < 0 )
                {
                    fprintf(stderr,"item %s write failed.\n",item.c_str());
                    
                }

            if(key)
                free(key);
            if(value)
                free(value);
        }
    }
    
    fclose(fp);
    m_qdb.close();
    m_qdb.closec();
    tagVec.clear();
    
    return 0;
                
}
    
    

 
