#include <string>
#include <string.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <errno.h>
#include <assert.h>
#include <map>
#include <vector>
using namespace std;

std::vector<std::string> m_xpath;
std::vector<int> m_len_min;
std::vector<int> m_len_max;
static int XmlEncodingInput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen) {
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
static int XmlEncodingOutput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen) {
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

int check_length(xmlDocPtr doc){
	/* Create xpath evaluation context */
	xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
	if(xpathCtx == NULL) {
		printf("Error: unable to create new XPath context\n");
		xmlXPathFreeContext(xpathCtx);
		return -2;
	}

    bool in_form = false;
    int form_len_max = 0;
    int form_len_min = 0;
    std::string form_xpath;
    
    std::map<int,std::vector<int> > formMap;
    formMap.clear();
    
	for( int i = 0; i < m_xpath.size(); i ++ ){
		/* Evaluate xpath expression */
		xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(m_xpath[i].c_str()), xpathCtx);
		if(xpathObj == NULL){
			printf("Error: unable to evaluate xpath expression %s\n", m_xpath[i].c_str());
			continue;
        }
        if(in_form == false && m_xpath[i].find("form") != -1)
        {
            std::string::size_type pos = m_xpath[i].find("col");
            if(pos != -1)
            {
                
                pos += 3;
                while(pos != std::string::npos)
                {
                    if(isdigit(m_xpath[i][pos]))
                    {
                        pos++;
                    }
                    
                    else
                        break;
                }
                
            }
            if(pos == m_xpath[i].length())
            {
                in_form = true;
                printf("form found in file %s %d.\n",m_xpath[i].c_str(),i);
                form_xpath.assign(m_xpath[i].c_str(),m_xpath[i].find("form") -1);
            }
                        
        }
        else if(in_form == true)
        {
            if(m_xpath[i].find("form") == -1)
                in_form = false;
            else
            {
                std::string::size_type pos = m_xpath[i].find("col");
                if(pos == -1)
                    in_form = false;
                else
                {
                    pos += 3;
                    while(pos != std::string::npos)
                    {
                        if(isdigit(m_xpath[i][pos]))
                            pos++;
                        else
                            break;
                    }
                    if(pos != m_xpath[i].length())
                        in_form = false;
                }
            }
        }
        if(in_form)
        {
            form_len_max += m_len_max[i];
            form_len_min += m_len_min[i];
        }

		/* get values of the selected nodes */
		xmlNodeSetPtr nodeset=xpathObj->nodesetval;
		if(xmlXPathNodeSetIsEmpty(nodeset)) {
			printf("No such nodes %s\n", m_xpath[i].c_str());
			xmlXPathFreeObject(xpathObj);
			continue;
		}

		//get the value    
		int size = (nodeset) ? nodeset->nodeNr : 0;
		for(int j = 0; j <size; j++) {
			//val=(char*)xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1);
			//cout<<"the results are: "<<val<<endl;
			char *gbk = NULL;
			int len = 0;
			if( UTF82GBK( xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1), &gbk ) ){
				len = strlen(gbk);
                //printf("xpath:\n%s\ncontent:\n%s\n",m_xpath[i].c_str(),gbk);
			}
			if( gbk )
                free(gbk);
                        
            if(in_form)
            {
                formMap[j].push_back(len);
                continue;
            }
            
			if( len < m_len_min[i] || len > m_len_max[i] ){
				printf("check length error: %d<=%d<=%d %s\n",
                       m_len_min[i], len, m_len_max[i], m_xpath[i].c_str());
				xmlXPathFreeObject(xpathObj);
				xmlXPathFreeContext(xpathCtx);
				return 1;
                }
			//xmlFree(val);
		}
        
		xmlXPathFreeObject(xpathObj);
	}
    //printf("form max:%d form min :%d\n",form_len_max,form_len_min);
    
    if(formMap.size() != 0)
    {
        printf("form size %d\n",(int)formMap.size());
        for(std::map<int,std::vector<int> >::iterator it = formMap.begin();it != formMap.end();it++)
        {
            int form_len =0;
            
            for(std::vector<int>::iterator vit = it->second.begin();vit != it->second.end();vit++)
            {
                printf("%d\t",(*vit));
                
                form_len += (*vit); 
                
            }
            printf("\n");

            if(form_len < form_len_min || form_len > form_len_max)
            {
                printf("check length error: %d<=%d<=%d %s\n",form_len_min,form_len,form_len_max,form_xpath.c_str());
            }
            it->second.clear();
        }

    }
    formMap.clear();
	//Cleanup of XPath data 
	xmlXPathFreeContext(xpathCtx);
	return 0;
}

int main(int argc,char *argv[])
{
    

    FILE* fp = NULL;
    fp = fopen(argv[1], "ro");
    if( NULL == fp ){
        printf("config length file error: %s\n", argv[1] );
        exit(-1);
    }
    char line[1024], tag[512];
    int a, b;
    while( fgets(line, 1024, fp) ){
        std::string line_str(line);
        int tab = 0;
        std::string scan_str(line_str);
        while(tab < 3){
            int tmp;
            if( -1 != (tmp=scan_str.find("\t")) ){
                tab ++;
                scan_str = scan_str.substr(tmp+1);
            }
            else
                break;
        }
        if( 3 > tab ){
            sscanf(line_str.c_str(), "%s\t%d\t%d\n", tag, &a, &b);
            m_xpath.push_back(tag);
            m_len_min.push_back(a);
            m_len_max.push_back(b);
        }else{	// add attributes' length check
            int pos = line_str.find("\t");
            std::string base = line_str.substr(0, pos);
            while(-1!=pos){
                line_str = line_str.substr(pos+1);
                pos = line_str.find("\t");
                m_xpath.push_back(base+"/@"+line_str.substr(0, pos));
                line_str = line_str.substr(pos+1);
                pos = line_str.find("\t");
                m_len_min.push_back(atoi(line_str.substr(0,pos).c_str()));
                line_str = line_str.substr(pos+1);
                pos = line_str.find_first_of("\t\r\n");
                m_len_max.push_back(atoi(line_str.substr(0,pos).c_str()));
                pos = line_str.find("\t");
            }
        }
        printf("config length %d %d %s\n", m_len_min[m_len_min.size()-1], m_len_max[m_len_max.size()-1], m_xpath[m_xpath.size()-1].c_str());
        if( 0 > m_len_min[m_len_min.size()-1] || 0 > m_len_max[m_len_max.size()-1] ||
            m_len_min[m_len_min.size()-1] > m_len_max[m_len_max.size()-1] ){
            printf("config length error: %d %d %s\n", m_len_min[m_len_min.size()-1], m_len_max[m_len_max.size()-1], m_xpath[m_xpath.size()-1].c_str());
        }
    }
    ifstream ifile(argv[2]);
    string linea,xml;
    
    if(ifile.is_open())
    {
        while( ifile.good())
        {
            getline(ifile,linea);
            xml += linea;
        }
        ifile.close();
    }
    
    std::string::size_type pos = xml.find("<item>");
    string::size_type next_pos = 0;
    int count = 0;
    int err_count = 0;
    
    while(pos != string::npos)
    {
        count++;

        std::string::size_type end_pos = xml.find("</item>", next_pos);
		if( std::string::npos == end_pos ){
			fprintf(stderr,"can't find </item>\n");
			break;
		}

        std::string item = xml.substr( pos, (end_pos+strlen("</item>")-pos) );
        if(item.find("<DOCUMENT>") == string::npos)
            item = "<DOCUMENT>" + item + "</DOCUMENT>";
        
        xmlDocPtr doc =  xmlReadMemory(item.c_str(), item.length(), NULL, "gbk", 0);  
        if (doc == NULL) {
            fprintf(stderr, "Error: unable to parse file \"%s\"\n", argv[2]);
            return(-1);
        }
        if(check_length(doc))
        {
            fprintf(stderr,"%s item check length failed.\n",item.c_str());
            err_count ++;
        }
        
        
        next_pos = end_pos + strlen("</item>");
		pos = xml.find("<item>", next_pos);
    }
    
    
        
    /*doc = xmlParseFile(argv[2]);
    if (doc == NULL) {
                fprintf(stderr, "Error: unable to parse file \"%s\"\n", argv[2]);
        return(-1);
    }
    check_length(doc);*/
    fprintf(stderr,"%d items checked, %d items failed\n",count,err_count);
    

    return 0;
}

