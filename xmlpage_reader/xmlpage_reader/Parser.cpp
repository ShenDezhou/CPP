#include "Parser.h"
#include "xmlpage.hpp"
#include <string.h>
//static iconv_t m_gbk2utf8 = iconv_open("utf-8", "gbk");
//static iconv_t m_utf82gbk = iconv_open("gbk", "utf-8");

static int XmlEncodingInput(unsigned char * out, int * outlen, const unsigned char * in, int * inlen)
{
 	iconv_t m_gbk2utf8 = iconv_open("utf-8//IGNORE", "gbk");

	char * outbuf = (char *)out;
	char * inbuf = (char *)in;
	size_t outlenbuf = *outlen;
	size_t inlenbuf = *inlen;

	size_t ret = iconv(m_gbk2utf8, &inbuf, &inlenbuf, &outbuf, &outlenbuf);
	iconv_close(m_gbk2utf8);
	
	if(ret == size_t(-1) && errno != E2BIG)
	{		
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;
		return -2;
	} else {
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;

		//assert(*outlen >= 0);	
//		std::cout << *outlen << std::endl;

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

	if (in == NULL) 
	{
		*outlen = 0; 
		*inlen = 0; 
		return 0;
	}    
	
	assert(*inlen >= 0 && *outlen>=0 );
	iconv_t m_utf82gbk = iconv_open("gbk", "utf-8");

	//if(*outlen <= 0 || *inlen <= 0)
	//	return -1;

	char * outbuf = (char *)out;
	char * inbuf = (char *)in;
	size_t outlenbuf = *outlen;
	size_t inlenbuf = *inlen;

	size_t ret = iconv(m_utf82gbk, &inbuf, &inlenbuf, &outbuf, &outlenbuf);

	iconv_close(m_utf82gbk);


//	if(ret == size_t(-1) && errno != E2BIG)
	if(ret == size_t(-1) && errno == EILSEQ)  
	{
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;
		return -2;
	} else {
		*outlen = (unsigned char *)outbuf - out;
		*inlen = (unsigned char *)inbuf - in;

		//assert(*outlen >= 0);	
//		std::cout << *outlen << std::endl;

		if(ret == size_t(-1) && errno == E2BIG)
			return -1;
		else
			return *outlen;
	}
}

static xmlCharEncodingHandlerPtr xml_encoding_handler = xmlNewCharEncodingHandler("gbk", XmlEncodingInput, XmlEncodingOutput);


static bool UTF82GBK(const char * utf8, char ** gbk)
{//½âÎö³¯ÏÊÓï»ácore
	iconv_t m_utf82gbk = iconv_open("gbk", "utf-8");
	size_t inbuflen = strlen(utf8) + 1;
	char * inbuf = (char *)utf8;

	size_t outbuflen = inbuflen;
	char * outbuf = (char *)malloc(outbuflen);

	size_t multi = 1;

	while (true) {
		size_t inbuflen_temp = inbuflen;
		char * inbuf_temp = inbuf;
		size_t outbuflen_temp = outbuflen;
		char * outbuf_temp = outbuf;

		size_t ret = iconv(m_utf82gbk, &inbuf_temp, &inbuflen_temp, &outbuf_temp, &outbuflen_temp);
		if (ret != size_t(-1))
			break;

		free(outbuf);
		outbuflen *= 2;
		outbuf = (char *)malloc(outbuflen);
	}

	*gbk = outbuf;

	//xmlGet*** will malloc memory,must free it outside.
	xmlChar* tmp = (xmlChar*)utf8;
	xmlFree(tmp);

	iconv_close(m_utf82gbk);
	return true;
}

static bool GBK2UTF8(const char * utf8, char ** gbk)
{
 	iconv_t m_gbk2utf8 = iconv_open("utf-8", "gbk");
	size_t inbuflen = strlen(utf8) + 1;
	char * inbuf = (char *)utf8;

	size_t outbuflen = inbuflen;
	char * outbuf = (char *)malloc(outbuflen);

	size_t multi = 1;

	while (true) {
		size_t inbuflen_temp = inbuflen;
		char * inbuf_temp = inbuf;
		size_t outbuflen_temp = outbuflen;
		char * outbuf_temp = outbuf;

		size_t ret = iconv(m_gbk2utf8, &inbuf_temp, &inbuflen_temp, &outbuf_temp, &outbuflen_temp);
		if (ret != size_t(-1))
			break;

		free(outbuf);
		outbuflen *= 2;
		outbuf = (char *)malloc(outbuflen);
	}

	*gbk = outbuf;

	//xmlGet*** will malloc memory,must free it outside.
	//			xmlChar* tmp = (xmlChar*)utf8;
	//			xmlFree(tmp);
	iconv_close(m_gbk2utf8);
	return true;
}
static xmlCharEncodingHandlerPtr ehp=xmlFindCharEncodingHandler("gbk"); 
static bool UTF82GBK_2(xmlChar *utf8, char **gbk)
{
	xmlBufferPtr xmlbufin=xmlBufferCreate(); 
	xmlBufferPtr xmlbufout=xmlBufferCreate();
	xmlBufferEmpty(xmlbufout); 
	xmlBufferCat(xmlbufin,utf8);
	xmlCharEncOutFunc(xml_encoding_handler,xmlbufout,xmlbufin);
	*gbk = (char *)malloc((size_t)xmlBufferLength(xmlbufout)+1);
	memcpy(*gbk,xmlBufferContent(xmlbufout),(size_t)xmlBufferLength(xmlbufout)+1);
//	printf("keyword: %s\n", xmlBufferContent(xmlbufout)); 
	xmlFree(utf8);
	xmlBufferFree(xmlbufout); 
	xmlBufferFree(xmlbufin);
}
static bool ContentParse(xmlNodePtr node,XmlDoc *&doc)
{
	std::stack<xmlNodePtr> stack;
	stack.push(node);
	doc->check_url=doc->check_id=doc->check_no=doc->check_source=doc->check_time=doc->check_display=doc->check_tplid=false;
        for (int j=0; j<MAX_PARAM; j++)
                doc->check_param[j]=false;
	xmlChar *attr;
	doc->status = XML_APPEND;
	while(stack.size() > 0) 
	{
		xmlNodePtr current = stack.top();
		char *gbk;

		if (xmlStrcmp(current->name, BAD_CAST "unique_url") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->unique_url = gbk;
			doc->check_url = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "objid") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->objid = gbk;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "classid") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->classid = gbk;
			doc->check_id = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "classtag") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->classtag = gbk;
			doc->check_tag = true;
			free(gbk);
		}/* else if (xmlStrcmp(current->name, BAD_CAST "classno") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->classno = atoi(gbk);
			doc->check_no = true;
			free(gbk);
		} */else if (xmlStrcmp(current->name, BAD_CAST "source") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->source = gbk;
			doc->check_source = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "fetch_time") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->fetch_time = gbk;
			doc->check_time = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "display") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->display = gbk;
			doc->check_display = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "tplid") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->tplid = atoi(gbk);
			doc->check_tplid = true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "param1") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->param[0]=atof(gbk);
			doc->check_param[0]=true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "param2") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->param[1]=atof(gbk);
			doc->check_param[1]=true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "param3") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->param[2]=atof(gbk);
			doc->check_param[2]=true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "param4") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->param[3]=atof(gbk);
			doc->check_param[3]=true;
			free(gbk);
		} else if (xmlStrcmp(current->name, BAD_CAST "param5") == 0) {
			UTF82GBK_2(xmlNodeGetContent(current), &gbk);
			doc->param[4]=atof(gbk);
			doc->check_param[4]=true;
			free(gbk);
		}else if((attr=xmlGetProp(current,BAD_CAST "index_level"))!=NULL){
			doc->check_index = true;
			doc->index += (char*)attr;
			xmlFree(attr);
		}else if (xmlStrcmp(current->name, BAD_CAST "status") == 0) {
                        UTF82GBK_2(xmlNodeGetContent(current), &gbk);
                        if (strcmp(gbk,"DELETE")==0)
                        {
                            doc->status = XML_DELETE;
                        }
                        else if (strcmp(gbk,"UPDATE") == 0)
                        {
                            doc->status = XML_UPDATE;
                        }
                        else
                        {
                            doc->status = XML_APPEND;
                        }
                        //doc->status = (strcmp(gbk,"DELETE")==0)?XML_DELETE:XML_APPEND;
                        free(gbk); 
                }else if (xmlStrcmp(current->name, BAD_CAST "update_type") == 0) {
                        UTF82GBK_2(xmlNodeGetContent(current), &gbk);
                        doc->update_type = gbk;
                        free(gbk); 
                }

		if (current->children != NULL) {
			stack.push(current->children);
		} else if (current->next != NULL) {
			stack.pop();
			stack.push(current->next);
		} else {
			stack.pop();
			while (stack.size() > 0) {
				current = stack.top();
				stack.pop();
				if (current->next != NULL) {
					stack.push(current->next);
					break;
				}
			}
		}
	}
	return true;
}

bool Parse(const char *in, int len, XmlDoc *&doc)
{
	bool ret = false;
	//xmlDocPtr xml_doc = xmlParseMemory(in.c_str(), in.length());
	//xmlDocPtr xml_doc = xmlReadMemory(in, len, NULL, "gb18030", 0);
	xmlDocPtr xml_doc = xmlReadMemory(in, len, NULL, NULL, 0);
	//xmlDocPtr xml_doc = xmlReadFile("../test.xml", "gbk", 0);
	if (xml_doc != NULL) 
	{
		xmlNodePtr xml_node = xmlDocGetRootElement(xml_doc);
		if (xml_node != NULL )//&& xmlStrcmp(xml_node->name, BAD_CAST "item") == 0) 
		{
			
			if(ContentParse(xml_node,doc))
				ret =  true;
/*			{

				xmlBufferPtr m_buffer = xmlBufferCreate();
				assert(m_buffer != NULL);
				xmlOutputBufferPtr temp_buffer = xmlOutputBufferCreateBuffer(m_buffer, xml_encoding_handler);
				if(temp_buffer)
				{
					if(xmlSaveFileTo(temp_buffer, xml_doc, "gbk") > 0)
					{
						std::string out;
						out.assign((const char *)xmlBufferContent(m_buffer),(size_t)xmlBufferLength(m_buffer));
						std::cout << out << std::endl;
						ret = true;
					}
				}
				xmlBufferFree(m_buffer);
			}
*/
		}
		xmlFreeDoc(xml_doc);
	}

	return ret;
}


