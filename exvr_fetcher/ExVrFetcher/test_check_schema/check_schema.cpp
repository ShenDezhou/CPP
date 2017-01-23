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
#include <string>
#include <iconv.h>
#include <errno.h>
#include <assert.h>
#include <string>
using namespace std;

std::string m_length_prefix = "lengthFile";
std::string m_length_suffix = ".txt";
std::string m_schema_prefix = "schemaFile";
std::string m_schema_suffix = ".xsd";
std::map<int,std::vector<std::string> > m_xpath_map;
std::map<int,std::vector<int> > m_len_min_map;
std::map<int,std::vector<int> > m_len_max_map;
std::map<int,time_t> m_len_time_map;

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

int reload_length_file(int type)
{
    struct stat sbuf;
    char length_file_name[256];

    sprintf(length_file_name, "%s%02d%s", m_length_prefix.c_str(), type, m_length_suffix.c_str());
    if(access(length_file_name,R_OK))
    {
        printf("ERROR: open length file %s in read mode error.\n",length_file_name);
        return -1;
    }

    stat(length_file_name,&sbuf);

    //if modify time changed, reload length file
    if(m_len_time_map.find(type) == m_len_time_map.end() || m_len_time_map[type] != sbuf.st_ctime )
    {
        m_len_min_map.erase(type);
        m_len_max_map.erase(type);
        m_xpath_map.erase(type);

        FILE* fp = NULL;
        fp = fopen(length_file_name, "ro");
        if( NULL == fp )
        {
            printf("ERROR: config length file error: %s\n", length_file_name);
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
                m_xpath_map[type].push_back(tag);
                m_len_min_map[type].push_back(a);
                m_len_max_map[type].push_back(b);
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
                    m_xpath_map[type].push_back(base+"/@"+line_str.substr(0, pos));
                    line_str = line_str.substr(pos+1);
                    pos = line_str.find("\t");
                    m_len_min_map[type].push_back(atoi(line_str.substr(0,pos).c_str()));
                    line_str = line_str.substr(pos+1);
                    pos = line_str.find_first_of("\t\n");
                    m_len_max_map[type].push_back(atoi(line_str.substr(0,pos).c_str()));
                    pos = line_str.find("\t");
                }
            }
            printf("DEBUG: config length %d %d %s\n", m_len_min_map[type][m_len_min_map[type].size()-1], 
                        m_len_max_map[type][m_len_max_map[type].size()-1], m_xpath_map[type][m_xpath_map[type].size()-1].c_str());

            if( 0 > m_len_min_map[type][m_len_min_map[type].size()-1] && 
                    0 > m_len_max_map[type][m_len_max_map[type].size()-1] ||
                    m_len_min_map[type][m_len_min_map[type].size()-1] > m_len_max_map[type][m_len_max_map[type].size()-1] )
            {
                printf("ERROR: config length error: %d %d %s\n", 
                            m_len_min_map[type][m_len_min_map[type].size()-1], m_len_max_map[type][m_len_max_map[type].size()-1], 
                            m_xpath_map[type][m_xpath_map[type].size()-1].c_str());
                fclose(fp);
                return -1;
            }
        }
        m_len_time_map[type]    = sbuf.st_ctime;
        printf("NOTICE: load length file %d success.\n",type);
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
            //可能存在的col2
            xmlNodePtr sub_xml_node = xml_node->xmlChildrenNode;
            int tlen2 = form_col_content_check_max(sub_xml_node);
            total_len += std::max(tlen1, tlen2);
        }
        xml_node = xml_node->next;
    }
    return total_len;
}

int form_col_content_check(const char *one_xpath, xmlXPathContextPtr xpathCtx, int low, int high)
{
    char buf[1024];
    int ret;
    char *pos = strstr(one_xpath, "/col/@content");
    if (pos == NULL) {
        return -1;
    }
    strncpy(buf, one_xpath, pos - one_xpath);
    buf[pos - one_xpath] = '\0';

    xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)buf, xpathCtx);
    xmlNodeSetPtr nodeset = xpathObj->nodesetval;
    if(xmlXPathNodeSetIsEmpty(nodeset)) {
        printf("WARNING: No such nodes %s\n", buf);
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
            printf("ERROR: check form/col length error: %d<=%d<=%d %s\n", low, total_len, high, one_xpath);
            return 1;
        }
    }
    xmlXPathFreeObject(xpathObj);

    return 0;
}

int check_length(xmlDocPtr doc, int type)
{
    bool in_form = false;
    int form_len_max = 0;
    int form_len_min = 0;
    std::string form_xpath;
    std::map<int,std::vector<int> > formMap;

    if (0 != reload_length_file(type)) {
        printf("Error: reload length file[type:%d] failed.\n", type);
        return -1;
    }

    /* Create xpath evaluation context */
    xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
    if(xpathCtx == NULL) 
    {
        printf("Error: unable to create new XPath context\n");
        xmlXPathFreeContext(xpathCtx);
        return -2;
    }

    int size = m_xpath_map[type].size();
    for( int i = 0; i < size; i ++ )
    {
        const std::string &one_xpath = m_xpath_map[type][i];
        if (one_xpath.find("form/col/@content") != -1) {
            if (1 == form_col_content_check(one_xpath.c_str(), xpathCtx, m_len_min_map[type][i], m_len_max_map[type][i])) {
                return 1;
            }
            continue;
        }
        /* Evaluate xpath expression */
        xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression((const xmlChar*)(one_xpath.c_str()), xpathCtx);
        if(xpathObj == NULL)
        {
            printf("Error: unable to evaluate xpath expression %s\n", one_xpath.c_str());
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
                printf("DEBUG: form exist in file %s %d.\n", one_xpath.c_str(),i);
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
            printf("WARNING: No such nodes %s\n", one_xpath.c_str());
            xmlXPathFreeObject(xpathObj);
            continue;
        }

        //get the value    
        int size = (nodeset) ? nodeset->nodeNr : 0;
        for(int j = 0; j <size; j++) 
        {
            char *gbk = NULL;
            int len = 0;
            if( UTF82GBK( xmlNodeListGetString(doc, nodeset->nodeTab[j]->xmlChildrenNode, 1), &gbk ) )
            {
                len = strlen(gbk);
            }
            if( gbk )
                free(gbk);

            if(in_form)
            {
                formMap[j].push_back(len);
                continue;
            }
            if( len < m_len_min_map[type][i] || len > m_len_max_map[type][i] )
            {
                printf("ERROR: check length error: %d<=%d<=%d %s\n",
                            m_len_min_map[type][i], len, m_len_max_map[type][i], one_xpath.c_str());
                xmlXPathFreeObject(xpathObj);
                xmlXPathFreeContext(xpathCtx);
                return 1;
            }
            //xmlFree(val);
        }
        xmlXPathFreeObject(xpathObj);
    }
    //DEBUG
    //printf("form_len_min: %d, form_len_max: %d\n", form_len_min, form_len_max);

    //for(std::map<int,std::vector<int> >::iterator it = formMap.begin();it != formMap.end();it++) {
    //    for(std::vector<int>::iterator vit = it->second.begin();vit != it->second.end();vit++) {
    //        printf("%d ", *vit);
    //    }
    //    puts("");
    //}
    //DEBUG
    if(formMap.size()) {
        printf("DEBUG: form size %d\n",(int) formMap.size());
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
            printf("ERROR: check length error in a form: %d<=%d<=%d %s\n", form_len_min,form_len,form_len_max,form_xpath.c_str());
            xmlXPathFreeContext(xpathCtx);
            return 1;
        }
    }

    //Cleanup of XPath data 
    xmlXPathFreeContext(xpathCtx);

    printf("NOTICE: check_length success.\n");
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

int check_schema(xmlDocPtr doc, int type)
{
    int ret;
    char schema_name[256];
    sprintf(schema_name, "%s%02d%s", m_schema_prefix.c_str(), type, m_schema_suffix.c_str());

    xmlSchemaValidCtxt* validityContext;
    xmlSchemaParserCtxt* schemaParser;
    xmlSchema* schema;
    if( ( schemaParser = xmlSchemaNewParserCtxt(schema_name)) != NULL )
    {
        if ( (schema = xmlSchemaParse(schemaParser)) != NULL )
        {
            if( (validityContext = xmlSchemaNewValidCtxt(schema)) != NULL )
            {
                xmlSchemaSetValidErrors(validityContext, schemaErrorCallback, schemaWarningCallback, 0);
                printf("NOTICE: load schema file %s success.\n", schema_name);
            }
            else
            {
                printf("ERROR: create validity Context from schema fail: %s\n", schema_name);
                return -1;
            }
        }
        else
        {
            printf("ERROR: create xmlschema parser fail: %s \n", schema_name);
            return -1;
        }
    }
    else
    {
        printf("ERROR: create schema fail: %s\n", schema_name);
        return -1;
    }

    ret = xmlSchemaValidateDoc(validityContext, doc);
    if (0 != ret) {
        printf("ERROR: check xml schema failed.\n");
        return -1;
    }
    printf("NOTICE: check xml schema success.\n");

    return 0;
}


int main(int argc, const char* argv[])
{
    if (argc != 3) {
        printf("Usage: %s xml_to_validate template_id\n", argv[0]);
        printf("Example: %s data331.xml 331\n", argv[0]);
        exit(-1);
    }
    const char *xml_to_validate = argv[1];
    int type = atoi(argv[2]);

    int fd = ::open(xml_to_validate, O_RDONLY);
    if (fd < 3) {
        printf("ERROR: %s not found\n", xml_to_validate);
        exit(-1);
    }

    struct stat st;
    fstat(fd, &st);
    char *buf = new char[st.st_size];
    if (::read(fd, buf, st.st_size) < 0) {
        printf("ERROR: read %s failed(errno: %d)\n", xml_to_validate, errno);
        exit(-1);
    }

    char *pos = buf;
    char *pos_end = buf + st.st_size;

    char *item_beg = NULL;
    char *item_end = NULL;
    xmlDocPtr doc = NULL;
    string one_record;
    while (pos < pos_end) {
        item_beg = strstr(pos, "<item>");
        if (!item_beg) {
            break;
        }
        item_end = strstr(item_beg, "</item>");
        if (!item_end) {
            break;
        }
        pos = item_end + strlen("</item>");
        one_record = string("<?xml version=\"1.0\" encoding=\"gbk\"?><DOCUMENT>")
            + string(item_beg, item_end - item_beg + strlen("</item>"))
            + string("</DOCUMENT>");

        //debug
        //printf("xml: %s\n", one_record.c_str());

        doc = xmlReadMemory(one_record.c_str(), one_record.length(), NULL, "gbk", 0);
        if (0 != check_schema(doc, type)) {
            printf("ERROR: check_schema failed. xml content: %s\n", one_record.c_str());
            goto theend;
        }
        if (0 != check_length(doc, type)) {
            printf("ERROR: check_length failed. xml content: %s\n", one_record.c_str());
            goto theend;
        }

theend:
        if (doc) {
            xmlFreeDoc(doc);
            doc = NULL;
        }
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

    return 0;
}
