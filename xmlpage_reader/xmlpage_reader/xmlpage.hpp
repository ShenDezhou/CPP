#ifndef XMLPAGE_HPP
#define XMLPAGE_HPP
#include <stdlib.h>

#define MIRROR_NUM 1
enum{
	XML_APPEND,
	XML_DELETE,
    XML_UPDATE
};
enum{
	VR_KEY_UPDATE,
	VR_CONTENT_UPDATE,
	VR_OTHERS
}; 
struct XmlPage{
	char * url;
	char * objid;
	char * doc;
	unsigned int doc_len;
	int status;
	int update_type;
};
void free_xml(XmlPage *&page);

#endif
