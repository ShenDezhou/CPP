#include <stack>
#include <iostream>
#include <stdlib.h>
#include <errno.h>
#include <iconv.h>
#include <libxml/parser.h>
#include <libxml/xmlreader.h>
#include <assert.h>
#include "string_Util.h"
#include "Platform/bchar.h"
#include "Platform/bchar_cxx.h"
#include "Platform/bchar_utils.h"
#include "Platform/bchar_stl.h"


#define MAX_PARAM 5
struct XmlDoc{
	int status;
	bool check_id, check_no,check_tag, check_param[MAX_PARAM], check_tplid, check_url, check_source, check_display, check_time, check_index;
	std::string classid;
	std::string classtag;
	int classno;
	float param[MAX_PARAM];
	int tplid;
	std::string index;
	std::string unique_url;
	std::string objid;
	std::string source;
	std::string display;
	std::string fetch_time;
	std::string update_type;
};
bool Parse(const char *in, int len, XmlDoc *&doc);
