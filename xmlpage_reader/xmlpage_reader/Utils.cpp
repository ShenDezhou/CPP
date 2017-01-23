#include <sstream>
#include "Utils.hpp"

bool Utils::objid2docid(const char *objid, gDocID_t &docID)
{
	std::stringstream ss1;
	std::stringstream ss2;
	std::string high, low;
	std::string xml;
	const char* key = objid;
	low.assign(key, 16);
	high.assign(key+16, 16);
	char temp[17];
	int len = low.length();
	int i;
	for( i = 0; i< len; i++)
	{
		temp[i] = low[len - i -1];
	}
	temp[i] = '\0';
	for(int i = 0 ; i < len; i += 2)
	{
		char tmp = temp[i];
		temp[i] = temp[i+1];
		temp[i+1] = tmp;
	}
	low.assign(temp);
	len = high.length();
	for( i = 0; i< len; i++)
	{
		temp[i] = high[len - i -1];
	}
	temp[i] = '\0';
	for(int i = 0 ; i < len; i += 2)
	{
		char tmp = temp[i];
		temp[i] = temp[i+1];
		temp[i+1] = tmp;
	}
	high.assign(temp);
	ss1 << std::hex << high;
	ss1 >>docID.id.value.value_high;
	ss2 << std::hex << low;
	ss2 >> docID.id.value.value_low;

	return true;
}
