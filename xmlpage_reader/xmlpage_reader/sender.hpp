#ifndef SENDER_HPP
#define SENDER_HPP

#include <string>
#include <string.h>
#include "Platform/encoding/URLInfo.h"
#include "config.hpp"
#include "xmlpage.hpp"

class Sender
{
public:
	Sender(const config& cfg,const std::string des);
	int send(const XmlPage *page);
	bool check_connect();
private:
	int send_to(const char* url, const char* objid, const void *value, size_t value_len);
private:
	class connection;
	class pimpl;
	std::vector<pimpl*> m_pimpl;
	std::string m_des;
};

#endif


