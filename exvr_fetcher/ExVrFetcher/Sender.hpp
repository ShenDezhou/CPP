#ifndef SENDER_HPP
#define SENDER_HPP

#include <string>
#include <vector>

class Sender
{
public:
	Sender(const std::string &des);
	//int send(const std::string page);
	int send_to(const char* url,const void *value,size_t value_len);
private:
	class connection;
	class pimpl;
	std::vector<pimpl*> m_pimpl;
	std::string m_des;
};
#endif
