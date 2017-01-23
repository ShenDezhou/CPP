#ifndef CONFIG_H_INCLUDED
#define CONFIG_H_INCLUDED

#include <string>
#include <vector>

class config
{
public:
	config(const char* file, const char* key);
	~config();
	bool read_value(const char* name, bool& value) const;
	bool read_value(const char* name, int& value) const;
	bool read_value(const char* name, unsigned int& value) const;
	bool read_value(const char* name, long& value) const;
	bool read_value(const char* name, unsigned long& value) const;
	bool read_value(const char* name, long long& value) const;
	bool read_value(const char* name, unsigned long long& value) const;
	bool read_value(const char* name, std::string& value) const;
	bool read_address_list(const char* name, std::vector<std::string>& va) const;
private:
	static bool parse_address_list(std::string& address_list, std::vector<std::string>& va);
private:
	class pimpl;
	pimpl* m_pimpl;
};


#endif
