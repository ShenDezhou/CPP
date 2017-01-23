#include <stdlib.h>
#include <stdexcept>

#include <ace/SString.h>
#include <ace/Configuration.h>
#include <ace/Configuration_Import_Export.h>

#include "config.hpp"

class config::pimpl
{
public:
	pimpl(const char* file, const char* key) : m_impexp(m_config)
	{
		if (m_config.open() != 0)
			throw std::runtime_error("Can't open config.");
		if (m_impexp.import_config(file) != 0)
			throw std::runtime_error(std::string("Can't import config file: ") + file);
		m_config.expand_path(m_config.root_section(), key, m_key);
	}

	bool read_value(const char* name, std::string& value)
	{
		ACE_TString str;
		if (m_config.get_string_value(m_key, name, str) == 0)
		{
			value.assign(str.c_str(), str.length());
			return true;
		}
		return false;
	}
	
	bool read_value(const char* name, bool& value)
	{
		u_int n;
		if (m_config.get_integer_value(m_key, name, n) == 0)
		{
			value = n;
			return true;
		}
		
		// try string format
		ACE_TString str;
		if (m_config.get_string_value(m_key, name, str) == 0)
		{
			if (str=="y" || str=="Y" || str=="yes" || str=="true")
			{
				value = true;
				return true;
			}
			else if (str=="n" || str=="N" || str=="no" || str=="false")
			{
				value = false;
				return true;
			}
			else
			{
				bad_format(name);
			}
		}

		return false;
	}
	
	template <typename T>
	bool read_value(const char* name, T& value)
	{
		u_int n;
		if (m_config.get_integer_value(m_key, name, n) == 0)
		{
			value = n;
			return true;
		}
			
		// try string format
		ACE_TString str;
		if (m_config.get_string_value(m_key, name, str) == 0)
		{
			char* endpos;
			if (str.length() > 2 && str[0]=='0' && (str[1]=='x' || str[1]=='X'))
			{
				strtoint(str.c_str()+2, &endpos, 16, value);
			}
			else
			{
				strtoint(str.c_str(), &endpos, 10, value);
			}
			if (*endpos)
				bad_format(name);
			return true;
		}
		return false;
	}
	
	static void strtoint(const char* str, char** endpos, int base, int &value)
	{
		value = strtol(str, endpos, base);
	}
	static void strtoint(const char* str, char** endpos, int base, unsigned int &value)
	{
		value = strtoul(str, endpos, base);
	}
	static void strtoint(const char* str, char** endpos, int base, long &value)
	{
		value = strtol(str, endpos, base);
	}
	static void strtoint(const char* str, char** endpos, int base, unsigned long &value)
	{
		value = strtoul(str, endpos, base);
	}
	static void strtoint(const char* str, char** endpos, int base, long long &value)
	{
		value = strtoll(str, endpos, base);
	}
	static void strtoint(const char* str, char** endpos, int base, unsigned long long &value)
	{
		value = strtoull(str, endpos, base);
	}
private:
	void bad_format(const char* name)
	{
		throw std::runtime_error(std::string("Bad configuration format: ") + name);
	}
private:
	ACE_Configuration_Heap m_config;
	ACE_Registry_ImpExp m_impexp;
	ACE_Configuration_Section_Key m_key;
};

config::config(const char* file, const char* key)
{
	m_pimpl = new pimpl(file, key);
}

config::~config()
{
	delete m_pimpl;
}

bool config::read_value(const char* name, std::string& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, bool& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, int& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, unsigned int& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, long& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, unsigned long& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, long long& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_value(const char* name, unsigned long long& value) const
{
	return m_pimpl->read_value(name, value);
}

bool config::read_address_list(const char* name, std::vector<std::string>& va) const
{
	std::string s;
	return m_pimpl->read_value(name, s) && parse_address_list(s, va);
}

bool config::parse_address_list(std::string& address_list, std::vector<std::string>& va)
{
	size_t begin = 0;
	size_t end;
	std::string address;
	va.clear();
	while ((end = address_list.find(';', begin)) != std::string::npos)
	{
		address.assign(address_list, begin, end-begin);
		va.push_back(address);
		begin = end + 1;
	}
	
	if (begin < address_list.length())
	{
		address.assign(address_list, begin, address_list.size()-begin);
		va.push_back(address);
	}
	
	return true;
}
