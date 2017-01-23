#include <stdlib.h>
 #include <stdexcept>
 
//#include <ace/SString.h>
 //#include <ace/Configuration.h>
 //#include <ace/Configuration_Import_Export.h>
 
#include "Configuration.hpp"
 
bool ConfigurationSection::ReadValue(const char* name, std::string& value) const
{
    ACE_TString str;
    if (m_config->get_string_value(m_key, name, str) == 0)
    {
        value.assign(str.c_str(), str.length());
        return true;
    }
    return false;
}
 
bool ConfigurationSection::ReadValue(const char* name, bool& value) const
{
    u_int n;
    if (m_config->get_integer_value(m_key, name, n) == 0)
    {
        value = n;
        return true;
    }

    // try string format
    ACE_TString str;
    if (m_config->get_string_value(m_key, name, str) == 0)
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
            BadFormat(name, str.c_str());
        }
    }

    return false;
}

bool ConfigurationSection::ReadList(const char* name, char delimiter, std::vector<std::string>& result) const
{
    std::string s;
    return ReadValue(name, s) && ParseList(s, delimiter, result);
}

bool ConfigurationSection::ParseList(const std::string& str, char delimiter, std::vector<std::string>& result)
{
    size_t begin = 0;
    size_t end;
    std::string element;
    result.clear();
    while ((end = str.find(delimiter, begin)) != std::string::npos)
    {
        element.assign(str, begin, end-begin);
        result.push_back(element);
        begin = end + 1;
    }

    if (begin < str.length())
    {
        element.assign(str, begin, str.size()-begin);
        result.push_back(element);
    }

    return true;
}

void ConfigurationSection::MissingSection(const char* name) const
{
    std::string message = "Missing configuration section: ";
    message += Name();
    message += "/";
    message += name;
    throw ConfigurationValueError(message);
}

void ConfigurationSection::MissingValue(const char* name) const
{
    std::string message = "Missing configuration value: ";
    message += Name();
    message += ":";
    message += name;
    fprintf(stderr, "%s\n", message.c_str());
    exit(-1);
    //throw ConfigurationValueError(message);
}

void ConfigurationSection::BadFormat(const char* name, const char* value) const
{
    std::string message = "Bad configuration format: ";
    message += Name();
    message += ":";
    message += name;
    message += "=";
    message += value;
    throw ConfigurationValueError(message);
}

bool ConfigurationSection::GetSection(const char* name, ConfigurationSection& section) const
{
    section.m_config = m_config;
    section.m_name = m_name;
    section.m_name += "/";
    section.m_name += name;
    return section.m_config->expand_path(m_key, name, section.m_key, false) == 0;
}

ConfigurationFile::ConfigurationFile(const char* filename) : 
    m_impexp(m_config_heap)
{
    m_config = &m_config_heap;
    if (m_config_heap.open() != 0)
        throw std::runtime_error("Can't open config.");
    if (m_impexp.import_config(filename) != 0)
        throw std::runtime_error(std::string("Can't import config file: ") + filename);
    m_key = m_config_heap.root_section();
}


