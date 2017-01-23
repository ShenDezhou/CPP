#ifndef CONFIG_H_INCLUDED
 #define CONFIG_H_INCLUDED
 
#include <string>
 #include <vector>
 #include <stdexcept>
 
#include <ace/SString.h>
 #include <ace/Configuration.h>
 #include <ace/Configuration_Import_Export.h>
 
class ConfigurationError : public std::runtime_error
 {
 public:
         ConfigurationError(const std::string& message) : 
                std::runtime_error(message)
         {
         }
 };
 
class ConfigurationSectionError : public ConfigurationError
 {
 public:
         ConfigurationSectionError(const std::string& message) : 
                ConfigurationError(message)
         {
         }
 };
 
class ConfigurationValueError : public ConfigurationError
 {
 public:
         ConfigurationValueError(const std::string& message) : 
                ConfigurationError(message)
         {
         }
 };
 
class ConfigurationSection
 {
 public:
         ConfigurationSection(){}
         ~ConfigurationSection(){}
         bool ReadValue(const char* name, std::string& value) const;
         bool ReadValue(const char* name, bool& value) const;
         bool ReadValue(const char* name, short& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, unsigned short& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, int& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, unsigned int& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, long& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, unsigned long& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, long long& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadValue(const char* name, unsigned long long& value) const
         {
                 return ReadIntegralValue(name, value);
         }
         bool ReadList(const char* name, char delimiter, std::vector<std::string>& va) const;
         bool GetSection(const char* name, ConfigurationSection& section) const;
 
        template <typename Type>
         Type Value(const char* name) const
         {
                 Type value = Type();
                 if (!ReadValue(name, value))
                         MissingValue(name);
                 return value;
         }
         template <typename Type, typename DefaultValueType>
         Type Value(const char* name, const DefaultValueType& default_value) const
         {
                 Type value = default_value;
                 if (ReadValue(name, value))
                         return value;
                 else
                         return default_value;
         }
         template <typename Type>
         void GetValue(const char* name, Type& value) const
         {
                 if (!ReadValue(name, value))
                         MissingValue(name);
         }
         template <typename Type, typename DefaultValueType>
         void GetValue(const char* name, Type& value, const DefaultValueType& default_value) const
         {
                 if (!ReadValue(name, value))
                         value = default_value;
         }
 
        /// Return Section of @a name, 
        /// @throw ConfigurationValueError if failed
         ConfigurationSection Section(const char* name) const
         {
                 ConfigurationSection section;
                 if (!GetSection(name, section))
                         MissingSection(name);
                 return section;
         }
 
        const std::string& Name() const
         {
                 return m_name;
         }
 private:
         static void strtoint(const char* str, char** endpos, int base, short &value)
         {
                 long v = strtol(str, endpos, base);
                 if (v > SHRT_MAX || v < SHRT_MIN)
                         throw std::domain_error("");
                 value = v;
 
        }
         static void strtoint(const char* str, char** endpos, int base, unsigned short &value)
         {
                 value = strtoul(str, endpos, base);
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
         template <typename T>
         bool ReadIntegralValue(const char* name, T& value) const
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
                                 BadFormat(name, str.c_str());
                         return true;
                 }
                 return false;
         }
 
        static bool ParseList(const std::string& str, char delimiter, std::vector<std::string>& result);
         void MissingSection(const char* name) const;
         void MissingValue(const char* name) const;
         void BadFormat(const char* name, const char* value) const;
 protected:
         ACE_Configuration* m_config;
         ACE_Configuration_Section_Key m_key;
         std::string m_name;
 };
 
class ConfigurationFile : public ConfigurationSection
 {
 public:
         ConfigurationFile(const char* filename);
         ~ConfigurationFile(){}
 private:
         ACE_Configuration_Heap m_config_heap;
         ACE_Registry_ImpExp m_impexp;   
};
 
#endif
 
