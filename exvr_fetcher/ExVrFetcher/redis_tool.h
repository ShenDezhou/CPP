#ifndef _REDIS_TOOL_H
#define _REDIS_TOOL_H

#include <hiredis/hiredis.h>
#include <string>
#include <iostream>
#include <sstream>
#include <string.h>
#include <vector>
#include <map>

enum actionType
{
	ACTION_FETCH,
	ACTION_DELETE,
	ACTION_NUM
};

class RedisTool{
public:
    RedisTool(std::string address, int  port, int spider_type, std::string password, int list_limit = 500):address(address), port(port), 
                connected(false), pRedisContext(NULL), spider_type(spider_type), list_limit(list_limit),password(password)
    {
        std::stringstream ss;
        ss << spider_type;
        type = ss.str();
        base_name = "";
        list_name = base_name + type + "_List";
        set_name = base_name + type + "_Set";
        del_list_name = base_name + type + "_Del" + "_List";
		del_set_name = base_name + type + "_Del" + "_Set";
        spider_uniquename_set_name = base_name + type + "_Uniquename_Set";
		spider_hash_name = base_name + type + "_Hash";
		xml_format_hash = "Xml_Format_Hash";
		incr_unique_num_name = base_name + type + "_IncrUniqueNum";
    }
	
	RedisTool(std::string address, int  port):address(address), port(port), 
                connected(false), pRedisContext(NULL)
    {
    }
    int Connect();
    int Close();
    int IsInSet(std::string value);
	int IsInSet(std::string set_name, std::string value);
    int Add(std::string value);
	int Add(std::string list_name, std::string set_name, std::string value);
    int Get(std::string &value);
	int Get(std::string list_name, std::string set_name, std::string &value, bool is_delete);
	// for del list and set
	int AddDel(std::string value);
	int GetDel(std::string &value);
	
    int DeleteSet(std::string value);
	int DeleteSet(std::string set_name, std::string value);
    int ListAdd(std::string value);
	int ListAdd(std::string list_name, std::string value);
    int SetAdd(std::string value);
	int SetAdd(std::string set_name, std::string value);
	int SetLen(std::string set_name);
    int ListLen();
	int ListLen(std::string list_name);
	int GetSetMembers(std::string set_name, std::vector<std::string> &vec);
    // key value function
    int Set(std::string key, std::string value);
    int Get(std::string key, std::string &value);

	// uniquename function
	int GetRandomUniqueName(std::string &value);
	int AddUniqueName(std::string name);
	int UniqueSetLen();
	int GetIncrUniqueName(std::string &value);
	// hash function
	int SetHash(std::string key, std::string value);
	int GetHash(std::string key, std::string &value);
	int DeleteHash(std::string key);
	int ExistsHash(std::string key);
	int HashLen();
	int GetAllHashMembers(std::map<std::string, std::string> &members);
	int SetFormat(std::string key, std::string value);
    int GetFormat(std::string key, std::string &value);
	// golbal function
	int DeleteKey(std::string key);
    bool GetConnected()
    {
        return connected;
    }
    int SetConnected(bool connected)
    {
        connected = connected;
        return 0;
    }
	
	int SetListSetName(int action)
	{
		std::string saction = "";
		if(ACTION_DELETE == action)
		{
			saction = "_Del";
		}

		list_name = base_name + type + saction + "_List";
        set_name = base_name + type + saction + "_Set";
        return 0;
	}
	
	std::string GetListSetName()
	{
		return list_name + ":" + set_name;
	}

    int splitStr(const std::string &str, const char separator, std::vector<std::string> &res);

private:
    std::string address;
    int port;
    bool connected;
    redisContext *pRedisContext;
    std::string list_name;
    std::string set_name;
	std::string base_name;
	std::string type;
    int spider_type;
    int list_limit;
	std::string spider_uniquename_set_name;
	std::string spider_hash_name;
	std::string xml_format_hash;
	std::string del_list_name;
	std::string del_set_name;
	std::string incr_unique_num_name;
    std::string password;
};

#endif
