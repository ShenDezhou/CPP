#include "redis_tool.h"
#include "time.h"
#include "stdlib.h"
int RedisTool::splitStr(const std::string &str, const char separator, std::vector<std::string> &res)
{
	size_t startPos = 0;
	size_t curPos = 0;
	while(curPos < str.size())
	{
		if(str[curPos] != separator)
		{
			curPos++;
		}
		else
		{
			res.push_back(str.substr(startPos, curPos - startPos));
			curPos++;
			startPos = curPos;
		}
	}
    res.push_back(str.substr(startPos,str.size() - startPos));
	return 0; 
}  
int RedisTool::Connect()
{
    connected = false;
    struct timeval timeout = {2, 0}; 
    pRedisContext = (redisContext*)redisConnectWithTimeout(address.c_str(), port, timeout);
    if ( (NULL == pRedisContext) || (pRedisContext->err)  )
    {
        redisFree(pRedisContext);
        pRedisContext = NULL;
        return -1;
    }

    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "AUTH %s", password.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	int ret = -1;
    if (pRedisReply->type == REDIS_REPLY_ERROR  && strcmp(pRedisReply->str, "ERR Client sent AUTH, but no password is set") == 0)
    {
        ret = 0;
        connected = true;
    }
    else if (pRedisReply->type != REDIS_REPLY_STATUS)
    {
        ret = -1;
    }
    else if (strcmp(pRedisReply->str, "OK") == 0)
    {
        ret = 0;
        connected = true;
    }
    else
    {
        ret = -1;
    }
    freeReplyObject(pRedisReply);
    return ret;
}

int RedisTool::Close()
{
    if (pRedisContext != NULL)
    {
        redisFree(pRedisContext);
        pRedisContext = NULL;
    }
    connected = false;
    return 0;
}

int RedisTool::IsInSet(std::string value)
{
	return IsInSet(set_name, value);
}

int RedisTool::IsInSet(std::string set_name, std::string value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SISMEMBER %s %s", set_name.c_str(), value.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(pRedisReply);
		return -1;
	}
	if (pRedisReply->integer > 0) // in 
	{
		freeReplyObject(pRedisReply);
		return 1;
	}
	else // not in
	{
		freeReplyObject(pRedisReply);
		return 0;
	}

}


int RedisTool::ListAdd(std::string list_name, std::string value)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "RPUSH %s %s", list_name.c_str(), value.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    else if (pRedisReply->integer > 0)
    {
        freeReplyObject(pRedisReply);
        return 1;
    }
    else
    {
        freeReplyObject(pRedisReply);
        return 0;
    }
}

int RedisTool::ListAdd(std::string value)
{
	return ListAdd(list_name, value);
}


int RedisTool::ListLen(std::string list_name)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "LLEN %s", list_name.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    int ret = pRedisReply->integer; 
    freeReplyObject(pRedisReply);
    return ret; 
}
int RedisTool::ListLen()
{
	return ListLen(list_name);
}

int RedisTool::SetLen(std::string set_name)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SCARD %s", set_name.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    int ret =  pRedisReply->integer; 
    freeReplyObject(pRedisReply);
    return ret;
}


int RedisTool::SetAdd(std::string set_name, std::string value)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SADD %s %s", set_name.c_str(), value.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    else if (pRedisReply->integer > 0)
    {
        freeReplyObject(pRedisReply);
        return 1;
    }
    else
    {
        freeReplyObject(pRedisReply);
        return 0;
    }
}

int RedisTool::SetAdd(std::string value)
{
	return SetAdd(set_name, value);
}


int RedisTool::Add(std::string list_name, std::string set_name, std::string value)
{
    if (!connected) Connect();
    //int list_len = ListLen();
    std::vector<std::string> vec;
    splitStr(value, '#', vec);
    int ret = -1;
    if (vec.size() < 3)
        return 3;
    int pos = value.find('#');
    std::string set_value = value.substr(0, pos);
    //if (vec[2] == "0")

    ret = IsInSet(set_name, set_value);
    if (ret > 0)
    {
        return 2;
    }

    if ((ret = ListAdd(list_name, value)) > 0)
    {
		SetAdd(set_name, set_value);
		return 1;
	}
    else if (ret == 0)
    {
    	return 0;
    }
    return -1;
}

int RedisTool::Add(std::string value)
{
	return Add(list_name, set_name, value);
}

int RedisTool::AddDel(std::string value)
{
	return Add(del_list_name, del_set_name, value);
}

int RedisTool::Get(std::string list_name, std::string set_name, std::string &value, bool is_delete)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "LPOP %s", list_name.c_str()); 
    //std::cout << "reply " << pRedisReply->type << std::endl;
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
    {
        freeReplyObject(pRedisReply);
        return 0;
    }
    else if (pRedisReply->type == REDIS_REPLY_STRING)
    {
        std::string str(pRedisReply->str);
        value = str;
        int pos = value.find('#');
        std::string set_value = value.substr(0, pos);
		if (is_delete)
		{
			DeleteSet(set_name, set_value);
		}
        freeReplyObject(pRedisReply);
        return 1;
    }   
    else
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
}

int RedisTool::Get(std::string &value)
{
	return Get(list_name, set_name, value, false);
}

int RedisTool::GetDel(std::string &value)
{
	return Get(del_list_name, del_set_name, value, true);
}


int RedisTool::DeleteSet(std::string value)
{
    return DeleteSet(set_name, value);
}

int RedisTool::DeleteSet(std::string set_name, std::string value)
{
	if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SREM %s %s", set_name.c_str(), value.c_str()); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    else if (pRedisReply->integer > 0)
    {
        freeReplyObject(pRedisReply);
        return 1;
    }
    else
    {
        freeReplyObject(pRedisReply);
        return 0;
    }   
}

int RedisTool::GetSetMembers(std::string set_name, std::vector<std::string> &vec)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SMEMBERS %s", set_name.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type != REDIS_REPLY_ARRAY)
    {
        ret = -1;
    }
    else 
    {
        for (int i = 0; i < pRedisReply->elements; i++)
        {
			std::string value =  pRedisReply->element[i]->str;
			vec.push_back(value);
		}
		ret = pRedisReply->elements;
    }
   
    freeReplyObject(pRedisReply);
    return ret;
}




int RedisTool::Set(std::string key, std::string value)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SET %s %s", key.c_str(), value.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	int ret = -1;
    if (pRedisReply->type != REDIS_REPLY_STATUS)
    {
        ret = -1;
    }
    else if (strcmp(pRedisReply->str, "OK") == 0)
    {
        ret = 1;
    }
    else
    {
        ret = -1;
    }
    freeReplyObject(pRedisReply);
    return ret;
}


int RedisTool::Get(std::string key, std::string &value)
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "GET %s", key.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	int ret = -1;
    if (pRedisReply->type == REDIS_REPLY_NIL)
    {
        ret = 0;
    }
    else if (pRedisReply->type == REDIS_REPLY_STRING)
    {
        value.assign(pRedisReply->str);
        ret = 1;
    }   
    else
    {
        ret = -1;
    }
    freeReplyObject(pRedisReply);
    return ret;
}


int RedisTool::GetRandomUniqueName(std::string &value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "SPOP %s", spider_uniquename_set_name.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_STRING)
	{
		value.assign(pRedisReply->str);
		ret = 1;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}

int RedisTool::AddUniqueName(std::string name)
{
	return SetAdd(spider_uniquename_set_name, name);
}


int RedisTool::UniqueSetLen()
{
	return SetLen(spider_uniquename_set_name);
}


int RedisTool::GetIncrUniqueName(std::string &value)
{
	if (!pRedisContext) return -1;
	// multi
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "MULTI"); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	//std::cout << pRedisReply->type << " " << pRedisReply->str << std::endl;
 	if (pRedisReply->type != REDIS_REPLY_STATUS || strcmp(pRedisReply->str, "OK") != 0)
	{
		freeReplyObject(pRedisReply);
		return -1;
	}
	freeReplyObject(pRedisReply);
	// incr
	pRedisReply = (redisReply*)redisCommand(pRedisContext, "INCR %s", incr_unique_num_name.c_str());
	if (pRedisReply == NULL)
        return -1;
	//std::cout << pRedisReply->type << " " << pRedisReply->str << std::endl;
	if (pRedisReply->type != REDIS_REPLY_STATUS || strcmp(pRedisReply->str, "QUEUED") != 0)
	{
		freeReplyObject(pRedisReply);
		return -1;
	}
	freeReplyObject(pRedisReply);
	// get
	pRedisReply = (redisReply*)redisCommand(pRedisContext, "GET %s", incr_unique_num_name.c_str());
	if (pRedisReply == NULL)
        return -1;
	//std::cout << pRedisReply->type << " " << pRedisReply->str << std::endl;
	if (pRedisReply->type != REDIS_REPLY_STATUS || strcmp(pRedisReply->str, "QUEUED") != 0)
	{
		freeReplyObject(pRedisReply);
		return -1;
	}
	freeReplyObject(pRedisReply);
	// exec 
	pRedisReply = (redisReply*)redisCommand(pRedisContext, "EXEC"); 
	if (pRedisReply == NULL)
        return -1;
	//std::cout << pRedisReply->type << " " << pRedisReply->str << std::endl;
	if (pRedisReply->type == REDIS_REPLY_ARRAY)
	{
		if (pRedisReply->elements > 1)
		{
			value = incr_unique_num_name + "_Set_" + pRedisReply->element[1]->str;
			ret = 1;
		}
	 	else
	 	{
			ret = -1;
		}
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;
}

int RedisTool::SetHash(std::string key, std::string value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HSET %s %s %s", spider_hash_name.c_str(), key.c_str(), value.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_INTEGER)
	{
		ret = pRedisReply->integer;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}
int RedisTool::GetHash(std::string key, std::string &value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HGET %s %s", spider_hash_name.c_str(), key.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_STRING)
	{
		value.assign(pRedisReply->str);
		ret = 1;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}
int RedisTool::DeleteHash(std::string key)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HDEL %s %s", spider_hash_name.c_str(), key.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_INTEGER)
	{
		ret = pRedisReply->integer;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}

int RedisTool::ExistsHash(std::string key)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HEXISTS %s %s", spider_hash_name.c_str(), key.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_STRING)
	{
		ret = pRedisReply->integer;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}

int RedisTool::HashLen()
{
    if (!pRedisContext) return -1;
    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HLEN %s", spider_hash_name.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    return pRedisReply->integer; 
}

int RedisTool::GetAllHashMembers(std::map<std::string, std::string> &members)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HGETALL %s", spider_hash_name.c_str()); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type != REDIS_REPLY_ARRAY)
    {
        ret = -1;
    }
    else 
    {
        for (int i = 0; i < pRedisReply->elements; i += 2)
        {
			std::string key =  pRedisReply->element[i]->str;
			std::string value =  pRedisReply->element[i + 1]->str;
			members[key] = value;
 		}
		ret = pRedisReply->elements/2;
    }
	freeReplyObject(pRedisReply);
	return ret;

}

int RedisTool::SetFormat(std::string key, std::string value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HSET %s %s %s", xml_format_hash.c_str(), key.c_str(), value.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_INTEGER)
	{
		ret = pRedisReply->integer;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}
int RedisTool::GetFormat(std::string key, std::string &value)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "HGET %s %s", xml_format_hash.c_str(), key.c_str() ); 
	if (pRedisReply == NULL)
        return -1;
	int ret = -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_STRING)
	{
		value.assign(pRedisReply->str);
		ret = 1;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}


int RedisTool::DeleteKey(std::string key)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "DEL %s", key.c_str() ); 
    if (pRedisReply == NULL)
        return -1;
	int ret = -1;
    if (pRedisReply == NULL)
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if (pRedisReply->type == REDIS_REPLY_INTEGER)
	{
		ret = pRedisReply->integer;
	}	
	else
	{
		ret = -1;
	}
	freeReplyObject(pRedisReply);
	return ret;

}

//int main()
//{
//    //RedisTool *redis = new RedisTool("b.redis.sogou", 1759, 1, "vrspiderCode");
//    RedisTool *redis = new RedisTool("127.0.0.1", 6379, 1, "vrspiderCode");
//    int ret = redis->Connect();
//    std::cout << ret << std::endl;
//    std::string value;
//    //sleep(10);
//    //ret = redis->GetIncrUniqueName(value);
//    ret = redis->Add("10254#21248201#1");
//    std::cout << ret << std::endl;
//    std::cout << value << std::endl;
//    return 0;
//}
