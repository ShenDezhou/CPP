#include <Platform/log.h>
#include <map>
#include <string>
#include "CWriteTask.hpp"
#include <unordered_set>
#include <Platform/encoding.h>

std::map<std::string, int> fetch_map;
pthread_mutex_t fetch_map_mutex;
pthread_mutex_t  counter_mutexes[10];
int _mem_check;

SS_LOG_MODULE_DEF(vrspider);

size_t PuncNormalize_gchar(gchar_t *str, bool delSpace = false, bool delNumberSign = true);
int main()
{
	char buf1[1024], buf2[1024];
	EncodingConvertor::initializeInstance();
	while(fgets(buf1, 1024, stdin))
	{
		strtok(buf1, "\r\n");	// remove trailing "\n"
		if(!*buf1)
			continue;

		// gbk to gchar
		if(EncodingConvertor::getInstance()->dbc2gchar(buf1, (gchar_t *)buf2, 512, true) > 0)
		{
			strcpy(buf1, buf2);
			//PuncNormalize_gbk(buf1, false);
			PuncNormalize_gchar((gchar_t *)buf2, true);
		}

		if(strcmp(buf1, buf2))
			printf("%s\t%s\n", buf1, buf2);
	}
	return 0;
}
