#include "stat_opt_word.h"

static int get_res_info(MYSQL *mysql, int &vr_id, int &res_id, int &group)
{
	MYSQL_RES *res_set; 
	MYSQL_ROW  row;
	char sql_str[1024];

	if(res_id > 0)
		sprintf(sql_str, "select id, vr_id, data_group from vr_resource where cast(vr_flag as unsigned) > 0 and id=%d", res_id);
	else
		sprintf(sql_str, "select id, vr_id, data_group from vr_resource where cast(vr_flag as unsigned) > 0 and vr_id=%d", vr_id);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-11);
	}
	res_set = mysql_store_result(mysql); 
	if (res_set == NULL)
	{
		printf ("[stat_opt_word] Error %u (%s):mysql_store_result() failed", mysql_errno(mysql), mysql_error(mysql));
	    return(-12);
	}
	while((row = mysql_fetch_row(res_set)) != NULL)
	{
		res_id = atoi(row[0]);
		vr_id  = atoi(row[1]);
		group = atoi(row[2]);
		break;
	}
	mysql_free_result(res_set);
	return(0);	
}

static int check_fetch_count(MYSQL *mysql, const int vr_id, const int res_id)
{
	MYSQL_RES *res_set; 
	MYSQL_ROW  row;
	char sql_str[1024];

	sprintf(sql_str, "select t1.id, t1.url, t1.data_source_id, t2.priority, t2.data_source from resource_status t1 inner join vr_open_datasource t2 on t1.data_source_id=t2.id where t1.res_id=%d  and t1.res_status>1", res_id);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-31);
	}
	res_set = mysql_store_result(mysql); 
	if (res_set == NULL)
	{
		printf ("[stat_opt_word] Error %u (%s):mysql_store_result() failed", mysql_errno(mysql), mysql_error(mysql));
	    return(-32);
	}
	int ret = 0;
	while((row = mysql_fetch_row(res_set)) != NULL)
	{
printf("[stat_opt_word] check url=[%s]\n", row[1]);
		char tmp_classid[20];
		snprintf(tmp_classid,20,"%d",vr_id);
		std::string unqi_key = tmp_classid;
		unqi_key += "#";
		unqi_key+= row[1];
		pthread_mutex_lock(&fetch_map_mutex);
		if( (fetch_map.find(unqi_key)!=fetch_map.end()) && fetch_map[unqi_key]>0 )
		{
			printf("[stat_opt_word] vr_id=%d res_id=%d url=[%s] not over\n", vr_id, res_id, row[1]);
			ret++;
		}
		pthread_mutex_unlock(&fetch_map_mutex);
		if(ret>0) break;
	}
	mysql_free_result(res_set);
	return(ret);	
}

static int get_opt_word_label(MYSQL *mysql, const int vr_id, std::set<std::string> *s_label)
{
	MYSQL_RES *res_set; 
	MYSQL_ROW  row;
	char sql_str[1024];

	s_label->clear();
	sprintf(sql_str, "select label_name from open_xml_format where vr_id=%d and is_select=1", vr_id);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-21);
	}
	res_set = mysql_store_result(mysql); 
	if (res_set == NULL)
	{
		printf ("[stat_opt_word] Error %u (%s):mysql_store_result() failed", mysql_errno(mysql), mysql_error(mysql));
	    return(-22);
	}
printf("[stat_opt_word] vr_id=%d allow label={", vr_id);
	while((row = mysql_fetch_row(res_set)) != NULL)
	{
		s_label->insert(row[0]);
printf("[%s]", row[0]);
	}
printf("}\n");
	mysql_free_result(res_set);
	return(0);	
}

static int get_opt_word_info(MYSQL *mysql, const int vr_id, std::map<std::string, int> *m_old)
{
	MYSQL_RES *res_set; 
	MYSQL_ROW  row;
	char sql_str[1024];

	m_old->clear();
	sprintf(sql_str, "select id, label_name, label_value from vr_opt_word where vr_id=%d", vr_id);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-31);
	}
	res_set = mysql_store_result(mysql); 
	if (res_set == NULL)
	{
		printf ("[stat_opt_word] Error %u (%s):mysql_store_result() failed", mysql_errno(mysql), mysql_error(mysql));
	    return(-32);
	}
	int id = 0;
	char key[1024];
	while((row = mysql_fetch_row(res_set)) != NULL)
	{
		id = atoi(row[0]);
		snprintf(key, 1024, "%s::%s", row[1], row[2]);
		(*m_old)[key] = id;
	}
	mysql_free_result(res_set);
	return(0);	
}

static int set_opt_word_info(MYSQL *mysql, const int vr_id, const char *label_name, const char *label_value, int count, int id)
{
	char sql_str[1024], temp_name[1024], temp_value[1024];

	if(id==0)
	{
		mysql_real_escape_string(mysql, temp_name, label_name, strlen(label_name));
		mysql_real_escape_string(mysql, temp_value, label_value, strlen(label_value));
		snprintf(sql_str, 1024, "insert into vr_opt_word(id, vr_id, label_name, label_value, times, update_date) values(0, %d, '%s',  '%s', %d, %ld)",
			vr_id, temp_name, temp_value, count, time(NULL));
	}
	else
	{
		snprintf(sql_str, 1024, "update vr_opt_word set times=%d, update_date=%ld where id=%d", count, time(NULL), id);
	}
	//printf("run sql =[%s]\n", sql_str);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-41);
	}
	return(0);	
}

static void make_field_value(std::map< std::string, int > & m_field, char *fv)
{
	if(strlen(fv) >= 1024) return;
		
	std::map< std::string, int >::iterator it;
	char *temp, *pos;
	int len;
	char buf[1024], key[2048], value[2048];
	strcpy(buf, fv);
	if((temp = strstr(buf, "::"))!= NULL)
	{
		*temp = 0;
		temp+=2;
	}
	else return;

	while(1)
	{
		pos = strstr(temp, ";");
		if(pos == NULL)
		{
			snprintf(key, 2048, "%s::%s", buf, temp);
			it = m_field.find(key);
			if(it == m_field.end())
			{
				m_field[key] = 1;
			}
			else it->second++;
			break;
		}
		else
		{
			len = pos - temp;
			memcpy(value, temp, len);
			value[len] = 0;
			snprintf(key, 2048, "%s::%s", buf, value);

			it = m_field.find(key);
			if(it == m_field.end())
			{
				m_field[key] = 1;
			}
			else it->second++;
			temp = pos + 1;
		}
	}
}

int stat_opt_word(int vr_id, int res_id, CMysql *mysql_vr)
{
	MYSQL_RES *res_set; 
	MYSQL_ROW  row;
	MYSQL *mysql = mysql_vr->getHandle();
	char sql_str[1024];
	int ret = 0;

	struct timeval tv;
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);

	int group=0;
	ret = get_res_info(mysql, vr_id, res_id, group);
	if(ret!=0) return(ret);
	printf("[stat_opt_word] get vr_id=%d res_id=%d group=%d\n", vr_id, res_id, group);
	if(vr_id < 70000000 || vr_id>=80000000) //for multi-hit vr
	{
		return(0);
	}

	std::set<std::string> s_label;	
	ret = get_opt_word_label(mysql, vr_id, &s_label);
	if(ret != 0) return(ret);
		
	if(s_label.size() == 0)
	{
		sprintf(sql_str, "update vr_opt_word set times=0, update_date=%ld where vr_id=%d", time(NULL), vr_id);
		if(mysql_query(mysql,sql_str))
		{
		    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
		    return(-1);
		}
		return(0);
	}
	
	ret = check_fetch_count(mysql, vr_id, res_id);
	if(ret!=0) return(100);
	
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] befor query time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);
	//////////////////////////////////////////////////////////
	sprintf(sql_str, "select hit_field from vr_data_%d where res_id=%d", group, res_id);
	if(mysql_query(mysql,sql_str))
	{
	    printf("[stat_opt_word] run [%s] failed (%u, %s)", sql_str, mysql_errno(mysql), mysql_error(mysql));
	    return(-2);
	}
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] query ok time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);
	res_set = mysql_store_result(mysql); 
	if (res_set == NULL)
	{
		printf ("[stat_opt_word] Error %u (%s):mysql_store_result() failed", mysql_errno(mysql), mysql_error(mysql));
	    return(-3);
	}
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] befor fetch time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);
	
	std::map< std::string, int > m_field;
	std::map< std::string, int >::iterator it;
	
	int i = 0, pos, len;
	char *temp, *str;
	char key[1024];
	while((row = mysql_fetch_row(res_set)) != NULL)
	{
		pos = 0;
		str = row[0];
		while(1)
		{
			temp = strstr(str, "||");
			if(temp == NULL)
			{
				make_field_value(m_field, str);
				break;
			}
			else
			{
				len = temp - str;
				if (len >= 1024)
                {
                    memcpy(key, str, 1024);
                    key[1024] = 0;
                }
                else
                {
                    memcpy(key, str, len);
                    key[len] = 0;
                }
				make_field_value(m_field, key);
				str = temp + 2;
			}
		}
		
		//printf("row[%d]={%s}\n", i, row[0]);
		i++;
	}
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] befor free time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);
	mysql_free_result(res_set);
	gettimeofday(&tv, NULL);	printf("[stat_opt_word] after free time=%ld-%ld\n", tv.tv_sec, tv.tv_usec);


	std::map<std::string, int> m_old;
	ret = get_opt_word_info(mysql, vr_id, &m_old);
	if(ret != 0) return(ret);
	
	std::map< std::string, int >::iterator it_opt;
	for(it = m_field.begin(); it!= m_field.end(); it++)
	{
		strcpy(key, it->first.c_str());
		if((temp = strstr(key, "::"))!= NULL)
		{
			*temp = 0;
			temp+=2;
		}
		else temp = key + it->first.size();
		//printf("key=[%s]\t[%s]\t[%s]\tvalue=%d\n", it->first.c_str(), key, temp, it->second);
		
		if(s_label.count(key)==0) continue;
			 
		it_opt = m_old.find(it->first);
		if(it_opt != m_old.end())
		{
			set_opt_word_info(mysql, vr_id, key, temp, it->second, it_opt->second);
			m_old.erase(it_opt);
		}
		else set_opt_word_info(mysql, vr_id, key, temp, it->second, 0);
	}

	for(it_opt = m_old.begin(); it_opt!= m_old.end(); it_opt++)
	{
		set_opt_word_info(mysql, vr_id, "", "", 0, it_opt->second);
	}
	return(0);	
}
