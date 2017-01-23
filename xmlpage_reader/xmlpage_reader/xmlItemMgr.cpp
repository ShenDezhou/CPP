#include "xmlItemMgr.h"
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>

#include "send_file_task.h"

XmlItemMgr g_xmlItemMgr;
const char* g_types[] = {"SUM", "INDEX"};
time_t last_item_time = time(NULL);

//std::bitset<((long long)1) << 32> send_recv_flag;
std::set<int> send_or_recv_set;
std::map<int, FILE*> recv_file_map;

int XmlItemMgr::clear()
{
	pthread_rwlock_wrlock(&m_rwlock);
	for(int i=0; i<v_items.size(); i++)
	{
		if(v_items[i] != NULL) delete v_items[i];
		v_items[i] = NULL;
	}
	v_items.clear();
	pthread_rwlock_unlock(&m_rwlock);
	return(0);
}

int XmlItemMgr::_init_file_id()
{
	min_file_id = 0;
	now_file_id = 1;
	now_item_idx = 0;

	DIR *dir = NULL;
	struct dirent *entry;
	struct stat statbuf;
	
	if((dir = opendir(base_dir.c_str())) == NULL)
	{
		reader_log_error("XmlItemMgr::init() opendir [%s] fail\n", base_dir.c_str());
		return(-1);
	}
	while (entry = readdir(dir))
	{
//reader_log_error("XmlItemMgr::init() check file [%s]\n", entry->d_name);
		if(strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0) continue;
		if(strncmp(entry->d_name, "xml_item_%8.8d.dat", 9)!=0) continue;
		if(strncmp(entry->d_name + 17, ".dat", 4)!=0) continue;
		
		char str[32];
		strncpy(str, entry->d_name + 9, 8);
		str[8]=0;
		int temp_id = atoi(str);
//reader_log_error("XmlItemMgr::init() parse file [%s] str=[%s] id=%d\n", entry->d_name, str, temp_id);
		if(temp_id > 0)
		{
			if(min_file_id == 0 || temp_id<min_file_id)  min_file_id = temp_id;
			if(temp_id >= now_file_id) now_file_id = temp_id + 1;
		}
	}
	if(min_file_id == 0) min_file_id = 1;
	closedir(dir);

    if (is_recv_file)
    {
        // recv reader read now_file_id
        std::string now_file_name = base_dir + "/" + "Recv_now_file_id.dat";
        FILE *fp_now = fopen(now_file_name.c_str(), "r");
        if(fp_now == NULL)
        {
            reader_log_error("XmlItemMgr::init open file %s fail\n", now_file_name.c_str());
        }
        else
        {
            char str[32];
            int n = fread(str, 1, 10, fp_now);
            if (n != 10)
            {
                reader_log_error("XmlItemMgr::init read file %s size=%d error\n", now_file_name.c_str(), n);
            }
            else
            {
                now_file_id = atoi(str);
                if (min_file_id > now_file_id)
                {
                    min_file_id = now_file_id;
                }
                reader_log_error("XmlItemMgr::read recv_file_id now_file_id=%d, min_file_id=%d\n", now_file_id, min_file_id);
            }
            fclose(fp_now);
        }   
    }

    if (is_send_file)
    {
        // send reader read send file_id
        std::string send_file_name = base_dir + "/" + "Send_file_id.dat";
        FILE *fp_send = fopen(send_file_name.c_str(), "r");
        if(fp_send == NULL)
        {
            reader_log_error("XmlItemMgr::init open file %s fail\n", send_file_name.c_str());
            send_file_id = min_file_id - 1;
        }
        else
        {
            char str[32];
            int n = fread(str, 1, 10, fp_send);
            if (n != 10)
            {
                reader_log_error("XmlItemMgr::init read file %s size=%d error\n", send_file_name.c_str(), n);
                send_file_id = min_file_id - 1;
            }
            else
            {
                send_file_id = atoi(str);
            }
            fclose(fp_send);
        }
        init_file_id = send_file_id + 1;
        // put not_sended file into send_file task
        for (int id = send_file_id + 1; id < now_file_id; id++)
        {
            reader_log_error("XmlItemMgr::put_to_send_file_task file_id =%d\n", id);
            SendFileRequest *req = new SendFileRequest();
            req->file_id = id;
            ACE_Message_Block *msg = new ACE_Message_Block(reinterpret_cast<char *>(req));
            SendFileTask::instance()->put(msg);
        }
    }
    return 0;
}

int XmlItemMgr::init()
{
	reader_log_error("xmlItemMgr.init() begin dir=%s linenum=%d\n", base_dir.c_str(), MAX_ITEM_COUNT);
	pthread_rwlock_wrlock(&m_rwlock);
	for(int i=0; i<MAX_ITEM_COUNT; i++)
	{
		ItemData *item = new ItemData();
		if(item == NULL)
		{
			pthread_rwlock_unlock(&m_rwlock);
			return(-1);
		}
		v_items.push_back(item);
	}
	_init_file_id();
	reader_log_error("XmlItemMgr::init() set file now=%d min=%d\n", now_file_id, min_file_id);
	pthread_rwlock_unlock(&m_rwlock);
	return(0);
}

int XmlItemMgr::set_min_file(int min_id, int min_index)
{
	if(min_id <= min_file_id || min_id>now_file_id || min_file_id>=now_file_id -1)
	{
		return(0);
	}
	reader_log_error("XmlItemMgr::set_min_file begin min=%d input=%d,%d\n", min_file_id, min_id, min_index);
	pthread_rwlock_wrlock(&m_rwlock);
    if (!is_send_file)
    {
        send_file_id = now_file_id - 1;   
    }
	for(int i=min_file_id; i<min_id && i<now_file_id -1 && i < send_file_id; i++)
	{
		char file_name[1024];
		snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), i);
		unlink(file_name);
		snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", base_dir.c_str(), i);
		unlink(file_name);
	    min_file_id++;
		reader_log_error("XmlItemMgr::set_min_file unlink %s, min=%d\n", file_name, min_file_id);
	}
	reader_log_error("XmlItemMgr::set_min_file end min=%d\n", min_file_id);
	pthread_rwlock_unlock(&m_rwlock);
	return(0);
}

//返回值1 正常读到数据， 发送
int XmlItemMgr::read(ItemData *data, unsigned int file_id, unsigned int item_index)
{
	int ret = 0;
	pthread_rwlock_rdlock(&m_rwlock);
	ItemData *item = v_items[(item_index-1)%MAX_ITEM_COUNT];
	if(item->header.file_id == file_id && item->header.item_idx == item_index)
	{
		data->header = item->header;
		data->doc_id = item->doc_id;
		data->url = item->url;
		data->content.assign(item->content.c_str(), item->content.size());
		ret = 1;
	}
	pthread_rwlock_unlock(&m_rwlock);
	return(ret);
}

void  XmlItemMgr::save_file(char *file_name)
{
	pthread_rwlock_wrlock(&m_rwlock);
    if(fp != NULL) fclose(fp);
    fp = NULL;
    if(fp_index != NULL) fclose(fp_index);
    fp_index = NULL;
    now_file_id++;
    now_item_idx = 0;
	snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), now_file_id);
    recv_file_id = now_file_id;
    now_file_id++;
	pthread_rwlock_unlock(&m_rwlock);
}

void XmlItemMgr::success_send_file(unsigned int file_id)
{
    pthread_rwlock_wrlock(&m_rwlock);
    //send_recv_flag[file_id] = 1;
    send_or_recv_set.insert(file_id);
    for (unsigned int i = send_file_id + 1; ; i++)
    {
        //if (send_recv_flag[i] == 1)
        if (send_or_recv_set.find(i) != send_or_recv_set.end())
        {
		    reader_log_error("XmlItemMgr::success_send_file add send_file_id=%d\n", send_file_id);
            send_file_id++;
            //send_recv_flag[i] = 0;
            send_or_recv_set.erase(i);
        }
        else
        {
		    reader_log_error("XmlItemMgr::success_send_file save send_file_id=%d\n", send_file_id);
            save_send_file_id(send_file_id);
            break;
        }
    }
	pthread_rwlock_unlock(&m_rwlock);
}

int XmlItemMgr::start_recv_file(SEND_FILE_TYPE type, unsigned int file_id)
{
	char file_name[1024];
    if (type == SEND_ITEM)
	    snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), file_id);
	else if (type == SEND_INDEX)
	    snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", base_dir.c_str(), file_id);
    FILE *fp_recv = fopen(file_name, "w+");
	if(fp_recv == NULL)
	{
		reader_log_error("XmlItemMgr::recv_file open file [%s] fail.\n", file_name);
        return -1;
	}
    if (recv_file_map.find(file_id) != recv_file_map.end() && recv_file_map[file_id] != NULL)
    {
        fclose(recv_file_map[file_id]);
        recv_file_map.erase(file_id);
    }
    recv_file_map[file_id] = fp_recv;
    return 0;
}

int XmlItemMgr::recv_data(void *buf, int size, unsigned int file_id)
{
    FILE *fp_recv = recv_file_map[file_id];
    if (fp_recv == NULL)
    {
		reader_log_error("XmlItemMgr::recv_file  fp_recv is NULL.\n");
        return -1;
    }
    int write_len = fwrite(buf, sizeof(char), size, fp_recv);
    if (write_len != size)
    {
		reader_log_error("XmlItemMgr::recv_file  not equal.\n");
        return -1;
    }
    return 0;
}

int XmlItemMgr::end_recv_file(SEND_FILE_TYPE type, unsigned int file_id)
{
    FILE *fp_recv = recv_file_map[file_id];
    fclose(fp_recv); 
    recv_file_map.erase(file_id);
	char file_name[1024];
    if (type == SEND_ITEM)
    {
	    snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), file_id);
        reader_log_error("XmlItemMgr::recv_item_file_end %s.\n", file_name);
    }
	else if (type == SEND_INDEX)
    {
	    snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", base_dir.c_str(), file_id);
        reader_log_error("XmlItemMgr::recv_index_file_end %s.\n", file_name);
    }

    if (type == SEND_ITEM)
    {
        pthread_rwlock_wrlock(&m_rwlock);
        //send_recv_flag[file_id] = 1;
        send_or_recv_set.insert(file_id);
        for (unsigned int i = now_file_id; ; i++)
        {
            //if (send_recv_flag[i] == 1)
            if (send_or_recv_set.find(i) != send_or_recv_set.end())
            {
		        reader_log_error("XmlItemMgr::end_recv_file add now_file_id=%d\n", now_file_id);
                now_file_id++;
                //send_recv_flag[i] = 0;
                send_or_recv_set.erase(i);
            }
            else
            {
		        reader_log_error("XmlItemMgr::end_recv_file save now_file_id=%d\n", now_file_id);
                save_now_file_id(now_file_id);
                break;
            }
        } 
	    pthread_rwlock_unlock(&m_rwlock);
    }
    return 0;
}

int XmlItemMgr::init_recv_reader_file_id(unsigned int file_id)
{
    pthread_rwlock_wrlock(&m_rwlock);
    now_file_id = file_id;
    for (unsigned int i = now_file_id; ; i++)
    {
        //if (send_recv_flag[i] == 1)
        if (send_or_recv_set.find(i) != send_or_recv_set.end())
        {
	        reader_log_error("XmlItemMgr::init_recv_reader_file_id now_file_id=%d\n", now_file_id);
            now_file_id++;
            //send_recv_flag[i] = 0;
            send_or_recv_set.erase(i);
        }
        else
        {
	        reader_log_error("XmlItemMgr::init_recv_reader_file_id save now_file_id=%d\n", now_file_id);
            save_now_file_id(now_file_id);
            break;
        }
    } 
	pthread_rwlock_unlock(&m_rwlock);
    return 0;
}
int XmlItemMgr::save_now_file_id(unsigned int file_id)
{
    std::string file_name = base_dir + "/" + "Recv_now_file_id.dat";
    FILE *fp = fopen(file_name.c_str(), "w");
    if (fp == NULL)
    {
        reader_log_error("XmlItemMgr::save_file %s fail\n", file_name.c_str());
        return -1;
    }
    fseek(fp, 0L, SEEK_SET);
    char str[32];
    sprintf(str, "%10.10d", file_id);
    int n = fwrite(str, 10, 1, fp);
    fclose(fp);
    reader_log_error("XmlItemMgr::save_now_file_id %s file_id=%d\n", file_name.c_str(), file_id);
    return 0;
}

int XmlItemMgr::save_send_file_id(unsigned int file_id)
{
    std::string send_file_name = base_dir + "/" + "Send_file_id.dat";
    FILE *fp = fopen(send_file_name.c_str(), "w");
    if (fp == NULL)
    {
        reader_log_error("XmlItemMgr::save_file %s fail\n", send_file_name.c_str());
        return -1;
    }
    fseek(fp, 0L, SEEK_SET);
    char str[32];
    sprintf(str, "%10.10d", file_id);
    int n = fwrite(str, 10, 1, fp);
    fclose(fp);
    reader_log_error("XmlItemMgr::save_send_file_id %s file_id=%d\n", send_file_name.c_str(), file_id);
    return 0;
}

int XmlItemMgr::check_time(int frequency)
{
    if (!is_send_file)
        return 0;
    pthread_rwlock_wrlock(&m_rwlock);
    time_t now = time(NULL);
    reader_log_error("XmlItemMgr::check_time start\n");
    if (now - last_item_time >= frequency)
    {
        reader_log_error("XmlItemMgr::check_time over time\n");
        if (fp != NULL)
        {
            fclose(fp);
            fp = NULL;
            if (fp_index != NULL)
            {
                fclose(fp_index);
                fp_index = NULL;
            }
            // send file_id to task
            SendFileRequest *req = new SendFileRequest();
            req->file_id = now_file_id;
            ACE_Message_Block *msg = new ACE_Message_Block(reinterpret_cast<char *>(req));
            SendFileTask::instance()->put(msg);
            reader_log_error("XmlItemMgr::put_to_send_file_task file_id =%d\n", now_file_id);
            // reset
            index_pos = 0;
            now_file_id++;
            now_item_idx = 0;
        }
    }
    reader_log_error("XmlItemMgr::check_time end\n");
    pthread_rwlock_unlock(&m_rwlock);
    return 0;
}


int XmlItemMgr::save(XmlPage *page)
{
	int ret = 0;
	pthread_rwlock_wrlock(&m_rwlock);
	ret = _save(page);
	pthread_rwlock_unlock(&m_rwlock);
	return(ret);
}


int XmlItemMgr::_save(XmlPage *page)
{
	int ret = 0;
	if(now_item_idx >= MAX_ITEM_COUNT)
	{
		if(fp != NULL) fclose(fp);
		fp = NULL;
        if (fp_index != NULL) fclose(fp_index);
        fp_index = NULL;
        index_pos = 0;
        // send file_id to task
        if (is_send_file)
        {
            SendFileRequest *req = new SendFileRequest();
            req->file_id = now_file_id;
            ACE_Message_Block *msg = new ACE_Message_Block(reinterpret_cast<char *>(req));
            SendFileTask::instance()->put(msg);
            reader_log_error("Xml_Sender::put_to_send_file_task file_id =%d\n", now_file_id);
        }
	    now_file_id++;
		now_item_idx = 0;
    }

	if(fp == NULL)
	{
		char file_name[1024];
		snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), now_file_id);
		fp = fopen(file_name, "w+");
		if(fp == NULL)
		{
			reader_log_error("XmlItemMgr::save open file [%s] fail.\n", file_name);
		}
	}
	if(fp == NULL)
	{
		return(-1);
	}
	if(fp_index == NULL)
	{
		char file_name[1024];
		snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", base_dir.c_str(), now_file_id);
		fp_index = fopen(file_name, "w+");
		if(fp_index == NULL)
		{
			reader_log_error("XmlItemMgr::save open file [%s] fail.\n", file_name);
		}
	}
	if(fp_index == NULL)
	{
		return(-1);
	}	

	ItemData *item = v_items[now_item_idx];
	if(item == NULL)
	{
		reader_log_error("XmlItemMgr::save not find buf for pos=%d,%d\n", now_file_id, now_item_idx);
		return(-2);
	}
	
	item->clear();
	item->header.file_id = now_file_id;
	item->header.item_idx = now_item_idx+1;
	
	item->doc_id = page->objid;
	item->header.doc_id_len = item->doc_id.size();
	
	item->url = page->url;
	item->header.url_len = item->url.size();
	
	item->content.assign(page->doc, page->doc_len);
	item->header.content_len = page->doc_len;
	
	item->header.item_time = 0;
	item->header.status = page->status;


    int pos = index_pos;
	ret = fwrite(&item->header, 1, sizeof(item->header), fp);
    if (ret > 0)
    {
        index_pos += ret;
    }
	if(ret != sizeof(item->header))
	{
		reader_log_error("XmlItemMgr::save %d,%d header fail %d!=%d\n", now_file_id, now_item_idx, ret, sizeof(item->header));
		return(-10);
	}
	ret = fwrite(item->doc_id.c_str(), 1, item->header.doc_id_len, fp);
    if (ret > 0)
    {
        index_pos += ret;
    }
	if(ret != item->header.doc_id_len)
	{
		reader_log_error("XmlItemMgr::save %d,%d header fail %d!=%d\n", now_file_id, now_item_idx, ret, sizeof(item->header));
		return(-11);
	}
	ret = fwrite(item->url.c_str(), 1, item->header.url_len, fp);
    if (ret > 0)
    {
        index_pos += ret;
    }
	if(ret != item->header.url_len)
	{
		reader_log_error("XmlItemMgr::save %d,%d url fail %d!=%d\n", now_file_id, now_item_idx, ret, item->header.url_len);
		return(-12);
	}
	ret = fwrite(item->content.c_str(), 1, item->header.content_len, fp);
    if (ret > 0)
    {
        index_pos += ret;
    }
	if(ret != item->header.content_len)
	{
		reader_log_error("XmlItemMgr::save %d,%d content fail %d!=%d\n", now_file_id, now_item_idx, ret, item->header.content_len);
		return(-13);
	}
    char buf[1024];
    snprintf(buf, 1023, "%d %d %s\n", now_item_idx, pos, item->doc_id.c_str());
    ret = fwrite(buf, 1,  strlen(buf), fp_index);
    if (ret != strlen(buf))
    {
        reader_log_error("XmlItemMgr::save index file fail!\n"); 
        return -14;
    }
	gDocID_t docid;
	Utils::objid2docid(item->doc_id.data(), docid);
	reader_log_error("save pos=%d:%d url=[%s] docid=%016llx-%016llx\n", item->header.file_id, item->header.item_idx, item->url.c_str(), docid.id.value.value_high,docid.id.value.value_low);
	now_item_idx++;
	return (0);
}

