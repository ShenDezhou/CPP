#include "xmlSender.h"
#include <fstream>
#include <sstream>

int hash_url_by_docid(const gDocID_t & docid, int number) 
{
	assert(number > 0);
	unsigned long long val = docid.id.value.value_high>>32;
	return (val*number)>>32;
}


int SUM_CONN::send(const char* url, gDocID_t &docid, const void *data, size_t size, int status)
{
	if(!m_connected)
	{
		if(qdb_conn.reconnect(0)<0) return(-1);
		m_connected = true;
	}
	
	int ret = 0;
	struct timeval tv={5,0};
	if (status == XML_DELETE)
	{
		ret = qdb_conn.del((const void*)&docid, sizeof(docid), 0, &tv);
	}
	else
	{
		ret = qdb_conn.put((const void*)&docid, sizeof(docid), data, size, 0, &tv);		
	}
	if(ret>=0) return(1); 

	m_connected = false;
	
	if (errno==EINVAL||errno==ENOSPC)
	{
		//offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",docid.id.value.value_high,docid.id.value.value_low,errno);
		return(-2);
	}
	return(-3);
}

int INDEX_CONN::connect()
{
	ACE_SOCK_Connector connector;
	ACE_INET_Addr addr(m_address.c_str());
	m_connected = connector.connect(m_stream, addr) != -1;
	if(m_connected)
	{
		return(0);
	}
	return(-1);	
}

int INDEX_CONN::send(const char* url, gDocID_t &docid, const void *data, size_t size, int status)
{
	platform::Transceiver r;
	if(!m_connected) connect();
	if(!m_connected) return(-1);

	void *val=NULL;
	size_t val_len;
	int rr;
	ACE_Time_Value tv(5),tv2(5),tv3(5);
	if (r.open(m_stream, m_allocator) == 0 && r.sendMsg(data, size,0,&tv) > 0 &&r.recvMsg(val,val_len,&tv2,&tv3)>0)
	{
		free(val);
		return(1);
	}
	else
	{
		if(val) free(val);
		m_stream.close();
		m_connected = false;
	}
	return(-2);
}

int Xml_Sender::init()
{
	FILE *fp = fopen(m_file_name.c_str(), "r");
	if(fp == NULL)
	{
		reader_log_error("Xml_Sender::init open file %s fail\n", m_file_name.c_str());
		return(-1);	
	}
	char str[32];
	int n = fread(str, 1, 21, fp);
	fclose(fp);
	if(n != 21)
	{
		reader_log_error("Xml_Sender::init read file %s size=%d error\n", m_file_name.c_str(), n);
		return(-2);
	}
	str[10] = 0; str[21] = 0;
	file_id = atoi(str);
	item_index = atoi(str+11);
	if(file_id < g_xmlItemMgr.min_file_id)
	{
		file_id = g_xmlItemMgr.min_file_id;
		item_index = 1;
		save();
	}
	reader_log_error("Xml_Sender::init read file %s old_pos=%d,%d\n", m_file_name.c_str(), file_id, item_index);
	return(0);	
}

int Xml_Sender::read()
{
	int ret = 0;
	if(file_id > g_xmlItemMgr.now_file_id)
	{
		file_id = g_xmlItemMgr.now_file_id;
		item_index = g_xmlItemMgr.now_item_idx;
		if(item_index == 0) item_index = 1;
		save();
		return(0);
	}
	if(file_id == g_xmlItemMgr.now_file_id && item_index > g_xmlItemMgr.now_item_idx)
	{
		//收到的数据已经全发了
		return(0);
	}
	if(m_data.header.file_id == file_id && m_data.header.item_idx == item_index)
	{
		//刚才已经读好了这个数据， 这种情况应该是前一次发送失败
		return(1);
	}
	if(file_id==g_xmlItemMgr.now_file_id-1 && item_index>g_xmlItemMgr.MAX_ITEM_COUNT)
	{
		file_id++;
		item_index = 1;
		save();
	}
	if(file_id==g_xmlItemMgr.now_file_id || (file_id==g_xmlItemMgr.now_file_id-1 && item_index>g_xmlItemMgr.now_item_idx))
	{
		ret = g_xmlItemMgr.read(&m_data, file_id, item_index);
		if(ret == 1) return(1);
		if(file_id==g_xmlItemMgr.now_file_id) return(0);
	}
	
	//从文件中读取
	if(file_id<g_xmlItemMgr.min_file_id)
	{
		file_id = g_xmlItemMgr.min_file_id;
		item_index = 1;
		save();
	}
	
	if(fd >=0 && fd_file_id!=file_id)
	{
		reader_log_error("Xml_Sender::read %s file %d!=%d over\n", m_key.c_str(), fd_file_id, file_id);
		close(fd);
		fd = -1;
		fd_file_id = -1;
        close(fd_index);
        fd_index = -1;
	}
	if(fd < 0)
	{
		char file_name[1024];
		snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", g_xmlItemMgr.base_dir.c_str(), file_id);
		fd = open(file_name, O_RDONLY);
		if(fd < 0)
		{
			reader_log_error("Xml_Sender::read read file %s fail\n", file_name);
			file_id++;
			item_index = 1;
			fd = -1;
			fd_file_id = -1;
			save();
			return(0);
		}
		fd_file_id = file_id;
		file_pos = 0;
	    
        snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", g_xmlItemMgr.base_dir.c_str(), file_id);
		fd_index = open(file_name, O_RDONLY);
		if(fd_index < 0)
		{
			reader_log_error("Xml_Sender::read index file %s fail\n", file_name);
            fd_index = -1;
            index_num = 0;
            reader_log_error("Xml_Sender::load index_num: %d\n", index_num);
		}
        else
        {
            index_num = load_index(file_name);
            reader_log_error("Xml_Sender::load index_num: %d\n", index_num);
        }
        //if(fd_index < 0)
		//{
		//	reader_log_error("Xml_Sender::read index file %s fail\n", file_name);
        //    file_id++;
        //    close(fd);
        //    fd = -1;
        //    fd_file_id = -1;
        //    item_index = 1;
        //    fd_index = -1;
        //    return 0;
		//}
        //index_num = load_index(file_name);
        //reader_log_error("Xml_Sender::load index_num: %d\n", index_num);
    }
    
    do
    {
        while (item_index < index_num + 1 && hash_index != xml_index[item_index - 1].second)
        {
            //reader_log_error("Xml_Sender::!!! item_index: %d hash_index: %d  data_index: %d  %s \n", item_index,  hash_index, xml_index[item_index - 1].second, m_key.c_str());
            item_index++;
            if (item_index%1000 == 0)
            {
                save();
            }
        }

        //reader_log_error("Xml_Sender::=== item_index: %d hash_index: %d  data_index: %d  %s \n", item_index,  hash_index, xml_index[item_index - 1].second, m_key.c_str());

        if (item_index >= index_num + 1)
            break;

        file_pos = xml_index[item_index - 1].first;
        int n = pread(fd, &m_data.header, sizeof(m_data.header), file_pos);
        //reader_log_error("XmlItemMgr::read() header n=%d file_pos=%d\n", n, file_pos);
		if(n != sizeof(m_data.header)) break;
			
        //reader_log_error("XmlItemMgr::read() header pos=%d,%d now=%d,%d\n", m_data.header.file_id, m_data.header.item_idx, file_id, item_index);
		if(m_data.header.file_id != file_id) break;
		if(m_data.header.item_idx != item_index) break;
        //reader_log_error("XmlItemMgr::read() header len=%d,%d,%d\n", m_data.header.doc_id_len, m_data.header.url_len, m_data.header.content_len);
		
		if(m_data.doc_id.capacity() < m_data.header.doc_id_len + 1) m_data.doc_id.reserve(m_data.header.doc_id_len + 1);
		if(m_data.url.capacity() < m_data.header.url_len + 1) m_data.url.reserve(m_data.header.url_len + 16);
		if(m_data.content.capacity() < m_data.header.content_len + 1) m_data.content.reserve(m_data.header.content_len + 1024);

		int temp_pos = file_pos + n;
		n = pread(fd, (char *)m_data.doc_id.data(), m_data.header.doc_id_len, temp_pos);
		if(n != m_data.header.doc_id_len) break;
		((char *)m_data.doc_id.data())[n] = 0;
			
		temp_pos = temp_pos + n;
		n = pread(fd, (char *)m_data.url.data(), m_data.header.url_len, temp_pos);
		if(n != m_data.header.url_len) break;
		((char *)m_data.url.data())[n] = 0;
			
		temp_pos = temp_pos + n;
		n = pread(fd, (char *)m_data.content.data(), m_data.header.content_len, temp_pos);
		if(n != m_data.header.content_len) break;
		((char *)m_data.content.data())[n] = 0;
		
		temp_pos = temp_pos + n;
		item_index = m_data.header.item_idx;
		file_pos = temp_pos;
		return(1);

    }while (0);
    
    if (index_num <= g_xmlItemMgr.MAX_ITEM_COUNT && item_index >= index_num + 1)
    {
        //索引数据没读完，继续读item数据
        while(1)
        {
            int n = pread(fd, &m_data.header, sizeof(m_data.header), file_pos);
            //reader_log_error("XmlItemMgr::read() header n=%d file_pos=%d\n", n, file_pos);
		    if(n != sizeof(m_data.header)) break;
		    	
            //reader_log_error("XmlItemMgr::read() continue  header pos=%d,%d now=%d,%d\n", m_data.header.file_id, m_data.header.item_idx, file_id, item_index);
		    if(m_data.header.file_id != file_id) break;
            if(m_data.header.item_idx < item_index)
            {
                file_pos += n + m_data.header.doc_id_len + m_data.header.url_len + m_data.header.content_len;
                continue;
            }
            //reader_log_error("XmlItemMgr::read() continue  header len=%d,%d,%d\n", m_data.header.doc_id_len, m_data.header.url_len, m_data.header.content_len);
		    
		    if(m_data.doc_id.capacity() < m_data.header.doc_id_len + 1) m_data.doc_id.reserve(m_data.header.doc_id_len + 1);
		    if(m_data.url.capacity() < m_data.header.url_len + 1) m_data.url.reserve(m_data.header.url_len + 16);
		    if(m_data.content.capacity() < m_data.header.content_len + 1) m_data.content.reserve(m_data.header.content_len + 1024);

		    int temp_pos = file_pos + n;
		    n = pread(fd, (char *)m_data.doc_id.data(), m_data.header.doc_id_len, temp_pos);
		    if(n != m_data.header.doc_id_len) break;
		    ((char *)m_data.doc_id.data())[n] = 0;
		    	
		    temp_pos = temp_pos + n;
		    n = pread(fd, (char *)m_data.url.data(), m_data.header.url_len, temp_pos);
		    if(n != m_data.header.url_len) break;
		    ((char *)m_data.url.data())[n] = 0;
		    	
		    temp_pos = temp_pos + n;
		    n = pread(fd, (char *)m_data.content.data(), m_data.header.content_len, temp_pos);
		    if(n != m_data.header.content_len) break;
		    ((char *)m_data.content.data())[n] = 0;
		    
		    temp_pos = temp_pos + n;
		    item_index = m_data.header.item_idx;
		    file_pos = temp_pos;
		    return(1);
        }
    }
	//文件读完了
	close(fd);
	reader_log_error("Xml_Sender::read %s file %d at %d over\n", m_key.c_str(), file_id, item_index);
	file_id++;
	item_index = 1;
	m_data.clear();
	fd = -1;
	fd_file_id = -1;
	save();
	close(fd_index);
    fd_index = -1;
	return(0);
}
//返回值为发送条数
int Xml_Sender::send()
{
	//if(skip_count>0) {skip_count--; return(0);}
	int ret = read();
	if(ret > 0)
	{
		ret = 0;
		gDocID_t docid;
		Utils::objid2docid(m_data.doc_id.data(), docid);
		int data_index = hash_url_by_docid(docid, hash_num);
		if(hash_index == data_index)
		{
			if(send_type == 1)
			{
				ret = index_conn->send(m_data.url.data(), docid, m_data.content.data(), m_data.header.content_len, m_data.header.status);
                //reader_log_error("Xml_Sender::send send_index addr-doc_id-hash_index:%s:%s:%d\n", m_key.c_str(), m_data.doc_id.data(), hash_index);
			}
			else if (m_data.header.status !=  XML_UPDATE)
			{
				ret = sum_conn->send(m_data.url.data(), docid, m_data.content.data(), m_data.header.content_len, m_data.header.status);
                //reader_log_error("Xml_Sender::send send_sum addr-doc_id-hash_index:%s:%s:%d\n", m_key.c_str(), m_data.doc_id.data(), hash_index);
			}
            //reader_log_error("Xml_Sender::send_ret %d addr-doc_id-hash_index:%s:%s:%d %d\n", ret, m_key.c_str(), m_data.doc_id.data(), hash_index, m_data.header.status);
            //reader_log_error("Xml_Sender::send send_ok addr-doc_id-hash_index:%s:%s:%d %d\n", m_key.c_str(), m_data.doc_id.data(), hash_index, m_data.header.status);
            //ret = 1;
            if (ret == 1)
            {
				reader_log_error("send_docid %s pos=%d:%d url=[%s] docid=%016llx-%016llx status=%d ret=%d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.url.c_str(), docid.id.value.value_high,docid.id.value.value_low, m_data.header.status, ret);
            }
			if(ret <0)
			{
				skip_count = 10000;
				if(ret == -2) //发送失败
				{
					error_count++;
				}
				if(error_count < 5) return(-1);
				//对于长期发送失败的数据， 按发送成功处理了， 跳过
				//reader_log_error("Xml_Sender::send skip %s pos=%d:%d url=[%s] docid=%016llx-%016llx status=%d ret=%d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.url.c_str(), docid.id.value.value_high,docid.id.value.value_low, m_data.header.status, ret);

				//目前对于发送失败的数据不跳过， 持续发送， 一定要成功！
				//reader_log_error("Xml_Sender::send error %s pos=%d:%d url=[%s] docid=%016llx-%016llx status=%d ret=%d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.url.c_str(), docid.id.value.value_high,docid.id.value.value_low, m_data.header.status, ret);
				reader_log_error("send error %s pos=%d:%d %d %d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.header.status, ret);
				error_count=0;
			}
		}
		else
		{
			//reader_log_error("Xml_Sender::send %s file %d:%d hash %d!=%d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, hash_index, data_index);
		}
		//发送成功
		//if(ret!=0) reader_log_error("Xml_Sender::send %s pos=%d:%d url=[%s] docid=%016llx-%016llx status=%d ret=%d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.url.c_str(), docid.id.value.value_high,docid.id.value.value_low, m_data.header.status, ret);
		//if(ret!=0) reader_log_error("send %s pos=%d:%d %d %d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.header.status, ret);
		if(ret == 1) 
        { 
            reader_log_error("send_success %s pos=%d:%d %d %d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.header.status, ret);
        }
        else if (ret != 0)
        {
		    reader_log_error("send %s pos=%d:%d %d %d\n", m_key.c_str(), m_data.header.file_id, m_data.header.item_idx, m_data.header.status, ret);
        }
        skip_count = 0;
		error_count = 0;
		item_index++;
		if(item_index%1000==0) save();
		return(1);
	}
	return(0);	
}

int Xml_Sender::load_index(char *file_name)
{
    std::ifstream fin(file_name);
    std::string line;

    if (!fin)
    {
        return 0;
    }

    int index_num = 0;

    while (getline(fin, line))
    {
        if (line.size() < 16)
            continue;
        std::stringstream ss;
        ss.str(line);
        int index, pos;
        std::string doc_id;
        ss >> index >> pos >> doc_id;
		//reader_log_error("Xml_Sender:: doc_id %s\n", doc_id.c_str());
        gDocID_t docid;
        Utils::objid2docid(doc_id.data(), docid);
        int data_index = hash_url_by_docid(docid, hash_num);
        xml_index[index_num] = std::pair<int, int>(pos, data_index);
        index_num++;
    }

    return index_num;
}

int Xml_Sender::save()
{
	FILE *fp = fopen(m_file_name.c_str(), "w");
	if(fp == NULL)
	{
		reader_log_error("Xml_Sender::save file %s fail\n", m_file_name.c_str());
		return(-1);	
	}
	fseek(fp, 0L, SEEK_SET);

	char str[32];
	sprintf(str, "%10.10d\t%10.10d", file_id, item_index);
	int n = fwrite(str, 21, 1, fp);
	fclose(fp);
	reader_log_error("Xml_Sender::save file %s pos=%d,%d\n", m_file_name.c_str(), file_id, item_index);
	return(0);	
}
