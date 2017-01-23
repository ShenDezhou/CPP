#include "send_file_task.h"
#include "Platform/bchar_utils.h"
#define DELETE_ITEM_LEN (1)

int XmlSendFile::connect()
{
    ACE_SOCK_Connector connector;
    ACE_INET_Addr addr(m_address.c_str());
    if (sock_sendbuf_size > 16384)
        m_stream.set_option(SOL_SOCKET,SO_SNDBUF,&sock_sendbuf_size,sizeof(sock_sendbuf_size));
    m_connected = connector.connect(m_stream, addr) != -1;
    if (m_connected)
    {
        return 0; 
    }
    return 1;
}

int XmlSendFile::close()
{
    m_stream.close();
    m_connected = false;
}

// return file_id
int XmlSendFile::recv_response()
{
    if (!m_connected) connect();
    if (!m_connected) return -1;

    ACE_Time_Value tv(50);
    SendFileHeader header;

    int recv_len;
    recv_len = m_stream.recv_n(&header, sizeof(SendFileHeader), &tv);
    if (recv_len != sizeof(SendFileHeader))
    {
        reader_log_error("XmlSendFile::recv_response error\n");
        return -1;
    }
    if (header.flag == SEND_RESPONSE)
    {
        reader_log_error("XmlSendFile::recv_response success file_id %d\n", header.file_id);
        return header.file_id;
    }
    else
    {
        reader_log_error("XmlSendFile::recv_response error not response header\n");
        return -2;
    }
}
int XmlSendFile::send_file(char *file_name, SEND_FILE_TYPE type, unsigned int file_id)
{
    if (!m_connected) connect();
    if (!m_connected) return -1;

    int read_len = 0;
    int send_len = 0;

    if (type == SEND_INIT_FILE_ID)
    {
        SendFileHeader header;
        header.file_size = 0;
        header.file_id = file_id;
        header.flag = type;

        reader_log_error("XmlSendFile::send init file_id %d\n", file_id);
        send_len = m_stream.send_n(&header, sizeof(SendFileHeader));
        
        if (send_len != sizeof(SendFileHeader))
        {
            reader_log_error("XmlSendFile::send init file_id err %d\n", file_id);
            m_stream.close();
            m_connected = false;
            return -3;
        }           
        return 0;
    }

    struct stat file_stat;   
    if (stat(file_name, &file_stat) != 0)
    {
        reader_log_error("XmlSendFile::stat error file_name %s\n", file_name);
        if (type != SEND_INDEX && type != SEND_ITEM)
        {
            reader_log_error("XmlSendFile::type error file_name %s\n", file_name);
            return -2;
        }
        else 
        {
            SendFileHeader header;
            header.file_size = DELETE_ITEM_LEN;
            header.file_id = file_id;
            header.flag = type;

            reader_log_error("XmlSendFile::send delete file %s\n", file_name);
            send_len = m_stream.send_n(&header, sizeof(SendFileHeader));
            
            if (send_len != sizeof(SendFileHeader))
            {
                reader_log_error("XmlSendFile::send header error delete file %s\n", file_name);
                m_stream.close();
                m_connected = false;
                return -3;
            }           
            char tmp_buf[2] = {'c', 0};
            int tmp_len = m_stream.send_n(tmp_buf, DELETE_ITEM_LEN);
            if (tmp_len != DELETE_ITEM_LEN)
            {
                reader_log_error("XmlSendFile::send data error delete file %s\n", file_name);
                m_stream.close();
                m_connected = false;
                return -5;
            }
            return 0;
        }
    }

    SendFileHeader header;
    header.file_size = file_stat.st_size;
    header.file_id = file_id;
    header.flag = type;

    send_len = m_stream.send_n(&header, sizeof(SendFileHeader));
    
    if (send_len != sizeof(SendFileHeader))
    {
        reader_log_error("XmlSendFile::send header error %s\n", file_name);
        m_stream.close();
        m_connected = false;
        return -3;
    }

    FILE *fp = NULL;
    if ((fp = fopen(file_name, "r")) == NULL) 
    { 
        reader_log_error("XmlSendFile::open error file_name %s\n", file_name);
        return -4;
    }
    bzero(send_buf, BUF_SIZE);
    while ((read_len = fread(send_buf, sizeof(char), BUF_SIZE, fp)) > 0)
    {
        send_len = m_stream.send_n(send_buf, read_len);
        if (send_len != read_len)
        {
            reader_log_error("XmlSendFile::send data error %s\n", file_name);
            fclose(fp);
            m_stream.close();
            m_connected = false;
            return -5;
        }
    }
    fclose(fp);
    return 0;
}
	
int SendFileTask::init(config *m_cfg)
{
	char str[128] = "XML_READER_ADDRESS";
	m_cfg->read_value(str, xml_reader_addr);
    m_cfg->read_value("BAK_FILE_DIR",base_dir);
    m_file_name = base_dir + "/" + "Send_file_id.dat";
    send_file_thread_num = 1;
    m_cfg->read_value("SEND_FILE_THREAD_NUM", send_file_thread_num);
	return open();
}

int SendFileTask::open(void *)
{
    return activate(THR_NEW_LWP, send_file_thread_num);
}

int SendFileTask::put(ACE_Message_Block *message, ACE_Time_Value *timeout)
{
     return putq(message, timeout);
}

int SendFileTask::svc()
{
    XmlSendFile *xml_send_file = new XmlSendFile(xml_reader_addr);
    xml_send_file->connect();
    for (ACE_Message_Block *msg_blk; getq(msg_blk) != -1; ) 
    {
        SendFileRequest  *req = reinterpret_cast<SendFileRequest *> (msg_blk->rd_ptr());
        msg_blk->release();
        while (1)
        {
            char file_name[1024];
            // send index file
            snprintf(file_name, 1023, "%s/xml_index_%8.8d.dat", base_dir.c_str(), req->file_id);
            bool ret_index = xml_send_file->send_file(file_name, SEND_INDEX, req->file_id) == 0 && xml_send_file->recv_response() > 0;
            reader_log_error("SendFileTask::send_file index_file_id %d  ret:%d \n", req->file_id, ret_index);
            // send item file
            snprintf(file_name, 1023, "%s/xml_item_%8.8d.dat", base_dir.c_str(), req->file_id);
            bool ret_item = xml_send_file->send_file(file_name, SEND_ITEM, req->file_id) == 0 && xml_send_file->recv_response() > 0;
            reader_log_error("SendFileTask::send_file item_file_id %d  ret:%d \n", req->file_id, ret_item);
            if (ret_item)
            {
                g_xmlItemMgr.success_send_file(req->file_id); 
                reader_log_error("SendFileTask::send_file item file_id %d ok\n", req->file_id);
                break;
            }
            else
            {
                reader_log_error("SendFileTask::send_file item file_id %d error\n", req->file_id);
                // close connect 
                xml_send_file->close();
                sleep(1);
                continue;
            }
        }
        delete req;
    }
    delete xml_send_file;
	return 0;
}

int SendFileTask::save(unsigned int file_id)
{
    FILE *fp = fopen(m_file_name.c_str(), "w");
    if (fp == NULL)
    {
        reader_log_error("SendFileTask::save_file %s fail\n", m_file_name.c_str());
        return -1;
    }
    fseek(fp, 0L, SEEK_SET);
    char str[32];
    sprintf(str, "%10.10d", file_id);
    int n = fwrite(str, 10, 1, fp);
    fclose(fp);
    reader_log_error("SendFileTask::save_send_file_id %s file_id=%d\n", m_file_name.c_str(), file_id);
    return 0;
}

void SendFileTask::stop_task()
{
	flush();
	wait();
	return;
}
