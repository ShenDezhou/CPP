#include <netinet/tcp.h>
#include "data_handler.h"
#include "xmlItemMgr.h"

pthread_mutex_t  mutex_data_all = PTHREAD_MUTEX_INITIALIZER;
int g_data_handle_count = 0;

int DataHandler::open(void* arg)
{
	queued_count_ = 0;
	deferred_close_ = 0;
	recv_len = 0;
	
	++g_data_handle_count;

	int flag = 1;
	setsockopt(get_handle(), IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag));
    if (sock_recvbuf_size > 87380)
        setsockopt(get_handle(), SOL_SOCKET, SO_RCVBUF, &sock_recvbuf_size, sizeof(sock_recvbuf_size)); 

	this->peer().get_remote_addr(client_addr);
	return parent::open(arg);
}

int DataHandler::handle_input(ACE_HANDLE handle)
{
	this->peer().get_remote_addr(client_addr);
	timeval _beg_time;
	gettimeofday(&_beg_time,NULL);
	int ret = peer().recv(recv_buf + left_index, MAX_XML_DATA_LEN - left_index);

	if (ret <= 0)
	{
		return -1;
	}

	recv_len += ret;
    int buf_size = ret + left_index;

	if(recv_len < sizeof(DataHeader)) 
    {
        left_index += ret;
        return 0;
    }

	while (recv_len > 0)
	{
		if(recv_len < sizeof(DataHeader)) return(0);
		
        if (!is_new_file)
        {
            DataHeader *data_header = (DataHeader *)recv_buf;

		    packet_len = sizeof(DataHeader) + data_header->len;
            header.flag = data_header->flag;
            file_id = data_header->file_id;
            if (header.flag == SEND_INIT_FILE_ID)
            {
                g_xmlItemMgr.init_recv_reader_file_id(file_id);
                left_index = buf_size - packet_len;
                recv_len -= packet_len;
                memmove(recv_buf, recv_buf + packet_len, buf_size - packet_len);
                buf_size -= packet_len;
                write_len = 0;
                continue;
            }
		    // create file
            g_xmlItemMgr.start_recv_file(header.flag, file_id); 
            is_new_file = true;
            
            // write to file 
            if (recv_len < packet_len)
            {
                //[sizeof(DataHandler), buf_size)
                g_xmlItemMgr.recv_data(recv_buf + sizeof(DataHeader), buf_size - sizeof(DataHeader), file_id);
                write_len += buf_size;
                left_index = 0;
                reader_log_error("DataHandler::start_recv file_id:%d, len :%d\n", file_id, data_header->len);
                return 0;
            }
            else
            {
                // [sizeof(DataHeader), packet_len)
                g_xmlItemMgr.recv_data(recv_buf + sizeof(DataHeader), packet_len - sizeof(DataHeader), file_id);
                left_index = buf_size - packet_len;
                recv_len -= packet_len;
                memmove(recv_buf, recv_buf + packet_len, buf_size - packet_len);
                buf_size -= packet_len;
                write_len = 0;
                // close file
                g_xmlItemMgr.end_recv_file(header.flag, file_id);
                is_new_file = false;
                // send response
                sendResult(file_id);
            }
        }
        else
        {

            if (recv_len < packet_len)
            {
                // write to file [0, buf_size)
                g_xmlItemMgr.recv_data(recv_buf, buf_size, file_id);
                write_len += buf_size;
                left_index = 0;
                return 0;
            }
            else
            {
                // write [0, packet_len - write_len)
                g_xmlItemMgr.recv_data(recv_buf, packet_len - write_len, file_id);
                recv_len -= packet_len;
                left_index = buf_size - (packet_len - write_len);
                memmove(recv_buf, recv_buf + (packet_len - write_len), buf_size - (packet_len - write_len));
                buf_size -= packet_len - write_len;
                write_len = 0;
                // close file
                g_xmlItemMgr.end_recv_file(header.flag, file_id);
                is_new_file = false;
                // send response
                sendResult(file_id);
            }
        }

	}
    return 0;
}

int DataHandler::handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask )
{
	int close_now = 0;
	if (handle != ACE_INVALID_HANDLE) 
	{
		//发生异常或是客户端主动关闭
		pthread_mutex_lock(&mutex_data_all);
		if (queued_count_ == 0) close_now = 1;
		else deferred_close_ = 1;
		pthread_mutex_unlock(&mutex_data_all);
	}
	else 
	{
		//正常处理结束， 不主动进行关闭
		pthread_mutex_lock(&mutex_data_all);
		queued_count_--; 
		if (queued_count_ == 0) close_now = deferred_close_;
		pthread_mutex_unlock(&mutex_data_all);
	} 

	if (close_now) 
	{
		//SS_DEBUG((LM_DEBUG, "[DataHandler::handle_close]\n"));
		--g_data_handle_count;
		return parent::handle_close(handle, close_mask);
	} else
		return 0;
}

int DataHandler::sendResult(uint32_t file_id)
{
    DataHeader header;
    header.file_id = file_id;
    header.flag = SEND_RESPONSE;
    ACE_Time_Value tv(50);
    int ret = peer().send_n(&header, sizeof(DataHeader), &tv);
    if (ret != sizeof(DataHeader))
    {
        reader_log_error("DataHandler::send_response error\n"); 
    }
    reader_log_error("DataHandler::send_response file_id:%d\n", file_id); 
    return 0;
}

