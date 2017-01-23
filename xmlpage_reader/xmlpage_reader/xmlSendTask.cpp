#include "xmlSendTask.h"
#include "Platform/bchar_utils.h"

std::vector<Xml_Send_Task *> g_send_tasks;
	
int Xml_Send_Task::init(config *m_cfg, int type, int index)
{
	int send_type = type;
	int send_index = index;
	
	char str[128], prefix[128];
	if(send_type == 1)  strcpy(prefix, "INDEX");
	else strcpy(prefix, "SUMMARY");

	sprintf(str,"%s_GROUP_%d_NUM", prefix, index);
    int sender_num;
	m_cfg->read_value(str, sender_num);
	if(sender_num <=0)
	{
		reader_log_error("Xml_Send_Task error %s=%d\n", str, sender_num);
		return(-1);
	}

	sprintf(str,"%s_GROUP_%d", prefix, index);
	std::vector<std::string> address_list;
	m_cfg->read_address_list(str, address_list);
	if(address_list.size() != sender_num)
	{
		reader_log_error("Xml_Send_Task error %s address_list.size()=%d!=%d\n", str, address_list.size(), sender_num);
		return(-2);
	}
	for(int i=0; i<sender_num; i++)
	{
        
		Xml_Sender *sender = new Xml_Sender(send_type, send_index, i, sender_num, address_list[i]);
		if(sender == NULL)
		{
			reader_log_error("Xml_Send_Task new Xml_Sender %d %d %d fail\n", send_type, send_index, i);
		}
		reader_log_error("Xml_Send_Task new Xml_Sender %d %d %d %d\n", send_type, send_index, i, sender_num);
		Xml_Send_Task *send_task = new Xml_Send_Task(type, index, sender_num, sender);
        g_send_tasks.push_back(send_task);
	}
	return(0);
}

Xml_Send_Task::Xml_Send_Task(int send_type, int send_index, int sender_num, Xml_Sender *sender):send_type(send_type), send_index(send_index),
    sender_num(sender_num), sender(sender)
{
    
}

int Xml_Send_Task::open(void *)
{
	if(sender_num <= 0)
	{
		reader_log_error("Xml_Send_Task::open %d %d fail\n", send_type, send_index);
		return -1;
	}
    return activate(THR_NEW_LWP, 1);
}

int Xml_Send_Task::svc()
{
	int ret = 0;
	while(stop_signal==0)
	{
		if(sender->send() == 0)
		{
			ret++;
		}

        //if (ret < 100)
        //{
		//    reader_log_error("Xml_Send_Task::task_ret %d\n", ret);
        //    sleep(1);
        //}
        //else
        //{
        //    ret = 200;
        //}

		//如果都没有数据可发送
		if(ret >= sender_num * 3)
		{
			int temp_min_id = 0, temp_min_index = 0;
			for(int n=0; n<g_send_tasks.size(); n++)
			{
				if(temp_min_id == 0 || temp_min_id>g_send_tasks[n]->sender->file_id)
					temp_min_id = g_send_tasks[n]->sender->file_id;
						
				if(temp_min_index == 0 || temp_min_index>g_send_tasks[n]->sender->item_index)
					temp_min_index = g_send_tasks[n]->sender->item_index;
			}
			if(temp_min_id>0) g_xmlItemMgr.set_min_file(temp_min_id, temp_min_index);
			ret = 0;
			sleep(1);
		}
	}
	
	delete sender;
	reader_log_error("Xml_Send_Task::svc %d %d completed.\n", send_type, send_index);
	return 0;
}

void Xml_Send_Task::stop_task()
{
	flush();
	wait();
	return;
}
