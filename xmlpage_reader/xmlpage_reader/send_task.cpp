#include "send_task.hpp"
#include "Platform/bchar_utils.h"

Send_Task::Send_Task(const char* file, const char* key, PageStore *summary_pagestore, PageStore *index_pagestore, bool bak_source):_thr_count(0)
{
	int thread_num = 1;
	m_cfg = new config(file, key);
	if (!bak_source)
	{
		m_cfg->read_value("SEND_TASK_THREAD_NUM",thread_num);
	}
	if_send_index = true;
	if_send_summary = true;
	m_summary_pagestore = summary_pagestore;
	m_index_pagestore = index_pagestore;
	m_bak_source = bak_source;

	m_cfg->read_value("IF_SEND_INDEX",if_send_index);
	m_cfg->read_value("IF_SEND_SUMMARY",if_send_summary);

	if (if_send_index){
		Sender *index_sender = new Sender(*m_cfg,"INDEX");
		delete index_sender;
	}
	if (if_send_summary){
		SummarySender *summary_sender = new SummarySender(*m_cfg);
		delete summary_sender;
	}

	msg_queue_->high_water_mark(1000*sizeof(ACE_Message_Block*));
	if (open((void*)thread_num)){
		reader_log_error("Send_Task::open send_task thread failed.\n");
		_exit(0);
	}
}
Send_Task::~Send_Task()
{
	for (int i=0;i<_thr_count;i++)
		delete []_stack[i];
	delete []_stack;
	delete []_stack_size;
	/*	if (index_sender)
		delete index_sender;
		if (summary_sender)
		delete summary_sender;
	 */
}
int Send_Task::open(void *arg)
{
	if (arg!=NULL)
		_thr_count = (size_t)arg;
	_stack_size=new size_t[_thr_count];
	_stack = new char*[_thr_count];
	for (int i=0;i<_thr_count;i++)
	{
		_stack[i]=new char[2*1024*1024];
		_stack_size[i]=2*1024*1024;
	}
	int res=activate(THR_NEW_LWP | THR_JOINABLE | THR_INHERIT_SCHED, _thr_count,0,
			ACE_DEFAULT_THREAD_PRIORITY,-1,0,0,(void**)_stack,_stack_size);
	return res;
}
int Send_Task::close(u_long)
{
	//	reader_log_error("Send_Task::close send_task thread.\n");
	return 0;
}
int Send_Task::svc()
{
	ACE_Message_Block *data;
	Sender* index_sender = NULL;
	SummarySender* summary_sender = NULL;
	if (if_send_index)
		index_sender = new Sender(*m_cfg,"INDEX");
	if (if_send_summary)
		summary_sender = new SummarySender(*m_cfg);

	if (!m_bak_source)
	{
		for(;;){
			//reader_log_error("Send_Task::send thread is running..\n");
			if (getq(data) == -1){
				reader_log_error("Send_Task::svc getq failed.\n");
				break;
			}
			XmlPage * page=(XmlPage *)data->rd_ptr();
			
			reader_log_error("Send_Task::get page,page url: %s\n",page->url);
			//reader_log_error("Send_Task::get page,page doc: %s\n",page->doc);
			delete data;
			if (summary_sender)
			{
				if(summary_sender->send(page) == -1)
				{
					reader_log_error("Send_Task::writing summary file..\n");
					m_summary_pagestore->WritePage(*page);
				}
			}
			if (index_sender)
			{
				if(index_sender->send(page) == -1)
				{
					reader_log_error("Send_Task::writing index file..\n");
					m_index_pagestore->WritePage(*page);
				}
			}
			free_xml(page);
		}
	}
	else
	{
		int reconnect_interval = 5 * 60;
		if (m_cfg != NULL)
		{
			m_cfg->read_value("RECONNECT_INTERVAL",reconnect_interval);
		}

		for(;;)
		{
			reader_log_error("Send_Task::try to send bak file..\n");
			if (summary_sender && summary_sender->check_connect())
			{
				reader_log_error("Send_Task::reconnecting summary succeed..\n");
				m_summary_pagestore->SendPage(summary_sender);
			}

			if (index_sender && index_sender->check_connect())
			{
				reader_log_error("Send_Task::reconnecting index succeed..\n");
				m_index_pagestore->SendPage(index_sender);
			}
			sleep(reconnect_interval);		
		}
	}
	
	if (summary_sender)
		delete summary_sender;
	if (index_sender)
		delete index_sender;
	reader_log_error("Send_Task::svc completed.\n");
	return 0;
}
void Send_Task::stop_task()
{
	flush();
	wait();
	return;
}
