#ifndef __XMLPAGE_READER_HPP
#define __XMLPAGE_READER_HPP

#include "BaseInputReceiverAck.h"
#include "send_task.hpp"
#include "config.hpp"
#include "Parser.h"
#include "base.h"
#include <time.h>
#include "send_file_task.h"
// Convert HTTP-date to local time
// see RFC 2616, 3.3 Date/Time formats
inline bool parse_time(const char* str, time_t& time)
{
    locale_t loc;
    loc = newlocale (LC_TIME, "en_US.utf8", (locale_t)0);

    struct tm tm;
    if (strptime_l(str, "%c", &tm, loc))
        time = mktime(&tm);
    else if (strptime_l(str, "%a, %d %b %Y %H:%M:%S %Z", &tm, loc) || // rfc1123-date
             strptime_l(str, "%a, %d-%b-%y %H:%M:%S %Z", &tm, loc)) // rfc950-date
    {
        time_t t = mktime(&tm);
        time = t + 8*3600; // convert GMT to local time.
    }
	else if (strptime_l(str, "%a %b %d %H:%M:%S %Y", &tm, loc)) //Tue Sep  1 15:09:50 2015
	{
	    time = mktime(&tm);
	}
	else
    {
        freelocale(loc);
        return false;
    }
    freelocale(loc);
	return true;
}
#define MAX_PAGE_SIZE 1*1024*1024
class page_time_filter
{
public:
	page_time_filter(time_t start_time = 0, time_t stop_time = 0) :
		m_start_time(start_time), m_stop_time(stop_time)
	{
	}
	~page_time_filter(){}
	bool check(const XmlDoc &doc)
	{
        reader_log_error("time check %d %d\n", m_start_time, m_stop_time);
        reader_log_error("time check fecth_time:%s\n", doc.fetch_time.c_str());
		time_t t;
		if (doc.source != "external")
			return true;
		if (!parse_time(doc.fetch_time.c_str(), t))
        {
            reader_log_error("time check page_time false\n");
            return false;
        }
		if (t < m_start_time)
        {
            reader_log_error("time check <<<<<<<\n");
            return false;
        }
		if (m_stop_time && t > m_stop_time)
        {
            reader_log_error("time check >>>>>>>>>\n");
            return false;
        }
		return true;
	}
private:
	time_t m_start_time;
	time_t m_stop_time;
};

class XmlPageReader:public BaseInputReceiverAck {
	public:
		XmlPageReader(int thread_num, int queue_size, config &cfg, ACE_Allocator *alloc = ACE_Allocator::instance());
		virtual ~XmlPageReader();
	public:
		int putData(const iovec &data, bool copy = true, ACE_Time_Value *tv = NULL);
		static ACE_THR_FUNC_RETURN queue_pick_loop(void *argc);
	public:
		virtual int initialize();
		virtual int clear();
		virtual int reset();
		virtual int wait_threads() { return ACE_Thread_Manager::instance()->wait_grp(m_group_id); }
		virtual bool queue_empty() { return m_req_queue.is_empty(); }
		virtual void setNext(Send_Task *next) { m_next = next; }
		virtual void stop_task() { clear(); }
	protected:
		ACE_Message_Queue_Ex<iovec, ACE_MT_SYNCH> m_req_queue;
		ACE_Allocator *m_alloc;
		Send_Task *m_next;
		int m_group_id;
		int m_queue_size;
		int m_thread_num;
		page_time_filter* m_filter;
		size_t m_max_page_size;
};

#endif	/* __XMLPAGE_READER_HPP */
