#include "xmlpage_reader.hpp"
#include "reader_log.h"
#include "xmlItemMgr.h"


XmlPageReader::XmlPageReader(int thread_num, int queue_size, config &cfg, ACE_Allocator *alloc):m_thread_num(thread_num), m_queue_size(queue_size), m_group_id(-1), m_alloc(alloc),m_filter(NULL)
{
	xmlInitParser();
        m_max_page_size = MAX_PAGE_SIZE;
        cfg.read_value("MAX_PAGE_SIZE",m_max_page_size);
	reader_log_error("MAX_PAGE_SIZE: %uByte\n",m_max_page_size);
	std::string str;
        time_t t;
        if(cfg.read_value("START_TIME",str) && !str.empty()){
                if (!parse_time(str.c_str(), t)){
                        reader_log_error("invalid START_TIME.\n");
                        return;
                }
                m_filter = new page_time_filter(t);
        }
        else if(cfg.read_value("TIME_FILTER",t)){
                struct timeval tv;
                gettimeofday(&tv,NULL);
                t = tv.tv_sec - t;
                reader_log_error("time_filter.%u.\n",t);
                m_filter = new page_time_filter(t);
        }
}

XmlPageReader::~XmlPageReader()
{
	if ( m_filter )
		delete m_filter;
}

int XmlPageReader::putData(const iovec &data, bool copy, ACE_Time_Value *tv)
{
	iovec *req = (iovec*)m_alloc->malloc(sizeof(iovec));
        if(req == 0){
		reader_log_error("XmlPageReader::putData: malloc req failed!\n");
		return -1;
	}
	if (copy) {
		req->iov_base = m_alloc->malloc(data.iov_len);
		if(req->iov_base == 0 ){
			m_alloc->free(req);
			reader_log_error("XmlPageReader::putData: malloc req failed!\n");
	                return -1;
		}
		memcpy(req->iov_base, data.iov_base, data.iov_len);
	}
	else req->iov_base = data.iov_base;
	req->iov_len = data.iov_len;
	if(m_req_queue.enqueue_tail(req, tv) == -1) {
		if(copy)
			m_alloc->free(req->iov_base);
		m_alloc->free(req);
		if(errno == EWOULDBLOCK) { 
			reader_log_error("XmlPageReader::putData: queue full, put data failed!\n");
			return -1;
		}
	}
	//reader_log_error("XmlPageReader::putData queue_len:%d\n", m_req_queue.message_count());
	
	return 0;
}

ACE_THR_FUNC_RETURN XmlPageReader::queue_pick_loop(void *argc) {
	XmlPageReader * reader = (XmlPageReader *)argc;
	unsigned int cnt(0);
	for(;;){
		iovec *data;
		int ret = reader->m_req_queue.dequeue(data);
		if (ret == -1){
			reader_log_error("dequeue error.\n");
			break;
		}
		cnt++;
		if (cnt%10000 == 0)
			reader_log_error("READ %u xmlpages.\n",cnt);
		XmlPage *page=(XmlPage*)malloc(sizeof(XmlPage));
		page->objid = page->url = page->doc = NULL;
		page->doc = (char *)data->iov_base;
		page->doc_len = data->iov_len;
		free(data);

		std::string str = SS_TO_ASCIIX((bchar_t*)(page->doc));
		//fprintf(stderr,"[page start]\n%s[page end]\nlen:%d\n",SS_TO_ASCIIX((bchar_t*)(page->doc)), page->doc_len);
		//fprintf(stderr,"[data start]\n%s[data end]\nlen:%d\n",SS_TO_ASCIIX((bchar_t*)(data->iov_base)), data->iov_len);
		//fprintf(stderr,"[page_data start]\ngbk:%s\n[page_data end]\n",str.c_str());
		if (str.find("<!--STATUS VR OK-->")==std::string::npos){
			reader_log_error("page format error,missing <!--STATUS VR OK-->\n");
			free_xml(page);	
			continue;
		}
		XmlDoc *doc = new XmlDoc;
		if(!Parse(page->doc, page->doc_len, doc)){
			reader_log_error("xml page parse error.\n");
			delete doc;
			free_xml(page);	
			continue;
		}
		int len = doc->unique_url.size();
		page->url = (char *)malloc((len+1)*sizeof(char));
		memcpy(page->url,(void *)doc->unique_url.c_str(),len);
		page->url[len] = '\0';
		//reader_log_error("page url:%s\n",SS_TO_ASCIIX((bchar_t*)page->url));
		len = doc->objid.size();
		page->objid = (char *)malloc((len+1)*sizeof(char));
		memcpy(page->objid, (void *)doc->objid.c_str(), len);
		page->objid[len] = '\0';
		page->status = doc->status;
		page->update_type = (doc->update_type == "KEY_UPDATE")?VR_KEY_UPDATE:
					(doc->update_type == "CONTENT_UPDATE")?VR_CONTENT_UPDATE:VR_OTHERS;

		//xml doc filter
		if (page->doc_len >= reader->m_max_page_size){
			reader_log_error("page size filter:%s\n",page->url);
			delete doc;
			free_xml(page);	
			continue;
		}

		if (page->status!=XML_DELETE && reader->m_filter!=NULL && !reader->m_filter->check(*doc)){
			reader_log_error("time filter,url:%s, status:%d, fetch_time:%s\n",page->url,page->status,doc->fetch_time.c_str());
			delete doc;
			free_xml(page);	
			continue;
		}
		//end
		ret = g_xmlItemMgr.save(page);
//reader_log_error("XmlPageReader g_xmlItemMgr.save(page)=%d.\n", ret);		
		/*
		reader_log_error("put data\n");
		if (reader->m_next){
			ACE_Message_Block * msg   = new (ACE_Allocator::instance()->malloc(sizeof(ACE_Message_Block))) ACE_Message_Block((char*)page,sizeof(XmlPage*));
			if(reader->m_next->putq(msg)==-1){
				reader_log_error("XmlPageReader::queue_pick_loop:send msg error.\n");
			}
			reader_log_error("put data......\n");
		}
		*/
		delete doc;
		free_xml(page);
	}
	reader_log_error("XmlPageReader completed.\n");
//	reader->clear();
	return 0;
}

int XmlPageReader::reset() {
	clear();
	return initialize();
}

int XmlPageReader::initialize() {
	m_req_queue.open(m_queue_size*sizeof(iovec), m_queue_size*sizeof(iovec));
	m_group_id = ACE_Thread_Manager::instance()->spawn_n(0, m_thread_num, queue_pick_loop, this, THR_NEW_LWP | THR_JOINABLE);
	if(m_group_id == -1) {
		reader_log_error("Error: can't spwan threads in XmlPageReader::initialize().\n");
		return -1;
	}
	return 0;
}

int XmlPageReader::clear() {
	if(m_group_id != -1) {
		m_req_queue.flush();
		m_req_queue.deactivate();
		ACE_Thread_Manager::instance()->wait_grp(m_group_id);
		m_req_queue.close();
	}
	m_group_id = -1;
	return 0;
}

