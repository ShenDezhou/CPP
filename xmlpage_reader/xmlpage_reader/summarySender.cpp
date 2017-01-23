#include <stdexcept>
#include "summarySender.hpp"
#include "offdb/offdb_log.h"
#include "reader_log.h"
#include "Utils.hpp"
#include "Platform/bchar.h"
#include "Platform/bchar_cxx.h"
#include "Platform/bchar_utils.h"
#include "Platform/bchar_stl.h"


QdbSender::QdbSender(int servernums)
{
	m_servernums = servernums;
	//	m_sender.resize(servernums);	
	for(int n=0; n<MIRROR_NUM; n++){
		for(int i=0; i<m_servernums; i++)
			m_sender[n].push_back(QuickdbAdapter());
		pthread_mutex_init(&m_mutex[n],NULL);
	}
}
QdbSender::~QdbSender()
{
	for(int n=0; n<MIRROR_NUM; n++)
		pthread_mutex_destroy(&m_mutex[n]);
}
int QdbSender::initialize()
{
	return 0;
}
int QdbSender::clear()
{
	return 0;
}
int QdbSender::setServer(size_t index, const char *device)
{
	if(index >= m_servernums)
		return -1;
	int ret;
	for (int n=0; n<MIRROR_NUM; n++)
		ret = m_sender[n][index].open(device);
	return ret;
}

inline unsigned int QdbSender::getDistributedId(const gDocID_t & docid)
{
	unsigned long long val = docid.id.value.value_high>>32;
	return (val*m_servernums)>>32;
}

int hash_by_docid(const gDocID_t &docid, const int circles)
{
	unsigned long long val = docid.id.value.value_high>>32;
	return (val*circles)>>32;
}

int QdbSender:: delData(const gDocID_t &docid, void *buf, int size, int flag )
{
	unsigned int id = getDistributedId(docid);
	int mirror = hash_by_docid(docid,MIRROR_NUM);
	const int retry_times = 2;
	int i = 0;

	pthread_mutex_lock(&(m_mutex[mirror]));
	int ret = -1;
	while(i < retry_times) {
		struct timeval tv={5,0};
		ret = m_sender[mirror][id].del((const void*)&docid, sizeof(docid), flag, &tv);
		if (ret>=0)
			break;
		if (errno==EINVAL||errno==ENOSPC)
		{
			offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",docid.id.value.value_high,docid.id.value.value_low,errno);
			break;
		}
		offdb_log_error("OffdbSender::delData: Error: can't send to %s, error# %d, Reconnecting...\n", m_sender[mirror][id].getaddr(), errno);
		m_sender[mirror][id].reconnect(2);
		i++;
	}
	pthread_mutex_unlock(&(m_mutex[mirror]));
	return ret;
}

int QdbSender:: sendData(const gDocID_t &docid, void *buf, int size, int flag )
{
	unsigned int id = getDistributedId(docid);
	int mirror = hash_by_docid(docid,MIRROR_NUM);
	const int retry_times = 2;
	int i = 0;

	pthread_mutex_lock(&(m_mutex[mirror]));
	int ret = -1;
	while(i < retry_times) {
		struct timeval tv={5,0};
		ret = m_sender[mirror][id].put((const void*)&docid, sizeof(docid), buf, size, flag, &tv);
		if (ret>=0)
			break;
		if (errno==EINVAL||errno==ENOSPC)
		{
			offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",docid.id.value.value_high,docid.id.value.value_low,errno);
			break;
		}
		offdb_log_error("OffdbSender::sendData: Error: can't send to %s, error# %d, Reconnecting...\n", m_sender[mirror][id].getaddr(), errno);
		m_sender[mirror][id].reconnect(2);
		i++;
	}
	pthread_mutex_unlock(&(m_mutex[mirror]));
	return ret;
}

bool QdbSender::check_connect()
{
	for(int n = 0; n < MIRROR_NUM; n++)
	{
		for(int i = 0; i < m_servernums; i++)
		{
			if(m_sender[n][i].reconnect(2) == -1)
			{
				return false;
			}
		}
	}

	return true;
}

SummarySender::SummarySender(const config &cfg)
{
	bool wait = true;
	std::vector<std::string> address_list;
	char conval[128];
	unsigned int SendTargetNums = 0;
	if (!cfg.read_value("SUMMARY_NUM",SendTargetNums)||SendTargetNums==0){
		//	        reader_log_error("SUMMARY_NUM error.\n");
		//	        exit(0);
		throw std::runtime_error(conval);
	}
	for (int i=0;i<SendTargetNums;i++)
	{ 
		sprintf(conval,"SUMMARY_GROUP_%d",i);

		if (!cfg.read_address_list(conval, address_list)){
			//			reader_log_error("SummarySender::read SUMMARY_GROUP_%d error.\n",i);
			//			exit(0);
			throw std::runtime_error(conval);
		}

		sprintf(conval,"SUMMARY_GROUP_%d_NUM",i);
		size_t sender_number;
		if (cfg.read_value(conval, sender_number) && sender_number != address_list.size())
		{
			//	                reader_log_error("SendTarget 's num not consist \n");
			//         	       exit(0);
			sprintf(conval,"SendTarget %d num not consist.",i);
			throw std::runtime_error(conval);
		}
		sprintf(conval,"SUMMARY_GROUP_%d_WAIT",i);
		cfg.read_value(conval, wait);
		m_sender.push_back(new QdbSender(sender_number));
		for (int j=0; j<sender_number; j++)
			m_sender[i]->setServer(j,address_list[j].c_str());		
	}
}

SummarySender::~SummarySender()
{
	for (int i=0; i<m_sender.size(); i++)
		delete m_sender[i];
}
int SummarySender::send(XmlPage *page){
	if (page->status == XML_DELETE)
		return send_del_to(page->url, page->objid, page->doc, page->doc_len);
	else return send_to(page->url, page->objid, page->doc, page->doc_len);
}


int SummarySender::send_del_to(const char* url, const char* objid, const void *val, size_t val_len)
{
	int success_num = 0,ret = 0;
	int i;
	for (i=0; i<m_sender.size(); i++)
	{
		gDocID_t docid;
		Utils::objid2docid(objid, docid);
		int result = m_sender[i]->delData(docid,(void *)val, val_len);

		if (result >= 0)
		{
			reader_log_error("[SummarySender]sending succeed:%d\n", ret);
			success_num++;
		}	
		else
		{
			reader_log_error("[SummarySender]ret:%d\n", ret);
			ret = -1;
		}
	}
	reader_log_error("[SummaryDelete]%s, %s\n", objid, url);
	if(success_num < m_sender.size())
	{
		ret = -1;
	}
	return ret;
}


int SummarySender::send_to(const char* url, const char* objid, const void *val, size_t val_len)
{
	int success_num = 0,ret = 0;
	int i;
	for (i=0; i<m_sender.size(); i++)
	{
		gDocID_t docid;
		Utils::objid2docid(objid, docid);
		int result = m_sender[i]->sendData(docid,(void *)val, val_len);

		if (result >= 0)
		{
			reader_log_error("[SummarySender]sending succeed:%d\n", ret);
			success_num++;
		}
		else
		{
			ret = -1;
			reader_log_error("[SummarySender]ret:%d\n", ret);
		}
	}
	reader_log_error("[SummarySender]%s, %s\n", objid, url);
	reader_log_error("[SummarySender]success_num:%d, sender size:%d\n", success_num, m_sender.size());
	if(success_num < m_sender.size())
	{
		ret = -1;
	}

	return ret;
}

bool SummarySender::check_connect()
{
	for (int i=0; i<m_sender.size(); i++)
	{
		if(!m_sender[i]->check_connect())
		{
			return false;
		}
	}

	return true;
}
