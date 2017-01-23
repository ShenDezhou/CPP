#include "QdbWriter.hpp"
#include "offdb/offdb_log.h"
#include "Platform/log.h"
#include <sstream>

QdbSender::QdbSender(int servernums)
{
	m_servernums = servernums;
	m_sender.resize(servernums);	
}
QdbSender::~QdbSender()
{
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

        return m_sender[index].open(device);
}

inline unsigned int QdbSender::getDistributedId(const gDocID_t & docid)
{
        unsigned long long val = docid.id.value.value_high>>32;
        return (val*m_servernums)>>32;
}

int QdbSender::del_data( const gDocID_t &docid )
{
	unsigned int id = getDistributedId(docid);
	int ret = -1;
	while(1) {
		struct timeval tv={5,0};
		ret = m_sender[id].del((const void*)&docid, sizeof(docid), 0, &tv);
		if (ret>=0)
			break;
		if (errno==EINVAL||errno==ENOSPC) {
			offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",(unsigned long long)docid.id.value.value_high,(unsigned long long)docid.id.value.value_low,errno);
			break;
		}
		offdb_log_error("QdbSender::del_data: Error: can't del at %s, error# %d, Reconnecting...\n", m_sender[id].getaddr(), errno);
		//m_sender[id].close();
		m_sender[id].reconnect();
	}
	return ret;
}

int QdbSender::sendData(const gDocID_t &docid, void *buf, int size, int flag )
{
	unsigned int id = getDistributedId(docid);
	int ret = -1;
	while(1) {
		struct timeval tv={5,0};
		ret = m_sender[id].put((const void*)&docid, sizeof(docid), buf, size, flag, &tv);
		if (ret>=0)
			break;
		if (errno==EINVAL||errno==ENOSPC)
		{
			offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",(unsigned long long)docid.id.value.value_high,(unsigned long long)docid.id.value.value_low,errno);
			break;
		}
		offdb_log_error("QdbSender::qdb_data: Error: can't send to %s, error# %d, Reconnecting...\n", m_sender[id].getaddr(), errno);
		//m_sender[id].close();
		m_sender[id].reconnect();
	}
	return ret;
}

int QdbSender::getData(const gDocID_t &docid,size_t key_len, std::string &xml)
{
    //gDocID_t docid;
    //sscanf(key,"%01611x-%01611x",(unsigned long long)&docid.id.value.value_high,(unsigned long long)&docid.id.value.value_low);

    unsigned int id = getDistributedId(docid);
    int ret = -1;
    void *key,*val = NULL;
    size_t val_len = 0;
    key = malloc(key_len);
    memcpy(key,(void *)&docid,key_len);

    while(1){
        ret = m_sender[id].get((void *)&docid,key_len,val,val_len);
        if(ret >= 0)
        {
            SS_DEBUG((LM_TRACE,"qdb get data ret:%d\n",ret));
            break;
        }

        if (errno==EINVAL||errno==ENOSPC)
        {
            offdb_log_error("[Discard] %016llx-%016llx errno=%d\n",
                    (unsigned long long)docid.id.value.value_high,(unsigned long long)docid.id.value.value_low,errno);
            break;
        }
        offdb_log_error("QdbSender::qdb_data: Error: can't get from %s, error# %d, Reconnecting...\n", m_sender[id].getaddr(), errno);
        //m_sender[id].close();
        m_sender[id].reconnect();
    }
    xml.clear();
    if(ret == 0){
        xml.assign((const char *)val,val_len);
        SS_DEBUG((LM_DEBUG,"qdb get data succeed %llx-%llx value length:%d\n%s\n",
                    (unsigned long long)docid.id.value.value_high,(unsigned long long)docid.id.value.value_low,val_len,xml.c_str()));
        free(val);
    }
    else
        SS_DEBUG((LM_ERROR,"qdb get data failed %llx-%llx value length:%d\n",
                    (unsigned long long)docid.id.value.value_high,(unsigned long long)docid.id.value.value_low,val_len));

    free(key);
    return ret;
}


QdbWriter::QdbWriter(const char *addr)
{
	//bool wait = true;
	std::vector<std::string> address_list;
	//char conval[64];
	unsigned int SendTargetNums = 1;	//0;
	for (int i=0;i<SendTargetNums;i++) {
		size_t sender_number = 1;
		m_sender.push_back(new QdbSender(sender_number));
		for (int i=0; i<sender_number; i++)
			m_sender[i]->setServer(i,addr);		
	}
}

QdbWriter::~QdbWriter()
{
	for (int i=0; i<m_sender.size(); i++)
		delete m_sender[i];
}

int QdbWriter::del(const char *url) {
	int i, result = -1;
	gDocID_t docid;
	url2DocId(url,&(docid.id));
	for (i=0; i<m_sender.size(); i++) {
		SS_DEBUG((LM_DEBUG, "url:%s\ndocid:%llx-%llx\n",url,docid.id.value.value_high,docid.id.value.value_low));
		result = m_sender[i]->del_data(docid);
	}
	//SSLOG_DEBUG("QdbWriter::del %s\n",url);
	return result;
}
int QdbWriter::put(const char *url, const char *val, size_t val_len) {
	int i, result = -1;
	for (i=0; i<m_sender.size(); i++) {
		gDocID_t docid;
		url2DocId(url,&(docid.id));
		//SSLOG_DEBUG("url:%s\ndocid:%llx-%llx\n",url,docid.id.value.value_high,docid.id.value.value_low);
		SS_DEBUG((LM_TRACE, "QdbWriter::put: docid=%llx-%llx,data_len=%d\n",docid.id.value.value_high,docid.id.value.value_low,val_len));
		result = m_sender[i]->sendData(docid,(void *)val, val_len);
	}
	//SSLOG_DEBUG("QdbWriter::send %s\n",url);
	return result;
}
int QdbWriter::put_by_docId(const gDocID_t& docid, const char *val, size_t val_len)
{
    int i, result = -1;
    for (i=0; i<m_sender.size(); i++) {
        //SSLOG_DEBUG("url:%s\ndocid:%llx-%llx\n",url,docid.id.value.value_high,docid.id.value.value_low);
		SS_DEBUG((LM_TRACE, "QdbWriter::put_by_docId: docid=%llx-%llx\n",docid.id.value.value_high,docid.id.value.value_low));
		result = m_sender[i]->sendData(docid,(void *)val, val_len);
	}
	//SSLOG_DEBUG("QdbWriter::send %s\n",url);
	return result;
}
int QdbWriter::del_by_docId(const gDocID_t & docid) {
	int i, result = -1;
    for (i=0; i<m_sender.size(); i++) {
		SS_DEBUG((LM_DEBUG, "QdbWriter::del_by_docId docid=%llx-%llx\n",docid.id.value.value_high,docid.id.value.value_low));
		result = m_sender[i]->del_data(docid);
	}
	//SSLOG_DEBUG("QdbWriter::del %s\n",url);
	return result;
}

int QdbWriter::get(const char *key,std::string &xml)
{
    int i, result = -1;
    gDocID_t docid;
    std::stringstream ss1,ss2;
    std::string high,low;
    
    const char *p = strchr(key,'-');
    high.assign(key,p-key);
    p++;

    low.assign(p);
    SS_DEBUG((LM_DEBUG,"high %s low %s\n",high.c_str(),low.c_str()));
    ss1 << std::hex << high;
    ss1 >> docid.id.value.value_high;
    
        
    ss2 << std::hex << low;
    ss2 >> docid.id.value.value_low;
    /*SS_DEBUG((LM_TRACE,"delete brb docid %s length %d\n",key,strlen(key)));
    char keystr[33];
    strncpy(keystr,key,33);
    sscanf(key,"11x-%llx",&docid.id.value.value_high,&docid.id.value.value_low);
    SS_DEBUG((LM_TRACE,"QdbWriter::get brb docid=%llx-%llx\n",docid.id.value.value_high,docid.id.value.value_low));*/
    for(i = 0; i<m_sender.size();i++){
        SS_DEBUG((LM_TRACE,"QdbWriter::get docid=%llx-%llx\n",docid.id.value.value_high,docid.id.value.value_low));
        result = m_sender[i]->getData(docid,sizeof(docid),xml);
    }
    return result;
}

    
