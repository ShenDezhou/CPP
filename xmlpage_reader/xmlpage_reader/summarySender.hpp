#ifndef SUMMARYSENDER_HPP
#define SUMMARYSENDER_HPP

#include "offdb/QuickdbAdapter.h"
#include "Platform/docId/docId.h"
#include <vector>
#include "config.hpp"
#include "xmlpage.hpp"
class QdbSender{
public:
        QdbSender(int servernums = 1);
        ~QdbSender();

public:
        int sendData(const gDocID_t &docid, void *buf, int size, int flag = 0);
        int delData(const gDocID_t &docid, void *buf, int size, int flag = 0);
		bool check_connect();

        int initialize();
	int clear();

        int setServer(size_t index, const char *device);

protected:
        unsigned int getDistributedId(const gDocID_t &docid);

protected:
        int m_servernums;
        std::vector<QuickdbAdapter> m_sender[MIRROR_NUM];
public:
	pthread_mutex_t m_mutex[MIRROR_NUM];
};

class SummarySender{
public:
	SummarySender(const config &cfg);
	~SummarySender();
	int send(XmlPage *page);
	bool check_connect();
private:
	int send_to(const char* url, const char* objid, const void *val, size_t val_len);
	int send_del_to(const char* url, const char* objid, const void *val, size_t val_len);
private:
	std::vector<QdbSender*>m_sender;  
};
#endif
