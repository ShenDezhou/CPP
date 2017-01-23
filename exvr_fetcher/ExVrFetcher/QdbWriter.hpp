#ifndef QDBWRITER_HPP
#define QDBWRITER_HPP

#include "offdb/QuickdbAdapter.h"
#include "Platform/docId/docId.h"
#include <vector>
class QdbSender
{
public:
    QdbSender(int servernums = 1);
    ~QdbSender();

public:
    int sendData(const gDocID_t &docid, void *buf, int size, int flag = 0);
    int del_data( const gDocID_t &docid );
    int getData(const gDocID_t &docid,size_t key_len, std::string &xml);

    int initialize();
    int clear();

    int setServer(size_t index, const char *device);

protected:
    unsigned int getDistributedId(const gDocID_t &docid);

protected:
    int m_servernums;
    std::vector<QuickdbAdapter> m_sender;
};

class QdbWriter
{
public:
	QdbWriter(const char *addr);
	~QdbWriter();
	int put(const char *url, const char *xml, size_t xml_len);
    int put_by_docId(const gDocID_t &docid, const char *xml, size_t xml_len);
	int del(const char *url);
    int del_by_docId(const gDocID_t &docid);
    int get(const char *key,std::string &xml);
    
private:
	std::vector<QdbSender*>	m_sender;  
};
#endif
