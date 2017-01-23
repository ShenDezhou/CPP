#include <string>
#include <set>
#include <iostream>
#include <offdb/QuickdbAdapter.h>
#include <Platform/docId/docId.h>
#include <Platform/log.h>
#include <boost/algorithm/string.hpp>

#include "../Configuration.hpp"
#include "../database.h"
#include "../Sender.hpp"

#define MAX_DOCID_BUFF_LEN 35

using namespace std;

class Options {
public:
	string mysqlAddress;
	string mysqlUserName;
	string mysqlPassword;
	string mysqlDB;

	vector<string> DBAddress;

	string normalReaderAddress;
	string multihitReaderAddress;
};

int getOptions(const string &configFileName, Options &options)
{
	ConfigurationFile config(configFileName.c_str());
	ConfigurationSection section;
	if(!config.GetSection("CleanDB", section)) {
		fprintf(stderr, "cannot find CleanDB config section\n");
		return -1;
	}

	options.mysqlAddress = section.Value<string>("mysqlAddress");
	options.mysqlUserName = section.Value<string>("mysqlUserName");
	options.mysqlPassword = section.Value<string>("mysqlPassword");
	options.mysqlDB = section.Value<string>("mysqlDB");
	
	int qdbNum = 0;
	qdbNum = section.Value<int>("qdbNum");
	if(qdbNum<=0)
		fprintf(stderr, "qdb number is less than 0\n");
	char buf[1024];
	for(int i=0; i<qdbNum; ++i) {
		snprintf(buf, 1024, "qdb_%d", i+1);
		options.DBAddress.push_back(section.Value<string>(buf));
	}

	options.normalReaderAddress = section.Value<string>("normalReaderAddress");
	options.multihitReaderAddress = section.Value<string>("multihitReaderAddress");

	return 0;
}

int scanMysql(const Options &options, set<string> &docIDs)
{
    CMysql mysql(options.mysqlAddress, options.mysqlUserName, options.mysqlPassword, options.mysqlDB, "set names gbk");
    int reconn = 0;
    while( reconn < 5 ){
        bool suc = true;
        if(mysql.isClosed()) {
            fprintf(stderr, "scanMysql: init fail\n");
            suc = false;
        }
        if(!mysql.connect()) {
            fprintf(stderr, "scanMysql: connect server fail: %s\n", mysql.error());
            suc = false;
        }
        if(!mysql.open()) {
            fprintf(stderr, "scanMysql: open database fail: %s\n", mysql.error());
            suc = false;
        }
        if( suc ){
            CMysqlExecute executor(&mysql);
            executor.execute("set names gbk");
            fprintf(stderr, "scanMysql: Mysql connect success\n");
            break;
        }
        reconn ++;
        mysql.close();
        mysql.init();
    }
    if( reconn >=5 ) {
        fprintf(stderr, "scanMysql: connect vr fail\n");
        return -1;
    }

    CMysqlQuery query(&mysql);
	char sql[1024];
	int ret = -1;
	char *docID;
	for(int i=0; i<4; ++i) {
		snprintf(sql, 1024, "select doc_id from vr_data_%d", i+1);
		ret = query.execute(sql);
		if(ret == -1) {
			fprintf(stderr, "scanMysql: %s failed\n", sql);
			return -1;
		}

		int count = query.rows();
		if(count < 1) {
			fprintf(stderr, "scanMysql: doc amount is zero, %s\n", sql);
			return -1;
		}

		int docNums = 0;
		for(int j=0; j<count; ++j) {
			CMysqlRow * row = query.fetch();
			if(!row) {
				fprintf(stderr, "scanMysql: get a row error, %s\n", mysql.error());
				return -1;
			}

			docID = row->fieldValue("doc_id");
			if(!docID || strlen(docID)<1) {
				fprintf(stderr, "scanMysql: docID error, %s\n", docID);
				return -1;
			}

			docIDs.insert(docID);
			docNums++;
		}

		fprintf(stderr, "scanMysql: read vr_data_%d compeleted, doc num: %d\n", i+1, docNums);
	}

	return 0;
}

int cleanDB(const Options &options, set<string> &docIDs)
{
	Sender sender(options.normalReaderAddress.c_str());
	Sender mhSender(options.multihitReaderAddress.c_str());
	int count = 0;
	void *key, *value;
	size_t key_len, value_len;
	gDocID_t docid;
	char DocIDBuffer[MAX_DOCID_BUFF_LEN];
	string::size_type pos_begin, pos_end;
	struct tm time_tm;
	time_t expireTime;
	string expireTimeStr;
	time_t now;
	bool isExpire;
	bool toDelete;
	int classID;
	for(int i=0; i<options.DBAddress.size(); ++i) {
		QuickdbAdapter m_qdb;
		if(m_qdb.open(options.DBAddress[i].c_str())) {
			fprintf(stderr, "cleanDB: open qdb failed, %s\n", options.DBAddress[i].c_str());
			return -1;
		}

		if(m_qdb.openc(options.DBAddress[i].c_str(), double_buffer_cursor)) {
			fprintf(stderr, "cleanDB: open qdb cursor failed, %s\n", options.DBAddress[i].c_str());
			return -1;
		}

		m_qdb.seek(0);
		while(1) {
			if(m_qdb.next(key, key_len, value, value_len, NULL) > 0) {
				break;

			} else {
				string item((const char *)value, value_len);
				boost::replace_all(item, "\n", "");
				memcpy(&docid, key, key_len);
				docid.c_str(DocIDBuffer, MAX_DOCID_BUFF_LEN);
				fprintf(stdout, "%s\t%s\n", DocIDBuffer, item.c_str());
				/*
				if(docIDs.find(DocIDBuffer) == docIDs.end()) {
					isExpire = false;
					pos_begin = item.find("<expire_time>");
					pos_end = item.find("</expire_time>");
					if(pos_begin!=string::npos && pos_end!=string::npos) {
						expireTimeStr = item.substr(pos_begin+strlen("<expire_time>"), pos_end-pos_begin-strlen("<expire_time>"));
						//fprintf(stderr, "cleanDB: %s\n", expireTimeStr.c_str());
						if(strptime(expireTimeStr.c_str()+4, "%b %d %H:%M:%S %Y", &time_tm)) {
							expireTime = mktime(&time_tm);
							now = time(NULL);
							if(now-expireTime >= 5*24*3600) {
								//fprintf(stdout, "cleanDB: %s are going to be deleted\n", DocIDBuffer);
								isExpire = true;
							}
						}
					}


					if(isExpire) {
						toDelete = false;
						pos_begin = item.find("<classid>");
						pos_end = item.find("</classid>");
						if(pos_begin!=string::npos && pos_end!=string::npos) {
							string classIDStr = item.substr(pos_begin+strlen("<classid>"), pos_end-pos_begin-strlen("<classid>"));
							fprintf(stdout, "cleanDB: classID:%s, expireTime:%s, docID:%s are going to be deleted\n", classIDStr.c_str(), expireTimeStr.c_str(), DocIDBuffer);
							classID = atoi(classIDStr.c_str());

							if((pos_begin=item.rfind("<status>")) != string::npos) {
								item.replace(pos_begin+strlen("<status>"), 6, string("DELETE"));
								toDelete = true;
							} else if((pos_begin=item.find("</item>")) != string::npos) {
								item.insert(pos_begin, "<status>DELETE</status>");
								toDelete = true;
							} else
								fprintf(stderr, "cleanDB: find status failed, %s\n", DocIDBuffer);


							//if(classID < 70000000)
								//toDelete = false;
						}

						//fprintf(stderr, "item:%s\n", item.c_str());

						int ret;
						if(toDelete) {
							if(classID >= 70000000)
								ret = mhSender.send_to(DocIDBuffer, item.c_str(), item.length());
							else
								ret = sender.send_to(DocIDBuffer, item.c_str(), item.length());
							if(ret == 0) {
								count++;
								fprintf(stdout, "cleanDB: %s has been deleted. item:%s\n", DocIDBuffer, item.c_str());
							} else
								fprintf(stderr, "cleanDB: it failed to delete %s\n", DocIDBuffer);
							usleep(10000);
						}
					}
				}
				*/

				if(key)
					free(key);
				if(value)
					free(value);
			}
		}

		m_qdb.close();
		m_qdb.closec();
	}

	return count;
}

SS_LOG_MODULE_DEF(exvr_fetcher_server);

int main(int argc, char *argv[])
{
	if(argc != 2) {
		fprintf(stderr, "input error, usage: ./clean_db clean_db.cfg\n");
		exit(-1);
	}

	string configFileName(argv[1]);
	Options options;
	getOptions(configFileName, options);

	fprintf(stderr, "%s\n%s\n%s\n%s\n", options.mysqlAddress.c_str(), options.mysqlUserName.c_str(), options.mysqlPassword.c_str(), options.mysqlDB.c_str());
	for(int i=0; i<options.DBAddress.size(); ++i)
		cout << options.DBAddress[i] << endl;
	fprintf(stderr, "%s\n%s\n", options.normalReaderAddress.c_str(), options.multihitReaderAddress.c_str());

	set<string> docIDs;
	if(scanMysql(options, docIDs) == -1) {
		fprintf(stderr, "read mysql error\n");
		exit(-1);
	}
	fprintf(stdout, "there are %ld docs in mysql\n", docIDs.size());

	int count = 0;
	count = cleanDB(options, docIDs);
	if(count < 0)
		fprintf(stderr, "clean db error\n");
	else
		fprintf(stdout, "%d docs has been deleted\n", count);

	return 0;
}
