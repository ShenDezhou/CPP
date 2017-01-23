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
#include "../QdbWriter.hpp"

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

int deleteItem(const Options &options, string &docID, int circle)
{
	if(options.DBAddress.size() < circle+1)
		return -1;
	Sender sender(options.normalReaderAddress.c_str());
	Sender mhSender(options.multihitReaderAddress.c_str());
	QdbWriter *qdb_writer = new QdbWriter(options.DBAddress[circle].c_str());
	string xml;
	int ret = -1;
	ret = qdb_writer->get(docID.c_str(), xml);
	if(ret == 0) {
		string::size_type pos_begin, pos_end;
		int classID;
		pos_begin = xml.find("<classid>");
		pos_end = xml.find("</classid>");
		if(pos_begin!=string::npos && pos_end!=string::npos) {
			string classIDStr = xml.substr(pos_begin+strlen("<classid>"), pos_end-pos_begin-strlen("<classid>"));
			classID = atoi(classIDStr.c_str());
		}
		if((pos_begin=xml.rfind("<status>")) != string::npos) {
			xml.replace(pos_begin+strlen("<status>"), 6, string("DELETE"));
		} else if((pos_begin=xml.find("</item>")) != string::npos) {
			xml.insert(pos_begin, "<status>DELETE</status>");
		}
		int delete_ret;
		if(classID >= 70000000)
			delete_ret = mhSender.send_to(docID.c_str(), xml.c_str(), xml.length());
		else
			delete_ret = sender.send_to(docID.c_str(), xml.c_str(), xml.length());
		if(delete_ret == 0) {
			fprintf(stderr, "delete doc(%s) succeed.\n", docID.c_str());
			return 0;
		} else {
			fprintf(stderr, "delete doc(%s) failed.\n", docID.c_str());
		}
	} else {
		fprintf(stderr, "get doc(%s) failed.\n", docID.c_str());
	}
	return -1;
}

SS_LOG_MODULE_DEF(exvr_fetcher_server);


/**
  *从qdb里按docid读出某个doc，然后将其删除
  *
  */
int main(int argc, char *argv[])
{
	if(argc != 4) {
		fprintf(stderr, "input error, usage: ./clean_db clean_db.cfg docid(ff2db8ba95f2db8b-a95dbff2db8ba950) circle\n");
		exit(-1);
	}


	string configFileName(argv[1]);
	Options options;
	getOptions(configFileName, options);
	string docID(argv[2]);

	for(int i=0; i<options.DBAddress.size(); ++i)
		cout << options.DBAddress[i] << endl;
	fprintf(stderr, "%s\n%s\n", options.normalReaderAddress.c_str(), options.multihitReaderAddress.c_str());


	int circle = atoi(argv[3]);
	deleteItem(options, docID, circle);

	return 0;
}
