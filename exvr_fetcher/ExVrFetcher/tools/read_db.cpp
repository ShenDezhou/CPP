#include <iostream>
#include <Platform/log.h>
#include <string>
#include "../QdbWriter.hpp"

using namespace std;

SS_LOG_MODULE_DEF(exvr_fetcher_server);

int main(int argc, char *argv[])
{
	if(argc != 3) {
		cout << "input error, \nusage: ./read_db 127.0.0.1:9111(qdb address) 9506fc846d506fc8-46db99506fc846d0(docid)" << endl;
		exit(-1);
	}

	string qdbAddress(argv[1]);
	string docID(argv[2]);

	QdbWriter dbWriter(qdbAddress.c_str());

	string xml;
	int ret = dbWriter.get(docID.c_str(), xml);
	if(ret == 0)
		cout << "get doc succed" << endl;
	else
		cout << "get doc failed" << endl;
	return 0;
}
