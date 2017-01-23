#include "config.hpp"
#include "offdb/OffDB.h"
#include "offdb/QuickdbAdapter.h"
#include "Platform/docId/docId.h"
#include <Platform/log.h>
#include "reader_log.h"
#include "Parser.h"

struct Configuration {
	bool if_dump_page;
	std::string src_address, des_address, mode, key_type, list, dump_file,op_type;
	int cnt,del_classno;
};

bool read_config(char const* config_file, struct Configuration &opt)
{
	config cfg(config_file,"XMLDB");

	if ( !cfg.read_value("SRC_ADDRESS", opt.src_address) || opt.src_address.empty())
		throw std::runtime_error("SRC_ADDRESS error");
	if ( !cfg.read_value("DES_ADDRESS", opt.des_address) || opt.des_address.empty())
		reader_log_error("DES_ADDRESS error\n");
	if ( !cfg.read_value("READ_MODE", opt.mode) || opt.mode.empty() )
		throw std::runtime_error("READ_MODE error");
	if ( opt.mode == "rand" ) 
	{
		opt.op_type = "get";
	    cfg.read_value("OP_TYPE",opt.op_type);
		if ( !cfg.read_value("KEY_TYPE", opt.key_type) || opt.key_type.empty() )
			throw std::runtime_error("KEY_TYPE error");
		if ( opt.key_type == "url" )
			cfg.read_value("URL_LIST", opt.list);
		else if ( opt.key_type == "docid" )
			cfg.read_value("DOCID_LIST", opt.list);
		if ( opt.list.empty())
			throw std::runtime_error("LIST file is error");
	}
	else if(opt.mode == "batch_del"){
		opt.del_classno = -1;
		cfg.read_value("DEL_CLASSNO",opt.del_classno);
	}
	if (!cfg.read_value("DUMP_FILE", opt.dump_file) || opt.dump_file.empty())
		throw std::runtime_error("DUMP_FILE error");
	opt.if_dump_page = false;
	cfg.read_value("IF_DUMP_PAGE", opt.if_dump_page);
	opt.cnt=10*10000*10000;
	cfg.read_value("CNT",opt.cnt);
	return true;
}

void run(struct Configuration &opt) 
{
	QuickdbAdapter qdb, des_qdb;
	int ret;
	void *key,*val;
	size_t key_length,val_length;
	gDocID_t docid;
	FILE* dump_file(NULL);
	bool if_put=false;
	if ( !opt.des_address.empty() && des_qdb.open(opt.des_address.c_str()) == 0)
		if_put = true;
	if ( opt.if_dump_page ){
		dump_file = fopen(opt.dump_file.c_str(),"w");
		if ( dump_file == NULL ){
			reader_log_error("open dump file %s error.\n",opt.dump_file.c_str());
			exit(1);
		}
	}
	if ( opt.mode == "rand" )
	{
		if (qdb.open(opt.src_address.c_str())!=0)
			if (qdb.reconnect(3) !=0 ) 
			{
				reader_log_error("error: XMLDB %s dsiconnected.\n", opt.src_address.c_str());
				return;
			}
			
		FILE* fp = fopen(opt.list.c_str(), "r");
		char str[1024];
		if (fp == NULL)
		{
			reader_log_error("error: can't open %s list file %s", opt.key_type.c_str(), opt.list.c_str());
			return ;
		}
		key_length = sizeof(gDocID_t);
		key = malloc(key_length);
		while(fgets(str,1024,fp))
		{
			str[strlen(str)-1]='\0';		
			if (opt.key_type == "url"){
				url2DocId(str, &docid);
		//		fprintf(docid_file,"%llx%llx\n",docid.id.value.value_high,docid.id.value.value_low);
			}
			else
				sscanf(str,"%llx-%llx",&docid.id.value.value_high,&docid.id.value.value_low);
//				memcpy((void*)&docid, str, sizeof(gDocID_t));
			memcpy(key,(void*)&docid, key_length);
			if(opt.op_type == "get"){
				ret = qdb.get(key, key_length, val, val_length);
				reader_log_error("Get: ret:%d,errno:%d,key:%016llx-%016llx.\n",ret,errno,docid.id.value.value_high,docid.id.value.value_low);
			}
			else if(opt.op_type == "del"){
				ret = qdb.del(key,key_length,0); 
				reader_log_error("Del: ret:%d,key:%016llx-%016llx.\n",ret,docid.id.value.value_high,docid.id.value.value_low);
			}
			if (ret == 0)
			{
				if (opt.if_dump_page)
					fprintf(dump_file,"len:%d\n%s\n\n",strlen((char*)val),(char*)val);
				if ( if_put )
				{			
					int des_ret = des_qdb.put(key, key_length,val, val_length,0);
					reader_log_error("PUT: ret:%d,key:%016llx-%016llx.\n",des_ret,docid.id.value.value_high,docid.id.value.value_low);
				}
				if(opt.op_type == "get")
					free(val);
			}

		}
		free(key);
		fclose(fp);
	}
	else if (opt.mode == "sequence" )
	{
		FILE* docid_file = fopen("docid.list","w");
		if (qdb.openc(opt.src_address.c_str())!=0)
		{
			reader_log_error("error: XMLDB %s dsiconnected.\n", opt.src_address.c_str());
			return;
		}
		unsigned int off;
		void *value;
		size_t value_length;
		int cnt(0);
		while(1)
		{
			ret = qdb.next(key, key_length, value, value_length, &off);
			if ( ret > 0 )
				break;
			else if ( ret == 0 )
			{
				memcpy((void*)&docid,key,key_length);
				fprintf(docid_file,"%016llx-%016llx\n",docid.id.value.value_high,docid.id.value.value_low);
				if ( opt.if_dump_page )
					fprintf(dump_file,"%s\n\n", (char*)value);
				if (if_put)
				{
					int des_ret = des_qdb.put(key, key_length, value, value_length,0);
					reader_log_error("PUT: ret:%d,key:%016llx-%016llx.\n",des_ret,docid.id.value.value_high,docid.id.value.value_low);
				}
				free(value);
				cnt++;
				if (cnt%500000==0)reader_log_error("scan %d records.\n",cnt);
				if(cnt==opt.cnt){reader_log_error("scan %d records.\n",cnt);break;}
			}
			else reader_log_error("Error: invalid data.\n");
			free(key);
		}
		fclose(docid_file);
	}
	else if(opt.mode == "batch_del"){
		if (qdb.openc(opt.src_address.c_str())!=0)
		{
			reader_log_error("error: XMLDB %s dsiconnected.\n", opt.src_address.c_str());
			return;
		}
		unsigned int off;
		void *value;
		size_t value_length;
		int cnt(0);
		while(1)
		{
			ret = qdb.next(key, key_length, value, value_length, &off);
			if ( ret > 0 )
				break;
			else if ( ret == 0 )
			{
				if ( opt.if_dump_page )
					fprintf(dump_file,"%s\n\n", (char*)value);
				XmlDoc *doc = new XmlDoc;
				if(!Parse((char*)value,value_length,doc)){
					delete doc;
					free(key);
					free(value);
					continue;
				}
				if(doc->classno == opt.del_classno){
					int des_ret = des_qdb.del(key, key_length,0);
					memcpy((void*)&docid,key,key_length);
					reader_log_error("del: ret:%d,key:%016llx-%016llx.\n",des_ret,docid.id.value.value_high,docid.id.value.value_low);		
				}
				delete doc;
				free(value);
			}
			else reader_log_error("Error: invalid data.\n");
			free(key);
		}
	}
	else reader_log_error("error qdb read mode.\n");
	if (dump_file)
		fclose(dump_file);
	return ;
}

SS_LOG_MODULE_DEF(xmlreader_server);

int main(int argc, char** argv)
{
	char const * config_filename = (argc > 1) ? argv[1] : "reader.conf";
	Configuration opt;
	try {
		read_config(config_filename, opt);
		run(opt);
	}
	catch (std::runtime_error& e)
	{
		reader_log_error("runtime error: %s.\n", e.what());
	}
	catch (...)
	{
		reader_log_error("Unknown error.\n");
	}
	reader_log_error("reader XMLDB complete.\n");
	
	return 0;
}

