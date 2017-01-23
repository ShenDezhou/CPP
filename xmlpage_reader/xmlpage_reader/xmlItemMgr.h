#ifndef _XML_ITEM_MGR_H_
#define _XML_ITEM_MGR_H_

#include <stdio.h>
#include <string>
#include <string.h>
#include <map>
#include <vector>
#include <set>
#include <bitset>
#include <pthread.h>
#include "ace/Message_Block.h"

#include "xmlpage.hpp"
#include "reader_log.h"
#include "Utils.hpp"

struct SendFileRequest {
    unsigned int file_id;
};

struct SendFileHeader{
    uint32_t file_size;
    uint32_t file_id;
    uint32_t flag;
};

enum SEND_FILE_TYPE
{
    SEND_ITEM = 1,
    SEND_INDEX,
    SEND_INIT_FILE_ID,
    SEND_RESPONSE
};

struct ItemHeader {
	unsigned int file_id;
	unsigned int item_time;
	unsigned int content_len;
	unsigned short item_idx;
	unsigned short doc_id_len;
	unsigned short url_len;
	unsigned short status;
};


class ItemData {
	public:
		ItemData() { use_times=0; clear(); }
		~ItemData() {}
		void clear()
		{
			memset(&header, 0, sizeof(header));
			if(use_times++ > 100)
			{
				if(doc_id.capacity()>64) doc_id.reserve(64);
				if(url.capacity()>256) url.reserve(256);
				if(content.capacity()>102400) content.reserve(4096);
				use_times = 0;
			}
		}
	public:
		struct ItemHeader header;
		std::string doc_id;
		std::string url;
		std::string content;
		int use_times;
};

class XmlItemMgr {
	public:
		XmlItemMgr()
		{
			pthread_rwlock_init(&m_rwlock, NULL);
			fp = NULL;
            fp_index = NULL;
            fp_recv = NULL;
			MAX_ITEM_COUNT = 10000;
			base_dir = ".";
            index_pos = 0;
            is_send_file = false;
		}
		~XmlItemMgr()
		{
			if(fp != NULL) fclose(fp);
			fp = NULL;
            if (fp_index != NULL) fclose(fp_index);
            fp_index = NULL;
			pthread_rwlock_destroy(&m_rwlock);
		}
		int clear();
		int init();
		int save(XmlPage *page);
		int set_min_file(int min_id, int min_index);
		int read(ItemData *data, unsigned int file_id, unsigned int item_index);
        void save_file(char *file_name);
        void success_send_file(unsigned int file_id);
        int start_recv_file(SEND_FILE_TYPE type, unsigned int file_id);
        int recv_data(void *buf, int size, unsigned int file_id);
        int end_recv_file(SEND_FILE_TYPE type, unsigned int file_id);
        int save_send_file_id(unsigned int file_id);
        int save_now_file_id(unsigned int file_id);
        int check_time(int frequency);
        int init_recv_reader_file_id(unsigned int file_id);
	private:
		int _save(XmlPage *page);
		int _init_file_id();
		
	public:
		unsigned int MAX_ITEM_COUNT;
		
		unsigned int min_file_id;
		unsigned int now_file_id;
		unsigned int now_item_idx;

		unsigned int init_file_id;
		unsigned int send_file_id;
        bool is_send_file;
		unsigned int recv_file_id;
        bool is_recv_file;
		
		std::vector<ItemData *> v_items;
		
		std::string base_dir;
		FILE *fp;
        FILE *fp_index;
        FILE *fp_recv;
        
        int index_pos;
		
		pthread_rwlock_t m_rwlock;
};

extern XmlItemMgr g_xmlItemMgr;
extern const char* g_types[];

#endif

