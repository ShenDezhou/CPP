#ifndef PAGE_STORE_HPP_INCLUDED
#define PAGE_STORE_HPP_INCLUDED

#pragma once

#include <string>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <vector>
#include "xmlpage.hpp"
#include "sender.hpp"
#include "summarySender.hpp"
#include "string_Util.h"
#include "Platform/bchar.h"
#include "Platform/bchar_cxx.h"
#include "Platform/bchar_utils.h"
#include "Platform/bchar_stl.h"

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

class PageStore
{
public:
	PageStore(
		std::string file_name,
		int index,
		int max_linenum
	);

	~PageStore();
	bool WritePage(const XmlPage &page);
	bool SendPage(SummarySender *summary_sender);
	bool SendPage(Sender *index_sender);
	bool ValidDoc(string doc);

//private:
public:
	std::ofstream m_file;
	string m_filename;
	int m_index;
	int m_linenum;
	int m_max_linenum;
	pthread_mutex_t m_lock;
};

#endif//PAGE_STORE_HPP_INCLUDED

