#! /usr/bin/python

import pylibozzy_init
from pylibozzy.libparse.spider_page_parser import get_part_block
from lxml import etree
import sys

#prefix = "/search/odin/PageDB/GetPage/tmp/url_aapages"
prefix = sys.argv[1]

#STOP_WORD = [u'\u3000', u'?', u'\u2001', u'\u0020']

def get_site(url):
	url_parts = url.split("/")
	if len(url_parts) < 3:
		return None
	return url_parts[2]

site_data_dic = {}



for info_dict in get_part_block(prefix, ["xmlpage"]):
	if "Url" not in info_dict:
		continue
	url = info_dict["Url"]
	xmlpage = info_dict["xmlpage"]
	xmlpage = xmlpage.decode("utf16")
	
	crumb_begin =  xmlpage.find("<breadcrumb>")
	crumb_end = xmlpage.find("</breadcrumb>", crumb_begin)
	crumb = ''
	if crumb_begin > 0 and crumb_end > crumb_begin:
		crumb = xmlpage[crumb_begin + len("<breadcrumb>"):crumb_end]
	print ('%s\t%s' % (url, crumb)).encode('gbk', 'ignore')
