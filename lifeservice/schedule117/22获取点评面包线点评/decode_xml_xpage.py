#! /usr/bin/python

import pylibozzy_init
from pylibozzy.libparse.spider_page_parser import get_part_block
from lxml import etree

#file_xml_page = open("xml_page", 'w')

prefix = "./tmp/pages"
prefix = "/search/odin/PageDB/GetPage/tmp/url_aapages"
STOP_WORD = [u'\u3000', u'?', u'\u2001', u'\u0020']

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
#	xmlpage = info_dict["xmlpage"].decode("utf16").encode("utf8")
	xmlpage = info_dict["xmlpage"]
	xmlpage = xmlpage.decode("utf16")

	print xmlpage.encode('gbk','ignore')
	sys.exit(1)

	content_title_tb = xmlpage.find("<content-title>")
	content_title_te = xmlpage.find("</content-title>")
	content_title = xmlpage[content_title_tb+len("<content-title>"):content_title_te]
	content_tb =  xmlpage.find("<major>")
	content_te = xmlpage.find("</major>", content_tb)
	content = xmlpage[content_tb+len("<major>"):content_te]
#	if content_title_tb < 0 or content_title_te < 0 or content_tb < 0 or content_te < 0:
#		print url + "^^^^None"
#		continue
	if content_title_tb < 0 or content_title_te < 0:
		l1 = None
	else:
		l1 = content_title.encode("utf-8")

	if  content_tb < 0 or content_te < 0:
		l2 = None
	else:
		l2 = content.encode("utf-8")
	print url + "^^^^" + str(l1) + "^^^^" + str(l2)	
#	print url + "^^^^" + content_title.encode("utf-8") + "^^^^" + content.encode("utf-8")
