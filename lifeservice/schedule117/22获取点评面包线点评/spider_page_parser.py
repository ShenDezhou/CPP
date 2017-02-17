#!/usr/bin/env python
#coding=utf-8

import zlib
try:
	import pylibozzy_init
except Exception, e:
	pass
from pylibozzy.libcommon.libfilesys import ContinuousFile
from pylibozzy.libcommon.timelogger import LOG_STDERR_NOTICE

def make_fake_error_spider_data(url, error_reason="HTTP 404"):
	fake_page = """%s
Version: 1.2
Type: deleted
Fetch-Time: Mon Jun 20 18:55:20 2011
Spider-Version: 4.3.5
User-Agent: Sogou inst spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)
Spider-Group: 0
Spider-Address: 10.10.64.51
Client: 16
Error-Reason: %s

""" % (url, error_reason)
	return fake_page
	

def make_fake_spider_data(url, page):
	page_size = len(page)
	fake_page = """%s\r
Version: 1.2\r
Type: normal\r
Fetch-Time: Mon Jun 20 18:55:20 2011\r
Original-Size: %d\r
Store-Size: %d\r
Spider-Version: 4.3.5
User-Agent: Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.7) Gecko/2009031915 Gentoo Firefox/3.0.7
Spider-Group: 0
Spider-Address: 10.10.64.51
Client: 16
IP-Address: 220.181.54.18
Digest: AB1023F5588012CA96CA7A04BD2B3C24
\r
HTTP/1.0 200 OK\r
Date: Mon, 20 Jun 2011 10:55:21 GMT\r
Server: Apache\r
Expires: Mon, 26 Jul 1997 05:00:00 GMT\r
Last-Modified: Mon, 20 Jun 2011 10:55:21 GMT\r
Cache-Control: no-store, no-cache, must-revalidate\r
Pragma: no-cache\r
Content-Encoding: gzip\r
Vary: Accept-Encoding,User-Agent\r
Content-Type: text/html; charset=utf-8\r
X-Cache: MISS from web3.edeng.cn\r
Via: 1.1 web3.edeng.cn:80 (squid)\r
Powered-By-ChinaCache: MISS from CHN-BJ-D-38N\r
Connection: close\r
\r
%s\r
""" % (url, page_size, page_size, page)
	return fake_page

def get_part_block(prefix, part_l, url_get_dict=None, url_pass_dict=None, compress=True):
	fp = ContinuousFile(prefix)
	part_dict = dict.fromkeys(part_l, 0)
	part_list = []
	total_size = 0
	bl = 0

	line = ""
	line_before = ""

	info_dict = {}

	skip = False
	if url_get_dict:
		skip = True

	while True:
		line_before = line
		line = fp.readline()
		if len(line) == 0:
			break
		line = line.strip()

		if line.startswith("Version:"):
			info_dict.clear()
			#part_dict = dict.fromkeys(part_dict.keys(), 0)
			part_list = []
			total_size = 0

			skip = False
			if url_get_dict:
				skip = True

			url = line_before
			
			if url_get_dict and url in url_get_dict:
				skip = False

			if url_pass_dict and url in url_pass_dict:
				skip = True

			info_dict["Url"] = url
			info_dict["Version"] = line[len("Version:"):].strip()
			continue

		ft = line.find(":")
		if ft != -1 :
			key = line[0:ft]
			value = line[ft+1:].strip()

			info_dict[key] = value
			if key == "Content-Type":
			#Original-Size: webpage, 5224;xmlpage, 7808;snapshot, 70634;
				c = value.split(";")
				for t in c:
					m = t.split(",")
					if len(m) != 2:
						continue
					size = int(m[1])
					total_size += size
					if size != 0:
						part_list.append((m[0].strip(), size))

		else:
			line_l = len(line)
			if line_l != 0 and line_l > 1024*1024:
				continue

			if skip and info_dict and "Url" in info_dict:
				fp.seek(total_size)
				fp.readline()
				continue
			elif "Url" in info_dict:
				for item in part_list:
					key, val = item
					if key not in part_dict:
						fp.seek(val)
					else:
						data = fp.read(val)
						try:
							if compress:
								data = zlib.decompress(data)
							info_dict[key] = data
						except Exception, e:
							LOG_STDERR_NOTICE("Error: %s %s", str(e), info_dict["Url"])

				fp.readline()

				part_list = []
				total_size = 0

				if info_dict and "Url" in info_dict:
					yield info_dict
	fp.close()

def get_block(prefix, url_get_dict=None, url_pass_dict=None, compress=True):

	fp = ContinuousFile(prefix)
	bl = 0
	size = 0

	line = ""
	line_before = ""

	info_dict = {}

	skip = False
	if url_get_dict:
		skip = True

	while True:
		line_before = line
		line = fp.readline()
		if len(line) == 0:
			break
		line = line.strip()

		if line.startswith("Version:"):
			info_dict.clear()
			size = 0
			skip = False
			if url_get_dict:
				skip = True

			url = line_before
			
			if url_get_dict and url in url_get_dict:
				skip = False

			if url_pass_dict and url in url_pass_dict:
				skip = True

			info_dict["Url"] = url
			info_dict["Version"] = line[len("Version:"):].strip()
			continue

		ft = line.find(":")
		if ft != -1:
			key = line[0:ft]
			value = line[ft+1:].strip()

			info_dict[key] = value
			if key == "Store-Size":
				size = int(value)

			if key == "Error-Reason":
				fp.readline()
				if not skip and info_dict and "Url" in info_dict:
					yield info_dict
				continue
		else:
			if len(line) == 0:
				bl += 1
			else:
				if bl == 1:
					info_dict["Status"] = line

			if bl == 2:
				bl = 0
				htmlstr = ""
				if skip and info_dict and "Url" in info_dict:
					fp.seek(size)
					fp.readline()
					continue
				else:
					compresseddata = fp.read(size)
					try:
						if compress:
							info_dict["HtmlPage"] = zlib.decompress(compresseddata)
						else:
							info_dict["HtmlPage"] = compresseddata
					except Exception, e:
						LOG_STDERR_NOTICE("Error: %s", str(e))		

					fp.readline()

				if info_dict and "Url" in info_dict:
					yield info_dict
	fp.close()

def test():
	#for info_dict in get_part_block("xxxxx", ["snapshot"], con = False):
	for info_dict in get_part_block("xxxxx", ["snapshot"]):
		"""
		for i in info_dict:
			if i == "HtmlPage":
				continue
			print i,":", info_dict[i]
		"""
		print info_dict["Url"]
		print "-" * 20
	

if __name__ == "__main__":
	test()
