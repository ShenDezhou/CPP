#! /usr/bin/python
#coding:gbk

import sys 
import re
import json
import urllib2


for line in sys.stdin:
	try:
		line = line.strip()
		line_parts = line.split("\t")
		if len(line_parts) != 4:
			continue
		lemmaIdEnc = line_parts[3][11:]
		mod_time = line_parts[2][9:]
		pv = -1
		pv_json_parse = urllib2.urlopen("http://baike.baidu.com/api/lemmapv?id="+lemmaIdEnc)
		pv_json = pv_json_parse.read()
		pv = pv_json[6:-1]
		print line_parts[0]+"\t"+mod_time+"\t"+pv+"\t"+lemmaIdEnc
	except Exception, e:
		print >> sys.stderr,e,line
		continue
