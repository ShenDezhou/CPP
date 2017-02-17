#!/bin/python
#coding=gb2312
import re, sys

scanUrls = 'Scan/data/dianping_scan_list_urls.head'

shopUrlRegex = 'http://www.dianping.com/shop/[0-9]+'

shopUrlDict = dict()

if len(sys.argv) > 1:
	scanUrls = sys.argv[1]

for line in open(scanUrls):
	url = line.split('\t')[0]
	# dianping shop urls
	match = re.match(shopUrlRegex, url)
	if match:
		shopUrl = match.group(0)
		if shopUrl not in shopUrlDict:
			shopUrlDict[shopUrl] = 1
			print shopUrl
