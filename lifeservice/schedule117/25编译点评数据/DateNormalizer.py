#!/bin/python
#coding=gb2312

import re

def loadRegexConf(conf):
	dateRegexList = []
	for line in open(conf):
		line = line.strip('\n')
		if len(line) == 0 or line.startswith('#'):
			continue
		if line not in dateRegexList:
			dateRegexList.append(line)
	return dateRegexList


def testExtractDate(data, conf):
	regexList = loadRegexConf(regexConf)
	for line in open(data):
		line = line.strip('\n')
		match = False
		for regex in regexList:
			searchGroup = re.search(regex, line)
			if searchGroup is None:
				continue
			else:
				len = searchGroup.groups()
				start, end = '', ''
				if len >= 2:
					start = searchGroup.group(1)
				if len >= 3:
					end = searchGroup.group(2)
				print 'Y\t%s\t%s\t%s\t%s' % (start, end, line, regex)

				match = True
				break

		if not match:
			print 'N\t%s' % line


regexConf = "conf/date_extract_regex_conf"

testData = "timeRow.data"



def main():
	testExtractDate(testData, regexConf)


main()
