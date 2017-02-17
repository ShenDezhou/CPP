#!/bin/python
#coding=gb2312
# Creator : liubing@sogou-inc.com


# read content of file to line
def readFileToLine(file):
	content = ''
	for line in open(file):
		content += line.strip('\n')
	return content



# read key list to key-index dict
def loadKeyIndexMap(keyList):
	keyIndexMap = dict()
	for idx in xrange(0, len(keyList)):
		keyIndexMap[keyList[idx]] = idx
	return keyIndexMap

