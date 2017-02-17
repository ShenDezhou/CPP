#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-03-31 20:13
# * Filename	 : JsonTool.py
# * Description	 : json相关
# * *****************************************************************************/

import simplejson as json
import copy, re


class JsonToLine(object):

	def __init__(self, keyConf, encoding='gb2312'):
		self.kvDict = dict()
		self.keyList = []
		self.keyConf = keyConf
		self.encoding = encoding
		self.loadKeyConf()
		self.result = []
	
	def toStr(self, input):
		output = ''
		if isinstance(input, unicode):
			output = input.encode(self.encoding, 'ignore')
		else:
			output = str(input)
		return re.sub('[\r\n\t]', '', output)
	


	def loadKeyConf(self):
		for line in open(self.keyConf):
			if len(line) == 0 or line.startswith("#"):
				continue
			self.keyList.append(line.strip('\n'))

	def createInsertKey(self, key, path):
		if len(path) == 0:
			return key
		return '%s:%s' % (path, key)

	
	def insert(self, key, value):
		value = value.strip()
		if key not in self.kvDict:
			self.kvDict[key] = value
		else:
			if isinstance(self.kvDict[key], list):
				self.kvDict[key].append(value)
			else:
				existValue = self.kvDict[key]
				self.kvDict[key] = []
				self.kvDict[key].append(existValue)
				self.kvDict[key].append(value)


	def toLineImp(self, jsonObj, path, kvDict):
		if isinstance(jsonObj, list):
			for item in jsonObj:
				self.toLineImp(item, path, copy.deepcopy(kvDict))
		elif isinstance(jsonObj, dict):
			listKeyList = []
			for key in jsonObj:
				value = jsonObj[key]
				if isinstance(value, list):
					listKeyList.append(key)
				else:
					insertKey = self.createInsertKey(key, path)
					kvDict[insertKey] = value
			if len(listKeyList) == 0:
				#for k in kvDict:
				#	print k, kvDict[k].encode('gb2312', 'ignore')

				valueList = []
				for key in self.keyList:
					value = "";
					if key in kvDict:
						value = kvDict[key]
					valueList.append(value)
				self.result.append("\t".join(self.toStr(v) for v in valueList))
				#print ("\t".join(valueList)).encode('gb2312', 'ignore')
				return

			for key in listKeyList:
				self.toLineImp(jsonObj[key], self.createInsertKey(key, path), copy.deepcopy(kvDict))

	def format(self):
		for key in self.kvDict:
			print key, len(self.kvDict[key])#.encode('gb2312', 'ignore')


	# 针对每行都是一个json对象字符串的文件
	def toLine(self, jsonFile):
		for line in open(jsonFile):
			path = ''
			kvDict = dict()
			jsonObj = json.loads(line)
			self.toLineImp(jsonObj, path, kvDict)
			#self.format()

	def parse(self, jsonObj):
		self.result = []
		path = ''
		kvDict = dict()
		self.toLineImp(jsonObj, path, kvDict)
		return self.result


#jsonToLineTool = JsonToLine('nuomi_conf')
#jsonToLineTool.toLine('/search/liubing/spiderTask/result/system/task-130/1459781105197.head')


