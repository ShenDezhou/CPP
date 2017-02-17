#!/bin/python
#coding=gb2312

import sys, re
import logging

from keyConfig import *
from DateTimeTool import DateTimeTool as dateTool

class NuomiTuanParser:

	# load conf file, init log file
	def __init__(self, conf):
		self.BEGIN_TAG = '<url>'
		self.END_TAG = '</url>'
		self.RANGE_KEY = 'range'
		self.TUAN_KEY = 'tuan'
		self.SHOP_KEY = 'shop'
		self.CITY_KEY = 'city'
		self.ID_KEY = 'id'
		self.TUANID_KEY = 'tuanid'
		self.POI_KEY = 'poi'
		self.LNG_KEY = 'long'
		self.LAT_KEY = 'lat'
		self.CATEGORY_KEY = 'firstCategory'
		self.SUBCATEGORY_KEY = 'secondCategory'
		self.STARTTIME = 'startTime'
		self.DEADLINE_KEY = 'deadline'
		self.sep = '\t'

		self.confMap = dict()
		self.loadConfFile(conf)
	
		self.tuanFD = ''
		self.shopFD = ''

		# config log format
		today = dateTool.today()
		logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='log/NuomiTuanParser_' + today + '.log',
                filemode='w+')
		logging.info('init NuomiTuanParser')
		

	def loadConfFile(self, conf):
		for line in open(conf):
			line = line.strip('\n')
			if len(line) == 0 or line.startswith('#'):
				continue
			segs = line.split('\t')
			if len(segs) != 3:
				continue
			type, key, regex = segs[0], segs[1], segs[2]
			if type not in self.confMap:
				map = dict()
				self.confMap[type] = map
			self.confMap[type][key] = regex


	def printTuanInfo(self, infoMap):
		tuanInfoList = []
		for key in tuanKey:
			value = ''
			if key in infoMap:
				value = infoMap[key]
			tuanInfoList.append(value)
		# output
		self.tuanFD.write("%s\n" % self.sep.join(tuanInfoList))
		#print self.sep.join(tuanInfoList)


	def printShopInfo(self, shopInfoList):
		for shop in shopInfoList:
			shopInfoList = []
			for key in shopKey:
				value = ''
				if key in shop:
					value = shop[key]
				shopInfoList.append(value)
			self.shopFD.write("%s\n" % self.sep.join(shopInfoList))
			#print self.sep.join(shopInfoList)


	def stripValue(self, value):
		value = value.strip()
		value = re.sub('\t', '  ', value)
		return value

	def getRegexValue(self, regex, content):
		findList = re.findall(regex, content)
		if len(findList) == 0:
			return ''
		value = findList[0]
		return self.stripValue(value)


	def normalValue(self, key, value):
		try:
			if key == self.DEADLINE_KEY or key == self.STARTTIME:
				value = dateTool.sec2time(value)
		except:
			pass
		return value



	def extractTuanInfo(self, content):
		tuanInfoMap = dict()
		# extract tuan info
		if self.TUAN_KEY not in self.confMap:
			logging.error('[%s] type is not in config' % self.TUAN_KEY)
			return
		if self.RANGE_KEY not in self.confMap[self.TUAN_KEY]:
			logging.error('[%s] key is not in tuan config' % self.RANGE_KEY)
			return
		rangeRegex = self.confMap[self.TUAN_KEY][self.RANGE_KEY]
		rangeContent = self.getRegexValue(rangeRegex, content)
		for key in self.confMap[self.TUAN_KEY]:
			if key == self.RANGE_KEY:
				continue
			regex = self.confMap[self.TUAN_KEY][key]
			value = self.getRegexValue(regex, rangeContent)
			tuanInfoMap[key] = self.normalValue(key, value)
		return tuanInfoMap


		
	# extract shop info
	def extractShopInfo(self, content):
		shopInfoList = []
		if self.SHOP_KEY not in self.confMap:
			logging.error('[%s] type is not in config' % self.SHOP_KEY)
			return
		if self.RANGE_KEY not in self.confMap[self.SHOP_KEY]:
			logging.error('[%s] key is not in shop config' % self.RANGE_KEY)
			return
		rangeRegex = self.confMap[self.SHOP_KEY][self.RANGE_KEY]
		# 1 tuan  .vs. n shop
		for rangeContent in re.findall(rangeRegex, content):
			shopMap = dict()
			for key in self.confMap[self.SHOP_KEY]:
				if key == self.RANGE_KEY:
					continue
				regex = self.confMap[self.SHOP_KEY][key]
				value = self.getRegexValue(regex, rangeContent)
				shopMap[key] = value
				#print "shop", key, value
			shopInfoList.append(shopMap)
		return shopInfoList


	def parseImp(self, content):
		# extract tuan info & shop info
		tuanInfoMap = self.extractTuanInfo(content)
		shopInfoList = self.extractShopInfo(content)
		# add city/id/poi/category of tuan info for shop
		id, city, category, subCategory = '', '', '', ''
		if self.ID_KEY in tuanInfoMap:
			id = tuanInfoMap[self.ID_KEY]
		if self.CITY_KEY in tuanInfoMap:
			city = tuanInfoMap[self.CITY_KEY]
		if self.CATEGORY_KEY in tuanInfoMap:
			category = tuanInfoMap[self.CATEGORY_KEY]
		if self.SUBCATEGORY_KEY in tuanInfoMap:
			subCategory = tuanInfoMap[self.SUBCATEGORY_KEY]
		for shop in shopInfoList:
			shop[self.TUANID_KEY] = id
			shop[self.CITY_KEY] = city
			shop[self.CATEGORY_KEY] = category
			shop[self.SUBCATEGORY_KEY] = subCategory

			if self.LNG_KEY in shop and self.LAT_KEY in shop:
				shop[self.POI_KEY] = shop[self.LAT_KEY] + ',' + shop[self.LNG_KEY]
		# print tuan info & shop infos
		self.printTuanInfo(tuanInfoMap)
		self.printShopInfo(shopInfoList)


	def parse(self, file):
		city = re.sub('\..*', '', file)
		city = re.sub('.*/', '', city)
		tuanOutput = 'data/' + city + '_tuan'
		shopOutput = 'data/' + city + '_shop'
		self.tuanFD = open(tuanOutput, 'w')
		self.shopFD = open(shopOutput, 'w')
		# 打印表头
		self.tuanFD.write("%s\n" % self.sep.join(tuanKey))
		self.shopFD.write("%s\n" % self.sep.join(shopKey))
		
		content = ''
		for line in open(file):
			line = line.strip()
			if re.search(self.BEGIN_TAG, line) is not None:
				content = line
			elif re.search(self.END_TAG, line) is not None:
				self.parseImp(content)
			else:
				content += line

		self.tuanFD.close()
		self.shopFD.close()


if __name__ == '__main__':
	opt = sys.argv[1]
	if opt == '-nuomi_tuan':
		conf, input = sys.argv[2], sys.argv[3]
		parser = NuomiTuanParser(conf)
		parser.parse(input)





