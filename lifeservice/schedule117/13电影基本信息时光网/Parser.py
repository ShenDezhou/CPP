#!/bin/python
#coding=gbk

import sys, re
import logging
import copy
import simplejson as json

from keyConfig import *
from DateTimeTool import DateTimeTool as dateTool

class CtimeCinemaParser:
	infoRegex = '.*cinemaShowtimesScriptVariables = (.*)'
	
	filmInfoRegex = '.*<div class="filminfo">(.*?)<div>'
	
	# IMAX: ico_c_f01   3D: ico_c_f02  VIP:ico_c_f03  中国巨幕:ico_c_f04  DOBLY:ico_c_f06  4k:ico_c_f08  可停车：ico_c_f14
	# 可刷卡:ico_c_f12  WIFI:ico_c_f13  取票机：ico_c_f25
	IMAX_FLAG = 'ico_c_f01'
	VIP_FLAG = 'ico_c_f03'

	roomNumRegex = '.*<p class="lovetxt"><b>([0-9]+)</b>个影厅'
	seatNumRegex = '.*<p class="lovetxt"><b>([0-9]+)</b>个座位'
	businessTimeRegex = '.*营业时间：([^<]+)</span>'
	loveRateRegex = '.*<li class="lovepic">.*?<i>.*?"([0-9]+)".*?<li>'


	id, title, addr, tel = '', '', '', ''
	lat, lng, roomNum, seatNum = '', '', '', ''
	hasIMAX, hasVIP = False, False
	cityid, businessTime, rate = '', '', ''

	def __init__(self):
		# config log format
		today = dateTool.today()
		logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='logs/CtimeCinemaParser_' + today + '.log',
                filemode='a')
		logging.info('init CtimeCinemaParser')
		

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


	def printItemList(self, itemList, fd):
		for item in itemList:
			fd.write("%s\n" % item)

	
	# 根据key列表从map中获取对应val的数组
	def getValueList(self, keyList, kvMap):
		valueList = []
		for key in keyList:
			value = ''
			if key in kvMap:
				value = kvMap[key]
				valueList.append(value)
		return valueList

	# 是否list中元都为空
	def isListItemAllEmpty(self, list):
		for item in list:
			if len(item) != 0:
				return False
		return True


	def normalValue(self, value):
		value = value.strip()
		value = re.sub('\t', ' ', value)
		value = value.replace('<![CDATA[', '')
		value = value.replace(']]>', '')
		return value.strip()

	def getRegexValue(self, regex, content):
		findList = re.findall(regex, content)
		if len(findList) == 0:
			return ''
		value = findList[0]
		return self.normalValue(value)



	# 解析影院的座位数，提供的服务等信息
	def parseCinemaServiceInfo(self, content):
		#roomNum = self.getRegexValue(self.roomNumRegex, content)
		#seatNum = self.getRegexValue(self.seatNumRegex, content)
		self.businessTime = self.getRegexValue(self.businessTimeRegex, content)

		content = self.getRegexValue(self.filmInfoRegex, content)
		if content.find(self.IMAX_FLAG) != -1:
			self.hasIMAX = True
		if content.find(self.VIP_FLAG) != -1:
			self.hasVIP = True
	

	# 解析传入的JSON数据
	def parseShowtimeJson(self, line):
		matchGroup = re.match(self.infoRegex, line)
		if not matchGroup:
			return
		infoJson = matchGroup.group(1)
		cityidRegex = '.*"cityid":([0-9]+)'
		cinemaIdRegex = '.*"cinemaId":([0-9]+),'
		cinemaNameRegex = '.*"namecn":"([^"]+)"'
		cinemaAddrRegex = '.*"address":"([^"]+)"'
		cinemaTelRegex = '.*"telphone":"([^"]+)"'
		lngRegex = '.*"longitude":([0-9\.]+)'
		latRegex = '.*"latidude":([0-9\.]+)'
		rateRegex = '.*"rating":([0-9\.]+)'

		self.id = self.getRegexValue(cinemaIdRegex, line)
		self.cityid = self.getRegexValue(cityidRegex, line)
		self.title = self.getRegexValue(cinemaNameRegex,line)
		self.addr = self.getRegexValue(cinemaAddrRegex,line)
		self.tel = self.getRegexValue(cinemaTelRegex,line)
		self.lng = self.getRegexValue(lngRegex,line)
		self.lat = self.getRegexValue(latRegex,line)
		self.rate = self.getRegexValue(rateRegex,line)

	def parse(self, file):
		content = ''
		for line in open(file):
			line = line.strip()
			content += line
			# 单行处理影院信息的json数据
			self.parseShowtimeJson(line)
			# 全部数据处理其他
		self.parseCinemaServiceInfo(content)
		print ("%s\t%s\t%s\t%s\t%s\t%s,%s\t%s\t%d\t%s" % (self.cityid, self.id, self.title, self.addr, self.tel, self.lat, self.lng, self.businessTime, self.hasIMAX, self.rate))



if __name__ == '__main__':
	input = sys.argv[1]
	parser = CtimeCinemaParser()
	parser.parse(input)



