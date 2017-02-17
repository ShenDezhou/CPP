#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-21 14:25
# * Filename	 : Shop.py
# * Description	 : 大众点评，糯米，美团等站点的店铺类
# * *****************************************************************************/

import re

# 店铺类
class Shop(object):
	def __init__(self, city, id, title, poi, addr, source='', tel=''):
		self.city = city
		self.id = id
		self.title = title
		self.poi = poi
		self.addr = addr
		self.tel = tel
		self.source = source
		self.mergeid = None
		self.mergeMethod = ''

		self.normTitle = Shop.normalTitle(self.title)
		self.simpleTitle = Shop.simpleTitle(self.title)
		self.poi = Shop.normalPoi(self.poi)
		self.normAddr = Shop.normalAddr(self.city, self.addr)
		self.tel = Shop.normalTel(self.tel)


	# 01084782821|01084782527
	# 01082509524,01082509534

	@staticmethod
	def normalTel(tel):
		normTelList = []
		tel = re.sub('\-', '', tel)
		telArray = re.split('[\|,]', tel)
		for telItem in telArray:
			if len(telItem.strip()) > 0:
				normTelList.append(telItem.strip())
		return normTelList

	@staticmethod
	def normalTitle(title):
		title = re.sub(u'[\-\(\)（）]', '', title)
		return title.strip()

	@staticmethod
	def simpleTitle(title):
		title = re.sub(u'\(.*?\)', '', title)
		title = re.sub(u'（.*?）', '', title)
		return title.strip()
	

	@staticmethod
	def normalAddr(city, addr):
		addr = re.sub(u'（.*$', '', addr)
		addr = re.sub('\(.*$', '', addr)
		normAddr = re.sub(city + u'(市)*', '', addr)
		return normAddr.strip()

	@staticmethod
	def normalPoi(poi):
		poiSeg = poi.split(',')
		if len(poiSeg) != 2:
			return ('', '')
		lat, lng = '', ''
		try:
			lat, lng = float(poiSeg[0]), float(poiSeg[1])
			lat, lng = str(float('%.6f' % lat)), str(float('%.6f' % lng))
		except:
			pass
		return (lat, lng)

	def toString(self):
		return '%s\t%s\t%s\t%s\t%s\t%s' % (self.source, self.id, self.title, ','.join(self.tel), self.addr, self.poi)

	def str(self):
		return '%s\t%s\t%s\t%s' % (self.source, self.id, self.title, self.mergeid)

#print Shop.simpleTitle(u'汤城小厨(太阳宫凯德MALL店) ')
