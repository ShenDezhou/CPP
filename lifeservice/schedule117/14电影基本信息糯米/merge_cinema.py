#!/bin/bash|python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-06 17:05
# * Filename	 : merge_cinema.py
# * Description	 : 
# * *****************************************************************************/

import re, sys
from MathTool import *


DistanceThreshold = 2000



# 影院类
class Cinema(object):
	def __init__(self, city, id, title, poi, addr, tel):
		self.city = city
		self.id = id
		self.title = Cinema.normalTitle(city, title)
		self.poi = Cinema.normalPoi(poi)
		self.addr = Cinema.normalAddr(city, addr)
		self.tel = Cinema.normalTel(tel)

	@staticmethod
	def normalTel(tel):
		normTelList = []
		telArray = re.split('[\s,]', tel)
		for telItem in telArray:
			telItemArray = telItem.strip().split('-')
			normTel = ''.join(telItemArray[0:min(len(telItemArray), 2)])
			if len(normTel.strip()) > 0:
				normTelList.append(normTel)
		return normTelList

	@staticmethod
	def normalTitle(city, title):
		title = re.sub(u'[\-\(\)（）]', '', title)
		normTitle = re.sub(city, "", title)
		return normTitle.strip()

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
		lat, lng = float(poiSeg[0]), float(poiSeg[1])
		lat, lng = str(float('%.6f' % lat)), str(float('%.6f' % lng))
		return (lat, lng)

	def toString(self):
		return '%s\t%s\t%s\t%s\t%s' % (self.city, self.id, self.title, self.poi, self.addr)	



class SimilarTool(object):
	
	@staticmethod
	def telSame(curTel, lastTel):
		if len(curTel) == 0 or len(lastTel) == 0:
			return False
		# 有交集
		curTelSet, lastTelSet = set(curTel), set(lastTel)
		if len(curTelSet.intersection(lastTelSet)) > 0:
			return True
		return False

	@staticmethod
	def isTitleSame(curTitle, lastTitle):
		if curTitle == lastTitle:
			return True
		if curTitle.find(lastTitle) != -1 or lastTitle.find(curTitle) != -1:
			return True
		return False

	@staticmethod
	def titleSimilar(curTitle, lastTitle, threshold):
		# 互为前后缀，则直接返回True
		if SimilarTool.isTitleSame(curTitle, lastTitle):
			return True

		curTitleSet, lastTitleSet = set(curTitle), set(lastTitle)
		curTitleLen, lastTitleLen = len(curTitleSet), len(lastTitleSet)
		minLen = lastTitleLen
		if curTitleLen < lastTitleLen:
			minLen = curTitleLen
		# 公共
		commLen = len(curTitleSet.intersection(lastTitleSet))
		if commLen >= (minLen * threshold):
			return True
		return False





def load_online_cinema(cinemaFile, encoding):
	cinemaMap = dict()
	firstLine = True
	for line in open(cinemaFile):
		segs = line.strip('\n').decode(encoding, 'ignore').split('\t')
		if len(segs) != 6: continue
		if firstLine: firstLine = False; continue
		city, id, title, poi, addr, tel = segs
		cinema = Cinema(city, id, title, poi, addr, tel)
		if city not in cinemaMap:
			cinemaMap[city] = []
		cinemaMap[city].append(cinema)
	return cinemaMap



mathTool = MathTool()

def merge_cinema_imp(curCinema, onlineCinemaList):
	for cinema in onlineCinemaList:
		# poi 距离要在一定范围之内
		distance = mathTool.poiDistance(curCinema.poi, cinema.poi)
		if distance > DistanceThreshold:
			continue
		
		if SimilarTool.titleSimilar(curCinema.title, cinema.title, 0.8):
			return cinema
		

		if SimilarTool.telSame(curCinema.tel, cinema.tel) and SimilarTool.titleSimilar(curCinema.title, cinema.title, 0.5):
			return cinema

		# title相同，返回
		
		# title相似， tel相同，返回
		
		# title相似， addr相似，返回
	return None



def merge_similar_cinemas(curCinema, onlineCinemaList):
	# poi 距离比较近，addr比较相似，包含城市
	for cinema in onlineCinemaList:
		# poi 距离要在一定范围之内
		distance = mathTool.poiDistance(curCinema.poi, cinema.poi)
		if distance > DistanceThreshold:
			continue
		if not SimilarTool.titleSimilar(curCinema.title, cinema.title, 0.6):
			continue
		if SimilarTool.telSame(curCinema.tel, cinema.tel):
			return cinema
		
	 	if SimilarTool.titleSimilar(curCinema.addr, cinema.addr, 0.7):
			return cinema
	return None







def merge_cinema(onlineCinemaFile, offlineCinemaFile, encoding):
	noMergerList = []
	onlineCinemaMap = load_online_cinema(onlineCinemaFile, encoding)
	for line in open(offlineCinemaFile):
		segs = line.strip('\n').decode(encoding, 'ignore').split('\t')
		if len(segs) != 6: continue
		city, id, title, poi, addr, tel = segs
		curCinema = Cinema(city, id, title, poi, addr, tel)
		if city not in onlineCinemaMap:
			continue
		mergeCinema = merge_cinema_imp(curCinema, onlineCinemaMap[city])

		if mergeCinema == None:
			noMergerList.append(curCinema)
			continue
		print ('equal\t%s\t%s' % (curCinema.toString(), mergeCinema.toString())).encode(encoding, 'ignore')
		#print ('%s' % curCinema.toString()).encode(encoding, 'ignore')
		#print ('%s' % mergeCinema.toString()).encode(encoding, 'ignore')
		print "\n"


	# 没有合并成功的，按照地域距离比较近，地址包含其城市的方法
	allCinemaList = []
	for city in onlineCinemaMap:
		for cinemaItem in onlineCinemaMap[city]:
			allCinemaList.append(cinemaItem)
	for noMergeCinema in noMergerList:
		mergeCinema = merge_similar_cinemas(noMergeCinema, allCinemaList)
		if mergeCinema != None:
			print ('similar\t%s\t%s' % (noMergeCinema.toString(), mergeCinema.toString())).encode(encoding, 'ignore')
			




merge_cinema('/fuwu/Spider/Nuomi/tmp/cinema.online', '/fuwu/Spider/Nuomi/tmp/cinema.sort', 'gb2312')




def test_load_online_cinema():
	cinemaMap = load_online_cinema('/fuwu/Spider/Nuomi/tmp/cinema.online', 'gb2312')
	print len(cinemaMap)
	total = 0
	for city in cinemaMap:
		total += len(cinemaMap[city])
	print total

#test_load_online_cinema()






	#def __init__(self, city, id, title, poi, addr, tel):
#cinema = Cinema(u'成都', '123', u'万达-国际影城（万达广场店)', '30.942205398028985,118.74754518812234', u'成都市流县航鹰东路6号润驰国际广场4楼（近城北市场,6路公交直达）', u'0731-28829915  0731-28829510')
#print cinema.tel, cinema.poi, cinema.title, cinema.addr


#0731-28829915  0731-28829510
#0731-22915555,0731-22915158
#0760-87132677-601


#print Cinema.normalTel('0731-28829915  0731-28829510')
#print Cinema.normalTitle(u'马鞍山', u'马鞍山万达-国际影城（万达广场店)')
#print Cinema.normalTitle(u'石家庄', u'17.5影城(卓达影城)')


#print Cinema.normPoi('30.942205398028985,118.74754518812234')
#print Cinema.normPoi('37.5314,122.0697')

#print Cinema.normalAddr(u'成都', u'成都市流县航鹰东路6号润驰国际广场4楼（近城北市场,6路公交直达）')
