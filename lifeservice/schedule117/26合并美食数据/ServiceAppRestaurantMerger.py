#!/bin/python
#coding=gb2312

import sys, re, logging
from MathTool import *
from DateTimeTool import DateTimeTool as dateTool

DEBUG = True
DEBUG = False


# 定义日志
today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='logs/ServiceAppMerger_' + today + '.log',
                filemode='w+')


class Item:
	# 构造函数
	def __init__(self, id, title, domain, tel, area, addr, poi):
		self.id = id
		self.poi = poi
		self.tel = tel
		self.area = area
		self.addr = addr
		self.title = title
		self.domain = domain



# 生活服务APP项目 美食商店的合并类
class RestaurantMerger:
	
	sep, telSep = '\t', '|'
	idRow, urlRow, titleRow = -1, -1, -1
	telRow, areaRow, addrRow, poiRow = -1, -1, -1, -1

	def __init__(self):
		self.lastID, self.lastUrl = '', ''
		self.lastTitle, self.lastTel = '', ''
		self.lastArea, self.lastAddr = '', ''
		self.lastPoiStr = ''
		self.lastPoi = ('', '')
		self.mathTool = MathTool()
		
		# 测试使用
		self.debugDistance = ''
		self.debugAddrDis = ''


	def getDomain(self, url):
		urlSegs = url.split('/')
		if len(urlSegs) > 2:
			return urlSegs[2]


	def normalPoi(self, poiStr):
		poiSeg = poiStr.split(',')
		if len(poiSeg) != 2:
			return ('', '')
		lat, lng = float(poiSeg[0]), float(poiSeg[1])
		lat = str(float("%.3f" % lat))
		lng = str(float("%.3f" % lng))
		return (lat, lng)

	
	def telSame(self, curTel, lastTel):
		curTelSet = set(curTel.split(self.telSep))
		lastTelSet = set(lastTel.split(self.telSep))
		# 有交集
		if len(curTelSet.intersection(lastTelSet)) > 0:
			return True
		return False


	# 地址不规范，不一致，使用LCS不适合
	# 直接计算相同的字符串个数(有缺陷的方法)
	def addrSimilar(self, curAddr, lastAddr):
		curAddrSet, lastAddrSet = set(curAddr), set(lastAddr)
		curAddrLen, lastAddrLen = len(curAddrSet), len(lastAddrSet)
		minLen = lastAddrLen
		if curAddrLen < lastAddrLen:
			minLen = curAddrLen
		# 公共
		commLen = len(curAddrSet.intersection(lastAddrSet))
		if commLen >= (minLen * 0.6):
			if DEBUG:
				#print '[Right]:', curAddr, lastAddr
				self.debugAddrDis = 'commLen= ' + str(commLen) + ', minLen=' + str(minLen) + ', ratio=' + str(float(commLen) / minLen) + ', curAddr=[' + curAddr + '],  lastAddr=[' + lastAddr + ']'
			return True
		if DEBUG:
			#print '[Error]:', curAddr, lastAddr
			self.debugAddrDis = 'commLen= ' + str(commLen) + ', minLen=' + str(minLen) + ', ratio=' + str(float(commLen) / minLen) + ', curAddr=[' + curAddr + '],  lastAddr=[' + lastAddr + ']'
		return False


	# 认为距离在 1km 之类的都相同
	def distanceNear(self, curPoi, lastPoi):
		distance = self.mathTool.poiDistance(curPoi, lastPoi)
		if distance <= 1000:
			self.debugDistance = distance
			return True
		self.debugDistance = distance
		return False


	def getKeyRow(self, headLine):
		segs = headLine.split(self.sep)
		for row in xrange(0, len(segs)):
			if segs[row] == 'id': self.idRow = row
			elif segs[row] == 'url': self.urlRow = row
			elif segs[row] == 'tel': self.telRow = row
			elif segs[row] == 'poi': self.poiRow = row
			elif segs[row] == 'area': self.areaRow = row
			elif segs[row] == 'addr': self.addrRow = row
			elif segs[row] == 'title': self.titleRow = row


	# 电话号码相同，区域相同，域名不同
	def mergeByTel(self, file):
		lastSameTelList = []
		isFirstLine = True

		for line in open(file):
			line = line.strip('\n')
			if isFirstLine:
				isFirstLine = False
				self.getKeyRow(line)
				continue
			
			segs = line.split(self.sep)

			curID = segs[self.idRow]
			curUrl, curTitle = segs[self.urlRow], segs[self.titleRow]
			curArea, curTel = segs[self.areaRow], segs[self.telRow]
			curAddr, curPoi = segs[self.addrRow], self.normalPoi(segs[self.poiRow])

			if len(curTel) < 7: continue
			distance = self.mathTool.poiDistance(curPoi, self.lastPoi)
			# 这里也得改
			if self.lastTel == curTel:
				if curArea == self.lastArea and \
					self.getDomain(self.lastUrl) != self.getDomain(curUrl):
					# 距离应该在一定范围内
					if DEBUG:
						#self.debugAddrDis = 'commLen= ' + str(commLen) + ', minLen=' + str(minLen) + ', ratio=' + str(float(commLen) / minLen) + ', curAddr=[' + curAddr + '],  lastAddr=[' + lastAddr + ']'
						self.debugAddrDis = 'curAddr=[' + curAddr + '],  lastAddr=[' + self.lastAddr + ']'
						print '\t[debug]: curAddr=[' + curAddr + '],  lastAddr=[' + self.lastAddr + ']'
					if self.distanceNear(curPoi, self.lastPoi):
						print 'tel\t' + curID + '\t' + self.lastID + '\t' + str(distance)
			
			self.lastID, self.lastUrl = curID, curUrl
			self.lastTel, self.lastTitle = curTel, curTitle
			self.lastArea, self.lastAddr = curArea, curAddr
			self.lastPoi = curPoi

	
	# 名称相同，domain不同，area相同
	# 电话相似 or 地址相似 or poi距离在一定范围
	def mergeByTitle(self, file):
		lastSameTitleList = []
		isFirstLine = True
		lineNum = 0

		for line in open(file):
			lineNum += 1
			if lineNum % 1000 == 0:
				logging.info("has handle %d lines" % lineNum)			
			line = line.strip('\n')
			if isFirstLine:
				isFirstLine = False
				self.getKeyRow(line)
				continue

			segs = line.split(self.sep)
			
			curID = segs[self.idRow]
			curUrl, curTitle = segs[self.urlRow], segs[self.titleRow]
			curArea, curTel = segs[self.areaRow], segs[self.telRow]
			curAddr, curPoi = segs[self.addrRow], self.normalPoi(segs[self.poiRow])
			curDomain = self.getDomain(curUrl)

			if len(curTitle) == 0: continue

			if curTitle == self.lastTitle:
				for lastItem in lastSameTitleList:
					if curArea == lastItem.area and curDomain != lastItem.domain:
						# 如下任意一个相似即可
						isSimiliar = True
						#if self.telSame(curTel, lastItem.tel):
						#	isSimiliar = True
						
						if not self.addrSimilar(curAddr, lastItem.addr):
							isSimiliar = False
						
						if not self.distanceNear(curPoi, lastItem.poi):
							isSimiliar = False
						
						if isSimiliar:
							print 'title\t' + curID + '\t' + lastItem.id
						if DEBUG:
							print '\t[debug] addrDis=[' + str(self.debugAddrDis) + ']  distance=[' + str(self.debugDistance) + ']'
			else:
				lastSameTitleList = []

			self.lastTitle = curTitle
			curItem = Item(curID, curTitle, curDomain, curTel, curArea, curAddr, curPoi)
			lastSameTitleList.append(curItem)



def test():
	merger = RestaurantMerger()
	#addr1 = '东直门内大街267号优品蛋糕房内'
	#addr2 = '德外六铺炕街15号(近人定湖公园南门)'
	addr1 = '北京市东城区龙潭路8号'
	addr2 = '东城区 龙潭湖公园8号(龙潭湖北门东)'
	print merger.addrSimilar(addr1, addr2)
#test()


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print 'Usage: python bin/ServiceAppMerger.py -opt input'
		sys.exit(-1)

	merger = RestaurantMerger()
	opt, input = sys.argv[1], sys.argv[2]

	if opt == '-title':
		merger.mergeByTitle(input)
	elif opt == '-tel':
		merger.mergeByTel(input)






