#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-05 18:58
# * Filename	 : build_activity_venues.py
# * Description	 : 
# * *****************************************************************************/
#!/bin/python
#coding=gb2312

import sys, re
from MathTool import *



class FindArticleMerger:
	dianpingShopDict = dict()

	# anhuisuzhou     id      title   address tel
	def __init__(self, dianpingFile):
		for line in open(dianpingFile):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 5: continue
			city, id, title, addr, tel = segs[0], segs[1], segs[2], segs[3], segs[4]
			if city not in self.dianpingShopDict:
				self.dianpingShopDict[city] = []
			self.dianpingShopDict[city].append((id, title, addr, tel))

	def is_tel_equal(self, srcTel, destTel):
		EmptyElement = ''
		srcTel = re.sub('\-', '', srcTel.strip())
		destTel = re.sub('\-', '', destTel.strip())
		if len(srcTel) == 0 or len(destTel) == 0:
			return False
		srcTelArray = re.split('[,;\s]', srcTel)
		destTelArray = re.split('[,;\s]', destTel)
		print srcTelArray
		commonSet = set(srcTelArray).intersection(set(destTelArray))
		if EmptyElement in commonSet:
			commonSet.remove(EmptyElement)
		return len(commonSet) > 0


	def is_addr_same(self, srcAddr, destAddr):
		if srcAddr == destAddr:
			return True
		if srcAddr.find(destAddr) != -1 or destAddr.find(srcAddr) != -1:
			return True
		return False

	def is_addr_similar_imp(self, srcAddr, destAddr):
		similarThreshold = 0.75
		srcAddrSet, destAddrSet = set(srcAddr), set(destAddr)
		srcAddrLen, destAddrLen = len(srcAddrSet), len(destAddrSet)
		minLen = destAddrLen
		if srcAddrLen < destAddrLen:
			minLen = srcAddrLen
		commLen = len(srcAddrSet.intersection(destAddrSet))
		if commLen >= (minLen * similarThreshold):
			return True
		return False
		

	def is_addr_similar(self, srcAddr, destAddr):
		if len(srcAddr) == 0 or len(destAddr) == 0:
			return False

		if self.is_addr_same(srcAddr, destAddr):
			return True

		if self.is_addr_similar_imp(srcAddr, destAddr):
			return True
		srcAddr = re.sub('\(.*?\)', '', srcAddr)
		destAddr = re.sub('\(.*?\)', '', destAddr)
		if self.is_addr_similar_imp(srcAddr, destAddr):
			return True
		return False


	def get_similarest_shop(self, srcShop, destShopList):
		srcAddr, srcTel = srcShop
		for destid, destAddr, destTel in destShopList:
			if self.is_tel_equal(srcTel, destTel):
				return destid
			if self.is_addr_similar(srcAddr, destAddr):
				return destid
		return ''



	# input <city id title addr tel>
	def merge_find_shops(self, input, output):
		fd = open(output, 'w+')
		for line in open(input):
			segs = line.strip('\n').split('\t')
			if len(segs) != 5: continue
			city, id, title, addr, tel = segs[0], segs[1], segs[2], segs[3], segs[4]
			if id == 'id' or city not in self.dianpingShopDict:
				continue
			
			meetShopList = []
			for sid, stitle, saddr, stel in self.dianpingShopDict[city]:
				if stitle == title:
					meetShopList.append((sid, saddr, stel))

			if len(meetShopList) == 0: continue
			if len(meetShopList) == 1:
				fd.write('%s\t%s\n' % (id, meetShopList[0][0]))
			else:
				sid = self.get_similarest_shop((addr, tel), meetShopList)
				if len(sid) > 0:
					fd.write('%s\t%s\n' % (id, sid))
		fd.close()




def test_is_tel_equal():
	print is_tel_equal('15591887668,', ';15596884767')

def test():
	input = 'tmp/xiancheng_article.extract'
	output = 'tmp/xiancheng_article.merge'
	conf = 'tmp/dianping_restaurant_play_shops'

	merger = FindArticleMerger(conf)
	merger.merge_find_shops(input, output)


if __name__ == '__main__':
	print len(sys.argv)
	if len(sys.argv) < 4:
		print 'Usage python ' + sys.argv[0] + ' dianpingShopConf xianchengArticle output'
		sys.exit(-1)

	conf, input, output = sys.argv[1], sys.argv[2], sys.argv[3]
	merger = FindArticleMerger(conf)
	merger.merge_find_shops(input, output)
