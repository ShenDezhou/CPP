#!/usr/bin/env python
#coding=gb2312

import sys, os
import re,math
#from ShopTool import Shop, MathTool, SimilarTool


# 全局常量
ENCODING = 'GB2312'
DISTANCE_THRESHOLD = 2500

MERGE_TEL = 'tel merge'
MERGE_EXACTLY_TITLE = 'exact title mrege'
MERGE_TITLE_SIMILAR = 'title similar'
MERGE_ADDR_SIMILAR = 'addr similar'




CurCity, LastCity = '', ''
MasterShopList, SlaveShopList = [], []


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


	@staticmethod
	def normalTel(tel):
		normTelList = []
		telArray = re.split('[\|,]', tel)
		for telItem in telArray:
			if len(telItem.strip()) > 0:
				normTelList.append(telItem)
		return normTelList

	@staticmethod
	def normalTitle(title):
		title = re.sub(u'[\-\(\)（）]', '', title)
		return title.strip().lower()

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
		lat, lng = float(poiSeg[0]), float(poiSeg[1])
		lat, lng = str(float('%.6f' % lat)), str(float('%.6f' % lng))
		return (lat, lng)

	def toString(self):
		return '%s\t%s\t%s\t%s\t%s\t%s' % (self.source, self.id, self.title, ','.join(self.tel), self.addr, self.poi)

	def str(self):
		return '%s\t%s\t%s\t%s\t%s' % (self.city, self.source, self.id, self.title, self.mergeid)





class SimilarTool(object):
	
	@staticmethod
	def hasIntersection(lList, rList):
		if len(rList) == 0 or len(rList) == 0:
			return False
		# 有交集
		lSet, rSet = set(lList), set(rList)
		if len(lSet.intersection(rSet)) > 0:
			return True
		return False

	@staticmethod
	def isStrSame(lStr, rStr):
		if lStr == rStr:
			return True
		if lStr.find(rStr) != -1 or rStr.find(lStr) != -1:
			return True
		return False

	@staticmethod
	def strSimilar(lStr, rStr, threshold):
		# 互为前后缀，则直接返回True
		if SimilarTool.isStrSame(lStr, rStr):
			return True

		lStrSet, rStrSet = set(lStr), set(rStr)
		lStrLen, rStrLen = len(lStrSet), len(rStrSet)
		
		totalLen = (lStrLen + rStrLen) / 2
		#minLen = rStrLen
		#if lStrLen < rStrLen:
		#	minLen = lStrLen

		# 公共
		commLen = len(lStrSet.intersection(rStrSet))
		#if commLen >= (minLen * threshold):
		if commLen >= (totalLen * threshold):
			return True
		return False

	
	@staticmethod
	def similar(lStr, rStr):
		lStrSet, rStrSet = set(lStr), set(rStr)
		lStrLen, rStrLen = len(lStrSet), len(rStrSet)
		if lStrLen == 0 or rStrLen == 0:
			return 0
		totalLen = (lStrLen + rStrLen) / 2
		# 公共
		commLen = len(lStrSet.intersection(rStrSet))
		return (1.0 * commLen / totalLen)






class MathTool(object):
	""" 常用函数工具类 """

	EARTH_RADIUS = 6378.137
	
	def __init__(self):
		pass

	# 角度转弧度
	def rad(self, d):
		return d * math.pi / 180.0
	
	# 计算两个经纬度坐标之间的距离(单位为  m)
	def distance(self, lat1, lng1, lat2, lng2):
		radLat1 = self.rad(lat1)
		radLat2 = self.rad(lat2)
		latDis = radLat1 - radLat2
		lngDis = self.rad(lng1) - self.rad(lng2)
		
		s = 2 * math.asin(math.sqrt(math.pow(math.sin(latDis/2),2)+math.cos(radLat1)*math.cos(radLat2)*math.pow(math.sin(lngDis/2),2)))
		distance = s * self.EARTH_RADIUS
		if distance < 0:
			retrun -distance
		return distance * 1000


	# 计算两个经纬度坐标之间的距离
	def poiDistance(self, poi1, poi2):
		lat1, lng1 = poi1
		lat2, lng2 = poi2
		if lat1=="" or lng1=="" or lat2=="" or lng2=="":
			return 1000000
		return self.distance(float(lat1), float(lng1), float(lat2), float(lng2))


# 加载店铺的基本信息
def load_shop_baseinfo(segs):
	if len(segs) < 7:
		return
	city, source, id, title, poi, tel, addr = segs
	shop = Shop(city, id, title, poi, addr, source, tel)
	if source == 'Dianping':
		MasterShopList.append(shop)
	#if source == 'Nuomi':
	else:
		SlaveShopList.append(shop)




# 将店铺列表转成以tel为键值的map
def get_tel_shoplist_map(shopList):
	telShoplistMap = dict()
	for shop in shopList:
		for tel in shop.tel:
			if len(tel) == 0:
				continue
			if tel not in telShoplistMap:
				list = []
				telShoplistMap[tel] = list
			telShoplistMap[tel].append(shop)
	return telShoplistMap



# 根据电话号码找候选合并对象
def get_candidate_shoplist_by_tel(telList, shopMap):
	candidateList = []
	for tel in telList:
		if len(tel) == 0 or tel not in shopMap:
			continue
		for shop in shopMap[tel]:
			candidateList.append(shop)
	return candidateList


# 两点是否足够靠近
def is_points_close(masterPoi, slavePoi):
	distance = mathTool.poiDistance(masterPoi, slavePoi)
	if distance < DISTANCE_THRESHOLD:
		return True
	return False
	


# 根据距离找候选合并对象
def get_candidate_shoplist_by_distance(shop, shopList):
	candidateList = []
	for candidateShop in shopList:
		if is_points_close(shop.poi, candidateShop.poi):
			candidateList.append(candidateShop)
	return candidateList



# 电话相同的前提下，还需要满足 1.距离很近 2.title很相似
def merge_by_tel(shop, candidateShopList):
	match = False
	#for cShop in candidateShopList:
	#	if is_points_close(shop.poi, cShop.poi) or SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7):
	#		shop.mergeid = cShop.id
	#		shop.mergeMethod = MERGE_TEL
	#		match = True
	#		break
	# 先用严格的要求，再用宽松的要求
	for cShop in candidateShopList:
		if is_points_close(shop.poi, cShop.poi) and SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7):
			#print "\t",  mathTool.poiDistance(shop.poi, cShop.poi), SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7)
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TEL
			return True
	for cShop in candidateShopList:
		if is_points_close(shop.poi, cShop.poi) and SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.5):
			#print "\t",  mathTool.poiDistance(shop.poi, cShop.poi), SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7)
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TEL
			return True
	for cShop in candidateShopList:
		if is_points_close(shop.poi, cShop.poi):
			#print "\t",  mathTool.poiDistance(shop.poi, cShop.poi), SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7)
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TEL
			return True
	return match


# 在距离很近的前提下，根据title, addr来合并
def merge_by_baseinfo(shop, candidateShopList):
	for cShop in candidateShopList:
		addrSimilar = SimilarTool.similar(shop.normAddr, cShop.normAddr)
		# title 严格相等
		if (shop.normTitle == cShop.normTitle or shop.simpleTitle == cShop.simpleTitle) and addrSimilar > 0.6:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_EXACTLY_TITLE
			break

		titleSimilar = SimilarTool.similar(shop.simpleTitle, cShop.simpleTitle)
		# title极其相似，地址极其相似
		if titleSimilar >= 0.9 and addrSimilar >= 0.85:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TITLE_SIMILAR
			break

		# title 与 addr 宽松相似
		if titleSimilar >= 0.75 and addrSimilar >= 0.75:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_ADDR_SIMILAR
	


# 对主从shop列表进行合并
def merge(masterShopList, slaveShopList):
	#print ('====== %s ========' % CurCity).encode('gb2312')

	masterTelShoplistMap = get_tel_shoplist_map(masterShopList)
	# 合并
	for shop in slaveShopList:
		# 优先考虑电话相同的，还需要满足 1.距离很近 2.title很相似
		candidateShopList = get_candidate_shoplist_by_tel(shop.tel, masterTelShoplistMap)
		if merge_by_tel(shop, candidateShopList):
			write(shop.str())
			continue
		
		# 如果电话相同没合并成功，再使用如下策略
		candidateShopList = get_candidate_shoplist_by_distance(shop, MasterShopList)
		merge_by_baseinfo(shop, candidateShopList)

		write(shop.str())



def write(line):
	print ('%s' % line).encode(ENCODING, 'ignore')




mathTool = MathTool()

for line in sys.stdin:
	segs = line.strip('\n').decode(ENCODING, 'ignore').split('\t')
	if len(segs) < 7:
		continue
	CurCity = segs[0]
	if len(CurCity) == 0:
		continue
	if CurCity != LastCity:
		if len(MasterShopList) > 0 and len(SlaveShopList) > 0:
			merge(MasterShopList, SlaveShopList)
		LastCity = CurCity
		MasterShopList, SlaveShopList = [], []	
	load_shop_baseinfo(segs)

# do not forget to merge the last if needed
merge(MasterShopList, SlaveShopList)


