#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-08 14:33
# * Filename	 : ShopMerger.py
# * Description	 : 将slaveShop 向 masterShop 合并
# * *****************************************************************************/

import sys, os
import ConfigParser
from Shop import *
from MathTool import *
from SimilarTool import *

# 全局常量
ENCODING = 'GB2312'
DISTANCE_THRESHOLD = 2500

MERGE_TEL = 'tel merge'
MERGE_EXACTLY_TITLE = 'exact title mrege'
MERGE_TITLE_SIMILAR = 'title similar'
MERGE_ADDR_SIMILAR = 'addr similar'

mathTool = MathTool()

# 加载店铺的基本信息
def load_shop_baseinfo(shopFile, encoding, filteFirstLine = False):
	isFirstLine = True
	shopList = []
	for line in open(shopFile):
		if filteFirstLine and isFirstLine:
			isFirstLine = False; continue
		segs = line.strip('\n').decode(encoding, 'ignore').split('\t')
		if len(segs) < 7: continue
		id, source, title, poi, tel, city, addr = segs
		shop = Shop(city, id, title, poi, addr, source, tel)
		shopList.append(shop)
	print 'load shop baseinfo of %s done.' % shopFile
	return shopList




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
	for cShop in candidateShopList:
		#print ('shop: ' + shop.toString()).encode(ENCODING, 'ignore')
		#print ('cShop:' + cShop.toString()).encode(ENCODING, 'ignore')
		#print SimilarTool.similar(shop.normTitle, cShop.normTitle)
		#print ''
		#if is_points_close(shop.poi, cShop.poi) and SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7):
		if is_points_close(shop.poi, cShop.poi) or SimilarTool.strSimilar(shop.normTitle, cShop.normTitle, 0.7):
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TEL
			print ('shop: ' + shop.toString()).encode(ENCODING, 'ignore')
			print ('cShop:' + cShop.toString()).encode(ENCODING, 'ignore')
			print MERGE_TEL, '\n'
			match = True
			break
	return match


# 在距离很近的前提下，根据title, addr来合并
def merge_by_baseinfo(shop, candidateShopList):
	#print len(candidateShopList)
	for cShop in candidateShopList:
		addrSimilar = SimilarTool.similar(shop.normAddr, cShop.normAddr)
		# title 严格相等
		if (shop.normTitle == cShop.normTitle or shop.simpleTitle == cShop.simpleTitle) and addrSimilar > 0.6:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_EXACTLY_TITLE
			print ('shop: ' + shop.toString()).encode(ENCODING, 'ignore')
			print ('cShop:' + cShop.toString()).encode(ENCODING, 'ignore')
			print MERGE_EXACTLY_TITLE, '\n'
			break

		titleSimilar = SimilarTool.similar(shop.simpleTitle, cShop.simpleTitle)
		# title极其相似，地址极其相似
		if titleSimilar >= 0.9 and addrSimilar >= 0.85:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_TITLE_SIMILAR
			print ('shop: ' + shop.toString()).encode(ENCODING, 'ignore')
			print ('cShop:' + cShop.toString()).encode(ENCODING, 'ignore')
			print MERGE_TITLE_SIMILAR, '\n'
			break

		# title 与 addr 宽松相似
		if titleSimilar >= 0.8 and addrSimilar >= 0.8:
			shop.mergeid = cShop.id
			shop.mergeMethod = MERGE_ADDR_SIMILAR
			print ('shop: ' + shop.toString()).encode(ENCODING, 'ignore')
			print ('cShop:' + cShop.toString()).encode(ENCODING, 'ignore')
			print MERGE_ADDR_SIMILAR, '\n'
			break
	



def merge(masterShopList, slaveShopList, fd):
	masterTelShoplistMap = get_tel_shoplist_map(masterShopList)
	# 合并
	line = 0
	for shop in slaveShopList:
		line += 1
		if line % 100 == 0:
			print "handle %d" % line
		# 优先考虑电话相同的，还需要满足 1.距离很近 2.title很相似
		candidateShopList = get_candidate_shoplist_by_tel(shop.tel, masterTelShoplistMap)
		#print len(candidateShopList)#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		#print shop.tel#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if merge_by_tel(shop, candidateShopList):
			write(fd, shop.str())
			continue
		
		# 如果电话相同没合并成功，再使用如下策略
		candidateShopList = get_candidate_shoplist_by_distance(shop, masterShopList)
		merge_by_baseinfo(shop, candidateShopList)

		# 打印合并情况
		#if shop.mergeid is not None:
		#	write(fd, shop.toString())
		write(fd, shop.str())



def write(fd, line):
	fd.write(('%s\n' % line).encode(ENCODING, 'ignore'))


# 主函数
def main(masterShopFile, slaveShopFile, mergeFile):
	# 加载待合并的店铺信息
	masterShopList = load_shop_baseinfo(masterShopFile, ENCODING)
	slaveShopList = load_shop_baseinfo(slaveShopFile, ENCODING)
	#print len(masterShopList), len(slaveShopList)
	#sys.exit(-1)

	fd = open(mergeFile, 'w+')
	merge(masterShopList, slaveShopList, fd)
	fd.close()



if len(sys.argv) < 3:
	print 'Usage: python %s masterShopFile slaveShopFile mergeFile' % sys.argv[0]
	sys.exit(-1)

main(sys.argv[1], sys.argv[2], sys.argv[3])




