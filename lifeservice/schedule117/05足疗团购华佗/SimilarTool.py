#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-08 16:51
# * Filename	 : SimilarTool.py
# * Description	 : 计算相似性
# * *****************************************************************************/

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


#print SimilarTool.similar('北京市海淀区地铁6号线五路居站C口出右拐胡同南行200米路东（亮甲店小学北行300米）', '海淀区 八里庄路61号院对面(地铁6号线五路居地铁C出口右拐胡同南行100米)')


