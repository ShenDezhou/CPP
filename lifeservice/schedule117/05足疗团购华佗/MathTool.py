#!/bin/python
#coding=gb2312

import math


class MathTool:
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


	
def test():
	tool = MathTool()
	#print tool.distance(34.668412,135.498735, 34.668693663309,135.50203323365)
	#print tool.distance(35.713, 139.762,35.713, 139.764)
	#print tool.poiDistance(('35.713', '139.762'),('35.713', '139.764'))
	print tool.distance(29.3527,104.7859, 29.3351529999791,104.721482999566)

#test()
