#!/bin/python
#coding=gb2312
import re, sys


# 构建每个城市的type url的文件名
def create_type_url_path(city, type):
	path = 'data/%s/url/%s_urls' % (type, city)
	#print path
	return path


def split_urls(input, type, encode='gb2312'):
	cityfdMap = dict()
	for line in open(input):
		segs = line.strip('\n').split('\t')
		if len(segs) < 5:
			continue
		#http://www.dianping.com/shop/50603629   青岛    qingdao 餐厅    restaurant
		url, city, _type = segs[0], segs[2], segs[4]
		if _type != type:
			continue
		# 将url写入对应城市的url文件中
		if city not in cityfdMap:
			urlPath = create_type_url_path(city, type)
			fd = open(urlPath, 'w+')
			cityfdMap[city] = fd
		cityfdMap[city].write('%s\n' % url)

	# 关闭文件描述符
	for city in cityfdMap:
		cityfdMap[city].close()


input = 'Scan/data/shop_urls.types'
type = 'restaurant'

if len(sys.argv) > 1:
	input = sys.argv[1]
if len(sys.argv) > 2:
	type = sys.argv[2]

split_urls(input, type)
