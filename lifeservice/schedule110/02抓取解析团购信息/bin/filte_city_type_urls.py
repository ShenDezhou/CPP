#!/bin/python
#coding=gb2312
# 过滤某种类别下的国内城市的URL

import re, sys

UrlCrumb = 'Scan/data/shop_urls.crumb'
CityCodeEnConf = 'conf/dianping_city_code_pinyin_conf'
CrumbTypeConf = 'conf/dianping_crumb_types'

if len(sys.argv) > 1:
	UrlCrumb = sys.argv[1]


# 加载城市名称中英文的映射
def load_city_conf(input, inCode='gb2312'):
	cityNameMap = dict()
	for line in open(input):
		segs = line.strip('\n').decode(inCode, 'ignore').split('\t')
		if len(segs) < 3:
			continue
		chName, code, enName = segs[0], segs[1], segs[2]
		cityNameMap[chName] = enName
	return cityNameMap

# 加载面包线的类别
def load_crumb_type_conf(input, inCode='gb2312'):
	crumbTypeMap = dict()
	for line in open(input):
		segs = line.strip('\n').decode(inCode, 'ignore').split('\t')
		if len(segs) < 3:
			continue
		_, category, type = segs[0], segs[1], segs[2]
		crumbTypeMap[category] = type
	return crumbTypeMap


def write_line(fd, line, inCode='gb2312'):
	fd.write(('%s\n' % line).encode(inCode, 'ignore'))


# 根据面包线，划分URL
def split_url_use_crumb(input, output, inCode='gb2312'):
	cityNameMap = load_city_conf(CityCodeEnConf)
	crumbTypeMap = load_crumb_type_conf(CrumbTypeConf)
	
	fd = open(output, 'w+')
	lines = 0
	for line in open(input):
		lines += 1
		if lines % 100000 == 0:
			print 'handle %d' % lines


		segs = line.strip('\n').decode(inCode, 'ignore').split('\t')
		if len(segs) < 2:
			continue
		url, crumb = segs[0].strip(), segs[1].strip()
		if crumb == 'null':
			continue
		crumbSegs = crumb.split(u'；')
		if len(crumbSegs) <= 2:
			continue
		# check category
		crumbType = crumbSegs[0].strip()
		for category in crumbTypeMap:
			if not crumbType.endswith(category):
				continue
			# check city 	
			city = crumbType.replace(category, '')
			if city in cityNameMap:
				write_line(fd, '%s\t%s\t%s\t%s\t%s' % (url, city, cityNameMap[city], category, crumbTypeMap[category]), inCode)
		# 单独处理足疗按摩
		if crumbType in cityNameMap:
			city = crumbType
			if crumb.find(u'足疗按摩') != -1:
				write_line(fd, u'%s\t%s\t%s\t足疗按摩\trestaurant' % (url, city, cityNameMap[city]), inCode)
		

output = 'Scan/data/shop_urls.types'
split_url_use_crumb(UrlCrumb, output)

