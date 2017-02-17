#!/bin/python
#coding=gb2312
import re, sys

# 将大众点评美食list列表也抓取的url,分发以城市为单位的文件中


# 加载城市的code与拼音的映射
def load_city_conf(input):
	cityCodeNameMap = dict()
	for line in open(input):
		segs = line.strip('\n').split('\t')
		if len(segs) != 3:
			continue
		code, enName = segs[1], segs[2]
		cityCodeNameMap[code] = enName
	return cityCodeNameMap



# 构建每个城市的restaurant/play url的文件名
def create_dianping_url_path(city, type):
	return 'data/%s/url/%s_urls' % (type, city)


def split_dianping_urls(cityConf, input, type, inEncode, outEncode='gb2312'):
	cityCodeNameMap = load_city_conf(cityConf)
	#print cityCodeNameMap
	cityfdMap = dict()
	curCityCode, curPage = '', ''
	for line in open(input):
		segs = line.strip('\n').decode(inEncode, 'ignore').split('\t')
		if len(segs) == 2 and segs[0] == 'url':
			#http://www.dianping.com/search/category/416/10/r28263p1
			url = segs[1].replace('http://www.dianping.com/search/category/', '')
			url = re.sub('\?.*$', '', url)
			curPage = re.sub('.*p', '', url)
			curCityCode = re.sub('/.*$', '', url)
		elif len(segs) > 4:
			if curCityCode not in cityCodeNameMap:
				print curCityCode
				continue
			# 将restaurant的url写入对应城市的url文件中
			url, title, city = segs[0], segs[1], cityCodeNameMap[curCityCode]
			line = '%s\t%s\t%s\t%s' % (url, title, city, curPage)
			if city not in cityfdMap:
				urlPath = create_dianping_url_path(city, type)
				fd = open(urlPath, 'w+')
				cityfdMap[city] = fd
			cityfdMap[city].write(('%s\n' % line).encode(outEncode, 'ignore'))
	# 关闭文件描述符
	for city in cityfdMap:
		cityfdMap[city].close()



if len(sys.argv) < 3:
	print 'Usage: python %s cityConfFile urlFile type[restaurant|play]' % sys.argv[0]
	sys.exit(-1)

cityConf, input, type = sys.argv[1], sys.argv[2], sys.argv[3]

split_dianping_urls(cityConf, input, type, 'utf8')
