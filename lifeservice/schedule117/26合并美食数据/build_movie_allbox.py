#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-04 18:45
# * Filename	 : build_movie_allbox.py
# * Description	 : 更新电影的票房信息
# * *****************************************************************************/

import re, sys
from FileTool import *
from keyConf import *


MovieDetail = '/search/zhangk/Fuwu/Merger/Output/movie/movie_detail.table'
MovieAllbox = '/search/zhangk/Fuwu/Spider/Cbooo/data/cbo_allbox'


def date2year(date):
	year = re.sub('\-.*$', '', date)
	return year

# 加载中国票房网的票房信息
def load_allbox_file(keyList, input, sep = '\t', hasHead = True):
	allboxMap = dict()
	# 这种方式不太好，如果格式修改还需要修改配置文件!!!!! 不如直接用第一行表头进行
	keyIndexMap = loadKeyIndexMap(keyList)
	isFirstLine = True
	for line in open(input):
		if isFirstLine:
			isFirstLine = False
			continue
		segs = line.strip('\n').split(sep)
		if len(segs) != len(keyList):
			continue
		title = segs[keyIndexMap[TitleKey]]
		date = segs[keyIndexMap[DateKey]]
		allbox = segs[keyIndexMap[BoxKey]]
		year = date2year(date)
		# 这里添加时间没有意义，因为票房的时间是计算票房的最后时间，电影的时间是上映时间
		if not re.search('[0-9]{4}', year):
			continue
		#allboxMap[title + '\t' + year] = allbox
		allboxMap[title] = allbox
	return allboxMap

# 更新票房信息
def update_allbox(keyList, input, output, sep = '\t'):
	allboxMap = load_allbox_file(MovieAllbocKey, MovieAllbox)
	keyIndexMap = loadKeyIndexMap(keyList)
	fd = open(output, 'w+')

	firstLine = True
	for line in open(input):
		if firstLine:
			firstLine = False
			fd.write('%s' % line)
			continue
		segs = line.strip('\n').split(sep)
		if len(segs) != len(keyList):
			continue
		kvList = []
		
		id = segs[keyIndexMap[IDKey]]
		title = segs[keyIndexMap[TitleKey]]
		year = segs[keyIndexMap[YearKey]]
		allbox = segs[keyIndexMap[BoxKey]]
		rank = segs[keyIndexMap[RankKey]]
		if id.find('_') != -1:
			fd.write('%s' % line); continue

		#print title, year, rank, (title in allboxMap)
		if not re.search('[0-9]{4}', year):
			fd.write('%s' % line)
			continue
		if len(rank) == 0 or int(rank) > 100:
			fd.write('%s' % line)
			continue
		
		#key = title + '\t' + year
		key = title
		if not key in allboxMap:
			fd.write('%s' % line); continue
		for idx in xrange(0, len(keyList)):
			kvList.append(segs[idx])
		kvList[keyIndexMap[BoxKey]] = allboxMap[key]
		#print title, allboxMap[key]
		fd.write('%s\n' % sep.join(kvList))
	fd.close()

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print 'Uasge: python ' + sys.argv[0] + ' output'
		sys.exit(-1)

	output = sys.argv[1]
	update_allbox(MovieDetailKey, MovieDetail, output)
