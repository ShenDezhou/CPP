#!/bin/python
#coding=gb2312

import sys, re

# 过滤合并错误的情况
def filte_merge_error_ting(roomMaster, roomSlave):
	regex = u'([0-9]+)号'
	if re.search(regex, roomMaster) and re.search(regex, roomSlave):
		masterTingNumber = re.search(regex, roomMaster).group(1)
		slaverTingNumber = re.search(regex, roomSlave).group(1)
		if masterTingNumber != slaverTingNumber:
			return False
	return True


input = '/fuwu/Merger/Output/movie/cinema_movie_rel.table'
if len(sys.argv) > 1:
	input = sys.argv[1]


sourceTingMap = dict()

for line in open(input):
	segs = line.strip('\n').decode('gb2312', 'ignore').split('\t')
	if len(segs) < 10:
		continue
	cid, mid, source, date = segs[1:5]
	#id      cinemaid        movieid source  date    week    start   end     price   room    language        dimensional     seat
	time, ting = segs[6], segs[9]

	key = (cid, mid, date, time)
	value = (cid, source, ting)
	if key not in sourceTingMap:
		sourceTingMap[key] = []
	sourceTingMap[key].append(value)

# 针对同一个影院同一场电影
for key in sourceTingMap:
	tingSourceMap = dict()
	cid = ''
	for cid, source, ting in sourceTingMap[key]:
		if ting not in tingSourceMap:
			tingSourceMap[ting] = []
		if source not in tingSourceMap[ting]:
			tingSourceMap[ting].append(source)

	# 如果一致
	if len(tingSourceMap) == 1:
		continue
	#for ting in tingSourceMap:
	#	print (cid + "\t" + ting + "\t" + "\t".join(tingSourceMap[ting])).encode('gb2312', 'ignore')
	
	# 糯米的，1的都需要映射
	tingName, tingCount = '', 0
	for ting in tingSourceMap:
		if ting.find('..') != -1:
			continue
		name, count = ting, len(tingSourceMap[ting])
		if count > tingCount:
			tingName, tingCount = name, count
	# 生成映射
	for ting in tingSourceMap:
		if ting == tingName:
			continue
		for source in tingSourceMap[ting]:
			if not filte_merge_error_ting(ting, tingName):
				continue
			print ('%s\t%s\t%s\t%s' % (cid, source, ting, tingName)).encode('gb2312', 'ignore')


