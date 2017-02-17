
#coding=gb2312

nuomiCinemaMap = dict()
otherCinemaMap = dict()
input = '/fuwu/Merger/Output/movie/cinema_movie_rel.table'
for line in open(input):
	segs = line.strip('\n').decode('gb2312', 'ignore').split('\t')
	cinemaid, source, ting = segs[1], segs[3], segs[9]
	
	if source.find(u'糯米') != -1:
		if cinemaid not in nuomiCinemaMap:
			nuomiCinemaMap[cinemaid] = []
		if ting not in nuomiCinemaMap[cinemaid]:
			nuomiCinemaMap[cinemaid].append(ting)
	else:
		if cinemaid not in otherCinemaMap:
			otherCinemaMap[cinemaid] = []
		if ting not in otherCinemaMap[cinemaid]:
			otherCinemaMap[cinemaid].append(ting)

# 糯米影院的厅名称是否都被包含
for cinemaid in otherCinemaMap:
	if cinemaid not in nuomiCinemaMap:
		#print ('#%s\t%s\t%s' % (cinemaid, u'糯米', '\t'.join(nuomiCinemaMap[cinemaid]))).encode('gb2312', 'ignore')
		continue
	noMatchTingList = []
	for ting in nuomiCinemaMap[cinemaid]:
		if ting not in otherCinemaMap[cinemaid]:
			noMatchTingList.append(ting)
	if len(noMatchTingList) == 0:
		continue

	# 存在不一致的情况
	normTing = '\t'.join(otherCinemaMap[cinemaid])
	noMatchTing = '\t'.join(noMatchTingList)
	print ('%s\t%s\t%s' % (cinemaid, u'非糯米', normTing)).encode('gb2312', 'ignore')
	print ('%s\t%s\t%s' % (cinemaid, u'糯米', noMatchTing)).encode('gb2312', 'ignore')





