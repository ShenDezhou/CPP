#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-05 18:58
# * Filename	 : build_activity_venues.py
# * Description	 : 
# * *****************************************************************************/
#!/bin/python
#coding=gb2312

import sys
from MathTool import *

class ActivityVenuesMerger:
	dianpingVenuesDict = dict()


	def __init__(self, dianpingFile):
		for line in open(dianpingFile):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 4: continue
			city, id, title, poi = segs[0], segs[1], segs[2], segs[3]
			if city not in self.dianpingVenuesDict:
				self.dianpingVenuesDict[city] = []
			self.dianpingVenuesDict[city].append((id, title, poi))


	def get_closest_venue(self, srcPoi, destPoiList):
		distance = sys.maxint
		srcSegs = srcPoi.split(',')
		closetid = ''
		if len(srcSegs) < 2: return closetid

		mathTool = MathTool()
		for id,  poi in destPoiList:
			#print srcPoi, ' | ', poi
			destSegs = poi.split(',')
			if len(destSegs) < 2:
				return closetid
			curDis = mathTool.poiDistance((srcSegs[0], srcSegs[1]), (destSegs[0], destSegs[1]))
			if curDis < distance:
				closetid = id
		return closetid
			

	def merge_venues(self, input, output):
		fd = open(output, 'w+')
		for line in open(input):
			segs = line.strip('\n').split('\t')
			if len(segs) != 4: continue
			id, city, title, poi = segs[0], segs[1], segs[2], segs[3]
			if id == 'id' or city not in self.dianpingVenuesDict:
				continue
			
			meetVenueList = []
			for vid, vtitle, vpoi in self.dianpingVenuesDict[city]:
				if vtitle == title:
					meetVenueList.append((vid, vpoi))

			if len(meetVenueList) == 0: continue
			if len(meetVenueList) == 1:
				fd.write('%s\t%s\n' % (id, meetVenueList[0][0]))
			else:
				vid = self.get_closest_venue(poi, meetVenueList)
				fd.write('%s\t%s\n' % (id, vid))

		fd.close()


def test():
	input = 'tmp/damai_activity.table.extract'
	output = 'tmp/damai_activity.table.merge'
	conf = 'tmp/dianping_play_shops'

	merger = ActivityVenuesMerger(conf)
	merger.merge_venues(input, output)



if __name__ == '__main__':
	if len(sys.argv) < 4:
		print 'Usage python ' + sys.argv[0] + ' dianpingShopConf activityFile output'
		sys.exit(-1)

	conf, input, output = sys.argv[1], sys.argv[2], sys.argv[3]
	merger = ActivityVenuesMerger(conf)
	merger.merge_venues(input, output)
