#!/bin/python
#coding=gbk

import sys, re
import pandas as pd
import numpy as np
from pandas import DataFrame
from optparse import OptionParser



MovieDetailKey = ['movieid','url','moviename','enName','posterlink','year','duration','type','date','version',\
		'onlinestatus','releasecountry','scorecnt','wantcnt','shortdesc','description','score','hot','boxoffice',\
		'photosUrl','videosUrl','photosCnt','photoSet']



class MovieDetailNormalizer():
	
	def __init__(self, _input):
		self.input = _input
		self.output = '%s.norm' % _input

	def toUnicode(self, value, encoding='gbk'):
		valType = type(value)
		if valType is str:
			return value.decode(encoding)
		if valType is int or valType is float:
			return str(value).decode(encoding)
		return value


	def normMin(self, value):
		#print type(value), value
		value = self.toUnicode(value)
		if not re.match(u'[0-9\.]', value) or len(value) <= 1 or len(value) > 3:
			value = u'暂无'
		if value != u'暂无' and value.find(u'分钟') == -1:
			value = u'%s分钟' % value
		return value

	def normDesc(self, value):
		value = self.toUnicode(value)
		if value.startswith('"'):
			value = value[1:]
		if value.endswith('"'):
			value = value[:-1]
		value = re.sub('""', '"', value)
		return value
		


	def norm(self):
		df = pd.read_table(self.input, names = MovieDetailKey, encoding='gbk')
		# 去重
		df = df.drop_duplicates(['movieid'])
		# 归一化时长
		df['duration'] = df['duration'].map(self.normMin)
		# 归一化简介
		df['description'] = df['description'].map(self.normDesc)
		
		df.to_csv(self.output, sep='\t', header=None, index=False, encoding='gbk')



movieDetailNorm = MovieDetailNormalizer(sys.argv[1])
movieDetailNorm.norm()







