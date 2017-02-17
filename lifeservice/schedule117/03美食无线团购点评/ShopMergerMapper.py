#!/usr/bin/env python
#coding=gb2312

import sys, os

# 全局常量
ENCODING = 'GB2312'

# 以city为key进行map
for line in sys.stdin:
	segs = line.strip('\n').decode(ENCODING, 'ignore').split('\t')
	if len(segs) < 7:
		continue
	city = segs[0]
	print ('%s\t%s' % (city, '\t'.join(segs[1:]))).encode(ENCODING, 'ignore')
