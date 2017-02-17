#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-14 16:09
# * Filename	 : bin/build_dianping_invalidshop.py
# * Description	 : 调用大众点评网的API确认shop是否是无效的
# * *****************************************************************************/

import hashlib, urllib, urllib2, time, logging
from HttpRequestTool import *
import simplejson as json
from JsonTool import JsonToLine
from DateTimeTool import DateTimeTool

ENCODE = 'GB2312'
DealCntPerRequest = 40

# 返回值中json键
OK = 'OK'
StatusKey = 'status'
CountKey = 'count'
BusinessesKey = 'businesses'
IDKey = 'business_id'

DealListKey = 'id_list'
DealDetailKey = 'deals'

# 大众点评API的签名信息
AppKey = '343629756'
Secret = 'a54b3d169af14dd688b56fe9c4b9ebcc'

# API
GetShopInfoAPI = 'http://api.dianping.com/v1/business/get_batch_businesses_by_id'


def initLog(logFile = 'log'):
	logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='logs/' + logFile + '.' + DateTimeTool.today() + '.log',
                filemode='a')


# 计算签名
def createSign(paramMap):
	codec = AppKey
	for key in sorted(paramMap.iterkeys()):
		codec += key + paramMap[key]
	codec += Secret
	sign = (hashlib.sha1(codec).hexdigest()).upper()
	return sign

# 计算获取一系列店铺的详情
def create_get_shopinfo_api(shopids):
	paramSet = []
	paramSet.append(("business_ids", shopids))
	paramMap = dict()
	for pair in paramSet:
		paramMap[pair[0]] = pair[1]

	sign = createSign(paramMap)
	urlTrail = 'appkey=%s&sign=%s' % (AppKey, sign)

	for pair in paramSet:
		urlTrail += ('&%s=%s' % (pair[0], pair[1]))
	requesturl = '%s?%s' % (GetShopInfoAPI, urlTrail)
	return requesturl




# 获取一个城市的无效的shopids
def get_invalid_shopids(input, output):
	fd = open(output, 'w+')
	for line in open(input):
		line = line.strip('\n')
		if len(line) == 0:
			continue
		idsegs = line.split(',')
		idCount = len(idsegs)

		requestUrl = create_get_shopinfo_api(line)
		logging.info('get shop info url: ' + requestUrl)
		response = GetRequest(requestUrl)
		jsonDict = json.loads(response)
	
		invalidList = []
		validList = []
		if StatusKey in jsonDict and jsonDict[StatusKey] == OK:
			if CountKey in jsonDict and jsonDict[CountKey] == idCount:
				return
			if BusinessesKey not in jsonDict:
				return
			for item in jsonDict[BusinessesKey]:
				if IDKey not in item:
					continue				
				validList.append(str(item[IDKey]))
		# get invalid ids
		for id in idsegs:
			if id not in validList:
				invalidList.append(id)
		fd.write("%s\n" % '\n'.join(invalidList))
		time.sleep(1)
	fd.close()




if len(sys.argv) < 3:
	print '[Usage]: python %s input output' % sys.argv[0]
	sys.exit(-1)
input, output = sys.argv[1], sys.argv[2]

initLog('invalid_shops')
get_invalid_shopids(input, output)
