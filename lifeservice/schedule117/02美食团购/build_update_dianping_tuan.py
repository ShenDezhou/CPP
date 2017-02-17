#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-14 16:09
# * Filename	 : build_dianping_tuan.py
# * Description	 : 调用大众点评网的API更新团购信息
# * *****************************************************************************/

import hashlib, urllib, urllib2, time, logging
from HttpRequestTool import *
import simplejson as json
from JsonTool import JsonToLine
from DateTimeTool import DateTimeTool
from multiprocessing.dummy import Pool as ThreadPool
import os.path


ENCODE = 'GB2312'
DealCntPerRequest = 40

# 返回值中json键
OK = 'OK'
StatusKey = 'status'
DealListKey = 'id_list'
DealDetailKey = 'deals'

# 大众点评API的签名信息
AppKey = '343629756'
Secret = 'a54b3d169af14dd688b56fe9c4b9ebcc'

# API
GetDealListAPI = 'http://api.dianping.com/v1/deal/get_all_id_list'
GetDealDetailAPI = 'http://api.dianping.com/v1/deal/get_batch_deals_by_id'
GetDealListFileAPI = 'http://i3.dpfile.com/data/open/deals/id/%s/%s-%s.json'
GetDailyDealListAPI = 'http://api.dianping.com/v1/deal/get_daily_new_id_list'
GetUpdateDealListAPI = 'http://api.dianping.com/v1/deal/get_incremental_id_list'

# 用到的配置文件
CityConf = 'conf/dianping_city_code_pinyin_conf'
CategoryConf = 'conf/dianping_category'

DealExtractConf = 'conf/dianping_deal_conf'
UpdateIDExtractConf = 'conf/dianping_update_idlist_conf'

# 默认获取一个上次update的时间
UpdateLastHour = 10



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



# 计算获取更新团购ID的API接口
def create_get_city_update_deal_list_api(cityChName, lastUpdateTime='', category=''):
	if len(lastUpdateTime) == 0:
		lastUpdateTime = DateTimeTool.nHoursAgoStr(UpdateLastHour)
	if len(category) == 0:
		category = u'美食,休闲娱乐,生活服务,丽人,亲子'

	paramSet = []
	paramSet.append(("city", cityChName.encode('utf8', 'ignore')))
	paramSet.append(("begin_time", lastUpdateTime))
	paramSet.append(("category", category.encode('utf8', 'ignore')))
	paramMap = dict()
	for pair in paramSet:
		paramMap[pair[0]] = pair[1]

	sign = createSign(paramMap)
	urlTrail = 'appkey=%s&sign=%s' % (AppKey, sign)

	for pair in paramSet:
		urlTrail += ('&%s=%s' % (pair[0], queryUrlEncode(pair[1])))
	requesturl = '%s?%s' % (GetUpdateDealListAPI, urlTrail)
	return requesturl








# 计算获取一个城市团购列表的URL
def create_get_city_deal_list_api(cityChName):
	paramSet = []
	paramSet.append(("city", cityChName.encode('utf8', 'ignore')))
	paramMap = dict()
	for pair in paramSet:
		paramMap[pair[0]] = pair[1]

	sign = createSign(paramMap)
	urlTrail = 'appkey=%s&sign=%s' % (AppKey, sign)

	for pair in paramSet:
		urlTrail += ('&%s=%s' % (pair[0], queryUrlEncode(pair[1])))
	requesturl = '%s?%s' % (GetDealListAPI, urlTrail)
	return requesturl


# 计算获取一串团购id列表详情信息的URL
def create_get_deal_detail_api(idsStr):
	paramSet = []
	paramSet.append(("deal_ids", idsStr))
	paramMap = dict()
	for pair in paramSet:
		paramMap[pair[0]] = pair[1]

	sign = createSign(paramMap)
	urlTrail = 'appkey=%s&sign=%s' % (AppKey, sign)

	for pair in paramSet:
		urlTrail += ('&%s=%s' % (pair[0], pair[1]))
	requesturl = '%s?%s' % (GetDealDetailAPI, urlTrail)
	return requesturl


# 计算获取每日新增团购id列表信息的URL
def create_get_city_daily_deal_list_api(cityChName, date=''):
	if len(date) == 0:
		date = DateTimeTool.todayStr()
	paramSet = []
	paramSet.append(("city", cityChName.encode('utf8', 'ignore')))
	paramSet.append(("date", date))
	paramMap = dict()
	for pair in paramSet:
		paramMap[pair[0]] = pair[1]

	sign = createSign(paramMap)
	urlTrail = 'appkey=%s&sign=%s' % (AppKey, sign)

	for pair in paramSet:
		urlTrail += ('&%s=%s' % (pair[0], queryUrlEncode(pair[1])))
	requesturl = '%s?%s' % (GetDailyDealListAPI, urlTrail)
	return requesturl



# 获取城市列表
def get_city_list(cityConf, inEncode='gb2312'):
	logging.info('get city list done.')
	cityList = []
	for line in open(cityConf):
		segs = line.strip('\n').decode(inEncode, 'ignore').split('\t')
		if len(segs) != 3:
			continue
		cityChName, cityCode, cityEnName = segs[0], segs[1], segs[2]
		cityList.append((cityChName, cityCode, cityEnName))
	return cityList


# 获取类别列表
def get_category_list(categoryConf, inEncode='gb2312'):
	logging.info('get category list done.')
	categoryList = []
	for line in open(categoryConf):
		line = line.strip('\n').decode(inEncode, 'ignore')
		if line.startswith('#'):
			continue
		categoryList.append(line)
	return categoryList



# 获取一串团购列表的详情
def get_deal_detail(dealList, fd, jsonTool):
	if isinstance(dealList, list):
		dealList = ','.join(dealList)

	requestUrl = create_get_deal_detail_api(dealList)
	logging.info('get ' + requestUrl + ' done.')
	#time.sleep(1)
	try:
		response = GetRequest(requestUrl)
		jsonDict = json.loads(response)
		if StatusKey in jsonDict and jsonDict[StatusKey] == OK:
			logging.info('deal count: %s' % str(jsonDict['count']))
			for deal in jsonDict[DealDetailKey]:
				for line in jsonTool.parse(deal):
					fd.write('%s\n' % line)
	except:
		logging.error('%s' % requestUrl)
		return


# 计算有更新的团购ID的文件路径
def getUpdateIDsFilePath(cityEnName):
	return 'tuandata/update/%s.ids' % cityEnName

# 计算更新的团购信息文件路径
def getUpdateDealFilePath(cityEnName):
	return 'tuandata/update/%s.data' % cityEnName


def get_update_idlist_imp(cityUrlPair):
	if len(cityUrlPair) != 2:
		return
	cityEnName, requestUrl = cityUrlPair
	logging.info('begin to get %s\'s update tuan idlist' % cityEnName)
	try:
		response = GetRequest(requestUrl)
		jsonDict = json.loads(response)
		if StatusKey not in jsonDict or jsonDict[StatusKey] != OK:
			logging.info('download %s \'s update deal failed' % cityEnName)
			return
		if DealListKey not in jsonDict:
			logging.info('%s \'s return json not contain %s' % (cityEnName, DealListKey))
			return

		jsonTool = JsonToLine(UpdateIDExtractConf, ENCODE)
		updateidsList = jsonTool.parse(jsonDict[DealListKey])
		if len(updateidsList) == 0:
			logging.info('%s \'s update ids is empty' % cityEnName)
			return
		fd = open(getUpdateIDsFilePath(cityEnName), 'w+')
		for line in updateidsList:
			fd.write('%s\n' % line)
		fd.close()
	except:
		pass
		

# 获取各城市状态有变化的团购ID列表
def get_update_idlist(updateUrlList):
	if len(updateUrlList) == 0:
		return
	
	pool = ThreadPool(20)
	pool.map(get_update_idlist_imp, updateUrlList)
	pool.close()
	pool.join()
	logging.info('get all city\'s update deal ids done.')

	


# 获取需要计算的所有的更新链接URL列表
def get_all_update_tuan_urllist(cityList, categoryList, lastUpdateTime=''):
	updateUrlList = []
	for cityChName, cityCode, cityEnName in cityList:
		for category in categoryList:
			requestUrl = create_get_city_update_deal_list_api(cityChName, lastUpdateTime, category)
			updateUrlList.append((cityEnName, requestUrl))
	return updateUrlList


def extract_update_idlist(updateIDFile):
	updateIDList = []
	if not os.path.exists(updateIDFile):
		return updateIDList
	# 抽取状态值为2,即发生变化的id
	idList = []
	for line in open(updateIDFile):
		segs = line.strip('\n').split('\t')
		if len(segs) != 2:
			continue
		id, status = segs
		if status != '2':
			continue
		idList.append(id)
	# 每40个组成一组
	requestCnt = len(idList) / DealCntPerRequest + 1
	for requestIdx in xrange(0, requestCnt):
		begin = requestIdx * DealCntPerRequest
		idGroup = ','.join(idList[begin : begin + DealCntPerRequest])
		updateIDList.append(idGroup)
	return updateIDList


def get_update_deals_imp(cityFilePair):
	if len(cityFilePair) != 2:
		return
	cityEnName, updateIDFile = cityFilePair

	updateDealList = extract_update_idlist(updateIDFile)

	if (updateDealList) == 0:
		return

	logging.info('begin to get %s\'s all update deal...' % cityEnName)
	dealFile = getUpdateDealFilePath(cityEnName)
	fd = open(dealFile, 'w+')
	jsonTool = JsonToLine(DealExtractConf, ENCODE)
	for dealListStr in updateDealList:
		get_deal_detail(dealListStr, fd, jsonTool)
	fd.close()
	logging.info('get %s\'s all deal done.' % cityEnName)


# 根据有更新的团购ID列表，获取有更新的团购数据
def get_update_deals(cityList):
	logging.info('begin to get all city\'s update deals ...')
	cityFileList = []
	for cityChName, cityCode, cityEnName in cityList:
		filePath = getUpdateIDsFilePath(cityEnName)
		cityFileList.append((cityEnName, filePath))
	if len(cityFileList) == 0:
		return
	pool = ThreadPool(20)
	pool.map(get_update_deals_imp, cityFileList)
	pool.close()
	pool.join()
	logging.info('get all city\'s update deals done.')
		


# 获取各城市有更新变化的团购数据
def get_update_city_daily_deal(cityConf, lastUpdateTime='', inEncode='gb2312'):
	# 各城市
	#0：已下线，1：新上线，2：内容变更
	cityList = get_city_list(cityConf, inEncode)
	categoryList = get_category_list(CategoryConf, inEncode)

	updateUrlList = get_all_update_tuan_urllist(cityList, categoryList, lastUpdateTime)

	get_update_idlist(updateUrlList)

	get_update_deals(cityList)



initLog('update_tuan')
get_update_city_daily_deal(CityConf)




