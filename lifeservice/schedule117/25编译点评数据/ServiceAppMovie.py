#!/bin/bash
#coding=gb2312

import re, sys, datetime, logging
import types
import keyConfig
import Normalizer
from DateTimeTool import DateTimeTool as dateTool
from keyConfig import *
import logging

# 定义日志
today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='logs/ServiceAppMovie_' + today + '.log',
                filemode='w+')

def normalKey(key):
	key = re.sub('(^[\s ]+|[\s ]+$)', '', key)
	return key

def normalVal(val):
	val = re.sub('\t', ' ', val)
	return val



# ============= 评论Item类 =================
class Item:
	''' content item '''
	URL, TITLE = 'url', 'title'
	
	# 构造函数
	def __init__(self, url='', title=''):
		self.content = dict()
		if len(url) > 0:
			self.content[self.URL] = url
		if len(title) > 0:
			self.content[self.TITLE] = title
	
	# 插入一行数据
	def insertLine(self, keyList, vals):
		if type(vals) is types.StringType:
			valSegs = vals.split('\t')
			if len(keyList) != len(valSegs):
				logging.error('Item init for %s error!' % vals)
				return
			for idx in xrange(0, len(keyList)):
				key, val = keyList[idx], valSegs[idx]
				self.insert(key, val)
		elif type(vals) is types.ListType:
			if len(keyList) == 0 or len(vals) == 0:
				logging.error('Item init error, is empty!')
				return
			for idx in xrange(0, len(keyList)):
				key, val = keyList[idx], vals[idx]
				self.insert(key, val)

	# 插入数据
	def insert(self, key, val):
		if val == '暂无':
			return
		if len(key) != 0 and len(val) != 0:
			#self.content[key] = val
			if key not in self.content:
				self.content[key] = []
			if val not in self.content[key]:
				self.content[key].append(val)

	# 将List的结果抓成字符串
	def list2String(self, list = []):
		separator = '###'
		return separator.join(list)


	# 转成字符串
	def toString(self, keyList):
		resultStr = ''
		if self.URL not in self.content or self.TITLE not in self.content:
			return resultStr
		if len(self.content) < 3:		# 即只有url, title
			return resultStr
		for key in keyList:
			tmpVal = ''
			# url, title是字符串，其他属性是List
			if key == self.URL or key == self.TITLE:
				tmpVal = self.content[key]
			elif key in self.content:
				tmpVal = self.list2String(self.content[key])
			if len(resultStr) == 0:
				resultStr = tmpVal
			else:
				resultStr += '\t' + tmpVal
		return resultStr



# ============ 转换类 转table格式=================
class Transfer:
	''' 转换类 '''

	URL, TITLE = 'url', 'title'
	#def __init__(self, tableKeyName):
	def __init__(self):
		self.Item = None
		logging.info('Transfer init.')
	

	# 根据文件名获取输出table格式的key
	def getTableKey(self, fileName):
		# recomfoodKey
		# food_dianping_detail.baseinfo.norm
		fileName = re.sub('.norm', '', fileName)
		fileName = re.sub('.*\.', '', fileName)
		return fileName + 'Key'


	# 打印评论的字段信息
	def printTableHead(self, fd):
		head = ''
		for key in self.tableKey:
			head = head + '\t' + key
		fd.write(head[1:] + '\n')
	
	# 打印评论的实际内容
	def printItem(self, fd):
		if self.Item is not None:
			itemLine = self.Item.toString(self.tableKey)
			if len(itemLine) > 0:
				fd.write(itemLine + '\n')
		

	# 转原始文件到table格式
	# input: key-value格式原始文件
	def transferTableFormat(self, input):
		tableKeyName = self.getTableKey(input)
		if not hasattr(keyConfig, tableKeyName):
			logging.error('transfer for %s is error! has not table key: %s' % (input, tableKeyName))
			return	
		self.tableKey = getattr(keyConfig, tableKeyName)
		# 打印表头
		output = re.sub('.norm', '.table', input)
		outputFD = open(output, 'w')
		self.printTableHead(outputFD)
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = normalKey(segs[0]), normalVal(segs[1])
			if key == self.URL:
				url = val
			elif key == self.TITLE:
				title = val;
				self.printItem(outputFD)
				self.Item = Item(url, title)
			else:
				self.Item.insert(key, val)
					
		self.printItem(outputFD)
		outputFD.close()
		logging.info('transfer raw file [%s] to table format [%s]' % (input, output))




# =======  归一化类  ================
class Normalize:
	''' 归一化类 '''
	
	def __init__(self, normConf):
		self.normConf = normConf
		self.normFunDict = dict()
		self.loadNormConf()
	
	# 加载归一化配置文件
	def loadNormConf(self):
		for line in open(self.normConf):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 3:
				continue
			fileType, key, normFun = segs[0], segs[1], segs[2]
			#print hasattr(Normalizer, normFun)

			if not hasattr(Normalizer, normFun):
				logging.error('%s is error, has no %s normal function' % (line, normFun))
				continue
			self.normFunDict[fileType + '\t' + key] = getattr(Normalizer, normFun)
			#print fileType + '\t' + key, getattr(Normalizer, normFun)

	# 归一化
	def normalize(self, input):
		fileType = re.sub('.*\.', '', input)
		outputFD = open(input + '.norm', 'w')
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = segs[0], segs[1]
			normKey = fileType + '\t' + key
			if normKey not in self.normFunDict:
				outputFD.write(('%s\n') % line)
				continue
			#print normKey

			normKVDict = self.normFunDict[normKey](key, val)
			for key in normKVDict:
				if len(key) != 0 and len(normKVDict[key]) != 0:
					outputFD.write(('%s\t%s\n') % (key, normKVDict[key]))
		outputFD.close()





# ======== 基本信息类 ===============
class BaseInfo:
	''' 旅游的基本信息类 '''

	URL, TITLE = 'url', 'title'
	CRUMB, CRUMBTYPE = 'breadcrumb', 'crumbType'

	def __init__(self):
		logging.info('BaseInfo init.')
		self.baseInfoItem = None


	# 打印基本信息的字段信息
	def printHead(self, keyList, fd):
		head = ''
		for key in keyList:
			head = head + '\t' + key
		fd.write(head[1:] + '\n')


	# 打印基本信息的实际内容
	def printBaseInfoContent(self, fd):
		if self.baseInfoItem is not None:
			baseInfoStr = self.baseInfoItem.toString(baseinfoKey)
			if len(baseInfoStr) > 0:
				fd.write(baseInfoStr + '\n')
	

	# 加载旅游的基本信息数据文件，转换成入库的格式
	def loadBaseInfoFile(self, input, output):
		outputFD = open(output, 'w')
		self.printHead(baseinfoKey, outputFD)
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = normalKey(segs[0]), segs[1]
			if key == self.URL:
				url = val
			elif key == self.TITLE:
				title = val;
				self.printBaseInfoContent(outputFD)
				self.baseInfoItem = Item(url, title)
			elif key == self.CRUMB:
				crumbType = self.getType(val)
				self.baseInfoItem.insert(key, val)
				if len(crumbType) != 0:
					self.baseInfoItem.insert(self.CRUMBTYPE, crumbType)
				else:
					logging.error('%s crumb error!' % url)
			else:
				self.baseInfoItem.insert(key, val)
					
		self.printBaseInfoContent(outputFD)
		outputFD.close()
		self.baseInfoItem = None
		logging.info('transfer raw baseinfo file [%s] to [%s]' % (input, output))

	
	# 从keyList中找出findKey的index
	def getKeyIndex(self, keyList, findKey):
		keyIndex = -1
		for idx in xrange(0, len(keyList)):
			if keyList[idx] == findKey:
				keyIndex = idx;  break
		return keyIndex

	
	# 根据面包线中的提示，计算其分类
	def getType(self, crumb):
		if crumb.find('美食') != -1:
			return 'food'
		elif crumb.find('景点') != -1:
			return 'sight'
		elif crumb.find('购物') != -1:
			return 'shop'
		elif crumb.find('餐厅') != -1:
			return 'food'
		else:
			return 'food'


	# 切分基本信息数据到（美食、购物、景点三部分）
	# input: baseinfo合集数据
	# 输出:  $input.food  $input.sight  $input.shop
	def splitBaseInfoFile(self, input):
		isHeadLine = True
		crumbTypeIdx = self.getKeyIndex(baseinfoKey, self.CRUMBTYPE)
		foodFD = open(input + '.food', 'w')
		shopFD = open(input + '.shop', 'w')
		sightFD = open(input + '.sight', 'w')
		self.printHead(foodKey, foodFD)	
		self.printHead(shopKey, shopFD)	
		self.printHead(sightKey, sightFD)	
	


		item = None
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if isHeadLine:
				isHeadLine = False;  continue
			if len(segs) != len(baseinfoKey):
				continue

			item = Item()
			item.insertLine(baseinfoKey, segs)
			# 根据类型，写入不同的分类文件中
			outputFD = None; keyList = []
			if segs[crumbTypeIdx] == 'food':
				outputFD, keyList = foodFD, foodKey
			elif segs[crumbTypeIdx] == 'shop':
				outputFD, keyList = shopFD, shopKey
			elif segs[crumbTypeIdx] == 'sight':
				outputFD, keyList = sightFD, sightKey
			
			#print segs[crumbTypeIdx]
			resultStr = item.toString(keyList)
			if outputFD is not None and len(resultStr) > 0:
				outputFD.write(resultStr + '\n')

		foodFD.close(); shopFD.close(); sightFD.close()






# ======= 服务app 原始数据拆分器 ===========
class Partitioner:
	''' 旅游类 '''
	def __init__(self):
		logging.info('Partitioner init.')

	# 将原始文件切分成基本信息/评论等部分
	def partition(self, input):
		baseInfoFD = open(input + '.baseinfo', 'w')
		commentFD = open(input + '.comment', 'w')
		recommFoodFD = open(input + '.recomfood', 'w')
		tuanFD = open(input + '.tuan', 'w')
		
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = segs[0], segs[1]
			if key in baseinfoKey:
				baseInfoFD.write(line + '\n')
			if key in commentKey:
				commentFD.write(line + '\n')
			if key in recomfoodKey:
				recommFoodFD.write(line + '\n')
			if key in tuanKey:
				tuanFD.write(line + '\n')
		baseInfoFD.close()
		commentFD.close()
		recommFoodFD.close()
		tuanFD.close()
		logging.info('partition for [%s] done.' % input)








# ====== 过滤类 过滤一些不符合要求的数据 ================
class Filter:
	''' 根据一定的条件过滤 '''

	ChineseRegx = u'.*[\u4e00-\u9fa5]+'
	CommentIdx = 9

	def __init__(self):
		logging.info('Filter init.')


	# 过滤纯英文评论，即过滤不包含中文的评论
	def filterEnglishComment(self, input, output):
		isFirstLine = True
		outputFD = open(output, 'w')
		for line in open(input):
			line = line.strip('\n').decode('gbk', 'ignore')
			if isFirstLine:
				outputFD.write((line + '\n').encode('gbk', 'ignore'))
				isFirstLine = False
			segs = line.split('\t')
			comment = segs[self.CommentIdx]
			if not re.match(self.ChineseRegx, comment):
				logging.error('english comment')
				continue
			outputFD.write((line + '\n').encode('gbk', 'ignore'))
		outputFD.close()

	# 对攻略数据进行过滤
	def filterTrevelNote(self, input, output):
		urlIdx, titleIdx, dateIdx = 0, 1, 4
		tagIdx, essenceIdx, contentIdx = 7, 8, 12
		FilterDate = '2014-06-30'

		isFirstLine = True
		outputFD = open(output, 'w')
		for line in open(input):
			line = line.strip('\n').decode('gbk', 'ignore')
			if isFirstLine:
				#outputFD.write((line + '\tbreadcrumb\n').encode('gbk', 'ignore'))
				outputFD.write((line + '\n').encode('gbk', 'ignore'))
				isFirstLine = False
			segs = line.split('\t')
			if len(segs) != len(travelNoteKey):
				continue
			url, title, date = segs[urlIdx], segs[titleIdx], segs[dateIdx]
			tags, essence, content = segs[tagIdx], segs[essenceIdx], segs[contentIdx]
			
			if date < FilterDate:
				logging.error(('too old nores\t' + url))
				continue

			outputFD.write((line + '\n').encode('gbk', 'ignore'))
			# ====================================================
			#  这里需要抽取出来，因为其他地方也需要
			# ====================================================
			#if title.find(u'东京')==-1 and tags.find(u'东京')==-1:
			#	logging.error(('not tokyo notes\t' + url))
			#	continue
			#outputFD.write((line + u'\t东京攻略\n').encode('gbk', 'ignore'))


			# 抽取第一张图片作为封面
			#photoRegex = u'.*?<img[^>]*?src="([^>]*?(.jpeg|680x))"[^>]*?>'
			#coverPhoto = ''
			#matches = re.match(photoRegex, content)
			#if matches is not None:
			#	coverPhoto = matches.group(1)

			#outputFD.write((line + '\t' + coverPhoto + '\n').encode('gbk', 'ignore'))
		outputFD.close()




# 为每个景点/店铺 添加国家，城市
class Classfier:
	''' 对每个poi进行分类 '''

	def __init__(self, countryCityConf):
		logging.info('Classfier init.')
		self.ccMapConf = countryCityConf
		self.ccDict = dict()
		self.loadCountryCityMap()



	# 加载<国家 城市>映射表
	def loadCountryCityMap(self):
		for line in open(self.ccMapConf):
			line = line.strip('\n').decode('gbk', 'ignore')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			country, city = segs[0], segs[1]
			if country not in self.ccDict:
				self.ccDict[country] = []
			if city not in self.ccDict[country]:
				self.ccDict[country].append(city)


	# 根据面包线获取城市列表
	def getCitys(self, crumb):
		country, cityList = '', []
		segs = crumb.split('>')

		itemList = []
		for item in segs:
			item = re.sub(u'(美食|景点|购物|餐厅)', '', item.strip())
			if item not in itemList:
				itemList.append(item)

		# get country
		for item in itemList:
			if item in self.ccDict:
				country = item
				break

		# get citys
		if len(country) != 0:
			for item in itemList:
				if item in self.ccDict[country]:
					cityList.append(item)	

		return country, ','.join(cityList)

	
	# 为数据添加
	def addCountryCityRow(self, dataFile, outFile, crumbIdx=10):
		isFirstLine = True
		outputFD = open(outFile, 'w')
		for line in open(dataFile):
			line = line.strip('\n').decode('gbk', 'ignore')
			country , citys = '', ''
			if isFirstLine:
				country, citys = 'country', 'city'
				isFirstLine = False
			else:
				segs = line.split('\t')
				crumb = segs[crumbIdx - 1]
				country, citys = self.getCitys(crumb)

			#print country, citys
			outputFD.write((line + '\t' + country + '\t' + citys + '\n').encode('gbk', 'ignore'))
		outputFD.close()
				






if __name__ == '__main__':

	opt = sys.argv[1]
	# 对旅游数据切分
	if opt == '-partition':
		input = sys.argv[2]
		partitioner = Partitioner()
		partitioner.partition(input)

	elif opt == '-table':
		input = sys.argv[2]
		transfer = Transfer()
		transfer.transferTableFormat(input)

	elif opt == '-normal':
		input, normConf = sys.argv[2], sys.argv[3]
		normalizer = Normalize(normConf)
		normalizer.normalize(input)

	# 将基本信息转成入库格式
	elif opt == 'LOADCOMMENT':
		input, output = sys.argv[2], sys.argv[3]
		comment = Comment()
		comment.loadCommentFile(input, output)
	# 将评论信息转成入库格式
	elif opt == 'LOADBASEINFO':
		input, output = sys.argv[2], sys.argv[3]
		baseInfo = BaseInfo()
		baseInfo.loadBaseInfoFile(input, output)
	# 将基本信息分成 购物/美食/景点
	elif opt == 'SPLITBASEINFO': 
		input = sys.argv[2]
		baseInfo = BaseInfo()
		baseInfo.splitBaseInfoFile(input)
	# 将攻略类文件转成入库文件格式
	elif opt == 'LOADTRAVELNOTE':
		input, output = sys.argv[2], sys.argv[3]
		travelNote = TravelNote()
		travelNote.loadTravelNoteFile(input, output)
	# 评论去除不包含中文的
	elif opt == 'FILTERENCOMMENT':
		input, output = sys.argv[2], sys.argv[3]
		filter = Filter()
		filter.filterEnglishComment(input, output)
	# 过滤不符合要求的攻略数据
	elif opt == 'FILTERNOTES':
		input, output = sys.argv[2], sys.argv[3]
		filter = Filter()
		filter.filterTrevelNote(input, output)
	# 为数据添加国家，城市字段
	elif opt == 'ADDCITY':
		classfier = Classfier('conf/country_city_map')
		input, output = sys.argv[2], sys.argv[3]
		if len(sys.argv) > 4:
			crumbIdx = sys.argv[4]
			classfier.addCountryCityRow(input, output, crumbIdx)
		else:
			classfier.addCountryCityRow(input, output)

