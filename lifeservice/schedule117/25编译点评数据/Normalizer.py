#!/bin/python
#coding=gb2312
import re, time


# 大众点评的餐馆面包线的归一化类
class DPCrumbNormlizer:
	# 娱乐类别的配置
	playTypeConf = "conf/dianping_play_type_conf"
	# 面包线第一列后缀
	crumbSuffixConf = "conf/dianping_crumb_suffix_conf"

	def __init__(self, conf):
		# 保留背个城市的 商圈 菜系这些数据
		self.CITY_KEY = 'city'
		self.AREA_KEY = 'area'
		self.CUISINE_KEY = 'cuisine'
		self.SUB_CUISINE_KEY = 'subcuisine'
		self.DISTRICT_KEY = 'district'
		# 娱乐相关的类别字段
		self.TYPE_KEY = 'type'
		self.SUBTYPE_KEY = 'subtype'

		self.crumbSuffixList = []
		self.cityBusinessConf = dict()
		self.playTypeConfDict = dict()
		self.cityBusinessAreaDict = dict()

		self.loadCityBusinessConf(conf)
		self.loadCrumbSuffixConf(self.crumbSuffixConf)
		self.loadPlayTypeConf(self.playTypeConf)


	# 加载大众点评的面包线首列的后缀
	def loadCrumbSuffixConf(self, conf):
		for line in open(conf):
			suffix = line.strip('\n')
			if suffix not in self.crumbSuffixList:
				self.crumbSuffixList.append(suffix)


	# 加载休闲娱乐类的配置文件
	def loadPlayTypeConf(self, conf):
		for line in open(conf):
			segs = line.strip('\n').split('\t')
			if len(segs) < 2: continue
			type, subType = segs[0], segs[1]
			if subType not in self.playTypeConfDict:
				self.playTypeConfDict[subType] = type


	# 加载商圈的配置文件
	def loadCityBusinessConf(self, conf):
		for line in open(conf):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) < 2:
				continue
			if segs[0] == 'business':
				if len(segs) < 6:
					continue
				city, area, district = segs[1], segs[2], segs[5]
				if city not in self.cityBusinessConf:
					cityDict = dict()
					self.cityBusinessConf[city] = cityDict
				# 添加 area
				if self.AREA_KEY not in self.cityBusinessConf[city]:
					areaList = []
					self.cityBusinessConf[city][self.AREA_KEY] = areaList
				if area not in self.cityBusinessConf[city][self.AREA_KEY]:
					self.cityBusinessConf[city][self.AREA_KEY].append(area)
				# 添加 district
				if self.DISTRICT_KEY not in self.cityBusinessConf[city]:
					districtList = []
					self.cityBusinessConf[city][self.DISTRICT_KEY] = districtList
				if district not in self.cityBusinessConf[city][self.DISTRICT_KEY]:
					self.cityBusinessConf[city][self.DISTRICT_KEY].append(district)
				# [city\tbusiness] = area
				key = '%s\t%s' % (city, district)
				self.cityBusinessAreaDict[key] = area



			elif segs[0] == 'cook':
				if len(segs) < 4:
					continue
				city, cuisine = segs[1].strip(), segs[3].strip()
				if city not in self.cityBusinessConf:
					ciryDict = dict()
					self.cityBusinessConf[city] = cityDict
				if self.CUISINE_KEY not in self.cityBusinessConf[city]:
					cuisineList = []
					self.cityBusinessConf[city][self.CUISINE_KEY] = cuisineList
				# 添加 cuisine
				if cuisine not in self.cityBusinessConf[city][self.CUISINE_KEY]:
					self.cityBusinessConf[city][self.CUISINE_KEY].append(cuisine)
		 

		#print ','.join(self.cityBusinessConf[city][self.CUISINE_KEY])


	def getCity(self, crumb):
		crumb = crumb.strip()
		city = crumb
		for suffix in self.crumbSuffixList:
			if crumb.endswith(suffix):
				city = re.sub(suffix, '', crumb)
				break
		return city


	def normlizeRestaurantCrumb(self, crumbKey, crumbVal):
		#print crumbVal
		normKVDict = dict()
		normKVDict[crumbKey] = crumbVal
		valSegs = crumbVal.split('>')
		city = self.getCity(valSegs[0])
		if city not in self.cityBusinessConf:
			return normKVDict
		normKVDict[self.CITY_KEY] = city

		cityConfMap = self.cityBusinessConf[city]		
		# set default value
		normKVDict[self.AREA_KEY] = '其他'
		normKVDict[self.CUISINE_KEY] = '其他'
		normKVDict[self.DISTRICT_KEY] = '其他'
		
		#cuisineIdx = -1
		#for idx in xrange(1, len(valSegs)):
		#	val = valSegs[idx].strip()
		#	if val in cityConfMap[self.AREA_KEY]:
		#		normKVDict[self.AREA_KEY] = val	
		#	elif val in cityConfMap[self.CUISINE_KEY]:
		#		normKVDict[self.CUISINE_KEY] = val	
		#		cuisineIdx = idx
		#	elif val in cityConfMap[self.DISTRICT_KEY]:
		#		normKVDict[self.DISTRICT_KEY] = val	
		# 如果菜系的idx 与 店铺的idx 相差2 说明有子菜系在中间
		#if cuisineIdx == len(valSegs) -3:
		#	normKVDict[self.SUB_CUISINE_KEY] = valSegs[cuisineIdx + 1].strip()

		# 新的解析方式
		cuisineIdx = -1
		for idx in xrange(1, len(valSegs)):
			val = valSegs[idx].strip()
			# 区
			if val in cityConfMap[self.AREA_KEY]:
				normKVDict[self.AREA_KEY] = val	
			# 商圈
			elif val in cityConfMap[self.DISTRICT_KEY]:
				normKVDict[self.DISTRICT_KEY] = val	
				key = '%s\t%s' % (city, val)
				if key in self.cityBusinessAreaDict:
					normKVDict[self.AREA_KEY] = self.cityBusinessAreaDict[key]
			# 菜单
			elif val in cityConfMap[self.CUISINE_KEY]:
				normKVDict[self.CUISINE_KEY] = val	
				cuisineIdx = idx
				# 子菜单
				if cuisineIdx == len(valSegs) -3:
					normKVDict[self.SUB_CUISINE_KEY] = valSegs[cuisineIdx + 1].strip()
					break
	
		
		return normKVDict
		

	def normlizePlayCrumb(self, crumbKey, crumbVal):
		#print crumbVal
		normKVDict = dict()
		normKVDict[crumbKey] = crumbVal
		valSegs = crumbVal.split('>')
		city = self.getCity(valSegs[0])
		if city not in self.cityBusinessConf:
			return normKVDict
		normKVDict[self.CITY_KEY] = city

		cityConfMap = self.cityBusinessConf[city]
		# set default value
		normKVDict[self.AREA_KEY] = '其他'
		normKVDict[self.TYPE_KEY] = '其他'
		normKVDict[self.SUBTYPE_KEY] = '其他'
		normKVDict[self.DISTRICT_KEY] = '其他'

		for idx in xrange(1, len(valSegs)):
			val = valSegs[idx].strip()
			if val in cityConfMap[self.AREA_KEY]:
				normKVDict[self.AREA_KEY] = val	
			elif val in self.playTypeConfDict:
				normKVDict[self.SUBTYPE_KEY] = val	
			elif val in cityConfMap[self.DISTRICT_KEY]:
				normKVDict[self.DISTRICT_KEY] = val	
		if normKVDict[self.SUBTYPE_KEY] in self.playTypeConfDict:
			normKVDict[self.TYPE_KEY] = self.playTypeConfDict[normKVDict[self.SUBTYPE_KEY]]
		
		# 这里先把<type subtype>分别映射到<菜系，子菜系>，目的是下一步转table时复用
		normKVDict[self.CUISINE_KEY] = normKVDict[self.TYPE_KEY]
		normKVDict[self.SUB_CUISINE_KEY] = normKVDict[self.SUBTYPE_KEY]

		#for key in normKVDict:
		#	print key, normKVDict[key]
		#print '=============='
		
		return normKVDict



# 归一化大众点评的面包线，抽取城市，商区，菜系字段
# e.g. 北京餐厅 > 湖北菜 > 九头鸟酒家
# e.g. 北京餐厅 > 西城区 > 清真菜 > 紫光园(西直门店)
# e.g. 北京餐厅 > 西城区 > 广外大街 > 新疆菜 > 新疆兵团食府
# e.g. 北京餐厅 > 朝阳区 > 对外经贸 > 川菜 > 川菜/家常菜 > 兄弟川菜(元大都店)
def normDianpingFoodBreadCrumb(key, val):
	normKVDict = dict()
	normKVDict[key] = val
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	valSegs = val.split('>')
	if valSegs[0].find('餐厅') != '':
		city = re.sub('餐厅', '', valSegs[0].strip())
		normKVDict['city'] = city
	if len(valSegs) == 4:
		normKVDict['cuisine'] = valSegs[2].strip()
	if len(valSegs) == 6:
		normKVDict['subcuisine'] = valSegs[4].strip()
	if len(valSegs) >= 4:
		normKVDict['area'] = valSegs[1].strip()
	if len(valSegs) >= 5:
		normKVDict['district'] = valSegs[2].strip()
		normKVDict['cuisine'] = valSegs[3].strip()
	return normKVDict



# 归一化大众点评网的电话
# 处理错误的电话号码  e.g. http://www.dianping.com/shop/23588151
def normDianpingFoodTel(key, val):
	normKVDict = dict()
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	val = re.sub('-', '', val)
	normalVal = []
	for tel in val.split(','):
		if len(tel) < 7:
			continue
		if tel not in normalVal:
			normalVal.append(tel)
	normKVDict[key] = ','.join(normalVal)
	return normKVDict
	



# 归一化大众点评的tags, commentSummary
# e.g. 无线上网(8),可以刷卡(8),情侣约会(4),免费停车(4),朋友聚餐(4),休闲小憩(3),家庭聚会(2)
def normTag(key, val):
	normKVDict = dict()
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	val = re.sub('\)', '', val)
	val = re.sub('\(', '@@@', val)
	normKVDict[key] = val
	return normKVDict





# 归一化大众点评的推荐菜字段
# 归一化为 foodUrl@@@foodName@@@recomCnt@@@foodPhoto@@@price
def normRecommFood(key, val):
	normKVDict = dict()
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	# 看产品需求，也可以选择过滤没有图片的数据
	valSegs = val.split('@@@')
	if len(valSegs) == 3:
		val = val + '@@@@@@'
	normKVDict[key] = val
	return normKVDict


# 归一化评论的评论时间
def normCommentDate(key, val):
	normKVDict = dict()
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	if val.find('更新') != -1:
		val = re.sub('.*更新于', '', val)
		val = re.sub(' .*$', '', val.strip())
	# 对于本年的数据，去除年份
	curYear = time.strftime("%Y", time.localtime())
	curShortYear = curYear[-2:]

	if re.match('[0-9]+-[0-9]+-[0-9]+', val):
		if val.startswith(curShortYear):
			val = val.replace(curShortYear + '-', '')
		elif val.startswith(curYear):
			val = val.replace(curYear + '-', '')

	normKVDict[key] = val
	return normKVDict


def normBusinessDate(key, val):
	normKVDict = dict()
	if len(key) == 0 or len(val) == 0:
		return normKVDict
	val = val.decode('gbk', 'ignore')
	val = re.sub(u'[\s]*[：；:]+[\s]*', ':', val)
	val = re.sub(u'[\s]*[\—\-－———]+[\s]*', '-', val)
	val = re.sub(u'[\s]*[~～]+[\s]*', '-', val)

	normKVDict[key] = val.encode('gbk', 'ignore')
	return normKVDict


def test_normCommentDate():
	print normCommentDate('dd', '03-09更新于16-05-13 10:36')
	print normCommentDate('dd', '03-09更新于17-05-14 10:36')
	print normCommentDate('dd', '03-09更新于2017-05-15 10:36')
	print normCommentDate('dd', '03-09更新于2016-05-16 10:36')
	print normCommentDate('dd', '03-09更新于 2016-05-17 10:36')


def testNormalFun():
	print normDianpingFoodTel('tel', '15040380068,010-')


def testDPCrumbNormlizer():
	conf = "/search/liubing/spiderTask/result/dianping/task-78/1447142745681.gbk"
	testFile = '/search/zhangk/Fuwu/Source/test.dat'
	
	normlizer = DPCrumbNormlizer(conf)
	for line in open(testFile):
		#print line
		segs = line.strip('\n').split('\t')
		key, value = segs[0], segs[1]
		normlizer.normlize(key, value)


def test_normBusinessDate():
	print normBusinessDate('date', '19:00--3:00')
	print normBusinessDate('date', '9:00～18:00')
	print normBusinessDate('date', '10：00－－20：00')
	print normBusinessDate('date', '周一~周日: 10:30-22:00')
	print normBusinessDate('date', '周一至周日，营业时间：16:00——次日1:00')
	print normBusinessDate('date', '19:00-02:00 周一至周日')


#test_normCommentDate()

#test_normBusinessDate()
