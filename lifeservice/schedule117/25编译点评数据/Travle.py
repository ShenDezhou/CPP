#!/bin/bash
#coding=gb2312

import re, sys, logging
import types
from config import *
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='log/travel.log',
                filemode='w')



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
			self.content[key] = val
	
	# 转成字符串
	def toString(self, keyList):
		resultStr = ''
		if self.URL not in self.content or self.TITLE not in self.content:
			return resultStr
		if len(self.content) < 3:		# 即只有url, title
			return resultStr
		for key in keyList:
			tmpVal = ''
			if key in self.content:
				tmpVal = self.content[key]
			if len(resultStr) == 0:
				resultStr = tmpVal
			else:
				resultStr += '\t' + tmpVal
		return resultStr


# ============ 评论类 =================
class Comment:
	''' 旅游的评论类 '''

	URL, TITLE, USERNAME = 'url', 'title', 'userName'

	def __init__(self):
		self.commentItem = None
		logging.info('Comment init.')
	
	# 打印评论的字段信息
	def printCommentHead(self, fd):
		head = ''
		for key in commentKey:
			head = head + '\t' + key
		fd.write(head[1:] + '\n')
	
	# 打印评论的实际内容
	def printCommentContent(self, fd):
		if self.commentItem is not None:
			commentStr = self.commentItem.toString(commentKey)
			if len(commentStr) > 0:
				fd.write(commentStr + '\n')
		

	# 加载评论文件，转成入库格式
	# input: 输入文件，即评论信息的原始数据文件
	# output: 输出文件，即转换成入库格式的评论文件
	def loadCommentFile(self, input, output):
		outputFD = open(output, 'w')
		self.printCommentHead(outputFD)
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
				self.printCommentContent(outputFD)
				self.commentItem = Item(url, title)
			elif key == self.USERNAME:
				self.printCommentContent(outputFD)
				self.commentItem = Item(url, title)
				self.commentItem.insert(key, val)
			else:
				self.commentItem.insert(key, val)
					
		self.printCommentContent(outputFD)
		outputFD.close()
		logging.info('transfer raw comment file [%s] to [%s]' % (input, output))



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



# ======= 攻略类 ===========
class TravelNote:
	''' 旅游攻略类 '''

	URL, TITLE, CONTENT = 'url', 'title', 'content'
	DATE, PHOTO, AUTHORNAME = 'date', 'photo', 'authorName'
	AUTHOR = ''
	def __init__(self):
		self.travelNoteItem = None
		self.page = -1
		logging.info('TravelNote init.')
	

	def subFun(self, matches):
		if matches is not None:
			matchStr = matches.group(1)
			# 单独处理<a><img .*></a>的情况
			if re.match("<img[^>]*>", matchStr):
				return matchStr
			# 包含其他标签的情况
			pos = matchStr.rfind('>')
			if pos != -1:
				return matchStr[pos+1 : ]
			else:
				return matchStr
		return ''


	# 处理每层楼的顶部的部分 
	# <div class="bbs_detail_title clearfix">.*?<div class="bbs_detail_content">.*?</div>
	def noteSubFun_(self, matches):
		contentRegex = '<div class="bbs_detail_content">.*?</div>'
		if matches:
			matchStr = matches.group(1)
			#print 'matchStr = ' + matchStr
			matchStr = re.sub("<[/]*(em|strong)>", "", matchStr)
			# 获取 回复者 与 被回复者
			if re.match('.*?<strong>(.*?)<em>(.*?)</em> </strong>', matchStr):
				#print "#######################################"
				authorRegex = '.*?<div class="bbs_detail_title.*?<h3 class="titles">(.*?)<span'
				replierRegex = '.*?<strong>回复 .*?<em>(.*?)</em> </strong>'
				if re.match(authorRegex, matchStr) and re.match(replierRegex, matchStr):
					author = re.match(authorRegex, matchStr).group(1).strip()
					replier = re.match(replierRegex, matchStr).group(1).strip()
					#print author, replier
					#print "================================="
					# 表示楼主答复自己的
					if author == replier:
						matchStr = re.sub(replierRegex, '', matchStr)
					else:
						matchStr = re.sub(contentRegex, '', matchStr)
			return matchStr

			#if re.match('.*?[^0-9]+1楼', matchStr):
			#	#print matchStr
			#	return matchStr
		return ''

	# 即 如果本楼主与本攻略作者不一样，直接退出
	# 如果一样，有回复# 但是不一致，则直接退出
	# 其他情况，打印
	def noteSubFun(self, matches):
		contentRegex = '<div class="bbs_detail_content">.*?</div>'
		curAuthorRegex = '.*?<div class="bbs_detail_title.*?<h3 class="titles">(.*?)<span'
		#replierRegex = '.*?bbsDetailContainer.*?回复.*?[0-9]+[#楼](.*?)</td>'
		replierRegex = r'.*?<!-- 楼层内容 -->.*?回复[\s]*[0-9]+[#楼](.*?)</div>'

		if matches:
			matchStr = matches.group(1)
			# 获取本楼楼主
			if re.match(curAuthorRegex, matchStr):
				curAuthor = re.match(curAuthorRegex, matchStr).group(1).strip()
				# 非本作者的楼
				if curAuthor != self.AUTHOR:
					return ''
				# 获取被答复者
				matchStr = re.sub("<[/]*(em|strong)>", "", matchStr)
				if re.match(replierRegex, matchStr):
					replier = re.match(replierRegex, matchStr).group(1).strip()[:30]
					#print self.AUTHOR, curAuthor, replier
					# 楼主回复自己
					if replier.find(self.AUTHOR) != -1:
						matchStr = re.sub('回复[\s]*[0-9]+[#楼].*?'+self.AUTHOR, '', matchStr)
						return matchStr
				else:
					return matchStr
		return ''


	# 去除攻略开始就是图的第一张图
	def removeHeadImg_(self, content):
		firstImgPos = content.find('<img ')
		if firstImgPos == -1:
			return content
		firstImgEndPos = content.find('>', firstImgPos+1)
		beforeImgContent = content[:firstImgPos]

		beforeImgContent = re.sub('<[^>]*?>', '', beforeImgContent)
		beforeImgContent = re.sub('[ \s]+', '', beforeImgContent)

		if len(beforeImgContent) == 0:
			content = re.sub('<img[^>]*?>', '', content, 1)
		return content

	def removeHeadImg(self, content):
		firstImgPos = content.find('<img ')
		if firstImgPos == -1:
			return content
		firstImgEndPos = content.find('>', firstImgPos+1)
		beforeImgContent = content[:firstImgPos]

		beforeImgContent = re.sub('<[^>]*?>', '', beforeImgContent)
		beforeImgContent = re.sub('[ \s]+', '', beforeImgContent)

		if len(beforeImgContent) == 0:
			content = re.sub('<img[^>]*?>', '', content, 1)
		return content





	# 归一化攻略
	def normalNote(self, note):
		# 去除空的
		note = re.sub("<p></p>", "", note)
		# 将外链跳转去除,注意这里是去除所有的href 还是仅仅去除内部的
		regex = '<a[^>]*>(.*?)</a>'
		note = re.sub(regex, self.subFun, note)

		# 去除穷游网的头部表格数据，以及几楼的那个数据
		regex = '<table class="contentTable">.*?</table>'
		note = re.sub(regex, '', note)
		
		# 每层楼的底部
		regex = '<div class="bbs_detail_seting clearfix">.*?<div class="floorReplyBox"></div>'
		note = re.sub(regex, '', note)

		# 每层楼的顶部（1楼不删除）
		regex = '(<div class="bbs_detail_title clearfix">.*?<div class="bbs_detail_content">.*?</div>)'	
		note = re.sub(regex, self.noteSubFun, note)
		#note = re.sub(regex, '', note)
		#if self.page == 1:
		#	note = re.sub(regex, self.noteSubFun, note)
		#else:
		#	note = re.sub(regex, '', note)
			

		regex = '(<div class="bbs_detail_title clearfix">.*?</div>)'
		note = re.sub(regex, '', note)

		# 去掉一楼的"1楼"标识
		#regex = '>[ \s]*?1楼[ \s]*?<'
		#note = re.sub(regex, '', note)
	
		# 只保留楼主的楼层，一楼直接输出，其他楼如果是回复自己的则输出
		# 去掉 回复几楼的标识, (需要根据是否是楼主回复楼主)
		replayRegex = '<td class="editor bbsDetailContainer"> <strong>回复 [0-9]+#.*?</td>'
		#note = re.sub(replayRegex, '', note)

		# 去除 过多的换行符号
		regex = '<br>[ \s]*<br>[ \s]*<br>[ \s]*'
		continueSub = re.search(regex, note)
		while continueSub is not None:
			note = re.sub(regex, '<br><br>', note)
			continueSub = re.search(regex, note)
		
		# 过滤掉插在中间的视频数据
		regex = '<object .*?getflashplayer.*?swf.*?<\/object>'
		note = re.sub(regex, '', note)
		
		# 去除攻略开始就是一张图片的那张图片
		note = self.removeHeadImg(note)

		return note



	# 归一化url, 找出是第几页的信息
	def normalUrl(self, url):
		self.page = 1
		regex = '\?authorid.*$'
		url = re.sub(regex, '', url)
		regex = '\-([0-9]+).html'
		pageMatch = re.findall(regex, url)
		if pageMatch:
			self.page = pageMatch[0]
		url = re.sub(regex, '-1.html', url)
		url = str(self.page) + '\t' + url
		return url



	# 归一化时间
	def normalDate(self, date):
		searchGroup = re.search('20[0-9][0-9]\-[0-1][0-9]\-[0-3][0-9]', date)
		if searchGroup is not None:
			return searchGroup.group()
		return date



	# 获取评论总的第一张图片
	def getFirstPhoto(self, content):	
		photoRegex = u'.*?<img[^>]*?src="([^>]*?(.jpeg|680x))"[^>]*?>'
		coverPhoto = ''
		matches = re.match(photoRegex, content)
		if matches is not None:
			coverPhoto = matches.group(1)
		return coverPhoto


	# 打印旅游攻略的字段信息
	def printTravelNoteHead(self, fd):
		head = ''
		for key in travelNoteKey:
			head = head + '\t' + key
		fd.write(head[1:] + '\n')

	# 打印旅游攻略的实际内容
	def printTravelNoteContent(self, fd):
		if self.travelNoteItem is not None:
			noteStr = self.travelNoteItem.toString(travelNoteKey)
			if len(noteStr) > 0:
				fd.write((noteStr + '\n'))	#.encode('gb2312', 'ignore'))

	# 加载旅游攻略文件，转成入库格式
	# input: 输入文件，即旅游攻略的原始数据文件
	# output: 输出文件，即转换成入库格式的攻略文件
	def loadTravelNoteFile(self, input, output):
		outputFD = open(output, 'w')
		self.printTravelNoteHead(outputFD)
		url, title = '', ''
		for line in open(input):
			line = line.strip('\n')	#.decode('gb2312', 'ignore')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = normalKey(segs[0]), normalVal(segs[1])
			if key == self.URL:
				url = self.normalUrl(val)
			elif key == self.TITLE:
				title = val;
				self.printTravelNoteContent(outputFD)
				self.travelNoteItem = Item(url, title)
			elif key == self.CONTENT:
				note = self.normalNote(val)
				self.travelNoteItem.insert(key, note)
				photo = self.getFirstPhoto(val)
				self.travelNoteItem.insert(self.PHOTO, photo)
			elif key == self.DATE:
				date = self.normalDate(val)
				self.travelNoteItem.insert(key, date)
			elif key == self.AUTHORNAME:
				self.AUTHOR = val
				self.travelNoteItem.insert(key, val)
			else:
				self.travelNoteItem.insert(key, val)
		self.printTravelNoteContent(outputFD)

		outputFD.close()
		logging.info('transfer raw travel note file [%s] to [%s]' % (input, output))






# ======= 旅游类 ===========
class Travle:
	''' 旅游类 '''
	def __init__(self):
		logging.info('Travel init.')

	# 将旅游数据文件切分成基本信息/评论两部分
	def splitTravelData(self, input, baseinfoOutput, commentOutput):
		baseInfoFD = open(baseinfoOutput, 'w')
		commentFD = open(commentOutput, 'w')
		for line in open(input):
			line = line.strip('\n')
			segs = line.split('\t')
			if len(segs) != 2:
				continue
			key, val = segs[0], segs[1]
			if key in commentKey:
				commentFD.write(line + '\n')
			if key in baseinfoKey:
				baseInfoFD.write(line + '\n')
		baseInfoFD.close()
		commentFD.close()
		logging.info('split [%s] into [%s] and [%s] done.' % (input, baseinfoOutput, commentOutput))


# 过滤类
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
	if opt == 'SPLIT':
		input, baseOutput, commentOut = sys.argv[2], sys.argv[3], sys.argv[4]
		travel = Travle()
		travel.splitTravelData(input, baseOutput, commentOut)
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

