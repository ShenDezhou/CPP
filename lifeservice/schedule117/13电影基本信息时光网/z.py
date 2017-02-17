#!/bin/bash
#coding=gb2312

import sys, logging, time, re
from HttpRequestTool import GetRequest
from DateTimeTool import DateTimeTool as dateTool
import simplejson as json

today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
		format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		filename='logs/get_mtime_movie_' + today + '.log',
		filemode='a')


Coding = 'utf-8'

# 时光网各个任务的存放路径
MtimePath = '/search/liubing/spiderTask/result/system/'
ActorPath = MtimePath + 'task-121'
ImagePath = MtimePath + 'task-122'
VideoPath = MtimePath + 'task-123'
DescribePath = MtimePath + 'task-144'


# 正在上映与即将上映的请求URL
OnlineUrl = 'http://m.mtime.cn/Service/callback.mi/Showtime/LocationMovies.api?locationId=290'
ComingUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/MovieComingNew.api?locationId=290'

# 电影基本信息的请求URL
baseinfoGetUrl = ''

# 演员表、剧照、片花的请求URL
VideoGetUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/Video.api?movieId=ID&pageIndex=1'
ImageGetUrl = 'http://m.mtime.cn/Service/callback.mi/movie/Image.api?movieId=ID'
ActorGetUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/MovieCreditsWithTypes.api?movieId=ID'




def get_movie_imp(response, listKey, titleKey):
	movieDict = dict()
	idKey, descKey = 'id', 'commonSpecial'

	jsonDict = json.loads(response)
	if listKey in jsonDict:
		movieList = jsonDict[listKey]
		for item in movieList:
			id, title, desc = '', '', ''
			if idKey not in item:
				logging.error('id key not in movie response!')
				continue
			id = str(item[idKey])
			#logging.info('id: %s' % id)
			if len(id.strip()) == 0:
				continue
			if descKey in item: desc = item[descKey]
			if titleKey in item: title = item[titleKey]
			movieDict[str(id)] = (title, desc)
	else:
		logging.error('%s key not in movie response!' % listKey)
		
	return movieDict


def get_movie_list_imp(response, listKeyList, titleKey):
	movieDict = dict()
	for listKey in listKeyList:
		mDict = get_movie_imp(response, listKey, titleKey)
		for key in mDict:
			movieDict[key] = mDict[key]
	return movieDict	


# 获取热门电影的列表
def get_hot_online_movie():
	listKeyList, titleKey = ['ms'], 't'

	response = GetRequest(OnlineUrl)
	movieDict = get_movie_list_imp(response, listKeyList, titleKey)
	logging.info('get online movie list done. total: %d' % len(movieDict))
	return movieDict


# 获取即将上映的电影列表
def get_coming_movie():
	listKeyList, titleKey = ['attention', 'moviecomings'], 'title'

	response = GetRequest(ComingUrl)
	movieDict = get_movie_list_imp(response, listKeyList, titleKey)
	logging.info('get coming movie list done. total: %d' % len(movieDict))
	return movieDict	



# 获取电影列表
def get_movie_list(output):
	onlineMovieDict = get_hot_online_movie()
	comingMovieDict = get_coming_movie()

	movieDict = dict()
	for key in comingMovieDict:
		movieDict[key] = comingMovieDict[key]
	for key in onlineMovieDict:
		movieDict[key] = onlineMovieDict[key]
	
	# 将电影的短评数据写入文件
	fd = open(output, 'w+')
	for key in movieDict:
		title, desc = movieDict[key]
		fd.write(('shortdesc\thttp://movie.mtime.com/%s/\t%s\n' % (key, desc)).encode(Coding, 'ignore'))
	fd.close()
	logging.info('write movie\'s short desc to %s: total: %d' % (output, len(movieDict)))

	return movieDict





def get_movie_image_imp(response, id):
	imageKey = 'image'
	imageList = []
	jsonList = json.loads(response)
	if not isinstance(jsonList, list):
		logging.error('response of image request of %s is not list' % id)
		return imageList 

	for item in jsonList:
		if imageKey not in item:
			logging.error('image key not in %s' % id)
			continue
		imageList.append(item[imageKey])
	return imageList
	

# 获取电影剧照
def get_movie_image(movieDict, output):
	#url     http://m.mtime.cn/#!/movie/79451/
	#title   ****
	#photoCnt        44
	#photo   http://img31.mtime.cn/pi/2016/01/31/113011.63758692_1000X1000.jpg,

	fd = open(output, 'w+')
	for id in movieDict:
		url = ImageGetUrl.replace('ID', id)
		print 'get image: [' + url + ']'
		response = GetRequest(url)
		
		# parse result
		imageList = get_movie_image_imp(response, id)	
		if len(imageList) == 0:
			logging.error('image list of movie %s is empty' % id)
			continue
		
		# record result
		title, desc = movieDict[id]
		fd.write('url\thttp://m.mtime.cn/#!/movie/%s/\n' % id)
		fd.write(('title\t%s\n' % title).encode(Coding, 'ignore'))
		fd.write('photoCnt\t%d\n' % len(imageList))
		fd.write('photo\t%s\n' % ','.join(imageList))
		time.sleep(1)
	fd.close()
	logging.info('write movie\'s image to %s' % output)
	logging.info('get all movie\'s image done.')	






def get_movie_video_imp(response, id):
	#title, image, video, time
	videoListKey, videoUrlKey = 'videoList', 'url'
	imageKey, titleKey, lengthKey = 'image', 'title', 'length'

	videoList = []
	jsonDict = json.loads(response)

	if videoListKey not in jsonDict:
		logging.error('response of video request of %s is not dict' % id)
		return videoList

	for item in jsonDict[videoListKey]:
		title, image, video, length = '','','',''
		if videoUrlKey not in item:
			logging.error('url key of not in video response of %s' % id)
			continue
		video = item[videoUrlKey]
		if imageKey in item: image = item[imageKey]
		if titleKey in item: title = item[titleKey]
		if lengthKey in item: length = item[lengthKey]
		time = dateTool.second2str(length)
		videoItem = '%s@@@%s@@@%s@@@%s' % (title, image, video, time)
		videoList.append(videoItem)
	return videoList



#url     http://m.mtime.cn/#!/movie/79451/
#title   飞鹰艾迪
#video   飞鹰艾迪 中文版终极预告片@@@http://img31.mtime.cn/**.jpg@@@http://vfx.mtime.cn/Video/**.mp4@@@29秒
# 获取电影片花
def get_movie_video(movieDict, output):
	fd = open(output, 'w+')
	for id in movieDict:
		url = VideoGetUrl.replace('ID', id)
		print 'get video: [' + url + ']'
		response = GetRequest(url)
		# parse result
		videoList = get_movie_video_imp(response, id)	
		if len(videoList) == 0:
			logging.error('video list of movie %s is empty' % id)
			continue
		# record result
		title, desc = movieDict[id]
		fd.write('url\thttp://m.mtime.cn/#!/movie/%s/\n' % id)
		fd.write(('title\t%s\n' % title).encode(Coding, 'ignore'))
		for videoItem in videoList:
			fd.write(('video\t%s\n' % videoItem).encode(Coding, 'ignore'))
		time.sleep(1)
	fd.close()
	logging.info('write movie\'s video to %s' % output)
	logging.info('get all movie\'s video done.')	




def get_movie_actor_imp(response, id):
	typesKey, typeNameEnKey, personsKey = 'types', 'typeNameEn', 'persons'
	dirEn, dirCh, actorEn, actorCh = 'Director', u'导演', 'Actor', u'演员'

	nameKey, enNameKey, imgKey, idKey = 'name', 'nameEn', 'image', 'id'
	roleKey, roleImgKey = 'personate', 'roleCover'

	actorList = []
	jsonDict = json.loads(response)
	if typesKey not in jsonDict:
		logging.error('types key not in movie %s' % id)
		return actorList

	for typeItem in jsonDict[typesKey]:
		if typeNameEnKey not in typeItem:
			logging.error('typeNameEn key not in movie %s' % id)
			continue
		if personsKey not in typeItem:
			logging.error('person key not in movie %s' % id)
			continue
			
		# get actor type
		actorType = ''
		if typeItem[typeNameEnKey].strip() == dirEn:
			actorType = dirCh
		elif typeItem[typeNameEnKey].strip() == actorEn:
			actorType = actorCh

		# only recode director / actor
		if actorType == '':
			continue

		# get person(actor/director) list
		for item in typeItem[personsKey]:
			actorName, actorNameEn, actorUrl, actorImg = '','','',''
			id, role, roleImg = '', '', ''			

			if idKey in item: id = item[idKey]
			if nameKey in item: actorName = item[nameKey]
			if imgKey in item: actorImg = item[imgKey]
			if enNameKey in item: actorNameEn = item[enNameKey]
			if roleKey in item: role = item[roleKey]
			if roleImgKey in item: roleImg = item[roleImgKey]
			
			actorUrl = ('http://m.mtime.cn/#!/person/%d/' % id)
		
			actorItem = '%s@@@%s@@@%s@@@%s@@@%s@@@%s@@@%s' % (actorType, actorName, actorNameEn, actorUrl, actorImg, role, roleImg)
			actorList.append(actorItem)
	return actorList



# 获取电影演员表
def get_movie_actor(movieDict, output):
	fd = open(output, 'w+')
	for id in movieDict:
		url = ActorGetUrl.replace('ID', id)
		print 'get actor: [' + url + ']'
		response = GetRequest(url)
		actorList = get_movie_actor_imp(response, id)	
		if len(actorList) == 0:
			logging.error('actor list of movie %s is empty' % id)
			continue
		fd.write('url\thttp://m.mtime.cn/#!/movie/%s/\n' % id)
		for actorItem in actorList:
			fd.write(('actorInfo\t%s\n' % actorItem).encode(Coding, 'ignore'))
		time.sleep(1)
	fd.close()
	logging.info('write movie\'s actor to %s' % output)
	logging.info('get all movie\'s actor done.')	
	pass




def main():
	# 获取电影ID列表，以及短评数据
	output = DescribePath + '/' + str(dateTool.current())
	movieDict = get_movie_list(output)
	
	# 获取电影的剧照
	output = ImagePath + '/' + str(dateTool.current())
	get_movie_image(movieDict, output)

	# 获取电影片花
	output = VideoPath + '/' + str(dateTool.current())
	get_movie_video(movieDict, output)

	# 获取电影演员表
	output = ActorPath + '/' + str(dateTool.current())
	get_movie_actor(movieDict, output)



def get_movie_summary_imp(response):
	summaryRegex = u'<div class="plots_out".*?<p>(.*?class="first_letter">.*?)</div>'
	match = re.search(summaryRegex, response)
	if match:
		summary = match.group(1)
		summary = re.sub('<.*?>', '', summary)
		return summary
	return ""


summaryUrl = "http://movie.mtime.com/%s/plots.html"
def get_movie_summary(movieDict, output):
	movieSummaryMap = dict()
	for movieid in movieDict:
		url = summaryUrl % movieid
		time.sleep(1)
		print 'handle %s' % url
		response = GetRequest(url)
		summary = get_movie_summary_imp(response)
		movieurl = url.replace("plots.html", "")
		if len(summary) > 10:
			movieSummaryMap[movieurl] = summary

	if len(movieSummaryMap) > 10:
		for movieurl in movieSummaryMap:
			print movieurl, movieSummaryMap[movieurl]

def parse_mtime_rank(url):
	response = GetRequest(url)
	response = re.sub('[\r\n]', '', response)
	#print response.encode('gb2312', 'ignore')
	regex = '<div class="mtiplist">.*?<div class="num.*?">(.*?)<i.*?<a href="(.*?)"'
	rankList = re.findall(regex, response)
	for rank, url in rankList:
		rank = re.sub('^0', '', rank.strip())
		print url, rank

if __name__ == '__main__':
	mtimeRankIndex = 'http://www.mtime.com/hotest/index.html'
	mtimeRankBaseUrl = 'http://www.mtime.com/hotest/index-%d.html'
	mtimeRankPage = 5
	parse_mtime_rank(mtimeRankIndex)
	for page in xrange(2, mtimeRankPage+1):
		rankUrl = mtimeRankBaseUrl % page
		parse_mtime_rank(rankUrl)
	



	#main()
	#output = 'zzzz'
	#movieDict = get_movie_list(output)
	#get_movie_summary(movieDict, 'kkk')
	#url = 'http://movie.mtime.com/207872/plots.html'
	#response = GetRequest(url)
	#print get_movie_summary_imp(response)



	

