#!/bin/python
#coding=gb2312

import sys, logging, time, re
from HttpRequestTool import GetRequest
from DateTimeTool import DateTimeTool as dateTool
import simplejson as json

today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
		format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		filename='logs/get_extra_mtime_movie_' + today + '.log',
		filemode='a')


Coding = 'gb2312'

# 时光网各个任务的存放路径
MovieInfoPath = '/search/zhangk/Fuwu/Source/Crawler/beijing/movie'

SummaryFile = '/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_summary'
OnlineStatusFile = '/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_online'
MovieRankFile = '/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_rank'


# 演员表、剧照、片花、简介的请求URL
VideoGetUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/Video.api?movieId=ID&pageIndex=1'
ImageGetUrl = 'http://m.mtime.cn/Service/callback.mi/movie/Image.api?movieId=ID'
ActorGetUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/MovieCreditsWithTypes.api?movieId=ID'
SummaryUrl = "http://movie.mtime.com/%s/plots.html"

MtimeSearchUrl = 'http://service.channel.mtime.com/Search.api?Ajax_CallBack=true&Ajax_CallBackType=Mtime.Channel.Services&Ajax_CallBackMethod=GetSuggestObjs&Ajax_CrossDomain=1&Ajax_RequestUrl=http%3A%2F%2Fwww.test.com&t=1464665123182&Ajax_CallBackArgument0='





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
		id, title = movieDict[id]
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
		videoItem = '%s\t%s\t%s\t%s' % (title, image, video, time)
		videoList.append(videoItem)
	return videoList



#url     http://m.mtime.cn/#!/movie/79451/
#title   飞鹰艾迪
#video   飞鹰艾迪 中文版终极预告片@@@http://img31.mtime.cn/**.jpg@@@http://vfx.mtime.cn/Video/**.mp4@@@29秒
# 获取电影片花
def get_movie_video(movieDict, output):
	fd = open(output, 'w+')
	lineIdx = 20000
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
		mid, title = movieDict[id]
		for videoItem in videoList:
			lineIdx += 1
			fd.write(('%s\t%s\t%s\n' % (str(lineIdx), mid, videoItem)).encode(Coding, 'ignore'))
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
		
			actorItem = '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (actorType, actorName, actorNameEn, actorUrl, actorImg, role, roleImg)
			actorList.append(actorItem)
	return actorList



# 获取电影演员表
def get_movie_actor(movieDict, output):
	fd = open(output, 'w+')
	lineIdx = 20000
	for id in movieDict:
		url = ActorGetUrl.replace('ID', id)
		print 'get actor: [' + url + ']'
		response = GetRequest(url)
		actorList = get_movie_actor_imp(response, id)	
		if len(actorList) == 0:
			logging.error('actor list of movie %s is empty' % id)
			continue

		mid, title = movieDict[id]
		for actorItem in actorList:
			lineIdx += 1
			url = 'http://movie.mtime.com/%s/' % id
			fd.write(('%s\t%s\t%s\t%s\t%s\n' % (str(lineIdx), mid, url, title, actorItem)).encode(Coding, 'ignore'))
		time.sleep(1)
	fd.close()
	logging.info('write movie\'s actor to %s' % output)
	logging.info('get all movie\'s actor done.')	


# 解析电影的summary
def get_movie_summary_imp(response):
	summaryRegex = u'<div class="plots_out".*?<p>(.*?class="first_letter">.*?)</div>'
	match = re.search(summaryRegex, response)
	if match:
		summary = match.group(1)
		summary = re.sub('<.*?>', '', summary)
		summary = re.sub('&nbsp;', '', summary)
		return summary
	return ""


def get_movie_summary(movieDict, output):
	movieSummaryMap = dict()
	for movieid in movieDict:
		url = SummaryUrl % movieid
		time.sleep(1)
		logging.info('get summary url: %s' % url)
		response = GetRequest(url)
		summary = get_movie_summary_imp(response)
		movieurl = url.replace("plots.html", "")
		if len(summary) > 10:
			movieSummaryMap[movieurl] = summary
		else:
			logging.error("get summary of %s error" % url)

	if len(movieSummaryMap) > 10:
		fd = open(output, 'w+')
		for movieurl in movieSummaryMap:
			summary = movieSummaryMap[movieurl]
			fd.write(('%s\t%s\n' % (movieurl, summary)).encode(Coding, 'ignore'))
		fd.close()
	logging.info('get all movie\'s summary done.')	


# 获取时光网的排名信息
def parse_mtime_rank(url):
	rankResultList = []
	response = GetRequest(url)
	response = re.sub('[\r\n]', '', response)
	regex = '<div class="mtiplist">.*?<div class="num.*?">(.*?)<i.*?<a href="(.*?)"'
	rankList = re.findall(regex, response)
	for rank, url in rankList:
		rank = re.sub('^0', '', rank.strip())
		rankResultList.append((url, rank))
	return rankResultList


def get_movie_rank(output):
	mtimeRankIndex = 'http://www.mtime.com/hotest/index.html'
	mtimeRankBaseUrl = 'http://www.mtime.com/hotest/index-%d.html'
	mtimeRankPage = 5
	rankResultList = []
	rankResultList += parse_mtime_rank(mtimeRankIndex)
	for page in xrange(2, mtimeRankPage+1):
		rankUrl = mtimeRankBaseUrl % page
		rankResultList += parse_mtime_rank(rankUrl)
	fd = open(output, 'w+')
	for url, rank in rankResultList:
		fd.write('%s\t%s\n' % (url, rank))
	fd.close()
	logging.info('get all movie\'s rank done.')	


# 名称近似相同，导演相同
def match_right_movie(movieInfos, mtimeMovieInfos):
	print ('\t'.join(movieInfos)).encode('gb2312', 'ignore')

	id, title, director = movieInfos
	for mtimeMovieItem in mtimeMovieInfos:
		mtimeid, mtimeTitle, mtimeDirector = mtimeMovieItem
		#print '\t'.join(mtimeMovieItem).encode('gb2312', 'ignore')
		if title==mtimeTitle and director.find(mtimeDirector) != -1:
			return mtimeid
	return ''
		



def get_candidate_movies(title):
	valueKey, objsKey = 'value', 'objs'
	idKey, titleKey, directorKey = 'id', 'titlecn', 'director'

	url = '%s%s' % (MtimeSearchUrl, title)
	candidateMovieList = []
	response = GetRequest(url)
	response = response.encode('gb2312', 'ignore')
	begin, end = response.find('{'), response.rfind('}')
	if begin > 0 and end > 0 and end > begin:
		response = response[begin : end+1]
		jsonDict = json.loads(response.decode('gb2312', 'ignore'))
		if valueKey in jsonDict and jsonDict[valueKey] and objsKey in jsonDict[valueKey]:
			for item in jsonDict[valueKey][objsKey]:
				if idKey not in item or directorKey not in item:
					continue
				id, title, director = item[idKey], item[titleKey], item[directorKey]
				candidateMovieList.append((str(id), title, director))
	#print response
	return candidateMovieList


# input:<id title director>
def get_mtime_idlist(input, coding='gb2312'):
	mtimeidDict = dict()
	for line in open(input):
		line = line.strip('\n').decode(coding, 'ignore')
		segs = line.split('\t')
		if len(segs) < 3:
			continue
		id, title, director = segs
		candidateMovieList = get_candidate_movies(title)
		
		mtimeid = match_right_movie(segs, candidateMovieList)	
		if mtimeid == '':
			continue
		mtimeidDict[mtimeid] = (id, title)
	return mtimeidDict


def main():
	# 根据电影名称 + 时光网的接口获取时光网的id
	movieDict = get_mtime_idlist('tmp/movie_titles')

	# 获取电影的剧照
	#output = MovieInfoPath + '/mtime_photos.extra'
	#get_movie_image(movieDict, output)

	# 获取电影片花
	output = MovieInfoPath + '/mtime_videos.extra'
	get_movie_video(movieDict, output)

	# 获取电影演员表
	output = MovieInfoPath + '/mtime_actors.extra'
	get_movie_actor(movieDict, output)

	# 获取时光网的完整的简介信息
	#get_movie_summary(movieDict, SummaryFile)



if __name__ == '__main__':
	main()
