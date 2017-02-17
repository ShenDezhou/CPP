#!/bin/python
#coding=gbk
from lxml import objectify
import sys, copy, re
from pandas import DataFrame



CinemaKey = ['cinemaid','source','cinemaname','brand','address','province','city','area','district','latlng']

MovieMetaKey = ['source', 'movieid','moviename','presale','director']

MovieDetailKey = ['movieid','source','moviename','enName','posterlink','year','duration','type','date','version',\
		'onlinestatus','releasecountry','scorecnt','wantcnt','shortdesc','description','score','hot','boxoffice',\
		'photosUrl','videosUrl','photosCnt','photoSet']

MoviePresaleKey = ['movieid','source', 'presale','province', 'city']

CinemaMovieRelKey = ['cinemaid','movieid','source','date','week','start','end','price','room','language','dimensional','seat']



def getValueList(keyList, kvDict):
	valueList = []
	for key in keyList:
		value = ''
		if key in kvDict and kvDict[key] != None:
			value = kvDict[key]
		value = re.sub('[\r\n\t]', ' ', value)
		valueList.append(value.strip())
	return valueList



MovieMetaDict, MovieDetailDict = dict(), dict()
def parseMovie(item, source='', sep='\t', encode='gbk'):
	info = item.display.movie
	
	movieid = info.movieid
	if movieid in MovieDetailDict:
		return

	movie = dict()
	for element in info.iterchildren():
		movie[element.tag] = element.text
	
	movie['source'] = source
	valueList = getValueList(MovieDetailKey, movie)
	MovieDetailDict[movieid] = sep.join(valueList).encode(encode, 'ignore')

	valueList = getValueList(MovieMetaKey, movie)
	MovieMetaDict[movieid] = sep.join(valueList).encode(encode, 'ignore')



CinemaDict = dict()
def parseCinema(item, source='', sep='\t', encode='gbk'):
	info = item.display.cinema
	gps = item.gps
	
	cinemaid = info.cinemaid
	if cinemaid in CinemaDict:
		return
	
	cinema = dict()
	for element in info.iterchildren():
		cinema[element.tag] = element.text
	for element in gps.iterchildren():
		cinema[element.tag] = element.text

	cinema['source'] = source
	valueList = getValueList(CinemaKey, cinema)
	CinemaDict[cinemaid] = sep.join(valueList).encode(encode, 'ignore')




MoviePresaleDict = dict()
def parseMoviePresale(item, source='', sep='\t', encode='gbk'):
	movieInfo = item.display.movie
	cinemaInfo = item.display.cinema

	cinemaid, movieid = cinemaInfo.cinemaid, movieInfo.movieid
	if movieid in MoviePresaleDict:
		return

	presale = dict()
	for element in movieInfo.iterchildren():
		presale[element.tag] = element.text
	for element in cinemaInfo.iterchildren():
		presale[element.tag] = element.text

	presale['source'] = source
	valueList = getValueList(MoviePresaleKey, presale)
	MoviePresaleDict[movieid] = sep.join(valueList).encode(encode, 'ignore')





CinemaMovieRelList = []
def parseCinemaMovieRelImp(cinemaid, movieid, dayItem, source='', sep='\t', encode='gbk'):
	baseinfo = {'cinemaid':cinemaid, 'movieid':movieid, 'source':source}
	baseinfo['date'] = dayItem.date1.text
	baseinfo['week'] = dayItem.xingqi.text
	
	for timeItem in dayItem.time:
		cmRel = dict()
		for element in timeItem.iterchildren():
			cmRel[element.tag] = element.text
		
		# 添加过滤条件
		if 'start' not in cmRel or len(cmRel['start']) == 0:
			continue

		for key in baseinfo:
			cmRel[key] = baseinfo[key]
		
		
		cmRel['source'] = source
		valueList = getValueList(CinemaMovieRelKey, cmRel)
		CinemaMovieRelList.append(sep.join(valueList).encode(encode, 'ignore'))


def parseCinemaMovieRel(item, source=''):
	movieid = item.display.movie.movieid.text
	cinemaid = item.display.cinema.cinemaid.text

	parseCinemaMovieRelImp(cinemaid, movieid, item.display.day1, source)
	parseCinemaMovieRelImp(cinemaid, movieid, item.display.day2, source)
	parseCinemaMovieRelImp(cinemaid, movieid, item.display.day3, source)
	

def parse(input, source=''):
	parsed = objectify.parse(open(input))

	root = parsed.getroot()
	for item in root.item:
		parseMovie(item, source)
		parseCinema(item, source)
		parseCinemaMovieRel(item, source)
		parseMoviePresale(item, source)


def printDict(file, dataDict):
	fd = open(file, 'a')
	for key in dataDict:
		fd.write('%s\n' % dataDict[key])
	fd.close()

def printList(file, dataList):
	fd = open(file, 'a')
	for item in dataList:
		fd.write('%s\n' % item)
	fd.close()
	

def printResult():
	movieDetailFile = 'data/movie_movie_detail'
	movieMetaFile = 'data/movie_movie'
	cinemaFile = 'data/movie_cinema'
	presaleFile = 'data/movie_presale'
	cmRelFile = 'data/movie_cm_relation'

	printDict(movieMetaFile, MovieMetaDict)
	printDict(movieDetailFile, MovieDetailDict)
	printDict(cinemaFile, CinemaDict)
	printDict(presaleFile, MoviePresaleDict)
	printList(cmRelFile, CinemaMovieRelList)



def testPrintResult():
	for key in MovieDetailDict:
		print MovieDetailDict[key]

	for key in MovieMetaDict.keys()[:5]:
		print MovieMetaDict[key]

	for item in CinemaMovieRelList[:5]:
		print item

	for key in MoviePresaleDict:
		print MoviePresaleDict[key]


input, source = sys.argv[1], sys.argv[2]
parse(input, source)
printResult()
#testPrintResult()
