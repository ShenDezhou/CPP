#!/bin/python
#coding=gb2312

import requests, commands
import sys, logging, urllib2
import time, json


# 解析json的几种返回状态
ERROR_CODE = -1
EMPTY_CODE = 1
OK_CODE = 0
sep = '\t'



def queryUrlEncode(query):
	if isinstance(query, unicode):
		query = query.encode('utf8', 'ignore')
	return urllib2.quote(query)



# Post 请求
def PostRequest(url, paramDict):
	response = requests.post(url, data=paramDict)
	#print response.status_code
	if response.status_code == 200:
		return response.text
	return 'failed'

# Get 请求
def GetRequest(url, headers = {}):
	response = None
	try:
		if len(headers) > 0:
			response = requests.get(url, headers = headers)
		else:
			response = requests.get(url)
		if response and response.status_code == 200:
			return response.text
	except:
		return ''
	return ''


def Post(url, payload):
	headers = {'content-type': 'application/json'}
	response = requests.post(url, data=json.dumps(payload), headers = headers)
	if response and response.status_code == 200:
		return response.text
	return 'Failed'


# Wget 请求
def Wget(url, page=""):
	wgetCmd = 'wget "URL" -U "Mozilla" '
	if not url.startswith('http://'):
		return
	wgetCmd = wgetCmd.replace('URL', url)
	if page != "":
		wgetCmd = wgetCmd + ' -O "' + page + '"'
	status, result = commands.getstatusoutput(wgetCmd)
	return status



def testPost(url):
	# 请求参数
	paramDict = dict()
	paramDict['key'] = value
	
	returnJson = PostRequest(url, paramDict)  #.encode('gb2312', 'ignore')
	print returnJson


def test(url):
	#url = 'http://www.51xiancheng.com/subject/1485'
	print GetRequest(url).encode('utf8', 'ignore')


def testGet():
	#url = "http://beijing.51xiancheng.com/interface/Subject/subjectMore?site_id=930&page=2&pagesize=8"
	url = "http://www.autohome.com.cn/382/#levelsource=000000B00_0&pvareaid=101594"
	headers = {'user-agent': 'Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 4 Build/JOP40D)'}

	result = GetRequest(url, headers)
	print result.encode('gb2312', 'ignore')



def test_post(url):
	paramDict = dict()
	response = PostRequest(url, paramDict)  #.encode('gb2312', 'ignore')
	print response
	
