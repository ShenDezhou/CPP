#!/bin/python
#coding=gb2312

import requests, commands
import sys, logging
#import simplejson as json
import time, json


# 解析json的几种返回状态
ERROR_CODE = -1
EMPTY_CODE = 1
OK_CODE = 0
sep = '\t'


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
	if len(headers) > 0:
		response = requests.get(url, headers = headers)
	else:
		response = requests.get(url)
	if response and response.status_code == 200:
		return response.text
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

def testWget():
	#url = 'wget "http://beijing.51xiancheng.com/interface/Subject/subjectMore?site_id=930&page=2&pagesize=8" -U "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 4 Build/JOP40D)" -O "zzzz"'
	url = 'http://www.51xiancheng.com/subject/1485'
	page = 'zzz'
	Wget(url, page)

def test(url, header):
	#url = 'http://www.51xiancheng.com/subject/1485'
	print GetRequest(url, header).encode('utf8', 'ignore')


def testGet():
	url = "http://beijing.51xiancheng.com/interface/Subject/subjectMore?site_id=930&page=2&pagesize=8"
	headers = {'user-agent': 'Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 4 Build/JOP40D)'}

	result = GetRequest(url, headers)
	print result.encode('gb2312', 'ignore')



def test_post(url):
	paramDict = dict()
	response = PostRequest(url, paramDict)  #.encode('gb2312', 'ignore')
	print response
	

#test_post('http://10.134.96.108:8080/taskservice/task/4/status/detail')
#test('http://10.134.96.108:8080/taskservice/task/4/status/detail')
#html = GetRequest("http://m.mtime.cn/#!/home/coming/")
#print html.encode('gb2312', 'ignore')



#header = {"Cookie":"mpuv=Cq0EVVbqJCFkQT2pTQMEAg==; pgv_pvi=7854299136; RK=LLumJZtDcb; ptcz=c9f33c54ef33f68243394c4889a46eec82f7a8467a38a4b2a61210c1c3499c7d; pt2gguin=o0949640570; pgv_pvid=71183140; o_cookie=949640570; pgv_si=s7993484288; uin=null; skey=null; luin=null; lskey=null; user_id=null; session_id=null","Referer":"http://lbs.qq.com/webservice_v1/guide-gcoder.html", "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36"}

#test('http://apis.map.qq.com/ws/geocoder/v1/?&callback=QQmap&location=36.552506%2C104.172763&get_poi=1&key=OB4BZ-D4W3U-B7VVO-4PJWW-6TKDJ-WPB77&output=jsonp&_=1460014450263', header)
