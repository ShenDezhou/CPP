#coding=gb2312
import xml.dom.minidom


# http://server.huatuojiadao.cn/huatuo_server/sougou/store/list

#key: a44747f527e14257abf3bbb90587db48 
#sign:  签名 必填
#last_time  起始时间   YYYY-MM-DD hh:mm:ss
#page_no   当前页数   默认值 1
#page_size   获取数据条数


import hashlib, base64
from HttpRequestTool import *
from xmlParser import parseService

import sys, logging, time
from DateTimeTool import DateTimeTool as dateTool

today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
		format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		filename='logs/get_service' + today + '.log',
		filemode='a')



SOGOU_KEY = 'a44747f527e14257abf3bbb90587db48'
PAGE_NO = 1
PAGE_SIZE = 200
LAST_TIME = '2015-12-25 00:00:00'
URL = 'http://server.huatuojiadao.com/huatuo_server/sougou/store/list'
xmlFile = 'data/service.xml'
tmpXmlFile = 'tmp/service.xml'

def Base64(str):
	return base64.encodestring(str)


def MD5(str):
	return hashlib.new("md5", str).hexdigest()


def createSign(last_time, page_no, page_size):
	paraStr = 'last_time=%s&page_no=%d&page_size=%d' % (last_time, page_no, page_size)
	paraStr += SOGOU_KEY
	sign = MD5(paraStr)
	return sign	


def getDate(last_time, page_no, page_size):
	sign = createSign(last_time, page_no, page_size)
	jsonStr = '{"sign":"%s","page_size":"%s","sign_type":"MD5","last_time":"%s","page_no":"%s"}' % (sign, page_size, last_time, page_no)
	content = Base64(jsonStr)
	return content
	

def parseXmlStr(xmlFile):
	parseService(xmlFile, 'Huatuojiadao')

gfd = open(xmlFile, 'w+')
def getAllServices():
	hasMoreDate = True
	lastXmlStr, curPage = "", PAGE_NO
	while hasMoreDate:
		fd = open(tmpXmlFile, 'w+')
		content = getDate(LAST_TIME, curPage, PAGE_SIZE)
		logging.info('get %s page services' % curPage)
		paraDict = {'content':content}
		# 请求xml，然后记录xml到临时文件
		xmlStr = PostRequest(URL, paraDict).encode('gbk', 'ignore')
		fd.write(xmlStr)
		gfd.write('%s\n' % xmlStr)
		fd.close()
		if (lastXmlStr != "" and xmlStr == lastXmlStr) or len(xmlStr) < 200:
			hasMoreDate = False
			break
		# 解析Shop Service
		parseXmlStr(tmpXmlFile)
		# 下一页
		curPage += 1
		lastXmlStr = xmlStr
	logging.info('get all services done. total %d page' % curPage)

getAllServices()
gfd.close()
