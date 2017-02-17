
#coding=gb2312
import xml.dom.minidom


# http://server.huatuojiadao.cn/huatuo_server/sougou/store/list

#key: a44747f527e14257abf3bbb90587db48 
#sign:  签名 必填
#last_time  起始时间   YYYY-MM-DD hh:mm:ss
#page_no   当前页数   默认值 1
#page_size   获取数据条数



#content = base64(md5(str))

#post(content)


import hashlib, base64
from HttpRequestTool import *

SOGOU_KEY = 'a44747f527e14257abf3bbb90587db48'
KEY_LIST = ['last_time', 'page_no', 'page_size']
URL = 'http://server.huatuojiadao.cn/huatuo_server/sougou/store/list?content='


def Base64(str):
	return base64.encodestring(str)


def MD5(str):
	return hashlib.new("md5", str).hexdigest()


def createSign(last_time, page_no, page_size):
	paraStr = 'last_time=%s&page_no=%s&page_size=%s' % (last_time, page_no, page_size)
	#print paraStr
	paraStr += SOGOU_KEY
	sign = MD5(paraStr)
	#print sign
	return sign	

def getDate(last_time, page_no, page_size):
	sign = createSign(last_time, page_no, page_size)
	#sign = 'bb9b6842d3773cfe88ff337082924d24'
	jsonStr = '{"sign":"%s","page_size":"%s","sign_type":"MD5","last_time":"%s","page_no":"%s"}' % (sign, page_size, last_time, page_no)
	#print jsonStr
	content = Base64(jsonStr)
	#print content
	return content
	

last_time = '2016-01-30 10:00:00'
page_no = '5'
page_size = '200'

content = getDate(last_time, page_no, page_size)

#url = 'http://server.huatuojiadao.cn/huatuo_server/sougou/store/list?content=eyJzaWduIjoiYmI5YjY4NDJkMzc3M2NmZTg4ZmYzMzcwODI5MjRkMjQiLCJwYWdlX3NpemUiOiIyMCIsInNpZ25fdHlwZSI6Ik1ENSIsImxhc3RfdGltZSI6IjIwMTYtMDMtMzAgMTA6MDA6MDAiLCJwYWdlX25vIjoiMSJ9'
url = 'http://server.huatuojiadao.com/huatuo_server/sougou/store/list'

paraDict = {'content':content}

print content

xmlStr = PostRequest(url, paraDict).encode('gb2312', 'ignore')

print xmlStr

#xml.dom.minidom.parseString(xmlStr)

#pretty_xml_as_string = xml.toprettyxml()

#print pretty_xml_as_string.decode('utf-8', 'ignore')




