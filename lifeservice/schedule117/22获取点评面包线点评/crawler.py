#coding=gbk

import sys
import urllib
import urllib2
import re
import time
import socket
import urllib
import urllib2
import httplib
import urlparse
import time
import StringIO
import gzip
import sys
import random

#set default timeout
socket.setdefaulttimeout( 30 )



def access_web(url):
    time.sleep(1)
    try:
        response = urllib2.urlopen(url, timeout=5).read()
#        response = urllib.urlopen(apiUrl, timeout=100).read()
    except:
        print 'bad access url', url
        return ''
    return responsez

def getPage(url, data = None, retry=3, interval=0.5, headers = {}, proxy = None):
    time.sleep(1)
    t = 0
    while t < retry:
        if t > 1:
            proxy = None 
        fd = None
        try:
            if data != None:
                request = urllib2.Request(url, data)
            else:
                request = urllib2.Request(url)
            if "Accept-encoding" not in headers:
                request.add_header('Accept-encoding', 'gzip')
            if "User-Agent" not in headers:
                request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.97 Safari/537.22')
            for k,v in headers.iteritems():
                request.add_header(k, v)
            if proxy != None:
                request.set_proxy(proxy, "http")
            opener = urllib2.build_opener()
            
            fd = opener.open(request)
            if fd != None:
                #if 'text/html' not in fd.headers.get("Content-Type") and \
                #    "text/xml" not in fd.headers.get("Content-Type") and \
                #    "text/css" not in fd.headers.get("Content-Type")  :
                #    return None
                
                contentEncoding = fd.headers.get("Content-Encoding")
                data = None
                if contentEncoding != None and 'gzip' in contentEncoding:
                    compresseddata = fd.read()
                    compressedstream = StringIO.StringIO(compresseddata)  
                    gzipper = gzip.GzipFile(fileobj=compressedstream)
                    data = gzipper.read()
                else:
                    data = fd.read()

                return data
        except Exception, e:
            print >> sys.stderr, "download %s by proxy %s error: %s" %(url, proxy,str(e)) 
            if hasattr(e,'code') and e.code == 404: 
                return ''
            t += 1
            time.sleep(interval * t)
            
        if fd != None:  fd.close()
        
    return ''

def toLine(page):
	#pageLine = ''
	#for line in page:
#		line = '%s\t\t' % line.strip()
#		pageLine = '%s%s' % (pageLine, line)
#	return pageLine
	return re.sub('[\r\n]', '\t\t\t', page)


def Process(infile_name, outfile_name):
    lineCnt = 0
    outFile = open(outfile_name, 'w')
    for line in open(infile_name):
        lineCnt += 1
        url = line.strip()
        raw_page = getPage(url)
        if raw_page == None:
            raw_page = ''
		# 这里在抓取医生的详情页时候使用 #@#  其他情况下不使用
        raw_page = toLine(raw_page)
        print lineCnt, len(raw_page), url
        #print >> outFile, 'URL\t%s' % url
        print >> outFile, '%s' % url
        print >> outFile, raw_page
        #print >> outFile, '============================================================'
        outFile.flush()
    outFile.close()
    

if __name__ == '__main__':
    infile_name = sys.argv[1]
    outfile_name = sys.argv[2]
    Process(infile_name, outfile_name)
