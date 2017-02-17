#coding=gb2312
import urllib2, re, sys, time
from multiprocessing.dummy import Pool as ThreadPool


ISOTIMEFORMAT = '%Y-%m-%d %X'
START_END_REGEX = u'<dt>有效期</dt>.*?<dd>([0-9]{4}年[0-9]{2}月[0-9]{2}日)至([0-9]{4}年[0-9]{2}月[0-9]{2}日)</dd>'
ITEM_CLOSE_REGEX = u'此团购已结束，为您推荐以下团购'


def normDate(dateStr):
	dateStr = re.sub(u'[年月]', '-', dateStr)
	dateStr = re.sub(u'日', '', dateStr)
	return dateStr


def fetchPage(url):
	try:
		fd = urllib2.urlopen(url)
		content = fd.read()
		fd.close()
		content = re.sub('[\r\n]', '', content)
		content = content.decode('utf8', 'ignore')
		match = re.search(START_END_REGEX, content, re.M)
		start, end, expired = '', '', '0'
		if match:
			start, end = normDate(match.group(1)), normDate(match.group(2))
		if end=='' or content.find(ITEM_CLOSE_REGEX) != -1:
			expired = '1'
		print ('%s\t%s\t%s\t%s' % (url, start, end, expired)).encode('gb2312', 'ignore')
	except:
		print ('%s\t\t\t1' % url).encode('gb2312', 'ignore')



def getUrlList(urlFile):
	urlList = [line.strip() for line in open(urlFile)]
	return urlList


urls = [
	'http://m.nuomi.com/bj/deal/ir4keyzv',
	'http://m.nuomi.com/bj/deal/ds8fsen9',
	'http://m.nuomi.com/bj/deal/sfwdeypt',
	'http://m.nuomi.com/bj/deal/khv9bab3',
	'http://m.nuomi.com/bj/deal/0vfdvesu',
	'http://m.nuomi.com/bj/deal/icqcxb7m',
	'http://m.nuomi.com/bj/deal/szgdq2ie',
	'http://m.nuomi.com/bj/deal/q5jwahkc'
]



urlFile = sys.argv[1]

start = time.strftime( ISOTIMEFORMAT, time.localtime() )
print start

urlList = getUrlList(urlFile)
#urlList = urls

pool = ThreadPool(20)
pool.map(fetchPage, urlList)
pool.close()
pool.join()

end = time.strftime( ISOTIMEFORMAT, time.localtime() )
print end
