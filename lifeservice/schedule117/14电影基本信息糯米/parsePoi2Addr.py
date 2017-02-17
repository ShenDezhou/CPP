from HttpRequestTool import *
import simplejson as json


header = {"Cookie":"mpuv=Cq0EVVbqJCFkQT2pTQMEAg==; pgv_pvi=7854299136; RK=LLumJZtDcb; ptcz=c9f33c54ef33f68243394c4889a46eec82f7a8467a38a4b2a61210c1c3499c7d; pt2gguin=o0949640570; pgv_pvid=71183140; o_cookie=949640570; pgv_si=s7993484288; uin=null; skey=null; luin=null; lskey=null; user_id=null; session_id=null","Referer":"http://lbs.qq.com/webservice_v1/guide-gcoder.html", "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36"}


getAddrUrl = 'http://apis.map.qq.com/ws/geocoder/v1/?&callback=QQmap&location=LAT%2CLNG&get_poi=1&key=OB4BZ-D4W3U-B7VVO-4PJWW-6TKDJ-WPB77&output=jsonp&_=1460014450263'



CITY_ROW, URL_ROW, POI_ROW = 0, 1, 3
CinemaFile = 'tmp/cinema.sort.tail'



def getAddr(url):
	response = GetRequest(url, header)
	response = response.replace('QQmap&&QQmap(', '').replace(')', '')
	#print response
	resultDict = json.loads(response)

	addrDict = resultDict["result"]["address_component"]

	city = ''
	#province = resultDict["result"]["address_component"]["province"]
	if 'city' in addrDict: city = addrDict['city']
	#district = resultDict["result"]["address_component"]["district"]
	return city.encode('gb2312', 'ignore')

count = 0
for line in open(CinemaFile):
	segs = line.strip('\n').split('\t')
	if len(segs) < 6:
		continue
	city, url, poi = segs[CITY_ROW], segs[URL_ROW], segs[POI_ROW]
	poiSeg = poi.split(',')
	if len(poiSeg) != 2:
		continue
	lat, lng = poiSeg[0], poiSeg[1]
	getUrl = getAddrUrl.replace('LAT', lat).replace('LNG', lng)
	addr = getAddr(getUrl)

	print addr, city
	if addr.find(city) != -1:
		print "Y\t", url
	else:
		print "N\t", url

	#count += 1
	#if (count % 10) == 0:
#		break

