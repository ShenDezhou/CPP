#coding=gb2312
import sys
from lxml import objectify

def printItem(item):
	if type(item) == unicode:
		print item.encode('gb2312', 'ignore')
	else:
		print item.decode('utf8', 'ignore').encode('gb2312', 'ignore')

def taskFirst(item):
	if type(item) == list and len(item) > 0:
		return item[0].content.strip()
	return item

def parseService(input, source='Huatuojiadao'):
	doc = objectify.parse(open(input))
	root = doc.getroot()
	for item in root.item:
		try:
			title = item.name.text
			img = item.imgs.img[0].text

			openTime = item.opening_time.text
			tel = item.phone.text

			# location
			city = item.location.city.text
			district = item.location.district.text
			area = item.location.area.text
			poi = item.location.latlng.text
			addr = ''
			try:
				addr = item.location.storeAddress.text
			except:
				addr = item.location.addr.text
			printItem('Shop\t%s\t%s\t%s@@@%s\t%s\t%s\t%s\t%s' % (city, source, title, poi, title, poi, tel, addr))


			# services
			for service in item.services.iterchildren():
				serviceName = service.service_name.text
				serviceImg = service.service_imgs.service_img.text
				serviceValue = service.original_price.text
				servicePrice = service.discount_price.text
				serviceScore = service.service_score.text
				serviceTime = service.service_time.text
				serviceMode = service.service_mode.text
				serviceUrl = service.url.text
				
				printItem('Service\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (serviceName, serviceUrl, serviceImg, serviceValue, servicePrice, serviceScore, serviceTime, serviceMode))
		except:
			pass


if __name__ == '__main__':
	# 解析足疗按摩的数据
	input, source = sys.argv[1], sys.argv[2]
	parseService(input, source)
