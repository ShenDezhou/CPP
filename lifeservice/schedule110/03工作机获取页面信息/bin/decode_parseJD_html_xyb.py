#! /usr/bin/python
from lxml import etree
import zlib
import sys
import re

UC="utf8"
GC="gb18030"
codePattern=re.compile("<meta.*?charset=.*?>")

def getBlock(file_name, url_dict = None):
	"""Parse the page file
	Return [url, html_string]
	"""
	lineArr = []
	fp=open(file_name,"rb")
	bl = 0 
	url = ""  
	size = 0 
	while True:
		line = fp.readline()
		if len(line) == 0:
			break
		if line.startswith("Error-Reason"):
			fp.readline()
			continue
		if line.startswith("http:"):
			url = line.strip()
			continue
		if line.startswith("Store-Size:"):
			size = int(line.split(":")[1].strip())
			continue
		if len(line.strip()) == 0:
			bl +=1 
		if bl == 2:
			bl = 0 
			htmlstr = ""
			if url_dict != None and url in url_dict:
				fp.seek(size, 1)
				fp.readline()
				continue
			else:
				compresseddata = fp.read(size)
				try:
					htmlstr = zlib.decompress(compresseddata)
				except Exception, e:
					sys.stderr.write("Error: " + url + "\n" + str(e) + "\n")
																																		   
				fp.readline()																											  
																																		   
			yield [url, htmlstr]  

def detectCode(page):
	curCode = GC
	codeTable = codePattern.findall(page)	
	if len(codeTable)> 0:
		codeLine = codeTable[0].lower()
		if codeLine.find("charset=\"utf")!=-1 or codeLine.find("charset=utf")!=-1:
			curCode = UC
	return curCode
		
def transList(oldList):
	newList = []
	for item in oldList:
		newList.append(item.encode(GC,'ignore').strip().replace("\r\n","").replace("\n","").strip())
	return newList


def parsePage(page):
	curCode = detectCode(page)
	pageU = page.decode(curCode,"ignore")
	pageXpath = etree.HTML(pageU)
	#print "URL:"+url	
	
	title=""
	titleT = pageXpath.xpath('//title')
	if len(titleT)>0:
		title = titleT[0].text.encode(GC,'ignore').replace("\r\n","").replace("\n","").strip()
	#print "TIT:"+title	
	
	skuName=""
	skuT = pageXpath.xpath('//div[@class="sku-name"]/text()')	
	if len(skuT)>0:
		skuName = skuT[0].encode(GC,'ignore').replace("\r\n","").replace("\n","").strip()
	else:
		skuT = pageXpath.xpath('//div[@class="m-item-inner"]/div[@id="itemInfo"]/div[@id="name"]/h1/text()')
		if len(skuT)>0:
			skuName = skuT[0].encode(GC,'ignore').replace("\r\n","").replace("\n","").strip()
	#print "SKU:"+skuName
	'''	
	cateLine = ""
	cateT = pageXpath.xpath('//div[@class="crumb fl clearfix"]/div/a/text()')
	lowestCate = pageXpath.xpath('//div[@class="crumb fl clearfix"]/div[last()]/text()')
	cateT.extend(lowestCate)
	if len(cateT)==0:
		cateT = pageXpath.xpath('//div[@class="breadcrumb"]//a/text()')
	cateT = transList(cateT)
	cateLine="\001".join(cateT)
	'''
	cateLine = ""
	cateT = []
	for index in range(1,21):
		classTag = "shangpin|keycount|product|mbNav-"+str(index)
		tmpCateT = pageXpath.xpath('//a[@clstag="'+classTag+'"]/text()')
		if len(tmpCateT)>0:
			cateT.append(tmpCateT[0])
	lowestCate = pageXpath.xpath('//div[@class="crumb fl clearfix"]/div[last()]/text()|//div[@class="breadcrumb"]/span[last()]/a[last()]/text()')
	if len(lowestCate)>0:
		cateT.extend(lowestCate)
	cateT = transList(cateT)	
	cateLine = "\001".join(cateT)


	para = ""
	paraT = pageXpath.xpath('//ul[@class="parameter2 p-parameter-list"]/li//text()')
	if len(paraT) == 0:
		paraT = pageXpath.xpath('//ul[@id="parameter2" and @class="p-parameter-list"]/li//text()')
	paraT = transList(paraT)
	para = "\001".join(paraT)	
	return title,skuName,cateLine,para

def test():
	input_file = sys.argv[1]
	for block in getBlock(input_file):
		url = block[0]
		page = block[1] 
		try:
			title,skuName,cateLine,para = parsePage(page)
			if len(skuName)==0 and len(cateLine)==0 and len(para)==0:
				continue 
			print url+"\t"+"TITL:"+title
			print url+"\t"+"NAME:"+skuName
			print url+"\t"+"CATE:"+cateLine
			print url+"\t"+"INFO:"+para
		except:
			continue
if __name__ == '__main__':
	test()
