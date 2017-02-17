#! /usr/bin/python
from lxml import etree
import zlib
import sys
import re

UC="utf8"
GC="gb18030"
codePattern=re.compile("<meta.*?charset=.*?>")
lemmaIdEncPattern=re.compile("newLemmaIdEnc:\".*?\"")

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
	modifiedtime="1900-01-01"
	if len(titleT)>0:
		title = titleT[0].text.encode(GC,'ignore').replace("\r\n","").replace("\n","").strip()
	modified_time = pageXpath.xpath('//span[@class="j-modified-time"]/text()');
	if len(modified_time) == 0:
		modified_time = pageXpath.xpath('//span[@id="lastModifyTime"]/text()');
	if len(modified_time) > 0:
		modifiedtime =  modified_time[0]
	lemmaId = lemmaIdEncPattern.findall(page)
	lemmaIdEnc = "-1"
	if len(lemmaId) > 0:
		lemmaIdEnc = lemmaId[0][15:-1]
	return title,modifiedtime,lemmaIdEnc
	
def test():
	input_file = sys.argv[1]
	for block in getBlock(input_file):
		url = block[0]
		page = block[1]
		try:
			title,modified_time,lemmaIdEnc = parsePage(page)
			print url+"\t"+"TITL:"+title+"\tMOD_TIME:"+modified_time+"\tlemmaIdEnc:"+lemmaIdEnc
		except Exception, e:
			print >> sys.stderr,e,url
			continue
if __name__ == '__main__':
	test()
