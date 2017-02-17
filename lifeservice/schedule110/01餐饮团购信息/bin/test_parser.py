import sys,re
#coding=gb2312

def getText(lines,tag,end):
    flag = 0
    text = ""
    for line in lines:
        line = line.strip();
        if line.find(tag) != -1:
            flag = 1
        if flag == 1 and line.find(end) != -1:
            break;
        if flag == 1 and line != "" and line.find("<") == -1:
            text = line
    return text

huiTag="shop-hui-list"
tuanTag="<div class=\"shop-tuan-list\">"
huiLink="http://m.dianping.com/hui/unicashier"
huiLink1="http://m.dianping.com/hui/help/unusablecoupons"

hui="0"
ding="0"
wai="0"
isClose="0"

huiStack=0
huiLine=[]
divStack=0
divLine=[]

content = ''
if __name__ == "__main__":
	if len(sys.argv) < 3:
		exit()
        file=sys.argv[1]
        shopId=sys.argv[2]
	for line in open(file) :
	    line = line.strip();   
	    content = content + line.strip('\n');   
	    if line.find("itemPicBg bookItem") != -1:
		ding="1"
	    if line.find("itemPicBg takeawayItem") != -1:
		wai="1"
	    if line.find('class=\"upload_error\"') != -1:
		isClose="1"
		
	    if line.find(huiLink) != -1 or line.find(huiLink1):
	       hui = "1"
		
	    if line.find(huiTag) != -1:
	        hui = "1"
		huiStack += 1
		huiLine.append(line)
		
	    if huiStack > 0 and line.find(huiTag) == -1:
		if line.find("<div") != -1:
                        huiStack += 1
                if line.find("</div>") != -1:
                        huiStack -= 1
                huiLine.append(line)
	    
	    if line.find(tuanTag) != -1:
		divStack+=1
		divLine.append(line)
	    if divStack > 0 and line.find(tuanTag) == -1:
		if line.find("<div") != -1:
			divStack += 1
		if line.find("</div>") != -1:
			divStack -= 1
		divLine.append(line)
	
	pattern_url=re.compile("<a class=\"item\" href=\"(.*)\" onclick")
	pattern_pic=re.compile("src=\"(.*)\"")
	pattern_title=re.compile("<div class=\"newtitle\">(.*)</div>")
	pattern_symbol=re.compile("<span class=\"symbol\">.*?</span>")
	pattern_price=re.compile("<span class=\"price\">.*?</span>")
	pattern_oprice=re.compile("<span class=\"o-price\">(.*)</span>")
	pattern_sale=re.compile("<span class=\"sale\">(.*)</span>")

	pattern_spanStart=re.compile("<span.*?>")
	pattern_spanEnd=re.compile("</span.*?>")
		
        for line in divLine:
		line=line.strip()
		if pattern_url.search(line) != None:
			print shopId+"\turl\t" + pattern_url.search(line).group(1)
		if pattern_pic.search(line) != None:
			print shopId+"\tpic\t" + pattern_pic.search(line).group(1)
		if pattern_title.search(line) != None:
			print shopId+"\ttitle\t"+pattern_title.search(line).group(1)
		if pattern_symbol.search(line) != None:
			symbol=pattern_symbol.search(line).group(0)
			symbol=pattern_spanStart.sub("",symbol)
			symbol=pattern_spanEnd.sub("",symbol)
			print shopId+"\tsymbol\t"+symbol
		if pattern_price.search(line) != None:
			price=pattern_price.search(line).group(0)
			price=pattern_spanStart.sub("",price)
			price=pattern_spanEnd.sub("",price)
			print shopId+"\tprice\t"+price
		if pattern_oprice.search(line) != None:
			print shopId+"\toprice\t"+pattern_oprice.search(line).group(1)
		if pattern_sale.search(line) != None:
			print shopId+"\tsale\t"+pattern_sale.search(line).group(1)
	
	text1 = getText(huiLine,"div class=\"newtitle\"","</div>")
	text2 = getText(huiLine,"div class=\"info\"","</div>")
	text3 = getText(huiLine,"span class=\"soldNum\"","</span>")
	text4 = getText(huiLine,"style=\"color:999999;background-color:","</span>")

	if hui == "1" or ding =="1" or wai == "1" or isClose == "1":
	    print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (shopId,hui,ding,wai,text1,text2,text3,text4,isClose)

	
	avgPrice, photo, score = '', '', ''
	pattern_avgPrice = 'class="shopInfoPagelet".*?class="price">([0-9\.]+)</span>'
	pattern_photo = 'class="shopInfoPagelet".*?<img src="(.*?)"'
	pattern_score = 'class="shopInfoPagelet".*?class="star star-([0-9]+)"'
	pattern_businesstime = 'class="businessTime">(.*?)</div>'

	if re.search(pattern_avgPrice, content):
		avgPrice = re.search(pattern_avgPrice, content).group(1)
		#print 'price\t%s' % re.search(pattern_avgPrice, content).group(1)
	if re.search(pattern_photo, content):
		photo = re.search(pattern_photo, content).group(1)
		#print 'photo\t%s' % re.search(pattern_photo, content).group(1)
	if re.search(pattern_score, content):
		score = re.search(pattern_score, content).group(1)
		score = str(float(score) / 10)
		#print 'score\t%s' % str(float(score) / 10)
	if re.search(pattern_businesstime, content):
		businesstime = re.search(pattern_businesstime, content).group(1)
		businesstime = businesstime.replace('&nbsp;', ' ')
	print '%s\tbaseinfo\t%s\t%s\t%s\t%s' % (shopId,photo,avgPrice,score,businesstime)
