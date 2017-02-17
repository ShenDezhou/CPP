#!/bin/bash
#coding=gb2312
# 扫库，更新大众点评的数据


# 1. 扫库，得到大众点评的列表url  http://web_sac.sogou-inc.com/scan/tasksubmit.html   (scan from index)
# 2. getmerge 扫库结果到本地  Scan/data/dianping_scan_list


. ./bin/Tool.sh

if [ $# -lt 3 ]; then
	echo "Usage: sh $0 urlFile resultFile failUrlFile" && exit -1
fi

# 都是绝对路径
urlFile=$1;  resultFile=$2;  failUrlFile=$3; threadIdx="";

# 线程号
if [ $# -gt 3 ]; then
	threadIdx=$4
fi

#urlFile=/search/fangzi/ServiceApp/Dianping/test_dianping_urls 
#resultFile=/search/fangzi/ServiceApp/Dianping/test/result
#failUrlFile=/search/fangzi/ServiceApp/Dianping/test/failUrl


#get_parse_restaurant_page $urlFile $resultFile $failUrlFile


Log=logs/get_parse_page_thread${threadIdx}.log



# 扫库获取url列表的页面
# 参数都是绝对路径
function get_page_from_db() {
	local urlFile=$1;  local pageFile=$2;  local failedUrl=$3

	LOG "begin to get page of $urlFile ..." >> $Log
	cd /search/fangzi/workspace/Getpage/
		cat $urlFile | ./bin/dbnetget -df cp -i url -o dd -d csum -l conf/offsum576 -pf $pageFile 1>get${threadIdx}.std 2>get${threadIdx}.err
		awk '$1=="N"{print $2}' get${threadIdx}.err >> $failedUrl
	cd -
	LOG "get page of $urlFile done." >> $Log
}


# 解压xpage页面
# 参数需要提供绝对路径
function decode_page() {
	local pageFile=$1;  local decodeFile=$2
	local pageFilePrefix=${pageFile/page0/page}
	
	LOG "begin to decode page $pageFile into $decodeFile ..." >> $Log
	cd /search/fangzi/workspace/ParsePage/
		cat conf/offsum.tailer >> $pageFile
		sed "s#PAGEPATH#$pageFilePrefix#g" ./antispam.struct/conf/parsePage.conf.template > ./antispam.struct/conf/parsePage${threadIdx}.conf
		# 解压xpath格式页面，并转成下一步可以被Nodejs解析的格式
		./decodePage ./antispam.struct/conf/parsePage${threadIdx}.conf > page${threadIdx}.html
		sh bin/convert_html_to_line.sh page${threadIdx}.html $decodeFile
	cd -
	LOG "decode page $pageFile into $decodeFile done." >> $Log
}

# nodejs 解析页面
# 参数是绝对路径
function parse_page() {
	local htmlFile=$1;  local resultFile=$2;
	LOG "begin to parse page $htmlFile into $resultFile ..." >> $Log
	
	cd /search/fangzi/workspace/Fuwu
		node parse-cheerio.js $htmlFile | iconv -futf8 -tgbk -c >> $resultFile
	cd -
	LOG "parse page $htmlFile into $resultFile done." >> $Log
}


function get_parse_restaurant_page() {
	local urlFile=$1;  local resultFile=$2;  local failedUrl=$3
	LOG "begin get & parse restaurant page, $urlFile" >> $Log
	
	# 扫库得到xpage页面
	pageFile=${urlFile}_page0
	get_page_from_db $urlFile $pageFile $failedUrl	
	
	# 解压页面 
	decodePageFile=$urlFile.html
	decode_page $pageFile $decodePageFile
	
	# 解析页面
	parse_page $decodePageFile $resultFile
}


#get_parse_restaurant_page $urlFile $resultFile $failUrlFile




