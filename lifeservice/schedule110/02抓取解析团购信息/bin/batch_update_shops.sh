#!/bin/bash
#coding=gb2312
# 批量更新大众点评的美食/休闲数据

Type="restaurant"
if [ $# -gt 0 ]; then
	Type=$1
fi


. ./bin/Tool.sh

# 各种中间文件的存储路径
UrlPath=/search/odin/dianping/url/$Type
PagePath=/search/odin/dianping/page/$Type
ResultPath=/search/odin/dianping/result/$Type

# 解析工具脚本
GetPagePath=/search/odin/Getpage/
GetPageScript=get_html_from_db.sh
ParsePagePath=/search/odin/NodeParser/
ParsePageScript=parse-cheerio.js

Node=/usr/local/node/0.10.24/bin/node

PartNumber=2

function clean() {
	rm -f $ResultPath/*
	rm -f $PagePath/*
}


# 获取html页面内容，并解析
function get_parse_shops() {
	local urlFile=$1;  local pageFile=$2;  local resultFile=$3;
	cd $GetPagePath
		sh $GetPageScript $urlFile $pageFile
	cd -
	LOG "get html of $urlFile done. [ $pageFile ]"

	cd $ParsePagePath
		$Node $ParsePageScript $pageFile | iconv -futf8 -tgbk -c > $resultFile
	cd -
	LOG "parse html page of $pageFile done. [ $resultFile ]"
}





function batch_get_parse_shops() {
	splitPath=/search/odin/dianping

	for urlFile in $(ls $UrlPath/*_urls); do
		urlFileName=$(basename $urlFile)
		resultFile=$ResultPath/${urlFileName}.result
		rm -f $resultFile

		split -l$PartNumber $urlFile $splitPath/${urlFileName}_part
		for partUrl in $(ls $splitPath/${urlFileName}_part*); do
			partPageFile=$PagePath/$(basename $partUrl)_page
			partResultFile=$ResultPath/$(basename $partUrl)_result

			get_parse_shops $partUrl $partPageFile $partResultFile
			cat $partResultFile >> $resultFile
			
			# 删除中间文件(part页面和结果)
			rm -f $partResultFile
			rm -f $partPageFile
		done
	
		rm -f $splitPath/${urlFileName}_part*
	done
	LOG "get and parse all city's shops done."
}


clean
batch_get_parse_shops


