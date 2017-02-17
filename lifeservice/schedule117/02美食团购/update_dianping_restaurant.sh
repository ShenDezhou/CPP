#!/bin/bash
#coding=gb2312
# 更新大众点评的美食的数据

. ./bin/Tool.sh
Log=logs/update.log
Python=/usr/bin/python

Restaurant_list_urls=conf/dianping_list_urls


function get_restaurant_urls() {
	backupFile="history/$(basename $Restaurant_list_urls).$(todayStr)"
	if [ ! -f $backupFile ]; then
		mv -f $Restaurant_list_urls $backupFile
	fi
	LOG "backup restaurant list url $Restaurant_list_urls into $backupFile" >> $Log

	rm -f tmp/scp/*
	scp 10.134.14.117:/search/liubing/spiderTask/result/system/task-2/* tmp/scp/
	cat tmp/scp/* > $Restaurant_list_urls

	LOG "get restaurant list url $Restaurant_list_urls from remote host 10.134.14.117 done." >> $Log
}




# 将大众点评列表页解析的shop url list按城市split
function split_restaurant_urls() {
	city_code_name_conf=conf/dianping_city_code_pinyin_conf
	$Python bin/split_restaurant_listurl.py $city_code_name_conf $Restaurant_list_urls
	LOG "split $Restaurant_list_urls into data/restaurant/url/" >> $Log
}



# 扫库获取url列表的页面
function get_page_from_db() {
	local urlFile=$1;  local pageFile=$2;  local failedUrl=$3

	LOG "begin to get page of $urlFile ..." >> $Log
	cd /search/fangzi/workspace/Getpage/
		awk -F'\t' '{if(!($1 in urls)){ urls[$1]; print $1 }}' $urlFile > urls
		cat urls | ./bin/dbnetget -df cp -i url -o dd -d csum -l conf/offsum576 -pf $pageFile 1>get.std 2>get.err

		awk '$1=="N"{print $2}' get.err >> $failedUrl
	cd -
	LOG "get page of $urlFile done." >> $Log
}


# 解析页面
function decode_page() {
	local pageFile=$1;  local decodeFile=$2
	local pageFilePrefix=${pageFile/page0/page}
	
	LOG "begin to decode page $pageFile into $decodeFile ..." >> $Log
	cd /search/fangzi/workspace/ParsePage/
		cat conf/offsum.tailer >> $pageFile
		sed "s#PAGEPATH#$pageFilePrefix#g" ./antispam.struct/conf/parsePage.conf.template > ./antispam.struct/conf/parsePage.conf
		# 解压xpath格式页面，并转成下一步可以被Nodejs解析的格式
		./decodePage ./antispam.struct/conf/parsePage.conf > page.html
		sh bin/convert_html_to_line.sh page.html $decodeFile
	cd -
	LOG "decode page $pageFile into $decodeFile done." >> $Log
}


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


	pageFile=${urlFile}_page0
	get_page_from_db $urlFile $pageFile $failedUrl	

	decodePageFile=$urlFile.html
	decode_page $pageFile $decodePageFile
	
	parse_page $decodePageFile $resultFile

}





function get_parse_restaurant_page_from_db() {
	restaurant_url_path=data/restaurant/url
	restaurant_failurl_path=data/restaurant/failurl
	restaurant_result_path=data/restaurant/result

	split_url_path=tmp/url
	count_per_file=10000

	cur_apth=$(pwd)

	# 将每个城市的url文件切分成更小的文件，逐个处理
	for cityUrlFile in $(ls $restaurant_url_path); do
		# 该城市的扫库失败的url
		failUrlFile=$restaurant_failurl_path/${cityUrlFile%_*}_failurl
		rm -f $failUrlFile
		# 该城市的解析结果
		resultFile=$restaurant_result_path/${cityUrlFile%_*}_result
		rm -f $resultFile

		# 切分成小文件
		rm -f $split_url_path/*
		split -l$count_per_file $restaurant_url_path/$cityUrlFile $split_url_path/$cityUrlFile
		for urlFile in $(ls $split_url_path); do
			splitUrlFile=$split_url_path/$urlFile
			get_parse_restaurant_page "$cur_apth/$splitUrlFile" "$cur_apth/$resultFile" "$cur_apth/$failUrlFile"
		done
	done
}




function main() {
	rm -f data/restaurant/url/*
	rm -f data/restaurant/result/*
	rm -f data/restaurant/failurl/*

	# 0. 获取抓取任务抓取的URL列表
	get_restaurant_urls

	# 1. 区分出各城市的URL列表
	split_restaurant_urls

	# 2. 扫库，解析页面
	get_parse_restaurant_page_from_db


}


main
