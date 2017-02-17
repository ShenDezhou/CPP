#!/bin/bash
#coding=gb2312
# 更新大众点评的休闲类的数据

. ./bin/Tool.sh
. ./bin/Host.sh

Log=logs/update.play.log
Python=/usr/bin/python

Play_list_urls=conf/dianping_play_list_urls


# 获取抓取任务抓取的URL列表
function get_play_urls() {
	backupFile="history/$(basename $Play_list_urls).$(todayStr)"
	if [ ! -f $backupFile ]; then
		mv -f $Play_list_urls $backupFile
	fi
	LOG "backup play list url $Play_list_urls into $backupFile" >> $Log

	rm -f tmp/scp/play/*
	scp 10.134.14.117:/search/liubing/spiderTask/result/system/task-4/* tmp/scp/play/
	cat tmp/scp/play/* > $Play_list_urls

	LOG "get play list url $Play_list_urls from remote host 10.134.14.117 done." >> $Log
}



# 将大众点评列表页解析的shop url list按城市split
function split_play_urls() {
	city_code_name_conf=conf/dianping_city_code_pinyin_conf
	$Python bin/split_dianping_listurl.py $city_code_name_conf $Play_list_urls "play"
	LOG "split $Play_list_urls into data/play/url/" >> $Log
}



# 扫库获取url列表的页面
function get_page_from_db() {
	local urlFile=$1;  local pageFile=$2;  local failedUrl=$3

	LOG "begin to get page of $urlFile ..." >> $Log
	cd /search/fangzi/workspace/Getpage/
		#awk -F'\t' '{if(!($1 in urls)){ urls[$1]; print $1 }}' $urlFile > urls
		#cat urls | ./bin/dbnetget -df cp -i url -o dd -d csum -l conf/offsum576 -pf $pageFile 1>get.std 2>get.err
		cat $urlFile | ./bin/dbnetget -df cp -i url -o dd -d csum -l conf/offsum576 -pf $pageFile 1>get.std 2>get.err

		awk '$1=="N"{print $2}' get.err >> $failedUrl
	cd -
	LOG "get page of $urlFile done." >> $Log
}


# 解压xpage页面
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

# nodejs 解析页面
function parse_page() {
	local htmlFile=$1;  local resultFile=$2;
	LOG "begin to parse page $htmlFile into $resultFile ..." >> $Log
	
	cd /search/fangzi/workspace/Fuwu
		node parse-cheerio.js $htmlFile | iconv -futf8 -tgbk -c >> $resultFile
	cd -
	LOG "parse page $htmlFile into $resultFile done." >> $Log
}


function get_parse_play_page() {
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





# 在本地执行先关操作
function get_parse_play_page_from_db() {
	play_url_path=data/play/url
	play_failurl_path=data/play/failurl
	play_result_path=data/play/result

	split_url_path=tmp/url/play
	count_per_file=10000

	cur_path=$(pwd)

	# 将每个城市的url文件切分成更小的文件，逐个处理
	for cityUrlFile in $(ls $play_url_path); do
		# 该城市的扫库失败的url
		failUrlFile=$play_failurl_path/${cityUrlFile%_*}_failurl
		rm -f $failUrlFile
		# 该城市的解析结果
		resultFile=$play_result_path/${cityUrlFile%_*}_result
		rm -f $resultFile

		# 切分成小文件
		rm -f $split_url_path/*
		awk -F'\t' '{if(!($1 in urls)){ urls[$1]; print $1 }}' $play_url_path/$cityUrlFile > $play_url_path/$cityUrlFile.uniq
		split -l$count_per_file $play_url_path/$cityUrlFile.uniq $split_url_path/$cityUrlFile
		for urlFile in $(ls $split_url_path); do
			splitUrlFile=$split_url_path/$urlFile
			get_parse_play_page "$cur_path/$splitUrlFile" "$cur_path/$resultFile" "$cur_path/$failUrlFile"
		done
	done
}




# 将远程机器上得到的结果传回到本地
function scp_host_result_to_local() {
	type=$1
	HostPath=/search/odin/dianping/result/$type
	LocalPath=/search/fangzi/ServiceApp/Dianping/data/$type/result/

	ScpFileExpect=/search/fangzi/ServiceApp/expect/scp_file.exp

	for host in ${NatHostList[@]}; do
		LOG "==========  scp result $host ============"
		$ScpFileExpect $host:$HostPath/*.result $LocalPath "$Passwd"
		LOG "scp result $host done."
		sleep 2
	done
	LOG "clean shop urls on host done."
}


	
# 清理远程分发机器上的urls
function clean_host_shop_urls() {
	cleanPath=$1
	cleanScript=/search/fangzi/ServiceApp/expect/clean_shop_urls.exp
	for host in ${NatHostList[@]}; do
		LOG "==========  clean $host ============"
		$cleanScript $host $Passwd $cleanPath
		LOG "clean $host done."
		sleep 2
	done
	LOG "clean shop urls on host done."
}


# 分发到不同的机器上
function dispatch_shop_urls() {
	urlFilePath=$1;  type=$2;
	echo "$type"

	HostPath=/search/odin/dianping/url/$type
	ScpFileExpect=/search/fangzi/ServiceApp/expect/scp_file.exp

	fileIdx=0
	hostSize=${#NatHostList[@]}
	for urlFile in $(ls $urlFilePath/*_urls); do
		urlFileName=$(basename $urlFile)

		# 计算要分发到的机器
		dispatchHost=${NatHostList[$fileIdx]}
		$ScpFileExpect $urlFile $dispatchHost:$HostPath/$urlFileName $Passwd
		
		fileIdx=$(( (fileIdx + 1) % $hostSize ))
		LOG "[dispatch url]: $dispatchHost  $urlFile"
	done
	LOG "dispatch $type urls done."
}


function batch_parse_page_imp() {
	type=$1

	# 将脚本拷贝过去，执行脚本
	LocalPath=/search/fangzi/ServiceApp/Dianping/bin
	HostPath=/search/odin/dianping/
	Script=batch_update_shops_on_host.sh

	ScpFileExpect=/search/fangzi/ServiceApp/expect/scp_file.exp
	ExecuteScriptExpect=/search/fangzi/ServiceApp/expect/execute_script.exp

	for host in ${NatHostList[@]}; do
		LOG "==========  scp & execute  $host ============"
		$ScpFileExpect $LocalPath/$Script $host:$HostPath "$Passwd"
		cmd="sh $Script $type 1>$type.std 2>$type.err &"
		echo "$cmd"
		$ExecuteScriptExpect $host "$HostPath" "$cmd" "$Passwd"
		#echo "$ExecuteScriptExpect $host $HostPath $cmd $Passwd"
		LOG "scp and execute for $host done."
		sleep 2
	done
	LOG "get and parse page on host done."
}


# 分发到多台机器上去做
function batch_parse_page_from_db() {
	$type="play"
	type=$1
	# 删除待分发的机器上的url文件
	cleanPath=/search/odin/dianping/url/$type/
	clean_host_shop_urls $cleanPath

	# 分发shop urls到分布式的机器上
	urlFilePath=/search/fangzi/ServiceApp/Dianping/data/$type/url
	#urlFilePath=/search/fangzi/ServiceApp/Dianping/test
	dispatch_shop_urls $urlFilePath $type

	# 到各台机器上执行处理脚本
	batch_parse_page_imp $type
}




function merge_online_urls_imp() {
	onlineUrlFile=$1; scanUrlFile=$2;
	if [ ! -f $onlineUrlFile -o ! -f $scanUrlFile ]; then
		LOG "$onlineUrlFile or $scanUrlFile is not exist"
	fi
	cat $onlineUrlFile $scanUrlFile | awk -F'\t' '{
		if (!($1 in urls)) {
			urls[$1];  print
		}
	}' > $scanUrlFile.merge
	rm -f $scanUrlFile;  mv $scanUrlFile.merge $scanUrlFile
}



# 将线上
function merge_online_urls() {
	#type="play"
	type=$1

	onlineUrlPath=/search/fangzi/ServiceApp/Dianping/data/$type/onlineUrl
	scanUrlPath=/search/fangzi/ServiceApp/Dianping/data/$type/url
	cityConf=conf/dianping_city_code_pinyin_conf


	while read cityCh cityCode cityEn; do
		scanUrlFile=$scanUrlPath/${cityEn}_urls
		onlineUrlFile=$onlineUrlPath/dianping_${type}_${cityEn}.urls
		merge_online_urls_imp $onlineUrlFile $scanUrlFile
		LOG "merge $city's $type urls"
	done < $cityConf
	LOG "merge all citys $type urls"
}



# 根据抓取的点评list页面解析shop urls的方式更新
function update_from_list() {
	rm -f data/play/url/*
	rm -f data/play/result/*
	rm -f data/play/failurl/*


	# -1. 获取线上shop URLs


	# 0. 获取抓取任务抓取的URL列表
	get_play_urls

	# 1. 区分出各城市的URL列表
	split_play_urls

	# 2. 扫库，解析页面
	get_parse_play_page_from_db
}



# 以扫库的方式进行增量更新，直接扫shop url的方式
function incre_update_from_scan_shopurls() {
	rm -f data/play/url/*
	rm -f data/play/result/*
	rm -f data/play/failurl/*

	# 对扫库清理出来的URL进行分离
	scanShopTypeUrls=Scan/data/shop_urls.types
	python bin/split_scan_shop_typeurl.py $scanShopTypeUrls play

	# 2. 扫库，解析页面
	get_parse_play_page_from_db
}



# 以扫库的方式进行全量更新，直接扫shop url的方式
function full_update_from_scan_shopurls() {
	#local type="play"
	local type="restaurant"

	rm -f data/$type/url/*
	rm -f data/$type/result/*
	rm -f data/$type/failurl/*

	# 对扫库清理出来的URL进行分离
	# 参考 bin/filte_city_type_urls.py 
	scanShopTypeUrls=Scan/data/shop_urls.types
	python bin/split_scan_shop_typeurl.py $scanShopTypeUrls $type

	# 将线上存在的URL加入进来 追加到后面
	merge_online_urls $type

	# 2. 扫库，解析页面
	batch_parse_page_from_db $type
}



#incre_update_from_scan_shopurls
#batch_parse_page_from_db

scp_host_result_to_local restaurant

#full_update_from_scan_shopurls

