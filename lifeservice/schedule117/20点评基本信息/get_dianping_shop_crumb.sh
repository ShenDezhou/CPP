#!/bin/bash
#coding=gbk
# 获取大众点评的crumb，用于对url分类

. ./bin/tool.sh

Python=/usr/bin/python

Max_Thread_Num=50000
#Min_Num_Per_Thread=100000
Min_Num_Per_Thread=10


UrlFile=/search/odin/PageDB/GetPage/input/pattern_dianping_shop/part-00000
#UrlFile=/search/odin/PageDB/GetPage/input/pattern_dianping_shop/testurls
UrlSplitPath=/search/odin/PageDB/GetPage/tmp/dianping_shop/url

GetPagePath=/search/odin/PageDB/GetPage
ParsePath=/search/odin/PageDB/Application/Dianping/data/crumb


# 切分url
function splitUrl() {
	input=$1; output=$2;

	threadNum=5
	urlCount=$(cat $input | wc -l)
	#if [ $urlCount -gt $Min_Num_Per_Thread ]; then
	#	threadNum=$Max_Thread_Num
	#fi
	numPreThread=$(( $urlCount / $threadNum + 1))

	rm -f $output/dianping_shop_url_*
	split -l$numPreThread $input $output/dianping_shop_url_
	INFO "split urls done.[$output]"	
}


# 对上一步切分的url文件，获取xpage
function crawler() {
	input=$1
	cd /search/odin/PageDB/GetPage/
		sh bin/scan_xml_xpage.sh $input
	cd -
	INFO "crawler $input done."
}



function multiCrawlerImp() {
	local input=$1;  local output=$2;

	# split to small url file
	rm -f ${input}_part*
	split -l$Max_Thread_Num $input -a3 ${input}_part
	
	# crawler parse
	for subUrlFile in $(ls ${input}_part*); do
		#crawler $subUrlFile
		#$Python bin/decode_dianping_crumb.py ${subUrlFile}pages > $output/$(basename $subUrlFile)
		#rm -f $subUrlFile
		rm -f ${subUrlFile}pages*
		sleep 1
	done

	INFO "handle $input"
}



function multiCrawler() {
	local output=$1
	rm -f $output/*

	#for urlFile in $(ls $UrlSplitPath/dianping_shop_url_*); do
	#	multiCrawlerImp $urlFile $output &
	#	sleep 2
	#done
	#wait

	outputPath=/search/odin/PageDB/GetPage/tmp/dianping_shop/output
	rm -f $UrlSplitPath/*part*
	for urlFile in $(ls $UrlSplitPath/dianping_shop_url_*); do
		split -l$Max_Thread_Num $urlFile -a3 ${urlFile}_part
		for subUrlFile in $(ls ${urlFile}_part*); do
			#echo $subUrlFile
			crawler $subUrlFile
			$Python bin/decode_dianping_crumb.py ${subUrlFile}pages > $outputPath/$(basename $subUrlFile)
			rm -f ${subUrlFile}pages*
			sleep 1
		done
		sleep 1
	done
	INFO "multi crawler dianping crumb done."
	
}

# 将新扫出来的数据拷贝到110机器上，去分发抓取解析页面
function scpShopurlsTo110() {
	scp data/crumb/dianping.url.types 10.134.96.110:/search/fangzi/ServiceApp/Dianping/Scan/data/shop_urls.types
}


function main() {
	outputPath=/search/odin/PageDB/GetPage/tmp/dianping_shop/output
	# 切分
	splitUrl $UrlFile $UrlSplitPath

	# 多线程处理，每个线程再次切分，然后顺序执行
	multiCrawler $outputPath

	# 合并结果
	cat $outputPath/* > $ParsePath/dianping.crumb

	# 对url进行分类
	$Python bin/split_city_type_urls.py $ParsePath/dianping.crumb
	
	# 拷贝到110机器上
	scpShopurlsTo110

	# 到110机器上去执行
	# 10.134.96.110:/search/fangzi/ServiceApp/Dianping/bin/update_dianping_baseinfo.sh
}

main



