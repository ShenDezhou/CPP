#!/bin/bash
#coding=gb2312

conf="conf/cp_dianping_restaurant_conf"
resultPath="/search/liubing/spiderTask/result/dianping_restaurant/task-"
destBasicDir="/search/zhangk/Fuwu/Source/Crawler/"
#destBasicDir="/tmp/Crawler"

#/search/zhangk/Fuwu/Source/Crawler/beijing/restaurant/

while read taskid city poiNum; do
	taskResultPath=${resultPath}${taskid}
	if [ -d $taskResultPath ]; then
		latestFile=$(ls -l -rt $taskResultPath | tail -n1 | awk 'NF>6{print $NF}')
		srcFile=$taskResultPath/$latestFile
		if [ ! -f $srcFile ]; then
			echo "$taskid is not done."
			continue
		fi
		
		# 目的目录
		destDir=$destBasicDir/$city
		if [ ! -d $destDir ]; then
			mkdir $destDir
		fi

		if [ ! -d $destDir/restaurant ]; then
			mkdir $destDir/restaurant
		fi
		
		# 验证抓取的成功率
		urlCnt=$(fgrep url $srcFile | wc -l)
		ratio=$(awk -v a=$urlCnt -v b=$poiNum 'BEGIN{c=a/b; if(c>0.8){print "succ"}else{print "failed"}}')
		if [ "$ratio" == "failed" ]; then
			echo -e "warn\t$city : $taskid only spider $urlCnt of $poiNum "
			#continue
		else
			echo -e "info\t$city : $taskid spider done."
		fi

		destFile="dianping_detail.task$taskid"

		iconv -futf8 -tgbk -c $srcFile > $destDir/restaurant/$destFile

	fi
done < $conf
