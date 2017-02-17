#!/bin/bash
#coding=gb2312

user="Dianping_Play"
type="play"
conf="conf/cp_dianping_play_conf"
resultPath="/search/liubing/spiderTask/result/"$user"/task-"
destBasicDir="/search/zhangk/Fuwu/Source/Crawler/"


#/search/zhangk/Fuwu/Source/Crawler/beijing/play/

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

		if [ ! -d $destDir/$type ]; then
			mkdir $destDir/$type
		fi
		
		# 验证抓取的成功率
		urlCnt=$(fgrep url $srcFile | wc -l)
		ratio=$(awk -v a=$urlCnt -v b=$poiNum 'BEGIN{c=a/b; if(c>0.8){print "succ"}else{print "failed"}}')
		if [ "$ratio" == "failed" ]; then
			echo -e "[Warn]: \t$city : $taskid only spider $urlCnt of $poiNum !!!!!!!!!!!!!!!!!!!!!!"
			#continue
		else
			echo -e "[Info]: \t$city : $taskid spider done."
		fi

		destFile="dianping_detail.task$taskid"

		iconv -futf8 -tgbk -c $srcFile > $destDir/$type/$destFile

	fi
done < $conf
