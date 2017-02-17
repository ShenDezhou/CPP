#!/bin/bash
#coding=gb2312

Log="logs/build_daily_dianping_tuan.log"

. ./bin/Tool.sh
. ./bin/dianping_tuan_tool.sh


# 合并团购信息的全量与增量
function merge_tuan_imp() {
	allFile=$1;  addFile=$2;  output=$3;

	# 合并，并去除过期数据
	awk -F'\t' ' ARGIND == 1 {
		# 团购全集
		if (FNR == 1) {
			print; next
		}
		tuanid=$1;  resid=$2;  url=$5;  deadline=$NF
		tuanItem = resid "\t" url
		allTuanItem[tuanItem]
		print

	} ARGIND == 2 {
		# 新增的团购
		if (FNR == 1) {
			next
		}
		# 过滤已存在的团购
		resid=$2;  url=$5;
		tuanItem = resid "\t" url
		if (tuanItem in allTuanItem) {
			next
		}
		print
	}' $allFile $addFile > $output
}


# 将每天的团购信息，添加到全局中
function merge_daily_tuan() {
	for allTuanFile in $(ls tuandata/all/*.data.format); do
		dailyTuanFile=${allTuanFile/all/daily}
		if [ ! -f $dailyTuanFile ]; then
			continue
		fi

		# 合并全量与增量
		merge_tuan_imp $allTuanFile $dailyTuanFile $allTuanFile.adddaily

		# 备份，更新
		backFile="tuandata/history/$(basename $allTuanFile).$(todayStr)"
		rm -f $backFile;   mv $allTuanFile $backFile
		cp -f $allTuanFile.adddaily $allTuanFile

		LOG "handle $allTuanFile done." >> $Log
	done
}


# 转格式
function format_daily_tuan() {
	for tuanFile in $(ls tuandata/daily/*.data); do
		format $tuanFile $tuanFile.format
		LOG "format $tuanFile done." >> $Log
	done
}


function main() {
	# 删除历史数据
	find ./tuandata/history/ -ctime +5 | xargs rm -f {}

	# 下载当天的团购信息
	/usr/bin/python  bin/build_daily_dianping_tuan.py
	LOG "get daily dianping tuan done." >> $Log

	# 转换格式
	format_daily_tuan
	LOG "format all daily dianping tuan done." >> $Log

	# 合并到全部团购信息中	
	merge_daily_tuan
	LOG "merge daily dianping tuan done." >> $Log
}

main

