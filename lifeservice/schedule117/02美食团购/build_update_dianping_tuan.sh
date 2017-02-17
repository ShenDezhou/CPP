#!/bin/bash
#coding=gb2312

Log="logs/build_update_dianping_tuan.log"

. ./bin/Tool.sh
. ./bin/dianping_tuan_tool.sh


# 合并团购信息的全量与增量
function merge_tuan_imp() {
	allFile=$1;  updateTuanFile=$2; updateStatusFile=$3;  output=$4;

	# 合并，并去除过期数据
	awk -F'\t' 'ARGIND == 1 {
		# 状态值为0 过滤掉
		if (NF != 2) {
			next
		}
		id=$1; status=$2;
		if (status == "0") {
			deleteids[id]	
		}
	} ARGIND == 2 {
		# 更新的团购数据
		deleteids[$1]
		print
	} ARGIND == 3 {
		# 团购全集
		tuanid=$1;  resid=$2;  url=$5;  deadline=$NF
		# 过滤掉已经下线的团购
		if (tuanid in deleteids) {
			next
		}
		tuanItem = resid "\t" url
		allTuanItem[tuanItem]
		print
	}' $updateStatusFile $updateTuanFile $allFile > $output
}


# 将每天的团购信息，添加到全局中
function merge_update_tuan() {
	for allTuanFile in $(ls tuandata/all/*.data.format); do
		updateTuanFile=${allTuanFile/all/update}
		updateStatusFile=${updateTuanFile/.data.format/.ids}

		if [ ! -f $updateTuanFile -o ! -f $updateStatusFile ]; then
			continue
		fi

		# 合并全量与增量
		merge_tuan_imp $allTuanFile $updateTuanFile $updateStatusFile $allTuanFile.addupdate

		# 备份，更新
		backFile="tuandata/history/$(basename $allTuanFile).$(todayStr)"
		rm -f $backFile;   mv $allTuanFile $backFile
		cp -f $allTuanFile.addupdate $allTuanFile

		LOG "handle $allTuanFile done." >> $Log
	done
}


# 转格式
function format_update_tuan() {
	for tuanFile in $(ls tuandata/update/*.data); do
		format $tuanFile $tuanFile.format
		LOG "format $tuanFile done." >> $Log
	done
}

# 放到output供其他使用
function put_to_output() {
	for tuanFile in $(ls tuandata/all/*.data.format); do
		onlinePath=${tuanFile/.data.format/}
		onlinePath=${onlinePath/all/output}
		rm -f $onlinePath.bak;  mv $onlinePath $onlinePath.bak
		cp $tuanFile $onlinePath
	done
}


function back_up_updatelog() {
	backupPath=tuandata/updatelog
	now=$(nowStr)
	for updateFile in $(ls tuandata/update/*.ids); do
		backupFile=$backupPath/$(basename $updateFile)
		echo -e "\n ======= $now =======" >> $backupFile
		cat $updateFile >> $backupFile
		LOG "backup $updateFile done." >> $Log
	done
}

function main() {
	# 删除历史数据
	find ./tuandata/history/ -ctime +5 | xargs rm -f {}

	# 下载当天的团购信息
	/usr/bin/python  bin/build_update_dianping_tuan.py
	LOG "get daily update dianping tuan done." >> $Log

	# 转换格式
	format_update_tuan
	LOG "format all daily dianping tuan done." >> $Log

	# 合并到全部团购信息中	
	merge_update_tuan
	LOG "merge all update dianping tuan done." >> $Log

	# 放到线上
	put_to_output
	LOG "put all dianping tuan to output path done." >> $Log
}

main
