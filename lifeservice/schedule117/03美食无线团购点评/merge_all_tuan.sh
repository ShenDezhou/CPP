#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-21 20:41
# * Filename	 : merge_all_tuan.sh
# * Description	 : 每天合并所有来源的团购数据，上传到线上
# * *****************************************************************************/

. ./bin/Tool.sh

Log=logs/all_tuan.log

# 拷贝大众点评的团购数据
function copy_dianping_tuan() {
	dianpingTuanPath=/fuwu/Source/Cooperation/Dianping/tuandata/output
	fileNumber=$(ls -l $dianpingTuanPath/ | wc -l)
	if [ $fileNumber -lt 300 ]; then
		LOG "$dianpingTuanPath tuan file is small" >> $Log
		exit -1
	fi
	rm -f data/dianping_tuan/*
	for tuanFile in $(ls $dianpingTuanPath/*); do
		cp $tuanFile data/dianping_tuan/$(basename $tuanFile)
	done
	LOG "copy dianping tuan file done." >> $Log
}


# 抽取特定类型的团购
function extract_dianping_tuan() {
	type=$1
	dianpingTuanPath=data/dianping_tuan	
	extractPath=Output/dianping_${type}
	baseinfoPath=/fuwu/DataCenter/baseinfo_${type}   #/beijing/dianping_detail.baseinfo.table

	for tuanFile in $(ls $dianpingTuanPath/*); do
		city=$(basename $tuanFile)

		baseinfoFile=$baseinfoPath/$city/dianping_detail.baseinfo.table
		extractFile=$extractPath/$city
		if [ -f $baseinfoFile -a -f $tuanFile ]; then
			extract_dianping_tuan_imp $baseinfoFile $tuanFile $extractFile
			echo "handle $city done."
		fi

	done

}


function extract_dianping_tuan_imp() {
	baseinfoFile=$1;  tuanFile=$2;  output=$3
	
	awk -F'\t' 'BEGIN {
		idRow = -1; residRow = -1;
	} ARGIND==1 {
		# 找到店铺id的列
		if(FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "id") {
					idRow = i
				}
			}
		} else {
			if (idRow != -1) {
				resids[$idRow] = 1
			}
		}
	} ARGIND==2 {
		if (FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "resid") {
					residRow = i
				}
			}
			print; next
		}		
		if (residRow != -1 && $residRow in resids) {
			print
		}
	}' $baseinfoFile $tuanFile > $output
}


function merge_restaurant_tuan() {
	# 大众点评，百度糯米
	dianpingTuanPath=Output/dianping_restaurant
	nuomiTuanPath=Output/nuomi_restaurant
	mergePath=Online/restaurant

	rm -f $mergePath/*

	today=$(today)
	for tuanFile in $(ls $dianpingTuanPath/*); do
		city=$(basename $tuanFile)
		cat $tuanFile $nuomiTuanPath/$city | awk -F'\t' -v TODAY=$today '{
			if (NR == 1) {
				print; next
			}
			if ($1 == "id") { next }
			if ($NF~/[0-9]+-[0-9]+/ && $NF < TODAY) {
				next
			}
			print
		}' > $mergePath/$city
	done
}


function merge_play_tuan() {
	# 大众点评，百度糯米
	dianpingTuanPath=Output/dianping_play
	nuomiTuanPath=Output/nuomi_play
	huatuojiadaoPath=Output/huatuojiadao_foot
	liangziPath=Output/liangzi_foot
	mergePath=Online/play

	rm -f $mergePath/*

	today=$(today)
	for tuanFile in $(ls $dianpingTuanPath/*); do
		city=$(basename $tuanFile)
		cat $tuanFile $nuomiTuanPath/$city $huatuojiadaoPath/$city $liangziPath/$city | awk -F'\t' -v TODAY=$today '{
			if (NR == 1) {
				print; next
			}
			if ($1 == "id") { next }
			if ($NF~/[0-9]+-[0-9]+/ && $NF < TODAY) {
				next
			}
			print
		}' > $mergePath/$city
	done
}

function put_to_online() {
	type=$1
	srcPath=Online/${type}
	destPath=/fuwu/DataCenter/tuan_${type}

	for city in $(ls $destPath/); do
		srcFile=$srcPath/$city
		destFile=$destPath/$city/dianping_detail.tuan.table
		
		if [ -f $srcFile ]; then
			rm -f $destFile.bak;  mv $destFile $destFile.bak
			cp $srcFile $destFile
		fi
	done
}

# 过滤线上的团购数据
function filter_invalid_dianping_tuan() {
	cd /fuwu/DataCenter/
		sh bin/filter_invalid_dianping_tuan.sh restaurant
		sh bin/filter_invalid_dianping_tuan.sh play
	cd -
}


# 更新大众点评的优惠/外卖信息
function update_dianping_hui_wai_info() {
	cd /fuwu/DataCenter
		 sh bin/build_update_wai_ding_datacenter.sh restaurant
		 sh bin/build_update_wai_ding_datacenter.sh play
	cd -
}



function main() {
	copy_dianping_tuan
	LOG "copy dianping tuan done." >> $Log

	rm -f Output/dianping_restaurant/*
	extract_dianping_tuan "restaurant"
	LOG "extract restaurant dianping tuan done." >> $Log

	rm -f Output/dianping_play/*
	extract_dianping_tuan "play"
	LOG "extract play dianping tuan done." >> $Log

	merge_restaurant_tuan
	LOG "merge restaurant dianping tuan done." >> $Log

	merge_play_tuan
	LOG "merge play dianping tuan done." >> $Log

	put_to_online restaurant
	LOG "dispatch restaurant dianping tuan done." >> $Log

	put_to_online play
	LOG "dispatch play dianping tuan done." >> $Log

	filter_invalid_dianping_tuan
	LOG "filter invalid dianping tuan done." >> $Log
	
	update_dianping_hui_wai_info
	LOG "update dianping hui/wai info done." >> $Log
	
}

main
