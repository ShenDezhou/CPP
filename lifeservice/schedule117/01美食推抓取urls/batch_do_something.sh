#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-14 10:15
# * Filename	 : batch_do_something.sh
# * Description	 : 批量处理结果
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/Merger/Output
DianpingAreaConf=/fuwu/Source/conf/dianping_city_business_cook_conf

FootPath=/fuwu/DataCenter/foot
PlayTuanPath=/fuwu/DataCenter/tuan_play


# 合并团购信息
function merge_foot_tuan() {
	footTuan=$1;  playTuan=$2;
	awk -F'\t' 'ARGIND==1{
		print
	}ARGIND==2{
		if (FNR == 1 || NF < 3) { 
			next 
		}	
		source = $3
		if (source == "大众点评") {
			next
		}
		print
	}' $playTuan $footTuan > $playTuan.addfoot
	
	rm -f $playTuan.bak;  mv $playTuan $playTuan.bak
	cp $playTuan.addfoot $playTuan
}

# 将足疗的团购合并到休闲中
function batch_merge_foot_tuan() {
	for city in $(ls $FootPath/); do
		footTuanFile=$FootPath/$city/dianping_detail.tuan.table
		playTuanFile=$PlayTuanPath/$city/dianping_detail.tuan.table

		if [ -f $footTuanFile -a -f $playTuanFile ]; then
			merge_foot_tuan $footTuanFile $playTuanFile
		fi
		echo "merge $city tuan done."
	done
}

function batch_do_something() {
	
	# 将足疗的团购合并到休闲中
	batch_merge_foot_tuan

}




batch_do_something

