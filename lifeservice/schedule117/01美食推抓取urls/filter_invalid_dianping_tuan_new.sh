#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-14 10:15
# * Filename	 : batch_do_something.sh
# * Description	 : 批量处理结果
# * *****************************************************************************/


RestaurantTuanPath=/fuwu/DataCenter/tuan_restaurant
PlayTuanPath=/fuwu/DataCenter/tuan_play

KqRestaurantPath=/fuwu/Source/Cooperation/Tuan/Input/dianping/restaurant_kq
KqPlayPath=/fuwu/Source/Cooperation/Tuan/Input/dianping/play_kq


# 过滤团购信息
function filter_dianping_tuan() {
	# 过滤掉三个月之内到期，并且不在康琪抓取的数据中的数据
	# 添加在康琪抓取中，但是不在现有数据中的部分	

	onlineTuan=$1;  kqTuan=$2;

	deadline=$(date -d "6 months" +%Y-%m-%d)

	awk -F'\t' -v DATE=$deadline 'ARGIND==1 {
		if(FNR == 1) { next }
		kqTuanDeals[$1] = $0
	} ARGIND==2 {
		if (FNR == 1) {
			print; next
		}
		source=$3;  dealid=$5;  deadline = $NF
		if (source != "大众点评") {
			print; next
		}
		gsub(/(.*\/|\?.*$)/, "", dealid)
		if (dealid in kqTuanDeals) {
			kqTuanDeals[dealid] = ""
			print; next
		}
		if (deadline > DATE) {
			print; next
		}
	} END {
		# 添加在康琪抓取中，但是不在现有数据中的部分
		for(dealid in kqTuanDeals) {
			if (kqTuanDeals[dealid] != "") {
				print kqTuanDeals[dealid]
			}
		}
	}' $kqTuan $onlineTuan > $onlineTuan.dianping


	rm -f $onlineTuan.bak;  mv $onlineTuan $onlineTuan.bak
	cp $onlineTuan.dianping $onlineTuan
}

# 过滤无效的百度糯米团购数据
function batch_filter_tuan() {
	onlineTuanPath=$1;  kqTuanPath=$2;

	for city in $(ls $onlineTuanPath/); do
		tuanFile=$onlineTuanPath/$city/dianping_detail.tuan.table
		kqTuanFile=${kqTuanPath}/$city
		
		if [ -f $tuanFile -a -f $kqTuanFile ]; then
			filter_dianping_tuan $tuanFile $kqTuanFile
		fi
		echo "filter $city invalid dianping tuan done."
	done
}

function batch_do_something() {
	batch_filter_tuan
}



Type="restaurant"
if [ $# -gt 0 ]; then
	Type=$1
fi

if [ "$Type" = "play" ]; then
	batch_do_something $PlayTuanPath $KqPlayPath
else
	batch_do_something $RestaurantTuanPath $KqRestaurantPath
fi



