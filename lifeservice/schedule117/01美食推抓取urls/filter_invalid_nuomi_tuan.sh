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


# 过滤团购信息
function filter_tuan() {
	conf=conf/nuomi_invalid_tuan
	
	input=$1;  output=$2;
	awk -F'\t' 'ARGIND==1{
		invalidids[$1]
	} ARGIND==2 {
		if (FNR == 1) { 
			print; next 
		}
		
		dealUrl = $5
		if (dealUrl in invalidids) {
			next
		}
		print
	}' $conf $input > $output
	
	rm -f $input.bak;  mv $input $input.bak
	cp $output $input
}

# 过滤无效的百度糯米团购数据
function batch_filter_tuan() {
	for city in $(ls $RestaurantTuanPath/); do
		tuanFile=$RestaurantTuanPath/$city/dianping_detail.tuan.table
		filterFile=$tuanFile.filter
		
		filter_tuan $tuanFile $filterFile

		echo "filter $city invalid nuomi tuan done."
	done
}

function batch_do_something() {
	batch_filter_tuan
}




batch_do_something


