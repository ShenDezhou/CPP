#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-04 10:15
# * Filename	 : get_dianping_urls.sh
# * Description	 : 获取美食/休闲的url列表，用于离线扫库更新
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/Merger/Output  #beijing/restaurant/dianping_detail.baseinfo.table

function get_shop_urls() {
	local Type="restaurant"
	if [ $# -gt 0 ]; then
		Type=$1
	fi


	for city in $(ls $RestaurantOnlinePath/); do
		#city="beijing"
		baseinfoFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.baseinfo.table"
		urlFile="$RestaurantOnlinePath/$city/$Type/dianping_${Type}_${city}.urls"
		if [ ! -f $baseinfoFile ]; then
			echo "$baseinfoFile is not exist!"
			continue
		fi

		awk -F'\t' '{
			if (FNR == 1) {
				for(idx=1; idx<=NF; ++idx) {
					if($idx == "url") { urlRow = idx }
				}
				next
			}
			if (!($urlRow in existUrls)) {
				print $urlRow
				existUrls[$urlRow]
			}
		}' $baseinfoFile > $urlFile

		echo "get $city's $Type urls done."
	done
}


if [ $# -gt 0 ]; then
	Type=$1
fi
get_shop_urls $Type
