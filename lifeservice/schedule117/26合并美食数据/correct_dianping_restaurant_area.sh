#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-04 10:15
# * Filename	 : correct_dianping_restaurant_area.sh
# * Description	 : 对错误的区域进行纠错
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/Merger/Output  #beijing/restaurant/dianping_detail.baseinfo.table
DianpingAreaConf=/fuwu/Source/conf/dianping_city_business_cook_conf
# business        北京    朝阳区  /search/category/2/10/r14       /search/category/2/10/r23013    十里河
DianpingCityEnChConf=/fuwu/Source/conf/dianping_city_pinyin_conf
# 山西    古交市  gujiao

function correct_restaurant_area() {
	local Type="restaurant"
	if [ $# -gt 0 ]; then
		Type=$1
	fi


	for city in $(ls $RestaurantOnlinePath/); do
		#city="beijing"
		baseinfoFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.baseinfo.table"
		if [ ! -f $baseinfoFile -o ! -f $DianpingAreaConf ]; then
			echo "$baseinfoFile or $DianpingAreaConf is not exist!"
			continue
		fi
		echo "begin to correct $baseinfoFile ..."
		awk -F'\t' -v CITYEN=$city 'ARGIND == 1 {
			if (NF != 6) {
				next
			}
			# 加载城市-区域的映射表
			if($1 == "business") {
				city=$2;  area=$3;  district=$6;
				if (district!~/(其他|其它)/) {
					areaMap[city "" district] = area
				}
			}
		} ARGIND == 2 {
			# 加载城市的拼音和文字的映射
			if (NF != 3) {
				next
			}
			cityEnChMap[$3] = $2
		} ARGIND == 3 {
			if (FNR == 1) {
				for(idx=1; idx<=NF; ++idx) {
					if($idx == "city") { cityRow = idx }
					if($idx == "area") { areaRow = idx }
					if($idx == "address") { addressRow = idx }
					if($idx == "district") { districtRow = idx }
				}
				print;  next
			}
			# 过滤掉有错误的行，即包含###的
			if ($0~/###/) {
				next
			}
			# 如果city不为空，并且不是应该对应的城市，则过滤掉
			if ($cityRow != "" && (CITYEN in cityEnChMap) && $cityRow != cityEnChMap[CITYEN]) {
				print > "'$baseinfoFile.error'"
				next
			}

			# 对area纠错
			city=$cityRow;  area=$areaRow;  address=$addressRow;  district=$districtRow
			key = city "" district
			if ((key in areaMap) && areaMap[key] != area) {
				# 如果地址里包含area,说明区对
				if (index(addr, area) != 0) {
					$areaRow = ""
				} else {
					# 区不对
					$districtRow = ""
				}
			}
			# 输出纠错后的行
			line = $1
			for (idx=2; idx<=NF; ++idx) {
				line = line "\t" $idx
			}
			print line
		}' $DianpingAreaConf $DianpingCityEnChConf $baseinfoFile > $baseinfoFile.correct

		if [ ! -f $baseinfoFile.uncorrect ]; then
			mv -f $baseinfoFile $baseinfoFile.uncorrect
		fi
		rm -f $baseinfoFile;  cp $baseinfoFile.correct $baseinfoFile

		echo "correct for $baseinfoFile done."
	done
}


if [ $# -gt 0 ]; then
	Type=$1
fi
correct_restaurant_area $Type
