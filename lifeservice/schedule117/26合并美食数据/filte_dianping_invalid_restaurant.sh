#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-30 15:09
# * Filename	 : filte_dianping_invalid_restaurant.sh
# * Description	 : 去除无效的点评的数据/脏数据
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/Merger/Output
Type=restaurant


# 1、过滤最低消费过万的
# 2、过滤 评分为0 && 没有价格 && 推荐为空的


#Output/beijing/restaurant/dianping_detail.baseinfo.table

# avgPrice  空  数字  -
# score 
# id

#Output/beijing/restaurant/dianping_detail.recomfood.table
#recomfoodid     resid   resUrl


function filte_invalid_restaurant() {
	local baseinfoFile=$1;  local recomFoodFile=$2

	awk -F'\t' 'ARGIND==1 {
		# 有推荐菜的店铺id
		if (FNR == 1) {
			for(idx=1; idx<=NF; ++idx) {
				if($idx == "resid") { residRow = idx }
			}
		} else {
			hasRecomRestids[$residRow]
		}
	} ARGIND==2 {
		# 根据过滤条件，过滤无效的数据
		if (FNR == 1) {
			for(idx=1; idx<=NF; ++idx) {
				if($idx == "id") { idRow = idx }
				if($idx == "score") { scoreRow = idx }
				if($idx == "avgPrice") { avgPriceRow = idx }
			}
			print;  next
		}
		# 过滤策略1：平均消费值过高的
		if ($avgPriceRow != "-" && $avgPriceRow > 2000) {
			print > "'$baseinfoFile.invalid'"
			next
		}

		# 过滤策略2： 评分为0 && 没有价格 && 推荐为空的
		if ($scoreRow==0 && ($avgPriceRow=="" || $avgPriceRow=="-") && !($idRow in hasRecomRestids)) {
			print > "'$baseinfoFile.invalid'"
			next
		}
		print
	}' $recomFoodFile $baseinfoFile > $baseinfoFile.valid
	echo "filter invalid shops for $baseinfoFile done."

}




function filte_by_score() {
	#for city in $(ls $RestaurantOnlinePath/); do
		city="lasa"
		baseinfoFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.baseinfo.table"
		recomFoodFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.recomfood.table"
		if [ ! -f $baseinfoFile -o ! -f $recomFoodFile ]; then
			echo "$baseinfoFile or $recomFoodFile is not exist!"
			continue
		fi
		echo "begin to correct $baseinfoFile ..."
		filte_invalid_restaurant $baseinfoFile $recomFoodFile
	#done
}



function filte_by_invalid_ids() {
	#for city in $(ls $RestaurantOnlinePath/); do
		city="beijing"
		baseinfoFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.baseinfo.table"
		invalidIDFile="$RestaurantOnlinePath/$city/$Type/dianping_invalid_ids"
		if [ ! -f $baseinfoFile -o ! -f $invalidIDFile ]; then
			echo "$baseinfoFile or $invalidIDFile is not exist!"
			continue
		fi
		echo "begin to filte invalid ids for $baseinfoFile ..."
		
		awk -F'\t' 'ARGIND==1 {
			if ($1 != "") {
				filteids["dianping_"$1]
			}
		} ARGIND==2 {
			if (FNR==1) {
				for(row=1; row<=NF; ++row) {
					if ($row=="id") { idRow = row }
				}
				print; next
			}
			if ($idRow in filteids) {
				next
			}
			print
		}' $invalidIDFile $baseinfoFile > $baseinfoFile.filte
		echo "filter invalid ids for $city done."
	#done
}

filte_by_invalid_ids




