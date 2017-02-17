#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-14 10:15
# * Filename	 : batch_do_something.sh
# * Description	 : 批量处理结果
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/DataCenter/baseinfo_restaurant #/beijing/dianping_detail.baseinfo.table
DianpingDingWaiConf=/fuwu/Merger/conf/dianping_restaurant_hui_wai_conf

Type="restaurant"


# 批量处理店铺的订餐，外卖的标识
function update_ding_wai() {
	baseinfo=$1;
	if [ -f $baseinfo ]; then
		awk -F'\t' 'ARGIND==1 {
			if (NF < 9) { next }
			id=$1; huiFlag=$2; dingFlag=$3; waiFlag=$4; huiText=$5; huiTime=$6; closed=$NF;
			gsub(/.*\//, "", id)
			innerid = "dianping_" id
			# 买单优惠信息
			if (huiFlag == 1) {
				huiInfo = ""
				if (huiText != "") { huiInfo = huiText "@@@" }
				if (huiTime != "") { huiInfo = huiInfo "" huiTime }
				if (huiInfo == "") { huiInfo = "@@@消费后在线买单" }
				huiInfoMap[innerid] = huiInfo
			}
			# 外卖信息
			if (waiFlag == 1) {
				waiUrl = "http://m.dianping.com/waimai/mindex#!detail/i=" id
				waiInfoMap[innerid] = waiUrl
			}
			# 订座信息
			if (dingFlag == 1) {
				dingInfoMap[innerid] = dingFlag
			}
			# 店铺关闭的标识
			if (closed == 1) {
				closeInfoMap[innerid] = 1
			}
		} ARGIND==2 {
			if (FNR == 1) {
				for (row=1; row<=NF; ++row) {
					if ($row == "id") { idRow = row }
					if ($row == "downreduce") { reduceRow = row }
					if ($row == "serviceDing") { dingRow = row }
					if ($row == "serviceWai") { waiRow = row }
				}
				print; next
			}
			# 如果是已经关闭的店铺，直接过滤掉
			if ($idRow in closeInfoMap) {
				next
			}
			# 更新优惠买单, 外卖，订座
			#$reduceRow = ""; $dingRow = ""; $waiRow = "";
			if ($idRow in huiInfoMap) {
				#print "Hui\t" huiInfoMap[$idRow]
				$reduceRow = huiInfoMap[$idRow]
			}
			if ($idRow in dingInfoMap) {
				#print "Ding\t" dingInfoMap[$idRow]
				$dingRow = dingInfoMap[$idRow]
			}
			if ($idRow in waiInfoMap) {
				#print "Wai\t" dingInfoMap[$idRow]
				$waiRow = waiInfoMap[$idRow]
			}

			# 打印更新后的行
			line = $1
			for (row=2; row<=NF; ++row) {
				line = line "\t" $row
			}
			print line
		}' $DianpingDingWaiConf $baseinfo > $baseinfo.addHui
		rm -f $baseinfo.bak;  mv $baseinfo $baseinfo.bak
		cp $baseinfo.addHui $baseinfo
	fi
}



function batch_do_something() {
	for city in $(ls $RestaurantOnlinePath/); do
		baseinfoFile="$RestaurantOnlinePath/$city/dianping_detail.baseinfo.table"
		update_ding_wai $baseinfoFile
		echo "handle $city  done."
	done
}


if [ $# -gt 0 ]; then
	Type=$1
fi

batch_do_something

