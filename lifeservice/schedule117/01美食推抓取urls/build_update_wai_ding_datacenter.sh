#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-14 10:15
# * Filename	 : bin/build_update_wai_ding_datacenter.sh
# * Description	 : 更新外卖和订餐的信息
# * *****************************************************************************/

Type="restaurant"

if [ $# -gt 0 ]; then
	Type=$1
fi

BaseinfoOnlinePath=/fuwu/DataCenter/baseinfo_${Type}
#DianpingDingWaiConf=/fuwu/Merger/conf/dianping_${Type}_hui_wai_conf
DianpingDingWaiConf=/fuwu/DataCenter/conf/dianping_${Type}_hui_wai_conf



# 批量处理店铺的订餐，外卖的标识
function update_ding_wai() {
	baseinfo=$1;  dingConf=$2;
	if [ ! -f $baseinfo -o ! -f $dingConf ]; then
		echo "$baseinfo or $dianConf is not exist!"
		return
	fi


	awk -F'\t' 'ARGIND==1 {
		if (NF==6 && $2=="baseinfo") {
			id=$1; photo=$3; avgPrice=$4; score=$5; businesstime=$6;
			gsub(/.*\//, "", id)
			innerid = "dianping_" id
			# 一些基本信息
			if (photo != "" && photo~/^http/) { photos[innerid] = photo }
			if (avgPrice != "") { avgPrices[innerid] = avgPrice }
			if (score != "") { scores[innerid] = score }
			if (businesstime != "") { 
				gsub(/营业时间：/, "", businesstime)
				businesstimes[innerid] = businesstime
			}
		} else {
			if (NF < 9) { next }
			id=$1; huiFlag=$2; dingFlag=$3; waiFlag=$4; huiText=$5; huiTime=$6; closed=$NF;
			gsub(/.*\//, "", id)
			innerid = "dianping_" id
			# 买单优惠信息
			if (huiFlag == 1) {
				huiInfo = ""
				if (huiText != "") { huiInfo = huiText "@@@" }
				if (huiTime != "") { huiInfo = huiInfo "(" huiTime ")"}
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
		}
	} ARGIND==2 {
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "id") { idRow = row }
				if ($row == "downreduce") { reduceRow = row }
				if ($row == "serviceDing") { dingRow = row }
				if ($row == "serviceWai") { waiRow = row }

				if ($row == "photo") { photoRow = row }
				if ($row == "avgPrice") { priceRow = row }
				if ($row == "score") { scoreRow = row }
				if ($row == "businessDate") { dateRow = row }
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
			$reduceRow = huiInfoMap[$idRow]
		}
		if ($idRow in dingInfoMap) {
			$dingRow = dingInfoMap[$idRow]
		}
		if ($idRow in waiInfoMap) {
			$waiRow = waiInfoMap[$idRow]
		}

		if ($idRow in photos) { 
			$photoRow = photos[$idRow]
		}
		if ($idRow in avgPrices) { 
			$priceRow = avgPrices[$idRow]
		}
		if ($idRow in scores) { 
			$scoreRow = scores[$idRow]
		}
		if ($idRow in businesstimes) { 
			$dateRow = businesstimes[$idRow]
		}

		# 打印更新后的行
		line = $1
		for (row=2; row<=NF; ++row) {
			line = line "\t" $row
		}
		print line
	}' $dingConf $baseinfo > $baseinfo.addHui
	rm -f $baseinfo.bak;  mv $baseinfo $baseinfo.bak
	cp $baseinfo.addHui $baseinfo
}



function batch_do_something() {
	for city in $(ls $BaseinfoOnlinePath/); do
		#city="zunyi"
		baseinfoFile="$BaseinfoOnlinePath/$city/dianping_detail.baseinfo.table"
		update_ding_wai $baseinfoFile $DianpingDingWaiConf
		echo "handle $city  done."
	done
}



batch_do_something

