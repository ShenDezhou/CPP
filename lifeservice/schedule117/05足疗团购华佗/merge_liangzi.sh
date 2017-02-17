#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-21 12:02
# * Filename	 : build_nuomi_dianping_merge_hadoop.sh
# * Description	 : 利用hadoop计算店铺的合并
# * *****************************************************************************/

. ./bin/Tool.sh

LOG "begin to merge"

LiangziShopConf=conf/liangzi_conf


function create_foot_service_imp() {
	FoodServices=$1;  output=$2;
	rm -f $output.bak;  mv $output $output.bak
	# 生成足疗服务的数据
	awk -F'\t' 'BEGIN {
		idx = 30000000; site="脉度良子"; type="预"
	} ARGIND==1 {
		if (NF < 4) {
			next
		}
		city=$1; title=$3; destid=$4;
		idMap[city "\t" title] = destid
	} ARGIND==2 {
		if ($1 == "Shop") {
			city=$2;  title=$5;  dianpingid="";
			if (city "\t" title in idMap) {
				dianpingid = idMap[city "\t" title]
			}
		} else if($1 == "Service") {
			if (dianpingid == "") {
				next
			}
			title=$2; url=$3; img=$4; value=$5; price=$6; min=$8; mode=$9
			if (img !~ /http/) { img = "" }
			if (url == "") { next }
			gsub("分钟", "", min)
			serviceItem = dianpingid "\t" site "\t" type "\t" url "\t" title "\t" img "\t" price "\t" value "\t" min "\t" mode
			if (!(serviceItem in existServiceItems)) {
				existServiceItems[serviceItem]
				print ++idx "\t" serviceItem
			}
		}
	}' $LiangziShopConf $FoodServices > $output

	LOG "create liangzi foot service done."
}

# 每个城市的足疗服务
function create_foot_service() {
	serviceResultPrefix=data/huatuojiadao_service
	for mergeFile in $(ls data/merge_shops.*); do
		city=${mergeFile/*./}
		output=$serviceResultPrefix.$city
		create_foot_service_imp $mergeFile $output
		echo "create foot service for $city done."
	done
	LOG "create all huatuojiadao services done."

}


# 将数据写入到线上
function dispatch() {
	footTuanPath=/fuwu/DataCenter/foot
	for serviceFile in $(ls data/liangzi_service.*); do
		city=${serviceFile/.bak/}
		if [ "$city" != "$serviceFile" ]; then
			continue
		fi
		# 检查线上团购数据是否存在
		city=${city/*./}
		tuanDestFile=$footTuanPath/$city/dianping_detail.tuan.table
		if [ ! -f $tuanDestFile ]; then
			echo "$tuanDestFile file is not exist"
			continue
		fi
		# 检查华夏良子数据是否为空
		lines=$(cat $serviceFile | wc -l)
		if [ $lines -gt 0 ]; then
			rm -f $tuanDestFile.bak3;  mv $tuanDestFile $tuanDestFile.bak3
			awk -F'\t' '{
				if ($3=="脉度良子") { next }
				print
			}' $tuanDestFile.bak3 > $tuanDestFile
			cat $serviceFile >> $tuanDestFile
		fi
		LOG "handle $serviceFile done."
	done
	LOG "dispatch liangzi services done."
}


# 将数据写入团购数据中心
function dispatch_() {
	footDestPath=/search/zhangk/Fuwu/Source/Cooperation/Tuan/Output/liangzi_foot
	for serviceFile in $(ls data/liangzi_service.*); do
		city=${serviceFile/.bak/}
		if [ "$city" != "$serviceFile" ]; then
			continue
		fi
		# 检查线上团购数据是否存在
		city=${city/*./}

		footDestFile=$footDestPath/$city

		# 检查华佗驾到数据是否为空
		lines=$(cat $serviceFile | wc -l)
		if [ $lines -gt 0 ]; then
			if [ -f $footDestFile ]; then
				rm -f $footDestFile.bak;  mv $footDestFile $footDestFile.bak
			fi
			cp $serviceFile $footDestFile
		fi
	done
}

# 下载华夏良子的数据
function get_liangzi_services() {
	output=data/liangzi_all_services 
	rm -f $output.bak;  mv $output $output.bak
	
	xmlFile=tmp/liangzi.xml
	getServiceAPI="http://api.maidu360.com/getStoreAndProducts"
	wget $getServiceAPI -O $xmlFile
	

	/usr/bin/python bin/get_liangzi.py $xmlFile > $output
	LOG "get liangzi services done."
}


function main() {
	# 请求华佗驾到的数据
	get_liangzi_services

	# 现在只有北京的数据，如果后面还有其他城市数据，则需要修改
	create_foot_service_imp data/liangzi_all_services data/liangzi_service.beijing

	# 合并到线上
	dispatch

	#
	dispatch_
}

main
