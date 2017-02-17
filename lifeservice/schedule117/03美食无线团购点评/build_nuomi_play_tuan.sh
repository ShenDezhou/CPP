#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-21 12:02
# * Filename	 : bin/build_nuomi_play_tuan.sh
# * Description	 : 利用hadoop计算店铺的合并
# * *****************************************************************************/

. ./bin/Tool.sh

Log=logs/nuomi_play_tuan.log


NuomiShopPath=/fuwu/Source/Cooperation/Tuan/Input/nuomi
DianpingShopPath=/fuwu/DataCenter/baseinfo_play
NeedMergeShop=/fuwu/Source/Cooperation/Tuan/tmp/nuomi_dianping_play_shops
MergeHadoopPath=/user/web_tupu/serviceapp/nuomi/merge/play_output
ShopMerge=/fuwu/Source/Cooperation/Tuan/tmp/nuomi_dianping_play_shop_merge
NuomiTuanOnlinePath=/fuwu/Source/Cooperation/Tuan/data/nuomi_play
NuomiTuanOutputPath=/fuwu/Source/Cooperation/Tuan/Output/nuomi_play



# 抽取大众点评的一个城市的shop信息 <id  source  title  poi  tel  city  addr>
function extract_dianping_city_shops() {
	input=$1;
	awk -F'\t' 'BEGIN {
		idRow=-1; titleRow=-1; poiRow=-1; cityRow=-1; addrRow=-1; telRow=-1;
		source = "Dianping"
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				#print "[" $row "]"
				if ($row == "id"){ idRow = row;}
				else if ($row == "tel") { telRow = row;}
				else if ($row == "poi") { poiRow = row;}
				else if ($row == "city") { cityRow = row;}
				else if ($row == "title") { titleRow = row;}
				else if ($row == "address") { addrRow = row;}
			}
			next
		}
		if (NF < 10 ) {
			next
		}
		#print idRow "\t" titleRow "\t" poiRow "\t" cityRow "\t" addrRow "\t" telRow
		id=$idRow; title=$titleRow; poi=$poiRow; city=$cityRow; addr=$addrRow; tel=$telRow; 
		shopInfo = city "\t" source "\t" id "\t" title "\t" poi "\t" tel "\t" addr
		if (!(id in existids)) {
			existids[id]
			print shopInfo
		}
	}' $input
	LOG "extract dianping play shops done." >> $Log
}



# 抽取糯米网一个城市的shop信息 <id  source  title  poi  tel  city  addr>
function extract_nuomi_city_shops() {
	input=$1;
	awk -F'\t' 'BEGIN {
		titleRow=-1; poiRow=-1; addrRow=-1; telRow=-1; cityRow=-1; categoryRow=-1;
		source = "Nuomi"
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "name") { titleRow = row;}
				else if ($row == "poi") { poiRow = row;}
				else if ($row == "tel") { telRow = row;}
				else if ($row == "city") { cityRow = row;}
				else if ($row == "address") { addrRow = row;}
				else if ($row == "firstCategory") { categoryRow = row; }
			}
			next
		}
		# @@@@@@@@ 需要修改的地方 @@@@@@@@
		# 去除美食类数据
		if (NF < 8 || $categoryRow == "美食") {
			next
		}

		title=$titleRow; poi=$poiRow; addr=$addrRow; tel=$telRow; city=$cityRow;
		id = title "@@@" poi
		shopInfo = city "\t" source "\t" id "\t" title "\t" poi "\t" tel "\t" addr
		if (!(id in existids)) {
			existids[id]
			print shopInfo
		}
	}' $input
	LOG "extract nuomi shops info done." >> $Log
}


# 构建
function create_nuomi_tuan_info() {
	shopidMergeConf=$1;  tuanFile=$2;  shopFile=$3
	today=$(today)
	awk -F'\t' -v TODAY=$today 'BEGIN {
		print "id\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
		# @@@@@@@@ 需要修改的地方 @@@@@@@@
		lineid = 20000000
	} ARGIND == 1 {
		# 加载两个来源的店铺id映射
		if (NF != 5) { next; }
		shopid = $3;  mergeid = $5
		if (mergeid ~ "dianping") {
			shopidMap[shopid] = mergeid
		}
	} ARGIND == 2 {
		if (FNR == 1) {
			next
		} else {
			
			# 一个团购项的基本信息
			tuanid = $1;  tuaninfo = $2 "\t团"; deadline=$NF
			if (deadline < TODAY) {
				next
			}
			for (idx=3; idx<=8; ++idx) {
				tuaninfo = tuaninfo "\t" $idx
			}
			tuaninfo = tuaninfo "\t" deadline
			tuanInfoMap[tuanid] = tuaninfo
		}
	} ARGIND == 3 {
		if (FNR == 1) {
			next
		} else {
			# 店铺信息
			title = $1;  poi = $6;  tuanid = $NF
			shopid = title "@@@" poi
			if (!(shopid in shopidMap)) {
				next
			}
			if (!(tuanid in tuanInfoMap)) {
				next
			}
			print ++lineid "\t" shopidMap[shopid] "\t" tuanInfoMap[tuanid]
		}
	}' $shopidMergeConf $tuanFile $shopFile
	LOG "handle $tuanFile done"
}


# 合并两个团购文件
function merge_tuaninfo() {
	masterTuanFile=$1;  slaveTuanFile=$2
	awk -F'\t' 'ARGIND==1 {
		if ($3=="百度糯米") { next }
		print;  tuanUrls[$2"\t"$5]
	} ARGIND==2 {
		if (!($2"\t"$5 in tuanUrls)) {
			print;  tuanUrls[$2"\t"$5]
		}
	}' $masterTuanFile $slaveTuanFile > $masterTuanFile.mergenuomi

	rm -f $masterTuanFile.bak;  mv $masterTuanFile $masterTuanFile.bak
	mv $masterTuanFile.mergenuomi $masterTuanFile
	LOG "merge $masterTuanFile and $slaveTuanFile done."
}



# 抽取需要合并的店铺的信息
function extract_need_merge_shops() {
	LOG "begin to extract shops ..." >> $Log
	rm -f $NeedMergeShop

	for nuomiShop in $(ls $NuomiShopPath/*shop); do
		city=${nuomiShop##*/}
		city=${city/_shop/}
		
		dianpingShopFile=$DianpingShopPath/$city/dianping_detail.baseinfo.table
		nuomiShopFile=$nuomiShop
		
		if [ ! -f $dianpingShopFile ]; then
			LOG "$city has not $dianpingShopFile" >> $Log
			continue
		fi

		# used to test
		#if [ "$city" != "zunyi" -a "$city" != "zhoushan" ]; then
		#	continue
		#fi

		extract_dianping_city_shops $dianpingShopFile >> $NeedMergeShop
		extract_nuomi_city_shops $nuomiShopFile >> $NeedMergeShop

		LOG "extract $city shops info done." >> $Log
	done


	LOG "extract shops done." >> $Log
}

# 合并店铺数据
function merge_shops_on_hadoop() {
	LOG "begin to merge shops on hadoop..." >> $Log
	
	# 在hadoop集群上执行合并
	input=$NeedMergeShop;
	sh bin/build_shop_merge_on_hadoop.sh $input $MergeHadoopPath

	# 合并结果
	hadoop dfs -cat $MergeHadoopPath/* > $ShopMerge
	LOG "merge shops on hadoop done. [$ShopMerge]" >> $Log
}


# 根据合并的店铺数据，生成团购数据
function create_tuan_items() {
	for nuomiShop in $(ls $NuomiShopPath/*shop); do
		LOG "begin to create nuomi tuan info for $city..." >> $Log

		city=${nuomiShop##*/}
		city=${city/_shop/}
		
		nuomiShopFile=$nuomiShop
		nuomiTuanFile=${nuomiShop/shop/tuan}	
		
		#if [ "$city" != "beijing" ]; then
		#	continue
		#fi
		create_nuomi_tuan_info $ShopMerge $nuomiTuanFile $nuomiShopFile > $NuomiTuanOnlinePath/$city

		LOG "create nuomi tuan info for $city done." >> $Log		
	done
	
	LOG "create all nuomi tuan done." >> $Log		
}

function put_to_output() {
	rm -f ./History/nuomi_play/*
	mv $NuomiTuanOutputPath/* ./History/nuomi_play/
	mv $NuomiTuanOnlinePath/* $NuomiTuanOutputPath/
	LOG "put nuomi restaurant data to output path done." >> $Log
}

function main() {
	extract_need_merge_shops

	merge_shops_on_hadoop
	
	create_tuan_items
	
	put_to_output
}


main
