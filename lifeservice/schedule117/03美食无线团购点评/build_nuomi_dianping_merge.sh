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
# 抽取大众点评的一个城市的shop信息 <id  source  title  poi  tel  city  addr>
function extract_dianping_city_shops() {
	input=$1;  #output=$2;
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
	}' $input #> $output
	LOG "extract dianping restaurant info of [$input] done."
}



# 抽取糯米网一个城市的shop信息 <id  source  title  poi  tel  city  addr>
function extract_nuomi_city_shops() {
	input=$1;  #output=$2;
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
		if (NF < 8 || $categoryRow != "美食") {
			next
		}

		title=$titleRow; poi=$poiRow; addr=$addrRow; tel=$telRow; city=$cityRow;
		id = title "@@@" poi
		shopInfo = city "\t" source "\t" id "\t" title "\t" poi "\t" tel "\t" addr
		if (!(id in existids)) {
			existids[id]
			print shopInfo
		}
	}' $input #> $output
	LOG "extract nuomi restaurant info of [$input] done."
}


# 构建
function create_nuomi_tuan_info() {
	shopidMergeConf=$1;  tuanFile=$2;  shopFile=$3
	today=$(today)
	awk -F'\t' -v TODAY=$today 'BEGIN {
		print "id\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
		lineid = 10000000
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



NuomiShopPath=/fuwu/Source/Cooperation/Nuomi/data
DianpingShopPath=/fuwu/Merger/Output
NeedMergeShop=/fuwu/Source/Cooperation/data/nuomi_dianping_shops

rm -f $NeedMergeShop

for nuomiShop in $(ls $NuomiShopPath/*shop); do
	city=${nuomiShop##*/}
	city=${city/_shop/}
	
	LOG "handle $city" 
	dianpingShopFile=/fuwu/Merger/Output/$city/restaurant/dianping_detail.baseinfo.table
	nuomiShopFile=$nuomiShop
	
	if [ ! -f $dianpingShopFile ]; then
		LOG "$city has not $dianpingShopFile" 
		continue
	fi

	# used to test
	#if [ "$city" != "zunyi" -a "$city" != "zhoushan" ]; then
	#	continue
	#fi

	extract_dianping_city_shops $dianpingShopFile >> $NeedMergeShop
	extract_nuomi_city_shops $nuomiShopFile >> $NeedMergeShop
done

echo "extract shops done. [$NeedMergeShop]"


LOG "begin to merge nuomi tuan info on hadoop."
ShopMerge=Merge/nuomi_dianping_shop_merge
# 放到hadoop集群上执行合并
sh -x bin/build_nuomi_dianping_merge_on_hadoop.sh
hadoop dfs -cat /user/web_tupu/serviceapp/nuomi/merge/output/* > $ShopMerge
LOG "merge nuomi tuan info on hadoop done. [$ShopMerge]"

NuomiTuanOnlinePath=/fuwu/Source/Cooperation/Nuomi/tuan
for nuomiShop in $(ls $NuomiShopPath/*shop); do
	city=${nuomiShop##*/}
	city=${city/_shop/}
	
	LOG "create nuomi online tuan info for $city" 
	dianpingShopFile=/fuwu/Merger/Output/$city/restaurant/dianping_detail.baseinfo.table
	dianpingTuanFile=/fuwu/Merger/Output/$city/restaurant/dianping_detail.tuan.table
	nuomiShopFile=$nuomiShop
	nuomiTuanFile=${nuomiShop/shop/tuan}	

	if [ ! -f $dianpingShopFile ]; then
		LOG "$city has not $dianpingShopFile" 
		continue
	fi
	
	#if [ "$city" != "beijing" ]; then
	#	continue
	#fi
	create_nuomi_tuan_info $ShopMerge $nuomiTuanFile $nuomiShopFile > $NuomiTuanOnlinePath/$city

	if [ -f $dianpingTuanFile ]; then
		merge_tuaninfo $dianpingTuanFile $NuomiTuanOnlinePath/$city		
	fi
done


LOG "merge all dianping & nuomi tuan done"
