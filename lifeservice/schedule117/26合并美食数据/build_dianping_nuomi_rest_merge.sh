#!/bin/bash
#coding=gb2312

# 合并大众点评的美食商铺与糯米团购的商铺
# 注意！！！！ 不同商家对于城市下区的名称的归一，需要处理
#			   否则无法应用到合并策略里的 区域相同 这个重要条件


. ./bin/Tool.sh

DIANPING_DIR="Input"
NUOMI_TUAN_DIR="/search/zhangk/Fuwu/Source/Cooperation/Nuomi/data/"


# 大众点评网的美食店铺数据
function extract_dianping_restaurant() {
	input=$1; output=$2;
	awk -F'\t' 'BEGIN {
		idRow=-1; urlRow=-1; titleRow=-1; poiRow=-1;
		addrRow=-1; telRow=-1; areaRow=-1;
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "id"){ idRow = row;}
				else if ($row == "url") { urlRow= row;}
				else if ($row == "title") { titleRow = row;}
				else if ($row == "poi") { poiRow = row;}
				else if ($row == "address") { addrRow = row;}
				else if ($row == "tel") { telRow = row;}
				else if ($row == "area") { areaRow = row;}
			}
			next
		}
		if (NF < 10 ) {
			next
		}
		id=$idRow; url=$urlRow; title=$titleRow; 
		poi=$poiRow; addr=$addrRow; tel=$telRow; area=$areaRow; 
		# 归一化 title  tel addr
		gsub(/\([^\)]+\)/, "", title);
		gsub(/（[^）]+）/, "", title);

		gsub("-", "", tel);
		gsub(",", "|", tel);
		
		# 如果area 为空
		if (area == "") {
			addrLen = split(addr, addrList, " ")
			if (addrLen > 0 && addrList[1]~/.*区$/ && length(addrList[1]) <= 5) {
				area = addrList[1]
			}
		}

		gsub(area, "", addr)
		gsub(/\([^\)]+\)/, "", addr);
		gsub(/（[^）]+）/, "", addr);

		print id"\t"url"\t"title"\t"tel"\t"area"\t"addr"\t"poi
	}' $input > $output
	LOG "extract dianping restaurant info of [$input] done. [$output]"
}



# 糯米团购涉及到的店铺信息
function extract_nuomi_restaurant() {
	input=$1;  output=$2
	# 只取美食类 && 区域不能为空
	awk -F'\t' 'BEGIN {
		titleRow=-1; telRow=-1; cityRow=-1; areaRow=-1; 
		addrRow=-1; poiRow=-1; categoryRow=-1; 
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "name"){ titleRow = row; continue }
				if ($row == "tel"){ telRow = row; continue }
				if ($row == "city"){ cityRow = row; continue }
				if ($row == "area"){ areaRow = row; continue }
				if ($row == "address"){ addrRow = row; continue }
				if ($row == "poi"){ poiRow = row; continue }
				if ($row == "firstCategory"){ categoryRow = row; continue }
			}
			next
		}
		# 过滤非餐馆类的团购
		if ($categoryRow != "美食" || $areaRow == "") {
			next
		}

		title=$titleRow;  tel=$telRow; area=$areaRow; 
		addr=$addrRow;  poi=$poiRow;

		id = area "@" title "@" poi;  url = "http://nuomi.com/"id
		# 归一化title addr
		gsub(/\([^\)]+\)/, "", title);
		gsub(/（[^）]+）/, "", title);
		
		# 归一化地址, 去除 **市 以及括号里面的东西
		gsub(/^[^市]+市/, "", addr)
		gsub(area, "", addr)
		gsub(/\([^\)]+\)/, "", addr);
		gsub(/（[^）]+）/, "", addr);

		print id"\t"url"\t"title"\t"tel"\t"area"\t"addr"\t"poi
	}' $input > $output
	LOG "extract nuomi restaurant info of [$input] done. [$output]"

}


#37592   http://www.dianping.com/shop/22510919   101西餐厅   01084031188 东城区  东城区 北京东城交道口东大街101号    39.94111,116.4117

function merge_restaurant() {
	nuomiData=$1;  dianpingData=$2; mergeData=$3;

	#uniq nuomi_data
	sort -t$'\t' -k1,1 $nuomiData | uniq > $nuomiData.uniq
	cat $nuomiData.uniq $dianpingData > $mergeData

	# 区域相同，名称相同，电话,地址,poi相近或相似
	# 打印表头
	echo -e "id\turl\ttitle\ttel\tarea\taddr\tpoi" > $mergeData.title.sort
	sort -t$'\t' -k5,5 -k3,3  $mergeData >> $mergeData.title.sort

	# 区域相同，电话相同  (名称相似)
	echo -e "id\turl\ttitle\ttel\tarea\taddr\tpoi" > $mergeData.tel.sort
	awk -F'\t' '{
		tel = $4;
		len = split(tel, telArr, "|")
		# 将电话分割成每个号码一行
		for (i=1; i<=len; ++i) {
			$4 = telArr[i]
			line = $1
			for(j=2; j<=NF; j++) {
				line = line "\t" $j
			}
			print line
		}
	}' $mergeData | sort -t$'\t' -k5,5 -k4,4 >> $mergeData.tel.sort
	LOG "merge for [$dianpingData] and [$nuomiData] done. output=[$mergeData]"
}


function filte_invalid_equal() {
	equalFile=$1;
	awk -F'\t' 'ARGIND==1{
		nuomiID=$2;  dianpingID=$3;
		nuomiIDNum[nuomiID]++
	} ARGIND ==2 {
		nuomiID=$2;  dianpingID=$3;
		if (nuomiIDNum[nuomiID] > 1) {
			next
		}
		print nuomiID"\t"dianpingID
	}' $equalFile $equalFile
	LOG "filte invalid equal items for [$equalFile] done."
}


# 第一版本，先把有重复的合并去除
# 第二版： 选择距离最近的 或者地址最接近的
function merge_tuan() {
	equalFile=$1;  dpTuan=$2; nmTuan=$3; nmShop=$4;
	# equalFile <nmID  dpID>
	# dpTuan <tuanid  resid  site  type    url     title   photo   price   value   sell>
	# nmTuan <id,url,businessTitle,title,value,price,bought,site,image,city,firstCategory,secondCategory,startTime,endTime>
	# nmShop <name, tel, area, address, poi, firstCategory, tuanid>

	awk -F'\t' 'BEGIN {
		# 初始化，如果需要
		dpMaxTuanID = 0;
	} ARGIND == 1 {
		# 糯米与点评的 shop id 映射
		nmID=$1;  dpID=$2;
		mergeIDMap[nmID] = dpID
	} ARGIND == 2 {
		# 糯米网的团购信息
		type = "团"
		id=$1; url=$2; title=$4; value=$5; price=$6; bought=$7; site=$8; image=$9;
		nmTuanMap[id] = site "\t" type "\t" url "\t" title "\t" image "\t" price "\t" value "\t" bought
	} ARGIND == 3 {
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "name") { titleRow = row; continue}
				if ($row == "area") { areaRow = row; continue}
				if ($row == "poi") { poiRow = row; continue}
				if ($row == "tuanid") { tuanidRow = row; continue}
			}
			next
		}

		# 糯米shop id与tuan id的映射
		title=$titleRow;  area=$areaRow;  poi=$poiRow;  tuanID=$tuanidRow;
		# 糯米shop id与点评tuan id的映射
		#title=$1;  area=$3;  poi=$5;  tuanID=$NF;
		shopID = area "@" title "@" poi;
		if (!(shopID in nmShopTuanIDMap)) {
			nmShopTuanIDMap[shopID] = tuanID
		} else {
			nmShopTuanIDMap[shopID] = nmShopTuanIDMap[shopID] "@@@" tuanID
		}
	} ARGIND == 4 {
		if (NF < 5) {
			next
		}
		# 获取当前团购信息的最大ID
		tuanID = $1;
		if (tuanID == "tuanid") {
			next
		}
		if (tuanID > dpMaxTuanID) {
			dpMaxTuanID = tuanID;
		}
	} END {
		for (nmShopID in nmShopTuanIDMap) {
			if (!(nmShopID in mergeIDMap)) {
				continue
			}
			dpID = mergeIDMap[nmShopID]

			tuanIDs = nmShopTuanIDMap[nmShopID]
			tuanIDLen = split(tuanIDs, tuanIDArray, "@@@")
			for (i=1; i<=tuanIDLen; i++) {
				tuanID = tuanIDArray[i]
				if (!(tuanID in nmTuanMap)) {
					continue
				}
				nmTuanInfo = nmTuanMap[tuanID]
				print (++dpMaxTuanID) "\t" dpID "\t" nmTuanInfo
			}
		}
	}' $equalFile $nmTuan $nmShop $dpTuan

	LOG "merge tuan file for restaurant done."
}


# 去除重复的团购信息
function uniq_tuan() {
	input=$1;
	# title: 百度糯米的优惠券
	# title：百度糯米的原价，售价，销量 float->int
	awk -F'\t' 'BEGIN {
		siteRow=-1; titleRow=-1; priceRow=-1; valueRow=-1;
	} {
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "site") { siteRow = row; continue }
				if ($row == "title") { titleRow = row; continue }
				if ($row == "price") { priceRow = row; continue }
				if ($row == "value") { valueRow = row; continue }
			}
			print
		} else {
			if ($siteRow == "百度糯米") {
				if ($titleRow~/代金券/) {
					$titleRow = "代金券";
				}
				gsub(/\.0$/, "", $priceRow)
				gsub(/\.0$/, "", $valueRow)
			}
			line = $1;
			for (row=2; row<=NF; ++row) {
				line = line "\t" $row
			}
			print line
		}
	}' $input.merge > $input.uniq
	mv $input $input.bak
	mv $input.uniq $input
	LOG "unique tuan file [$input] done. output is [$input]"

}

# 分发数据到Output下
function dispatch() {
	for cityDir in $(ls $DIANPING_DIR/); do
		typeDir="restaurant"
		if [ "$cityDir" == "movie" ]; then
			LOG "movie dir, continue."
			continue
		fi

		for srcFile in $(ls $DIANPING_DIR/$cityDir/$typeDir/*.table); do
			destFile=${srcFile/Input/Output}
			destDir=$(dirname $destFile)
			if [ ! -d $destDir ]; then
				mkdir -p $destDir
			fi
			mv $destFile $destFile.bak
			cp $srcFile $destFile
		done

		LOG "dispatch data for $cityDir city done."
	done
}



function main() {
	# 这里只考虑美食团购的合并
	for cityDir in $(ls $DIANPING_DIR/); do
		typeDir="restaurant"
		if [ "$cityDir" = "movie" ]; then
			LOG "movie dir, continue."
			continue
		fi

		dianpingShopFile=$DIANPING_DIR/$cityDir/$typeDir/dianping_detail.baseinfo.table
		dianpingTuanFile=$DIANPING_DIR/$cityDir/$typeDir/dianping_detail.tuan.table
		dianpingExtractData="tmp/${cityDir}_dianping_data"
		extract_dianping_restaurant $dianpingShopFile $dianpingExtractData
		

		nuomiShopFile=$NUOMI_TUAN_DIR/${cityDir}_shop
		nuomiTuanFile=$NUOMI_TUAN_DIR/${cityDir}_tuan
		nuomiExtractData="tmp/${cityDir}_nuomi_data"
		if [ ! -f $nuomiShopFile ]; then
			LOG "$nuomiShopFile not exist, continue."
			continue
		fi
		extract_nuomi_restaurant $nuomiShopFile $nuomiExtractData
		
		mergeData="tmp/${cityDir}_merge_data"
		 merge_restaurant $nuomiExtractData $dianpingExtractData $mergeData

		# 这里能够复用之前合并的结果？？？？？？？？？？

		# 上一步得到排序的文件，用于合并
		telEqual=$mergeData.tel.equal;  titleEqual=$mergeData.title.equal;
		python bin/ServiceAppRestaurantMerger.py -title $mergeData.title.sort > $titleEqual
		python bin/ServiceAppRestaurantMerger.py -tel $mergeData.tel.sort > $telEqual
		LOG "apply merge strategy done."
		
		mergeEqual="tmp/${cityDir}.equal"
		filte_invalid_equal $telEqual > $mergeEqual
		filte_invalid_equal $titleEqual >> $mergeEqual
		LOG "filte invalid merge equal pairs done. [$mergeEqual]"
		
		# 合并团购信息
		cat $dianpingTuanFile > $dianpingTuanFile.merge
		merge_tuan $mergeEqual $dianpingTuanFile $nuomiTuanFile $nuomiShopFile >> $dianpingTuanFile.merge
		
		# 去除重复的团购信息
		uniq_tuan $dianpingTuanFile
		

		LOG "handle $cityDir city's tuan merge done."
	done


	# 分发到Output目录下
	dispatch
}

#这里不再合并团购数据了
#main


# 分发到Output目录下
dispatch


