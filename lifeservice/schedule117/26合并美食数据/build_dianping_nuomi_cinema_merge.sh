#!/bin/bash
#coding=gb2312

# 合并大众点评的影院与糯米团购的影院信息
# 注意！！！！ 不同商家对于城市下区的名称的归一，需要处理
#			   否则无法应用到合并策略里的 区域相同 这个重要条件


. ./bin/Tool.sh

VR_CINEMA_DIR="Input"
NUOMI_TUAN_DIR="/search/zhangk/Fuwu/Source/Cooperation/Nuomi/data/"


# 大众点评网的美食店铺数据
function extract_vr_cinema() {
	input=$1; output=$2;
	awk -F'\t' 'BEGIN {
		idRow=-1; titleRow=-1; poiRow=-1;
		addrRow=-1; cityRow=-1; areaRow=-1;
		domain="http://www.vr.com/"
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "id"){ idRow = row;}
				else if ($row == "title") { titleRow = row;}
				else if ($row == "poi") { poiRow = row;}
				else if ($row == "address") { addrRow = row;}
				else if ($row == "city") { cityRow = row;}
				else if ($row == "district") { areaRow = row;}
			}
			next
		}
		if ($cityRow == "" || $areaRow == "") {
			next
		}

		id=$idRow; title=$titleRow; city=$cityRow;
		poi=$poiRow; addr=$addrRow; area=$areaRow; 
		
		# 归一化 area title  tel addr
		# area:  黄浦区;黄浦
		if (area~/;/) {
			len = split(area, areaArray, ";")
			area = areaArray[1]
		}

		gsub("（", "", title);
		gsub("）", "", title);

		#gsub("-", "", tel);
		#gsub(",", "|", tel);
		
		gsub(area, "", addr)
		gsub(/\([^\)]+\)/, "", addr);
		gsub(/（[^）]+）/, "", addr);

		print id"\t"domain"\t"title"\t"city"\t"area"\t"addr"\t"poi
	}' $input > $output
	LOG "extract cinema info of [$input] done. [$output]"
}



# 糯米团购涉及到的店铺信息
function extract_nuomi_cinema() {
	input=$1;  output=$2
	# 只取美食类 && 区域不能为空
	awk -F'\t' 'BEGIN {
		titleRow=-1; cityRow=-1; areaRow=-1; 
		addrRow=-1; poiRow=-1; categoryRow=-1;
		domain = "http://www.nuomi.com/"
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "name"){ titleRow = row; continue }
				#if ($row == "tel"){ telRow = row; continue }
				if ($row == "city"){ cityRow = row; continue }
				if ($row == "area"){ areaRow = row; continue }
				if ($row == "address"){ addrRow = row; continue }
				if ($row == "poi"){ poiRow = row; continue }
				if ($row == "secondCategory"){ categoryRow = row; continue }
			}
			next
		}
		# 过滤非餐馆类的团购
		if ($categoryRow != "电影" || $areaRow == "") {
			next
		}

		title=$titleRow; area=$areaRow; 
		addr=$addrRow;  poi=$poiRow; city=$cityRow;
		id = area "@" title "@" poi;

		# 归一化title addr
		gsub("（", "", title);
		gsub("）", "", title);
		
		# 归一化地址, 去除 **市 以及括号里面的东西
		citySubStr = city"市"
		#gsub(/^[^市]+市/, "", addr)
		gsub(citySubStr, "", addr)
		gsub(area, "", addr)
		gsub(/\([^\)]+\)/, "", addr);
		gsub(/（[^）]+）/, "", addr);

		print id"\t"domain"\t"title"\t"city"\t"area"\t"addr"\t"poi
	}' $input > $output
	LOG "extract nuomi cinema info of [$input] done. [$output]"

}


# id  title  city  area addr  poi
# 海淀区@国图电影院@39.9489,116.3307  国图电影院  北京    海淀区  中关村南大街33号国家图书馆内    39.9489,116.3307

function merge_cinema() {
	nuomiData=$1;  vrData=$2;  mergeData=$3;

	#uniq nuomi_data
	sort -t$'\t' -k1,1 $nuomiData | uniq > $nuomiData.uniq
	cat $nuomiData.uniq $vrData > $mergeData

	# 区域相同，名称相同，电话,地址,poi相近或相似
	# 打印表头
	echo -e "id\turl\ttitle\tcity\tarea\taddr\tpoi" > $mergeData.title.sort
	sort -t$'\t' -k4,4 -k5,5 -k3,3  $mergeData >> $mergeData.title.sort

	echo -e "id\turl\ttitle\tcity\tarea\taddr\tpoi" > $mergeData.addr.sort
	sort -t$'\t' -k6,6 -k4,4  $mergeData >> $mergeData.addr.sort

:<<EOF
	# 区域相同，电话相同  (名称相似)
	echo -e "id\turl\ttitle\tcity\tarea\taddr\tpoi" > $mergeData.tel.sort
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
EOF
	LOG "merge for [$vrData] and [$nuomiData] done. output=[$mergeData]"
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
	equalFile=$1; nmTuan=$2; nmShop=$3;
	# equalFile <nmID  vrID>
	# nmTuan <id,url,businessTitle,title,value,price,bought,site,image,city,firstCategory,secondCategory,startTime,endTime>
	# nmShop <name, tel, city, area, address, poi, firstCategory, secondCategory, tuanid>

	awk -F'\t' 'BEGIN {
		# 初始化，如果需要
		cinemaTuanID = 0;
	} ARGIND == 1 {
		# 糯米与VR影院ID映射
		nmID=$1;  vrID=$2;
		mergeIDMap[nmID] = vrID
	###### print nmID "\t" vrID
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
		shopID = area "@" title "@" poi;
		if (!(shopID in nmShopTuanIDMap)) {
			nmShopTuanIDMap[shopID] = tuanID
		} else {
			nmShopTuanIDMap[shopID] = nmShopTuanIDMap[shopID] "@@@" tuanID
		}
	} END {
		print "tuanid\tcinemaid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell"
		for (nmShopID in nmShopTuanIDMap) {
			if (!(nmShopID in mergeIDMap)) {
				continue
			}
			vrCinemaID = mergeIDMap[nmShopID]

			tuanIDs = nmShopTuanIDMap[nmShopID]
			tuanIDLen = split(tuanIDs, tuanIDArray, "@@@")
			for (i=1; i<=tuanIDLen; i++) {
				tuanID = tuanIDArray[i]
				if (!(tuanID in nmTuanMap)) {
					continue
				}
				nmTuanInfo = nmTuanMap[tuanID]
				print (++cinemaTuanID) "\t" vrCinemaID "\t" nmTuanInfo
			}
		}
	}' $equalFile $nmTuan $nmShop

	#LOG "merge tuan file for cinema done."
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
	for srcFile in $(ls $VR_CINEMA_DIR/movie/*.table); do
		destFile=${srcFile/Input/Output}
		destDir=$(dirname $destFile)
		if [ ! -d $destDir ]; then
			mkdir -p $destDir
		fi
		mv $destFile $destFile.bak
		cp $srcFile $destFile
	done

	LOG "dispatch data for cinema done."
}



function main() {
:<<EOF
EOF
	# 抽取时光网的影院信息
	typeDir="movie"
	vrCinemaFile=$VR_CINEMA_DIR/$typeDir/cinema_detail.table
	vrCinemaExtractData="tmp/vr_cinema_data"

	extract_vr_cinema $vrCinemaFile $vrCinemaExtractData


	# 抽取糯米团购里影院的数据
	nuomiShopFile="tmp/nuomi_shop"
	nuomiTuanFile="tmp/nuomi_tuan"

	cat $NUOMI_TUAN_DIR/*_shop > $nuomiShopFile
	cat $NUOMI_TUAN_DIR/*_tuan > $nuomiTuanFile

	nuomiExtractData="tmp/nuomi_cinema"
	extract_nuomi_cinema $nuomiShopFile $nuomiExtractData
	

	mergeData="tmp/cinema_merge_data"
	merge_cinema $nuomiExtractData $vrCinemaExtractData $mergeData


	# 上一步得到排序的文件，用于合并
	addrEqual=$mergeData.addr.equal;  titleEqual=$mergeData.title.equal;
	python bin/ServiceAppCinemaMerger.py -title $mergeData.title.sort > $titleEqual
	python bin/ServiceAppCinemaMerger.py -addr $mergeData.addr.sort > $addrEqual
	#python bin/ServiceAppRestaurantMerger.py -tel $mergeData.tel.sort > $telEqual
	LOG "apply merge strategy done."
	
	mergeEqual="tmp/cinema_equal_merge"
	# 合并不同策略下的equal merge,去重
	cat $titleEqual $addrEqual | awk -F'\t' '{
		if (NF != 3) { next }
		id1=$2; id2=$3;
		if (id1~/@/) {
			mergeItem = id1 "\t" id2
		} else {
			mergeItem = id2 "\t" id1
		}
		if (!(mergeItem in existMergeItems)) {
			existMergeItems[mergeItem] = 1
			print mergeItem
		}
	}' > $mergeEqual
	LOG "merge and uniq equal items done. [$mergeEqual]"

	# 生成团购信息
	cinemaTuanFile="$VR_CINEMA_DIR/movie/cinema_tuan.table"

	merge_tuan $mergeEqual $nuomiTuanFile $nuomiShopFile > $cinemaTuanFile
	LOG "get tuan info for cinema done. [$cinemaTuanFile]"
	exit -1;
		
	# 分发到Output目录下
	dispatch

	LOG "handle nuomi's cinema tuan info done."


}

main
#dispatch
